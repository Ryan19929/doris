// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.backup;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.nereids.trees.plans.commands.BackupCommand.BackupContent;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.ref.Reference;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manually invoked backup metadata memory benchmark.
 *
 * <p>The class deliberately does not end in {@code Test}, so ordinary Surefire runs do not discover it.
 * The fe-core Surefire fork has a 1 GiB default heap. A 200,000 tablet by 3 replica run must override that
 * default and assert that the override took effect. For example, run this command from {@code fe/}:</p>
 *
 * <pre>
 * mvn test -pl fe-common,fe-core -am \
 *   -Dtest=org.apache.doris.backup.BackupRestoreMemoryBenchmark \
 *   -Ddoris.benchmark.stage=selective_copy \
 *   -Ddoris.benchmark.scenario=full_streaming \
 *   -Ddoris.benchmark.tablets=200000 \
 *   -Ddoris.benchmark.replicas=3 \
 *   -Ddoris.benchmark.expected_max_heap_bytes=2147483648 \
 *   -Dfe.ut.max.heap=2g \
 *   -Dfe.ut.extra.jvm.args=-Xms2g \
 *   -Dsurefire.failIfNoSpecifiedTests=false \
 *   -DfailIfNoTests=false -Dcheckstyle.skip=true \
 *   -Dmaven.build.cache.enabled=false -Dfe_ut_parallel=1
 * </pre>
 *
 * <p>Run exactly one stage and scenario in each forked JVM. Valid stages are {@code selective_copy},
 * {@code backup_meta_write}, {@code backup_meta_read}, {@code journal_write}, and {@code journal_replay}.
 * Reader stages accept {@code doris.benchmark.read_table_streaming} and
 * {@code doris.benchmark.read_job_streaming} for mixed writer/reader compatibility measurements. Use
 * {@code -Dfe.ut.extra.jvm.args=...} for optional JVM flags such as JFR recording; this is composed with the
 * JaCoCo late-replacement {@code argLine} instead of replacing it.</p>
 *
 * <p>{@code peak_heap_bytes} is sampled every 10 ms while the operation runs, so it is an approximate peak and
 * may miss short-lived allocation spikes. The GC collection count and time metrics bracket only the operation.
 * After the operation returns, the benchmark performs a full GC while keeping its result reachable and reports
 * {@code retained_after_gc_bytes}; this estimates the live result graph rather than transient peak allocation.
 * The elapsed time excludes result verification and both the retained-heap GC and its measurement.</p>
 */
public class BackupRestoreMemoryBenchmark {
    private static final String RESULT_PREFIX = "BACKUP_RESTORE_MEMORY_BENCHMARK_RESULT=";
    private static final String STAGE_PROPERTY = "doris.benchmark.stage";
    private static final String SCENARIO_PROPERTY = "doris.benchmark.scenario";
    private static final String TABLET_COUNT_PROPERTY = "doris.benchmark.tablets";
    private static final String REPLICA_COUNT_PROPERTY = "doris.benchmark.replicas";
    private static final String EXPECTED_MAX_HEAP_PROPERTY = "doris.benchmark.expected_max_heap_bytes";
    private static final String READ_TABLE_STREAMING_PROPERTY = "doris.benchmark.read_table_streaming";
    private static final String READ_JOB_STREAMING_PROPERTY = "doris.benchmark.read_job_streaming";
    private static final int DEFAULT_TABLET_COUNT = 200_000;
    private static final int DEFAULT_REPLICA_COUNT = 3;
    private static final long DB_ID = 10L;
    private static final long TABLE_ID = 20L;
    private static final long PARTITION_ID = 30L;
    private static final long INDEX_ID = 40L;
    private static final long FIRST_TABLET_ID = 1_000_000L;
    private static final long FIRST_REPLICA_ID = 10_000_000L;
    private static final String TABLE_NAME = "backup_memory_benchmark_table";

    @Test
    public void runSingleStage() throws Throwable {
        Stage stage = Stage.parse(requiredProperty(STAGE_PROPERTY));
        Scenario scenario = Scenario.parse(requiredProperty(SCENARIO_PROPERTY));
        int tabletCount = positiveIntProperty(TABLET_COUNT_PROPERTY, DEFAULT_TABLET_COUNT);
        int replicaCount = positiveIntProperty(REPLICA_COUNT_PROPERTY, DEFAULT_REPLICA_COUNT);
        long expectedMaxHeap = nonNegativeLongProperty(EXPECTED_MAX_HEAP_PROPERTY, 0L);
        boolean readTableStreaming = booleanProperty(READ_TABLE_STREAMING_PROPERTY, scenario.tableStreaming);
        boolean readJobStreaming = booleanProperty(READ_JOB_STREAMING_PROPERTY, scenario.jobStreaming);

        long maxHeap = Runtime.getRuntime().maxMemory();
        verifyExpectedMaxHeap(expectedMaxHeap, maxHeap);

        boolean savedReserveReplicas = Config.backup_meta_reserve_replica_info;
        boolean savedTableStreaming = Config.enable_table_meta_streaming_json;
        boolean savedJobStreaming = Config.enable_backup_restore_job_streaming_json;
        boolean savedBackupCompression = Config.backup_job_compressed_serialization;
        Path tempDirectory = null;
        Path backupMetaFile = null;
        Path journalFile = null;
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("stage", stage.propertyValue);
        metrics.put("scenario", scenario.propertyValue);
        metrics.put("tablet_count", tabletCount);
        metrics.put("replicas_per_tablet", replicaCount);
        metrics.put("reserve_replicas", scenario.reserveReplicas);
        metrics.put("table_streaming", scenario.tableStreaming);
        metrics.put("job_streaming", scenario.jobStreaming);
        metrics.put("read_table_streaming", readTableStreaming);
        metrics.put("read_job_streaming", readJobStreaming);
        metrics.put("max_heap_bytes", maxHeap);

        Throwable failure = null;
        boolean outOfMemory = false;
        String outOfMemoryResult = buildMinimalFailureResult(
                "oom", stage, scenario, tabletCount, replicaCount, maxHeap, null);
        try {
            tempDirectory = Files.createTempDirectory("doris-backup-memory-benchmark-");
            backupMetaFile = tempDirectory.resolve("backup-meta.bin");
            journalFile = tempDirectory.resolve("backup-job.journal");
            outOfMemoryResult = buildMinimalFailureResult(
                    "oom", stage, scenario, tabletCount, replicaCount, maxHeap, tempDirectory.toString());
            Config.backup_meta_reserve_replica_info = scenario.reserveReplicas;
            Config.enable_table_meta_streaming_json = scenario.tableStreaming;
            Config.enable_backup_restore_job_streaming_json = scenario.jobStreaming;
            Config.backup_job_compressed_serialization = false;
            runStage(stage, scenario, tabletCount, replicaCount, readTableStreaming, readJobStreaming,
                    backupMetaFile, journalFile, metrics);
        } catch (OutOfMemoryError e) {
            failure = e;
            outOfMemory = true;
        } catch (Throwable e) {
            failure = e;
        } finally {
            Config.backup_meta_reserve_replica_info = savedReserveReplicas;
            Config.enable_table_meta_streaming_json = savedTableStreaming;
            Config.enable_backup_restore_job_streaming_json = savedJobStreaming;
            Config.backup_job_compressed_serialization = savedBackupCompression;
        }

        if (outOfMemory) {
            cleanupAfterOutOfMemory(backupMetaFile, journalFile, tempDirectory);
        } else if (tempDirectory != null) {
            try {
                deleteRecursively(tempDirectory);
            } catch (Throwable cleanupFailure) {
                if (failure == null) {
                    failure = cleanupFailure;
                } else {
                    failure.addSuppressed(cleanupFailure);
                }
            }
        }

        if (failure == null) {
            metrics.put("status", "ok");
            System.out.println(RESULT_PREFIX + GsonUtils.GSON.toJson(metrics));
            return;
        }
        if (outOfMemory) {
            emitOutOfMemory(outOfMemoryResult);
        } else {
            metrics.put("status", "error");
            metrics.put("error_type", failure.getClass().getName());
            metrics.put("error_message", String.valueOf(failure.getMessage()));
            try {
                System.out.println(RESULT_PREFIX + GsonUtils.GSON.toJson(metrics));
            } catch (Throwable outputFailure) {
                failure.addSuppressed(outputFailure);
                emitError(stage, scenario, tabletCount, replicaCount, maxHeap,
                        tempDirectory == null ? null : tempDirectory.toString());
            }
        }
        throw failure;
    }

    private static void runStage(Stage stage, Scenario scenario, int tabletCount, int replicaCount,
            boolean readTableStreaming, boolean readJobStreaming, Path backupMetaFile, Path journalFile,
            Map<String, Object> metrics) throws Throwable {
        long expectedReplicas = scenario.reserveReplicas ? (long) tabletCount * replicaCount : 0L;
        switch (stage) {
            case SELECTIVE_COPY:
                runSelectiveCopyStage(scenario, tabletCount, replicaCount, expectedReplicas, metrics);
                return;
            case BACKUP_META_WRITE:
                runBackupMetaWriteStage(scenario, tabletCount, replicaCount, expectedReplicas,
                        backupMetaFile, metrics);
                return;
            case BACKUP_META_READ:
                runBackupMetaReadStage(scenario, tabletCount, replicaCount, expectedReplicas,
                        readTableStreaming, backupMetaFile, metrics);
                return;
            case JOURNAL_WRITE:
                runJournalWriteStage(scenario, tabletCount, replicaCount, expectedReplicas,
                        journalFile, metrics);
                return;
            case JOURNAL_REPLAY:
                runJournalReplayStage(scenario, tabletCount, replicaCount, expectedReplicas,
                        readTableStreaming, readJobStreaming, journalFile, metrics);
                return;
            default:
                throw new AssertionError("unhandled benchmark stage: " + stage);
        }
    }

    private static void runSelectiveCopyStage(Scenario scenario, int tabletCount, int replicaCount,
            long expectedReplicas, Map<String, Object> metrics) throws Throwable {
        long setupStart = System.nanoTime();
        OlapTable sourceTable = createTable(tabletCount, replicaCount, true);
        Assert.assertEquals((long) tabletCount * replicaCount, countReplicas(sourceTable));
        metrics.put("setup_ms", elapsedMillis(setupStart));

        OlapTable copiedTable = measure(metrics,
                () -> sourceTable.selectiveCopy(null, IndexExtState.VISIBLE, true));

        Assert.assertNotNull("selectiveCopy returned null without propagating its failure", copiedTable);
        Assert.assertEquals(tabletCount, countTablets(copiedTable));
        Assert.assertEquals(expectedReplicas, countReplicas(copiedTable));
        Assert.assertEquals((long) tabletCount * replicaCount, countReplicas(sourceTable));
        metrics.put("copied_replica_count", countReplicas(copiedTable));
        metrics.put("writer_table_streaming", scenario.tableStreaming);
    }

    private static void runBackupMetaWriteStage(Scenario scenario, int tabletCount, int replicaCount,
            long expectedReplicas, Path file, Map<String, Object> metrics) throws Throwable {
        long setupStart = System.nanoTime();
        BackupMeta backupMeta = createDetachedBackupMeta(scenario, tabletCount, replicaCount, expectedReplicas);
        metrics.put("setup_ms", elapsedMillis(setupStart));

        measure(metrics, () -> {
            backupMeta.writeToFile(file.toFile());
            return null;
        });

        metrics.put("payload_bytes", Files.size(file));
        metrics.put("writer_table_streaming", scenario.tableStreaming);
    }

    private static void runBackupMetaReadStage(Scenario scenario, int tabletCount, int replicaCount,
            long expectedReplicas, boolean readTableStreaming, Path file, Map<String, Object> metrics)
            throws Throwable {
        long setupStart = System.nanoTime();
        prepareBackupMetaFixture(scenario, tabletCount, replicaCount, expectedReplicas, file);
        Config.enable_table_meta_streaming_json = readTableStreaming;
        metrics.put("setup_ms", elapsedMillis(setupStart));
        metrics.put("payload_bytes", Files.size(file));
        metrics.put("fixture_writer_table_streaming", true);

        BackupMeta restored = measure(metrics,
                () -> BackupMeta.fromFile(file.toString(), FeConstants.meta_version));

        verifyBackupMeta(restored, tabletCount, expectedReplicas);
    }

    private static void runJournalWriteStage(Scenario scenario, int tabletCount, int replicaCount,
            long expectedReplicas, Path file, Map<String, Object> metrics) throws Throwable {
        long setupStart = System.nanoTime();
        BackupJob backupJob = createDetachedBackupJob(scenario, tabletCount, replicaCount, expectedReplicas);
        JournalEntity journalEntity = createBackupJournalEntity(backupJob);
        metrics.put("setup_ms", elapsedMillis(setupStart));

        measure(metrics, () -> {
            writeJournal(file, journalEntity);
            return null;
        });

        metrics.put("payload_bytes", Files.size(file));
        metrics.put("writer_table_streaming", scenario.tableStreaming);
        metrics.put("writer_job_streaming", scenario.jobStreaming);
    }

    private static void runJournalReplayStage(Scenario scenario, int tabletCount, int replicaCount,
            long expectedReplicas, boolean readTableStreaming, boolean readJobStreaming, Path file,
            Map<String, Object> metrics) throws Throwable {
        long setupStart = System.nanoTime();
        prepareJournalFixture(scenario, tabletCount, replicaCount, expectedReplicas, file);
        Config.enable_table_meta_streaming_json = readTableStreaming;
        Config.enable_backup_restore_job_streaming_json = readJobStreaming;
        metrics.put("setup_ms", elapsedMillis(setupStart));
        metrics.put("payload_bytes", Files.size(file));
        metrics.put("fixture_writer_table_streaming", true);
        metrics.put("fixture_writer_job_streaming", true);

        JournalEntity restored = measure(metrics, () -> readJournal(file));

        verifyBackupJob(restored, tabletCount, expectedReplicas);
    }

    private static BackupMeta createDetachedBackupMeta(Scenario scenario, int tabletCount, int replicaCount,
            long expectedReplicas) {
        OlapTable table = createTable(tabletCount, replicaCount, scenario.reserveReplicas);
        Assert.assertEquals(tabletCount, countTablets(table));
        Assert.assertEquals(expectedReplicas, countReplicas(table));
        return new BackupMeta(Collections.singletonList(table), Collections.emptyList());
    }

    private static BackupJob createDetachedBackupJob(Scenario scenario, int tabletCount, int replicaCount,
            long expectedReplicas) throws ReflectiveOperationException {
        BackupMeta backupMeta = createDetachedBackupMeta(scenario, tabletCount, replicaCount, expectedReplicas);
        BackupJob backupJob = new BackupJob("backup_memory_benchmark", DB_ID, "benchmark_db",
                Lists.newArrayList(), 3_600_000L, BackupContent.METADATA_ONLY, null, 50L, 60L);
        setBackupMeta(backupJob, backupMeta);
        return backupJob;
    }

    private static void prepareBackupMetaFixture(Scenario scenario, int tabletCount, int replicaCount,
            long expectedReplicas, Path file) throws IOException {
        // Use the bounded-memory writer for fixture setup so a reader-stage JVM measures only the reader graph.
        // Streaming and legacy table metadata remain byte-compatible; read_* properties select the reader path.
        BackupMeta backupMeta = createDetachedBackupMeta(scenario, tabletCount, replicaCount, expectedReplicas);
        boolean previousTableStreaming = Config.enable_table_meta_streaming_json;
        try {
            Config.enable_table_meta_streaming_json = true;
            backupMeta.writeToFile(file.toFile());
        } finally {
            Config.enable_table_meta_streaming_json = previousTableStreaming;
        }
    }

    private static void prepareJournalFixture(Scenario scenario, int tabletCount, int replicaCount,
            long expectedReplicas, Path file) throws IOException, ReflectiveOperationException {
        // Keep the fixture writer outside measure() and use bounded-memory serialization. The helper return plus
        // measure()'s full GC prevents the writer graph from being counted with the replay graph.
        BackupJob backupJob = createDetachedBackupJob(scenario, tabletCount, replicaCount, expectedReplicas);
        boolean previousTableStreaming = Config.enable_table_meta_streaming_json;
        boolean previousJobStreaming = Config.enable_backup_restore_job_streaming_json;
        try {
            Config.enable_table_meta_streaming_json = true;
            Config.enable_backup_restore_job_streaming_json = true;
            writeJournal(file, createBackupJournalEntity(backupJob));
        } finally {
            Config.enable_table_meta_streaming_json = previousTableStreaming;
            Config.enable_backup_restore_job_streaming_json = previousJobStreaming;
        }
    }

    private static <T> T measure(Map<String, Object> metrics, CheckedSupplier<T> operation) throws Throwable {
        forceFullGc();
        long baselineHeap = usedHeap();
        metrics.put("baseline_heap_bytes", baselineHeap);
        GcSnapshot gcBefore = gcSnapshot();
        HeapPeakSampler heapSampler = new HeapPeakSampler();
        long startNanos = System.nanoTime();
        T result;
        try {
            result = operation.get();
        } catch (OutOfMemoryError e) {
            heapSampler.abortAfterOutOfMemory();
            throw e;
        } catch (Throwable e) {
            try {
                heapSampler.close();
            } catch (OutOfMemoryError samplerOutOfMemory) {
                samplerOutOfMemory.addSuppressed(e);
                throw samplerOutOfMemory;
            } catch (Throwable closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
        long elapsedMillis = elapsedMillis(startNanos);
        heapSampler.close();
        long peakHeap = heapSampler.peakBytes();
        GcSnapshot gcAfter = gcSnapshot();
        forceFullGc();
        long retainedHeap = usedHeap();
        Reference.reachabilityFence(result);
        metrics.put("stage_elapsed_ms", elapsedMillis);
        metrics.put("peak_heap_bytes", peakHeap);
        metrics.put("peak_heap_delta_bytes", Math.max(0L, peakHeap - baselineHeap));
        metrics.put("gc_collection_count_before", gcBefore.collectionCount);
        metrics.put("gc_collection_time_ms_before", gcBefore.collectionTimeMillis);
        metrics.put("gc_collection_count_after", gcAfter.collectionCount);
        metrics.put("gc_collection_time_ms_after", gcAfter.collectionTimeMillis);
        metrics.put("gc_collection_count_delta", gcAfter.collectionCount - gcBefore.collectionCount);
        metrics.put("gc_collection_time_ms_delta", gcAfter.collectionTimeMillis - gcBefore.collectionTimeMillis);
        metrics.put("retained_after_gc_bytes", retainedHeap);
        metrics.put("retained_delta_bytes", retainedHeap - baselineHeap);
        return result;
    }

    private static OlapTable createTable(int tabletCount, int replicaCount, boolean includeReplicas) {
        List<Column> schema = new ArrayList<>();
        Column keyColumn = new Column("key", PrimitiveType.INT);
        keyColumn.setIsKey(true);
        schema.add(keyColumn);

        RandomDistributionInfo distribution = new RandomDistributionInfo(tabletCount);
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(PARTITION_ID, new DataProperty(TStorageMedium.HDD));
        partitionInfo.setReplicaAllocation(PARTITION_ID, new ReplicaAllocation((short) replicaCount));
        OlapTable table = new OlapTable(TABLE_ID, TABLE_NAME, schema, KeysType.DUP_KEYS,
                partitionInfo, distribution);
        MaterializedIndex index = new MaterializedIndex(INDEX_ID, IndexState.NORMAL);
        List<Tablet> tablets = new ArrayList<>(tabletCount);
        long replicaId = FIRST_REPLICA_ID;
        for (int tabletOffset = 0; tabletOffset < tabletCount; tabletOffset++) {
            LocalTablet tablet = new LocalTablet(FIRST_TABLET_ID + tabletOffset);
            if (includeReplicas) {
                for (int replicaOffset = 0; replicaOffset < replicaCount; replicaOffset++) {
                    tablet.addReplica(new LocalReplica(replicaId++, replicaOffset + 1L,
                            ReplicaState.NORMAL, 1L, 0), true);
                }
            }
            tablets.add(tablet);
        }
        index.appendTablets(tablets);
        table.addPartition(new Partition(PARTITION_ID, "p1", index, distribution));
        table.setIndexMeta(INDEX_ID, TABLE_NAME, schema, 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setBaseIndexId(INDEX_ID);
        return table;
    }

    private static long countReplicas(OlapTable table) {
        long replicas = 0L;
        for (Partition partition : table.getPartitions()) {
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    replicas += tablet.getReplicas().size();
                }
            }
        }
        return replicas;
    }

    private static int countTablets(OlapTable table) {
        int tablets = 0;
        for (Partition partition : table.getPartitions()) {
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                tablets += index.getTablets().size();
            }
        }
        return tablets;
    }

    private static void verifyBackupMeta(BackupMeta restored, int expectedTablets, long expectedReplicas) {
        Table table = restored.getTable(TABLE_ID);
        Assert.assertTrue(table instanceof OlapTable);
        Assert.assertEquals(expectedTablets, countTablets((OlapTable) table));
        Assert.assertEquals(expectedReplicas, countReplicas((OlapTable) table));
    }

    private static void setBackupMeta(BackupJob job, BackupMeta backupMeta) throws ReflectiveOperationException {
        Field field = BackupJob.class.getDeclaredField("backupMeta");
        field.setAccessible(true);
        field.set(job, backupMeta);
    }

    private static JournalEntity createBackupJournalEntity(BackupJob job) {
        JournalEntity entity = new JournalEntity();
        entity.setOpCode(OperationType.OP_BACKUP_JOB);
        entity.setData(job);
        return entity;
    }

    private static void writeJournal(Path file, JournalEntity entity) throws IOException {
        try (DataOutputStream output = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(file)))) {
            entity.write(output);
        }
    }

    private static JournalEntity readJournal(Path file) throws IOException {
        JournalEntity restored = new JournalEntity();
        try (DataInputStream input = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(file)))) {
            restored.readFields(input);
        }
        return restored;
    }

    private static void verifyBackupJob(JournalEntity restored, int expectedTablets, long expectedReplicas) {
        Assert.assertEquals(OperationType.OP_BACKUP_JOB, restored.getOpCode());
        Assert.assertTrue(restored.getData() instanceof BackupJob);
        BackupMeta backupMeta = ((BackupJob) restored.getData()).getBackupMeta();
        Assert.assertNotNull(backupMeta);
        Table table = backupMeta.getTable(TABLE_ID);
        Assert.assertTrue(table instanceof OlapTable);
        Assert.assertEquals(expectedTablets, countTablets((OlapTable) table));
        Assert.assertEquals(expectedReplicas, countReplicas((OlapTable) table));
    }

    private static void forceFullGc() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            System.gc();
            Thread.sleep(100L);
        }
    }

    private static long usedHeap() {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }

    private static GcSnapshot gcSnapshot() {
        long collectionCount = 0L;
        long collectionTimeMillis = 0L;
        for (GarbageCollectorMXBean collector : ManagementFactory.getGarbageCollectorMXBeans()) {
            long collectorCount = collector.getCollectionCount();
            long collectorTimeMillis = collector.getCollectionTime();
            if (collectorCount >= 0L) {
                collectionCount += collectorCount;
            }
            if (collectorTimeMillis >= 0L) {
                collectionTimeMillis += collectorTimeMillis;
            }
        }
        return new GcSnapshot(collectionCount, collectionTimeMillis);
    }

    private static long elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }

    private static String requiredProperty(String name) {
        String value = System.getProperty(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("missing required system property: " + name);
        }
        return value;
    }

    private static int positiveIntProperty(String name, int defaultValue) {
        int value = Integer.parseInt(System.getProperty(name, String.valueOf(defaultValue)));
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive: " + value);
        }
        return value;
    }

    private static long nonNegativeLongProperty(String name, long defaultValue) {
        long value = Long.parseLong(System.getProperty(name, String.valueOf(defaultValue)));
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative: " + value);
        }
        return value;
    }

    private static boolean booleanProperty(String name, boolean defaultValue) {
        return Boolean.parseBoolean(System.getProperty(name, String.valueOf(defaultValue)));
    }

    private static void verifyExpectedMaxHeap(long expectedMaxHeap, long actualMaxHeap) {
        if (expectedMaxHeap == 0L) {
            return;
        }
        long minimum = expectedMaxHeap * 9L / 10L;
        if (actualMaxHeap < minimum || actualMaxHeap > expectedMaxHeap) {
            throw new IllegalStateException("max heap is outside the expected range [" + minimum + ", "
                    + expectedMaxHeap + "]: " + actualMaxHeap);
        }
    }

    private static void emitOutOfMemory(String result) {
        try {
            System.out.println(result);
        } catch (Throwable ignored) {
            // The process may have no allocation headroom. Preserve the original OOM.
        }
    }

    private static void emitError(Stage stage, Scenario scenario, int tabletCount,
            int replicaCount, long maxHeap, String tempDirectory) {
        try {
            System.out.println(buildMinimalFailureResult(
                    "error", stage, scenario, tabletCount, replicaCount, maxHeap, tempDirectory));
        } catch (Throwable ignored) {
            // Preserve the original failure if even the fallback result cannot be emitted.
        }
    }

    private static String buildMinimalFailureResult(String status, Stage stage, Scenario scenario,
            int tabletCount, int replicaCount, long maxHeap, String tempDirectory) {
        return RESULT_PREFIX + "{\"status\":\"" + status + "\",\"stage\":\""
                + stage.propertyValue + "\",\"scenario\":\"" + scenario.propertyValue
                + "\",\"tablet_count\":" + tabletCount + ",\"replicas_per_tablet\":" + replicaCount
                + ",\"max_heap_bytes\":" + maxHeap + ",\"temp_directory\":"
                + GsonUtils.GSON.toJson(tempDirectory) + "}";
    }

    private static void cleanupAfterOutOfMemory(Path backupMetaFile, Path journalFile, Path tempDirectory) {
        deleteAfterOutOfMemory(backupMetaFile);
        deleteAfterOutOfMemory(journalFile);
        deleteAfterOutOfMemory(tempDirectory);
    }

    private static void deleteAfterOutOfMemory(Path path) {
        if (path == null) {
            return;
        }
        try {
            Files.deleteIfExists(path);
        } catch (Throwable ignored) {
            // Best-effort OOM cleanup must never replace the original OOM.
        }
    }

    private static void deleteRecursively(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(directory)) {
            Path[] ordered = paths.sorted((left, right) -> right.compareTo(left)).toArray(Path[]::new);
            for (Path path : ordered) {
                Files.deleteIfExists(path);
            }
        }
    }

    private enum Scenario {
        LEGACY("legacy", true, false, false),
        STRIP_REPLICAS("strip_replicas", false, false, false),
        TABLE_STREAMING("table_streaming", false, true, false),
        JOB_STREAMING("job_streaming", false, false, true),
        FULL_STREAMING("full_streaming", false, true, true);

        private final String propertyValue;
        private final boolean reserveReplicas;
        private final boolean tableStreaming;
        private final boolean jobStreaming;

        Scenario(String propertyValue, boolean reserveReplicas, boolean tableStreaming, boolean jobStreaming) {
            this.propertyValue = propertyValue;
            this.reserveReplicas = reserveReplicas;
            this.tableStreaming = tableStreaming;
            this.jobStreaming = jobStreaming;
        }

        private static Scenario parse(String value) {
            String normalized = value.toLowerCase(Locale.ROOT);
            for (Scenario scenario : values()) {
                if (scenario.propertyValue.equals(normalized)) {
                    return scenario;
                }
            }
            throw new IllegalArgumentException("unknown benchmark scenario '" + value
                    + "'; expected legacy, strip_replicas, table_streaming, job_streaming, or full_streaming");
        }
    }

    private enum Stage {
        SELECTIVE_COPY("selective_copy"),
        BACKUP_META_WRITE("backup_meta_write"),
        BACKUP_META_READ("backup_meta_read"),
        JOURNAL_WRITE("journal_write"),
        JOURNAL_REPLAY("journal_replay");

        private final String propertyValue;

        Stage(String propertyValue) {
            this.propertyValue = propertyValue;
        }

        private static Stage parse(String value) {
            String normalized = value.toLowerCase(Locale.ROOT);
            for (Stage stage : values()) {
                if (stage.propertyValue.equals(normalized)) {
                    return stage;
                }
            }
            throw new IllegalArgumentException("unknown benchmark stage '" + value
                    + "'; expected selective_copy, backup_meta_write, backup_meta_read, journal_write, "
                    + "or journal_replay");
        }
    }

    @FunctionalInterface
    private interface CheckedSupplier<T> {
        T get() throws Throwable;
    }

    private static final class GcSnapshot {
        private final long collectionCount;
        private final long collectionTimeMillis;

        private GcSnapshot(long collectionCount, long collectionTimeMillis) {
            this.collectionCount = collectionCount;
            this.collectionTimeMillis = collectionTimeMillis;
        }
    }

    private static final class HeapPeakSampler implements AutoCloseable {
        private final MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        private final AtomicLong peak = new AtomicLong(memory.getHeapMemoryUsage().getUsed());
        private final AtomicReference<OutOfMemoryError> outOfMemory = new AtomicReference<>();
        private final Thread sampler;
        private volatile boolean running = true;

        private HeapPeakSampler() {
            sampler = new Thread(this::sampleUntilClosed, "backup-memory-benchmark-heap-sampler");
            sampler.setDaemon(true);
            sampler.start();
        }

        private void sampleUntilClosed() {
            try {
                while (running) {
                    peak.accumulateAndGet(memory.getHeapMemoryUsage().getUsed(), Math::max);
                    Thread.sleep(10L);
                }
                peak.accumulateAndGet(memory.getHeapMemoryUsage().getUsed(), Math::max);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (OutOfMemoryError e) {
                outOfMemory.compareAndSet(null, e);
            }
        }

        private long peakBytes() {
            return peak.get();
        }

        private void abortAfterOutOfMemory() {
            running = false;
            sampler.interrupt();
        }

        @Override
        public void close() throws InterruptedException {
            running = false;
            sampler.join();
            OutOfMemoryError samplerOutOfMemory = outOfMemory.get();
            if (samplerOutOfMemory != null) {
                throw samplerOutOfMemory;
            }
        }
    }
}
