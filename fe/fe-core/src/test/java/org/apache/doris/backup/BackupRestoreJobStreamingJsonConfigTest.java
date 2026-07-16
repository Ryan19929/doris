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

import org.apache.doris.analysis.BackupStmt;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.cloud.backup.CloudRestoreJob;
import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.nereids.trees.plans.commands.RestoreCommand;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;

public class BackupRestoreJobStreamingJsonConfigTest {
    private boolean savedStreamingConfig;
    private boolean savedBackupCompressionConfig;
    private boolean savedRestoreCompressionConfig;

    @Before
    public void saveConfig() {
        savedStreamingConfig = Config.enable_backup_restore_job_streaming_json;
        savedBackupCompressionConfig = Config.backup_job_compressed_serialization;
        savedRestoreCompressionConfig = Config.restore_job_compressed_serialization;
    }

    @After
    public void restoreConfig() {
        Config.enable_backup_restore_job_streaming_json = savedStreamingConfig;
        Config.backup_job_compressed_serialization = savedBackupCompressionConfig;
        Config.restore_job_compressed_serialization = savedRestoreCompressionConfig;
    }

    @Test
    public void testRestoreJobCanonicalBytesAndCrossModeReads() throws Exception {
        RestoreJob pendingJob = newRestoreJob("pending", 7);
        assertCanonicalBytesAndCrossModeReads(pendingJob);

        RestoreJob snapshottingJob = newRestoreJob("snapshotting", 1024);
        setField(snapshottingJob, "state", RestoreJob.RestoreJobState.SNAPSHOTING);
        setField(snapshottingJob, "showState", RestoreJob.RestoreJobState.SNAPSHOTING);
        assertCanonicalBytesAndCrossModeReads(snapshottingJob);

        RestoreJob finishedJob = newRestoreJob("finished", 7);
        setField(finishedJob, "state", RestoreJob.RestoreJobState.FINISHED);
        setField(finishedJob, "showState", RestoreJob.RestoreJobState.FINISHED);
        assertCanonicalBytesAndCrossModeReads(finishedJob);
    }

    @Test
    public void testActualJournalReplayPathCrossMode() throws Exception {
        RestoreJob job = newRestoreJob("journal", 128);
        setField(job, "state", RestoreJob.RestoreJobState.DOWNLOADING);
        setField(job, "showState", RestoreJob.RestoreJobState.DOWNLOADING);

        byte[] legacyJournal = writeJournal(job, false);
        RestoreJob streamingRead = readRestoreJournal(legacyJournal, true);
        assertLargeFields(job, streamingRead);

        byte[] streamingJournal = writeJournal(job, true);
        RestoreJob legacyRead = readRestoreJournal(streamingJournal, false);
        assertLargeFields(job, legacyRead);
        Assert.assertArrayEquals(legacyJournal, streamingJournal);
    }

    @Test
    public void testCompressedJobsCrossMode() throws Exception {
        RestoreJob restoreJob = newRestoreJob("compressed_restore", 128);
        Config.restore_job_compressed_serialization = true;
        byte[] legacyRestore = writeJob(restoreJob, false);
        Assert.assertEquals(AbstractJob.COMPRESSED_JOB_ID, readFirstString(legacyRestore));
        Config.enable_backup_restore_job_streaming_json = true;
        assertLargeFields(restoreJob, (RestoreJob) AbstractJob.read(dataInput(legacyRestore)));

        byte[] streamingRestore = writeJob(restoreJob, true);
        Config.enable_backup_restore_job_streaming_json = false;
        assertLargeFields(restoreJob, RestoreJob.read(dataInput(streamingRestore)));

        BackupJob backupJob = new BackupJob("compressed_backup", 1L, "db", Lists.newArrayList(), 1000L,
                BackupStmt.BackupContent.ALL, null, 2L, 9L);
        Config.backup_job_compressed_serialization = true;
        byte[] legacyBackup = writeJob(backupJob, false);
        Assert.assertEquals(AbstractJob.COMPRESSED_JOB_ID, readFirstString(legacyBackup));
        Config.enable_backup_restore_job_streaming_json = true;
        Assert.assertEquals(backupJob.getLabel(), BackupJob.read(dataInput(legacyBackup)).getLabel());

        byte[] streamingBackup = writeJob(backupJob, true);
        Config.enable_backup_restore_job_streaming_json = false;
        Assert.assertEquals(backupJob.getLabel(), AbstractJob.read(dataInput(streamingBackup)).getLabel());
    }

    @Test
    public void testBackupAndCloudRestoreSubtypeCompatibility() throws Exception {
        BackupJob backupJob = new BackupJob("backup", 1L, "db", Lists.newArrayList(), 1000L,
                BackupStmt.BackupContent.ALL, null, 2L, 9L);
        byte[] legacyBackup = writeJob(backupJob, false);
        Config.enable_backup_restore_job_streaming_json = true;
        BackupJob streamingReadBackup = BackupJob.read(dataInput(legacyBackup));
        Assert.assertEquals(backupJob.getLabel(), streamingReadBackup.getLabel());

        byte[] streamingBackup = writeJob(backupJob, true);
        Config.enable_backup_restore_job_streaming_json = false;
        AbstractJob legacyReadBackup = AbstractJob.read(dataInput(streamingBackup));
        Assert.assertTrue(legacyReadBackup instanceof BackupJob);
        Assert.assertArrayEquals(legacyBackup, streamingBackup);

        CloudRestoreJob cloudJob = new CloudRestoreJob(AbstractJob.JobType.RESTORE);
        setField(cloudJob, "label", "cloud_restore");
        setField(cloudJob, "state", RestoreJob.RestoreJobState.SNAPSHOTING);
        setField(cloudJob, "showState", RestoreJob.RestoreJobState.SNAPSHOTING);
        setField(cloudJob, "storageVaultName", "vault_a");
        cloudJob.properties.put(RestoreCommand.PROP_STORAGE_VAULT_NAME, "vault_a");
        cloudJob.snapshotInfos.put(100L, 200L,
                new SnapshotInfo(1L, 2L, 3L, 4L, 100L, 200L, 5, "/cloud",
                        Lists.newArrayList("cloud.dat")));
        byte[] legacyCloud = writeJob(cloudJob, false);
        Config.enable_backup_restore_job_streaming_json = true;
        CloudRestoreJob streamingReadCloud = (CloudRestoreJob) AbstractJob.read(dataInput(legacyCloud));
        Assert.assertEquals("cloud_restore", streamingReadCloud.getLabel());
        Assert.assertEquals("vault_a", getField(streamingReadCloud, "storageVaultName"));
        assertSnapshotInfos(cloudJob.snapshotInfos, streamingReadCloud.snapshotInfos);

        byte[] streamingCloud = writeJob(cloudJob, true);
        Config.enable_backup_restore_job_streaming_json = false;
        CloudRestoreJob legacyReadCloud = (CloudRestoreJob) RestoreJob.read(dataInput(streamingCloud));
        Assert.assertEquals("vault_a", getField(legacyReadCloud, "storageVaultName"));
        assertSnapshotInfos(cloudJob.snapshotInfos, legacyReadCloud.snapshotInfos);
        Assert.assertArrayEquals(legacyCloud, streamingCloud);
    }

    @Test
    public void testDisabledModeUsesGlobalLegacyTableDelegate() {
        LegacyMarkerTableAdapter legacyAdapter = new LegacyMarkerTableAdapter();
        Gson gson = new GsonBuilder()
                .registerTypeHierarchyAdapter(Table.class, legacyAdapter)
                .create();
        AnnotatedTableHolder holder = new AnnotatedTableHolder();
        holder.table.put(1L, 2L, 3L);

        Config.enable_backup_restore_job_streaming_json = false;
        Assert.assertEquals("{\"table\":{\"legacy\":true}}", gson.toJson(holder));
        Assert.assertEquals(1, legacyAdapter.writeCount);
        gson.fromJson("{\"table\":{\"legacy\":true}}", AnnotatedTableHolder.class);
        Assert.assertEquals(1, legacyAdapter.readCount);

        Config.enable_backup_restore_job_streaming_json = true;
        String streamingJson = gson.toJson(holder);
        Assert.assertTrue(streamingJson.contains("\"clazz\":\"HashBasedTable\""));
        AnnotatedTableHolder streamingRead = gson.fromJson(streamingJson, AnnotatedTableHolder.class);
        Assert.assertEquals(Long.valueOf(3L), streamingRead.table.get(1L, 2L));
        Assert.assertEquals(1, legacyAdapter.writeCount);
        Assert.assertEquals(1, legacyAdapter.readCount);

        PlainTableHolder unrelatedHolder = new PlainTableHolder();
        unrelatedHolder.table.put(4L, 5L, 6L);
        Assert.assertEquals("{\"table\":{\"legacy\":true}}", gson.toJson(unrelatedHolder));
        Assert.assertEquals(2, legacyAdapter.writeCount);
    }

    private static void assertCanonicalBytesAndCrossModeReads(RestoreJob job) throws Exception {
        String legacyJson = toJson(job, false);
        String streamingJson = toJson(job, true);
        Assert.assertEquals(legacyJson, streamingJson);

        byte[] legacyBytes = writeJob(job, false);
        Config.enable_backup_restore_job_streaming_json = true;
        RestoreJob streamingRead = (RestoreJob) AbstractJob.read(dataInput(legacyBytes));
        assertLargeFields(job, streamingRead);

        byte[] streamingBytes = writeJob(job, true);
        Config.enable_backup_restore_job_streaming_json = false;
        RestoreJob legacyRead = RestoreJob.read(dataInput(streamingBytes));
        assertLargeFields(job, legacyRead);
        Assert.assertArrayEquals(legacyBytes, streamingBytes);
    }

    private static RestoreJob newRestoreJob(String label, int cellCount) throws Exception {
        BackupJobInfo jobInfo = new BackupJobInfo();
        jobInfo.isForceReplicationAllocation = false;
        RestoreJob job = new RestoreJob(label, "2026-07-16 12:00:00", 1L, "db", jobInfo, false,
                new ReplicaAllocation((short) 1), 1000L, 123, false, false, false, true,
                false, false, false, false, null, 2L);
        for (int i = 0; i < cellCount; i++) {
            long tabletId = 10_000L + i;
            long backendId = 20_000L + i % 4;
            job.snapshotInfos.put(tabletId, backendId,
                    new SnapshotInfo(1L, 2L, 3L, 4L, tabletId, backendId, 5,
                            "/snapshot/" + tabletId, Lists.newArrayList(tabletId + ".dat")));
        }
        Table<Long, Long, Long> restoredVersionInfo = getField(job, "restoredVersionInfo");
        for (int i = 0; i < Math.max(1, cellCount / 8); i++) {
            restoredVersionInfo.put(30_000L + i, 40_000L + i, 50_000L + i);
        }
        return job;
    }

    private static void assertLargeFields(RestoreJob expected, RestoreJob actual) throws Exception {
        Assert.assertEquals(expected.getState(), actual.getState());
        Assert.assertEquals(expected.isBeingSynced(), actual.isBeingSynced());
        assertSnapshotInfos(expected.snapshotInfos, actual.snapshotInfos);
        Assert.assertEquals(getField(expected, "restoredVersionInfo"), getField(actual, "restoredVersionInfo"));
    }

    private static void assertSnapshotInfos(Table<Long, Long, SnapshotInfo> expected,
            Table<Long, Long, SnapshotInfo> actual) {
        Assert.assertEquals(expected.size(), actual.size());
        for (Table.Cell<Long, Long, SnapshotInfo> cell : expected.cellSet()) {
            SnapshotInfo actualInfo = actual.get(cell.getRowKey(), cell.getColumnKey());
            Assert.assertNotNull(actualInfo);
            assertSnapshotInfo(cell.getValue(), actualInfo);
        }
    }

    private static void assertSnapshotInfo(SnapshotInfo expected, SnapshotInfo actual) {
        Assert.assertEquals(expected.getDbId(), actual.getDbId());
        Assert.assertEquals(expected.getTblId(), actual.getTblId());
        Assert.assertEquals(expected.getPartitionId(), actual.getPartitionId());
        Assert.assertEquals(expected.getIndexId(), actual.getIndexId());
        Assert.assertEquals(expected.getTabletId(), actual.getTabletId());
        Assert.assertEquals(expected.getBeId(), actual.getBeId());
        Assert.assertEquals(expected.getSchemaHash(), actual.getSchemaHash());
        Assert.assertEquals(expected.getPath(), actual.getPath());
        Assert.assertEquals(expected.getFiles(), actual.getFiles());
        Assert.assertEquals(expected.getStorageVaultId(), actual.getStorageVaultId());
    }

    private static String toJson(AbstractJob job, boolean streaming) {
        Config.enable_backup_restore_job_streaming_json = streaming;
        return GsonUtils.GSON.toJson(job);
    }

    private static byte[] writeJob(AbstractJob job, boolean streaming) throws IOException {
        Config.enable_backup_restore_job_streaming_json = streaming;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        job.write(new DataOutputStream(bytes));
        return bytes.toByteArray();
    }

    private static byte[] writeJournal(AbstractJob job, boolean streaming) throws IOException {
        Config.enable_backup_restore_job_streaming_json = streaming;
        JournalEntity entity = new JournalEntity();
        entity.setOpCode(OperationType.OP_RESTORE_JOB);
        entity.setData(job);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        entity.write(new DataOutputStream(bytes));
        return bytes.toByteArray();
    }

    private static RestoreJob readRestoreJournal(byte[] bytes, boolean streaming) throws IOException {
        Config.enable_backup_restore_job_streaming_json = streaming;
        JournalEntity entity = new JournalEntity();
        entity.readFields(dataInput(bytes));
        Assert.assertEquals(OperationType.OP_RESTORE_JOB, entity.getOpCode());
        Assert.assertTrue(entity.getData() instanceof RestoreJob);
        return (RestoreJob) entity.getData();
    }

    private static DataInputStream dataInput(byte[] bytes) {
        return new DataInputStream(new ByteArrayInputStream(bytes));
    }

    private static String readFirstString(byte[] bytes) throws IOException {
        return Text.readString(dataInput(bytes));
    }

    @SuppressWarnings("unchecked")
    private static <T> T getField(Object target, String name) throws Exception {
        Field field = findField(target.getClass(), name);
        field.setAccessible(true);
        return (T) field.get(target);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = findField(target.getClass(), name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Field findField(Class<?> type, String name) throws NoSuchFieldException {
        Class<?> current = type;
        while (current != null) {
            try {
                return current.getDeclaredField(name);
            } catch (NoSuchFieldException e) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }

    private static class AnnotatedTableHolder {
        @JsonAdapter(RestoreJobTableTypeAdapterFactory.class)
        private Table<Long, Long, Long> table = HashBasedTable.create();
    }

    private static class PlainTableHolder {
        private Table<Long, Long, Long> table = HashBasedTable.create();
    }

    private static class LegacyMarkerTableAdapter extends TypeAdapter<Table<?, ?, ?>> {
        private int writeCount;
        private int readCount;

        @Override
        public void write(JsonWriter out, Table<?, ?, ?> value) throws IOException {
            writeCount++;
            out.beginObject();
            out.name("legacy").value(true);
            out.endObject();
        }

        @Override
        public Table<?, ?, ?> read(JsonReader in) throws IOException {
            readCount++;
            in.skipValue();
            return HashBasedTable.create();
        }
    }
}
