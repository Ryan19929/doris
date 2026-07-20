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

package org.apache.doris.journal.bdbje;

import org.apache.doris.common.io.DataOutputBuffer;
import org.apache.doris.common.io.Writable;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.gson.GsonUtils;

import com.sleepycat.je.DatabaseEntry;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.ref.Reference;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manually invoked memory benchmark for the BDBJE journal size check.
 *
 * <p>The class deliberately does not end in {@code Test}, so ordinary Surefire runs do not discover it.
 * Run one mode in each forked JVM from {@code fe/}:</p>
 *
 * <pre>
 * mvn test -pl fe-common,fe-core -am \
 *   -Dtest=org.apache.doris.journal.bdbje.BDBJEJournalSizeMemoryBenchmark \
 *   -Ddoris.benchmark.mode=counting \
 *   -Ddoris.benchmark.payload_bytes=536870912 \
 *   -Ddoris.benchmark.expected_max_heap_bytes=2147483648 \
 *   -Dfe.ut.max.heap=2g -Dfe.ut.extra.jvm.args=-Xms2g \
 *   -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false \
 *   -Dcheckstyle.skip=true -Dmaven.build.cache.enabled=false -Dfe_ut_parallel=1
 * </pre>
 */
public class BDBJEJournalSizeMemoryBenchmark { // CHECKSTYLE IGNORE THIS LINE: BDBJE should use uppercase
    private static final String RESULT_PREFIX = "JOURNAL_SIZE_MEMORY_BENCHMARK_RESULT=";
    private static final String MODE_PROPERTY = "doris.benchmark.mode";
    private static final String PAYLOAD_BYTES_PROPERTY = "doris.benchmark.payload_bytes";
    private static final String EXPECTED_MAX_HEAP_PROPERTY = "doris.benchmark.expected_max_heap_bytes";
    private static final long DEFAULT_PAYLOAD_BYTES = 512L << 20;
    private static final int WRITE_CHUNK_BYTES = 1 << 20;
    private static final int OLD_BUFFER_INITIAL_BYTES = 128;
    private static final short OP_CODE = OperationType.OP_START_ROLLUP;

    @Test
    public void runSingleMode() throws Throwable {
        Mode mode = Mode.parse(requiredProperty(MODE_PROPERTY));
        long payloadBytes = positiveLongProperty(PAYLOAD_BYTES_PROPERTY, DEFAULT_PAYLOAD_BYTES);
        long expectedMaxHeap = nonNegativeLongProperty(EXPECTED_MAX_HEAP_PROPERTY, 0L);
        long maxHeap = Runtime.getRuntime().maxMemory();
        verifyExpectedMaxHeap(expectedMaxHeap, maxHeap);

        byte[] chunk = new byte[WRITE_CHUNK_BYTES];
        Writable writable = output -> writePayload(output, chunk, payloadBytes);
        warmUp(mode);

        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("mode", mode.propertyValue);
        metrics.put("payload_bytes", payloadBytes);
        metrics.put("max_heap_bytes", maxHeap);

        try {
            forceFullGc();
            long baselineHeap = usedHeap();
            GcSnapshot gcBefore = gcSnapshot();
            long start = System.nanoTime();
            SizeCheckResult result;
            long peakHeap;
            try (HeapPeakSampler sampler = new HeapPeakSampler()) {
                result = runSizeCheck(mode, writable);
                peakHeap = sampler.closeAndGetPeak();
            }
            long elapsedMillis = elapsedMillis(start);
            GcSnapshot gcAfter = gcSnapshot();

            long expectedSize = Math.addExact(payloadBytes, Short.BYTES);
            Assert.assertEquals(expectedSize, result.serializedSize);
            forceFullGc();
            long retainedAfterGc = usedHeap();

            metrics.put("baseline_heap_bytes", baselineHeap);
            metrics.put("stage_elapsed_ms", elapsedMillis);
            metrics.put("peak_heap_bytes", peakHeap);
            metrics.put("peak_heap_delta_bytes", Math.max(0L, peakHeap - baselineHeap));
            metrics.put("gc_collection_count_delta", gcAfter.collectionCount - gcBefore.collectionCount);
            metrics.put("gc_collection_time_ms_delta",
                    gcAfter.collectionTimeMillis - gcBefore.collectionTimeMillis);
            metrics.put("retained_after_gc_bytes", retainedAfterGc);
            metrics.put("retained_delta_bytes", Math.max(0L, retainedAfterGc - baselineHeap));
            metrics.put("serialized_size_bytes", result.serializedSize);
            metrics.put("retained_buffer_capacity_bytes", result.retainedBufferCapacity);
            metrics.put("status", "ok");
            System.out.println(RESULT_PREFIX + GsonUtils.GSON.toJson(metrics));
            Reference.reachabilityFence(result.retainedResult);
            Reference.reachabilityFence(result);
            Reference.reachabilityFence(writable);
            Reference.reachabilityFence(chunk);
        } catch (OutOfMemoryError error) {
            metrics.put("status", "oom");
            System.out.println(RESULT_PREFIX + GsonUtils.GSON.toJson(metrics));
            throw error;
        }
    }

    private static SizeCheckResult runSizeCheck(Mode mode, Writable writable) throws IOException {
        if (mode == Mode.BUFFERED) {
            JournalEntity entity = createJournalEntity(writable);
            DataOutputBuffer buffer = new DataOutputBuffer(OLD_BUFFER_INITIAL_BYTES);
            entity.write(buffer);
            DatabaseEntry databaseEntry = new DatabaseEntry(buffer.getData());
            return new SizeCheckResult(buffer.getLength(), databaseEntry.getSize(), databaseEntry);
        }
        long count = BDBJEJournal.countJournalSize(OP_CODE, writable);
        return new SizeCheckResult(count, 0L, Long.valueOf(count));
    }

    private static void warmUp(Mode mode) throws IOException {
        runSizeCheck(mode, output -> output.writeByte(1));
    }

    private static JournalEntity createJournalEntity(Writable writable) {
        JournalEntity entity = new JournalEntity();
        entity.setOpCode(OP_CODE);
        entity.setData(writable);
        return entity;
    }

    private static void writePayload(java.io.DataOutput output, byte[] chunk, long payloadBytes)
            throws IOException {
        long remaining = payloadBytes;
        while (remaining > 0) {
            int writeBytes = (int) Math.min(remaining, chunk.length);
            output.write(chunk, 0, writeBytes);
            remaining -= writeBytes;
        }
    }

    private static String requiredProperty(String name) {
        String value = System.getProperty(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("missing required system property: " + name);
        }
        return value;
    }

    private static long positiveLongProperty(String name, long defaultValue) {
        String value = System.getProperty(name);
        long parsed = value == null ? defaultValue : Long.parseLong(value);
        if (parsed <= 0) {
            throw new IllegalArgumentException(name + " must be positive: " + parsed);
        }
        return parsed;
    }

    private static long nonNegativeLongProperty(String name, long defaultValue) {
        String value = System.getProperty(name);
        long parsed = value == null ? defaultValue : Long.parseLong(value);
        if (parsed < 0) {
            throw new IllegalArgumentException(name + " must not be negative: " + parsed);
        }
        return parsed;
    }

    private static void verifyExpectedMaxHeap(long expectedMaxHeap, long actualMaxHeap) {
        if (expectedMaxHeap != 0L && expectedMaxHeap != actualMaxHeap) {
            throw new IllegalStateException("expected max heap " + expectedMaxHeap + " but was " + actualMaxHeap);
        }
    }

    private static long elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }

    private static long usedHeap() {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }

    private static void forceFullGc() throws InterruptedException {
        for (int attempt = 0; attempt < 3; attempt++) {
            System.gc();
            System.runFinalization();
            Thread.sleep(100L);
        }
    }

    private static GcSnapshot gcSnapshot() {
        long collectionCount = 0L;
        long collectionTimeMillis = 0L;
        for (GarbageCollectorMXBean collector : ManagementFactory.getGarbageCollectorMXBeans()) {
            collectionCount += Math.max(0L, collector.getCollectionCount());
            collectionTimeMillis += Math.max(0L, collector.getCollectionTime());
        }
        return new GcSnapshot(collectionCount, collectionTimeMillis);
    }

    private enum Mode {
        BUFFERED("buffered"),
        COUNTING("counting");

        private final String propertyValue;

        Mode(String propertyValue) {
            this.propertyValue = propertyValue;
        }

        private static Mode parse(String value) {
            String normalized = value.toLowerCase(Locale.ROOT);
            for (Mode mode : values()) {
                if (mode.propertyValue.equals(normalized)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("unknown benchmark mode '" + value
                    + "'; expected buffered or counting");
        }
    }

    private static final class SizeCheckResult {
        private final long serializedSize;
        private final long retainedBufferCapacity;
        private final Object retainedResult;

        private SizeCheckResult(long serializedSize, long retainedBufferCapacity, Object retainedResult) {
            this.serializedSize = serializedSize;
            this.retainedBufferCapacity = retainedBufferCapacity;
            this.retainedResult = retainedResult;
        }
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
            sampler = new Thread(this::sampleUntilClosed, "journal-size-memory-benchmark-heap-sampler");
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
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } catch (OutOfMemoryError error) {
                outOfMemory.compareAndSet(null, error);
            }
        }

        private long closeAndGetPeak() throws InterruptedException {
            close();
            OutOfMemoryError samplerOutOfMemory = outOfMemory.get();
            if (samplerOutOfMemory != null) {
                throw samplerOutOfMemory;
            }
            return peak.get();
        }

        @Override
        public void close() throws InterruptedException {
            running = false;
            sampler.join();
        }
    }
}
