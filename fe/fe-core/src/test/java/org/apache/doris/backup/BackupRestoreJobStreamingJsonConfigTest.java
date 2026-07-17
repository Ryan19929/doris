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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.cloud.backup.CloudRestoreJob;
import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.nereids.trees.plans.commands.BackupCommand;
import org.apache.doris.nereids.trees.plans.commands.RestoreCommand;
import org.apache.doris.persist.EditLog;
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
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;

public class BackupRestoreJobStreamingJsonConfigTest {
    private boolean savedStreamingConfig;
    private boolean savedBackupCompressionConfig;
    private boolean savedRestoreCompressionConfig;

    @Before
    public void saveConfig() {
        savedStreamingConfig = Config.enable_backup_restore_job_streaming_json;
        savedBackupCompressionConfig = Config.backup_job_compressed_serialization;
        savedRestoreCompressionConfig = Config.restore_job_compressed_serialization;
        Config.enable_backup_restore_job_streaming_json = false;
        Config.backup_job_compressed_serialization = false;
        Config.restore_job_compressed_serialization = false;
    }

    @After
    public void restoreConfig() {
        Config.enable_backup_restore_job_streaming_json = savedStreamingConfig;
        Config.backup_job_compressed_serialization = savedBackupCompressionConfig;
        Config.restore_job_compressed_serialization = savedRestoreCompressionConfig;
    }

    @Test
    public void testRestoreJobCanonicalBytesAndAllStateModeReads() throws Exception {
        for (RestoreJob.RestoreJobState state : RestoreJob.RestoreJobState.values()) {
            int cellCount = isLargeMappingState(state) ? 1024 : 7;
            RestoreJob job = newRestoreJob(state.name(), cellCount);
            setField(job, "state", state);
            setField(job, "showState", state);
            assertCanonicalBytesAndAllModeReads(job);
        }
    }

    @Test
    public void testJournalEntityReadFieldsCompatibilityMatrix() throws Exception {
        RestoreJob job = newRestoreJob("journal", 128);
        setField(job, "state", RestoreJob.RestoreJobState.DOWNLOADING);
        setField(job, "showState", RestoreJob.RestoreJobState.DOWNLOADING);

        byte[] legacyJournal = writeJournal(job, false);
        byte[] streamingJournal = writeJournal(job, true);
        Assert.assertArrayEquals(legacyJournal, streamingJournal);
        for (byte[] journal : new byte[][] {legacyJournal, streamingJournal}) {
            assertLargeFields(job, readRestoreJournal(journal, false));
            assertLargeFields(job, readRestoreJournal(journal, true));
        }
    }

    @Test
    public void testEditLogLoadJournalCompatibilityMatrix() throws Exception {
        RestoreJob job = newRestoreJob("edit_log_replay", 128);
        byte[] pendingLegacyJournal = writeJournal(job, false);
        byte[] pendingStreamingJournal = writeJournal(job, true);
        setField(job, "state", RestoreJob.RestoreJobState.COMMIT);
        setField(job, "showState", RestoreJob.RestoreJobState.COMMIT);
        byte[] commitLegacyJournal = writeJournal(job, false);
        byte[] commitStreamingJournal = writeJournal(job, true);

        for (int writeMode = 0; writeMode < 2; writeMode++) {
            byte[] pendingJournal = writeMode == 0 ? pendingLegacyJournal : pendingStreamingJournal;
            byte[] commitJournal = writeMode == 0 ? commitLegacyJournal : commitStreamingJournal;
            for (boolean streamingRead : new boolean[] {false, true}) {
                Env env = Mockito.mock(Env.class);
                BackupHandler handler = new BackupHandler(env);
                Mockito.when(env.getBackupHandler()).thenReturn(handler);

                Config.enable_backup_restore_job_streaming_json = streamingRead;
                JournalEntity pendingEntity = new JournalEntity();
                pendingEntity.readFields(dataInput(pendingJournal));
                EditLog.loadJournal(env, 1L, pendingEntity);

                JournalEntity commitEntity = new JournalEntity();
                commitEntity.readFields(dataInput(commitJournal));
                EditLog.loadJournal(env, 2L, commitEntity);

                Map<Long, Deque<AbstractJob>> jobs = getField(handler, "dbIdToBackupOrRestoreJobs");
                Assert.assertEquals(1, jobs.size());
                Assert.assertEquals(1, jobs.get(job.getDbId()).size());
                AbstractJob replayedJob = jobs.get(job.getDbId()).getLast();
                Assert.assertTrue(replayedJob instanceof RestoreJob);
                RestoreJob replayedRestoreJob = (RestoreJob) replayedJob;
                Assert.assertSame(env, getField(replayedRestoreJob, "env"));
                assertLargeFields(job, replayedRestoreJob);
            }
        }
    }

    @Test
    public void testBackupHandlerImageCompatibilityMatrix() throws Exception {
        RestoreJob job = newRestoreJob("image_replay", 128);
        setField(job, "state", RestoreJob.RestoreJobState.DOWNLOADING);
        setField(job, "showState", RestoreJob.RestoreJobState.DOWNLOADING);

        byte[] legacyImage = writeBackupHandlerImage(job, false);
        byte[] streamingImage = writeBackupHandlerImage(job, true);
        Assert.assertArrayEquals(legacyImage, streamingImage);
        for (byte[] image : new byte[][] {legacyImage, streamingImage}) {
            for (boolean streamingRead : new boolean[] {false, true}) {
                Config.enable_backup_restore_job_streaming_json = streamingRead;
                BackupHandler handler = new BackupHandler();
                handler.readFields(dataInput(image));

                Map<Long, Deque<AbstractJob>> jobs = getField(handler, "dbIdToBackupOrRestoreJobs");
                Assert.assertEquals(1, jobs.size());
                Assert.assertEquals(1, jobs.get(job.getDbId()).size());
                AbstractJob replayedJob = jobs.get(job.getDbId()).getLast();
                Assert.assertTrue(replayedJob instanceof RestoreJob);
                assertLargeFields(job, (RestoreJob) replayedJob);
            }
        }
    }

    @Test
    public void testCompressedJobsCompatibilityMatrix() throws Exception {
        RestoreJob restoreJob = newRestoreJob("compressed_restore", 128);
        Config.restore_job_compressed_serialization = true;
        byte[] legacyRestore = writeJob(restoreJob, false);
        byte[] streamingRestore = writeJob(restoreJob, true);
        Assert.assertEquals(AbstractJob.COMPRESSED_JOB_ID, readFirstString(legacyRestore));
        Assert.assertArrayEquals(legacyRestore, streamingRestore);
        for (byte[] bytes : new byte[][] {legacyRestore, streamingRestore}) {
            for (boolean streamingRead : new boolean[] {false, true}) {
                Config.enable_backup_restore_job_streaming_json = streamingRead;
                assertLargeFields(restoreJob, (RestoreJob) AbstractJob.read(dataInput(bytes)));
                assertLargeFields(restoreJob, RestoreJob.read(dataInput(bytes)));
            }
        }

        BackupJob backupJob = new BackupJob("compressed_backup", 1L, "db", Lists.newArrayList(), 1000L,
                BackupCommand.BackupContent.ALL, null, 2L, 9L);
        Config.backup_job_compressed_serialization = true;
        byte[] legacyBackup = writeJob(backupJob, false);
        byte[] streamingBackup = writeJob(backupJob, true);
        Assert.assertEquals(AbstractJob.COMPRESSED_JOB_ID, readFirstString(legacyBackup));
        Assert.assertArrayEquals(legacyBackup, streamingBackup);
        for (byte[] bytes : new byte[][] {legacyBackup, streamingBackup}) {
            for (boolean streamingRead : new boolean[] {false, true}) {
                Config.enable_backup_restore_job_streaming_json = streamingRead;
                Assert.assertEquals(backupJob.getLabel(), AbstractJob.read(dataInput(bytes)).getLabel());
                Assert.assertEquals(backupJob.getLabel(), BackupJob.read(dataInput(bytes)).getLabel());
            }
        }

        CloudRestoreJob cloudJob = newCloudRestoreJob();
        byte[] legacyCloud = writeJob(cloudJob, false);
        byte[] streamingCloud = writeJob(cloudJob, true);
        Assert.assertEquals(AbstractJob.COMPRESSED_JOB_ID, readFirstString(legacyCloud));
        Assert.assertArrayEquals(legacyCloud, streamingCloud);
        for (byte[] bytes : new byte[][] {legacyCloud, streamingCloud}) {
            for (boolean streamingRead : new boolean[] {false, true}) {
                Config.enable_backup_restore_job_streaming_json = streamingRead;
                assertCloudRestoreJob(cloudJob, (CloudRestoreJob) AbstractJob.read(dataInput(bytes)));
                assertCloudRestoreJob(cloudJob, (CloudRestoreJob) RestoreJob.read(dataInput(bytes)));
            }
        }
    }

    @Test
    public void testBackupAndCloudRestoreSubtypeCompatibility() throws Exception {
        BackupJob backupJob = new BackupJob("backup", 1L, "db", Lists.newArrayList(), 1000L,
                BackupCommand.BackupContent.ALL, null, 2L, 9L);
        byte[] legacyBackup = writeJob(backupJob, false);
        byte[] streamingBackup = writeJob(backupJob, true);
        for (byte[] bytes : new byte[][] {legacyBackup, streamingBackup}) {
            for (boolean streamingRead : new boolean[] {false, true}) {
                Config.enable_backup_restore_job_streaming_json = streamingRead;
                AbstractJob readBackup = AbstractJob.read(dataInput(bytes));
                Assert.assertTrue(readBackup instanceof BackupJob);
                Assert.assertEquals(backupJob.getLabel(), readBackup.getLabel());
            }
        }
        Assert.assertArrayEquals(legacyBackup, streamingBackup);

        CloudRestoreJob cloudJob = newCloudRestoreJob();
        byte[] legacyCloud = writeJob(cloudJob, false);
        byte[] streamingCloud = writeJob(cloudJob, true);
        for (byte[] bytes : new byte[][] {legacyCloud, streamingCloud}) {
            for (boolean streamingRead : new boolean[] {false, true}) {
                Config.enable_backup_restore_job_streaming_json = streamingRead;
                assertCloudRestoreJob(cloudJob, (CloudRestoreJob) RestoreJob.read(dataInput(bytes)));
            }
        }
        Assert.assertArrayEquals(legacyCloud, streamingCloud);
    }

    @Test
    public void testTenByteNonMarkerPayloadIsReplayedAndDoesNotConsumeNextField() throws Exception {
        byte[] json = "\"12345678\"".getBytes(StandardCharsets.UTF_8);
        Assert.assertEquals(AbstractJob.COMPRESSED_JOB_ID.length(), json.length);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(bytes);
        output.writeInt(json.length);
        output.write(json);
        output.writeLong(987654321L);

        DataInputStream input = dataInput(bytes.toByteArray());
        Assert.assertEquals("12345678", AbstractJob.readStreamingJob(input, String.class));
        Assert.assertEquals(987654321L, input.readLong());
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

    private static void assertCanonicalBytesAndAllModeReads(RestoreJob job) throws Exception {
        String legacyJson = toJson(job, false);
        String streamingJson = toJson(job, true);
        Assert.assertEquals(legacyJson, streamingJson);

        byte[] legacyBytes = writeJob(job, false);
        byte[] streamingBytes = writeJob(job, true);
        Assert.assertArrayEquals(legacyBytes, streamingBytes);
        for (byte[] bytes : new byte[][] {legacyBytes, streamingBytes}) {
            for (boolean streamingRead : new boolean[] {false, true}) {
                Config.enable_backup_restore_job_streaming_json = streamingRead;
                assertLargeFields(job, (RestoreJob) AbstractJob.read(dataInput(bytes)));
            }
        }
    }

    private static boolean isLargeMappingState(RestoreJob.RestoreJobState state) {
        return state == RestoreJob.RestoreJobState.SNAPSHOTING
                || state == RestoreJob.RestoreJobState.DOWNLOAD
                || state == RestoreJob.RestoreJobState.DOWNLOADING
                || state == RestoreJob.RestoreJobState.COMMIT
                || state == RestoreJob.RestoreJobState.COMMITTING;
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

    private static CloudRestoreJob newCloudRestoreJob() throws Exception {
        CloudRestoreJob cloudJob = new CloudRestoreJob(AbstractJob.JobType.RESTORE);
        setField(cloudJob, "label", "cloud_restore");
        setField(cloudJob, "state", RestoreJob.RestoreJobState.SNAPSHOTING);
        setField(cloudJob, "showState", RestoreJob.RestoreJobState.SNAPSHOTING);
        setField(cloudJob, "storageVaultName", "vault_a");
        cloudJob.properties.put(RestoreCommand.PROP_STORAGE_VAULT_NAME, "vault_a");
        cloudJob.snapshotInfos.put(100L, 200L,
                new SnapshotInfo(1L, 2L, 3L, 4L, 100L, 200L, 5, "/cloud",
                        Lists.newArrayList("cloud.dat")));
        return cloudJob;
    }

    private static void assertLargeFields(RestoreJob expected, RestoreJob actual) throws Exception {
        Assert.assertEquals(expected.getState(), actual.getState());
        Assert.assertEquals(expected.showState, actual.showState);
        Assert.assertEquals(expected.isBeingSynced(), actual.isBeingSynced());
        assertSnapshotInfos(expected.snapshotInfos, actual.snapshotInfos);
        Table<Long, Long, Long> expectedVersionInfo = getField(expected, "restoredVersionInfo");
        Table<Long, Long, Long> actualVersionInfo = getField(actual, "restoredVersionInfo");
        Assert.assertEquals(expectedVersionInfo, actualVersionInfo);
    }

    private static void assertCloudRestoreJob(CloudRestoreJob expected, CloudRestoreJob actual) throws Exception {
        Assert.assertEquals(expected.getLabel(), actual.getLabel());
        String expectedStorageVaultName = getField(expected, "storageVaultName");
        String actualStorageVaultName = getField(actual, "storageVaultName");
        Assert.assertEquals(expectedStorageVaultName, actualStorageVaultName);
        assertLargeFields(expected, actual);
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

    private static byte[] writeBackupHandlerImage(AbstractJob job, boolean streaming) throws Exception {
        Config.enable_backup_restore_job_streaming_json = streaming;
        BackupHandler handler = new BackupHandler();
        Map<Long, Deque<AbstractJob>> jobs = getField(handler, "dbIdToBackupOrRestoreJobs");
        Deque<AbstractJob> dbJobs = new LinkedList<>();
        dbJobs.add(job);
        jobs.put(job.getDbId(), dbJobs);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        handler.write(new DataOutputStream(bytes));
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
