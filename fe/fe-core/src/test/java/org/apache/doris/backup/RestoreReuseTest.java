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

import org.apache.doris.backup.BackupJobInfo.BackupIndexInfo;
import org.apache.doris.backup.BackupJobInfo.BackupOlapTableInfo;
import org.apache.doris.backup.BackupJobInfo.BackupPartitionInfo;
import org.apache.doris.backup.BackupJobInfo.BackupTabletInfo;
import org.apache.doris.backup.RestoreJob.RestoreJobState;
import org.apache.doris.backup.RestoreReuseJudge.CheckLevel;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.RestoreLineage;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.DirMoveTask;
import org.apache.doris.task.RestoreDigestTask;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TLogicalDigest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Partition level reuse of restore: the candidate conditions, the check levels, the VERIFYING state, the partitions
 * that keep their local data, persistence and the recheck before commit.
 */
public class RestoreReuseTest {
    private static final long V1 = 10;
    private static final long BACKUP_TIME = 1700000000000L;
    private static final long RESTORE_TIME = 1800000000000L;
    private static final long COMMIT_SEQ = 500;
    private static final String ROOT_P1 = "aa11";
    private static final String ROOT_P2 = "bb22";
    private static final String SIG = "sig-1";

    private final Env env = Mockito.mock(Env.class);
    private final InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
    private final EditLog editLog = Mockito.mock(EditLog.class);
    private final SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
    private MockedStatic<Env> mockedEnvStatic;
    private MockedStatic<AgentTaskExecutor> mockedExecutor;
    // the tasks submitted to the backends
    private final List<AgentTask> submitted = Lists.newArrayList();

    private Database db;
    private OlapTable tbl2;

    private boolean origEnable;
    private String origLevel;
    private double origRatio;
    private long origMinBytes;
    private int origTimeout;

    @BeforeEach
    public void setUp() throws Exception {
        origEnable = Config.enable_restore_partition_reuse;
        origLevel = Config.restore_reuse_default_check_level;
        origRatio = Config.restore_reuse_sample_ratio;
        origMinBytes = Config.restore_reuse_min_partition_bytes;
        origTimeout = Config.restore_digest_timeout_s;
        Config.enable_restore_partition_reuse = true;
        Config.restore_reuse_default_check_level = "full";
        Config.restore_reuse_min_partition_bytes = 100;

        db = CatalogMocker.mockDb();
        tbl2 = (OlapTable) db.getTableNullable(CatalogMocker.TEST_TBL2_ID);
        Deencapsulation.setField(tbl2, "keysType", KeysType.DUP_KEYS);

        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnvJournalVersion).thenReturn(FeConstants.meta_version);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(catalog.getDbNullable(Mockito.anyLong())).thenReturn(db);
        Mockito.when(catalog.getDbOrMetaException(Mockito.anyLong())).thenReturn(db);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
        AtomicLong nextId = new AtomicLong(90000);
        Mockito.when(env.getNextId()).thenAnswer(inv -> nextId.getAndIncrement());
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.isAlive()).thenReturn(true);
        Mockito.when(systemInfo.getBackend(Mockito.anyLong())).thenReturn(backend);
        Mockito.when(systemInfo.checkExceedDiskCapacityLimit(Mockito.any(), Mockito.anyBoolean()))
                .thenReturn(org.apache.doris.common.Status.OK);

        mockedExecutor = Mockito.mockStatic(AgentTaskExecutor.class);
        mockedExecutor.when(() -> AgentTaskExecutor.submit(Mockito.any(AgentBatchTask.class)))
                .thenAnswer(inv -> {
                    submitted.addAll(((AgentBatchTask) inv.getArgument(0)).getAllTasks());
                    return null;
                });

        // CatalogMocker does not set the range of p2, the restore job compares it with the backup meta.
        PartitionInfo partitionInfo = tbl2.getPartitionInfo();
        if (partitionInfo.getItem(CatalogMocker.TEST_PARTITION2_ID) == null) {
            partitionInfo.setItem(CatalogMocker.TEST_PARTITION2_ID, false,
                    partitionInfo.getItem(CatalogMocker.TEST_PARTITION1_ID));
        }
        // p1 and p2 were restored from the backup at V1, are not written since then, and have a size.
        BackupJobInfo infoV1 = selfBackupJobInfo();
        for (Partition part : Lists.newArrayList(p1(), p2())) {
            part.updateVersionForRestore(V1);
            part.setRestoreLineage(lineage(infoV1, part));
            for (MaterializedIndex index : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                for (Tablet tablet : index.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        replica.updateVersionForRestore(V1);
                        replica.setDataSize(1000);
                    }
                }
            }
        }
    }

    @AfterEach
    public void tearDown() {
        Config.enable_restore_partition_reuse = origEnable;
        Config.restore_reuse_default_check_level = origLevel;
        Config.restore_reuse_sample_ratio = origRatio;
        Config.restore_reuse_min_partition_bytes = origMinBytes;
        Config.restore_digest_timeout_s = origTimeout;
        if (mockedExecutor != null) {
            mockedExecutor.close();
        }
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    private Partition p1() {
        return tbl2.getPartition(CatalogMocker.TEST_PARTITION1_NAME);
    }

    private Partition p2() {
        return tbl2.getPartition(CatalogMocker.TEST_PARTITION2_NAME);
    }

    // the number of replicas of all tablets of all visible indexes of the partition (one task for each)
    private static int replicasOf(Partition part) {
        int num = 0;
        for (MaterializedIndex index : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
            for (Tablet tablet : index.getTablets()) {
                num += tablet.getReplicas().size();
            }
        }
        return num;
    }

    private static long tabletOf(Partition part) {
        return part.getBaseIndex().getTablets().get(0).getId();
    }

    private static String rootOf(Partition part) {
        return part.getName().equals(CatalogMocker.TEST_PARTITION1_NAME) ? ROOT_P1 : ROOT_P2;
    }

    // A backup of test_tbl2 taken in this cluster at V1 with the logical digest of every tablet.
    private BackupJobInfo selfBackupJobInfo() {
        BackupJobInfo info = new BackupJobInfo();
        info.name = "snapshot_v1";
        info.backupTime = BACKUP_TIME;
        info.dbId = db.getId();
        info.dbName = db.getFullName();
        info.success = true;
        info.tableCommitSeqMap = Maps.newHashMap();
        info.tableCommitSeqMap.put(tbl2.getId(), COMMIT_SEQ);
        BackupOlapTableInfo tblInfo = new BackupOlapTableInfo();
        tblInfo.id = tbl2.getId();
        for (Partition part : tbl2.getPartitions()) {
            BackupPartitionInfo partInfo = new BackupPartitionInfo();
            partInfo.id = part.getId();
            partInfo.version = V1;
            for (MaterializedIndex index : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                BackupIndexInfo idxInfo = new BackupIndexInfo();
                idxInfo.id = index.getId();
                idxInfo.schemaHash = tbl2.getSchemaHashByIndexId(index.getId());
                idxInfo.logicalDigests = Maps.newHashMap();
                partInfo.indexes.put(tbl2.getIndexNameById(index.getId()), idxInfo);
                for (Tablet tablet : index.getTablets()) {
                    idxInfo.sortedTabletInfoList.add(new BackupTabletInfo(tablet.getId(), Lists.newArrayList()));
                    idxInfo.logicalDigests.put(tablet.getId(), LogicalDigestInfo.of(1, SIG, rootOf(part)));
                }
            }
            tblInfo.partitions.put(part.getName(), partInfo);
        }
        info.backupOlapTableObjects.put(tbl2.getName(), tblInfo);
        return info;
    }

    private static RestoreLineage lineage(BackupJobInfo info, Partition part) {
        BackupOlapTableInfo tblInfo = info.getOlapTableInfo(CatalogMocker.TEST_TBL2_NAME);
        return RestoreJob.buildRestoreLineage(info, tblInfo, tblInfo.getPartInfo(part.getName()), RESTORE_TIME);
    }

    private BackupMeta backupMeta() {
        OlapTable remoteTbl = tbl2.selectiveCopy(null, IndexExtState.VISIBLE, true);
        return new BackupMeta(Lists.newArrayList(remoteTbl), Lists.<Resource>newArrayList());
    }

    private RestoreJob newJob(BackupJobInfo info, String level) {
        RestoreJob job = new RestoreJob("restore_label", "2024-01-01 00:00:00", db.getId(), db.getFullName(), info,
                false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false, false, false,
                false, false, env, Repository.KEEP_ON_LOCAL_REPO_ID, backupMeta());
        job.setReuseCheckLevel(level);
        return job;
    }

    // A non-atomic restore of the backup onto the existing test_tbl2, after checkAndPrepareMeta.
    private RestoreJob prepareJob(String level) {
        RestoreJob job = newJob(selfBackupJobInfo(), level);
        Deencapsulation.invoke(job, "checkAndPrepareMeta");
        Assertions.assertTrue(job.getStatus().ok(), job.getStatus().toString());
        Assertions.assertEquals(RestoreJobState.CREATING, job.getState());
        return job;
    }

    private static com.google.common.collect.Table<Long, Long, Long> versionInfo(RestoreJob job) {
        return Deencapsulation.getField(job, "restoredVersionInfo");
    }

    private static RestoreFileMapping fileMapping(RestoreJob job) {
        return Deencapsulation.getField(job, "fileMapping");
    }

    private static com.google.common.collect.Table<Long, Long, SnapshotInfo> snapshotInfos(RestoreJob job) {
        return Deencapsulation.getField(job, "snapshotInfos");
    }

    private static List<RestoreDigestTask> digestTasks(List<AgentTask> tasks) {
        return tasks.stream().filter(t -> t instanceof RestoreDigestTask).map(t -> (RestoreDigestTask) t)
                .collect(Collectors.toList());
    }

    private static TFinishTaskRequest okReport(String root, String sig, int algo) {
        TFinishTaskRequest request = new TFinishTaskRequest();
        request.setTaskStatus(new TStatus(TStatusCode.OK));
        TLogicalDigest digest = new TLogicalDigest();
        digest.setStatusCode("OK");
        digest.setAlgoVersion(algo);
        digest.setSchemaSig(sig);
        digest.setRoot(root);
        request.setLogicalDigest(digest);
        return request;
    }

    private static TFinishTaskRequest failedReport() {
        TFinishTaskRequest request = new TFinishTaskRequest();
        TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
        status.setErrorMsgs(Lists.newArrayList("boom"));
        request.setTaskStatus(status);
        return request;
    }

    // Report every digest task: the digest of a replica is the digest of the backup, unless the partition (and
    // optionally the replica of the backend) is in the map of the faulty ones.
    private void reportAll(RestoreJob job, List<RestoreDigestTask> tasks, Map<Long, TFinishTaskRequest> faulty) {
        for (RestoreDigestTask task : tasks) {
            TFinishTaskRequest report = faulty.get(task.getSignature());
            if (report == null) {
                String root = task.getPartitionId() == p1().getId() ? ROOT_P1 : ROOT_P2;
                report = okReport(root, SIG, 1);
            }
            Assertions.assertTrue(job.finishRestoreDigestTask(task, report));
        }
    }

    private List<RestoreDigestTask> newDigestTasks() {
        List<RestoreDigestTask> tasks = Lists.newArrayList();
        for (AgentTask task : submitted) {
            if (task instanceof RestoreDigestTask) {
                tasks.add((RestoreDigestTask) task);
            }
        }
        submitted.clear();
        return tasks;
    }

    private void toVerifying(RestoreJob job) {
        Deencapsulation.invoke(job, "allReplicasCreated");
        Assertions.assertEquals(RestoreJobState.VERIFYING, job.getState());
    }

    private void waitDigests(RestoreJob job) {
        Deencapsulation.invoke(job, "waitingAllDigestsFinished");
    }

    private static RestoreJob writeAndRead(RestoreJob restoreJob) throws Exception {
        Path path = Files.createTempFile("restoreJob", "tmp");
        try {
            try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path))) {
                restoreJob.write(out);
            }
            try (DataInputStream in = new DataInputStream(Files.newInputStream(path))) {
                return RestoreJob.read(in);
            }
        } finally {
            Files.delete(path);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // the chain of candidate conditions
    // ---------------------------------------------------------------------------------------------

    private RestoreReuseJudge.Input input(CheckLevel level) {
        BackupJobInfo info = selfBackupJobInfo();
        RestoreReuseJudge.Input in = new RestoreReuseJudge.Input();
        in.atomicRestore = false;
        in.allowLoad = false;
        in.cloudMode = false;
        in.minPartitionBytes = 100;
        in.level = level;
        in.jobInfo = info;
        in.backupTable = info.getOlapTableInfo(CatalogMocker.TEST_TBL2_NAME);
        in.backupPartition = in.backupTable.getPartInfo(CatalogMocker.TEST_PARTITION1_NAME);
        in.srcCommitSeq = COMMIT_SEQ;
        in.localTable = tbl2;
        in.localPartition = p1();
        tbl2.setState(OlapTableState.RESTORE);
        return in;
    }

    @Test
    public void testCandidateConditions() {
        Assertions.assertNull(RestoreReuseJudge.firstReject(input(CheckLevel.FULL)));

        RestoreReuseJudge.Input in = input(CheckLevel.FULL);
        in.atomicRestore = true;
        Assertions.assertEquals("ATOMIC_RESTORE", RestoreReuseJudge.firstReject(in));

        in = input(CheckLevel.FULL);
        in.allowLoad = true;
        Assertions.assertEquals("ALLOW_LOAD", RestoreReuseJudge.firstReject(in));

        in = input(CheckLevel.FULL);
        tbl2.setState(OlapTableState.NORMAL);
        Assertions.assertEquals("TABLE_STATE_NORMAL", RestoreReuseJudge.firstReject(in));
        tbl2.setState(OlapTableState.RESTORE_WITH_LOAD);
        Assertions.assertEquals("TABLE_STATE_RESTORE_WITH_LOAD", RestoreReuseJudge.firstReject(in));

        // L0: no lineage, or a lineage of another source.
        in = input(CheckLevel.FULL);
        p1().setRestoreLineage(null);
        Assertions.assertEquals("L0_NO_LINEAGE", RestoreReuseJudge.firstReject(in));
        p1().setRestoreLineage(new RestoreLineage(7, 7, 7, V1, COMMIT_SEQ, 1, 1));
        Assertions.assertEquals("L0_LINEAGE_MISMATCH", RestoreReuseJudge.firstReject(in));
        // the local partition was written
        p1().setRestoreLineage(lineage(in.jobInfo, p1()));
        p1().updateVersionForRestore(V1 + 1);
        Assertions.assertEquals("L0_LOCAL_VERSION_CHANGED", RestoreReuseJudge.firstReject(in));
        p1().updateVersionForRestore(V1);

        // unsupported cases
        in = input(CheckLevel.FULL);
        in.cloudMode = true;
        Assertions.assertEquals("CLOUD_MODE", RestoreReuseJudge.firstReject(in));
        Deencapsulation.setField(tbl2, "keysType", KeysType.AGG_KEYS);
        in = input(CheckLevel.FULL);
        Assertions.assertEquals("AGGREGATE_TABLE", RestoreReuseJudge.firstReject(in));
        Deencapsulation.setField(tbl2, "keysType", KeysType.DUP_KEYS);
        Tablet tablet = p1().getBaseIndex().getTablets().get(0);
        Replica replica = tablet.getReplicas().get(0);
        tablet.setCooldownConf(replica.getId(), 1);
        replica.setRemoteDataSize(100);
        Assertions.assertEquals("REMOTE_STORAGE", RestoreReuseJudge.firstReject(input(CheckLevel.FULL)));
        replica.setRemoteDataSize(0);

        // a tablet of the backup has no digest, except for the check level off
        in = input(CheckLevel.FULL);
        BackupIndexInfo idx = in.backupPartition.indexes.values().iterator().next();
        idx.logicalDigests.put(tabletOf(p1()), LogicalDigestInfo.none(LogicalDigestInfo.REASON_NOT_SUPPORTED));
        Assertions.assertEquals("NO_DIGEST", RestoreReuseJudge.firstReject(in));
        in.level = CheckLevel.OFF;
        Assertions.assertNull(RestoreReuseJudge.firstReject(in));
        in.level = CheckLevel.SAMPLE;
        idx.logicalDigests = null;
        Assertions.assertEquals("NO_DIGEST", RestoreReuseJudge.firstReject(in));

        // too small
        in = input(CheckLevel.FULL);
        long size = in.getSingleReplicaBytes();
        Assertions.assertTrue(size > 0);
        in.minPartitionBytes = size + 1;
        Assertions.assertEquals("TOO_SMALL", RestoreReuseJudge.firstReject(in));
        in.minPartitionBytes = size;
        Assertions.assertNull(RestoreReuseJudge.firstReject(in));
    }

    @Test
    public void testReplicaMustBeHealthyAtTheBackupVersion() {
        Replica replica = p1().getBaseIndex().getTablets().get(0).getReplicas().get(0);
        RestoreReuseJudge.Input in = input(CheckLevel.FULL);
        replica.updateVersionForRestore(V1 - 1);
        Assertions.assertEquals("REPLICA_UNHEALTHY", RestoreReuseJudge.firstReject(in));
        replica.updateVersionForRestore(V1);
        replica.setState(ReplicaState.DECOMMISSION);
        Assertions.assertEquals("REPLICA_UNHEALTHY", RestoreReuseJudge.firstReject(in));
        replica.setState(ReplicaState.NORMAL);
        replica.updateLastFailedVersion(V1 + 1);
        Assertions.assertEquals("REPLICA_UNHEALTHY", RestoreReuseJudge.firstReject(in));
    }

    @Test
    public void testConditionChainIsExtensible() {
        // A new way to pass L0 (b, c of the design for the later phase) only needs to be added to the list.
        Assertions.assertEquals(1, RestoreReuseJudge.L0_CHECKS.size());
        Assertions.assertEquals(6, RestoreReuseJudge.CONDITIONS.size());
    }

    // ---------------------------------------------------------------------------------------------
    // levels, sampling, comparison
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testCheckLevel() {
        Assertions.assertEquals(CheckLevel.OFF, CheckLevel.parse("OFF", "sample"));
        Assertions.assertEquals(CheckLevel.FULL, CheckLevel.parse(" full ", "sample"));
        Assertions.assertEquals(CheckLevel.DISABLE, CheckLevel.parse("disable", "sample"));
        // absent or invalid: the default, and sample if the default is invalid too
        Assertions.assertEquals(CheckLevel.FULL, CheckLevel.parse(null, "full"));
        Assertions.assertEquals(CheckLevel.FULL, CheckLevel.parse("bad", "full"));
        Assertions.assertEquals(CheckLevel.SAMPLE, CheckLevel.parse(null, "bad"));
        Assertions.assertEquals(CheckLevel.SAMPLE, CheckLevel.parse(null, null));

        // unique tables do not allow off
        Assertions.assertEquals(CheckLevel.SAMPLE, RestoreReuseJudge.levelOfTable(CheckLevel.OFF, KeysType.UNIQUE_KEYS));
        Assertions.assertEquals(CheckLevel.OFF, RestoreReuseJudge.levelOfTable(CheckLevel.OFF, KeysType.DUP_KEYS));
        Assertions.assertEquals(CheckLevel.FULL, RestoreReuseJudge.levelOfTable(CheckLevel.FULL, KeysType.UNIQUE_KEYS));
    }

    @Test
    public void testPickSample() {
        List<Integer> pool = Lists.newArrayList();
        for (int i = 0; i < 20; i++) {
            pool.add(i);
        }
        Random random = new Random(1);
        Assertions.assertEquals(2, RestoreReuseJudge.pickSample(pool, 0.1, random).size());
        // at least one
        Assertions.assertEquals(1, RestoreReuseJudge.pickSample(pool, 0.0, random).size());
        Assertions.assertEquals(1, RestoreReuseJudge.pickSample(Lists.newArrayList(5), 0.1, random).size());
        // at most all, no duplicates
        Assertions.assertEquals(20, Sets.newHashSet(RestoreReuseJudge.pickSample(pool, 5, random)).size());
        Assertions.assertTrue(RestoreReuseJudge.pickSample(Lists.<Integer>newArrayList(), 0.5, random).isEmpty());
    }

    @Test
    public void testCompareTablet() {
        LogicalDigestInfo expected = LogicalDigestInfo.of(1, SIG, "abcd");
        Assertions.assertNull(RestoreReuseJudge.compareTablet(expected, Lists.newArrayList(
                LogicalDigestInfo.of(1, SIG, "abcd"), LogicalDigestInfo.of(1, SIG, "ABCD"))));
        // one replica is different
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_ROOT_MISMATCH, RestoreReuseJudge.compareTablet(expected,
                Lists.newArrayList(LogicalDigestInfo.of(1, SIG, "abcd"), LogicalDigestInfo.of(1, SIG, "abce"),
                        LogicalDigestInfo.of(1, SIG, "abcd"))));
        // the schema signature or the algorithm version is different
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_SCHEMA_MISMATCH, RestoreReuseJudge.compareTablet(expected,
                Lists.newArrayList(LogicalDigestInfo.of(1, "other", "abcd"))));
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_SCHEMA_MISMATCH, RestoreReuseJudge.compareTablet(expected,
                Lists.newArrayList(LogicalDigestInfo.of(2, SIG, "abcd"))));
        // no digest of a replica, or of the backup
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_DIGEST_ERROR, RestoreReuseJudge.compareTablet(expected,
                Lists.newArrayList(LogicalDigestInfo.of(1, SIG, "abcd"),
                        LogicalDigestInfo.none(LogicalDigestInfo.REASON_ERROR))));
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_DIGEST_ERROR, RestoreReuseJudge.compareTablet(expected,
                Lists.newArrayList((LogicalDigestInfo) null)));
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_DIGEST_ERROR, RestoreReuseJudge.compareTablet(null,
                Lists.newArrayList(LogicalDigestInfo.of(1, SIG, "abcd"))));
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_DIGEST_ERROR,
                RestoreReuseJudge.compareTablet(expected, Lists.<LogicalDigestInfo>newArrayList()));
    }

    // ---------------------------------------------------------------------------------------------
    // selecting the candidates in the job
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testSwitchOffKeepsTheBehavior() {
        Config.enable_restore_partition_reuse = false;
        RestoreJob job = prepareJob(null);
        Assertions.assertNull(job.getReuseResult());
        Assertions.assertEquals(2, versionInfo(job).size());
        // the lineage of the overwritten partitions is invalidated at once, as before
        Assertions.assertNull(p1().getRestoreLineage());
        Assertions.assertNull(p2().getRestoreLineage());

        // allReplicasCreated goes to SNAPSHOTING directly, with snapshot tasks of all partitions.
        Deencapsulation.invoke(job, "allReplicasCreated");
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()),
                submitted.stream().filter(t -> t instanceof SnapshotTask).count());
        Assertions.assertTrue(digestTasks(submitted).isEmpty());
    }

    @Test
    public void testPropertyDisable() {
        RestoreJob job = prepareJob("disable");
        Assertions.assertNull(job.getReuseResult());
        Assertions.assertEquals(2, versionInfo(job).size());
        Assertions.assertNull(p1().getRestoreLineage());
        Deencapsulation.invoke(job, "allReplicasCreated");
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()),
                submitted.stream().filter(t -> t instanceof SnapshotTask).count());
    }

    @Test
    public void testNoCandidateSkipsVerifying() {
        // p1 was written after the restore, p2 is too small.
        p1().updateVersionForRestore(V1 + 1);
        Config.restore_reuse_min_partition_bytes = 3001;
        RestoreJob job = prepareJob(null);
        Assertions.assertEquals(0, job.getReuseResult().getDecisions().size());
        Assertions.assertEquals(1L, (long) job.getReuseResult().getRejected().get("L0_LOCAL_VERSION_CHANGED"));
        Assertions.assertEquals(1L, (long) job.getReuseResult().getRejected().get("TOO_SMALL"));
        Deencapsulation.invoke(job, "allReplicasCreated");
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assertions.assertTrue(digestTasks(submitted).isEmpty());
    }

    @Test
    public void testCandidatesKeepTheirLineageUntilDecided() {
        RestoreJob job = prepareJob(null);
        Assertions.assertNotNull(job.getReuseResult());
        Assertions.assertNotNull(p1().getRestoreLineage());
        Assertions.assertNotNull(p2().getRestoreLineage());
        Assertions.assertEquals(2, versionInfo(job).size());
    }

    @Test
    public void testAtomicRestoreAndAllowLoadAreNotCandidates() {
        RestoreJob job = new RestoreJob("l", "2024-01-01 00:00:00", db.getId(), db.getFullName(), selfBackupJobInfo(),
                true /* allow load */, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, env, Repository.KEEP_ON_LOCAL_REPO_ID, backupMeta());
        Deencapsulation.invoke(job, "checkAndPrepareMeta");
        Assertions.assertTrue(job.getStatus().ok(), job.getStatus().toString());
        Assertions.assertEquals(2L, (long) job.getReuseResult().getRejected().get("ALLOW_LOAD"));
        Assertions.assertTrue(job.getReuseResult().getDecisions().isEmpty());
    }

    // ---------------------------------------------------------------------------------------------
    // VERIFYING
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testAllKeptSkipsSnapshotDownloadAndMove() {
        RestoreJob job = prepareJob("full");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        // one task for each replica of the 2 partitions, at the local visible version
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()), tasks.size());
        for (RestoreDigestTask task : tasks) {
            Assertions.assertEquals(V1, task.getVersion());
            Assertions.assertEquals(job.getJobId(), task.getJobId());
        }
        Assertions.assertEquals(tasks.size(), tasks.stream().map(AgentTask::getSignature).distinct().count());
        Assertions.assertTrue(submitted.stream().noneMatch(t -> t instanceof SnapshotTask));

        // still waiting
        reportAll(job, tasks.subList(0, tasks.size() - 1), Maps.newHashMap());
        waitDigests(job);
        Assertions.assertEquals(RestoreJobState.VERIFYING, job.getState());

        reportAll(job, tasks.subList(tasks.size() - 1, tasks.size()), Maps.newHashMap());
        waitDigests(job);
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());

        // both are kept: nothing to snapshot, download or move, and they are not in restoredVersionInfo
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(2, result.getKeptPartitions());
        Assertions.assertEquals(0, result.getDownloadedPartitions());
        Assertions.assertEquals(RestoreReuseShadowStats.getSingleReplicaLocalDataSize(p1())
                + RestoreReuseShadowStats.getSingleReplicaLocalDataSize(p2()), result.getKeptBytesSingleReplica());
        for (RestoreReuseResult.Decision decision : result.getDecisions()) {
            Assertions.assertEquals(RestoreReuseResult.KEPT_DIGEST_VERIFIED, decision.reason);
        }
        Assertions.assertTrue(versionInfo(job).isEmpty());
        Assertions.assertTrue(fileMapping(job).getMapping().isEmpty());
        Assertions.assertTrue(submitted.isEmpty());
        // the lineage is not invalidated
        Assertions.assertNotNull(p1().getRestoreLineage());
        Assertions.assertNotNull(p2().getRestoreLineage());

        // no move task
        Deencapsulation.setField(job, "state", RestoreJobState.COMMIT);
        job.commit();
        Assertions.assertTrue(submitted.stream().noneMatch(t -> t instanceof DirMoveTask));
        Assertions.assertEquals(RestoreJobState.COMMITTING, job.getState());

        // commit: versions are untouched, the lineage is written again with the time of this restore
        Status st = job.allTabletCommitted(false);
        Assertions.assertTrue(st.ok(), st.toString());
        Assertions.assertEquals(V1, p1().getVisibleVersion());
        Assertions.assertEquals(V1, p2().getVisibleVersion());
        for (Partition part : Lists.newArrayList(p1(), p2())) {
            Assertions.assertEquals(lineage(selfBackupJobInfo(), part).getSrcVersion(),
                    part.getRestoreLineage().getSrcVersion());
            Assertions.assertEquals(job.getFinishedTime(), part.getRestoreLineage().getRestoreTime());
            Assertions.assertEquals(org.apache.doris.catalog.Partition.PartitionState.NORMAL, part.getState());
            for (Replica replica : part.getBaseIndex().getTablets().get(0).getReplicas()) {
                Assertions.assertEquals(V1, replica.getVersion());
            }
        }
        Assertions.assertEquals(OlapTableState.NORMAL, tbl2.getState());
        Assertions.assertEquals(2, job.getReuseResult().getKeptPartitions());
    }

    @Test
    public void testOneReplicaDiffersDownloadsOnlyThatPartition() {
        RestoreJob job = prepareJob("full");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        // one replica of p1 has a different root
        Map<Long, TFinishTaskRequest> faulty = Maps.newHashMap();
        RestoreDigestTask bad = tasks.stream().filter(t -> t.getPartitionId() == p1().getId()).skip(1).findFirst().get();
        faulty.put(bad.getSignature(), okReport("ffff", SIG, 1));
        reportAll(job, tasks, faulty);
        waitDigests(job);

        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(1, result.getKeptPartitions());
        Assertions.assertTrue(result.isKept(tbl2.getId(), p2().getId()));
        Assertions.assertFalse(result.isKept(tbl2.getId(), p1().getId()));
        for (RestoreReuseResult.Decision decision : result.getDecisions()) {
            if (decision.partitionId == p1().getId()) {
                Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_ROOT_MISMATCH, decision.reason);
            }
        }
        // p1 is restored as usual, p2 is not
        Assertions.assertTrue(versionInfo(job).contains(tbl2.getId(), p1().getId()));
        Assertions.assertFalse(versionInfo(job).contains(tbl2.getId(), p2().getId()));
        Assertions.assertTrue(fileMapping(job).getMapping().keySet().stream()
                .allMatch(chain -> chain.getPartId() == p1().getId()));
        Assertions.assertEquals(replicasOf(p1()), fileMapping(job).getMapping().size());
        // only p1 is snapshotted
        List<SnapshotTask> snapshotTasks = submitted.stream().filter(t -> t instanceof SnapshotTask)
                .map(t -> (SnapshotTask) t).collect(Collectors.toList());
        Assertions.assertEquals(replicasOf(p1()), snapshotTasks.size());
        Assertions.assertTrue(snapshotTasks.stream().allMatch(t -> t.getPartitionId() == p1().getId()));
        // the lineage of p1 is invalidated, p2 keeps it
        Assertions.assertNull(p1().getRestoreLineage());
        Assertions.assertNotNull(p2().getRestoreLineage());

        // download and move follow the snapshots, so only p1 has them
        for (SnapshotTask task : snapshotTasks) {
            TFinishTaskRequest report = new TFinishTaskRequest();
            report.setTaskStatus(new TStatus(TStatusCode.OK));
            report.setSnapshotPath("/path/snapshot");
            Assertions.assertTrue(job.finishTabletSnapshotTask(task, report));
        }
        submitted.clear();
        Deencapsulation.setField(job, "state", RestoreJobState.COMMIT);
        job.commit();
        List<DirMoveTask> moves = submitted.stream().filter(t -> t instanceof DirMoveTask)
                .map(t -> (DirMoveTask) t).collect(Collectors.toList());
        Assertions.assertEquals(replicasOf(p1()), moves.size());
        Assertions.assertTrue(moves.stream().allMatch(t -> t.getPartitionId() == p1().getId()));
        Assertions.assertTrue(snapshotInfos(job).values().stream().allMatch(i -> i.getPartitionId() == p1().getId()));

        // commit: p1 gets the backup version, p2 is kept as it is
        Status st = job.allTabletCommitted(false);
        Assertions.assertTrue(st.ok(), st.toString());
        Assertions.assertEquals(V1, p1().getVisibleVersion());
        Assertions.assertEquals(V1, p2().getVisibleVersion());
        Assertions.assertNotNull(p1().getRestoreLineage());
        Assertions.assertNotNull(p2().getRestoreLineage());
    }

    @Test
    public void testSchemaSigDifferenceDownloads() {
        RestoreJob job = prepareJob("full");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        Map<Long, TFinishTaskRequest> faulty = Maps.newHashMap();
        for (RestoreDigestTask task : tasks) {
            if (task.getPartitionId() == p2().getId()) {
                faulty.put(task.getSignature(), okReport(ROOT_P2, "another-schema", 1));
            }
        }
        reportAll(job, tasks, faulty);
        waitDigests(job);
        Assertions.assertEquals(1, job.getReuseResult().getKeptPartitions());
        Assertions.assertTrue(job.getReuseResult().isKept(tbl2.getId(), p1().getId()));
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_SCHEMA_MISMATCH, job.getReuseResult().getDecisions()
                .stream().filter(d -> !d.kept).findFirst().get().reason);
    }

    @Test
    public void testFailedTaskAndUnsupportedDownload() {
        RestoreJob job = prepareJob("full");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        Map<Long, TFinishTaskRequest> faulty = Maps.newHashMap();
        // a failed task in p1, a report without digest (an old backend) in p2
        faulty.put(tasks.stream().filter(t -> t.getPartitionId() == p1().getId()).findFirst().get().getSignature(),
                failedReport());
        TFinishTaskRequest noDigest = new TFinishTaskRequest();
        noDigest.setTaskStatus(new TStatus(TStatusCode.OK));
        faulty.put(tasks.stream().filter(t -> t.getPartitionId() == p2().getId()).findFirst().get().getSignature(),
                noDigest);
        reportAll(job, tasks, faulty);
        waitDigests(job);
        Assertions.assertEquals(0, job.getReuseResult().getKeptPartitions());
        Assertions.assertTrue(job.getReuseResult().getDecisions().stream()
                .allMatch(d -> RestoreReuseResult.DOWNLOAD_DIGEST_ERROR.equals(d.reason)));
        Assertions.assertEquals(2, versionInfo(job).size());
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()),
                submitted.stream().filter(t -> t instanceof SnapshotTask).count());
        Assertions.assertNull(p1().getRestoreLineage());
        Assertions.assertNull(p2().getRestoreLineage());
    }

    @Test
    public void testTimeoutDownloadsTheUnfinished() {
        RestoreJob job = prepareJob("full");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        // p2 is done, one task of p1 is not
        reportAll(job, tasks.stream().filter(t -> t.getPartitionId() == p2().getId()).collect(Collectors.toList()),
                Maps.newHashMap());
        reportAll(job, tasks.stream().filter(t -> t.getPartitionId() == p1().getId()).skip(1)
                .collect(Collectors.toList()), Maps.newHashMap());
        waitDigests(job);
        Assertions.assertEquals(RestoreJobState.VERIFYING, job.getState());

        Config.restore_digest_timeout_s = 1;
        Deencapsulation.setField(job, "verifyRoundStartMs", System.currentTimeMillis() - 5000);
        waitDigests(job);
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertTrue(result.isKept(tbl2.getId(), p2().getId()));
        Assertions.assertFalse(result.isKept(tbl2.getId(), p1().getId()));
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_TIMEOUT, result.getDecisions().stream()
                .filter(d -> d.partitionId == p1().getId()).findFirst().get().reason);
        // a report that comes after the timeout is ignored
        Assertions.assertTrue(job.finishRestoreDigestTask(tasks.get(0), okReport(ROOT_P1, SIG, 1)));
    }

    @Test
    public void testUnavailableBackendDownloads() {
        Mockito.when(systemInfo.getBackend(CatalogMocker.BACKEND1_ID)).thenReturn(null);
        RestoreJob job = prepareJob("full");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        // no task for the replicas on the missing backend
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()) - 4, tasks.size());
        reportAll(job, tasks, Maps.newHashMap());
        waitDigests(job);
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assertions.assertEquals(0, job.getReuseResult().getKeptPartitions());
    }

    @Test
    public void testLevelOffKeepsWithoutDigest() {
        RestoreJob job = prepareJob("off");
        // the lineage of the candidates is kept until they are decided
        Assertions.assertNotNull(p1().getRestoreLineage());
        Deencapsulation.invoke(job, "allReplicasCreated");
        // no digest to compute: VERIFYING is skipped
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assertions.assertTrue(digestTasks(submitted).isEmpty());
        Assertions.assertEquals(0, submitted.size());
        Assertions.assertEquals(2, job.getReuseResult().getKeptPartitions());
        Assertions.assertEquals(2, job.getReuseResult().getDecisions().size());
        for (RestoreReuseResult.Decision decision : job.getReuseResult().getDecisions()) {
            Assertions.assertEquals(RestoreReuseResult.KEPT_L0_ONLY, decision.reason);
            Assertions.assertEquals("off", decision.level);
        }
        Assertions.assertTrue(versionInfo(job).isEmpty());
        Assertions.assertTrue(fileMapping(job).getMapping().isEmpty());
    }

    @Test
    public void testUniqueTableUpgradesOffToSample() {
        Deencapsulation.setField(tbl2, "keysType", KeysType.UNIQUE_KEYS);
        Config.restore_reuse_sample_ratio = 0.1;
        RestoreJob job = prepareJob("off");
        // not kept on the lineage alone, they need a digest
        Assertions.assertTrue(job.getReuseResult().getDecisions().isEmpty());
        Deencapsulation.invoke(job, "allReplicasCreated");
        Assertions.assertEquals(RestoreJobState.VERIFYING, job.getState());
        // sampled: one of the two partitions
        List<RestoreDigestTask> tasks = newDigestTasks();
        Assertions.assertEquals(replicasOf(p1()), tasks.size());
    }

    @Test
    public void testSampleMismatchUpgradesTheRestToFull() {
        Config.restore_reuse_sample_ratio = 0.1;
        RestoreJob job = prepareJob("sample");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        // only the sampled partition (at least one) is computed first
        Assertions.assertEquals(replicasOf(p1()), tasks.size());
        long sampledPart = tasks.get(0).getPartitionId();
        Map<Long, TFinishTaskRequest> faulty = Maps.newHashMap();
        faulty.put(tasks.get(1).getSignature(), okReport("ffff", SIG, 1));
        reportAll(job, tasks, faulty);
        waitDigests(job);

        // the other partition is verified too, in a second round
        Assertions.assertEquals(RestoreJobState.VERIFYING, job.getState());
        List<RestoreDigestTask> second = newDigestTasks();
        Assertions.assertEquals(replicasOf(p2()), second.size());
        Assertions.assertTrue(second.stream().noneMatch(t -> t.getPartitionId() == sampledPart));
        Assertions.assertEquals(2, (int) Deencapsulation.getField(job, "verifyRound"));
        reportAll(job, second, Maps.newHashMap());
        waitDigests(job);
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(1, result.getKeptPartitions());
        Assertions.assertFalse(result.isKept(tbl2.getId(), sampledPart));
        RestoreReuseResult.Decision other = result.getDecisions().stream()
                .filter(d -> d.partitionId != sampledPart).findFirst().get();
        Assertions.assertTrue(other.kept);
        Assertions.assertEquals(RestoreReuseResult.KEPT_DIGEST_VERIFIED, other.reason);
        Assertions.assertEquals("full", other.level);
    }

    @Test
    public void testSamplePassedKeepsTheRestWithoutDigest() {
        Config.restore_reuse_sample_ratio = 0.1;
        RestoreJob job = prepareJob("sample");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        Assertions.assertEquals(replicasOf(p1()), tasks.size());
        long sampledPart = tasks.get(0).getPartitionId();
        reportAll(job, tasks, Maps.newHashMap());
        waitDigests(job);
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(2, result.getKeptPartitions());
        for (RestoreReuseResult.Decision decision : result.getDecisions()) {
            Assertions.assertEquals(decision.partitionId == sampledPart ? RestoreReuseResult.KEPT_DIGEST_VERIFIED
                    : RestoreReuseResult.KEPT_SAMPLE_PASSED, decision.reason);
        }
    }

    @Test
    public void testSampleFailureDownloadsTheRest() {
        Config.restore_reuse_sample_ratio = 0.1;
        RestoreJob job = prepareJob("sample");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        long sampledPart = tasks.get(0).getPartitionId();
        Map<Long, TFinishTaskRequest> faulty = Maps.newHashMap();
        faulty.put(tasks.get(0).getSignature(), failedReport());
        reportAll(job, tasks, faulty);
        waitDigests(job);
        // an uncertain sample proves nothing about the others, no second round
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assertions.assertEquals(0, job.getReuseResult().getKeptPartitions());
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_SAMPLE_FAILED, job.getReuseResult().getDecisions()
                .stream().filter(d -> d.partitionId != sampledPart).findFirst().get().reason);
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()),
                submitted.stream().filter(t -> t instanceof SnapshotTask).count());
    }

    @Test
    public void testVersionChangedWhileVerifyingDownloads() {
        RestoreJob job = prepareJob("full");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        reportAll(job, tasks, Maps.newHashMap());
        // a write sneaks in after the digests are computed
        p1().updateVersionForRestore(V1 + 1);
        waitDigests(job);
        Assertions.assertEquals(1, job.getReuseResult().getKeptPartitions());
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_CHANGED, job.getReuseResult().getDecisions().stream()
                .filter(d -> d.partitionId == p1().getId()).findFirst().get().reason);
        Assertions.assertTrue(versionInfo(job).contains(tbl2.getId(), p1().getId()));
    }

    // ---------------------------------------------------------------------------------------------
    // cancel, persistence and replay, the recheck before commit
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testCancelWhileVerifyingChangesNothingLocal() {
        RestoreJob job = prepareJob("full");
        toVerifying(job);
        newDigestTasks();
        Assertions.assertTrue(job.cancel().ok());
        Assertions.assertEquals(RestoreJobState.CANCELLED, job.getState());
        Assertions.assertEquals(OlapTableState.NORMAL, tbl2.getState());
        Assertions.assertEquals(V1, p1().getVisibleVersion());
        Assertions.assertEquals(V1, p2().getVisibleVersion());
        // the lineage was never touched, the next restore can still reuse the partitions
        Assertions.assertNotNull(p1().getRestoreLineage());
        Assertions.assertNotNull(p2().getRestoreLineage());
        Assertions.assertEquals(Partition.PartitionState.NORMAL, p1().getState());
        // a late report is ignored
        Assertions.assertTrue(job.finishRestoreDigestTask(new RestoreDigestTask(CatalogMocker.BACKEND1_ID, 1,
                job.getJobId(), db.getId(), tbl2.getId(), p1().getId(), tbl2.getId(), tabletOf(p1()), 1, V1, 0),
                okReport(ROOT_P1, SIG, 1)));
    }

    @Test
    public void testPersistAndReplay() throws Exception {
        RestoreJob job = prepareJob("full");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        Map<Long, TFinishTaskRequest> faulty = Maps.newHashMap();
        faulty.put(tasks.stream().filter(t -> t.getPartitionId() == p1().getId()).findFirst().get().getSignature(),
                okReport("ffff", SIG, 1));
        reportAll(job, tasks, faulty);
        waitDigests(job);
        // the DOWNLOAD edit log
        Deencapsulation.setField(job, "state", RestoreJobState.DOWNLOAD);
        RestoreJob replayed = writeAndRead(job);

        RestoreReuseResult result = replayed.getReuseResult();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.getDecisions().size());
        Assertions.assertEquals(1, result.getKeptPartitions());
        Assertions.assertTrue(result.isKept(tbl2.getId(), p2().getId()));
        Assertions.assertEquals(job.getReuseResult().toString(), result.toString());
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_ROOT_MISMATCH, result.getDecisions().stream()
                .filter(d -> !d.kept).findFirst().get().reason);
        Assertions.assertEquals(RestoreReuseResult.KEPT_DIGEST_VERIFIED, result.getDecisions().stream()
                .filter(d -> d.kept).findFirst().get().reason);
        Assertions.assertTrue(versionInfo(replayed).contains(tbl2.getId(), p1().getId()));
        Assertions.assertFalse(versionInfo(replayed).contains(tbl2.getId(), p2().getId()));
        Assertions.assertEquals(replicasOf(p1()), fileMapping(replayed).getMapping().size());

        // the follower replays it: the lineage of the partition to download is invalidated, the kept one stays.
        p1().setRestoreLineage(lineage(selfBackupJobInfo(), p1()));
        p2().setRestoreLineage(lineage(selfBackupJobInfo(), p2()));
        p1().setState(Partition.PartitionState.NORMAL);
        replayed.setEnv(env);
        replayed.replayRun();
        Assertions.assertNull(p1().getRestoreLineage());
        Assertions.assertNotNull(p2().getRestoreLineage());
        Assertions.assertEquals(Partition.PartitionState.RESTORE, p2().getState());

        // the finished job (the log written at the end) keeps the result, and the follower commits the same
        Deencapsulation.setField(job, "state", RestoreJobState.COMMITTING);
        Status st = job.allTabletCommitted(false);
        Assertions.assertTrue(st.ok(), st.toString());
        RestoreJob finished = writeAndRead(job);
        Assertions.assertEquals(1, finished.getReuseResult().getKeptPartitions());
        Assertions.assertEquals(RestoreJobState.FINISHED, finished.getState());
    }

    @Test
    public void testGsonWithoutReuseResult() throws Exception {
        // A job written by an FE without the field (or with the switch off).
        Config.enable_restore_partition_reuse = false;
        RestoreJob job = prepareJob(null);
        RestoreJob replayed = writeAndRead(job);
        Assertions.assertNull(replayed.getReuseResult());
        String json = org.apache.doris.persist.gson.GsonUtils.GSON.toJson(job);
        Assertions.assertFalse(json.contains("\"rru\""));
        // the working state is not persisted
        Assertions.assertFalse(json.contains("digestResults"));
        Assertions.assertFalse(json.contains("reuseCandidates"));
    }

    private RestoreJob keptJobAtCommit() {
        RestoreJob job = prepareJob("full");
        toVerifying(job);
        reportAll(job, newDigestTasks(), Maps.newHashMap());
        waitDigests(job);
        Assertions.assertEquals(2, job.getReuseResult().getKeptPartitions());
        return job;
    }

    @Test
    public void testRecheckBeforeCommitFailsTheJob() {
        RestoreJob job = keptJobAtCommit();
        // a write that should never happen
        p2().updateVersionForRestore(V1 + 3);
        Status st = job.allTabletCommitted(false);
        Assertions.assertFalse(st.ok());
        Assertions.assertTrue(st.getErrMsg().contains("changed from " + V1 + " to " + (V1 + 3)), st.getErrMsg());
        Assertions.assertTrue(st.getErrMsg().contains("p2"), st.getErrMsg());
        // nothing is committed: still RESTORE state and the version is not rewritten
        Assertions.assertEquals(OlapTableState.RESTORE, tbl2.getState());
        Assertions.assertEquals(V1 + 3, p2().getVisibleVersion());

        // before any tablet is moved
        Deencapsulation.setField(job, "state", RestoreJobState.COMMIT);
        job.commit();
        Assertions.assertFalse(job.getStatus().ok());
        Assertions.assertEquals(RestoreJobState.COMMIT, job.getState());
        Assertions.assertTrue(submitted.stream().noneMatch(t -> t instanceof DirMoveTask));
    }

    @Test
    public void testRecheckFailsWhenTheKeptPartitionIsDropped() {
        RestoreJob job = keptJobAtCommit();
        Map<Long, Partition> idToPartition = Deencapsulation.getField(tbl2, "idToPartition");
        Map<String, Partition> nameToPartition = Deencapsulation.getField(tbl2, "nameToPartition");
        Partition p1 = p1();
        idToPartition.remove(p1.getId());
        nameToPartition.remove(p1.getName());
        Status st = job.allTabletCommitted(false);
        Assertions.assertFalse(st.ok());
        Assertions.assertTrue(st.getErrMsg().contains("has been dropped"), st.getErrMsg());
    }

    @Test
    public void testReplayDoesNotRecheck() {
        RestoreJob job = keptJobAtCommit();
        p2().updateVersionForRestore(V1 + 3);
        // the follower trusts the master
        Status st = job.allTabletCommitted(true);
        Assertions.assertTrue(st.ok(), st.toString());
    }
}
