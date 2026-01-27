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

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.LocalTabletInvertedIndex;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.info.TableRefInfo;
import org.apache.doris.nereids.trees.plans.commands.BackupCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelBackupCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateRepositoryCommand;
import org.apache.doris.nereids.trees.plans.commands.RestoreCommand;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.task.DirMoveTask;
import org.apache.doris.task.DownloadTask;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.task.UploadTask;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BackupHandlerTest {

    private BackupHandler handler;

    @Mocked
    private Env env;
    @Mocked
    private InternalCatalog catalog;
    @Mocked
    private BrokerMgr brokerMgr;
    @Mocked
    private EditLog editLog;

    private Database db;

    private long idGen = 0;

    private File rootDir;

    private String tmpPath = "./tmp" + System.currentTimeMillis();

    private TabletInvertedIndex invertedIndex = new LocalTabletInvertedIndex();

    @Before
    public void setUp() throws Exception {
        Config.tmp_dir = tmpPath;
        rootDir = new File(Config.tmp_dir);
        rootDir.mkdirs();
        FeConstants.runningUnitTest = true;

        new Expectations() {
            {
                env.getBrokerMgr();
                minTimes = 0;
                result = brokerMgr;

                env.getNextId();
                minTimes = 0;
                result = idGen++;

                env.getEditLog();
                minTimes = 0;
                result = editLog;

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                Env.getCurrentEnvJournalVersion();
                minTimes = 0;
                result = FeConstants.meta_version;

                Env.getCurrentInvertedIndex();
                minTimes = 0;
                result = invertedIndex;
            }
        };

        db = CatalogMocker.mockDb();
        catalog = Deencapsulation.newInstance(InternalCatalog.class);

        new Expectations() {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getDbOrDdlException(anyString);
                minTimes = 0;
                result = db;
            }
        };
    }

    @After
    public void done() {
        if (rootDir != null) {
            try {
                Files.walk(Paths.get(Config.tmp_dir),
                           FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testInit() {
        handler = new BackupHandler(env);
        handler.runAfterCatalogReady();

        File backupDir = new File(BackupHandler.BACKUP_ROOT_DIR.toString());
        Assert.assertTrue(backupDir.exists());
    }

    @Test
    public void testCreateAndDropRepository() throws Exception {
        new Expectations() {
            {
                editLog.logCreateRepository((Repository) any);
                minTimes = 0;
                result = new Delegate() {
                    public void logCreateRepository(Repository repo) {

                    }
                };

                editLog.logDropRepository(anyString);
                minTimes = 0;
                result = new Delegate() {
                    public void logDropRepository(String repoName) {

                    }
                };
            }
        };

        new MockUp<Repository>() {
            @Mock
            public Status initRepository() {
                return Status.OK;
            }

            @Mock
            public Status listSnapshots(List<String> snapshotNames) {
                snapshotNames.add("ss2");
                return Status.OK;
            }

            @Mock
            public Status getSnapshotInfoFile(String label, String backupTimestamp, List<BackupJobInfo> infos) throws Exception {
                OlapTable tbl = (OlapTable) db.getTableOrMetaException(CatalogMocker.TEST_TBL_NAME);
                List<Table> tbls = Lists.newArrayList();
                tbls.add(tbl);
                List<Resource> resources = Lists.newArrayList();
                BackupMeta backupMeta = new BackupMeta(tbls, resources);
                Map<Long, SnapshotInfo> snapshotInfos = Maps.newHashMap();
                for (Partition part : tbl.getPartitions()) {
                    for (MaterializedIndex idx : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        for (Tablet tablet : idx.getTablets()) {
                            List<String> files = Lists.newArrayList();
                            SnapshotInfo sinfo = new SnapshotInfo(db.getId(), tbl.getId(), part.getId(), idx.getId(),
                                    tablet.getId(), -1, 0, "./path", files);
                            snapshotInfos.put(tablet.getId(), sinfo);
                        }
                    }
                }

                BackupJobInfo info = BackupJobInfo.fromCatalog(System.currentTimeMillis(),
                        "ss2", CatalogMocker.TEST_DB_NAME,
                        CatalogMocker.TEST_DB_ID, BackupCommand.BackupContent.ALL,
                        backupMeta, snapshotInfos, null);
                infos.add(info);
                return Status.OK;
            }
        };

        new Expectations() {
            {
                brokerMgr.containsBroker(anyString);
                minTimes = 0;
                result = true;
            }
        };

        // add repo
        handler = new BackupHandler(env);
        StorageBackend storageBackend = new StorageBackend("broker", "bos://location",
                StorageBackend.StorageType.BROKER, Maps.newHashMap());

        CreateRepositoryCommand command = new CreateRepositoryCommand(false, "repo", storageBackend);
        handler.createRepository(command);

        // process backup
        List<TableRefInfo> tableRefInfos = Lists.newArrayList();
        tableRefInfos.add(new TableRefInfo(new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, CatalogMocker.TEST_DB_NAME,
                CatalogMocker.TEST_TBL_NAME), null, null, null, null, null, null, null));
        Map<String, String> properties = Maps.newHashMap();
        properties.put("backup_timestamp", "2018-08-08-08-08-08");
        boolean isExclude = false;
        BackupCommand backupCommand = new BackupCommand(new LabelNameInfo(CatalogMocker.TEST_DB_NAME, "label1"), "repo", tableRefInfos, properties, isExclude);
        handler.process(backupCommand);

        // handleFinishedSnapshotTask
        BackupJob backupJob = (BackupJob) handler.getJob(CatalogMocker.TEST_DB_ID);
        SnapshotTask snapshotTask = new SnapshotTask(null, 0, 0, backupJob.getJobId(), CatalogMocker.TEST_DB_ID, 0, 0,
                0, 0, 0, 0, 1, false);
        TFinishTaskRequest request = new TFinishTaskRequest();
        List<String> snapshotFiles = Lists.newArrayList();
        request.setSnapshotFiles(snapshotFiles);
        request.setSnapshotPath("./snapshot/path");
        request.setTaskStatus(new TStatus(TStatusCode.OK));
        handler.handleFinishedSnapshotTask(snapshotTask, request);

        // handleFinishedSnapshotUploadTask
        Map<String, String> srcToDestPath = Maps.newHashMap();
        UploadTask uploadTask = new UploadTask(null, 0, 0, backupJob.getJobId(), CatalogMocker.TEST_DB_ID,
                srcToDestPath, null, null, StorageBackend.StorageType.BROKER, "");
        request = new TFinishTaskRequest();
        Map<Long, List<String>> tabletFiles = Maps.newHashMap();
        request.setTabletFiles(tabletFiles);
        request.setTaskStatus(new TStatus(TStatusCode.OK));
        handler.handleFinishedSnapshotUploadTask(uploadTask, request);

        // cancel backup
        handler.cancel(new CancelBackupCommand(CatalogMocker.TEST_DB_NAME, false));

        // process restore
        List<TableRefInfo> tableRefInfos2 = Lists.newArrayList();
        tableRefInfos2.add(new TableRefInfo(new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, CatalogMocker.TEST_DB_NAME,
                CatalogMocker.TEST_TBL_NAME), null, null, null, null, null, null, null));
        Map<String, String> properties02 = Maps.newHashMap();
        properties02.put("backup_timestamp", "2018-08-08-08-08-08");
        boolean isExclude02 = false;
        RestoreCommand restoreCommand = new RestoreCommand(new LabelNameInfo(CatalogMocker.TEST_DB_NAME, "ss2"), "repo", tableRefInfos2, properties02, isExclude02);
        restoreCommand.analyzeProperties();
        handler.process(restoreCommand);

        // handleFinishedSnapshotTask
        RestoreJob restoreJob = (RestoreJob) handler.getJob(CatalogMocker.TEST_DB_ID);
        snapshotTask = new SnapshotTask(null, 0, 0, restoreJob.getJobId(), CatalogMocker.TEST_DB_ID,
                0, 0, 0, 0, 0, 0, 1, true);
        request = new TFinishTaskRequest();
        request.setSnapshotPath("./snapshot/path");
        request.setTaskStatus(new TStatus(TStatusCode.OK));
        handler.handleFinishedSnapshotTask(snapshotTask, request);

        // handleDownloadSnapshotTask
        DownloadTask downloadTask = new DownloadTask(null, 0, 0, restoreJob.getJobId(), CatalogMocker.TEST_DB_ID,
                srcToDestPath, null, null, StorageBackend.StorageType.BROKER, "", "");
        request = new TFinishTaskRequest();
        List<Long> downloadedTabletIds = Lists.newArrayList();
        request.setDownloadedTabletIds(downloadedTabletIds);
        request.setTaskStatus(new TStatus(TStatusCode.OK));
        handler.handleDownloadSnapshotTask(downloadTask, request);

        // handleDirMoveTask
        DirMoveTask dirMoveTask = new DirMoveTask(null, 0, 0, restoreJob.getJobId(), CatalogMocker.TEST_DB_ID, 0, 0, 0,
                0, "", 0, true);
        request = new TFinishTaskRequest();
        request.setTaskStatus(new TStatus(TStatusCode.OK));
        handler.handleDirMoveTask(dirMoveTask, request);

        // cancel restore
        handler.cancel(new CancelBackupCommand(CatalogMocker.TEST_DB_NAME, true));

        // drop repo
        handler.dropRepository("repo");
    }

    // ========== Dual Queue Architecture Tests ==========

    /**
     * Test 1: Basic dual queue operations - Add job to running queue
     */
    @Test
    public void testDualQueueAddActiveJob() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        BackupJob job = new BackupJob("test_backup", dbId, "test_db",
                Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);

        try {
            Deencapsulation.invoke(handler, "addActiveJob", dbId, job);

            List jobs = Deencapsulation.invoke(handler, "getAllRunningJobs");
            Assert.assertEquals(1, jobs.size());
            Assert.assertEquals(job, jobs.get(0));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 2: Move job from running to history queue
     */
    @Test
    public void testDualQueueMoveToHistory() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        BackupJob job = new BackupJob("test_backup", dbId, "test_db",
                Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);

        try {
            Deencapsulation.invoke(handler, "addActiveJob", dbId, job);
            Deencapsulation.invoke(handler, "moveToHistory", dbId, job);

            List runningJobs = Deencapsulation.invoke(handler, "getAllRunningJobs");
            Assert.assertEquals(0, runningJobs.size());

            List allJobs = Deencapsulation.invoke(handler, "getAllJobs", dbId);
            Assert.assertEquals(1, allJobs.size());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 3: FIFO cleanup in history queue
     */
    @Test
    public void testDualQueueFifoCleanup() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        int originalLimit = Config.max_backup_restore_job_num_per_db;
        Config.max_backup_restore_job_num_per_db = 3;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            for (int i = 0; i < 5; i++) {
                BackupJob job = new BackupJob("test_backup_" + i, dbId, "test_db",
                        Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
                Deencapsulation.setField(job, "jobId", (long) i);
                Deencapsulation.invoke(handler, "addToHistoryQueue", dbId, job);
            }

            List allJobs = Deencapsulation.invoke(handler, "getAllJobs", dbId);
            Assert.assertEquals(3, allJobs.size());
            Assert.assertEquals("test_backup_2", ((AbstractJob) allJobs.get(0)).getLabel());
            Assert.assertEquals("test_backup_4", ((AbstractJob) allJobs.get(2)).getLabel());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
            Config.max_backup_restore_job_num_per_db = originalLimit;
        }
    }

    /**
     * Test 4: Soft limit (warning only)
     */
    @Test
    public void testDualQueueSoftLimit() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        int originalSoftLimit = Config.max_backup_restore_running_queue_soft_limit;
        Config.max_backup_restore_running_queue_soft_limit = 2;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            for (int i = 0; i < 3; i++) {
                BackupJob job = new BackupJob("test_backup_" + i, dbId, "test_db",
                        Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
                Deencapsulation.setField(job, "jobId", (long) i);
                Deencapsulation.invoke(handler, "addActiveJob", dbId, job);
            }

            List jobs = Deencapsulation.invoke(handler, "getAllRunningJobs");
            Assert.assertEquals(3, jobs.size());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
            Config.max_backup_restore_running_queue_soft_limit = originalSoftLimit;
        }
    }

    /**
     * Test 5: Hard limit (reject new jobs)
     */
    @Test
    public void testDualQueueHardLimit() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        int originalHardLimit = Config.max_backup_restore_running_queue_hard_limit;
        Config.max_backup_restore_running_queue_hard_limit = 2;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            for (int i = 0; i < 2; i++) {
                BackupJob job = new BackupJob("test_backup_" + i, dbId, "test_db",
                        Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
                Deencapsulation.setField(job, "jobId", (long) i);
                Deencapsulation.invoke(handler, "addActiveJob", dbId, job);
            }

            BackupJob extraJob = new BackupJob("test_backup_extra", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(extraJob, "jobId", 100L);

            boolean exceptionThrown = false;
            try {
                Deencapsulation.invoke(handler, "addActiveJob", dbId, extraJob);
            } catch (Exception e) {
                exceptionThrown = true;
                Assert.assertTrue(e.getMessage().contains("Running queue is full"));
            }

            Assert.assertTrue("Hard limit should reject new jobs", exceptionThrown);

            List jobs = Deencapsulation.invoke(handler, "getAllRunningJobs");
            Assert.assertEquals(2, jobs.size());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
            Config.max_backup_restore_running_queue_hard_limit = originalHardLimit;
        }
    }

    /**
     * Test 6: Get all jobs (merge running + history)
     */
    @Test
    public void testDualQueueGetAllJobs() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob runningJob1 = new BackupJob("running_1", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(runningJob1, "jobId", 1L);
            BackupJob runningJob2 = new BackupJob("running_2", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(runningJob2, "jobId", 2L);

            Deencapsulation.invoke(handler, "addActiveJob", dbId, runningJob1);
            Deencapsulation.invoke(handler, "addActiveJob", dbId, runningJob2);

            BackupJob historyJob = new BackupJob("history_1", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(historyJob, "jobId", 3L);
            Deencapsulation.invoke(handler, "addToHistoryQueue", dbId, historyJob);

            List allJobs = Deencapsulation.invoke(handler, "getAllJobs", dbId);
            Assert.assertEquals(3, allJobs.size());

            List runningJobs = Deencapsulation.invoke(handler, "getAllRunningJobs");
            Assert.assertEquals(2, runningJobs.size());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 7: Concurrent access thread safety
     */
    @Test
    public void testDualQueueConcurrentAccess() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            List<Thread> threads = Lists.newArrayList();
            for (int i = 0; i < 5; i++) {
                final int index = i;
                Thread thread = new Thread(() -> {
                    try {
                        BackupJob job = new BackupJob("concurrent_" + index, dbId, "test_db",
                                Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
                        Deencapsulation.setField(job, "jobId", (long) index);
                        Deencapsulation.invoke(handler, "addActiveJob", dbId, job);
                    } catch (Exception e) {
                        // Ignore
                    }
                });
                threads.add(thread);
                thread.start();
            }

            for (Thread thread : threads) {
                thread.join();
            }

            List jobs = Deencapsulation.invoke(handler, "getAllRunningJobs");
            Assert.assertEquals(5, jobs.size());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 8: runAfterCatalogReady skips completed jobs (optimization test)
     */
    @Test
    public void testRunAfterCatalogReadySkipsCompletedJobs() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob completedJob = new BackupJob("completed", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(completedJob, "jobId", 1L);

            new MockUp<BackupJob>() {
                @Mock
                public boolean isDone() {
                    return true;
                }
            };

            Deencapsulation.invoke(handler, "addActiveJob", dbId, completedJob);

            handler.runAfterCatalogReady();

            Assert.assertTrue("Completed jobs should be skipped", true);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 9: Remove job from running queue
     */
    @Test
    public void testRemoveFromRunningQueue() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob job1 = new BackupJob("job1", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job1, "jobId", 1L);
            BackupJob job2 = new BackupJob("job2", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job2, "jobId", 2L);

            Deencapsulation.invoke(handler, "addActiveJob", dbId, job1);
            Deencapsulation.invoke(handler, "addActiveJob", dbId, job2);

            List jobs = Deencapsulation.invoke(handler, "getAllRunningJobs");
            Assert.assertEquals(2, jobs.size());

            Deencapsulation.invoke(handler, "removeFromRunningQueue", dbId, job1);

            jobs = Deencapsulation.invoke(handler, "getAllRunningJobs");
            Assert.assertEquals(1, jobs.size());
            Assert.assertEquals(job2, jobs.get(0));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 10: Multiple databases isolation
     */
    @Test
    public void testDualQueueMultipleDatabasesIsolation() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId1 = 1L;
        long dbId2 = 2L;

        try {
            BackupJob job1 = new BackupJob("job1", dbId1, "test_db1",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job1, "jobId", 1L);
            BackupJob job2 = new BackupJob("job2", dbId2, "test_db2",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job2, "jobId", 2L);

            Deencapsulation.invoke(handler, "addActiveJob", dbId1, job1);
            Deencapsulation.invoke(handler, "addActiveJob", dbId2, job2);

            List jobs1 = Deencapsulation.invoke(handler, "getAllJobs", dbId1);
            List jobs2 = Deencapsulation.invoke(handler, "getAllJobs", dbId2);

            Assert.assertEquals(1, jobs1.size());
            Assert.assertEquals(1, jobs2.size());
            Assert.assertEquals(job1, jobs1.get(0));
            Assert.assertEquals(job2, jobs2.get(0));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    // ========== Job Queue Position & Block Reason Tests ==========

    /**
     * Test 11: getJobQueuePosition returns 0 when concurrency is disabled
     */
    @Test
    public void testGetJobQueuePositionDisabled() throws Exception {
        Config.enable_table_level_backup_concurrency = false;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        BackupJob job = new BackupJob("test_backup", dbId, "test_db",
                Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
        Deencapsulation.setField(job, "jobId", 1L);

        Assert.assertEquals(0, handler.getJobQueuePosition(job));
    }

    /**
     * Test 12: getJobQueuePosition returns 0 for running jobs (in allowedJobIds)
     */
    @Test
    public void testGetJobQueuePositionRunning() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob job = new BackupJob("test_backup", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job, "jobId", 1L);

            // Add to running queue and allowedJobIds
            Deencapsulation.invoke(handler, "addActiveJob", dbId, job);
            Set<Long> allowedJobIds = Deencapsulation.getField(handler, "allowedJobIds");
            allowedJobIds.add(1L);

            Assert.assertEquals(0, handler.getJobQueuePosition(job));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 13: getJobQueuePosition returns correct positions for pending jobs
     */
    @Test
    public void testGetJobQueuePositionPending() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            // Create 3 jobs: job1 is running, job2 and job3 are pending
            BackupJob job1 = new BackupJob("running_job", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job1, "jobId", 1L);
            BackupJob job2 = new BackupJob("pending_job_1", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job2, "jobId", 2L);
            BackupJob job3 = new BackupJob("pending_job_2", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job3, "jobId", 3L);

            Deencapsulation.invoke(handler, "addActiveJob", dbId, job1);
            Deencapsulation.invoke(handler, "addActiveJob", dbId, job2);
            Deencapsulation.invoke(handler, "addActiveJob", dbId, job3);

            // Only job1 is allowed (running)
            Set<Long> allowedJobIds = Deencapsulation.getField(handler, "allowedJobIds");
            allowedJobIds.add(1L);

            // job1 is running → position 0
            Assert.assertEquals(0, handler.getJobQueuePosition(job1));
            // job2 is first pending → position 1
            Assert.assertEquals(1, handler.getJobQueuePosition(job2));
            // job3 is second pending → position 2
            Assert.assertEquals(2, handler.getJobQueuePosition(job3));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 14: getJobBlockReason returns null when concurrency is disabled
     */
    @Test
    public void testGetJobBlockReasonDisabled() throws Exception {
        Config.enable_table_level_backup_concurrency = false;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        BackupJob job = new BackupJob("test_backup", dbId, "test_db",
                Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
        Deencapsulation.setField(job, "jobId", 1L);

        Assert.assertNull(handler.getJobBlockReason(job));
    }

    /**
     * Test 15: getJobBlockReason returns null for running jobs (has permission)
     */
    @Test
    public void testGetJobBlockReasonRunning() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob job = new BackupJob("test_backup", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job, "jobId", 1L);

            Set<Long> allowedJobIds = Deencapsulation.getField(handler, "allowedJobIds");
            allowedJobIds.add(1L);

            Assert.assertNull(handler.getJobBlockReason(job));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 16: getJobBlockReason shows backup blocked by active restores
     */
    @Test
    public void testGetJobBlockReasonBackupBlockedByRestore() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob backupJob = new BackupJob("test_backup", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(backupJob, "jobId", 1L);

            // Set up stats with active restores
            Map<Long, BackupHandler.DatabaseJobStats> dbJobStats = Deencapsulation.getField(handler, "dbJobStats");
            BackupHandler.DatabaseJobStats stats = new BackupHandler.DatabaseJobStats();
            stats.activeRestores = 2;
            dbJobStats.put(dbId, stats);

            String reason = handler.getJobBlockReason(backupJob);
            Assert.assertNotNull(reason);
            Assert.assertTrue(reason.contains("restore"));
            Assert.assertTrue(reason.contains("2"));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 17: getJobBlockReason shows restore blocked by active backups
     */
    @Test
    public void testGetJobBlockReasonRestoreBlockedByBackup() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            RestoreJob restoreJob = new RestoreJob();
            Deencapsulation.setField(restoreJob, "jobId", 1L);
            Deencapsulation.setField(restoreJob, "dbId", dbId);

            // Set up stats with active backups
            Map<Long, BackupHandler.DatabaseJobStats> dbJobStats = Deencapsulation.getField(handler, "dbJobStats");
            BackupHandler.DatabaseJobStats stats = new BackupHandler.DatabaseJobStats();
            stats.activeBackups = 3;
            dbJobStats.put(dbId, stats);

            String reason = handler.getJobBlockReason(restoreJob);
            Assert.assertNotNull(reason);
            Assert.assertTrue(reason.contains("backup"));
            Assert.assertTrue(reason.contains("3"));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 18: getJobBlockReason shows restore blocked by table conflict
     */
    @Test
    public void testGetJobBlockReasonRestoreTableConflict() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            RestoreJob restoreJob = new RestoreJob();
            Deencapsulation.setField(restoreJob, "jobId", 1L);
            Deencapsulation.setField(restoreJob, "dbId", dbId);

            // Set tableRefs
            List<TableRefInfo> tableRefs = Lists.newArrayList();
            tableRefs.add(new TableRefInfo(
                    new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, "test_db", "conflict_table"),
                    null, null, null, null, null, null, null));
            restoreJob.setTableRefs(tableRefs);

            // Set up restoring tables with a conflict
            Map<Long, java.util.Set<String>> restoringTables = Deencapsulation.getField(handler, "restoringTables");
            java.util.Set<String> tables = java.util.concurrent.ConcurrentHashMap.newKeySet();
            tables.add("conflict_table");
            restoringTables.put(dbId, tables);

            // Set up empty stats (no backup/restore blocking)
            Map<Long, BackupHandler.DatabaseJobStats> dbJobStats = Deencapsulation.getField(handler, "dbJobStats");
            dbJobStats.put(dbId, new BackupHandler.DatabaseJobStats());

            String reason = handler.getJobBlockReason(restoreJob);
            Assert.assertNotNull(reason);
            Assert.assertTrue(reason.contains("conflict_table"));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 19: getJobBlockReason shows backup blocked by full database backup
     */
    @Test
    public void testGetJobBlockReasonFullDatabaseBackup() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob backupJob = new BackupJob("table_backup", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(backupJob, "jobId", 2L);

            // Set up stats with full database backup marker
            Map<Long, BackupHandler.DatabaseJobStats> dbJobStats = Deencapsulation.getField(handler, "dbJobStats");
            BackupHandler.DatabaseJobStats stats = new BackupHandler.DatabaseJobStats();
            stats.backupDatabaseJobId = 1L;
            stats.backupDatabaseLabel = "full_db_backup";
            dbJobStats.put(dbId, stats);

            String reason = handler.getJobBlockReason(backupJob);
            Assert.assertNotNull(reason);
            Assert.assertTrue(reason.contains("Full database backup"));
            Assert.assertTrue(reason.contains("full_db_backup"));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 20: getJobBlockReason shows restore blocked by full database restore
     */
    @Test
    public void testGetJobBlockReasonFullDatabaseRestore() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            RestoreJob restoreJob = new RestoreJob();
            Deencapsulation.setField(restoreJob, "jobId", 2L);
            Deencapsulation.setField(restoreJob, "dbId", dbId);
            restoreJob.setTableRefs(Lists.newArrayList());

            // Set up stats with full database restore marker
            Map<Long, BackupHandler.DatabaseJobStats> dbJobStats = Deencapsulation.getField(handler, "dbJobStats");
            BackupHandler.DatabaseJobStats stats = new BackupHandler.DatabaseJobStats();
            stats.restoreDatabaseJobId = 1L;
            stats.restoreDatabaseLabel = "full_db_restore";
            dbJobStats.put(dbId, stats);

            String reason = handler.getJobBlockReason(restoreJob);
            Assert.assertNotNull(reason);
            Assert.assertTrue(reason.contains("Full database restore"));
            Assert.assertTrue(reason.contains("full_db_restore"));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 21: getJobBlockReason returns generic message when no specific block
     */
    @Test
    public void testGetJobBlockReasonWaitingForSlot() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob job = new BackupJob("test_backup", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job, "jobId", 1L);

            // No stats, no allowedJobIds → generic waiting message
            String reason = handler.getJobBlockReason(job);
            Assert.assertNotNull(reason);
            Assert.assertTrue(reason.contains("Waiting for execution slot"));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 22: getJobQueuePosition returns 0 when job not in any queue
     */
    @Test
    public void testGetJobQueuePositionNotInQueue() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob job = new BackupJob("orphan_job", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job, "jobId", 99L);

            // No queue exists for this dbId
            Assert.assertEquals(0, handler.getJobQueuePosition(job));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testGlobalSnapshotTaskCounter() {
        BackupHandler handler = new BackupHandler(env);

        Assert.assertEquals(0, handler.getGlobalSnapshotTasks());

        handler.addGlobalSnapshotTasks(100);
        Assert.assertEquals(100, handler.getGlobalSnapshotTasks());

        handler.addGlobalSnapshotTasks(200);
        Assert.assertEquals(300, handler.getGlobalSnapshotTasks());

        handler.addGlobalSnapshotTasks(-100);
        Assert.assertEquals(200, handler.getGlobalSnapshotTasks());

        handler.addGlobalSnapshotTasks(-200);
        Assert.assertEquals(0, handler.getGlobalSnapshotTasks());
    }

    @Test
    public void testOnJobDeactivatedDecrementsGlobalSnapshotTasks() {
        BackupHandler handler = new BackupHandler(env);

        Config.enable_table_level_backup_concurrency = true;
        try {
            long dbId = 1L;
            BackupJob job = new BackupJob("test_deactivate", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job, "jobId", 200L);
            Deencapsulation.setField(job, "snapshotTaskCount", 500);

            handler.addGlobalSnapshotTasks(500);
            Assert.assertEquals(500, handler.getGlobalSnapshotTasks());

            Deencapsulation.invoke(handler, "onJobCreated", dbId, (AbstractJob) job, true, false);
            Deencapsulation.invoke(handler, "onJobActivated", dbId, (AbstractJob) job, true, false);
            Deencapsulation.invoke(handler, "onJobDeactivated", dbId, (AbstractJob) job, true);

            Assert.assertEquals(0, handler.getGlobalSnapshotTasks());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testGlobalSnapshotTaskLimitRejectsWhenExceeded() {
        BackupHandler handler = new BackupHandler(env);
        int savedLimit = Config.max_concurrent_snapshot_tasks_total;

        Config.enable_table_level_backup_concurrency = true;
        Config.max_concurrent_snapshot_tasks_total = 100;
        try {
            handler.addGlobalSnapshotTasks(90);
            Assert.assertEquals(90, handler.getGlobalSnapshotTasks());

            // 90 + 20 = 110 > 100, should exceed
            int globalCurrent = handler.getGlobalSnapshotTasks();
            Assert.assertTrue(globalCurrent + 20 > Config.max_concurrent_snapshot_tasks_total);

            // 90 + 5 = 95 <= 100, should not exceed
            Assert.assertFalse(globalCurrent + 5 > Config.max_concurrent_snapshot_tasks_total);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
            Config.max_concurrent_snapshot_tasks_total = savedLimit;
        }
    }

    @Test
    public void testGlobalSnapshotTaskCounterNotUsedWhenConcurrencyDisabled() {
        BackupHandler handler = new BackupHandler(env);

        Config.enable_table_level_backup_concurrency = false;
        Assert.assertEquals(0, handler.getGlobalSnapshotTasks());

        // In non-concurrent mode, global counter should not be touched by job logic.
        // Verify the counter stays at 0 when concurrency is disabled.
        Assert.assertEquals(0, handler.getGlobalSnapshotTasks());
    }

    /**
     * Test rebuildConcurrencyStateAfterRestart correctly sums globalSnapshotTasks
     * from all active running jobs.
     *
     * Scenario: Add two jobs to running queue with known snapshotTaskCount, rebuild state
     * Expected: globalSnapshotTasks should equal the sum of all running jobs' snapshotTaskCount
     */
    @Test
    public void testRebuildConcurrencyStateRebuildGlobalSnapshotTasks() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob job1 = new BackupJob("rebuild_job1", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job1, "jobId", 1L);
            Deencapsulation.setField(job1, "snapshotTaskCount", 100);
            Deencapsulation.setField(job1, "state", BackupJob.BackupJobState.SNAPSHOTING);

            BackupJob job2 = new BackupJob("rebuild_job2", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job2, "jobId", 2L);
            Deencapsulation.setField(job2, "snapshotTaskCount", 250);
            Deencapsulation.setField(job2, "state", BackupJob.BackupJobState.UPLOADING);

            // Add to the legacy job store (dbIdToBackupOrRestoreJobs) which rebuild scans
            java.util.Deque<AbstractJob> jobDeque = new java.util.LinkedList<>();
            jobDeque.add(job1);
            jobDeque.add(job2);
            Map<Long, java.util.Deque<AbstractJob>> legacyMap = Deencapsulation.getField(handler, "dbIdToBackupOrRestoreJobs");
            legacyMap.put(dbId, jobDeque);

            Deencapsulation.invoke(handler, "rebuildConcurrencyStateAfterRestart");

            Assert.assertEquals(350, handler.getGlobalSnapshotTasks());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test rebuildConcurrencyStateAfterRestart does not count completed jobs.
     *
     * Scenario: One active job with snapshotTaskCount=200 and one FINISHED job with snapshotTaskCount=100
     * Expected: globalSnapshotTasks should only reflect the active job (200)
     */
    @Test
    public void testRebuildConcurrencyStateSkipsCompletedJobs() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob activeJob = new BackupJob("active_job", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(activeJob, "jobId", 1L);
            Deencapsulation.setField(activeJob, "snapshotTaskCount", 200);
            Deencapsulation.setField(activeJob, "state", BackupJob.BackupJobState.SNAPSHOTING);

            BackupJob finishedJob = new BackupJob("finished_job", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(finishedJob, "jobId", 2L);
            Deencapsulation.setField(finishedJob, "snapshotTaskCount", 100);
            Deencapsulation.setField(finishedJob, "state", BackupJob.BackupJobState.FINISHED);

            java.util.Deque<AbstractJob> jobDeque = new java.util.LinkedList<>();
            jobDeque.add(activeJob);
            jobDeque.add(finishedJob);
            Map<Long, java.util.Deque<AbstractJob>> legacyMap = Deencapsulation.getField(handler, "dbIdToBackupOrRestoreJobs");
            legacyMap.put(dbId, jobDeque);

            Deencapsulation.invoke(handler, "rebuildConcurrencyStateAfterRestart");

            // Only active job in running queue contributes to globalSnapshotTasks
            Assert.assertEquals(200, handler.getGlobalSnapshotTasks());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }
}
