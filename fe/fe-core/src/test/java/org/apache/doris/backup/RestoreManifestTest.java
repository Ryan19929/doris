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
import org.apache.doris.backup.BackupJobInfo.BackupIndexInfo;
import org.apache.doris.backup.BackupJobInfo.BackupOlapTableInfo;
import org.apache.doris.backup.BackupJobInfo.BackupPartitionInfo;
import org.apache.doris.backup.BackupJobInfo.ManifestEntry;
import org.apache.doris.backup.BackupJobInfo.TabletManifest;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.trees.plans.commands.BackupCommand.BackupContent;
import org.apache.doris.nereids.trees.plans.commands.ShowRestoreCommand;
import org.apache.doris.persist.EditLog;
import org.apache.doris.task.DownloadTask;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TDownloadReq;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TRemoteTabletSnapshot;
import org.apache.doris.thrift.TSnapshotFileStat;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTabletManifest;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class RestoreManifestTest {
    private static final long TABLET_1 = 101;
    private static final long TABLET_2 = 102;
    private static final String MD5 = "4f158689243a3d6030352fec3cfd3798";

    private final Env env = Mockito.mock(Env.class);
    private final InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
    private final EditLog editLog = Mockito.mock(EditLog.class);
    private MockedStatic<Env> mockedEnvStatic;
    private Database db;

    @BeforeEach
    public void setUp() throws Exception {
        db = CatalogMocker.mockDb();
        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnvJournalVersion).thenReturn(FeConstants.meta_version);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(catalog.getDbNullable(Mockito.anyLong())).thenReturn(db);
    }

    @AfterEach
    public void tearDown() {
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
    }

    private static String sha(char c) {
        return String.valueOf(c).repeat(64);
    }

    // A job info with one table, one partition, one index and the given tablets, each with files 1.dat, 1.idx and
    // <tablet>.hdr.
    private static BackupJobInfo newJobInfo(long... tabletIds) {
        BackupJobInfo jobInfo = new BackupJobInfo();
        jobInfo.name = "snapshot_1";
        jobInfo.dbName = "src_db";
        jobInfo.dbId = 1;
        jobInfo.backupTime = 1700000000000L;
        jobInfo.success = true;
        jobInfo.content = BackupContent.ALL;
        jobInfo.metaVersion = FeConstants.meta_version;
        BackupOlapTableInfo tblInfo = new BackupOlapTableInfo();
        tblInfo.id = 2;
        BackupPartitionInfo partInfo = new BackupPartitionInfo();
        partInfo.id = 3;
        partInfo.version = 10;
        BackupIndexInfo idxInfo = new BackupIndexInfo();
        idxInfo.id = 4;
        idxInfo.schemaHash = 5;
        for (long tabletId : tabletIds) {
            idxInfo.tablets.put(tabletId, Lists.newArrayList("1.dat." + MD5, "1.idx." + MD5,
                    tabletId + ".hdr." + MD5));
            idxInfo.tabletsOrder.add(tabletId);
        }
        partInfo.indexes.put("tbl", idxInfo);
        tblInfo.partitions.put("p1", partInfo);
        jobInfo.backupOlapTableObjects.put("tbl", tblInfo);
        return jobInfo;
    }

    private static SnapshotInfo snapshotInfo(long tabletId, List<ManifestEntry> fileStats) {
        SnapshotInfo info = new SnapshotInfo(1, 2, 3, 4, tabletId, 10001, 5, "/path",
                Lists.newArrayList("1.dat." + MD5, "1.idx." + MD5, tabletId + ".hdr." + MD5));
        info.setFileStats(fileStats);
        return info;
    }

    private static List<ManifestEntry> stats(long tabletId, boolean withDigest) {
        return Lists.newArrayList(new ManifestEntry(tabletId + ".hdr", 3, withDigest ? sha('c') : null),
                new ManifestEntry("1.idx", 20, withDigest ? sha('B') : null),
                new ManifestEntry("1.dat", 100, withDigest ? sha('a') : null));
    }

    private static Map<Long, SnapshotInfo> snapshotInfos(SnapshotInfo... infos) {
        Map<Long, SnapshotInfo> map = Maps.newHashMap();
        for (SnapshotInfo info : infos) {
            map.put(info.getTabletId(), info);
        }
        return map;
    }

    private static BackupJobInfo jobInfoWithManifest(long... tabletIds) {
        BackupJobInfo jobInfo = newJobInfo(tabletIds);
        SnapshotInfo[] infos = new SnapshotInfo[tabletIds.length];
        for (int i = 0; i < tabletIds.length; i++) {
            infos[i] = snapshotInfo(tabletIds[i], stats(tabletIds[i], true));
        }
        Assertions.assertTrue(jobInfo.buildManifest(snapshotInfos(infos), true, true));
        return jobInfo;
    }

    // ---- generating the manifest ----

    @Test
    public void testBuildManifest() {
        BackupJobInfo jobInfo = jobInfoWithManifest(TABLET_1, TABLET_2);
        Assertions.assertEquals(1, (int) jobInfo.manifestVersion);
        Assertions.assertEquals("sha256", jobInfo.digestAlgorithm);
        TabletManifest manifest = jobInfo.getTabletManifest(TABLET_1);
        // sorted by name, digests in lower case, no digest for the tablet meta file
        Assertions.assertEquals(Lists.newArrayList(new ManifestEntry("1.dat", 100, sha('a')),
                new ManifestEntry("1.idx", 20, sha('b')), new ManifestEntry("101.hdr", 3, null)), manifest.files);
        Assertions.assertEquals(TabletManifest.computeRoot(manifest.files), manifest.root);
        Assertions.assertEquals(64, manifest.root.length());
        Assertions.assertNotEquals(manifest.root, jobInfo.getTabletManifest(TABLET_2).root);
        Assertions.assertNull(jobInfo.getTabletManifest(999));

        // without root
        Assertions.assertTrue(jobInfo.buildManifest(snapshotInfos(snapshotInfo(TABLET_1, stats(TABLET_1, true)),
                snapshotInfo(TABLET_2, stats(TABLET_2, true))), true, false));
        Assertions.assertNull(jobInfo.getTabletManifest(TABLET_1).root);
    }

    @Test
    public void testBuildManifestWithoutDigest() {
        // http path without manifest_digest: sizes only
        BackupJobInfo jobInfo = newJobInfo(TABLET_1, TABLET_2);
        Assertions.assertTrue(jobInfo.buildManifest(snapshotInfos(snapshotInfo(TABLET_1, stats(TABLET_1, false)),
                snapshotInfo(TABLET_2, stats(TABLET_2, false))), true, true));
        Assertions.assertEquals("none", jobInfo.digestAlgorithm);

        // mixed: the digests are all or nothing
        Assertions.assertTrue(jobInfo.buildManifest(snapshotInfos(snapshotInfo(TABLET_1, stats(TABLET_1, true)),
                snapshotInfo(TABLET_2, stats(TABLET_2, false))), true, true));
        Assertions.assertEquals("none", jobInfo.digestAlgorithm);
        for (ManifestEntry entry : jobInfo.getTabletManifest(TABLET_1).files) {
            Assertions.assertNull(entry.digest);
        }
    }

    @Test
    public void testNoManifestIfAnyTabletLacksFileStats() {
        BackupJobInfo jobInfo = newJobInfo(TABLET_1, TABLET_2);
        // tablet 2 is on an old backend
        Assertions.assertFalse(jobInfo.buildManifest(snapshotInfos(snapshotInfo(TABLET_1, stats(TABLET_1, true)),
                snapshotInfo(TABLET_2, null)), true, true));
        Assertions.assertFalse(jobInfo.hasManifest());
        Assertions.assertNull(jobInfo.manifestVersion);
        Assertions.assertNull(jobInfo.digestAlgorithm);
        Assertions.assertNull(jobInfo.getTabletManifest(TABLET_1));
        Assertions.assertFalse(jobInfo.toJson(false).contains("manifest"));

        // the file stats do not match the snapshot files: one file less
        List<ManifestEntry> lessFiles = stats(TABLET_2, true);
        lessFiles.remove(0);
        Assertions.assertFalse(jobInfo.buildManifest(snapshotInfos(snapshotInfo(TABLET_1, stats(TABLET_1, true)),
                snapshotInfo(TABLET_2, lessFiles)), true, true));
        // a duplicated file
        List<ManifestEntry> duplicated = stats(TABLET_2, true);
        duplicated.add(new ManifestEntry("1.dat", 100, sha('a')));
        Assertions.assertFalse(jobInfo.buildManifest(snapshotInfos(snapshotInfo(TABLET_1, stats(TABLET_1, true)),
                snapshotInfo(TABLET_2, duplicated)), true, true));
        // a snapshot info is missing
        Assertions.assertFalse(jobInfo.buildManifest(snapshotInfos(snapshotInfo(TABLET_1, stats(TABLET_1, true))),
                true, true));

        // a successful build after a failed one
        Assertions.assertTrue(jobInfo.buildManifest(snapshotInfos(snapshotInfo(TABLET_1, stats(TABLET_1, true)),
                snapshotInfo(TABLET_2, stats(TABLET_2, true))), true, true));
        Assertions.assertTrue(jobInfo.hasManifest());

        // metadata only backup has no files
        jobInfo.content = BackupContent.METADATA_ONLY;
        Assertions.assertFalse(jobInfo.buildManifest(snapshotInfos(snapshotInfo(TABLET_1, stats(TABLET_1, true)),
                snapshotInfo(TABLET_2, stats(TABLET_2, true))), true, true));
        Assertions.assertFalse(jobInfo.hasManifest());
        Assertions.assertNull(jobInfo.getTabletManifest(TABLET_1));
    }

    @Test
    public void testBuildManifestLocalSnapshotFilesWithoutChecksum() {
        BackupJobInfo jobInfo = newJobInfo(TABLET_1);
        SnapshotInfo info = new SnapshotInfo(1, 2, 3, 4, TABLET_1, 10001, 5, "/path",
                Lists.newArrayList("1.dat", "1.idx", TABLET_1 + ".hdr"));
        info.setFileStats(stats(TABLET_1, false));
        Assertions.assertTrue(jobInfo.buildManifest(snapshotInfos(info), false, true));
        // treated as names with checksum, they do not match
        Assertions.assertFalse(jobInfo.buildManifest(snapshotInfos(info), true, true));
    }

    @Test
    public void testFileStatsFromThrift() {
        Assertions.assertNull(SnapshotInfo.fileStatsFromThrift(null));
        TSnapshotFileStat stat = new TSnapshotFileStat();
        stat.setName("1.dat");
        stat.setSize(10);
        stat.setSha256(sha('a'));
        TSnapshotFileStat noDigest = new TSnapshotFileStat();
        noDigest.setName("1.hdr");
        noDigest.setSize(1);
        Assertions.assertEquals(Lists.newArrayList(new ManifestEntry("1.dat", 10, sha('a')),
                new ManifestEntry("1.hdr", 1, null)),
                SnapshotInfo.fileStatsFromThrift(Lists.newArrayList(stat, noDigest)));
        TSnapshotFileStat noSize = new TSnapshotFileStat();
        noSize.setName("1.idx");
        Assertions.assertNull(SnapshotInfo.fileStatsFromThrift(Lists.newArrayList(stat, noSize)));
    }

    // ---- persisting ----

    @Test
    public void testGsonRoundTrip() {
        BackupJobInfo jobInfo = jobInfoWithManifest(TABLET_1, TABLET_2);
        String json = jobInfo.toJson(false);
        Assertions.assertTrue(json.contains("\"manifest_version\":1"), json);
        Assertions.assertTrue(json.contains("\"digest_algorithm\":\"sha256\""), json);
        Assertions.assertTrue(json.contains("\"tablet_manifests\":{"), json);
        Assertions.assertTrue(json.contains("{\"n\":\"1.dat\",\"s\":100,\"d\":\"" + sha('a') + "\"}"), json);
        // no digest key for the tablet meta file
        Assertions.assertTrue(json.contains("{\"n\":\"101.hdr\",\"s\":3}"), json);

        BackupJobInfo read = BackupJobInfo.genFromJson(json);
        Assertions.assertTrue(read.hasManifest());
        Assertions.assertEquals("sha256", read.digestAlgorithm);
        for (long tabletId : new long[] {TABLET_1, TABLET_2}) {
            TabletManifest expected = jobInfo.getTabletManifest(tabletId);
            TabletManifest actual = read.getTabletManifest(tabletId);
            Assertions.assertEquals(expected.files, actual.files);
            Assertions.assertEquals(expected.root, actual.root);
        }
        // the BackupTabletInfo view
        BackupIndexInfo idxInfo = read.backupOlapTableObjects.get("tbl").partitions.get("p1").indexes.get("tbl");
        Assertions.assertEquals(2, idxInfo.sortedTabletInfoList.size());
        Assertions.assertEquals(read.getTabletManifest(TABLET_1).files, idxInfo.sortedTabletInfoList.get(0).manifest);
        Assertions.assertEquals(read.getTabletManifest(TABLET_1).root,
                idxInfo.sortedTabletInfoList.get(0).manifestRoot);
        // written again, the same
        Assertions.assertEquals(json, read.toJson(false));

        // released after the restore: the manifest is gone, the version and algorithm are kept for SHOW RESTORE.
        read.releaseSnapshotInfo();
        Assertions.assertTrue(read.hasManifest());
        Assertions.assertNull(read.getTabletManifest(TABLET_1));
        Assertions.assertNull(idxInfo.sortedTabletInfoList.get(0).manifest);
        Assertions.assertFalse(read.toJson(false).contains("tablet_manifests"));
    }

    @Test
    public void testReadOldJobInfo() {
        // a job info written by an old version, without any manifest field.
        String json = "{\"name\":\"snapshot_1\",\"database\":\"src_db\",\"id\":1,\"backup_time\":1700000000000,"
                + "\"content\":\"ALL\",\"backup_objects\":{\"tbl\":{\"id\":2,\"partitions\":{\"p1\":{\"id\":3,"
                + "\"version\":10,\"indexes\":{\"tbl\":{\"id\":4,\"schema_hash\":5,\"tablets\":{\"101\":"
                + "[\"1.dat." + MD5 + "\",\"101.hdr." + MD5 + "\"]},\"tablets_order\":[101]}}}}}},"
                + "\"new_backup_objects\":{},\"backup_result\":\"succeed\",\"meta_version\":0,"
                + "\"tablet_be_map\":{},\"tablet_snapshot_path_map\":{}}";
        BackupJobInfo jobInfo = BackupJobInfo.genFromJson(json);
        Assertions.assertFalse(jobInfo.hasManifest());
        Assertions.assertNull(jobInfo.manifestVersion);
        Assertions.assertNull(jobInfo.digestAlgorithm);
        Assertions.assertNull(jobInfo.getTabletManifest(101));
        BackupIndexInfo idxInfo = jobInfo.backupOlapTableObjects.get("tbl").partitions.get("p1").indexes.get("tbl");
        Assertions.assertNull(idxInfo.tabletManifests);
        Assertions.assertNull(idxInfo.sortedTabletInfoList.get(0).manifest);
        Assertions.assertEquals(2, idxInfo.sortedTabletInfoList.get(0).files.size());
        Assertions.assertFalse(jobInfo.toJson(false).contains("manifest"));
    }

    @Test
    public void testUnknownManifestVersionIsNotChecked() {
        String json = jobInfoWithManifest(TABLET_1).toJson(false)
                .replace("\"manifest_version\":1", "\"manifest_version\":2");
        BackupJobInfo read = BackupJobInfo.genFromJson(json);
        Assertions.assertEquals(2, (int) read.manifestVersion);
        Assertions.assertFalse(read.hasManifest());
        Assertions.assertNull(read.getTabletManifest(TABLET_1));

        RestoreJob job = newRestoreJob(read);
        Assertions.assertNull(job.getExpectedTabletManifest(TABLET_1));
        Assertions.assertEquals(FeConstants.null_string, manifestCheckColumn(job));
    }

    // ---- restore ----

    private RestoreJob newRestoreJob(BackupJobInfo jobInfo) {
        return new RestoreJob("restore_label", "2024-01-01 00:00:00", db.getId(), db.getFullName(), jobInfo, false,
                new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false, false, false,
                false, false, env, 20000);
    }

    private static String manifestCheckColumn(RestoreJob job) {
        List<String> fullInfo = job.getFullInfo();
        Assertions.assertEquals(ShowRestoreCommand.TITLE_NAMES.size(), fullInfo.size());
        return fullInfo.get(ShowRestoreCommand.TITLE_NAMES.indexOf("ManifestCheck"));
    }

    @Test
    public void testShowRestoreColumn() {
        // appended after ReuseEstimate, at the end; SHOW BRIEF RESTORE is unchanged.
        int column = ShowRestoreCommand.TITLE_NAMES.indexOf("ManifestCheck");
        Assertions.assertEquals(ShowRestoreCommand.TITLE_NAMES.size() - 1, column);
        Assertions.assertEquals(column - 1, ShowRestoreCommand.TITLE_NAMES.indexOf("ReuseEstimate"));
        Assertions.assertFalse(ShowRestoreCommand.BRIEF_TITLE_NAMES.contains("ManifestCheck"));
        Assertions.assertFalse(ShowRestoreCommand.BRIEF_TITLE_NAMES.contains("ReuseEstimate"));

        // an old backup has no manifest
        RestoreJob job = newRestoreJob(newJobInfo(TABLET_1));
        Assertions.assertEquals(FeConstants.null_string, manifestCheckColumn(job));
        Assertions.assertNull(job.getExpectedTabletManifest(TABLET_1));
        Assertions.assertEquals(ShowRestoreCommand.BRIEF_TITLE_NAMES.size(), job.getBriefInfo().size());
    }

    @Test
    public void testExpectedManifestInDownloadTask() {
        RestoreJob job = newRestoreJob(jobInfoWithManifest(TABLET_1));
        TTabletManifest manifest = job.getExpectedTabletManifest(TABLET_1);
        Assertions.assertNotNull(manifest);
        Assertions.assertEquals(3, manifest.getFilesSize());
        TSnapshotFileStat dat = manifest.getFiles().get(0);
        Assertions.assertEquals("1.dat", dat.getName());
        Assertions.assertEquals(100, dat.getSize());
        Assertions.assertEquals(sha('a'), dat.getSha256());
        TSnapshotFileStat hdr = manifest.getFiles().get(2);
        Assertions.assertEquals("101.hdr", hdr.getName());
        Assertions.assertFalse(hdr.isSetSha256());
        Assertions.assertNull(job.getExpectedTabletManifest(999));

        // repository path: keyed by the src path, the same as src_dest_map
        Map<String, String> srcToDest = Maps.newHashMap();
        srcToDest.put("bos://repo/__101", "/local/snapshot/201/5");
        DownloadTask task = new DownloadTask(null, 10001, 1, 2, 3, srcToDest, new FsBroker("127.0.0.1", 8000),
                Maps.newHashMap(), StorageBackend.StorageType.BROKER, "bos://repo", "");
        TDownloadReq req = task.toThrift();
        Assertions.assertFalse(req.isSetExpectedFiles());
        Map<String, TTabletManifest> expected = Maps.newHashMap();
        expected.put("bos://repo/__101", manifest);
        task.setExpectedFiles(expected);
        req = task.toThrift();
        Assertions.assertTrue(req.isSetExpectedFiles());
        Assertions.assertEquals(manifest, req.getExpectedFiles().get("bos://repo/__101"));
        Assertions.assertEquals(req.getSrcDestMap().keySet(), req.getExpectedFiles().keySet());

        // http path: in the remote tablet snapshot
        TRemoteTabletSnapshot snapshot = new TRemoteTabletSnapshot();
        snapshot.setLocalTabletId(201);
        snapshot.setRemoteTabletId(TABLET_1);
        snapshot.setManifest(manifest);
        req = new DownloadTask(null, 10001, 1, 2, 3, Lists.newArrayList(snapshot)).toThrift();
        Assertions.assertFalse(req.isSetExpectedFiles());
        Assertions.assertEquals(manifest, req.getRemoteTabletSnapshots().get(0).getManifest());

        // not in cloud mode
        String origDeployMode = Config.deploy_mode;
        try {
            Config.deploy_mode = "cloud";
            Assertions.assertNull(job.getExpectedTabletManifest(TABLET_1));
        } finally {
            Config.deploy_mode = origDeployMode;
        }
    }

    private static TFinishTaskRequest downloadFinished(Boolean digestChecked, List<Long> verifiedTablets) {
        TFinishTaskRequest request = new TFinishTaskRequest(new TBackend("", 0, 1), TTaskType.DOWNLOAD, 1,
                new TStatus(TStatusCode.OK));
        if (verifiedTablets != null) {
            request.setManifestVerifiedTablets(verifiedTablets);
        }
        if (digestChecked != null) {
            request.setManifestDigestChecked(digestChecked);
        }
        return request;
    }

    @Test
    public void testManifestCheckResult() throws Exception {
        RestoreJob job = newRestoreJob(jobInfoWithManifest(TABLET_1, TABLET_2));
        // 4 replicas to download: tablet 201 and 202 on backend 1 and 2.
        for (long beId : new long[] {1, 2}) {
            for (long tabletId : new long[] {201, 202}) {
                job.snapshotInfos.put(tabletId, beId, new SnapshotInfo(1, 2, 3, 4, tabletId, beId, 5, "/p",
                        Lists.newArrayList()));
            }
        }
        Assertions.assertEquals("{\"version\":1,\"algo\":\"sha256\",\"verified_tablets\":0,\"unverified_tablets\":4,"
                + "\"digest_checked\":false}", manifestCheckColumn(job));

        // backend 1 is new and checked both tablets, with digest
        job.recordManifestCheck(1, downloadFinished(true, Lists.newArrayList(201L, 202L)));
        // reported again by a retried task, counted once
        job.recordManifestCheck(1, downloadFinished(true, Lists.newArrayList(201L)));
        RestoreManifestCheck check = job.computeManifestCheck();
        Assertions.assertEquals(2, check.getVerifiedTablets());
        Assertions.assertEquals(2, check.getUnverifiedTablets());
        Assertions.assertTrue(check.isDigestChecked());

        // backend 2 is old, reports nothing: unverified, never failed
        job.recordManifestCheck(2, downloadFinished(null, null));
        Assertions.assertEquals("{\"version\":1,\"algo\":\"sha256\",\"verified_tablets\":2,\"unverified_tablets\":2,"
                + "\"digest_checked\":true}", manifestCheckColumn(job));

        // backend 2 upgraded, checks without digest (restore_manifest_digest_check=false)
        job.recordManifestCheck(2, downloadFinished(false, Lists.newArrayList(201L, 202L)));
        String shown = "{\"version\":1,\"algo\":\"sha256\",\"verified_tablets\":4,\"unverified_tablets\":0,"
                + "\"digest_checked\":false}";
        Assertions.assertEquals(shown, manifestCheckColumn(job));

        // fixed when the download finishes, persisted with the job, then shown on followers and after restart.
        Deencapsulation.setField(job, "manifestCheck", job.computeManifestCheck());
        job.jobInfo.releaseSnapshotInfo();
        job.snapshotInfos.clear();
        Assertions.assertEquals(shown, manifestCheckColumn(job));
        RestoreJob readJob = writeAndRead(job);
        Assertions.assertEquals(shown, manifestCheckColumn(readJob));
    }

    @Test
    public void testManifestCheckOfRestoreJobWithoutManifest() throws Exception {
        // as persisted by an old version: no "mck", and the job info has no manifest.
        RestoreJob job = newRestoreJob(newJobInfo(TABLET_1));
        RestoreJob readJob = writeAndRead(job);
        Assertions.assertNull(readJob.getManifestCheck());
        Assertions.assertEquals(FeConstants.null_string, manifestCheckColumn(readJob));
    }

    private static RestoreJob writeAndRead(RestoreJob job) throws Exception {
        Path path = Files.createTempFile("restoreManifest", "tmp");
        try {
            try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path))) {
                job.write(out);
            }
            try (DataInputStream in = new DataInputStream(Files.newInputStream(path))) {
                return RestoreJob.read(in);
            }
        } finally {
            Files.delete(path);
        }
    }

    // ---- the size of the manifest ----

    /**
     * Measure how much a manifest adds to the job info, which is kept in FE memory and written to the edit log with
     * the restore job, to decide whether the manifest should be split into separate files. The file names and
     * sizes mimic real snapshots: rowset id v2 (48 hex chars) segment and index files, SHA-256 digests.
     */
    @Test
    public void testManifestSize() {
        int filesPerTablet = 30;
        int tablets = 10000;
        long[] withRoot = measureManifestSize(tablets, filesPerTablet, true, true);
        long[] noRoot = measureManifestSize(tablets, filesPerTablet, true, false);
        long[] noDigest = measureManifestSize(tablets, filesPerTablet, false, true);
        long[] half = measureManifestSize(tablets / 2, filesPerTablet, true, true);
        // grows linearly with the number of tablets
        long perTablet = (withRoot[1] - withRoot[0]) / tablets;
        long perTabletHalf = (half[1] - half[0]) / (tablets / 2);
        Assertions.assertTrue(Math.abs(perTablet - perTabletHalf) <= 2, perTablet + " vs " + perTabletHalf);
        System.out.printf("manifest size, %d files per tablet, job info bytes without manifest -> with manifest:%n",
                filesPerTablet);
        print("sha256 + root", tablets, withRoot);
        print("sha256, no root", tablets, noRoot);
        print("size only (http path default)", tablets, noDigest);
    }

    private static void print(String what, int tablets, long[] r) {
        long delta = r[1] - r[0];
        System.out.printf("  %s: %d tablets: %d -> %d bytes, +%d bytes (+%d per tablet, x%.1f); "
                        + "estimated +%.1f MB for 10k tablets, +%.1f MB for 100k tablets%n",
                what, tablets, r[0], r[1], delta, delta / tablets, (double) r[1] / r[0],
                delta * 10000.0 / tablets / 1024 / 1024, delta * 100000.0 / tablets / 1024 / 1024);
    }

    // Returns {bytes without manifest, bytes with manifest} of the job info json.
    private static long[] measureManifestSize(int tablets, int filesPerTablet, boolean withDigest, boolean withRoot) {
        BackupJobInfo jobInfo = new BackupJobInfo();
        jobInfo.name = "snapshot_1";
        jobInfo.dbName = "src_db";
        jobInfo.content = BackupContent.ALL;
        BackupOlapTableInfo tblInfo = new BackupOlapTableInfo();
        BackupPartitionInfo partInfo = new BackupPartitionInfo();
        BackupIndexInfo idxInfo = new BackupIndexInfo();
        partInfo.indexes.put("tbl", idxInfo);
        tblInfo.partitions.put("p1", partInfo);
        jobInfo.backupOlapTableObjects.put("tbl", tblInfo);
        Map<Long, SnapshotInfo> infos = Maps.newHashMap();
        long baseTabletId = 1_700_000_000L;
        for (int t = 0; t < tablets; t++) {
            long tabletId = baseTabletId + t;
            List<String> files = Lists.newArrayListWithCapacity(filesPerTablet);
            List<ManifestEntry> stats = Lists.newArrayListWithCapacity(filesPerTablet);
            files.add(tabletId + ".hdr." + MD5);
            stats.add(new ManifestEntry(tabletId + ".hdr", 4096 + t, null));
            for (int f = 1; f < filesPerTablet; f++) {
                String rowsetId = String.format("02%014x%032x", tabletId, (long) t * filesPerTablet + f);
                String name = rowsetId + "_" + (f % 4) + (f % 2 == 0 ? ".dat" : ".idx");
                files.add(name + "." + MD5);
                String digest = withDigest ? String.format("%064x", (long) t * 1000003 + f).replace('0', 'e') : null;
                stats.add(new ManifestEntry(name, 1_234_567_890L + f, digest));
            }
            idxInfo.tablets.put(tabletId, files);
            idxInfo.tabletsOrder.add(tabletId);
            SnapshotInfo info = new SnapshotInfo(1, 2, 3, 4, tabletId, 10001, 5, "/path", files);
            info.setFileStats(stats);
            infos.put(tabletId, info);
        }
        long without = jobInfo.toJson(false).getBytes(StandardCharsets.UTF_8).length;
        Assertions.assertTrue(jobInfo.buildManifest(infos, true, withRoot));
        long with = jobInfo.toJson(false).getBytes(StandardCharsets.UTF_8).length;
        return new long[] {without, with};
    }
}
