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

import org.apache.doris.backup.RestoreFileMapping.IdChain;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OdbcCatalogResource;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.View;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Version;
import org.apache.doris.info.TableRefInfo;
import org.apache.doris.nereids.trees.plans.commands.BackupCommand.BackupContent;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TSnapshotFileStat;
import org.apache.doris.thrift.TTabletManifest;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/*
 * This is a memory structure mapping the job info file in repository.
 * It contains all content of a job info file.
 * It also be used to save the info of a restore job, such as alias of table and meta info file path
 */
public class BackupJobInfo implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(BackupJobInfo.class);

    public static final int MANIFEST_VERSION = 1;
    public static final String DIGEST_SHA256 = "sha256";
    public static final String DIGEST_NONE = "none";

    @SerializedName("name")
    public String name;
    @SerializedName("database")
    public String dbName;
    @SerializedName("id")
    public long dbId;
    @SerializedName("backup_time")
    public long backupTime;
    @SerializedName("content")
    public BackupContent content;
    // only include olap table
    @SerializedName("backup_objects")
    public Map<String, BackupOlapTableInfo> backupOlapTableObjects = Maps.newHashMap();
    // include other objects: view, external table
    @SerializedName("new_backup_objects")
    public NewBackupObjects newBackupObjects = new NewBackupObjects();
    public boolean success;
    @SerializedName("backup_result")
    public String successJson = "succeed";

    @SerializedName("meta_version")
    public int metaVersion;
    @SerializedName("major_version")
    public int majorVersion;
    @SerializedName("minor_version")
    public int minorVersion;
    @SerializedName("patch_version")
    public int patchVersion;
    @SerializedName("is_force_replication_allocation")
    public boolean isForceReplicationAllocation;

    @SerializedName("tablet_be_map")
    public Map<Long, Long> tabletBeMap = Maps.newHashMap();

    @SerializedName("tablet_snapshot_path_map")
    public Map<Long, String> tabletSnapshotPathMap = Maps.newHashMap();

    @SerializedName("table_commit_seq_map")
    public Map<Long, Long> tableCommitSeqMap;

    // The version of the manifest (the expected files of each tablet snapshot, see TabletManifest).
    // Null (absent in the json) means there is no manifest, e.g. a backup by an old version, or some backend
    // did not report the file stats. Not written if null, so a job info without manifest is the same as before.
    @SerializedName("manifest_version")
    public Integer manifestVersion;
    // The digest algorithm of the files in the manifest, DIGEST_SHA256 or DIGEST_NONE.
    @SerializedName("digest_algorithm")
    public String digestAlgorithm;

    // tablet id -> manifest, built from the tablet manifests of all indexes on demand, not persisted.
    private Map<Long, TabletManifest> tabletManifestIndex;

    public static class ExtraInfo {
        public static class NetworkAddrss {
            @SerializedName("ip")
            public String ip;
            @SerializedName("port")
            public int port;
        }

        @SerializedName("be_network_map")
        public Map<Long, NetworkAddrss> beNetworkMap = Maps.newHashMap();

        @SerializedName("token")
        public String token;
    }

    @SerializedName("extra_info")
    public ExtraInfo extraInfo;


    // This map is used to save the table alias mapping info when processing a restore job.
    // origin -> alias
    @SerializedName("tblalias")
    public Map<String, String> tblAlias = Maps.newHashMap();

    public long getBackupTime() {
        return backupTime;
    }

    public void initBackupJobInfoAfterDeserialize() {
        // transform success
        if (successJson.equals("succeed")) {
            success = true;
        } else {
            success = false;
        }

        // init meta version
        if (metaVersion == 0) {
            // meta_version does not exist
            metaVersion = FeConstants.meta_version;
        }

        // init olap table info
        for (BackupOlapTableInfo backupOlapTableInfo : backupOlapTableObjects.values()) {
            for (BackupPartitionInfo backupPartitionInfo : backupOlapTableInfo.partitions.values()) {
                for (BackupIndexInfo backupIndexInfo : backupPartitionInfo.indexes.values()) {
                    List<Long> sortedTabletIds = backupIndexInfo.getSortedTabletIds();
                    for (Long tabletId : sortedTabletIds) {
                        List<String> files = backupIndexInfo.getTabletFiles(tabletId);
                        if (files == null) {
                            continue;
                        }
                        BackupTabletInfo backupTabletInfo = new BackupTabletInfo(tabletId, files);
                        TabletManifest manifest = backupIndexInfo.getTabletManifest(tabletId);
                        if (manifest != null) {
                            backupTabletInfo.manifest = manifest.files;
                            backupTabletInfo.manifestRoot = manifest.root;
                        }
                        backupIndexInfo.sortedTabletInfoList.add(backupTabletInfo);
                    }
                }
            }
        }
    }

    public Table.TableType getTypeByTblName(String tblName) {
        if (backupOlapTableObjects.containsKey(tblName)) {
            return Table.TableType.OLAP;
        }
        for (BackupViewInfo backupViewInfo : newBackupObjects.views) {
            if (backupViewInfo.name.equals(tblName)) {
                return Table.TableType.VIEW;
            }
        }
        for (BackupOdbcTableInfo backupOdbcTableInfo : newBackupObjects.odbcTables) {
            if (backupOdbcTableInfo.dorisTableName.equals(tblName)) {
                return Table.TableType.ODBC;
            }
        }
        return null;
    }

    public BackupOlapTableInfo getOlapTableInfo(String tblName) {
        return backupOlapTableObjects.get(tblName);
    }

    public void removeTable(TableRefInfo tableRefInfo, TableType tableType) {
        switch (tableType) {
            case OLAP:
                removeOlapTable(tableRefInfo);
                break;
            case VIEW:
                removeView(tableRefInfo);
                break;
            case ODBC:
                removeOdbcTable(tableRefInfo);
                break;
            default:
                break;
        }
    }

    public void removeOlapTable(TableRefInfo tableRefInfo) {
        String tblName = tableRefInfo.getTableNameInfo().getTbl();
        BackupOlapTableInfo tblInfo = backupOlapTableObjects.get(tblName);
        if (tblInfo == null) {
            LOG.info("Ignore error: exclude table " + tblName + " does not exist in snapshot " + name);
            return;
        }
        PartitionNamesInfo partitionNamesInfo = tableRefInfo.getPartitionNamesInfo();
        if (partitionNamesInfo == null) {
            backupOlapTableObjects.remove(tblInfo);
            return;
        }
        // check the selected partitions
        for (String partName : partitionNamesInfo.getPartitionNames()) {
            if (tblInfo.containsPart(partName)) {
                tblInfo.partitions.remove(partName);
            } else {
                LOG.info("Ignore error: exclude partition " + partName + " of table " + tblName
                        + " does not exist in snapshot");
            }
        }
    }

    public void removeView(TableRefInfo tableRefInfo) {
        Iterator<BackupViewInfo> iter = newBackupObjects.views.listIterator();
        while (iter.hasNext()) {
            if (iter.next().name.equals(tableRefInfo.getTableNameInfo().getTbl())) {
                iter.remove();
                return;
            }
        }
    }

    public void removeOdbcTable(TableRefInfo tableRefInfo) {
        Iterator<BackupOdbcTableInfo> iter = newBackupObjects.odbcTables.listIterator();
        while (iter.hasNext()) {
            BackupOdbcTableInfo backupOdbcTableInfo = iter.next();
            if (backupOdbcTableInfo.dorisTableName.equals(tableRefInfo.getTableNameInfo().getTbl())) {
                if (backupOdbcTableInfo.resourceName != null) {
                    Iterator<BackupOdbcResourceInfo> resourceIter = newBackupObjects.odbcResources.listIterator();
                    while (resourceIter.hasNext()) {
                        if (resourceIter.next().name.equals(backupOdbcTableInfo.resourceName)) {
                            resourceIter.remove();
                        }
                    }
                }
                iter.remove();
                return;
            }
        }
    }

    public void retainOlapTables(Set<String> tblNames) {
        Iterator<Map.Entry<String, BackupOlapTableInfo>> iter = backupOlapTableObjects.entrySet().iterator();
        while (iter.hasNext()) {
            if (!tblNames.contains(iter.next().getKey())) {
                iter.remove();
            }
        }
    }

    public void retainView(Set<String> viewNames) {
        Iterator<BackupViewInfo> iter = newBackupObjects.views.listIterator();
        while (iter.hasNext()) {
            if (!viewNames.contains(iter.next().name)) {
                iter.remove();
            }
        }
    }

    public void retainOdbcTables(Set<String> odbcTableNames) {
        Iterator<BackupOdbcTableInfo> odbcIter = newBackupObjects.odbcTables.listIterator();
        Set<String> retainOdbcResourceNames = Sets.newHashSet();
        while (odbcIter.hasNext()) {
            BackupOdbcTableInfo backupOdbcTableInfo = odbcIter.next();
            if (!odbcTableNames.contains(backupOdbcTableInfo.dorisTableName)) {
                odbcIter.remove();
            } else {
                retainOdbcResourceNames.add(backupOdbcTableInfo.resourceName);
            }
        }
        Iterator<BackupOdbcResourceInfo> resourceIter = newBackupObjects.odbcResources.listIterator();
        while (resourceIter.hasNext()) {
            if (!retainOdbcResourceNames.contains(resourceIter.next().name)) {
                resourceIter.remove();
            }
        }
    }

    public void setAlias(String orig, String alias) {
        tblAlias.put(orig, alias);
    }

    public String getAliasByOriginNameIfSet(String orig) {
        return tblAlias.containsKey(orig) ? tblAlias.get(orig) : orig;
    }

    public String getOrginNameByAlias(String alias) {
        for (Map.Entry<String, String> entry : tblAlias.entrySet()) {
            if (entry.getValue().equals(alias)) {
                return entry.getKey();
            }
        }
        return alias;
    }

    public static class BriefBackupJobInfo {
        @SerializedName("name")
        public String name;
        @SerializedName("database")
        public String database;
        @SerializedName("backup_time")
        public long backupTime;
        @SerializedName("content")
        public BackupContent content;
        @SerializedName("olap_table_list")
        public List<BriefBackupOlapTable> olapTableList = Lists.newArrayList();
        @SerializedName("view_list")
        public List<BackupViewInfo> viewList = Lists.newArrayList();
        @SerializedName("odbc_table_list")
        public List<BackupOdbcTableInfo> odbcTableList = Lists.newArrayList();
        @SerializedName("odbc_resource_list")
        public List<BackupOdbcResourceInfo> odbcResourceList = Lists.newArrayList();

        public static BriefBackupJobInfo fromBackupJobInfo(BackupJobInfo backupJobInfo) {
            BriefBackupJobInfo briefBackupJobInfo = new BriefBackupJobInfo();
            briefBackupJobInfo.name = backupJobInfo.name;
            briefBackupJobInfo.database = backupJobInfo.dbName;
            briefBackupJobInfo.backupTime = backupJobInfo.backupTime;
            briefBackupJobInfo.content = backupJobInfo.content;
            for (Map.Entry<String, BackupOlapTableInfo> olapTableEntry :
                    backupJobInfo.backupOlapTableObjects.entrySet()) {
                BriefBackupOlapTable briefBackupOlapTable = new BriefBackupOlapTable();
                briefBackupOlapTable.name = olapTableEntry.getKey();
                briefBackupOlapTable.partitionNames = Lists.newArrayList(olapTableEntry.getValue().partitions.keySet());
                briefBackupJobInfo.olapTableList.add(briefBackupOlapTable);
            }
            briefBackupJobInfo.viewList = backupJobInfo.newBackupObjects.views;
            briefBackupJobInfo.odbcTableList = backupJobInfo.newBackupObjects.odbcTables;
            briefBackupJobInfo.odbcResourceList = backupJobInfo.newBackupObjects.odbcResources;
            return briefBackupJobInfo;
        }
    }

    public static class BriefBackupOlapTable {
        @SerializedName("name")
        public String name;
        @SerializedName("partition_names")
        public List<String> partitionNames;
    }

    public static class NewBackupObjects {
        @SerializedName("views")
        public List<BackupViewInfo> views = Lists.newArrayList();
        @SerializedName("odbc_tables")
        public List<BackupOdbcTableInfo> odbcTables = Lists.newArrayList();
        @SerializedName("odbc_resources")
        public List<BackupOdbcResourceInfo> odbcResources = Lists.newArrayList();
    }

    public static class BackupOlapTableInfo {
        @SerializedName("id")
        public long id;
        @SerializedName("partitions")
        public Map<String, BackupPartitionInfo> partitions = Maps.newHashMap();

        public boolean containsPart(String partName) {
            return partitions.containsKey(partName);
        }

        public BackupPartitionInfo getPartInfo(String partName) {
            return partitions.get(partName);
        }

        public void retainPartitions(Collection<String> partNames) {
            if (partNames == null || partNames.isEmpty()) {
                // retain all
                return;
            }
            Iterator<Map.Entry<String, BackupPartitionInfo>> iter = partitions.entrySet().iterator();
            while (iter.hasNext()) {
                if (!partNames.contains(iter.next().getKey())) {
                    iter.remove();
                }
            }
        }
    }

    public static class BackupPartitionInfo {
        @SerializedName("id")
        public long id;
        @SerializedName("version")
        public long version;
        @SerializedName("indexes")
        public Map<String, BackupIndexInfo> indexes = Maps.newHashMap();

        public BackupIndexInfo getIdx(String idxName) {
            return indexes.get(idxName);
        }
    }

    public static class BackupIndexInfo {
        @SerializedName("id")
        public long id;
        @SerializedName("schema_hash")
        public int schemaHash;
        @SerializedName("tablets")
        public Map<Long, List<String>> tablets = Maps.newHashMap();
        @SerializedName("tablets_order")
        public List<Long> tabletsOrder = Lists.newArrayList();
        // tablet id -> the manifest of the tablet snapshot, next to "tablets". Null if the backup has no manifest.
        @SerializedName("tablet_manifests")
        public Map<Long, TabletManifest> tabletManifests;
        public List<BackupTabletInfo> sortedTabletInfoList = Lists.newArrayList();

        public List<String> getTabletFiles(long tabletId) {
            return tablets.get(tabletId);
        }

        public TabletManifest getTabletManifest(long tabletId) {
            return tabletManifests == null ? null : tabletManifests.get(tabletId);
        }

        private List<Long> getSortedTabletIds() {
            if (tabletsOrder == null || tabletsOrder.isEmpty()) {
                // in previous version, we are not saving tablets order(which was a BUG),
                // so we have to sort the tabletIds to restore the original order of tablets.
                List<Long> tmpList = Lists.newArrayList(tablets.keySet());
                tmpList.sort((o1, o2) -> Long.valueOf(o1).compareTo(Long.valueOf(o2)));
                return tmpList;
            } else {
                return tabletsOrder;
            }
        }
    }

    public static class BackupTabletInfo {
        @SerializedName("id")
        public long id;
        @SerializedName("files")
        public List<String> files;
        // The expected files of the tablet snapshot, sorted by name, null if the backup has no manifest.
        // Persisted in BackupIndexInfo.tabletManifests, this is a view of it.
        public List<ManifestEntry> manifest;
        // optional, see TabletManifest.root
        public String manifestRoot;

        public BackupTabletInfo(long id, List<String> files) {
            this.id = id;
            this.files = files;
        }
    }

    /**
     * A file of a tablet snapshot in the manifest. The short names keep the manifest small, a backup may
     * have millions of files.
     */
    public static class ManifestEntry {
        // file name in the source tablet snapshot dir, without the md5 suffix used in the repository.
        @SerializedName("n")
        public String name;
        @SerializedName("s")
        public long size;
        // hex encoded digest by the digest_algorithm of the job, absent if not computed.
        // Never recorded for the tablet meta file (.hdr), which is rewritten in restore.
        @SerializedName("d")
        public String digest;

        public ManifestEntry() {
            // for persist
        }

        public ManifestEntry(String name, long size, String digest) {
            this.name = name;
            this.size = size;
            this.digest = digest;
        }

        public TSnapshotFileStat toThrift() {
            TSnapshotFileStat stat = new TSnapshotFileStat();
            stat.setName(name);
            stat.setSize(size);
            if (digest != null) {
                stat.setSha256(digest);
            }
            return stat;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ManifestEntry)) {
                return false;
            }
            ManifestEntry that = (ManifestEntry) o;
            return size == that.size && Objects.equals(name, that.name) && Objects.equals(digest, that.digest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, size, digest);
        }

        @Override
        public String toString() {
            return name + ":" + size + (digest == null ? "" : ":" + digest);
        }
    }

    /**
     * The manifest of a tablet snapshot: the complete list of its files, sorted by name.
     */
    public static class TabletManifest {
        @SerializedName("files")
        public List<ManifestEntry> files = Lists.newArrayList();
        // SHA-256 of the sorted (name, size, digest) of all files, see computeRoot(). Optional, the entries
        // are the authority.
        @SerializedName("root")
        public String root;

        public TabletManifest() {
            // for persist
        }

        public TabletManifest(List<ManifestEntry> files, boolean withRoot) {
            this.files = files;
            this.root = withRoot ? computeRoot(files) : null;
        }

        public TTabletManifest toThrift() {
            TTabletManifest manifest = new TTabletManifest();
            List<TSnapshotFileStat> stats = Lists.newArrayListWithCapacity(files.size());
            for (ManifestEntry entry : files) {
                stats.add(entry.toThrift());
            }
            manifest.setFiles(stats);
            return manifest;
        }

        // One line "name \t size \t digest \n" for each file in the given order, digest is empty if absent.
        public static String computeRoot(List<ManifestEntry> files) {
            try {
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                for (ManifestEntry entry : files) {
                    String line = entry.name + "\t" + entry.size + "\t" + (entry.digest == null ? "" : entry.digest)
                            + "\n";
                    md.update(line.getBytes(StandardCharsets.UTF_8));
                }
                return Hex.encodeHexString(md.digest());
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public static class BackupViewInfo {
        @SerializedName("id")
        public long id;
        @SerializedName("name")
        public String name;
    }

    public static class BackupOdbcTableInfo {
        @SerializedName("id")
        public long id;
        @SerializedName("doris_table_name")
        public String dorisTableName;
        @SerializedName("linked_odbc_database_name")
        public String linkedOdbcDatabaseName;
        @SerializedName("linked_odbc_table_name")
        public String linkedOdbcTableName;
        @SerializedName("resource_name")
        public String resourceName;
        @SerializedName("host")
        public String host;
        @SerializedName("port")
        public String port;
        @SerializedName("user")
        public String user;
        @SerializedName("driver")
        public String driver;
        @SerializedName("odbc_type")
        public String odbcType;
    }

    public static class BackupOdbcResourceInfo {
        @SerializedName("name")
        public String name;
    }

    // eg: __db_10001/__tbl_10002/__part_10003/__idx_10002/__10004
    public String getFilePath(String db, String tbl, String part, String idx, long tabletId) {
        if (!db.equalsIgnoreCase(dbName)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("db name does not equal: {}-{}", dbName, db);
            }
            return null;
        }

        BackupOlapTableInfo tblInfo = backupOlapTableObjects.get(tbl);
        if (tblInfo == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("tbl {} does not exist", tbl);
            }
            return null;
        }

        BackupPartitionInfo partInfo = tblInfo.getPartInfo(part);
        if (partInfo == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("part {} does not exist", part);
            }
            return null;
        }

        BackupIndexInfo idxInfo = partInfo.getIdx(idx);
        if (idxInfo == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("idx {} does not exist", idx);
            }
            return null;
        }

        List<String> pathSeg = Lists.newArrayList();
        pathSeg.add(Repository.PREFIX_DB + dbId);
        pathSeg.add(Repository.PREFIX_TBL + tblInfo.id);
        pathSeg.add(Repository.PREFIX_PART + partInfo.id);
        pathSeg.add(Repository.PREFIX_IDX + idxInfo.id);
        pathSeg.add(Repository.PREFIX_COMMON + tabletId);

        return Joiner.on("/").join(pathSeg);
    }

    // eg: __db_10001/__tbl_10002/__part_10003/__idx_10002/__10004
    public String getFilePath(IdChain ids) {
        List<String> pathSeg = Lists.newArrayList();
        pathSeg.add(Repository.PREFIX_DB + dbId);
        pathSeg.add(Repository.PREFIX_TBL + ids.getTblId());
        pathSeg.add(Repository.PREFIX_PART + ids.getPartId());
        pathSeg.add(Repository.PREFIX_IDX + ids.getIdxId());
        pathSeg.add(Repository.PREFIX_COMMON + ids.getTabletId());

        return Joiner.on("/").join(pathSeg);
    }

    // struct TRemoteTabletSnapshot {
    //     1: optional i64 local_tablet_id
    //     2: optional string local_snapshot_path
    //     3: optional i64 remote_tablet_id
    //     4: optional i64 remote_be_id
    //     5: optional Types.TSchemaHash schema_hash
    //     6: optional Types.TNetworkAddress remote_be_addr
    //     7: optional string remote_snapshot_path
    //     8: optional string token
    // }

    public String getTabletSnapshotPath(Long tabletId) {
        return tabletSnapshotPathMap.get(tabletId);
    }

    public Long getBeId(Long tabletId) {
        return tabletBeMap.get(tabletId);
    }

    public String getToken() {
        return extraInfo.token;
    }

    public TNetworkAddress getBeAddr(Long beId) {
        ExtraInfo.NetworkAddrss addr = extraInfo.beNetworkMap.get(beId);
        if (addr == null) {
            return null;
        }

        return new TNetworkAddress(addr.ip, addr.port);
    }

    // TODO(Drogon): improve this find perfermance
    public Long getSchemaHash(long tableId, long partitionId, long indexId) {
        for (BackupOlapTableInfo backupOlapTableInfo : backupOlapTableObjects.values()) {
            if (backupOlapTableInfo.id != tableId) {
                continue;
            }

            for (BackupPartitionInfo backupPartitionInfo : backupOlapTableInfo.partitions.values()) {
                if (backupPartitionInfo.id != partitionId) {
                    continue;
                }

                for (BackupIndexInfo backupIndexInfo : backupPartitionInfo.indexes.values()) {
                    if (backupIndexInfo.id != indexId) {
                        continue;
                    }

                    return Long.valueOf(backupIndexInfo.schemaHash);
                }
            }
        }
        return null;
    }

    public static BackupJobInfo fromCatalog(long backupTime, String label, String dbName, long dbId,
                                            BackupContent content, BackupMeta backupMeta,
                                            Map<Long, SnapshotInfo> snapshotInfos, Map<Long, Long> tableCommitSeqMap) {

        BackupJobInfo jobInfo = new BackupJobInfo();
        jobInfo.backupTime = backupTime;
        jobInfo.name = label;
        jobInfo.dbName = dbName;
        jobInfo.dbId = dbId;
        jobInfo.metaVersion = FeConstants.meta_version;
        jobInfo.content = content;
        jobInfo.tableCommitSeqMap = tableCommitSeqMap;
        jobInfo.majorVersion = Version.DORIS_BUILD_VERSION_MAJOR;
        jobInfo.minorVersion = Version.DORIS_BUILD_VERSION_MINOR;
        jobInfo.patchVersion = Version.DORIS_BUILD_VERSION_PATCH;
        jobInfo.isForceReplicationAllocation = !Config.force_olap_table_replication_allocation.isEmpty();

        Collection<Table> tbls = backupMeta.getTables().values();
        // tbls
        for (Table tbl : tbls) {
            if (tbl instanceof OlapTable) {
                OlapTable olapTbl = (OlapTable) tbl;
                BackupOlapTableInfo tableInfo = new BackupOlapTableInfo();
                tableInfo.id = tbl.getId();
                jobInfo.backupOlapTableObjects.put(tbl.getName(), tableInfo);
                // partitions
                for (Partition partition : olapTbl.getPartitions()) {
                    BackupPartitionInfo partitionInfo = new BackupPartitionInfo();
                    partitionInfo.id = partition.getId();
                    partitionInfo.version = partition.getVisibleVersion();
                    tableInfo.partitions.put(partition.getName(), partitionInfo);
                    // indexes
                    for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        BackupIndexInfo idxInfo = new BackupIndexInfo();
                        idxInfo.id = index.getId();
                        idxInfo.schemaHash = olapTbl.getSchemaHashByIndexId(index.getId());
                        partitionInfo.indexes.put(olapTbl.getIndexNameById(index.getId()), idxInfo);
                        // tablets
                        if (content == BackupContent.METADATA_ONLY) {
                            for (Tablet tablet : index.getTablets()) {
                                idxInfo.tablets.put(tablet.getId(), Lists.newArrayList());
                            }
                        } else {
                            for (Tablet tablet : index.getTablets()) {
                                SnapshotInfo snapshotInfo = snapshotInfos.get(tablet.getId());
                                idxInfo.tablets.put(tablet.getId(),
                                        Lists.newArrayList(snapshotInfo.getFiles()));
                                jobInfo.tabletBeMap.put(tablet.getId(), snapshotInfo.getBeId());
                                jobInfo.tabletSnapshotPathMap.put(tablet.getId(), snapshotInfo.getPath());
                            }
                        }
                        idxInfo.tabletsOrder.addAll(index.getTabletIdsInOrder());
                    }
                }
            } else if (tbl instanceof View) {
                View view = (View) tbl;
                BackupViewInfo backupViewInfo = new BackupViewInfo();
                backupViewInfo.id = view.getId();
                backupViewInfo.name = view.getName();
                jobInfo.newBackupObjects.views.add(backupViewInfo);
            } else if (tbl instanceof OdbcTable) {
                // ODBC tables are deprecated. We still record their name/id for backup metadata
                // compatibility, but no longer read properties from the table.
                BackupOdbcTableInfo backupOdbcTableInfo = new BackupOdbcTableInfo();
                backupOdbcTableInfo.id = tbl.getId();
                backupOdbcTableInfo.dorisTableName = tbl.getName();
                jobInfo.newBackupObjects.odbcTables.add(backupOdbcTableInfo);
            }
        }
        // resources
        Collection<Resource> resources = backupMeta.getResourceNameMap().values();
        for (Resource resource : resources) {
            if (resource instanceof OdbcCatalogResource) {
                OdbcCatalogResource odbcCatalogResource = (OdbcCatalogResource) resource;
                BackupOdbcResourceInfo backupOdbcResourceInfo = new BackupOdbcResourceInfo();
                backupOdbcResourceInfo.name = odbcCatalogResource.getName();
                jobInfo.newBackupObjects.odbcResources.add(backupOdbcResourceInfo);
            }
        }

        return jobInfo;
    }

    /**
     * Build the manifest of all the tablets from the file stats reported by the backends, see
     * SnapshotInfo.getFileStats(). All or nothing: if any tablet has no file stats (e.g. an old backend), or the
     * file stats do not match the snapshot files, no manifest is written for the whole job, the same as a backup
     * of an old version.
     *
     * @param snapshotInfos tablet id -> snapshot info
     * @param filesWithChecksum whether the file names in the snapshot infos have the md5 suffix (remote repository)
     * @param withRoot whether to record the manifest root of each tablet
     * @return true if the manifest is written
     */
    public boolean buildManifest(Map<Long, SnapshotInfo> snapshotInfos, boolean filesWithChecksum,
            boolean withRoot) {
        clearManifest();
        if (content == BackupContent.METADATA_ONLY) {
            return false;
        }

        // index info -> tablet id -> sorted entries
        Map<BackupIndexInfo, Map<Long, List<ManifestEntry>>> built = Maps.newHashMap();
        boolean allDigested = true;
        for (BackupOlapTableInfo tblInfo : backupOlapTableObjects.values()) {
            for (BackupPartitionInfo partInfo : tblInfo.partitions.values()) {
                for (BackupIndexInfo idxInfo : partInfo.indexes.values()) {
                    Map<Long, List<ManifestEntry>> idxManifests = Maps.newHashMap();
                    built.put(idxInfo, idxManifests);
                    for (Long tabletId : idxInfo.tablets.keySet()) {
                        SnapshotInfo info = snapshotInfos.get(tabletId);
                        List<ManifestEntry> entries = info == null ? null
                                : toManifestEntries(info, filesWithChecksum);
                        if (entries == null) {
                            LOG.info("no manifest for backup {}, tablet {} has no valid file stats", name, tabletId);
                            return false;
                        }
                        for (ManifestEntry entry : entries) {
                            if (entry.digest == null && !isTabletMetaFile(entry.name)) {
                                allDigested = false;
                            }
                        }
                        idxManifests.put(tabletId, entries);
                    }
                }
            }
        }

        for (Map.Entry<BackupIndexInfo, Map<Long, List<ManifestEntry>>> idxEntry : built.entrySet()) {
            Map<Long, TabletManifest> tabletManifests = Maps.newHashMap();
            for (Map.Entry<Long, List<ManifestEntry>> tabletEntry : idxEntry.getValue().entrySet()) {
                List<ManifestEntry> entries = tabletEntry.getValue();
                if (!allDigested) {
                    // keep the manifest uniform, the digests are all or nothing too.
                    for (ManifestEntry entry : entries) {
                        entry.digest = null;
                    }
                }
                tabletManifests.put(tabletEntry.getKey(), new TabletManifest(entries, withRoot));
            }
            idxEntry.getKey().tabletManifests = tabletManifests;
        }
        manifestVersion = MANIFEST_VERSION;
        digestAlgorithm = allDigested ? DIGEST_SHA256 : DIGEST_NONE;
        tabletManifestIndex = null;
        return true;
    }

    // Returns the entries sorted by name, or null if the file stats are absent or do not match the files.
    private static List<ManifestEntry> toManifestEntries(SnapshotInfo info, boolean filesWithChecksum) {
        List<ManifestEntry> stats = info.getFileStats();
        if (stats == null || info.getFiles() == null) {
            return null;
        }
        Set<String> expectedNames = Sets.newHashSet();
        for (String file : info.getFiles()) {
            if (filesWithChecksum) {
                Pair<String, String> decoded = Repository.decodeFileNameWithChecksum(file);
                if (decoded == null) {
                    return null;
                }
                expectedNames.add(decoded.first);
            } else {
                expectedNames.add(file);
            }
        }
        List<ManifestEntry> entries = Lists.newArrayListWithCapacity(stats.size());
        Set<String> names = Sets.newHashSet();
        for (ManifestEntry stat : stats) {
            if (stat == null || stat.name == null || stat.size < 0 || !names.add(stat.name)) {
                return null;
            }
            String digest = isTabletMetaFile(stat.name) || stat.digest == null || stat.digest.isEmpty()
                    ? null : stat.digest.toLowerCase();
            entries.add(new ManifestEntry(stat.name, stat.size, digest));
        }
        if (!names.equals(expectedNames)) {
            return null;
        }
        entries.sort((a, b) -> a.name.compareTo(b.name));
        return entries;
    }

    public static boolean isTabletMetaFile(String name) {
        return name.endsWith(".hdr");
    }

    /**
     * Whether this backup has a manifest that this version can check. A manifest of an unknown (newer) version
     * is not checked, never fails a restore.
     */
    public boolean hasManifest() {
        return manifestVersion != null && manifestVersion == MANIFEST_VERSION;
    }

    // Returns the manifest of a tablet in the backup, null if absent.
    public TabletManifest getTabletManifest(long tabletId) {
        if (!hasManifest()) {
            return null;
        }
        if (tabletManifestIndex == null) {
            Map<Long, TabletManifest> index = Maps.newHashMap();
            for (BackupOlapTableInfo tblInfo : backupOlapTableObjects.values()) {
                if (tblInfo == null) {
                    continue;
                }
                for (BackupPartitionInfo partInfo : tblInfo.partitions.values()) {
                    for (BackupIndexInfo idxInfo : partInfo.indexes.values()) {
                        if (idxInfo.tabletManifests != null) {
                            index.putAll(idxInfo.tabletManifests);
                        }
                    }
                }
            }
            tabletManifestIndex = index;
        }
        return tabletManifestIndex.get(tabletId);
    }

    private void clearManifest() {
        manifestVersion = null;
        digestAlgorithm = null;
        tabletManifestIndex = null;
        for (BackupOlapTableInfo tblInfo : backupOlapTableObjects.values()) {
            for (BackupPartitionInfo partInfo : tblInfo.partitions.values()) {
                for (BackupIndexInfo idxInfo : partInfo.indexes.values()) {
                    idxInfo.tabletManifests = null;
                }
            }
        }
    }

    public static BackupJobInfo fromFile(String path) throws IOException {
        byte[] bytes = Files.readAllBytes(Paths.get(path));
        String json = new String(bytes, StandardCharsets.UTF_8);
        return genFromJson(json);
    }

    public static BackupJobInfo genFromJson(String json) {
        /* parse the json string:
         * {
         *   "backup_time": 1522231864000,
         *   "name": "snapshot1",
         *   "database": "db1"
         *   "id": 10000
         *   "backup_result": "succeed",
         *   "meta_version" : 40 // this is optional
         *   "backup_objects": {
         *       "table1": {
         *           "partitions": {
         *               "partition2": {
         *                   "indexes": {
         *                       "rollup1": {
         *                           "id": 10009,
         *                           "schema_hash": 3473401,
         *                           "tablets": {
         *                               "10008": ["__10029_seg1.dat", "__10029_seg2.dat"],
         *                               "10007": ["__10030_seg1.dat", "__10030_seg2.dat"]
         *                           },
         *                           "tablets_order": ["10007", "10008"]
         *                       },
         *                       "table1": {
         *                           "id": 10008,
         *                           "schema_hash": 9845021,
         *                           "tablets": {
         *                               "10004": ["__10027_seg1.dat", "__10027_seg2.dat"],
         *                               "10005": ["__10028_seg1.dat", "__10028_seg2.dat"]
         *                           },
         *                           "tablets_order": ["10004, "10005"]
         *                       }
         *                   },
         *                   "id": 10007
         *                   "version": 10
         *                   "version_hash": 1273047329538
         *               },
         *           },
         *           "id": 10001
         *       }
         *   },
         *   "new_backup_objects": {
         *       "views": [
         *           {"id": 1,
         *            "name": "view1"
         *           }
         *       ],
         *       "odbc_tables": [
         *           {"id": 2,
         *            "doris_table_name": "oracle1",
         *            "linked_odbc_database_name": "external_db1",
         *            "linked_odbc_table_name": "external_table1",
         *            "resource_name": "bj_oracle"
         *           }
         *       ],
         *       "odbc_resources": [
         *           {"name": "bj_oracle"}
         *       ]
         *   }
         * }
         */
        BackupJobInfo jobInfo = GsonUtils.GSON.fromJson(json, BackupJobInfo.class);
        return jobInfo;
    }

    public static BackupJobInfo fromInputStream(InputStream inputStream) throws IOException {
        try (InputStreamReader reader = new InputStreamReader(inputStream)) {
            return GsonUtils.GSON.fromJson(reader, BackupJobInfo.class);
        }
    }

    public void writeToFile(File jobInfoFile) throws FileNotFoundException {
        PrintWriter printWriter = new PrintWriter(jobInfoFile);
        try {
            printWriter.print(toJson(false));
            printWriter.flush();
        } finally {
            printWriter.close();
        }
    }

    // Only return basic info, table and partitions
    public String getBrief() {
        BriefBackupJobInfo briefBackupJobInfo = BriefBackupJobInfo.fromBackupJobInfo(this);
        Gson gson = GsonUtils.GSON_PRETTY_PRINTING;
        return gson.toJson(briefBackupJobInfo);
    }

    public String toJson(boolean prettyPrinting) {
        Gson gson;
        if (prettyPrinting) {
            gson = GsonUtils.GSON_PRETTY_PRINTING;
        } else {
            gson = GsonUtils.GSON;
        }
        return gson.toJson(this);
    }

    public String getInfo() {
        return getBrief();
    }

    public void releaseSnapshotInfo() {
        tabletBeMap.clear();
        tabletSnapshotPathMap.clear();
        for (BackupOlapTableInfo tableInfo : backupOlapTableObjects.values()) {
            for (BackupPartitionInfo partInfo : tableInfo.partitions.values()) {
                for (BackupIndexInfo indexInfo : partInfo.indexes.values()) {
                    for (BackupTabletInfo tabletInfo : indexInfo.sortedTabletInfoList) {
                        tabletInfo.files.clear();
                        tabletInfo.manifest = null;
                        tabletInfo.manifestRoot = null;
                    }
                    // The manifest is only needed for downloading. Keep manifest_version and digest_algorithm,
                    // they are shown in SHOW RESTORE.
                    indexInfo.tabletManifests = null;
                }
            }
        }
        tabletManifestIndex = null;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        initBackupJobInfoAfterDeserialize();
    }

    public String toString() {
        return toJson(true);
    }
}
