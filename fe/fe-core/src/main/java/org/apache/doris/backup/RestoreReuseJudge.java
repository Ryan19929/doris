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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.RestoreLineage;
import org.apache.doris.catalog.RestoreSource;
import org.apache.doris.catalog.Tablet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * The decisions of partition level reuse that need no catalog changes: the check level, the chain of conditions that
 * select the candidate partitions, the choice of the sampled partitions, and the comparison with the digests of the
 * backup. All static and pure, so each of them can be tested alone.
 *
 * <p>The conditions are an ordered chain, cheap ones first. The first condition that is not satisfied gives the
 * reason that the partition is downloaded. Other ways to pass L0 (see {@link #L0_CHECKS}) and other conditions can be
 * added to the chains without changing the callers.
 */
public final class RestoreReuseJudge {

    private RestoreReuseJudge() {
    }

    /** The check level of partition level reuse, from the restore property reuse_check_level. */
    public enum CheckLevel {
        // Judge by L0 only, no digest. Not allowed for unique tables.
        OFF,
        // Compute the digest of a sample of the candidates, and of all of them if the sample finds a mismatch.
        SAMPLE,
        // Compute the digest of all candidates.
        FULL,
        // Never keep a local partition.
        DISABLE;

        /** Parses the property, an absent or invalid value gives the default, or SAMPLE if that is invalid. */
        public static CheckLevel parse(String value, String defaultValue) {
            CheckLevel level = parseOrNull(value);
            if (level == null) {
                level = parseOrNull(defaultValue);
            }
            return level == null ? SAMPLE : level;
        }

        private static CheckLevel parseOrNull(String value) {
            if (value == null) {
                return null;
            }
            try {
                return valueOf(value.trim().toUpperCase());
            } catch (IllegalArgumentException e) {
                return null;
            }
        }

        public String lower() {
            return name().toLowerCase();
        }
    }

    /** The check level that the partitions of a table are judged with. Unique tables do not allow OFF. */
    public static CheckLevel levelOfTable(CheckLevel jobLevel, KeysType keysType) {
        if (jobLevel == CheckLevel.OFF && keysType == KeysType.UNIQUE_KEYS) {
            return CheckLevel.SAMPLE;
        }
        return jobLevel;
    }

    /** What a condition needs to know about a partition to restore. */
    public static class Input {
        public boolean atomicRestore;
        public boolean allowLoad;
        public boolean cloudMode;
        public long minPartitionBytes;
        public CheckLevel level;
        public BackupJobInfo jobInfo;
        public BackupOlapTableInfo backupTable;
        public BackupPartitionInfo backupPartition;
        public long srcCommitSeq;
        public long localDbId;
        public OlapTable localTable;
        public Partition localPartition;
        // What the backup meta carried before it was cleared (see RestoreJob#clearRestoreLineageInBackupMeta), for
        // the reverse L0 checks. The source partition's own restore lineage, the source table's own restore source,
        // and the source table itself for the partition range. Null if the backup has none.
        public RestoreLineage backupLineage;
        public RestoreSource backupTableSource;
        public OlapTable backupOlapTable;
        // Set by checkL0: the L0 check that passed, null if none did.
        public String l0Path;
        private long singleReplicaBytes = -1;

        /** The local data size of a single replica of the partition, computed once. */
        public long getSingleReplicaBytes() {
            if (singleReplicaBytes < 0) {
                singleReplicaBytes = RestoreReuseShadowStats.getSingleReplicaLocalDataSize(localPartition);
            }
            return singleReplicaBytes;
        }
    }

    /** A condition of the chain: returns the reason that the partition is not a candidate, or null if satisfied. */
    public interface Condition {
        String check(Input in);
    }

    /** A way to pass L0, the partition passes if any of them does. */
    public abstract static class L0Check {
        final String name;

        L0Check(String name) {
            this.name = name;
        }

        abstract RestoreReuseShadowStats.L0Verdict check(Input in);
    }

    public static final String L0_FORWARD = "a";
    public static final String L0_REVERSE = "b";
    public static final String L0_TABLE = "c";

    /** a. forward: the local partition has the lineage of the source partition of this backup. */
    public static final L0Check FORWARD_L0 = new L0Check(L0_FORWARD) {
        @Override
        RestoreReuseShadowStats.L0Verdict check(Input in) {
            return RestoreReuseShadowStats.checkL0(in.localPartition, in.jobInfo.dbId, in.backupTable.id,
                    in.backupPartition.id, in.backupPartition.version, in.srcCommitSeq);
        }
    };

    /**
     * b. reverse: the source partition in the backup carries the lineage that points to this local partition, and
     * the versions are equal. Only used when a digest is computed.
     */
    public static final L0Check REVERSE_L0 = new L0Check(L0_REVERSE) {
        @Override
        RestoreReuseShadowStats.L0Verdict check(Input in) {
            if (in.level == CheckLevel.OFF || in.level == CheckLevel.DISABLE) {
                return RestoreReuseShadowStats.L0Verdict.LEVEL_NOT_ALLOWED;
            }
            if (in.localPartition == null) {
                return RestoreReuseShadowStats.L0Verdict.NO_LOCAL_PARTITION;
            }
            if (in.backupLineage == null) {
                return RestoreReuseShadowStats.L0Verdict.NO_BACKUP_LINEAGE;
            }
            if (!in.backupLineage.isSameSource(in.localDbId, in.localTable.getId(), in.localPartition.getId())) {
                return RestoreReuseShadowStats.L0Verdict.LINEAGE_MISMATCH;
            }
            if (in.backupPartition.version != in.localPartition.getVisibleVersion()) {
                return RestoreReuseShadowStats.L0Verdict.VERSION_MISMATCH;
            }
            return RestoreReuseShadowStats.L0Verdict.REUSABLE;
        }
    };

    /**
     * c. table level relation: the local table and the backup table are the same replicated table (the source of one
     * is the other, in either direction), the partitions have the same name and range, and the versions are equal.
     * Only used when a digest is computed.
     */
    public static final L0Check TABLE_L0 = new L0Check(L0_TABLE) {
        @Override
        RestoreReuseShadowStats.L0Verdict check(Input in) {
            if (in.level == CheckLevel.OFF || in.level == CheckLevel.DISABLE) {
                return RestoreReuseShadowStats.L0Verdict.LEVEL_NOT_ALLOWED;
            }
            if (in.localPartition == null) {
                return RestoreReuseShadowStats.L0Verdict.NO_LOCAL_PARTITION;
            }
            RestoreSource localSource = in.localTable.getRestoreSource();
            boolean forward = localSource != null && localSource.isSameSource(in.jobInfo.dbId, in.backupTable.id);
            boolean reverse = in.backupTableSource != null
                    && in.backupTableSource.isSameSource(in.localDbId, in.localTable.getId());
            if (!forward && !reverse) {
                return RestoreReuseShadowStats.L0Verdict.NO_TABLE_RELATION;
            }
            if (!sameRange(in)) {
                return RestoreReuseShadowStats.L0Verdict.PARTITION_MISMATCH;
            }
            if (in.backupPartition.version != in.localPartition.getVisibleVersion()) {
                return RestoreReuseShadowStats.L0Verdict.VERSION_MISMATCH;
            }
            return RestoreReuseShadowStats.L0Verdict.REUSABLE;
        }
    };

    // The partition name is the same by the way the local partition is found, compare the partition types and ranges
    // of the two tables. Unknown is not the same.
    private static boolean sameRange(Input in) {
        if (in.backupOlapTable == null) {
            return false;
        }
        Partition backupPart = in.backupOlapTable.getPartition(in.localPartition.getName(), false);
        if (backupPart == null) {
            return false;
        }
        PartitionInfo localInfo = in.localTable.getPartitionInfo();
        PartitionInfo backupInfo = in.backupOlapTable.getPartitionInfo();
        if (localInfo == null || backupInfo == null || localInfo.getType() != backupInfo.getType()) {
            return false;
        }
        PartitionItem localItem = localInfo.getItem(in.localPartition.getId());
        PartitionItem backupItem = backupInfo.getItem(backupPart.getId());
        return localItem != null && localItem.equals(backupItem);
    }

    /**
     * Whether the candidate of the L0 path must have its digest computed, whatever the sample says: the partitions
     * that came in by the reverse or the table level relation are only proved by the relation and the version,
     * which is weaker than the lineage stamp of the forward path.
     */
    public static boolean needsFullDigest(String l0Path, boolean forceFullForRelation) {
        return forceFullForRelation && (L0_REVERSE.equals(l0Path) || L0_TABLE.equals(l0Path));
    }

    public static final List<L0Check> L0_CHECKS = ImmutableList.of(FORWARD_L0, REVERSE_L0, TABLE_L0);

    /**
     * The first L0 check that passes, null if none does. At the check level off only the forward check is used.
     *
     * @param first if not null, [0] receives the verdict of the forward check
     */
    static String passedL0(Input in, RestoreReuseShadowStats.L0Verdict[] first) {
        for (L0Check check : L0_CHECKS) {
            RestoreReuseShadowStats.L0Verdict verdict = check.check(in);
            if (first != null && first[0] == null) {
                first[0] = verdict;
            }
            if (verdict == RestoreReuseShadowStats.L0Verdict.REUSABLE) {
                return check.name;
            }
        }
        return null;
    }

    public static final List<Condition> CONDITIONS = ImmutableList.of(
            RestoreReuseJudge::checkMode,
            RestoreReuseJudge::checkL0,
            RestoreReuseJudge::checkSupported,
            RestoreReuseJudge::checkDigestPresent,
            RestoreReuseJudge::checkSize,
            RestoreReuseJudge::checkReplicas);

    /** Returns the reason that the partition is not a candidate, null if it is. */
    public static String firstReject(Input in) {
        for (Condition condition : CONDITIONS) {
            String reason = condition.check(in);
            if (reason != null) {
                return reason;
            }
        }
        return null;
    }

    // 1. non-atomic restore into an existing table that is forbidden to write.
    static String checkMode(Input in) {
        if (in.atomicRestore) {
            return "ATOMIC_RESTORE";
        }
        if (in.allowLoad) {
            // the version may change after the judgement
            return "ALLOW_LOAD";
        }
        if (in.localTable.getState() != OlapTableState.RESTORE) {
            return "TABLE_STATE_" + in.localTable.getState().name();
        }
        return null;
    }

    // 2. L0: the lineage and the versions.
    static String checkL0(Input in) {
        RestoreReuseShadowStats.L0Verdict[] first = new RestoreReuseShadowStats.L0Verdict[1];
        in.l0Path = passedL0(in, first);
        if (in.l0Path != null) {
            return null;
        }
        return "L0_" + (first[0] == null ? "FAILED" : first[0].name());
    }

    // 3. cases that the first version does not cover.
    static String checkSupported(Input in) {
        if (in.cloudMode) {
            return "CLOUD_MODE";
        }
        RestoreReuseShadowStats.Unsupported unsupported = RestoreReuseShadowStats.checkUnsupported(
                in.atomicRestore, in.localTable, in.localPartition);
        return unsupported == RestoreReuseShadowStats.Unsupported.NONE ? null : unsupported.name();
    }

    // 4. every tablet of the backup partition has a digest, matching the local tablets one to one. Not needed
    // if no digest is computed.
    static String checkDigestPresent(Input in) {
        if (in.level == CheckLevel.OFF) {
            return null;
        }
        for (MaterializedIndex localIdx : in.localPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
            BackupIndexInfo backupIdx = in.backupPartition.getIdx(in.localTable.getIndexNameById(localIdx.getId()));
            if (backupIdx == null || backupIdx.sortedTabletInfoList.size() != localIdx.getTablets().size()) {
                return "INDEX_MISMATCH";
            }
            for (BackupTabletInfo backupTablet : backupIdx.sortedTabletInfoList) {
                LogicalDigestInfo digest = in.jobInfo.getLogicalDigest(backupTablet.id);
                if (digest == null || !digest.hasDigest()) {
                    return "NO_DIGEST";
                }
            }
        }
        return null;
    }

    // 5. small partitions are cheaper to download.
    static String checkSize(Input in) {
        return in.getSingleReplicaBytes() >= in.minPartitionBytes ? null : "TOO_SMALL";
    }

    // 6. every replica is healthy and has the version of the backup, so the digest of each is computed on that
    // version, and the replicas are the same as after a download.
    static String checkReplicas(Input in) {
        long version = in.backupPartition.version;
        for (MaterializedIndex index : in.localPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
            for (Tablet tablet : index.getTablets()) {
                if (tablet.getReplicas().isEmpty()) {
                    return "REPLICA_UNHEALTHY";
                }
                for (Replica replica : tablet.getReplicas()) {
                    if (!isReplicaHealthyAt(replica, version)) {
                        return "REPLICA_UNHEALTHY";
                    }
                }
            }
        }
        return null;
    }

    public static boolean isReplicaHealthyAt(Replica replica, long version) {
        return replica.getState() == ReplicaState.NORMAL && !replica.isBad() && replica.getVersion() == version
                && replica.getLastFailedVersion() < 0;
    }

    /**
     * Pick the partitions to compute the digest for in the sample check level: the ratio of them, at least one.
     *
     * @return a new list of the picked elements, empty if the pool is empty
     */
    public static <T> List<T> pickSample(List<T> pool, double ratio, Random random) {
        if (pool.isEmpty()) {
            return Lists.newArrayList();
        }
        int num = (int) Math.ceil(pool.size() * Math.max(0, Math.min(1, ratio)));
        num = Math.max(1, Math.min(pool.size(), num));
        List<T> shuffled = Lists.newArrayList(pool);
        Collections.shuffle(shuffled, random);
        return Lists.newArrayList(shuffled.subList(0, num));
    }

    /**
     * Compare the digest of a tablet in the backup with the digests of its local replicas. Consistent only if the
     * algorithm version and schema signature are the same as the backup, and the root of every replica is the same
     * as the backup.
     *
     * @param expected the digest in the backup
     * @param replicaDigests the digest of each local replica, null for a replica without a result
     * @return null if consistent, otherwise the reason (a DOWNLOAD_ constant of {@link RestoreReuseResult})
     */
    public static String compareTablet(LogicalDigestInfo expected, List<LogicalDigestInfo> replicaDigests) {
        if (expected == null || !expected.hasDigest() || replicaDigests.isEmpty()) {
            return RestoreReuseResult.DOWNLOAD_DIGEST_ERROR;
        }
        String reason = null;
        for (LogicalDigestInfo actual : replicaDigests) {
            if (actual == null || !actual.hasDigest()) {
                return RestoreReuseResult.DOWNLOAD_DIGEST_ERROR;
            }
            if (!expected.algoVersion.equals(actual.algoVersion) || !expected.schemaSig.equals(actual.schemaSig)) {
                reason = RestoreReuseResult.DOWNLOAD_SCHEMA_MISMATCH;
            } else if (!expected.root.equalsIgnoreCase(actual.root) && reason == null) {
                reason = RestoreReuseResult.DOWNLOAD_ROOT_MISMATCH;
            }
        }
        return reason;
    }
}
