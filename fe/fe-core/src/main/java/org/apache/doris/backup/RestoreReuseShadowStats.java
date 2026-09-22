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

import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.RestoreLineage;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

/**
 * Shadow statistics of a restore job: how many partitions (and bytes) the restore job could have
 * kept locally instead of downloading, if partition level reuse were enabled.
 *
 * <p>A partition is counted as reusable only if the lineage check (L0) passes for it, and it is not in a
 * case that the first version of partition level reuse does not cover (see {@link Unsupported}); a partition
 * that passes L0 but is in such a case is counted separately. L0 is necessary but not sufficient for reuse,
 * so the numbers are an upper bound. Computing the statistics never changes the behavior of the restore job.
 */
public class RestoreReuseShadowStats {

    /**
     * The result of the L0 check of a partition, in the order the conditions are checked.
     */
    public enum L0Verdict {
        // All conditions are satisfied.
        REUSABLE,
        // The partition does not exist locally, nothing to reuse.
        NO_LOCAL_PARTITION,
        // The local partition has no restore lineage.
        NO_LINEAGE,
        // The lineage of the local partition points to another source partition.
        LINEAGE_MISMATCH,
        // The local partition was written after it was restored.
        LOCAL_VERSION_CHANGED,
        // The source partition was written after the backup that the local partition was restored from.
        SOURCE_VERSION_CHANGED,
        // The commit seq of the source table is unknown, or goes backwards.
        COMMIT_SEQ_MISMATCH
    }

    /**
     * The cases not covered by the first version of partition level reuse. Only checked for the partitions
     * that pass L0, in this order.
     */
    public enum Unsupported {
        // Covered.
        NONE,
        // Atomic restore replaces the whole table, keeping local partitions there is not supported yet.
        ATOMIC_RESTORE,
        // The logical equivalence of an aggregate table can not be proved (e.g. SUM of floating point values).
        AGGREGATE_TABLE,
        // Part of the data is cooled down to remote storage, reading it costs about the same as downloading.
        REMOTE_STORAGE
    }

    @SerializedName("partitions")
    private long partitions = 0;
    @SerializedName("reusable")
    private long reusable = 0;
    @SerializedName("reusable_bytes_single_replica")
    private long reusableBytesSingleReplica = 0;
    @SerializedName("l0_passed_but_atomic_restore")
    private long l0PassedButAtomicRestore = 0;
    @SerializedName("l0_passed_but_aggregate_table")
    private long l0PassedButAggregateTable = 0;
    @SerializedName("l0_passed_but_remote_storage")
    private long l0PassedButRemoteStorage = 0;
    @SerializedName("no_local")
    private long noLocalPartition = 0;
    @SerializedName("no_lineage")
    private long noLineage = 0;
    @SerializedName("lineage_mismatch")
    private long lineageMismatch = 0;
    @SerializedName("local_version_changed")
    private long localVersionChanged = 0;
    @SerializedName("source_version_changed")
    private long sourceVersionChanged = 0;
    @SerializedName("commit_seq_mismatch")
    private long commitSeqMismatch = 0;

    /**
     * Check whether the local partition holds the same data as the source partition in the backup,
     * judged by the restore lineage and the versions only (L0).
     *
     * @param localPart the local partition with the same name, null if absent
     * @param srcDbId the source db id of the backup
     * @param srcTableId the source table id of the backup
     * @param srcPartitionId the source partition id of the backup
     * @param srcVersion the version of the source partition in the backup
     * @param srcCommitSeq the commit seq of the source table in the backup, or
     *         {@link RestoreLineage#UNKNOWN_COMMIT_SEQ}
     */
    public static L0Verdict checkL0(Partition localPart, long srcDbId, long srcTableId, long srcPartitionId,
            long srcVersion, long srcCommitSeq) {
        if (localPart == null) {
            return L0Verdict.NO_LOCAL_PARTITION;
        }
        RestoreLineage lineage = localPart.getRestoreLineage();
        // 1. the local partition was restored from the same source partition.
        if (lineage == null) {
            return L0Verdict.NO_LINEAGE;
        }
        if (!lineage.isSameSource(srcDbId, srcTableId, srcPartitionId)) {
            return L0Verdict.LINEAGE_MISMATCH;
        }
        // 2. no local write since the restore.
        if (localPart.getVisibleVersion() != lineage.getSrcVersion()) {
            return L0Verdict.LOCAL_VERSION_CHANGED;
        }
        // 3. no source write since the backup that the local partition was restored from.
        if (srcVersion != lineage.getSrcVersion()) {
            return L0Verdict.SOURCE_VERSION_CHANGED;
        }
        // 4. both backups come from the same monotonic commit seq of the source table.
        if (lineage.getSrcCommitSeq() == RestoreLineage.UNKNOWN_COMMIT_SEQ
                || srcCommitSeq == RestoreLineage.UNKNOWN_COMMIT_SEQ
                || srcCommitSeq < lineage.getSrcCommitSeq()) {
            return L0Verdict.COMMIT_SEQ_MISMATCH;
        }
        return L0Verdict.REUSABLE;
    }

    /**
     * Whether the local partition is in a case not covered by the first version of partition level reuse.
     */
    public static Unsupported checkUnsupported(boolean isAtomicRestore, OlapTable localTbl, Partition localPart) {
        if (isAtomicRestore) {
            return Unsupported.ATOMIC_RESTORE;
        }
        if (localTbl.getKeysType() == KeysType.AGG_KEYS) {
            return Unsupported.AGGREGATE_TABLE;
        }
        if (localPart.getRemoteDataSize() > 0) {
            return Unsupported.REMOTE_STORAGE;
        }
        return Unsupported.NONE;
    }

    /**
     * The local data size of one replica of the partition: for each tablet of each visible index, the local
     * data size of one NORMAL replica. The replica with the largest size is chosen, so that a replica whose
     * size has not been reported yet (0) is not picked; all replicas of a tablet hold the same logical data.
     */
    public static long getSingleReplicaLocalDataSize(Partition partition) {
        long dataSize = 0;
        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
            for (Tablet tablet : index.getTablets()) {
                long replicaDataSize = 0;
                for (Replica replica : tablet.getReplicas()) {
                    if (replica.getState() == ReplicaState.NORMAL) {
                        replicaDataSize = Math.max(replicaDataSize, replica.getDataSize());
                    }
                }
                dataSize += replicaDataSize;
            }
        }
        return dataSize;
    }

    /**
     * Add a partition.
     *
     * @param verdict the result of the L0 check
     * @param unsupported whether the partition is in a case not covered, only used if L0 passes
     * @param singleReplicaBytes the local data size of one replica of the partition, only used if the
     *         partition is reusable
     */
    public void add(L0Verdict verdict, Unsupported unsupported, long singleReplicaBytes) {
        partitions++;
        switch (verdict) {
            case REUSABLE:
                switch (unsupported) {
                    case ATOMIC_RESTORE:
                        l0PassedButAtomicRestore++;
                        break;
                    case AGGREGATE_TABLE:
                        l0PassedButAggregateTable++;
                        break;
                    case REMOTE_STORAGE:
                        l0PassedButRemoteStorage++;
                        break;
                    default:
                        reusable++;
                        reusableBytesSingleReplica += singleReplicaBytes;
                        break;
                }
                break;
            case NO_LOCAL_PARTITION:
                noLocalPartition++;
                break;
            case NO_LINEAGE:
                noLineage++;
                break;
            case LINEAGE_MISMATCH:
                lineageMismatch++;
                break;
            case LOCAL_VERSION_CHANGED:
                localVersionChanged++;
                break;
            case SOURCE_VERSION_CHANGED:
                sourceVersionChanged++;
                break;
            case COMMIT_SEQ_MISMATCH:
                commitSeqMismatch++;
                break;
            default:
                break;
        }
    }

    public long getPartitions() {
        return partitions;
    }

    public long getReusable() {
        return reusable;
    }

    public long getReusableBytesSingleReplica() {
        return reusableBytesSingleReplica;
    }

    public long getL0PassedButAtomicRestore() {
        return l0PassedButAtomicRestore;
    }

    public long getL0PassedButAggregateTable() {
        return l0PassedButAggregateTable;
    }

    public long getL0PassedButRemoteStorage() {
        return l0PassedButRemoteStorage;
    }

    public long getNoLocalPartition() {
        return noLocalPartition;
    }

    public long getNoLineage() {
        return noLineage;
    }

    public long getLineageMismatch() {
        return lineageMismatch;
    }

    public long getLocalVersionChanged() {
        return localVersionChanged;
    }

    public long getSourceVersionChanged() {
        return sourceVersionChanged;
    }

    public long getCommitSeqMismatch() {
        return commitSeqMismatch;
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }
}
