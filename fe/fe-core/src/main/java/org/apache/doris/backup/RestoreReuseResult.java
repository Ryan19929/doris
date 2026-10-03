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

import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.Map;

/**
 * The outcome of partition level reuse in a restore job: which existing partitions keep their local data instead of
 * being downloaded, and why each candidate partition was kept or downloaded. Fixed when the VERIFYING state ends,
 * and persisted with the job (the edit log written when the job enters DOWNLOAD, and the later ones).
 */
public class RestoreReuseResult {
    // kept
    // The partition is kept on the lineage check only (check level off), no digest was computed.
    public static final String KEPT_L0_ONLY = "KEPT_L0_ONLY";
    // Every replica of every tablet has the digest of the backup.
    public static final String KEPT_DIGEST_VERIFIED = "KEPT_DIGEST_VERIFIED";
    // The partition was not sampled and the sampled partitions are all consistent (check level sample).
    public static final String KEPT_SAMPLE_PASSED = "KEPT_SAMPLE_PASSED";

    // incremental append (see Decision#incremental): the local partition is behind the backup, its data up to its own
    // version was proved equal to the backup at that version on every replica, and the backup can be cut there, so
    // only the rowsets after it are downloaded and appended.
    public static final String INCREMENTAL_VERIFIED = "INCREMENTAL_VERIFIED";

    // downloaded
    // The version of the local partition is not the end version of a rowset of the backup, which can not be cut
    // there (a compaction merged the versions across it).
    public static final String DOWNLOAD_NOT_BOUNDARY = "DOWNLOAD_NOT_BOUNDARY";
    // The backup has no decomposed digest the backend could read, or it does not match the job info.
    public static final String DOWNLOAD_PREFIX_ERROR = "DOWNLOAD_PREFIX_ERROR";
    // A replica has a digest different from the backup.
    public static final String DOWNLOAD_ROOT_MISMATCH = "DOWNLOAD_ROOT_MISMATCH";
    // The digest algorithm version or the schema signature differs from the backup.
    public static final String DOWNLOAD_SCHEMA_MISMATCH = "DOWNLOAD_SCHEMA_MISMATCH";
    // The digest task failed, is not supported by the backend, or the backend did not report a digest.
    public static final String DOWNLOAD_DIGEST_ERROR = "DOWNLOAD_DIGEST_ERROR";
    // The digest tasks did not finish in time.
    public static final String DOWNLOAD_TIMEOUT = "DOWNLOAD_TIMEOUT";
    // The version or a replica of the partition changed while verifying.
    public static final String DOWNLOAD_CHANGED = "DOWNLOAD_CHANGED";
    // A sampled partition failed to be verified (a failed task, a timeout), so the partitions not sampled can not
    // be proved either.
    public static final String DOWNLOAD_SAMPLE_FAILED = "DOWNLOAD_SAMPLE_FAILED";
    // The job failed to verify the partition.
    public static final String DOWNLOAD_VERIFY_ERROR = "DOWNLOAD_VERIFY_ERROR";

    /** The decision of one candidate partition. */
    public static class Decision {
        @SerializedName("tid")
        public long tableId;
        @SerializedName("pid")
        public long partitionId;
        @SerializedName("tn")
        public String tableName;
        @SerializedName("pn")
        public String partitionName;
        // the version of the backup, equal to the local visible version of a kept partition
        @SerializedName("v")
        public long version;
        // the local data size of a single replica
        @SerializedName("b")
        public long bytes;
        // off|sample|full, the check level the partition was judged with
        @SerializedName("l")
        public String level;
        // the local data size of all replicas, what the partition would have downloaded
        @SerializedName("ab")
        public long bytesAllReplicas;
        // the L0 check that let the partition in: a forward, b reverse, c table level
        @SerializedName("p")
        public String l0Path;
        @SerializedName("k")
        public boolean kept;
        // The partition is restored by appending the rowsets of the versions (version, targetVersion] of the backup
        // to the local tablets, "version" is then the visible version of the local partition. Not kept.
        @SerializedName("i")
        public boolean incremental;
        // the version of the backup, only for an incremental partition (otherwise "version" is the one)
        @SerializedName("tv")
        public long targetVersion;
        @SerializedName("r")
        public String reason;

        @Override
        public String toString() {
            return tableName + "." + partitionName + "(" + tableId + "," + partitionId + ", v" + version
                    + (incremental ? "->v" + targetVersion : "") + ", " + level + ", L0 " + l0Path + "): "
                    + (kept ? "KEEP " : incremental ? "INCREMENTAL " : "DOWNLOAD ") + reason;
        }
    }

    @SerializedName("d")
    private List<Decision> decisions = Lists.newArrayList();
    // reason of not being a candidate -> number of partitions
    @SerializedName("rc")
    private Map<String, Long> rejected = Maps.newTreeMap();

    public void addDecision(Decision decision) {
        decisions.add(decision);
    }

    public void addRejected(String reason) {
        rejected.merge(reason, 1L, Long::sum);
    }

    public List<Decision> getDecisions() {
        return decisions;
    }

    public Map<String, Long> getRejected() {
        return rejected;
    }

    public boolean isEmpty() {
        return decisions.isEmpty();
    }

    public boolean isKept(long tableId, long partitionId) {
        for (Decision decision : decisions) {
            if (decision.kept && decision.tableId == tableId && decision.partitionId == partitionId) {
                return true;
            }
        }
        return false;
    }

    /** The decision of the partition if it is restored incrementally, otherwise null. */
    public Decision getIncremental(long tableId, long partitionId) {
        for (Decision decision : decisions) {
            if (decision.incremental && decision.tableId == tableId && decision.partitionId == partitionId) {
                return decision;
            }
        }
        return null;
    }

    public List<Decision> getIncrementalDecisions() {
        List<Decision> result = Lists.newArrayList();
        for (Decision decision : decisions) {
            if (decision.incremental) {
                result.add(decision);
            }
        }
        return result;
    }

    public long getIncrementalPartitions() {
        return decisions.stream().filter(d -> d.incremental).count();
    }

    /** The local data size of all replicas of the incremental partitions, what a download of them would write. */
    public long getIncrementalLocalBytesAllReplicas() {
        return decisions.stream().filter(d -> d.incremental).mapToLong(d -> d.bytesAllReplicas).sum();
    }

    public List<Decision> getKeptDecisions() {
        List<Decision> kept = Lists.newArrayList();
        for (Decision decision : decisions) {
            if (decision.kept) {
                kept.add(decision);
            }
        }
        return kept;
    }

    public long getKeptPartitions() {
        return decisions.stream().filter(d -> d.kept).count();
    }

    /** The local data size of a single replica of the kept partitions. */
    public long getKeptBytesSingleReplica() {
        return decisions.stream().filter(d -> d.kept).mapToLong(d -> d.bytes).sum();
    }

    /** The local data size of all replicas of the kept partitions, the bytes that were not downloaded. */
    public long getKeptBytesAllReplicas() {
        return decisions.stream().filter(d -> d.kept).mapToLong(d -> d.bytesAllReplicas).sum();
    }

    /** The kept partitions that passed L0 by the given check. */
    public long getKeptByPath(String l0Path) {
        return decisions.stream().filter(d -> d.kept && l0Path.equals(d.l0Path)).count();
    }

    /** The candidate partitions that are downloaded for one of the reasons. */
    public long countReasons(String... reasons) {
        long count = 0;
        for (Decision decision : decisions) {
            if (!decision.kept && !decision.incremental) {
                for (String reason : reasons) {
                    if (reason.equals(decision.reason)) {
                        count++;
                        break;
                    }
                }
            }
        }
        return count;
    }

    /** The candidate partitions that are downloaded as a whole (neither kept nor incremental). */
    public long getDownloadedPartitions() {
        return decisions.stream().filter(d -> !d.kept && !d.incremental).count();
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }
}
