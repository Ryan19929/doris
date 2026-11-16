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

package org.apache.doris.catalog;

import com.google.gson.annotations.SerializedName;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

/**
 * The state token of a partition: the name of the logical content of the partition at its visible version. Two
 * partitions with the same effective token are the same initial state after the same sequence of writes, so their
 * content is the same as long as the replicas of a tablet agree and CCR applies a version of the source as it is.
 *
 * <p>A local write gives the partition a new token with a random part. A CCR increment adopts the token of the source
 * version when the local token is the token the source had before the write. A RESTORE adopts the token of the backup.
 *
 * <p>The explicit token is bound to the (visible version, visible version time) of the partition it was set at. A
 * path that changes the version without knowing the token (an older FE as the master, the tracking switched off, a
 * path not registered) breaks the binding, and the partition falls back to its implicit token, a name that no other
 * partition has. So such a path only makes the partition not reused.
 *
 * <p>The object is immutable. The serialized names are part of the persisted metadata and must not be changed.
 */
public class PartitionStateToken {
    private static final SecureRandom RANDOM = new SecureRandom();

    @SerializedName("tk")
    private String token;
    // the visible version and the visible version time of the partition the token is bound to
    @SerializedName("bv")
    private long boundVersion;
    @SerializedName("bt")
    private long boundVersionTime;
    // the partition was changed outside of the versions (an admin operation, an overwrite by a restore): no token
    @SerializedName("bad")
    private boolean bad;

    private PartitionStateToken() {
    }

    public PartitionStateToken(String token, long boundVersion, long boundVersionTime) {
        this.token = token;
        this.boundVersion = boundVersion;
        this.boundVersionTime = boundVersionTime;
        this.bad = false;
    }

    /** A token that makes the partition have no effective token, until the next write or restore. */
    public static PartitionStateToken invalid() {
        PartitionStateToken t = new PartitionStateToken();
        t.bad = true;
        return t;
    }

    public String getToken() {
        return token;
    }

    public long getBoundVersion() {
        return boundVersion;
    }

    public long getBoundVersionTime() {
        return boundVersionTime;
    }

    public boolean isBad() {
        return bad;
    }

    /**
     * The effective token of the partition, null if it has none (invalidated). Null never equals any token.
     *
     * @param clusterId the cluster id of this cluster, part of the implicit token
     */
    public static String effective(Partition partition, long clusterId) {
        PartitionStateToken explicit = partition.getStateToken();
        if (explicit != null) {
            if (explicit.bad) {
                return null;
            }
            if (explicit.token != null && explicit.boundVersion == partition.getVisibleVersion()
                    && explicit.boundVersionTime == partition.getVisibleVersionTime()) {
                return explicit.token;
            }
        }
        return implicit(clusterId, partition.getId(), partition.getVisibleVersion(),
                partition.getVisibleVersionTime());
    }

    /** The name of a partition of this cluster at a version and version time that has no explicit token. */
    public static String implicit(long clusterId, long partitionId, long version, long versionTime) {
        return hash("g|" + clusterId + "|" + partitionId + "|" + version + "|" + versionTime);
    }

    /** A new token for a local write: never equal to another token. */
    public static String fresh(String prev, long clusterId, long partitionId, long version) {
        byte[] nonce = new byte[16];
        RANDOM.nextBytes(nonce);
        return hash("t|" + prev + "|" + clusterId + "|" + partitionId + "|" + version + "|" + toHex(nonce));
    }

    private static String hash(String s) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
            byte[] head = new byte[16];
            System.arraycopy(d, 0, head, 0, 16);
            return toHex(head);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return bad ? "invalid" : token + "@" + boundVersion + "/" + boundVersionTime;
    }
}
