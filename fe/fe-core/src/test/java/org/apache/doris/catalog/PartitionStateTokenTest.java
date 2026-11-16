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

import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PartitionStateTokenTest {

    private static Partition partition(long id, long version, long versionTime) {
        Partition p = new Partition(id, "p" + id, new MaterializedIndex(id + 1, MaterializedIndex.IndexState.NORMAL),
                new RandomDistributionInfo(1));
        p.updateVisibleVersionAndTime(version, versionTime);
        return p;
    }

    @Test
    public void testExplicitTokenOnlyAtItsBinding() {
        Partition p = partition(10, 5, 1000);
        p.setStateToken(new PartitionStateToken("aaaa", 5, 1000));
        Assertions.assertEquals("aaaa", PartitionStateToken.effective(p, 7));
        // a write that did not set the token: the binding breaks, the implicit token is a new name
        p.updateVisibleVersionAndTime(6, 2000);
        String implicit = PartitionStateToken.effective(p, 7);
        Assertions.assertNotEquals("aaaa", implicit);
        Assertions.assertEquals(PartitionStateToken.implicit(7, 10, 6, 2000), implicit);
        // the same version with another version time (a rewrite after a metadata rollback) is another name
        p.updateVisibleVersionAndTime(6, 2001);
        Assertions.assertNotEquals(implicit, PartitionStateToken.effective(p, 7));
    }

    @Test
    public void testInvalidHasNoToken() {
        Partition p = partition(10, 5, 1000);
        p.setStateToken(PartitionStateToken.invalid());
        Assertions.assertNull(PartitionStateToken.effective(p, 7));
    }

    @Test
    public void testImplicitDependsOnClusterAndPartition() {
        Assertions.assertNotEquals(PartitionStateToken.implicit(1, 10, 5, 1000),
                PartitionStateToken.implicit(2, 10, 5, 1000));
        Assertions.assertNotEquals(PartitionStateToken.implicit(1, 10, 5, 1000),
                PartitionStateToken.implicit(1, 11, 5, 1000));
        Assertions.assertEquals(32, PartitionStateToken.implicit(1, 10, 5, 1000).length());
    }

    @Test
    public void testFreshNeverRepeats() {
        // two writes of the same version from the same state (both sides of a failover) get different tokens
        Assertions.assertNotEquals(PartitionStateToken.fresh("prev", 1, 10, 6),
                PartitionStateToken.fresh("prev", 1, 10, 6));
    }

    @Test
    public void testPersisted() {
        Partition p = partition(10, 5, 1000);
        p.setStateToken(new PartitionStateToken("bbbb", 5, 1000));
        Partition q = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(p), Partition.class);
        Assertions.assertEquals("bbbb", PartitionStateToken.effective(q, 7));
        p.setStateToken(PartitionStateToken.invalid());
        q = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(p), Partition.class);
        Assertions.assertNull(PartitionStateToken.effective(q, 7));
    }
}
