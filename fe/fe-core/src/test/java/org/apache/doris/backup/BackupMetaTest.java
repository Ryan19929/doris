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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.catalog.CloudTablet;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BackupMetaTest {
    private static final long TABLE_ID = 100L;
    private static final long PARTITION_ID = 200L;
    private static final long INDEX_ID = 300L;
    private static final long TABLET_ID = 400L;
    private static final long REPLICA_ID = 500L;
    private static final String TABLE_NAME = "backup_meta_test_table";

    @Test
    public void testReplicaReservationRoundTripForLocalAndCloudTablets() throws Exception {
        boolean savedConfig = Config.backup_meta_reserve_replica_info;
        boolean savedStreaming = Config.enable_table_meta_streaming_json;
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnv.when(Env::getCurrentEnvJournalVersion).thenReturn(FeConstants.meta_version);

            verifyRoundTrip(false, false);
            verifyRoundTrip(false, true);
            verifyRoundTrip(true, false);
            verifyRoundTrip(true, true);
        } finally {
            Config.backup_meta_reserve_replica_info = savedConfig;
            Config.enable_table_meta_streaming_json = savedStreaming;
        }
    }

    @Test
    public void testStreamingCopyRestoresPreviousMetaContext() {
        boolean savedStreaming = Config.enable_table_meta_streaming_json;
        OlapTable table = createTable(false);
        MetaContext previousContext = MetaContext.get();
        MetaContext expectedContext = new MetaContext();
        expectedContext.setMetaVersion(FeConstants.meta_version);
        expectedContext.setThreadLocalInfo();
        try {
            Config.enable_table_meta_streaming_json = true;
            Assert.assertNotNull(table.selectiveCopy(null, IndexExtState.VISIBLE, true));
            Assert.assertSame(expectedContext, MetaContext.get());
        } finally {
            Config.enable_table_meta_streaming_json = savedStreaming;
            if (previousContext == null) {
                MetaContext.remove();
            } else {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    private void verifyRoundTrip(boolean cloudTablet, boolean reserveReplica) throws Exception {
        OlapTable sourceTable = createTable(cloudTablet);
        Tablet sourceTablet = getOnlyTablet(sourceTable);
        Assert.assertEquals(1, sourceTablet.getReplicas().size());

        Config.backup_meta_reserve_replica_info = reserveReplica;
        OlapTable backupTable = sourceTable.selectiveCopy(null, IndexExtState.VISIBLE, true);
        Assert.assertNotNull(backupTable);

        BackupMeta backupMeta = new BackupMeta(
                Collections.singletonList(backupTable), Collections.emptyList());
        byte[] legacyBytes = serialize(backupMeta, false);
        byte[] streamingBytes = serialize(backupMeta, true);
        Assert.assertArrayEquals(legacyBytes, streamingBytes);

        for (byte[] bytes : new byte[][] {legacyBytes, streamingBytes}) {
            for (boolean streamingRead : new boolean[] {false, true}) {
                Config.enable_table_meta_streaming_json = streamingRead;
                assertRestoredMeta(BackupMeta.fromBytes(bytes, FeConstants.meta_version),
                        cloudTablet, reserveReplica);
            }
        }

        Assert.assertEquals(1, sourceTablet.getReplicas().size());
    }

    private void assertRestoredMeta(BackupMeta restoredMeta, boolean cloudTablet, boolean reserveReplica) {
        Table restoredTableByName = restoredMeta.getTable(TABLE_NAME);
        Assert.assertSame(restoredTableByName, restoredMeta.getTable(TABLE_ID));
        Assert.assertTrue(restoredTableByName instanceof OlapTable);

        Tablet restoredTablet = getOnlyTablet((OlapTable) restoredTableByName);
        if (cloudTablet) {
            Assert.assertTrue(restoredTablet instanceof CloudTablet);
        } else {
            Assert.assertTrue(restoredTablet instanceof LocalTablet);
        }
        Assert.assertEquals(reserveReplica ? 1 : 0, restoredTablet.getReplicas().size());
        if (reserveReplica) {
            Replica restoredReplica = restoredTablet.getReplicas().get(0);
            Assert.assertEquals(cloudTablet, restoredReplica instanceof CloudReplica);
            Assert.assertEquals(!cloudTablet, restoredReplica instanceof LocalReplica);
        }
    }

    private byte[] serialize(BackupMeta backupMeta, boolean streaming) throws Exception {
        boolean savedStreaming = Config.enable_table_meta_streaming_json;
        try {
            Config.enable_table_meta_streaming_json = streaming;
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                backupMeta.write(output);
            }
            return bytes.toByteArray();
        } finally {
            Config.enable_table_meta_streaming_json = savedStreaming;
        }
    }

    private OlapTable createTable(boolean cloudTablet) {
        List<Column> schema = new ArrayList<>();
        Column keyColumn = new Column("key", PrimitiveType.INT);
        keyColumn.setIsKey(true);
        schema.add(keyColumn);

        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(1);
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(PARTITION_ID, new DataProperty(TStorageMedium.HDD));
        partitionInfo.setReplicaAllocation(PARTITION_ID, new ReplicaAllocation((short) 1));

        OlapTable table = new OlapTable(TABLE_ID, TABLE_NAME, schema, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo);
        MaterializedIndex index = new MaterializedIndex(INDEX_ID, IndexState.NORMAL);
        Tablet tablet;
        Replica replica;
        if (cloudTablet) {
            tablet = new CloudTablet(TABLET_ID);
            replica = new CloudReplica(REPLICA_ID, -1L, ReplicaState.NORMAL, 1L, 0,
                    1L, TABLE_ID, PARTITION_ID, INDEX_ID, 0L);
        } else {
            tablet = new LocalTablet(TABLET_ID);
            replica = new LocalReplica(REPLICA_ID, 1L, 1L, 0, 0L, 0L, 0L,
                    ReplicaState.NORMAL, -1L, 0L);
        }
        index.appendTablets(Collections.singletonList(tablet));
        tablet.addReplica(replica, true);

        Partition partition = new Partition(PARTITION_ID, "p1", index, distributionInfo);
        table.addPartition(partition);
        table.setIndexMeta(INDEX_ID, TABLE_NAME, schema, 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setBaseIndexId(INDEX_ID);
        return table;
    }

    private Tablet getOnlyTablet(OlapTable table) {
        Partition partition = table.getPartitions().iterator().next();
        MaterializedIndex index = partition.getMaterializedIndices(IndexExtState.VISIBLE).get(0);
        return index.getTablets().get(0);
    }
}
