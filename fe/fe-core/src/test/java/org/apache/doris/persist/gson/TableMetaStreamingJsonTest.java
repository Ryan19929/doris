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

package org.apache.doris.persist.gson;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.catalog.CloudTablet;
import org.apache.doris.common.Config;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class TableMetaStreamingJsonTest {
    private boolean savedStreaming;

    @BeforeEach
    void saveConfig() {
        savedStreaming = Config.enable_table_meta_streaming_json;
    }

    @AfterEach
    void restoreConfig() {
        Config.enable_table_meta_streaming_json = savedStreaming;
    }

    @Test
    void localTableCompatibilityMatrix() {
        OlapTable table = createLocalTable();
        String legacy = toJson(table, Table.class, false);
        String streaming = toJson(table, Table.class, true);
        Assertions.assertEquals(legacy, streaming);
        Assertions.assertTrue(streaming.startsWith("{\"clazz\":\"OlapTable\","));
        Assertions.assertTrue(streaming.contains("\"clazz\":\"Partition\""));
        Assertions.assertTrue(streaming.contains("\"clazz\":\"LocalTablet\""));
        Assertions.assertTrue(streaming.contains("\"clazz\":\"LocalReplica\""));

        for (String json : new String[] {legacy, streaming}) {
            for (boolean streamingRead : new boolean[] {false, true}) {
                Config.enable_table_meta_streaming_json = streamingRead;
                OlapTable restored = (OlapTable) GsonUtils.GSON.fromJson(json, Table.class);
                Tablet tablet = restored.getPartition("p1").getBaseIndex().getTablets().get(0);
                Assertions.assertInstanceOf(LocalTablet.class, tablet);
                Assertions.assertInstanceOf(LocalReplica.class, tablet.getReplicas().get(0));
            }
        }
    }

    @Test
    void tableAndOlapTableIoCompatibilityMatrix() throws Exception {
        OlapTable table = createLocalTable();
        byte[] legacy = writeTable(table, false);
        byte[] streaming = writeTable(table, true);
        Assertions.assertArrayEquals(legacy, streaming);
        for (byte[] bytes : new byte[][] {legacy, streaming}) {
            for (boolean streamingRead : new boolean[] {false, true}) {
                Config.enable_table_meta_streaming_json = streamingRead;
                Assertions.assertInstanceOf(OlapTable.class, Table.read(dataInput(bytes)));
                Assertions.assertEquals(table.getId(), OlapTable.read(dataInput(bytes)).getId());
            }
        }
    }

    @Test
    void explicitCloudSubtypesCompatibilityMatrix() {
        assertSubtype(new CloudTablet(10), Tablet.class, CloudTablet.class, "CloudTablet");
        CloudReplica replica = new CloudReplica(11, -1L, Replica.ReplicaState.NORMAL, 1, 0,
                1, 2, 3, 4, 0);
        assertSubtype(replica, Replica.class, CloudReplica.class, "CloudReplica");

        MaterializedIndex index = new MaterializedIndex(12, MaterializedIndex.IndexState.NORMAL);
        CloudPartition partition = new CloudPartition(13, "cloud", index,
                new RandomDistributionInfo(1), 14, 15);
        assertSubtype(partition, Partition.class, CloudPartition.class, "CloudPartition");
    }

    @Test
    void defaultAndNonLeadingTypePayloadsRemainCompatible() {
        LocalReplica replica = new LocalReplica(20, 21, Replica.ReplicaState.NORMAL, 22, 0);
        String canonical = toJson(replica, Replica.class, false);
        String prefix = "{\"clazz\":\"LocalReplica\",";
        Assertions.assertTrue(canonical.startsWith(prefix));

        String withoutType = "{" + canonical.substring(prefix.length());
        Config.enable_table_meta_streaming_json = true;
        Assertions.assertInstanceOf(LocalReplica.class, GsonUtils.GSON.fromJson(withoutType, Replica.class));

        String body = canonical.substring(prefix.length(), canonical.length() - 1);
        String typeLast = "{" + body + ",\"clazz\":\"LocalReplica\"}";
        Assertions.assertInstanceOf(LocalReplica.class, GsonUtils.GSON.fromJson(typeLast, Replica.class));

        String compatible = canonical.replaceFirst("LocalReplica", "Replica");
        Assertions.assertInstanceOf(LocalReplica.class, GsonUtils.GSON.fromJson(compatible, Replica.class));
    }

    private static <T> void assertSubtype(Object value, Class<T> declaredType, Class<?> expectedType,
            String label) {
        String legacy = toJson(value, declaredType, false);
        String streaming = toJson(value, declaredType, true);
        Assertions.assertEquals(legacy, streaming);
        Assertions.assertTrue(streaming.startsWith("{\"clazz\":\"" + label + "\","));
        for (String json : new String[] {legacy, streaming}) {
            for (boolean streamingRead : new boolean[] {false, true}) {
                Config.enable_table_meta_streaming_json = streamingRead;
                Assertions.assertInstanceOf(expectedType, GsonUtils.GSON.fromJson(json, declaredType));
            }
        }
    }

    private static String toJson(Object value, Class<?> declaredType, boolean streaming) {
        Config.enable_table_meta_streaming_json = streaming;
        return GsonUtils.GSON.toJson(value, declaredType);
    }

    private static byte[] writeTable(OlapTable table, boolean streaming) throws IOException {
        Config.enable_table_meta_streaming_json = streaming;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            table.write(output);
        }
        return bytes.toByteArray();
    }

    private static DataInputStream dataInput(byte[] bytes) {
        return new DataInputStream(new ByteArrayInputStream(bytes));
    }

    private static OlapTable createLocalTable() {
        long partitionId = 2;
        long indexId = 3;
        List<Column> schema = new ArrayList<>();
        Column key = new Column("key", PrimitiveType.INT);
        key.setIsKey(true);
        schema.add(key);

        RandomDistributionInfo distribution = new RandomDistributionInfo(1);
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, new DataProperty(TStorageMedium.HDD));
        partitionInfo.setReplicaAllocation(partitionId, new ReplicaAllocation((short) 1));
        OlapTable table = new OlapTable(1, "table_meta_streaming", schema, KeysType.DUP_KEYS,
                partitionInfo, distribution);

        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(4);
        tablet.addReplica(new LocalReplica(5, 6, Replica.ReplicaState.NORMAL, 7, 0), true);
        index.appendTablets(Collections.singletonList(tablet));
        table.addPartition(new Partition(partitionId, "p1", index, distribution));
        table.setIndexMeta(indexId, table.getName(), schema, 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setBaseIndexId(indexId);
        return table;
    }
}
