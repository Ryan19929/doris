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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GsonUtilsBaseStreamingCollectionTest {
    private static final Gson TREE_GSON = new GsonBuilder()
            .enableComplexMapKeySerialization()
            .registerTypeHierarchyAdapter(Table.class, new GsonUtilsBase.GuavaTableAdapter<>())
            .registerTypeHierarchyAdapter(Multimap.class, new GsonUtilsBase.GuavaMultimapAdapter<>())
            .create();

    private static final Gson STREAMING_GSON = new GsonBuilder()
            .enableComplexMapKeySerialization()
            .registerTypeAdapterFactory(new GsonUtilsBase.GuavaTableTypeAdapterFactory())
            .registerTypeAdapterFactory(new GsonUtilsBase.GuavaMultimapTypeAdapterFactory())
            .create();

    @Test
    public void testTableTreeAndStreamingCompatibility() {
        Type simpleTableType = new TypeToken<Table<Long, String, Long>>() {
        }.getType();
        Table<Long, String, Long> simpleTable = HashBasedTable.create();
        simpleTable.put(1L, "c1", 10L);
        Assert.assertEquals(
                "{\"clazz\":\"HashBasedTable\",\"rowKeys\":[1],\"columnKeys\":[\"c1\"],\"cells\":[0,0,10]}",
                STREAMING_GSON.toJson(simpleTable, simpleTableType));
        assertFourWayCompatibility(simpleTable, simpleTableType);

        Type tableType = new TypeToken<Table<Long, String, Map<Long, Long>>>() {
        }.getType();
        Table<Long, String, Map<Long, Long>> table = HashBasedTable.create();
        table.put(1L, "c1", Maps.newHashMap(Map.of(10L, 100L)));
        table.put(2L, "c2", Maps.newHashMap(Map.of(20L, 200L)));

        String treeJson = TREE_GSON.toJson(table, tableType);
        String streamingJson = STREAMING_GSON.toJson(table, tableType);

        Assert.assertEquals(treeJson, streamingJson);
        Assert.assertEquals(table, TREE_GSON.fromJson(treeJson, tableType));
        Assert.assertEquals(table, STREAMING_GSON.fromJson(treeJson, tableType));
        Assert.assertEquals(table, TREE_GSON.fromJson(streamingJson, tableType));
        Assert.assertEquals(table, STREAMING_GSON.fromJson(streamingJson, tableType));

        Table<Long, String, Map<Long, Long>> empty = HashBasedTable.create();
        assertFourWayCompatibility(empty, tableType);
        assertFourWayCompatibility(null, tableType);
    }

    @Test
    public void testMultimapImplementationsTreeAndStreamingCompatibility() {
        Type multimapType = new TypeToken<Multimap<Long, String>>() {
        }.getType();
        Multimap<Long, String> canonical = ArrayListMultimap.create();
        canonical.put(1L, "v1");
        canonical.put(1L, "v2");
        Assert.assertEquals("{\"clazz\":\"ArrayListMultimap\",\"map\":{\"1\":[\"v1\",\"v2\"]}}",
                STREAMING_GSON.toJson(canonical, multimapType));

        List<Multimap<Long, String>> multimaps = Lists.newArrayList(
                ArrayListMultimap.create(), HashMultimap.create(),
                LinkedListMultimap.create(), LinkedHashMultimap.create());

        for (Multimap<Long, String> multimap : multimaps) {
            multimap.put(1L, "v1");
            multimap.put(1L, "v2");
            String treeJson = TREE_GSON.toJson(multimap, multimapType);
            String streamingJson = STREAMING_GSON.toJson(multimap, multimapType);
            Assert.assertEquals(multimap.getClass().getSimpleName(), treeJson, streamingJson);
            Assert.assertEquals(multimap, TREE_GSON.fromJson(treeJson, multimapType));
            Assert.assertEquals(multimap, STREAMING_GSON.fromJson(treeJson, multimapType));
            Assert.assertEquals(multimap, TREE_GSON.fromJson(streamingJson, multimapType));
            Assert.assertEquals(multimap, STREAMING_GSON.fromJson(streamingJson, multimapType));
        }

        Multimap<Long, String> singleton = ArrayListMultimap.create();
        singleton.put(9L, "only");
        assertFourWayCompatibility(singleton, multimapType);

        Multimap<Long, String> empty = ArrayListMultimap.create();
        assertFourWayCompatibility(empty, multimapType);
        assertFourWayCompatibility(null, multimapType);
    }

    @Test
    public void testLargeTableWithNestedGenericValuesCompatibility() {
        Type tableType = new TypeToken<Table<ComplexKey, Integer, List<Map<String, Long>>>>() {
        }.getType();
        Table<ComplexKey, Integer, List<Map<String, Long>>> table = HashBasedTable.create();
        for (int i = 0; i < 2000; i++) {
            Map<String, Long> value = new LinkedHashMap<>();
            value.put("value", (long) i);
            table.put(new ComplexKey(i, "row-" + i), i % 17, Lists.newArrayList(value));
        }

        assertFourWayCompatibility(table, tableType);
    }

    @Test
    public void testLargeMultimapWithComplexKeysAndNestedValuesCompatibility() {
        Type multimapType = new TypeToken<Multimap<ComplexKey, List<Map<String, Long>>>>() {
        }.getType();
        Multimap<ComplexKey, List<Map<String, Long>>> multimap = ArrayListMultimap.create();
        for (int i = 0; i < 2000; i++) {
            Map<String, Long> value = new LinkedHashMap<>();
            value.put("value", (long) i);
            multimap.put(new ComplexKey(i % 31, "key-" + i % 31), Lists.newArrayList(value));
        }

        assertFourWayCompatibility(multimap, multimapType);
    }

    @Test
    public void testStreamingCollectionsReadLegacyFieldOrderAndIgnoreUnknownFields() {
        Type tableType = new TypeToken<Table<Long, String, Long>>() {
        }.getType();
        String shuffledTableJson = "{\"cells\":[0,0,10,1,1,20],"
                + "\"unknown\":{\"nested\":[1,2]},\"columnKeys\":[\"c1\",\"c2\"],"
                + "\"clazz\":\"HashBasedTable\",\"rowKeys\":[1,2]}";
        Table<Long, String, Long> table = STREAMING_GSON.fromJson(shuffledTableJson, tableType);
        Assert.assertEquals(Long.valueOf(10L), table.get(1L, "c1"));
        Assert.assertEquals(Long.valueOf(20L), table.get(2L, "c2"));
        Assert.assertEquals(TREE_GSON.fromJson(shuffledTableJson, tableType), table);

        Type multimapType = new TypeToken<Multimap<Long, String>>() {
        }.getType();
        String shuffledMultimapJson = "{\"map\":{\"1\":[\"v1\",\"v2\"]},"
                + "\"unknown\":[{\"nested\":true}],\"clazz\":\"ArrayListMultimap\"}";
        Multimap<Long, String> multimap = STREAMING_GSON.fromJson(shuffledMultimapJson, multimapType);
        Assert.assertEquals(Lists.newArrayList("v1", "v2"), Lists.newArrayList(multimap.get(1L)));
        Assert.assertEquals(TREE_GSON.fromJson(shuffledMultimapJson, multimapType), multimap);
    }

    @Test
    public void testStreamingCollectionsRejectUnknownAndTruncatedPayloads() {
        Type tableType = new TypeToken<Table<Long, String, Long>>() {
        }.getType();
        assertJsonFailure("{\"clazz\":\"UnknownTable\",\"rowKeys\":[],\"columnKeys\":[],\"cells\":[]}",
                tableType, "unknown guava table class: UnknownTable");
        assertJsonFailure("{\"clazz\":\"HashBasedTable\",\"rowKeys\":[1],\"columnKeys\":[\"c\"],\"cells\":[0,0",
                tableType, "End of input");

        Type multimapType = new TypeToken<Multimap<Long, String>>() {
        }.getType();
        assertJsonFailure("{\"clazz\":\"UnknownMultimap\",\"map\":{}}", multimapType,
                "unknown guava multi map class: UnknownMultimap");
        assertJsonFailure("{\"clazz\":\"ArrayListMultimap\",\"map\":{\"1\":[\"v\"]}", multimapType,
                "End of input");
    }

    @Test
    public void testStreamingCollectionsRejectMissingDuplicateAndIncorrectFields() {
        Type tableType = new TypeToken<Table<Long, String, Long>>() {
        }.getType();
        assertJsonFailure("{\"rowKeys\":[],\"columnKeys\":[],\"cells\":[]}", tableType,
                "missing json field: clazz");
        assertJsonFailure("{\"clazz\":\"HashBasedTable\",\"clazz\":\"HashBasedTable\","
                + "\"rowKeys\":[],\"columnKeys\":[],\"cells\":[]}", tableType,
                "duplicate json field: clazz");
        assertJsonFailure("{\"clazz\":\"HashBasedTable\",\"rowKeys\":{},"
                + "\"columnKeys\":[],\"cells\":[]}", tableType, "Expected BEGIN_ARRAY");
        assertJsonFailure("{\"clazz\":\"HashBasedTable\",\"rowKeys\":[1],"
                + "\"columnKeys\":[\"c\"],\"cells\":[\"bad\",0,1]}", tableType,
                "For input string");

        Type multimapType = new TypeToken<Multimap<Long, String>>() {
        }.getType();
        assertJsonFailure("{\"clazz\":\"ArrayListMultimap\"}", multimapType,
                "missing json field: map");
        assertJsonFailure("{\"clazz\":\"ArrayListMultimap\",\"map\":{},\"map\":{}}",
                multimapType, "duplicate json field: map");
        assertJsonFailure("{\"clazz\":true,\"map\":{}}", multimapType, "Expected a string");
    }

    private static void assertJsonFailure(String json, Type type, String expectedMessage) {
        try {
            STREAMING_GSON.fromJson(json, type);
            Assert.fail("expected JSON failure");
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(expectedMessage));
        }
    }

    private static <T> void assertFourWayCompatibility(T value, Type type) {
        String treeJson = TREE_GSON.toJson(value, type);
        String streamingJson = STREAMING_GSON.toJson(value, type);
        Assert.assertEquals(treeJson, streamingJson);
        Assert.assertEquals(value, TREE_GSON.fromJson(treeJson, type));
        Assert.assertEquals(value, STREAMING_GSON.fromJson(treeJson, type));
        Assert.assertEquals(value, TREE_GSON.fromJson(streamingJson, type));
        Assert.assertEquals(value, STREAMING_GSON.fromJson(streamingJson, type));
    }

    private static class ComplexKey {
        @SerializedName("id")
        private final int id;
        @SerializedName("name")
        private final String name;

        private ComplexKey(int id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ComplexKey)) {
                return false;
            }
            ComplexKey that = (ComplexKey) other;
            return id == that.id && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name);
        }
    }
}
