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
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

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

        Type tableType = new TypeToken<Table<Long, String, Map<Long, Long>>>() {
        }.getType();
        Table<Long, String, Map<Long, Long>> table = HashBasedTable.create();
        table.put(1L, "c1", Maps.newHashMap(Map.of(10L, 100L)));
        table.put(2L, "c2", Maps.newHashMap(Map.of(20L, 200L)));

        String treeJson = TREE_GSON.toJson(table, tableType);
        String streamingJson = STREAMING_GSON.toJson(table, tableType);

        Assert.assertEquals(treeJson, streamingJson);
        Assert.assertEquals(table, STREAMING_GSON.fromJson(treeJson, tableType));
        Assert.assertEquals(table, TREE_GSON.fromJson(streamingJson, tableType));

        Table<Long, String, Map<Long, Long>> empty = HashBasedTable.create();
        Assert.assertEquals(TREE_GSON.toJson(empty, tableType), STREAMING_GSON.toJson(empty, tableType));
        Assert.assertTrue(((Table<?, ?, ?>) STREAMING_GSON.fromJson(
                STREAMING_GSON.toJson(empty, tableType), tableType)).isEmpty());
        Assert.assertEquals("null", STREAMING_GSON.toJson(null, tableType));
        Assert.assertNull(STREAMING_GSON.fromJson("null", tableType));
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
            Assert.assertEquals(multimap, STREAMING_GSON.fromJson(treeJson, multimapType));
            Assert.assertEquals(multimap, TREE_GSON.fromJson(streamingJson, multimapType));
        }

        Multimap<Long, String> empty = ArrayListMultimap.create();
        Assert.assertEquals(TREE_GSON.toJson(empty, multimapType), STREAMING_GSON.toJson(empty, multimapType));
        Assert.assertTrue(((Multimap<?, ?>) STREAMING_GSON.fromJson(
                STREAMING_GSON.toJson(empty, multimapType), multimapType)).isEmpty());
        Assert.assertEquals("null", STREAMING_GSON.toJson(null, multimapType));
        Assert.assertNull(STREAMING_GSON.fromJson("null", multimapType));
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
                tableType);
        assertJsonFailure("{\"clazz\":\"HashBasedTable\",\"rowKeys\":[1],\"columnKeys\":[\"c\"],\"cells\":[0,0",
                tableType);

        Type multimapType = new TypeToken<Multimap<Long, String>>() {
        }.getType();
        assertJsonFailure("{\"clazz\":\"UnknownMultimap\",\"map\":{}}", multimapType);
        assertJsonFailure("{\"clazz\":\"ArrayListMultimap\",\"map\":{\"1\":[\"v\"]}", multimapType);
    }

    private static void assertJsonFailure(String json, Type type) {
        try {
            STREAMING_GSON.fromJson(json, type);
            Assert.fail("expected JsonParseException");
        } catch (JsonParseException | IllegalStateException | IndexOutOfBoundsException e) {
            // Expected: malformed persistence payloads must fail instead of being partially accepted.
        }
    }
}
