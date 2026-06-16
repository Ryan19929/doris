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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.annotations.SerializedName;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class RuntimeTypeAdapterFactoryStreamingTest {
    public abstract static class Shape {
        @SerializedName("n")
        public String name;
    }

    public static class Circle extends Shape {
        @SerializedName("r")
        public double radius;
        @SerializedName("tags")
        public List<String> tags = Lists.newArrayList();
        @SerializedName("attrs")
        public Map<String, Long> attrs = Maps.newHashMap();
        @SerializedName("inner")
        public Shape inner;
        @SerializedName("tbl")
        @JsonAdapter(GsonUtilsBase.GuavaTableTypeAdapterFactory.class)
        public Table<Long, Long, String> tbl = HashBasedTable.create();
    }

    public static class Rectangle extends Shape {
        @SerializedName("w")
        public long width;
        @SerializedName("h")
        public long height;
    }

    public static class BadShape extends Shape {
        @SerializedName("clazz")
        public String clazz = "boom";
    }

    private static RuntimeTypeAdapterFactory<Shape> newFactory(boolean dynamicStreaming) {
        RuntimeTypeAdapterFactory<Shape> factory = RuntimeTypeAdapterFactory.of(Shape.class, "clazz");
        if (dynamicStreaming) {
            factory.withStreamingDispatch(BackupRestoreJobJsonMode::isStreaming);
        }
        return factory.registerSubtype(Circle.class, Circle.class.getSimpleName())
                .registerSubtype(Rectangle.class, Rectangle.class.getSimpleName())
                .registerSubtype(BadShape.class, BadShape.class.getSimpleName());
    }

    private static Gson newGson(boolean dynamicStreaming) {
        return new GsonBuilder()
                .serializeSpecialFloatingPointValues()
                .enableComplexMapKeySerialization()
                .registerTypeAdapterFactory(newFactory(dynamicStreaming))
                .create();
    }

    private static final Gson TREE_GSON = newGson(false);
    private static final Gson DYNAMIC_GSON = newGson(true);

    private static Circle buildCircle() {
        Circle circle = new Circle();
        circle.name = "special <html> &chars\" 中文";
        circle.radius = 3.25;
        circle.tags.add("a");
        circle.tags.add(null);
        circle.tags.add("b");
        circle.attrs.put("x", 1L);
        Rectangle inner = new Rectangle();
        inner.width = 10;
        inner.height = 20;
        circle.inner = inner;
        circle.tbl.put(1L, 2L, "v12");
        circle.tbl.put(1L, 3L, "v13");
        circle.tbl.put(4L, 2L, "v42");
        return circle;
    }

    @Test
    public void testDynamicStreamingWriteByteIdenticalWithTree() {
        Circle circle = buildCircle();
        String treeJson = TREE_GSON.toJson(circle, Shape.class);
        String streamingJson;
        try (BackupRestoreJobJsonMode.Scope ignored = BackupRestoreJobJsonMode.withMode(true)) {
            streamingJson = DYNAMIC_GSON.toJson(circle, Shape.class);
        }
        Assert.assertEquals(treeJson, streamingJson);
        Assert.assertTrue(streamingJson.startsWith("{\"clazz\":\"Circle\","));
    }

    @Test
    public void testDynamicSwitchFallsBackToTreeRead() {
        String typeFieldSecond = "{\"w\":7,\"clazz\":\"Rectangle\",\"h\":8}";
        Rectangle byTree = (Rectangle) TREE_GSON.fromJson(typeFieldSecond, Shape.class);
        Assert.assertEquals(7, byTree.width);

        try (BackupRestoreJobJsonMode.Scope ignored = BackupRestoreJobJsonMode.withMode(false)) {
            Rectangle byDynamicTree = (Rectangle) DYNAMIC_GSON.fromJson(typeFieldSecond, Shape.class);
            Assert.assertEquals(7, byDynamicTree.width);
        }

        try (BackupRestoreJobJsonMode.Scope ignored = BackupRestoreJobJsonMode.withMode(true)) {
            DYNAMIC_GSON.fromJson(typeFieldSecond, Shape.class);
            Assert.fail("expect JsonParseException");
        } catch (JsonParseException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("the first field is 'w'"));
        }
    }

    @Test
    public void testCrossModeRoundTrip() {
        Circle circle = buildCircle();
        String treeJson = TREE_GSON.toJson(circle, Shape.class);
        String streamingJson;
        try (BackupRestoreJobJsonMode.Scope ignored = BackupRestoreJobJsonMode.withMode(true)) {
            streamingJson = DYNAMIC_GSON.toJson(circle, Shape.class);
        }

        try (BackupRestoreJobJsonMode.Scope ignored = BackupRestoreJobJsonMode.withMode(true)) {
            Circle fromTreeByStreaming = (Circle) DYNAMIC_GSON.fromJson(treeJson, Shape.class);
            Assert.assertEquals(circle.tbl, fromTreeByStreaming.tbl);
        }
        try (BackupRestoreJobJsonMode.Scope ignored = BackupRestoreJobJsonMode.withMode(false)) {
            Circle fromStreamingByTree = (Circle) DYNAMIC_GSON.fromJson(streamingJson, Shape.class);
            Assert.assertEquals(circle.tbl, fromStreamingByTree.tbl);
        }
    }

    @Test
    public void testStreamingWriteRejectsConflictTypeField() {
        try (BackupRestoreJobJsonMode.Scope ignored = BackupRestoreJobJsonMode.withMode(true)) {
            DYNAMIC_GSON.toJson(new BadShape(), Shape.class);
            Assert.fail("expect JsonParseException");
        } catch (JsonParseException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("already defines a field named clazz"));
        }
    }
}
