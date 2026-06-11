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
import com.google.gson.annotations.SerializedName;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

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
        public Table<Long, Long, String> table = HashBasedTable.create();
    }

    public static class Rectangle extends Shape {
        @SerializedName("w")
        public long width;
        @SerializedName("h")
        public long height;
    }

    public static class ConflictingShape extends Shape {
        @SerializedName("clazz")
        public String clazz = "conflict";
    }

    private static RuntimeTypeAdapterFactory<Shape> newFactory(AtomicBoolean streaming) {
        return RuntimeTypeAdapterFactory.of(Shape.class, "clazz")
                .withStreamingDispatch(streaming::get)
                .registerSubtype(Circle.class)
                .registerSubtype(Rectangle.class)
                .registerSubtype(ConflictingShape.class);
    }

    private static Gson newGson(RuntimeTypeAdapterFactory<Shape> factory) {
        return new GsonBuilder()
                .serializeSpecialFloatingPointValues()
                .enableComplexMapKeySerialization()
                .registerTypeAdapterFactory(factory)
                .registerTypeAdapterFactory(new GsonUtilsBase.GuavaTableTypeAdapterFactory())
                .create();
    }

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
        circle.table.put(1L, 2L, "v12");
        circle.table.put(1L, 3L, "v13");
        return circle;
    }

    @Test
    public void testTreeAndStreamingAreByteCompatibleAndCrossReadable() {
        AtomicBoolean streaming = new AtomicBoolean(false);
        Gson gson = newGson(newFactory(streaming));
        Circle circle = buildCircle();

        String treeJson = gson.toJson(circle, Shape.class);
        streaming.set(true);
        String streamingJson = gson.toJson(circle, Shape.class);

        Assert.assertEquals(treeJson, streamingJson);
        Assert.assertTrue(streamingJson.startsWith("{\"clazz\":\"Circle\","));

        Circle fromTree = (Circle) gson.fromJson(treeJson, Shape.class);
        Assert.assertEquals(circle.table, fromTree.table);
        Assert.assertTrue(fromTree.inner instanceof Rectangle);

        streaming.set(false);
        Circle fromStreaming = (Circle) gson.fromJson(streamingJson, Shape.class);
        Assert.assertEquals(circle.table, fromStreaming.table);
        Assert.assertTrue(fromStreaming.inner instanceof Rectangle);
    }

    @Test
    public void testStreamingReadsTypeFieldAfterPayloadFields() {
        AtomicBoolean streaming = new AtomicBoolean(true);
        Gson gson = newGson(newFactory(streaming));

        Shape shape = gson.fromJson("{\"w\":7,\"clazz\":\"Rectangle\",\"h\":8}", Shape.class);

        Assert.assertTrue(shape instanceof Rectangle);
        Assert.assertEquals(7, ((Rectangle) shape).width);
        Assert.assertEquals(8, ((Rectangle) shape).height);
    }

    @Test
    public void testStreamingReplaysLegacyPayloadWithDefaultSubtype() {
        AtomicBoolean streaming = new AtomicBoolean(true);
        RuntimeTypeAdapterFactory<Shape> factory = newFactory(streaming).registerDefaultSubtype(Circle.class);
        Gson gson = newGson(factory);

        Circle circle = (Circle) gson.fromJson("{\"n\":\"legacy\",\"r\":2.5}", Shape.class);
        Circle empty = (Circle) gson.fromJson("{}", Shape.class);

        Assert.assertEquals("legacy", circle.name);
        Assert.assertEquals(2.5, circle.radius, 0.0);
        Assert.assertNull(empty.name);
        Assert.assertEquals(0.0, empty.radius, 0.0);
    }

    @Test
    public void testNullAndEmptyCollectionsRoundTrip() {
        AtomicBoolean streaming = new AtomicBoolean(true);
        Gson gson = newGson(newFactory(streaming));
        Circle circle = new Circle();
        circle.tags.clear();
        circle.attrs.clear();
        circle.table.clear();

        Assert.assertEquals("null", gson.toJson(null, Shape.class));
        Assert.assertNull(gson.fromJson("null", Shape.class));
        Circle result = (Circle) gson.fromJson(gson.toJson(circle, Shape.class), Shape.class);
        Assert.assertTrue(result.tags.isEmpty());
        Assert.assertTrue(result.attrs.isEmpty());
        Assert.assertTrue(result.table.isEmpty());
    }

    @Test
    public void testStreamingRejectsUnknownAndDuplicateTypeFields() {
        AtomicBoolean streaming = new AtomicBoolean(true);
        Gson gson = newGson(newFactory(streaming));

        assertJsonFailure(gson, "{\"clazz\":\"Triangle\",\"n\":\"x\"}", "subtype named Triangle");
        assertJsonFailure(gson, "{\"clazz\":\"Circle\",\"clazz\":\"Rectangle\"}", "duplicate fields");
        assertJsonFailure(gson, "{\"n\":\"x\",\"clazz\":\"Circle\",\"clazz\":\"Rectangle\"}",
                "duplicate fields");
    }

    @Test
    public void testStreamingRejectsTruncatedPayload() {
        AtomicBoolean streaming = new AtomicBoolean(true);
        Gson gson = newGson(newFactory(streaming));

        assertJsonFailure(gson, "{\"clazz\":\"Circle\",\"n\":\"truncated", "Unterminated string");
        assertJsonFailure(gson, "{\"n\":\"truncated\",\"clazz\":", "End of input");
    }

    @Test
    public void testStreamingWriteRejectsConflictingTypeField() {
        AtomicBoolean streaming = new AtomicBoolean(true);
        Gson gson = newGson(newFactory(streaming));

        try {
            gson.toJson(new ConflictingShape(), Shape.class);
            Assert.fail("expected JsonParseException");
        } catch (JsonParseException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("already defines a field named clazz"));
        }
    }

    private static void assertJsonFailure(Gson gson, String json, String expectedMessage) {
        try {
            gson.fromJson(json, Shape.class);
            Assert.fail("expected JsonParseException");
        } catch (JsonParseException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(expectedMessage));
        }
    }
}
