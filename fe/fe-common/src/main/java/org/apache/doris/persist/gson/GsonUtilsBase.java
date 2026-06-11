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

import org.apache.doris.common.io.Text;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.google.protobuf.MessageLite;
import org.apache.commons.lang3.reflect.TypeUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/*
 * Some utilities about Gson.
 * User should get GSON instance from this class to do the serialization.
 *
 *      GsonUtils.GSON.toJson(...)
 *      GsonUtils.GSON.fromJson(...)
 *
 * More example can be seen in unit test case: "org.apache.doris.common.util.GsonSerializationTest.java".
 *
 * For inherited class serialization, see "org.apache.doris.common.util.GsonDerivedClassSerializationTest.java"
 *
 * And developers may need to add other serialization adapters for custom complex java classes.
 * You need implement a class to implements TypeAdapterFactory (preferred, streaming mode) or
 * JsonSerializer/JsonDeserializer (tree mode, may cause large transient memory for big objects),
 * and register it to GSON_BUILDER.
 * See the following "GuavaTableTypeAdapterFactory" and "GuavaMultimapTypeAdapterFactory" for example.
 */
public class GsonUtilsBase {

    /*
     * The exclusion strategy of GSON serialization.
     * Any fields without "@SerializedName" annotation with be ignore with
     * serializing and deserializing.
     */
    public static class HiddenAnnotationExclusionStrategy implements ExclusionStrategy {
        public boolean shouldSkipField(FieldAttributes f) {
            return f.getAnnotation(SerializedName.class) == null;
        }

        @Override
        public boolean shouldSkipClass(Class<?> clazz) {
            return false;
        }
    }

    /*
     *
     * The json adapter for Guava Table.
     * Current support:
     * 1. HashBasedTable
     *
     * The RowKey, ColumnKey and Value classes in Table should also be serializable.
     *
     * What is Adapter and Why we should implement it?
     *
     * Adapter is mainly used to provide serialization and deserialization methods for some complex classes.
     * Complex classes here usually refer to classes that are complex and cannot be modified.
     * These classes mainly include third-party library classes or some inherited classes.
     */
    public static class GuavaTableAdapter<R, C, V>
            implements JsonSerializer<Table<R, C, V>>, JsonDeserializer<Table<R, C, V>> {
        /*
         * serialize Table<R, C, V> as:
         * {
         * "rowKeys": [ "rowKey1", "rowKey2", ...],
         * "columnKeys": [ "colKey1", "colKey2", ...],
         * "cells" : [[0, 0, value1], [0, 1, value2], ...]
         * }
         *
         * the [0, 0] .. in cells are the indexes of rowKeys array and columnKeys array.
         * This serialization method can reduce the size of json string because it
         * replace the same row key
         * and column key to integer.
         */
        @Override
        public JsonElement serialize(Table<R, C, V> src, Type typeOfSrc, JsonSerializationContext context) {
            JsonArray rowKeysJsonArray = new JsonArray();
            Map<R, Integer> rowKeyToIndex = new HashMap<>();
            for (R rowKey : src.rowKeySet()) {
                rowKeyToIndex.put(rowKey, rowKeyToIndex.size());
                rowKeysJsonArray.add(context.serialize(rowKey));
            }
            JsonArray columnKeysJsonArray = new JsonArray();
            Map<C, Integer> columnKeyToIndex = new HashMap<>();
            for (C columnKey : src.columnKeySet()) {
                columnKeyToIndex.put(columnKey, columnKeyToIndex.size());
                columnKeysJsonArray.add(context.serialize(columnKey));
            }
            JsonArray cellsJsonArray = new JsonArray();
            for (Table.Cell<R, C, V> cell : src.cellSet()) {
                int rowIndex = rowKeyToIndex.get(cell.getRowKey());
                int columnIndex = columnKeyToIndex.get(cell.getColumnKey());
                cellsJsonArray.add(rowIndex);
                cellsJsonArray.add(columnIndex);
                cellsJsonArray.add(context.serialize(cell.getValue()));
            }
            JsonObject tableJsonObject = new JsonObject();
            tableJsonObject.addProperty("clazz", src.getClass().getSimpleName());
            tableJsonObject.add("rowKeys", rowKeysJsonArray);
            tableJsonObject.add("columnKeys", columnKeysJsonArray);
            tableJsonObject.add("cells", cellsJsonArray);
            return tableJsonObject;
        }

        @Override
        public Table<R, C, V> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) {
            Type typeOfR;
            Type typeOfC;
            Type typeOfV;
            { // CHECKSTYLE IGNORE THIS LINE
                ParameterizedType parameterizedType = (ParameterizedType) typeOfT;
                Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                typeOfR = actualTypeArguments[0];
                typeOfC = actualTypeArguments[1];
                typeOfV = actualTypeArguments[2];
            } // CHECKSTYLE IGNORE THIS LINE
            JsonObject tableJsonObject = json.getAsJsonObject();
            String tableClazz = tableJsonObject.get("clazz").getAsString();
            JsonArray rowKeysJsonArray = tableJsonObject.getAsJsonArray("rowKeys");
            Map<Integer, R> rowIndexToKey = new HashMap<>();
            for (JsonElement jsonElement : rowKeysJsonArray) {
                R rowKey = context.deserialize(jsonElement, typeOfR);
                rowIndexToKey.put(rowIndexToKey.size(), rowKey);
            }
            JsonArray columnKeysJsonArray = tableJsonObject.getAsJsonArray("columnKeys");
            Map<Integer, C> columnIndexToKey = new HashMap<>();
            for (JsonElement jsonElement : columnKeysJsonArray) {
                C columnKey = context.deserialize(jsonElement, typeOfC);
                columnIndexToKey.put(columnIndexToKey.size(), columnKey);
            }
            JsonArray cellsJsonArray = tableJsonObject.getAsJsonArray("cells");
            Table<R, C, V> table = null;
            switch (tableClazz) {
                case "HashBasedTable":
                    table = HashBasedTable.create();
                    break;
                default:
                    Preconditions.checkState(false, "unknown guava table class: " + tableClazz);
                    break;
            }
            for (int i = 0; i < cellsJsonArray.size(); i = i + 3) {
                // format is [rowIndex, columnIndex, value]
                int rowIndex = cellsJsonArray.get(i).getAsInt();
                int columnIndex = cellsJsonArray.get(i + 1).getAsInt();
                R rowKey = rowIndexToKey.get(rowIndex);
                C columnKey = columnIndexToKey.get(columnIndex);
                V value = context.deserialize(cellsJsonArray.get(i + 2), typeOfV);
                table.put(rowKey, columnKey, value);
            }
            return table;
        }
    }

    /*
     * The json adapter for Guava Multimap.
     * Current support:
     * 1. ArrayListMultimap
     * 2. HashMultimap
     * 3. LinkedListMultimap
     * 4. LinkedHashMultimap
     *
     * The key and value classes of multi map should also be json serializable.
     */
    public static class GuavaMultimapAdapter<K, V>
            implements JsonSerializer<Multimap<K, V>>, JsonDeserializer<Multimap<K, V>> {

        private static final Type asMapReturnType = getAsMapMethod().getGenericReturnType();

        private static Type asMapType(Type multimapType) {
            return com.google.common.reflect.TypeToken.of(multimapType).resolveType(asMapReturnType).getType();
        }

        private static Method getAsMapMethod() {
            try {
                return Multimap.class.getDeclaredMethod("asMap");
            } catch (NoSuchMethodException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public JsonElement serialize(Multimap<K, V> map, Type typeOfSrc, JsonSerializationContext context) {
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("clazz", map.getClass().getSimpleName());
            Map<K, Collection<V>> asMap = map.asMap();
            Type type = asMapType(typeOfSrc);
            JsonElement jsonElement = context.serialize(asMap, type);
            jsonObject.add("map", jsonElement);
            return jsonObject;
        }

        @Override
        public Multimap<K, V> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException {
            JsonObject jsonObject = json.getAsJsonObject();
            String clazz = jsonObject.get("clazz").getAsString();

            JsonElement mapElement = jsonObject.get("map");
            Map<K, Collection<V>> asMap = context.deserialize(mapElement, asMapType(typeOfT));

            Multimap<K, V> map = null;
            switch (clazz) {
                case "ArrayListMultimap":
                    map = ArrayListMultimap.create();
                    break;
                case "HashMultimap":
                    map = HashMultimap.create();
                    break;
                case "LinkedListMultimap":
                    map = LinkedListMultimap.create();
                    break;
                case "LinkedHashMultimap":
                    map = LinkedHashMultimap.create();
                    break;
                default:
                    Preconditions.checkState(false, "unknown guava multi map class: " + clazz);
                    break;
            }

            for (Map.Entry<K, Collection<V>> entry : asMap.entrySet()) {
                map.putAll(entry.getKey(), entry.getValue());
            }
            return map;
        }
    }

    /*
     *
     * The streaming json adapter factory for Guava Table.
     * Current support:
     * 1. HashBasedTable
     *
     * The RowKey, ColumnKey and Value classes in Table should also be serializable.
     *
     * serialize Table<R, C, V> as:
     * {
     * "clazz": "HashBasedTable",
     * "rowKeys": [ "rowKey1", "rowKey2", ...],
     * "columnKeys": [ "colKey1", "colKey2", ...],
     * "cells" : [0, 0, value1, 0, 1, value2, ...]
     * }
     *
     * the [0, 0] .. in cells are the indexes of rowKeys array and columnKeys array.
     * This serialization method can reduce the size of json string because it
     * replace the same row key
     * and column key to integer.
     *
     * Why TypeAdapterFactory instead of JsonSerializer/JsonDeserializer?
     *
     * JsonSerializer/JsonDeserializer works in tree mode, which materializes the whole
     * JsonElement DOM tree in memory before writing (or after parsing). For large tables
     * (e.g. RestoreJob.snapshotInfos with tens of thousands of tablets), the transient DOM
     * costs tens of times more memory than the serialized bytes and easily causes FE heap
     * spikes. The streaming TypeAdapter writes to JsonWriter and reads from JsonReader
     * directly, without building the DOM.
     */
    public static class GuavaTableTypeAdapterFactory implements TypeAdapterFactory {
        @Override
        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> typeToken) {
            if (!Table.class.isAssignableFrom(typeToken.getRawType())) {
                return null;
            }
            Map<TypeVariable<?>, Type> typeArgs = TypeUtils.getTypeArguments(typeToken.getType(), Table.class);
            TypeVariable<?>[] typeVars = Table.class.getTypeParameters();
            TypeAdapter<?> rowKeyAdapter = gson.getAdapter(TypeToken.get(resolveTypeArgument(typeArgs, typeVars[0])));
            TypeAdapter<?> columnKeyAdapter
                    = gson.getAdapter(TypeToken.get(resolveTypeArgument(typeArgs, typeVars[1])));
            TypeAdapter<?> valueAdapter = gson.getAdapter(TypeToken.get(resolveTypeArgument(typeArgs, typeVars[2])));
            @SuppressWarnings("unchecked")
            TypeAdapter<T> adapter = (TypeAdapter<T>) new TableTypeAdapter<>(gson, rowKeyAdapter,
                    columnKeyAdapter, valueAdapter).nullSafe();
            return adapter;
        }

        private static Type resolveTypeArgument(Map<TypeVariable<?>, Type> typeArgs, TypeVariable<?> typeVar) {
            Type type = typeArgs == null ? null : typeArgs.get(typeVar);
            // unresolvable type argument (e.g. raw Table), fall back to runtime type dispatch
            return (type == null || type instanceof TypeVariable) ? Object.class : type;
        }

        private static class TableTypeAdapter<R, C, V> extends TypeAdapter<Table<R, C, V>> {
            private final Gson gson;
            private final TypeAdapter<R> rowKeyAdapter;
            private final TypeAdapter<C> columnKeyAdapter;
            private final TypeAdapter<V> valueAdapter;

            @SuppressWarnings("unchecked")
            TableTypeAdapter(Gson gson, TypeAdapter<?> rowKeyAdapter, TypeAdapter<?> columnKeyAdapter,
                    TypeAdapter<?> valueAdapter) {
                this.gson = gson;
                this.rowKeyAdapter = (TypeAdapter<R>) rowKeyAdapter;
                this.columnKeyAdapter = (TypeAdapter<C>) columnKeyAdapter;
                this.valueAdapter = (TypeAdapter<V>) valueAdapter;
            }

            @Override
            public void write(JsonWriter out, Table<R, C, V> src) throws IOException {
                out.beginObject();
                out.name("clazz").value(src.getClass().getSimpleName());
                out.name("rowKeys");
                out.beginArray();
                Map<R, Integer> rowKeyToIndex = new HashMap<>();
                for (R rowKey : src.rowKeySet()) {
                    rowKeyToIndex.put(rowKey, rowKeyToIndex.size());
                    writeByRuntimeType(out, rowKey);
                }
                out.endArray();
                out.name("columnKeys");
                out.beginArray();
                Map<C, Integer> columnKeyToIndex = new HashMap<>();
                for (C columnKey : src.columnKeySet()) {
                    columnKeyToIndex.put(columnKey, columnKeyToIndex.size());
                    writeByRuntimeType(out, columnKey);
                }
                out.endArray();
                out.name("cells");
                out.beginArray();
                for (Table.Cell<R, C, V> cell : src.cellSet()) {
                    out.value((long) rowKeyToIndex.get(cell.getRowKey()));
                    out.value((long) columnKeyToIndex.get(cell.getColumnKey()));
                    writeByRuntimeType(out, cell.getValue());
                }
                out.endArray();
                out.endObject();
            }

            // dispatch by runtime class, same as the tree mode JsonSerializationContext.serialize(src)
            @SuppressWarnings("unchecked")
            private void writeByRuntimeType(JsonWriter out, Object src) throws IOException {
                TypeAdapter<Object> adapter = (TypeAdapter<Object>) gson.getAdapter(src.getClass());
                adapter.write(out, src);
            }

            @Override
            public Table<R, C, V> read(JsonReader in) throws IOException {
                Table<R, C, V> table = null;
                List<R> rowKeys = null;
                List<C> columnKeys = null;
                List<PendingCell<V>> pendingCells = new ArrayList<>();
                boolean clazzSeen = false;
                boolean rowKeysSeen = false;
                boolean columnKeysSeen = false;
                boolean cellsSeen = false;
                in.beginObject();
                while (in.hasNext()) {
                    String name = in.nextName();
                    switch (name) {
                        case "clazz":
                            Preconditions.checkState(!clazzSeen, "duplicate json field: clazz");
                            clazzSeen = true;
                            table = createTable(in.nextString());
                            break;
                        case "rowKeys":
                            Preconditions.checkState(!rowKeysSeen, "duplicate json field: rowKeys");
                            rowKeysSeen = true;
                            rowKeys = readArray(in, rowKeyAdapter);
                            break;
                        case "columnKeys":
                            Preconditions.checkState(!columnKeysSeen, "duplicate json field: columnKeys");
                            columnKeysSeen = true;
                            columnKeys = readArray(in, columnKeyAdapter);
                            break;
                        case "cells":
                            Preconditions.checkState(!cellsSeen, "duplicate json field: cells");
                            cellsSeen = true;
                            readCells(in, table, rowKeys, columnKeys, pendingCells);
                            break;
                        default:
                            in.skipValue();
                            break;
                    }
                }
                in.endObject();
                Preconditions.checkState(clazzSeen, "missing json field: clazz");
                Preconditions.checkState(rowKeysSeen, "missing json field: rowKeys");
                Preconditions.checkState(columnKeysSeen, "missing json field: columnKeys");
                Preconditions.checkState(cellsSeen, "missing json field: cells");
                for (PendingCell<V> cell : pendingCells) {
                    table.put(rowKeys.get(cell.rowIndex), columnKeys.get(cell.columnIndex), cell.value);
                }
                return table;
            }

            private <E> List<E> readArray(JsonReader in, TypeAdapter<E> adapter) throws IOException {
                List<E> values = new ArrayList<>();
                in.beginArray();
                while (in.hasNext()) {
                    values.add(adapter.read(in));
                }
                in.endArray();
                return values;
            }

            private void readCells(JsonReader in, Table<R, C, V> table, List<R> rowKeys, List<C> columnKeys,
                    List<PendingCell<V>> pendingCells) throws IOException {
                in.beginArray();
                // format is [rowIndex, columnIndex, value, rowIndex, columnIndex, value, ...]
                while (in.hasNext()) {
                    int rowIndex = in.nextInt();
                    int columnIndex = in.nextInt();
                    V value = valueAdapter.read(in);
                    if (table != null && rowKeys != null && columnKeys != null) {
                        table.put(rowKeys.get(rowIndex), columnKeys.get(columnIndex), value);
                    } else {
                        pendingCells.add(new PendingCell<>(rowIndex, columnIndex, value));
                    }
                }
                in.endArray();
            }

            private static class PendingCell<V> {
                private final int rowIndex;
                private final int columnIndex;
                private final V value;

                PendingCell(int rowIndex, int columnIndex, V value) {
                    this.rowIndex = rowIndex;
                    this.columnIndex = columnIndex;
                    this.value = value;
                }
            }
        }
    }

    /*
     * The streaming json adapter factory for Guava Multimap.
     * Current support:
     * 1. ArrayListMultimap
     * 2. HashMultimap
     * 3. LinkedListMultimap
     * 4. LinkedHashMultimap
     *
     * The key and value classes of multi map should also be json serializable.
     *
     * serialize Multimap<K, V> as:
     * {
     * "clazz": "HashMultimap",
     * "map": { ... the asMap() view of the multimap ... }
     * }
     *
     * Same as GuavaTableTypeAdapterFactory, it works in streaming mode to avoid
     * materializing the JsonElement DOM tree of the whole map.
     */
    public static class GuavaMultimapTypeAdapterFactory implements TypeAdapterFactory {

        private static final Type asMapReturnType = getAsMapMethod().getGenericReturnType();

        private static Type asMapType(Type multimapType) {
            return com.google.common.reflect.TypeToken.of(multimapType).resolveType(asMapReturnType).getType();
        }

        private static Method getAsMapMethod() {
            try {
                return Multimap.class.getDeclaredMethod("asMap");
            } catch (NoSuchMethodException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> typeToken) {
            if (!Multimap.class.isAssignableFrom(typeToken.getRawType())) {
                return null;
            }
            TypeAdapter<?> asMapAdapter = gson.getAdapter(TypeToken.get(asMapType(typeToken.getType())));
            @SuppressWarnings("unchecked")
            TypeAdapter<T> adapter = (TypeAdapter<T>) new MultimapTypeAdapter<>(asMapAdapter).nullSafe();
            return adapter;
        }

        private static class MultimapTypeAdapter<K, V> extends TypeAdapter<Multimap<K, V>> {
            private final TypeAdapter<Map<K, Collection<V>>> asMapAdapter;

            @SuppressWarnings("unchecked")
            MultimapTypeAdapter(TypeAdapter<?> asMapAdapter) {
                this.asMapAdapter = (TypeAdapter<Map<K, Collection<V>>>) asMapAdapter;
            }

            @Override
            public void write(JsonWriter out, Multimap<K, V> src) throws IOException {
                out.beginObject();
                out.name("clazz").value(src.getClass().getSimpleName());
                out.name("map");
                asMapAdapter.write(out, src.asMap());
                out.endObject();
            }

            @Override
            public Multimap<K, V> read(JsonReader in) throws IOException {
                Multimap<K, V> map = null;
                Map<K, Collection<V>> asMap = null;
                boolean clazzSeen = false;
                boolean mapSeen = false;
                in.beginObject();
                while (in.hasNext()) {
                    String name = in.nextName();
                    switch (name) {
                        case "clazz":
                            Preconditions.checkState(!clazzSeen, "duplicate json field: clazz");
                            clazzSeen = true;
                            map = createMultimap(in.nextString());
                            break;
                        case "map":
                            Preconditions.checkState(!mapSeen, "duplicate json field: map");
                            mapSeen = true;
                            asMap = asMapAdapter.read(in);
                            break;
                        default:
                            in.skipValue();
                            break;
                    }
                }
                in.endObject();
                Preconditions.checkState(clazzSeen, "missing json field: clazz");
                Preconditions.checkState(mapSeen, "missing json field: map");
                for (Map.Entry<K, Collection<V>> entry : asMap.entrySet()) {
                    map.putAll(entry.getKey(), entry.getValue());
                }
                return map;
            }
        }
    }

    private static <R, C, V> Table<R, C, V> createTable(String clazz) {
        Preconditions.checkState("HashBasedTable".equals(clazz), "unknown guava table class: " + clazz);
        return HashBasedTable.create();
    }

    private static <K, V> Multimap<K, V> createMultimap(String clazz) {
        switch (clazz) {
            case "ArrayListMultimap":
                return ArrayListMultimap.create();
            case "HashMultimap":
                return HashMultimap.create();
            case "LinkedListMultimap":
                return LinkedListMultimap.create();
            case "LinkedHashMultimap":
                return LinkedHashMultimap.create();
            default:
                throw new IllegalStateException("unknown guava multi map class: " + clazz);
        }
    }

    public static class AtomicBooleanAdapter
            implements JsonSerializer<AtomicBoolean>, JsonDeserializer<AtomicBoolean> {

        @Override
        public AtomicBoolean deserialize(JsonElement jsonElement, Type type,
                JsonDeserializationContext jsonDeserializationContext)
                throws JsonParseException {
            JsonObject jsonObject = jsonElement.getAsJsonObject();
            boolean value = jsonObject.get("boolean").getAsBoolean();
            return new AtomicBoolean(value);
        }

        @Override
        public JsonElement serialize(AtomicBoolean atomicBoolean, Type type,
                JsonSerializationContext jsonSerializationContext) {
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("boolean", atomicBoolean.get());
            return jsonObject;
        }
    }

    public static final class ImmutableMapDeserializer implements JsonDeserializer<ImmutableMap<?, ?>> {
        @Override
        public ImmutableMap<?, ?> deserialize(final JsonElement json, final Type type,
                final JsonDeserializationContext context) throws JsonParseException {
            final Type type2 = TypeUtils.parameterize(Map.class, ((ParameterizedType) type).getActualTypeArguments());
            final Map<?, ?> map = context.deserialize(json, type2);
            return ImmutableMap.copyOf(map);
        }
    }

    public static final class ImmutableListDeserializer implements JsonDeserializer<ImmutableList<?>> {
        @Override
        public ImmutableList<?> deserialize(final JsonElement json, final Type type,
                final JsonDeserializationContext context) throws JsonParseException {
            final Type type2 = TypeUtils.parameterize(List.class, ((ParameterizedType) type).getActualTypeArguments());
            final List<?> list = context.deserialize(json, type2);
            return ImmutableList.copyOf(list);
        }
    }

    public static class PreProcessTypeAdapterFactory implements TypeAdapterFactory {

        public PreProcessTypeAdapterFactory() {
        }

        @Override
        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
            TypeAdapter<T> delegate = gson.getDelegateAdapter(this, type);

            return new TypeAdapter<T>() {
                public void write(JsonWriter out, T value) throws IOException {
                    if (value instanceof GsonPreProcessable) {
                        ((GsonPreProcessable) value).gsonPreProcess();
                    }
                    delegate.write(out, value);
                }

                public T read(JsonReader reader) throws IOException {
                    return delegate.read(reader);
                }
            };
        }
    }

    public static class PostProcessTypeAdapterFactory implements TypeAdapterFactory {

        public PostProcessTypeAdapterFactory() {
        }

        @Override
        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
            TypeAdapter<T> delegate = gson.getDelegateAdapter(this, type);

            return new TypeAdapter<T>() {
                public void write(JsonWriter out, T value) throws IOException {
                    delegate.write(out, value);
                }

                public T read(JsonReader reader) throws IOException {
                    T obj = delegate.read(reader);
                    if (obj instanceof GsonPostProcessable) {
                        ((GsonPostProcessable) obj).gsonPostProcess();
                    }
                    return obj;
                }
            };
        }
    }

    public static class SkipClassExclusionStrategy implements ExclusionStrategy {
        @Override
        public boolean shouldSkipField(FieldAttributes f) {
            return false;
        }

        @Override
        public boolean shouldSkipClass(Class<?> clazz) {
            // due to java.lang.IllegalArgumentException: com.lmax.disruptor.RingBuffer
            // <org.apache.doris.scheduler.disruptor.TimerTaskEvent> declares multiple
            // JSON fields named p1
            return clazz.getName().startsWith("com.lmax.disruptor.RingBuffer")
                    // Protobuf 4 builders expose duplicate internal fields such as
                    // "meAsParent". They are runtime-only and must not enter FE metadata.
                    || MessageLite.Builder.class.isAssignableFrom(clazz);
        }
    }

    public static void toJsonCompressed(DataOutput out, Object src, Gson gson) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream)) {
            try (OutputStreamWriter writer = new OutputStreamWriter(gzipStream)) {
                gson.toJson(src, writer);
            }
        }
        Text text = new Text(byteStream.toByteArray());
        text.write(out);
    }

    public static <T> T fromJsonCompressed(DataInput in, Class<T> clazz, Gson gson) throws IOException {
        Text text = new Text();
        text.readFields(in);

        ByteArrayInputStream byteStream = new ByteArrayInputStream(text.getBytes());
        try (GZIPInputStream gzipStream = new GZIPInputStream(byteStream)) {
            try (InputStreamReader reader = new InputStreamReader(gzipStream)) {
                return gson.fromJson(reader, clazz);
            }
        }
    }
}
