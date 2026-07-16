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

import org.apache.doris.common.Config;
import org.apache.doris.persist.gson.GsonUtilsBase;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;

/**
 * Selects the streaming Table adapter only for RestoreJob's large Table fields.
 *
 * <p>The legacy delegate comes from the global Gson registration. This keeps the
 * runtime switch a real rollback path for both reads and writes, while avoiding a
 * global change for unrelated Guava Table fields.</p>
 */
public class RestoreJobTableTypeAdapterFactory implements TypeAdapterFactory {
    @Override
    public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> typeToken) {
        TypeAdapter<T> legacyAdapter = gson.getAdapter(typeToken);
        TypeAdapter<T> streamingAdapter = new GsonUtilsBase.GuavaTableTypeAdapterFactory().create(gson, typeToken);
        Preconditions.checkNotNull(streamingAdapter, "RestoreJob streaming adapter requires a Guava Table field");
        return new SwitchingTypeAdapter<>(legacyAdapter, streamingAdapter);
    }

    private static class SwitchingTypeAdapter<T> extends TypeAdapter<T> {
        private final TypeAdapter<T> legacyAdapter;
        private final TypeAdapter<T> streamingAdapter;

        SwitchingTypeAdapter(TypeAdapter<T> legacyAdapter, TypeAdapter<T> streamingAdapter) {
            this.legacyAdapter = legacyAdapter;
            this.streamingAdapter = streamingAdapter;
        }

        @Override
        public void write(JsonWriter out, T value) throws IOException {
            currentAdapter().write(out, value);
        }

        @Override
        public T read(JsonReader in) throws IOException {
            return currentAdapter().read(in);
        }

        private TypeAdapter<T> currentAdapter() {
            return Config.enable_backup_restore_job_streaming_json ? streamingAdapter : legacyAdapter;
        }
    }
}
