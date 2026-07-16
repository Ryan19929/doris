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

package org.apache.doris.common.io;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

class LengthPrefixedJsonStreamTest {
    private static final Gson GSON = new Gson();

    @Test
    void matchesTextAndSupportsCrossRead() throws Exception {
        Value value = new Value("备份-" + (char) 0xd800, 7);
        ByteArrayOutputStream legacyBytes = new ByteArrayOutputStream();
        Text.writeString(new DataOutputStream(legacyBytes), GSON.toJson(value));

        ByteArrayOutputStream streamingBytes = new ByteArrayOutputStream();
        LengthPrefixedJsonStream.write(new DataOutputStream(streamingBytes), value, GSON);
        Assertions.assertArrayEquals(legacyBytes.toByteArray(), streamingBytes.toByteArray());

        DataInputStream legacyInput = new DataInputStream(new ByteArrayInputStream(streamingBytes.toByteArray()));
        Value legacyValue = GSON.fromJson(Text.readString(legacyInput), Value.class);
        DataInputStream streamingInput = new DataInputStream(new ByteArrayInputStream(legacyBytes.toByteArray()));
        Assertions.assertEquals(legacyValue,
                LengthPrefixedJsonStream.read(streamingInput, Value.class, GSON));
    }

    @Test
    void supportsDataInputThatIsNotAnInputStreamAndPreservesNextField() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(bytes);
        LengthPrefixedJsonStream.write(output, new Value("large", 11), GSON);
        output.writeLong(987654321L);

        DataInputStream delegate = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
        DataInput dataInput = new DelegatingDataInput(delegate);
        Assertions.assertEquals(new Value("large", 11),
                LengthPrefixedJsonStream.read(dataInput, Value.class, GSON));
        Assertions.assertEquals(987654321L, dataInput.readLong());
    }

    @Test
    void makesProgressWhenUnderlyingInputStreamInitiallyReturnsZero() throws Exception {
        Value expected = new Value("zero-then-data", 17);
        byte[] json = GSON.toJson(expected).getBytes(StandardCharsets.UTF_8);
        DataInputStream input = new DataInputStream(new ZeroThenDataInputStream(json));

        Assertions.assertEquals(expected, LengthPrefixedJsonStream.read(input, json.length, Value.class, GSON));
    }

    @Test
    void rejectsNegativeLengthTruncationAndTrailingGarbage() throws Exception {
        Assertions.assertThrows(IOException.class,
                () -> LengthPrefixedJsonStream.read(dataInputWithLength(-1), Value.class, GSON));

        byte[] json = GSON.toJson(new Value("short", 1)).getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream truncated = new ByteArrayOutputStream();
        DataOutputStream truncatedOut = new DataOutputStream(truncated);
        truncatedOut.writeInt(json.length + 1);
        truncatedOut.write(json);
        Assertions.assertThrows(EOFException.class,
                () -> LengthPrefixedJsonStream.read(
                        new DataInputStream(new ByteArrayInputStream(truncated.toByteArray())), Value.class, GSON));

        byte[] trailingJson = (GSON.toJson(new Value("tail", 2)) + "x").getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream trailing = new ByteArrayOutputStream();
        DataOutputStream trailingOut = new DataOutputStream(trailing);
        trailingOut.writeInt(trailingJson.length);
        trailingOut.write(trailingJson);
        Assertions.assertThrows(JsonParseException.class,
                () -> LengthPrefixedJsonStream.read(
                        new DataInputStream(new ByteArrayInputStream(trailing.toByteArray())), Value.class, GSON));
    }

    @Test
    void replacesMalformedUtf8LikeText() throws Exception {
        byte[] prefix = "{\"text\":\"".getBytes(StandardCharsets.UTF_8);
        byte[] suffix = "(\" ,\"number\":3}".getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[prefix.length + 1 + suffix.length];
        System.arraycopy(prefix, 0, payload, 0, prefix.length);
        payload[prefix.length] = (byte) 0xc3;
        System.arraycopy(suffix, 0, payload, prefix.length + 1, suffix.length);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(bytes);
        output.writeInt(payload.length);
        output.write(payload);
        byte[] encoded = bytes.toByteArray();

        Value streamed = LengthPrefixedJsonStream.read(
                new DataInputStream(new ByteArrayInputStream(encoded)), Value.class, GSON);
        Value legacy = GSON.fromJson(Text.readString(
                new DataInputStream(new ByteArrayInputStream(encoded))), Value.class);
        Assertions.assertEquals(legacy, streamed);
        Assertions.assertEquals(String.valueOf((char) 0xfffd) + "(", streamed.text);
    }

    @Test
    void handlesMultipleSegmentsAndLeavesDestinationUntouchedOnSerializationFailure() throws Exception {
        char[] chars = new char[256 * 1024];
        Arrays.fill(chars, '中');
        Value value = new Value(new String(chars), 13);
        try (LengthPrefixedJsonStream.JsonBuffer buffer = LengthPrefixedJsonStream.serialize(value, GSON)) {
            Assertions.assertTrue(buffer.size() > 64 * 1024);
            Assertions.assertEquals(value, LengthPrefixedJsonStream.read(buffer, Value.class, GSON));
        }

        ByteArrayOutputStream destination = new ByteArrayOutputStream();
        Assertions.assertThrows(IOException.class, () -> {
            LengthPrefixedJsonStream.serialize(value, GSON, 8);
        });

        Gson failingGson = new GsonBuilder().registerTypeAdapter(Value.class, new TypeAdapter<Value>() {
            @Override
            public void write(JsonWriter out, Value ignored) throws IOException {
                throw new IOException("expected serialization failure");
            }

            @Override
            public Value read(JsonReader in) {
                throw new UnsupportedOperationException();
            }
        }).create();
        Assertions.assertThrows(IOException.class,
                () -> LengthPrefixedJsonStream.write(new DataOutputStream(destination), value, failingGson));
        Assertions.assertEquals(0, destination.size());
    }

    @Test
    void spillsAtLowThresholdAndReleasesBufferAfterRoundTrip() throws Exception {
        Value value = new Value("spill-" + "备份".repeat(1024), 19);
        ByteArrayOutputStream legacyBytes = new ByteArrayOutputStream();
        Text.writeString(new DataOutputStream(legacyBytes), GSON.toJson(value));

        LengthPrefixedJsonStream.JsonBuffer buffer =
                LengthPrefixedJsonStream.serialize(value, GSON, Integer.MAX_VALUE, 32);
        Assertions.assertTrue(buffer.isSpilled());
        try {
            ByteArrayOutputStream streamingBytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(streamingBytes);
            output.writeInt(buffer.size());
            buffer.writeTo(output);
            Assertions.assertArrayEquals(legacyBytes.toByteArray(), streamingBytes.toByteArray());
            Assertions.assertEquals(value, LengthPrefixedJsonStream.read(buffer, Value.class, GSON));
        } finally {
            buffer.close();
        }

        Assertions.assertThrows(IOException.class,
                () -> buffer.writeTo(new DataOutputStream(new ByteArrayOutputStream())));
        Assertions.assertThrows(IOException.class,
                () -> LengthPrefixedJsonStream.read(buffer, Value.class, GSON));
    }

    @Test
    void resetsSpillAfterSerializationAndDestinationFailures() throws Exception {
        IOException serializationFailure = new IOException("expected serialization failure");
        TrackingBufferStore serializationStore = new TrackingBufferStore();
        Gson failingGson = new GsonBuilder().registerTypeAdapter(Value.class, new TypeAdapter<Value>() {
            @Override
            public void write(JsonWriter out, Value ignored) throws IOException {
                out.beginObject();
                out.name("text").value("x".repeat(4096));
                out.flush();
                throw serializationFailure;
            }

            @Override
            public Value read(JsonReader in) {
                throw new UnsupportedOperationException();
            }
        }).create();

        IOException actualSerializationFailure = Assertions.assertThrows(IOException.class,
                () -> LengthPrefixedJsonStream.serialize(
                        new Value("ignored", 1), failingGson, Integer.MAX_VALUE, 32, serializationStore));
        Assertions.assertSame(serializationFailure, actualSerializationFailure);
        Assertions.assertEquals(1, serializationStore.resetCount);
        Assertions.assertEquals(0, serializationStore.size());

        IOException destinationFailure = new IOException("expected destination failure");
        TrackingBufferStore destinationStore = new TrackingBufferStore();
        DataOutputStream failingDestination = new DataOutputStream(new OutputStream() {
            @Override
            public void write(int value) throws IOException {
                throw destinationFailure;
            }

            @Override
            public void write(byte[] bytes, int offset, int length) throws IOException {
                throw destinationFailure;
            }
        });
        IOException actualDestinationFailure = Assertions.assertThrows(IOException.class,
                () -> LengthPrefixedJsonStream.write(failingDestination,
                        new Value("y".repeat(4096), 2), GSON, Integer.MAX_VALUE, 32, destinationStore));
        Assertions.assertSame(destinationFailure, actualDestinationFailure);
        Assertions.assertEquals(1, destinationStore.resetCount);
        Assertions.assertEquals(0, destinationStore.size());
    }

    @Test
    void preservesPrimaryFailureWhenSpillCleanupFails() throws Exception {
        IOException cleanupFailure = new IOException("expected cleanup failure");
        TrackingBufferStore store = new TrackingBufferStore(cleanupFailure);
        LengthPrefixedJsonStream.JsonBuffer buffer = LengthPrefixedJsonStream.serialize(
                new Value("z".repeat(4096), 3), GSON, Integer.MAX_VALUE, 32, store);
        IOException primaryFailure = new IOException("primary failure");
        IOException actual = Assertions.assertThrows(IOException.class, () -> {
            try (LengthPrefixedJsonStream.JsonBuffer ignored = buffer) {
                throw primaryFailure;
            }
        });
        Assertions.assertSame(primaryFailure, actual);
        Assertions.assertArrayEquals(new Throwable[] {cleanupFailure}, actual.getSuppressed());
    }

    @Test
    void enforcesPayloadLimitAfterSpilling() throws Exception {
        Value value = new Value("limit-" + "x".repeat(4096), 23);
        int payloadSize = GSON.toJson(value).getBytes(StandardCharsets.UTF_8).length;
        try (LengthPrefixedJsonStream.JsonBuffer buffer =
                     LengthPrefixedJsonStream.serialize(value, GSON, payloadSize, 32)) {
            Assertions.assertEquals(payloadSize, buffer.size());
            Assertions.assertTrue(buffer.isSpilled());
        }
        Assertions.assertThrows(IOException.class,
                () -> LengthPrefixedJsonStream.serialize(value, GSON, payloadSize - 1L, 32));

        TrackingBufferStore limitedStore = new TrackingBufferStore();
        Assertions.assertThrows(IOException.class,
                () -> LengthPrefixedJsonStream.serialize(
                        value, GSON, payloadSize - 1L, 32, limitedStore));
        Assertions.assertEquals(1, limitedStore.resetCount);
        Assertions.assertEquals(0, limitedStore.size());
    }

    private static DataInputStream dataInputWithLength(int length) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        new DataOutputStream(bytes).writeInt(length);
        return new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
    }

    private static class Value {
        private String text;
        private int number;

        private Value(String text, int number) {
            this.text = text;
            this.number = number;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Value)) {
                return false;
            }
            Value that = (Value) other;
            return number == that.number && text.equals(that.text);
        }

        @Override
        public int hashCode() {
            return 31 * text.hashCode() + number;
        }
    }

    private static class DelegatingDataInput implements DataInput {
        private final DataInput delegate;

        private DelegatingDataInput(DataInput delegate) {
            this.delegate = delegate;
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            delegate.readFully(bytes);
        }

        @Override
        public void readFully(byte[] bytes, int offset, int length) throws IOException {
            delegate.readFully(bytes, offset, length);
        }

        @Override
        public int skipBytes(int count) throws IOException {
            return delegate.skipBytes(count);
        }

        @Override
        public boolean readBoolean() throws IOException {
            return delegate.readBoolean();
        }

        @Override
        public byte readByte() throws IOException {
            return delegate.readByte();
        }

        @Override
        public int readUnsignedByte() throws IOException {
            return delegate.readUnsignedByte();
        }

        @Override
        public short readShort() throws IOException {
            return delegate.readShort();
        }

        @Override
        public int readUnsignedShort() throws IOException {
            return delegate.readUnsignedShort();
        }

        @Override
        public char readChar() throws IOException {
            return delegate.readChar();
        }

        @Override
        public int readInt() throws IOException {
            return delegate.readInt();
        }

        @Override
        public long readLong() throws IOException {
            return delegate.readLong();
        }

        @Override
        public float readFloat() throws IOException {
            return delegate.readFloat();
        }

        @Override
        public double readDouble() throws IOException {
            return delegate.readDouble();
        }

        @Override
        public String readLine() throws IOException {
            return delegate.readLine();
        }

        @Override
        public String readUTF() throws IOException {
            return delegate.readUTF();
        }
    }

    private static class ZeroThenDataInputStream extends InputStream {
        private final ByteArrayInputStream delegate;
        private boolean returnedZero;

        private ZeroThenDataInputStream(byte[] bytes) {
            delegate = new ByteArrayInputStream(bytes);
        }

        @Override
        public int read() {
            return delegate.read();
        }

        @Override
        public int read(byte[] bytes, int offset, int length) {
            if (!returnedZero) {
                returnedZero = true;
                return 0;
            }
            return delegate.read(bytes, offset, length);
        }
    }

    private static class TrackingBufferStore implements LengthPrefixedJsonStream.BufferStore {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final IOException resetFailure;
        private int resetCount;

        private TrackingBufferStore() {
            this(null);
        }

        private TrackingBufferStore(IOException resetFailure) {
            this.resetFailure = resetFailure;
        }

        @Override
        public void write(int value) {
            bytes.write(value);
        }

        @Override
        public void write(byte[] source, int offset, int length) {
            bytes.write(source, offset, length);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }

        @Override
        public void reset() throws IOException {
            resetCount++;
            if (resetFailure != null) {
                throw resetFailure;
            }
            bytes.reset();
        }

        @Override
        public InputStream openInputStream() {
            return new ByteArrayInputStream(bytes.toByteArray());
        }

        private int size() {
            return bytes.size();
        }
    }
}
