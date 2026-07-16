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

import com.google.common.io.FileBackedOutputStream;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Reads and writes the four-byte-length-prefixed UTF-8 JSON format used by {@link Text}.
 *
 * <p>Writing serializes once into a spillable buffer so the length is known before the
 * destination is touched. Small payloads stay in memory and larger payloads spill to a
 * temporary file. Reading exposes only the declared payload to Gson and does not allocate
 * a contiguous byte array proportional to the payload size.</p>
 */
public final class LengthPrefixedJsonStream {
    // Keep routine metadata in memory while bounding large backup payloads well below FE heap sizes.
    private static final int DEFAULT_MEMORY_THRESHOLD_BYTES = 8 * 1024 * 1024;
    private static final int COPY_BUFFER_SIZE = 64 * 1024;

    private LengthPrefixedJsonStream() {
    }

    public static void write(DataOutput out, Object value, Gson gson) throws IOException {
        write(out, value, gson, Integer.MAX_VALUE, DEFAULT_MEMORY_THRESHOLD_BYTES);
    }

    static void write(DataOutput out, Object value, Gson gson, long maxPayloadBytes, int memoryThresholdBytes)
            throws IOException {
        write(out, value, gson, maxPayloadBytes, memoryThresholdBytes,
                new FileBackedBufferStore(memoryThresholdBytes));
    }

    static void write(DataOutput out, Object value, Gson gson, long maxPayloadBytes, int memoryThresholdBytes,
            BufferStore store) throws IOException {
        try (JsonBuffer buffer = serialize(value, gson, maxPayloadBytes, memoryThresholdBytes, store)) {
            out.writeInt(buffer.size());
            buffer.writeTo(out);
        }
    }

    public static JsonBuffer serialize(Object value, Gson gson) throws IOException {
        return serialize(value, gson, Integer.MAX_VALUE);
    }

    static JsonBuffer serialize(Object value, Gson gson, long maxPayloadBytes) throws IOException {
        return serialize(value, gson, maxPayloadBytes, DEFAULT_MEMORY_THRESHOLD_BYTES);
    }

    static JsonBuffer serialize(Object value, Gson gson, long maxPayloadBytes, int memoryThresholdBytes)
            throws IOException {
        return serialize(value, gson, maxPayloadBytes, memoryThresholdBytes,
                new FileBackedBufferStore(memoryThresholdBytes));
    }

    static JsonBuffer serialize(Object value, Gson gson, long maxPayloadBytes, int memoryThresholdBytes,
            BufferStore store) throws IOException {
        SpillableOutputStream output = new SpillableOutputStream(
                maxPayloadBytes, memoryThresholdBytes, store);
        CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
        try {
            try (Writer writer = new OutputStreamWriter(output, encoder)) {
                try {
                    gson.toJson(value, writer);
                } catch (JsonIOException e) {
                    throw unwrapIoException(e);
                }
            }
            return output.toBuffer();
        } catch (IOException | RuntimeException | Error failure) {
            try {
                output.reset();
            } catch (IOException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    public static int readLength(DataInput in) throws IOException {
        int length = in.readInt();
        if (length < 0) {
            throw new IOException("negative JSON payload length: " + length);
        }
        return length;
    }

    public static <T> T read(DataInput in, Class<T> type, Gson gson) throws IOException {
        return read(in, readLength(in), type, gson);
    }

    public static <T> T read(DataInput in, int length, Class<T> type, Gson gson) throws IOException {
        if (length < 0) {
            throw new IOException("negative JSON payload length: " + length);
        }
        BoundedDataInputStream input = new BoundedDataInputStream(in, length);
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
        T value;
        try (Reader reader = new InputStreamReader(input, decoder)) {
            try {
                value = gson.fromJson(reader, type);
            } catch (JsonIOException e) {
                throw unwrapIoException(e);
            }
        }
        if (input.remaining() != 0) {
            throw new IOException("JSON parser did not consume " + input.remaining() + " payload bytes");
        }
        return value;
    }

    public static <T> T read(JsonBuffer buffer, Class<T> type, Gson gson) throws IOException {
        try (Reader reader = buffer.newReader()) {
            try {
                return gson.fromJson(reader, type);
            } catch (JsonIOException e) {
                throw unwrapIoException(e);
            }
        }
    }

    private static IOException unwrapIoException(JsonIOException exception) {
        if (exception.getCause() instanceof IOException) {
            return (IOException) exception.getCause();
        }
        throw exception;
    }

    public static final class JsonBuffer implements AutoCloseable {
        private final BufferStore buffer;
        private final int size;
        private final boolean spilled;
        private boolean closed;

        private JsonBuffer(BufferStore buffer, int size, boolean spilled) {
            this.buffer = buffer;
            this.size = size;
            this.spilled = spilled;
        }

        public int size() {
            return size;
        }

        public void writeTo(DataOutput out) throws IOException {
            ensureOpen();
            byte[] bytes = new byte[COPY_BUFFER_SIZE];
            try (InputStream input = buffer.openInputStream()) {
                int read;
                while ((read = input.read(bytes)) != -1) {
                    out.write(bytes, 0, read);
                }
            }
        }

        private Reader newReader() throws IOException {
            ensureOpen();
            CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPLACE)
                    .onUnmappableCharacter(CodingErrorAction.REPLACE);
            return new InputStreamReader(buffer.openInputStream(), decoder);
        }

        boolean isSpilled() {
            return spilled;
        }

        private void ensureOpen() throws IOException {
            if (closed) {
                throw new IOException("JSON buffer is closed");
            }
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                buffer.reset();
                closed = true;
            }
        }
    }

    private static final class SpillableOutputStream extends OutputStream {
        private final BufferStore buffer;
        private final long maxPayloadBytes;
        private final int memoryThresholdBytes;
        private long size;
        private boolean spilled;

        private SpillableOutputStream(long maxPayloadBytes, int memoryThresholdBytes, BufferStore buffer) {
            if (maxPayloadBytes < 0 || maxPayloadBytes > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("maxPayloadBytes must be between 0 and "
                        + Integer.MAX_VALUE + ": " + maxPayloadBytes);
            }
            if (memoryThresholdBytes < 0) {
                throw new IllegalArgumentException("memoryThresholdBytes must be non-negative: "
                        + memoryThresholdBytes);
            }
            this.maxPayloadBytes = maxPayloadBytes;
            this.memoryThresholdBytes = memoryThresholdBytes;
            this.buffer = buffer;
        }

        @Override
        public void write(int value) throws IOException {
            ensureWithinLimit(1);
            buffer.write(value);
            size++;
            updateSpilled();
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            if (offset < 0 || length < 0 || length > bytes.length - offset) {
                throw new IndexOutOfBoundsException();
            }
            ensureWithinLimit(length);
            buffer.write(bytes, offset, length);
            size += length;
            updateSpilled();
        }

        private void ensureWithinLimit(int additionalBytes) throws IOException {
            if (additionalBytes > maxPayloadBytes - size) {
                throw new IOException("JSON payload exceeds maximum length " + maxPayloadBytes);
            }
        }

        private void updateSpilled() {
            spilled |= size > memoryThresholdBytes;
        }

        private JsonBuffer toBuffer() {
            return new JsonBuffer(buffer, (int) size, spilled);
        }

        @Override
        public void flush() throws IOException {
            buffer.flush();
        }

        @Override
        public void close() throws IOException {
            buffer.close();
        }

        private void reset() throws IOException {
            buffer.reset();
        }
    }

    interface BufferStore {
        void write(int value) throws IOException;

        void write(byte[] bytes, int offset, int length) throws IOException;

        void flush() throws IOException;

        void close() throws IOException;

        void reset() throws IOException;

        InputStream openInputStream() throws IOException;
    }

    private static final class FileBackedBufferStore implements BufferStore {
        private final FileBackedOutputStream buffer;

        private FileBackedBufferStore(int memoryThresholdBytes) {
            buffer = new FileBackedOutputStream(memoryThresholdBytes);
        }

        @Override
        public void write(int value) throws IOException {
            buffer.write(value);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            buffer.write(bytes, offset, length);
        }

        @Override
        public void flush() throws IOException {
            buffer.flush();
        }

        @Override
        public void close() throws IOException {
            buffer.close();
        }

        @Override
        public void reset() throws IOException {
            buffer.reset();
        }

        @Override
        public InputStream openInputStream() throws IOException {
            return buffer.asByteSource().openStream();
        }
    }

    private static final class BoundedDataInputStream extends InputStream {
        private final DataInput input;
        private int remaining;

        private BoundedDataInputStream(DataInput input, int remaining) {
            this.input = input;
            this.remaining = remaining;
        }

        @Override
        public int read() throws IOException {
            if (remaining == 0) {
                return -1;
            }
            int value = input.readUnsignedByte();
            remaining--;
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            if (remaining == 0) {
                return -1;
            }
            int requested = Math.min(length, remaining);
            if (input instanceof InputStream) {
                int read = ((InputStream) input).read(bytes, offset, requested);
                if (read < 0) {
                    throw new EOFException("truncated JSON payload with " + remaining + " bytes remaining");
                }
                if (read == 0) {
                    bytes[offset] = (byte) input.readUnsignedByte();
                    remaining--;
                    return 1;
                }
                remaining -= read;
                return read;
            }
            input.readFully(bytes, offset, requested);
            remaining -= requested;
            return requested;
        }

        private int remaining() {
            return remaining;
        }

        @Override
        public void close() {
            // The enclosing journal or image reader owns the DataInput lifecycle.
        }
    }
}
