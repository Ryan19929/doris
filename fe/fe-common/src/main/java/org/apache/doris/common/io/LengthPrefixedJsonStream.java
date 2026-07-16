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
import java.util.ArrayList;
import java.util.List;

/**
 * Reads and writes the four-byte-length-prefixed UTF-8 JSON format used by {@link Text}.
 *
 * <p>Writing serializes once into fixed-size byte segments so the length is known before
 * the destination is touched. Reading exposes only the declared payload to Gson and does
 * not allocate a contiguous byte array proportional to the payload size.</p>
 */
public final class LengthPrefixedJsonStream {
    private static final int SEGMENT_SIZE = 64 * 1024;

    private LengthPrefixedJsonStream() {
    }

    public static void write(DataOutput out, Object value, Gson gson) throws IOException {
        JsonBuffer buffer = serialize(value, gson);
        out.writeInt(buffer.size());
        buffer.writeTo(out);
    }

    public static JsonBuffer serialize(Object value, Gson gson) throws IOException {
        return serialize(value, gson, Integer.MAX_VALUE);
    }

    static JsonBuffer serialize(Object value, Gson gson, long maxPayloadBytes) throws IOException {
        SegmentedOutputStream output = new SegmentedOutputStream(maxPayloadBytes);
        CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
        try (Writer writer = new OutputStreamWriter(output, encoder)) {
            try {
                gson.toJson(value, writer);
            } catch (JsonIOException e) {
                throw unwrapIoException(e);
            }
        }
        return output.toBuffer();
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

    public static final class JsonBuffer {
        private final List<byte[]> segments;
        private final int lastSegmentSize;
        private final int size;

        private JsonBuffer(List<byte[]> segments, int lastSegmentSize, int size) {
            this.segments = segments;
            this.lastSegmentSize = lastSegmentSize;
            this.size = size;
        }

        public int size() {
            return size;
        }

        public void writeTo(DataOutput out) throws IOException {
            for (int i = 0; i < segments.size(); i++) {
                int length = i == segments.size() - 1 ? lastSegmentSize : segments.get(i).length;
                out.write(segments.get(i), 0, length);
            }
        }

        private Reader newReader() {
            CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPLACE)
                    .onUnmappableCharacter(CodingErrorAction.REPLACE);
            return new InputStreamReader(new SegmentedInputStream(segments, lastSegmentSize), decoder);
        }
    }

    private static final class SegmentedOutputStream extends OutputStream {
        private final List<byte[]> segments = new ArrayList<>();
        private final long maxPayloadBytes;
        private byte[] current;
        private int currentPosition;
        private long size;

        private SegmentedOutputStream(long maxPayloadBytes) {
            this.maxPayloadBytes = maxPayloadBytes;
        }

        @Override
        public void write(int value) throws IOException {
            ensureCapacity(1);
            current[currentPosition++] = (byte) value;
            size++;
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            if (length < 0 || offset < 0 || offset + length > bytes.length) {
                throw new IndexOutOfBoundsException();
            }
            ensureWithinLimit(length);
            int remaining = length;
            int sourceOffset = offset;
            while (remaining > 0) {
                ensureSegment();
                int copied = Math.min(remaining, current.length - currentPosition);
                System.arraycopy(bytes, sourceOffset, current, currentPosition, copied);
                currentPosition += copied;
                sourceOffset += copied;
                remaining -= copied;
                size += copied;
            }
        }

        private void ensureCapacity(int additionalBytes) throws IOException {
            ensureWithinLimit(additionalBytes);
            ensureSegment();
        }

        private void ensureWithinLimit(int additionalBytes) throws IOException {
            if (size + additionalBytes > maxPayloadBytes) {
                throw new IOException("JSON payload exceeds maximum length " + maxPayloadBytes);
            }
        }

        private void ensureSegment() {
            if (current == null || currentPosition == current.length) {
                current = new byte[SEGMENT_SIZE];
                segments.add(current);
                currentPosition = 0;
            }
        }

        private JsonBuffer toBuffer() {
            return new JsonBuffer(segments, currentPosition, (int) size);
        }
    }

    private static final class SegmentedInputStream extends InputStream {
        private final List<byte[]> segments;
        private final int lastSegmentSize;
        private int segmentIndex;
        private int segmentPosition;

        private SegmentedInputStream(List<byte[]> segments, int lastSegmentSize) {
            this.segments = segments;
            this.lastSegmentSize = lastSegmentSize;
        }

        @Override
        public int read() {
            if (!advance()) {
                return -1;
            }
            return segments.get(segmentIndex)[segmentPosition++] & 0xff;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) {
            if (length == 0) {
                return 0;
            }
            if (!advance()) {
                return -1;
            }
            int segmentLength = segmentLength();
            int copied = Math.min(length, segmentLength - segmentPosition);
            System.arraycopy(segments.get(segmentIndex), segmentPosition, bytes, offset, copied);
            segmentPosition += copied;
            return copied;
        }

        private boolean advance() {
            while (segmentIndex < segments.size() && segmentPosition == segmentLength()) {
                segmentIndex++;
                segmentPosition = 0;
            }
            return segmentIndex < segments.size();
        }

        private int segmentLength() {
            return segmentIndex == segments.size() - 1 ? lastSegmentSize : segments.get(segmentIndex).length;
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
