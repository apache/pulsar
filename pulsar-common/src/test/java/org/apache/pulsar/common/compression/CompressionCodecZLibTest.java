/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.common.compression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.DuplicatedByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CompressionCodecZLibTest {

    private static final int PADDING = 13;

    private final CompressionCodecZLib codec = new CompressionCodecZLib();

    private enum BufferType {
        HEAP,
        DIRECT,
        HEAP_OFFSET,
        DIRECT_OFFSET,
        HEAP_SLICE,
        DIRECT_SLICE,
        HEAP_READ_ONLY,
        DIRECT_READ_ONLY,
        COMPOSITE,
        COMPOSITE_SINGLE_RANGE,
        NON_NIO,
        NON_NIO_SINGLE_COUNT;

        ByteBuf create(byte[] data) {
            return switch (this) {
                case HEAP -> Unpooled.wrappedBuffer(data);
                case DIRECT -> Unpooled.directBuffer(data.length).writeBytes(data);
                case HEAP_OFFSET -> paddedBuffer(data, false);
                case DIRECT_OFFSET -> paddedBuffer(data, true);
                case HEAP_SLICE -> slicedBuffer(data, false);
                case DIRECT_SLICE -> slicedBuffer(data, true);
                case HEAP_READ_ONLY -> slicedBuffer(data, false).asReadOnly();
                case DIRECT_READ_ONLY -> slicedBuffer(data, true).asReadOnly();
                case COMPOSITE -> compositeBuffer(data, false);
                case COMPOSITE_SINGLE_RANGE -> compositeBuffer(data, true);
                case NON_NIO -> new NonNioByteBuf(slicedBuffer(data, true), -1);
                case NON_NIO_SINGLE_COUNT -> new NonNioByteBuf(slicedBuffer(data, true), 1);
            };
        }
    }

    private static final class NonNioByteBuf extends DuplicatedByteBuf {
        private final int nioBufferCount;

        private NonNioByteBuf(ByteBuf buffer, int nioBufferCount) {
            super(buffer);
            this.nioBufferCount = nioBufferCount;
        }

        @Override
        public boolean hasArray() {
            return false;
        }

        @Override
        public int nioBufferCount() {
            return nioBufferCount;
        }

        @Override
        public ByteBuffer nioBuffer(int index, int length) {
            throw new UnsupportedOperationException("NIO views are unavailable");
        }

        @Override
        public ByteBuffer internalNioBuffer(int index, int length) {
            throw new UnsupportedOperationException("NIO views are unavailable");
        }

        @Override
        public ByteBuffer[] nioBuffers(int index, int length) {
            throw new UnsupportedOperationException("NIO views are unavailable");
        }
    }

    private record Payload(String name, byte[] bytes) {
        @Override
        public String toString() {
            return name;
        }
    }

    @DataProvider
    public Object[][] bufferTypesAndPayloads() {
        List<Payload> payloads = List.of(
                new Payload("empty", new byte[0]),
                new Payload("single byte", new byte[] {42}),
                new Payload("text", "Pulsar ZLib compression".getBytes(StandardCharsets.UTF_8)),
                new Payload("4 KiB random", randomBytes(4096)),
                new Payload("64 KiB random", randomBytes(65536)),
                new Payload("256 KiB random", randomBytes(262144)),
                new Payload("256 KiB repeated", new byte[262144]));
        List<Object[]> cases = new ArrayList<>();
        for (BufferType type : BufferType.values()) {
            for (Payload payload : payloads) {
                cases.add(new Object[] {type, payload});
            }
        }
        return cases.toArray(Object[][]::new);
    }

    @Test(dataProvider = "bufferTypesAndPayloads")
    public void testCompatibilityWithArrayCodec(BufferType type, Payload payload) throws Exception {
        byte[] data = payload.bytes();
        byte[] legacyEncoded = legacyEncode(data);
        ByteBuf source = type.create(data);
        ByteBuf encoded = null;
        ByteBuf legacyInput = null;
        ByteBuf decoded = null;
        try {
            int sourceReaderIndex = source.readerIndex();
            int sourceWriterIndex = source.writerIndex();
            encoded = codec.encode(source);

            byte[] actualEncoded = ByteBufUtil.getBytes(encoded);
            assertThat(actualEncoded).as("legacy SYNC_FLUSH encoding").isEqualTo(legacyEncoded);
            assertThat(legacyDecode(actualEncoded, data.length)).as("legacy decoder output").isEqualTo(data);
            assertUnchanged(source, sourceReaderIndex, sourceWriterIndex);

            legacyInput = type.create(legacyEncoded);
            int encodedReaderIndex = legacyInput.readerIndex();
            int encodedWriterIndex = legacyInput.writerIndex();
            decoded = codec.decode(legacyInput, data.length);

            assertThat(ByteBufUtil.getBytes(decoded)).as("decoded legacy payload").isEqualTo(data);
            assertUnchanged(legacyInput, encodedReaderIndex, encodedWriterIndex);
        } finally {
            release(decoded, legacyInput, encoded, source);
        }
    }

    @DataProvider
    public Object[][] singleRangeBufferTypes() {
        return new Object[][] {{BufferType.DIRECT_SLICE}, {BufferType.COMPOSITE_SINGLE_RANGE}};
    }

    @Test(dataProvider = "singleRangeBufferTypes")
    public void testSingleRangeInputsDoNotCopyToByteArray(BufferType type) throws IOException, DataFormatException {
        byte[] data = randomBytes(65536);
        ByteBuf source = type.create(data);
        ByteBuf legacyInput = type.create(legacyEncode(data));
        ByteBuf sourceWrapper = mock(ByteBuf.class, delegatesTo(source));
        ByteBuf encodedWrapper = mock(ByteBuf.class, delegatesTo(legacyInput));
        ByteBuf encoded = null;
        ByteBuf decoded = null;
        try {
            encoded = codec.encode(sourceWrapper);
            decoded = codec.decode(encodedWrapper, data.length);

            assertThat(legacyDecode(ByteBufUtil.getBytes(encoded), data.length)).isEqualTo(data);
            assertThat(ByteBufUtil.getBytes(decoded)).isEqualTo(data);
            verify(sourceWrapper, never()).getBytes(anyInt(), any(byte[].class));
            verify(encodedWrapper, never()).getBytes(anyInt(), any(byte[].class));
        } finally {
            release(decoded, encoded, legacyInput, source);
        }
    }

    @Test
    public void testReuseAfterDecodeFailures() throws IOException {
        List<ByteBuf> allocated = new ArrayList<>();
        ByteBufAllocator allocator = mock(ByteBufAllocator.class);
        doAnswer(invocation -> {
            ByteBuf output = Unpooled.buffer(invocation.getArgument(0), invocation.getArgument(1));
            allocated.add(output);
            return output;
        }).when(allocator).heapBuffer(anyInt(), anyInt());
        CompressionCodecZLib recoveringCodec = new CompressionCodecZLib(allocator);
        ByteBuf corrupt = BufferType.DIRECT_OFFSET.create(new byte[] {0, 0, 0, 0});
        byte[] data = randomBytes(4096);
        ByteBuf valid = BufferType.DIRECT_SLICE.create(legacyEncode(data));
        ByteBuf decoded = null;
        try {
            int corruptReaderIndex = corrupt.readerIndex();
            int corruptWriterIndex = corrupt.writerIndex();
            assertThatThrownBy(() -> recoveringCodec.decode(corrupt, 1))
                    .isInstanceOf(IOException.class)
                    .hasCauseInstanceOf(DataFormatException.class);
            assertUnchanged(corrupt, corruptReaderIndex, corruptWriterIndex);
            assertThat(allocated).hasSize(1);
            assertThat(allocated.get(0).refCnt()).as("output after invalid compressed data").isZero();

            decoded = recoveringCodec.decode(valid, data.length);
            assertThat(ByteBufUtil.getBytes(decoded)).as("recovery after invalid compressed data").isEqualTo(data);
            decoded.release();
            decoded = null;

            int readerIndex = valid.readerIndex();
            int writerIndex = valid.writerIndex();
            assertThatThrownBy(() -> recoveringCodec.decode(valid, data.length + 1))
                    .isInstanceOf(IllegalArgumentException.class);
            assertUnchanged(valid, readerIndex, writerIndex);
            assertThat(allocated).hasSize(3);
            assertThat(allocated.get(2).refCnt()).as("output after incorrect decoded length").isZero();

            decoded = recoveringCodec.decode(valid, data.length);
            assertThat(ByteBufUtil.getBytes(decoded)).as("recovery after incorrect output length").isEqualTo(data);
            assertUnchanged(valid, readerIndex, writerIndex);
        } finally {
            release(decoded, valid, corrupt);
            for (ByteBuf output : allocated) {
                if (output.refCnt() > 0) {
                    output.release();
                }
            }
        }
    }

    @DataProvider
    public Object[][] operations() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "operations")
    public void testInputsWithoutNioBuffersDoNotRequestNioViews(boolean encode) throws IOException {
        byte[] data = randomBytes(4096);
        byte[] input = encode ? data : legacyEncode(data);
        ByteBuf source = BufferType.NON_NIO.create(input);
        ByteBuf sourceWrapper = mock(ByteBuf.class, delegatesTo(source));
        ByteBuf result = null;
        try {
            int readerIndex = source.readerIndex();
            int writerIndex = source.writerIndex();
            result = encode ? codec.encode(sourceWrapper) : codec.decode(sourceWrapper, data.length);

            assertThat(ByteBufUtil.getBytes(result)).isEqualTo(encode ? legacyEncode(data) : data);
            verify(sourceWrapper, never()).nioBuffer(anyInt(), anyInt());
            verify(sourceWrapper, never()).internalNioBuffer(anyInt(), anyInt());
            verify(sourceWrapper, never()).nioBuffers(anyInt(), anyInt());
            assertUnchanged(source, readerIndex, writerIndex);
        } finally {
            release(result, source);
        }
    }

    @Test(dataProvider = "operations")
    public void testNioViewFailureReleasesOutput(boolean encode) {
        assertInputFailureReleasesOutput(encode, false);
    }

    @Test(dataProvider = "operations")
    public void testFallbackCopyFailureReleasesOutput(boolean encode) {
        assertInputFailureReleasesOutput(encode, true);
    }

    private static void assertInputFailureReleasesOutput(boolean encode, boolean fallbackCopy) {
        ByteBuf source = BufferType.DIRECT_OFFSET.create(randomBytes(256));
        if (fallbackCopy) {
            source = new NonNioByteBuf(source, -1);
        }
        ByteBuf sourceWrapper = mock(ByteBuf.class, delegatesTo(source));
        IllegalStateException failure = new IllegalStateException("Cannot access the input");
        if (fallbackCopy) {
            doThrow(failure).when(sourceWrapper).getBytes(anyInt(), any(byte[].class));
        } else {
            doThrow(failure).when(sourceWrapper).nioBuffer(anyInt(), anyInt());
        }
        ByteBuf output = Unpooled.buffer(512);
        ByteBufAllocator allocator = mock(ByteBufAllocator.class);
        doReturn(output).when(allocator).heapBuffer(anyInt());
        doReturn(output).when(allocator).heapBuffer(anyInt(), anyInt());
        CompressionCodecZLib failingCodec = new CompressionCodecZLib(allocator);
        try {
            int readerIndex = source.readerIndex();
            int writerIndex = source.writerIndex();
            assertThatThrownBy(() -> {
                ByteBuf unexpected = encode ? failingCodec.encode(sourceWrapper)
                        : failingCodec.decode(sourceWrapper, 256);
                unexpected.release();
            }).isSameAs(failure);
            assertThat(output.refCnt()).as("output after failing to expose the input").isZero();
            assertUnchanged(source, readerIndex, writerIndex);
        } finally {
            source.release();
            if (output.refCnt() > 0) {
                output.release();
            }
        }
    }

    @Test
    public void testRepeatedRoundTrips() throws IOException {
        for (int iteration = 0; iteration < 20; iteration++) {
            byte[] data = randomBytes(iteration * 997);
            BufferType type = iteration % 2 == 0 ? BufferType.DIRECT_OFFSET : BufferType.HEAP_SLICE;
            ByteBuf source = type.create(data);
            ByteBuf encoded = null;
            ByteBuf directEncoded = null;
            ByteBuf decoded = null;
            try {
                encoded = codec.encode(source);
                directEncoded = BufferType.DIRECT_OFFSET.create(ByteBufUtil.getBytes(encoded));
                decoded = codec.decode(directEncoded, data.length);
                assertThat(ByteBufUtil.getBytes(decoded)).as("round trip %s", iteration).isEqualTo(data);
            } finally {
                release(decoded, directEncoded, encoded, source);
            }
        }
    }

    private static ByteBuf paddedBuffer(byte[] data, boolean direct) {
        ByteBuf buffer = allocate(data.length + 2 * PADDING, direct);
        buffer.writeZero(PADDING).writeBytes(data).writeZero(PADDING);
        return buffer.setIndex(PADDING, PADDING + data.length);
    }

    private static ByteBuf slicedBuffer(byte[] data, boolean direct) {
        ByteBuf buffer = allocate(data.length + 4 * PADDING, direct);
        buffer.writeZero(2 * PADDING).writeBytes(data).writeZero(2 * PADDING);
        return buffer.slice(PADDING, data.length + 2 * PADDING).setIndex(PADDING, PADDING + data.length);
    }

    private static ByteBuf compositeBuffer(byte[] data, boolean singleRange) {
        CompositeByteBuf composite = Unpooled.compositeBuffer();
        if (singleRange) {
            composite.addComponents(true,
                    Unpooled.directBuffer(PADDING).writeZero(PADDING),
                    Unpooled.directBuffer(data.length).writeBytes(data),
                    Unpooled.directBuffer(PADDING).writeZero(PADDING));
        } else {
            int split = data.length / 2;
            composite.addComponents(true,
                    Unpooled.directBuffer(PADDING + split).writeZero(PADDING).writeBytes(data, 0, split),
                    Unpooled.directBuffer(data.length - split + PADDING)
                            .writeBytes(data, split, data.length - split).writeZero(PADDING));
        }
        return composite.setIndex(PADDING, PADDING + data.length);
    }

    private static ByteBuf allocate(int capacity, boolean direct) {
        return direct ? Unpooled.directBuffer(capacity) : Unpooled.buffer(capacity);
    }

    private static void assertUnchanged(ByteBuf buffer, int readerIndex, int writerIndex) {
        assertThat(buffer.readerIndex()).as("input reader index").isEqualTo(readerIndex);
        assertThat(buffer.writerIndex()).as("input writer index").isEqualTo(writerIndex);
        assertThat(buffer.refCnt()).as("input reference count").isEqualTo(1);
    }

    private static byte[] randomBytes(int length) {
        byte[] data = new byte[length];
        new Random(123456789L + length).nextBytes(data);
        return data;
    }

    // Preserve the previous array-based codec as an independent wire-compatibility oracle.
    private static byte[] legacyEncode(byte[] data) {
        Deflater deflater = new Deflater();
        try {
            byte[] compressed = new byte[(int) Math.ceil(data.length * 1.001) + 14];
            int writerIndex = 0;
            deflater.setInput(data);
            while (!deflater.needsInput()) {
                int count;
                do {
                    count = deflater.deflate(compressed, writerIndex, compressed.length - writerIndex,
                            Deflater.SYNC_FLUSH);
                    writerIndex += count;
                } while (count > 0);
            }
            return Arrays.copyOf(compressed, writerIndex);
        } finally {
            deflater.end();
        }
    }

    private static byte[] legacyDecode(byte[] data, int uncompressedLength) throws DataFormatException {
        Inflater inflater = new Inflater();
        try {
            inflater.setInput(data);
            byte[] uncompressed = new byte[uncompressedLength];
            assertThat(inflater.inflate(uncompressed)).as("legacy decoded length").isEqualTo(uncompressedLength);
            return uncompressed;
        } finally {
            inflater.end();
        }
    }

    private static void release(ByteBuf... buffers) {
        for (ByteBuf buffer : buffers) {
            if (buffer != null) {
                buffer.release();
            }
        }
    }
}
