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
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CompressionCodecZLibCompositeTest {

    private static final int PADDING = 7;
    private final CompressionCodecZLib codec = new CompressionCodecZLib();

    @DataProvider
    public Object[][] payloads() {
        List<Object[]> cases = new ArrayList<>();
        for (int length : new int[] {0, 1, 32767, 32768, 65536, 131072}) {
            cases.add(new Object[] {length, false});
            cases.add(new Object[] {length, true});
        }
        return cases.toArray(Object[][]::new);
    }

    @Test(dataProvider = "payloads")
    public void testSegmentedInputsAvoidCoalescing(int length, boolean repeated) throws Exception {
        byte[] data = payload(length, repeated);
        byte[] legacyEncoded = legacyEncode(data);
        ByteBuf source = segmented(data);
        ByteBuf legacyInput = segmented(legacyEncoded);
        ByteBuf encoded = null;
        ByteBuf decoded = null;
        try {
            BufferState sourceBefore = BufferState.capture(source);
            encoded = codec.encode(withoutCoalescing(source));
            byte[] actual = ByteBufUtil.getBytes(encoded);
            assertThat(actual).as("legacy SYNC_FLUSH encoding").isEqualTo(legacyEncoded);
            assertThat(legacyDecode(actual, length)).isEqualTo(data);
            sourceBefore.assertUnchanged(source);

            BufferState encodedBefore = BufferState.capture(legacyInput);
            decoded = codec.decode(withoutCoalescing(legacyInput), length);
            assertThat(ByteBufUtil.getBytes(decoded)).isEqualTo(data);
            encodedBefore.assertUnchanged(legacyInput);
        } finally {
            release(decoded, encoded, legacyInput, source);
        }
    }

    @Test
    public void testReuseAfterLaterComponentDecodeFailures() throws Exception {
        List<ByteBuf> allocated = new ArrayList<>();
        ByteBufAllocator allocator = mock(ByteBufAllocator.class);
        doAnswer(invocation -> {
            ByteBuf output = Unpooled.buffer(invocation.getArgument(0), invocation.getArgument(1));
            allocated.add(output);
            return output;
        }).when(allocator).heapBuffer(anyInt(), anyInt());
        CompressionCodecZLib recoveringCodec = new CompressionCodecZLib(allocator);
        byte[] data = payload(4096, false);
        byte[] valid = legacyEncode(data);
        // A valid two-byte ZLIB header followed by an invalid DEFLATE block type, in the next component.
        ByteBuf corrupt = segmented(new byte[] {0x78, (byte) 0x9c, 0x07});
        ByteBuf truncated = segmented(Arrays.copyOf(valid, valid.length / 2));
        try {
            BufferState corruptBefore = BufferState.capture(corrupt);
            assertThatThrownBy(() -> decodeAndRelease(recoveringCodec, withoutCoalescing(corrupt), data.length))
                    .isInstanceOf(IOException.class).hasCauseInstanceOf(DataFormatException.class);
            corruptBefore.assertUnchanged(corrupt);
            assertThat(allocated).hasSize(1);
            assertThat(allocated.get(0).refCnt()).as("output after an invalid later component").isZero();
            assertRecovered(recoveringCodec, valid, data);

            BufferState truncatedBefore = BufferState.capture(truncated);
            assertThatThrownBy(() -> decodeAndRelease(recoveringCodec, withoutCoalescing(truncated), data.length))
                    .isInstanceOf(IllegalArgumentException.class);
            truncatedBefore.assertUnchanged(truncated);
            assertThat(allocated).hasSize(3);
            assertThat(allocated.get(2).refCnt()).as("output after an incomplete compressed payload").isZero();
            assertRecovered(recoveringCodec, valid, data);
        } finally {
            release(truncated, corrupt);
            allocated.stream().filter(buffer -> buffer.refCnt() > 0).forEach(ByteBuf::release);
        }
    }

    @Test
    public void testFinishedStreamTrailerIsValidatedAcrossComponents() throws Exception {
        List<ByteBuf> allocated = new ArrayList<>();
        CompressionCodecZLib recoveringCodec = trackingDecoder(allocated);
        byte[] data = payload(4096, false);
        byte[] valid = finishedEncode(data);
        byte[] damaged = valid.clone();
        // segmented() puts this final checksum byte in its own component, after all output can be produced.
        damaged[damaged.length - 1] ^= (byte) 0xff;
        ByteBuf input = segmented(damaged);
        try {
            BufferState before = BufferState.capture(input);
            assertThatThrownBy(() -> decodeAndRelease(recoveringCodec, withoutCoalescing(input), data.length))
                    .isInstanceOf(IOException.class).hasCauseInstanceOf(DataFormatException.class);
            before.assertUnchanged(input);
            assertThat(allocated).hasSize(1);
            assertThat(allocated.get(0).refCnt()).as("output after a bad checksum in a later component").isZero();
            assertRecovered(recoveringCodec, valid, data);
        } finally {
            input.release();
            allocated.stream().filter(buffer -> buffer.refCnt() > 0).forEach(ByteBuf::release);
        }
    }

    @Test
    public void testCorruptHeaderIsRejectedForEmptyOutput() throws Exception {
        List<ByteBuf> allocated = new ArrayList<>();
        CompressionCodecZLib recoveringCodec = trackingDecoder(allocated);
        ByteBuf input = segmented(new byte[] {0x78, 0, 0});
        try {
            BufferState before = BufferState.capture(input);
            assertThatThrownBy(() -> decodeAndRelease(recoveringCodec, withoutCoalescing(input), 0))
                    .isInstanceOf(IOException.class).hasCauseInstanceOf(DataFormatException.class);
            before.assertUnchanged(input);
            assertThat(allocated).hasSize(1);
            assertThat(allocated.get(0).refCnt()).as("output after a corrupt header with an empty output").isZero();
            assertRecovered(recoveringCodec, finishedEncode(new byte[0]), new byte[0]);
        } finally {
            input.release();
            allocated.stream().filter(buffer -> buffer.refCnt() > 0).forEach(ByteBuf::release);
        }
    }

    @DataProvider
    public Object[][] shortOutputLengths() {
        List<Object[]> cases = new ArrayList<>();
        for (int length : new int[] {0, 1, 2048, 4095}) {
            cases.add(new Object[] {length, false});
            cases.add(new Object[] {length, true});
        }
        return cases.toArray(Object[][]::new);
    }

    @Test(dataProvider = "shortOutputLengths")
    public void testSmallerAdvertisedOutputPreservesExistingBehavior(int length, boolean finished) throws Exception {
        byte[] data = payload(4096, false);
        byte[] encoded = finished ? finishedEncode(data) : legacyEncode(data);
        ByteBuf input = segmented(encoded);
        ByteBuf decoded = null;
        try {
            BufferState before = BufferState.capture(input);
            decoded = codec.decode(withoutCoalescing(input), length);
            assertThat(ByteBufUtil.getBytes(decoded)).isEqualTo(legacyDecode(encoded, length));
            assertThat(ByteBufUtil.getBytes(decoded)).isEqualTo(Arrays.copyOf(data, length));
            before.assertUnchanged(input);
        } finally {
            release(decoded, input);
        }
    }

    @Test(timeOut = 10000)
    public void testSegmentedEncodingGrowsOutput() throws Exception {
        ByteBufAllocator allocator = mock(ByteBufAllocator.class);
        List<ByteBuf> allocated = new ArrayList<>();
        doAnswer(invocation -> {
            ByteBuf output = Unpooled.buffer(8);
            allocated.add(output);
            return output;
        }).when(allocator).heapBuffer(anyInt());
        CompressionCodecZLib smallOutputCodec = new CompressionCodecZLib(allocator);
        byte[] data = payload(65536, false);
        ByteBuf source = segmented(data);
        ByteBuf encoded = null;
        try {
            BufferState before = BufferState.capture(source);
            encoded = smallOutputCodec.encode(withoutCoalescing(source));
            assertThat(encoded.readableBytes()).isGreaterThan(8);
            assertThat(ByteBufUtil.getBytes(encoded)).isEqualTo(legacyEncode(data));
            assertThat(legacyDecode(ByteBufUtil.getBytes(encoded), data.length)).isEqualTo(data);
            before.assertUnchanged(source);
        } finally {
            release(encoded, source);
            allocated.stream().filter(buffer -> buffer.refCnt() > 0).forEach(ByteBuf::release);
        }
    }

    @DataProvider
    public Object[][] operations() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "operations")
    public void testNioArrayFailureReleasesOutput(boolean encode) {
        ByteBuf source = segmented(payload(256, false));
        ByteBuf wrapper = mock(ByteBuf.class, delegatesTo(source));
        IllegalStateException failure = new IllegalStateException("Cannot expose NIO components");
        doThrow(failure).when(wrapper).nioBuffers(anyInt(), anyInt());
        ByteBuf output = Unpooled.buffer(512);
        ByteBufAllocator allocator = mock(ByteBufAllocator.class);
        doReturn(output).when(allocator).heapBuffer(anyInt());
        doReturn(output).when(allocator).heapBuffer(anyInt(), anyInt());
        CompressionCodecZLib failingCodec = new CompressionCodecZLib(allocator);
        try {
            BufferState before = BufferState.capture(source);
            assertThatThrownBy(() -> {
                if (encode) {
                    failingCodec.encode(wrapper).release();
                } else {
                    decodeAndRelease(failingCodec, wrapper, 256);
                }
            }).isSameAs(failure);
            assertThat(output.refCnt()).as("output after failing to expose NIO components").isZero();
            before.assertUnchanged(source);
        } finally {
            source.release();
            if (output.refCnt() > 0) {
                output.release();
            }
        }
    }

    @DataProvider
    public Object[][] nioViewFallbacks() {
        return new Object[][] {{true, true}, {true, false}, {false, true}, {false, false}};
    }

    @Test(dataProvider = "nioViewFallbacks")
    public void testUnsupportedNioArrayFallsBack(boolean encode, boolean singleViewAvailable) throws Exception {
        byte[] data = payload(4096, false);
        byte[] expectedEncoding = legacyEncode(data);
        ByteBuf source = segmented(encode ? data : expectedEncoding);
        ByteBuf wrapper = mock(ByteBuf.class, delegatesTo(source));
        doThrow(new UnsupportedOperationException("NIO component array is unavailable"))
                .when(wrapper).nioBuffers(anyInt(), anyInt());
        if (!singleViewAvailable) {
            doThrow(new UnsupportedOperationException("Contiguous NIO view is unavailable"))
                    .when(wrapper).nioBuffer(anyInt(), anyInt());
        }
        ByteBuf output = null;
        try {
            assertThat(source.nioBufferCount()).isGreaterThan(1);
            BufferState before = BufferState.capture(source);
            output = encode ? codec.encode(wrapper) : codec.decode(wrapper, data.length);
            assertThat(ByteBufUtil.getBytes(output)).isEqualTo(encode ? expectedEncoding : data);
            output.release();
            output = null;
            before.assertUnchanged(source);

            verify(wrapper).nioBuffers(source.readerIndex(), source.readableBytes());
            verify(wrapper).nioBuffer(source.readerIndex(), source.readableBytes());
            if (singleViewAvailable) {
                verify(wrapper, never()).getBytes(anyInt(), any(byte[].class));
            } else {
                verify(wrapper).getBytes(anyInt(), any(byte[].class));
            }
        } finally {
            release(output, source);
        }
    }

    private static void assertRecovered(CompressionCodecZLib codec, byte[] encoded, byte[] data) throws IOException {
        ByteBuf input = segmented(encoded);
        ByteBuf decoded = null;
        try {
            BufferState before = BufferState.capture(input);
            decoded = codec.decode(withoutCoalescing(input), data.length);
            assertThat(ByteBufUtil.getBytes(decoded)).isEqualTo(data);
            before.assertUnchanged(input);
        } finally {
            release(decoded, input);
        }
    }

    private static CompressionCodecZLib trackingDecoder(List<ByteBuf> allocated) {
        ByteBufAllocator allocator = mock(ByteBufAllocator.class);
        doAnswer(invocation -> {
            // Keep zero-length outputs releasable so failure cleanup is observable as well.
            int initial = Math.max(1, (int) invocation.getArgument(0));
            int maximum = Math.max(1, (int) invocation.getArgument(1));
            ByteBuf output = Unpooled.buffer(initial, maximum);
            allocated.add(output);
            return output;
        }).when(allocator).heapBuffer(anyInt(), anyInt());
        return new CompressionCodecZLib(allocator);
    }

    private static void decodeAndRelease(CompressionCodecZLib codec, ByteBuf input, int length) throws IOException {
        codec.decode(input, length).release();
    }

    private static ByteBuf withoutCoalescing(ByteBuf source) {
        ByteBuf wrapper = mock(ByteBuf.class, delegatesTo(source));
        if (source.nioBuffers(source.readerIndex(), source.readableBytes()).length > 1) {
            doThrow(new AssertionError("The readable range must not be coalesced"))
                    .when(wrapper).nioBuffer(anyInt(), anyInt());
        }
        doThrow(new AssertionError("The input must not be copied into a byte array"))
                .when(wrapper).getBytes(anyInt(), any(byte[].class));
        return wrapper;
    }

    private static ByteBuf segmented(byte[] data) {
        CompositeByteBuf result = Unpooled.compositeBuffer(16);
        try {
            result.addComponent(true, Unpooled.directBuffer(PADDING).writeZero(PADDING));
            int[] boundaries = Arrays.stream(new int[] {0, 1, 2, 31, data.length / 3,
                            data.length / 2, data.length - 1, data.length})
                    .filter(boundary -> boundary >= 0 && boundary <= data.length).distinct().sorted().toArray();
            for (int i = 1; i < boundaries.length; i++) {
                int first = boundaries[i - 1];
                int length = boundaries[i] - first;
                ByteBuf component = i % 2 == 0 ? Unpooled.buffer(length) : Unpooled.directBuffer(length);
                component.writeBytes(data, first, length);
                result.addComponent(true, component);
            }
            result.addComponent(true, Unpooled.buffer(PADDING).writeZero(PADDING));
            return result.setIndex(PADDING, PADDING + data.length);
        } catch (RuntimeException | Error failure) {
            result.release();
            throw failure;
        }
    }

    private static byte[] payload(int length, boolean repeated) {
        byte[] bytes = new byte[length];
        if (repeated) {
            Arrays.fill(bytes, (byte) 0x5a);
        } else {
            new Random(0x9747b28cL + length).nextBytes(bytes);
        }
        return bytes;
    }

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

    private static byte[] finishedEncode(byte[] data) {
        Deflater deflater = new Deflater();
        try {
            byte[] compressed = new byte[(int) Math.ceil(data.length * 1.001) + 14];
            deflater.setInput(data);
            deflater.finish();
            int length = deflater.deflate(compressed);
            assertThat(deflater.finished()).as("reference stream is complete").isTrue();
            return Arrays.copyOf(compressed, length);
        } finally {
            deflater.end();
        }
    }

    private static byte[] legacyDecode(byte[] encoded, int length) throws DataFormatException {
        Inflater inflater = new Inflater();
        try {
            inflater.setInput(encoded);
            byte[] output = new byte[length];
            assertThat(inflater.inflate(output)).isEqualTo(length);
            return output;
        } finally {
            inflater.end();
        }
    }

    private record BufferState(int readerIndex, int writerIndex, byte[] contents) {
        static BufferState capture(ByteBuf buffer) {
            byte[] contents = new byte[buffer.capacity()];
            buffer.getBytes(0, contents);
            return new BufferState(buffer.readerIndex(), buffer.writerIndex(), contents);
        }

        void assertUnchanged(ByteBuf buffer) {
            assertThat(buffer.readerIndex()).as("input reader index").isEqualTo(readerIndex);
            assertThat(buffer.writerIndex()).as("input writer index").isEqualTo(writerIndex);
            assertThat(buffer.refCnt()).as("input reference count").isEqualTo(1);
            byte[] actual = new byte[buffer.capacity()];
            buffer.getBytes(0, actual);
            assertThat(actual).as("input contents including padding").isEqualTo(contents);
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
