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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.DuplicatedByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Compares segmented input with Netty's coalescing NIO view, using the same codec and output allocator.
 * Input construction and compatibility checks are outside the measured methods; output allocation and release are not.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
@Threads(1)
public class ZLibCompositeBufferBenchmark {

    private static final int PADDING = 8;

    @Param({"8192", "131072", "1048576"})
    public int payloadSize;

    @Param({"1", "2", "8"})
    public int componentCount;

    @Param({"RANDOM", "REPEATED"})
    public String content;

    private CompressionCodecZLib codec;
    private ByteBuf input;
    private ByteBuf coalescingInput;
    private ByteBuf encodedInput;
    private ByteBuf coalescingEncodedInput;

    @Setup
    public void setup() throws IOException, DataFormatException {
        codec = new CompressionCodecZLib();
        byte[] payload = new byte[payloadSize];
        switch (content) {
            case "RANDOM" -> new Random(0x5eed5eedL + payloadSize).nextBytes(payload);
            case "REPEATED" -> Arrays.fill(payload, (byte) 42);
            default -> throw new IllegalArgumentException("Unknown content: " + content);
        }
        try {
            input = composite(payload);
            // Each adapter owns an additional reference to the same input; neither view copies the payload here.
            coalescingInput = new CoalescingByteBuf(input.retain());
            byte[] encoded = checkedEncoding(input, payload);
            byte[] coalesced = checkedEncoding(coalescingInput, payload);
            encodedInput = composite(coalesced);
            coalescingEncodedInput = new CoalescingByteBuf(encodedInput.retain());
            checkDecode(encodedInput, payload);
            checkDecode(coalescingEncodedInput, payload);
            // Also verify the coalesced decoder accepts the segmented encoder's output.
            ByteBuf segmentedEncoding = new CoalescingByteBuf(composite(encoded));
            try {
                checkDecode(segmentedEncoding, payload);
            } finally {
                segmentedEncoding.release();
            }
        } catch (IOException | DataFormatException | RuntimeException | Error failure) {
            tearDown();
            throw failure;
        }
    }

    @Benchmark
    public int segmentedEncode() {
        return encode(input);
    }

    @Benchmark
    public int coalescedEncode() {
        return encode(coalescingInput);
    }

    @Benchmark
    public int segmentedDecode() throws IOException {
        return decode(encodedInput);
    }

    @Benchmark
    public int coalescedDecode() throws IOException {
        return decode(coalescingEncodedInput);
    }

    private int encode(ByteBuf source) {
        ByteBuf encoded = codec.encode(source);
        try {
            return encoded.readableBytes();
        } finally {
            encoded.release();
        }
    }

    private int decode(ByteBuf source) throws IOException {
        ByteBuf decoded = codec.decode(source, payloadSize);
        try {
            return decoded.readableBytes() + decoded.getByte(decoded.readerIndex());
        } finally {
            decoded.release();
        }
    }

    private byte[] checkedEncoding(ByteBuf source, byte[] expected) throws DataFormatException {
        int readerIndex = source.readerIndex();
        int writerIndex = source.writerIndex();
        ByteBuf encoded = codec.encode(source);
        try {
            byte[] bytes = ByteBufUtil.getBytes(encoded);
            Inflater inflater = new Inflater();
            try {
                inflater.setInput(bytes);
                byte[] decoded = new byte[expected.length];
                if (inflater.inflate(decoded) != expected.length || !Arrays.equals(decoded, expected)) {
                    throw new IllegalStateException("Independent decoder rejected the benchmark encoding");
                }
            } finally {
                inflater.end();
            }
            checkIndexes(source, readerIndex, writerIndex);
            return bytes;
        } finally {
            encoded.release();
        }
    }

    private void checkDecode(ByteBuf source, byte[] expected) throws IOException {
        int readerIndex = source.readerIndex();
        int writerIndex = source.writerIndex();
        ByteBuf decoded = codec.decode(source, payloadSize);
        try {
            if (!Arrays.equals(ByteBufUtil.getBytes(decoded), expected)) {
                throw new IllegalStateException("Benchmark decoder returned a different payload");
            }
            checkIndexes(source, readerIndex, writerIndex);
        } finally {
            decoded.release();
        }
    }

    private static void checkIndexes(ByteBuf source, int readerIndex, int writerIndex) {
        if (source.readerIndex() != readerIndex || source.writerIndex() != writerIndex) {
            throw new IllegalStateException("Benchmark codec changed the input indexes");
        }
    }

    private ByteBuf composite(byte[] bytes) {
        CompositeByteBuf composite = Unpooled.compositeBuffer(componentCount);
        try {
            int offset = 0;
            for (int component = 0; component < componentCount; component++) {
                int length = (bytes.length - offset) / (componentCount - component);
                int prefix = component == 0 ? PADDING : 0;
                int suffix = component == componentCount - 1 ? PADDING : 0;
                ByteBuf part = Unpooled.directBuffer(prefix + length + suffix);
                part.writeZero(prefix).writeBytes(bytes, offset, length).writeZero(suffix);
                composite.addComponent(true, part);
                offset += length;
            }
            return composite.setIndex(PADDING, PADDING + bytes.length);
        } catch (RuntimeException | Error failure) {
            composite.release();
            throw failure;
        }
    }

    @TearDown
    public void tearDown() {
        try {
            if (coalescingEncodedInput != null) {
                coalescingEncodedInput.release();
                coalescingEncodedInput = null;
            }
            if (encodedInput != null) {
                encodedInput.release();
                encodedInput = null;
            }
            if (coalescingInput != null) {
                coalescingInput.release();
                coalescingInput = null;
            }
            if (input != null) {
                input.release();
                input = null;
            }
        } finally {
            FastThreadLocal.removeAll();
        }
    }

    /**
     * Forces the contiguous-view path while letting Netty perform the same coalescing copy as the original codec.
     */
    private static final class CoalescingByteBuf extends DuplicatedByteBuf {
        private CoalescingByteBuf(ByteBuf source) {
            super(source);
        }

        @Override
        public int nioBufferCount() {
            return 1;
        }
    }
}
