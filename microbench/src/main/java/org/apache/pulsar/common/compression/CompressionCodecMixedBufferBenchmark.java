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

import io.airlift.compress.Decompressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.snappy.SnappyDecompressor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.FastThreadLocal;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
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
 * Measures LZ4 and Snappy encoding from heap and direct inputs into the default direct output.
 * Includes output allocation and release. Run the same benchmark against the baseline and modified codecs.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
@Threads(1)
public class CompressionCodecMixedBufferBenchmark {

    private static final int PADDING = 8;

    @Param({"LZ4", "SNAPPY"})
    public String algorithm;

    @Param({"HEAP", "DIRECT"})
    public String inputType;

    @Param({"1024", "4096"})
    public int payloadSize;

    @Param({"RANDOM", "REPEATED"})
    public String content;

    private CompressionCodec codec;
    private ByteBuf input;
    private byte[] originalContents;
    private int originalReaderIndex;
    private int originalWriterIndex;
    private int originalRefCount;

    @Setup
    public void setup() {
        Decompressor referenceDecoder;
        switch (algorithm) {
            case "LZ4" -> {
                codec = new CompressionCodecLZ4();
                referenceDecoder = new Lz4Decompressor();
            }
            case "SNAPPY" -> {
                codec = new CompressionCodecSnappy();
                referenceDecoder = new SnappyDecompressor();
            }
            default -> throw new IllegalArgumentException("Unknown algorithm: " + algorithm);
        }
        byte[] payload = new byte[payloadSize];
        switch (content) {
            case "RANDOM" -> new Random(0x5eed5eedL + payloadSize).nextBytes(payload);
            case "REPEATED" -> Arrays.fill(payload, (byte) 42);
            default -> throw new IllegalArgumentException("Unknown content: " + content);
        }
        try {
            input = switch (inputType) {
                case "HEAP" -> Unpooled.buffer(payloadSize + 4 * PADDING);
                case "DIRECT" -> Unpooled.directBuffer(payloadSize + 4 * PADDING);
                default -> throw new IllegalArgumentException("Unknown input type: " + inputType);
            };
            input.writeZero(2 * PADDING).writeBytes(payload).writeZero(2 * PADDING);
            // A slice and a nonzero reader index exercise both offsets used by the heap and address paths.
            input = input.slice(PADDING, payloadSize + 2 * PADDING).setIndex(PADDING, PADDING + payloadSize);
            if ("DIRECT".equals(inputType) && !input.hasMemoryAddress()) {
                throw new IllegalStateException("This benchmark requires native addresses for direct inputs");
            }
            originalReaderIndex = input.readerIndex();
            originalWriterIndex = input.writerIndex();
            originalRefCount = input.refCnt();
            originalContents = new byte[input.capacity()];
            input.getBytes(0, originalContents);
            ByteBuf encoded = codec.encode(input);
            try {
                if (!encoded.isDirect() || !encoded.hasMemoryAddress()) {
                    throw new IllegalStateException("This benchmark requires the default direct addressable output");
                }
                byte[] bytes = ByteBufUtil.getBytes(encoded);
                byte[] decoded = new byte[payloadSize];
                int length = referenceDecoder.decompress(bytes, 0, bytes.length, decoded, 0, decoded.length);
                if (length != payloadSize || !Arrays.equals(decoded, payload)) {
                    throw new IllegalStateException("Array decoder rejected the benchmark encoding");
                }
                checkInput();
            } finally {
                encoded.release();
            }
        } catch (RuntimeException | Error failure) {
            releaseInput();
            FastThreadLocal.removeAll();
            throw failure;
        }
    }

    @Benchmark
    public int encode() {
        ByteBuf encoded = codec.encode(input);
        try {
            return encoded.readableBytes();
        } finally {
            encoded.release();
        }
    }

    @TearDown
    public void tearDown() {
        try {
            if (input != null) {
                checkInput();
            }
        } finally {
            releaseInput();
            FastThreadLocal.removeAll();
        }
    }

    private void checkInput() {
        if (input.readerIndex() != originalReaderIndex || input.writerIndex() != originalWriterIndex
                || input.refCnt() != originalRefCount || input.capacity() != originalContents.length) {
            throw new IllegalStateException("Benchmark encoder changed the input state");
        }
        // The GC profiler can include trial teardown in its final sample. Reuse the setup snapshot.
        for (int index = 0; index < originalContents.length; index++) {
            if (input.getByte(index) != originalContents[index]) {
                throw new IllegalStateException("Benchmark encoder changed input contents at " + index);
            }
        }
    }

    private void releaseInput() {
        if (input != null) {
            input.release();
            input = null;
        }
    }
}
