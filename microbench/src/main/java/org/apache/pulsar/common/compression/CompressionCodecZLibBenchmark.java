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
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Measures ZLIB compression of producer payloads and decompression of consumer payloads.
 * Inputs rotate through a prebuilt corpus of structured log batches or incompressible data.
 * Heap inputs provide a control for the direct-input path; result allocation and release are included.
 *
 * <p>Build and run with allocation profiling:
 * <pre>
 * ./gradlew :microbench:shadowJar
 * java -jar microbench/build/libs/microbench-*-benchmarks.jar '.*CompressionCodecZLibBenchmark.*' \
 *     -p size=131072 -p dataType=LOG -prof gc
 * </pre>
 * Run the same benchmark against each production revision to compare implementations.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class CompressionCodecZLibBenchmark {

    private static final int CORPUS_SIZE = 16;

    @Param({"1024", "131072", "1048576"})
    public int size;

    @Param({"HEAP", "DIRECT"})
    public String inputType;

    @Param({"LOG", "RANDOM"})
    public String dataType;

    private CompressionCodec codec;
    private ByteBuf[] source;
    private ByteBuf[] encoded;
    private int cursor;

    @Setup
    public void setup() {
        codec = new CompressionCodecZLib();
        source = new ByteBuf[CORPUS_SIZE];
        encoded = new ByteBuf[CORPUS_SIZE];
        Random random = new Random(42);
        for (int i = 0; i < CORPUS_SIZE; i++) {
            byte[] payload = new byte[size];
            if ("RANDOM".equals(dataType)) {
                random.nextBytes(payload);
            } else {
                fillLogPayload(payload, random, i);
            }
            source[i] = inputBuffer(size).writeBytes(payload);
            ByteBuf compressed = codec.encode(source[i]);
            try {
                encoded[i] = inputBuffer(compressed.readableBytes());
                encoded[i].writeBytes(compressed, compressed.readerIndex(), compressed.readableBytes());
            } finally {
                compressed.release();
            }
        }
    }

    private ByteBuf inputBuffer(int capacity) {
        return "DIRECT".equals(inputType)
                ? PooledByteBufAllocator.DEFAULT.directBuffer(capacity, capacity)
                : PooledByteBufAllocator.DEFAULT.heapBuffer(capacity, capacity);
    }

    private static void fillLogPayload(byte[] payload, Random random, int sample) {
        int offset = 0;
        int sequence = 0;
        while (offset < payload.length) {
            String record = "{\"timestamp\":" + (1700000000000L + sequence)
                    + ",\"level\":\"INFO\",\"service\":\"checkout\",\"event\":\"order_completed\",\"batch\":" + sample
                    + ",\"orderId\":\"" + Long.toHexString(random.nextLong())
                    + "\",\"customerId\":" + random.nextInt(100_000)
                    + ",\"amount\":" + random.nextInt(10_000) + ",\"currency\":\"USD\"}\n";
            byte[] bytes = record.getBytes(StandardCharsets.UTF_8);
            int length = Math.min(bytes.length, payload.length - offset);
            System.arraycopy(bytes, 0, payload, offset, length);
            offset += length;
            sequence++;
        }
    }

    @Benchmark
    public void encode(Blackhole blackhole) {
        ByteBuf compressed = codec.encode(source[cursor++ & (CORPUS_SIZE - 1)]);
        try {
            blackhole.consume(compressed);
        } finally {
            compressed.release();
        }
    }

    @Benchmark
    public void decode(Blackhole blackhole) throws IOException {
        ByteBuf uncompressed = codec.decode(encoded[cursor++ & (CORPUS_SIZE - 1)], size);
        try {
            blackhole.consume(uncompressed);
        } finally {
            uncompressed.release();
        }
    }

    @TearDown
    public void tearDown() {
        for (int i = 0; i < CORPUS_SIZE; i++) {
            source[i].release();
            encoded[i].release();
        }
        FastThreadLocal.removeAll();
    }
}
