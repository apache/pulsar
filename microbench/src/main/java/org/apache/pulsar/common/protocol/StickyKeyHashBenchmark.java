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
package org.apache.pulsar.common.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.Base64;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.util.Murmur3_32Hash;
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

/**
 * Compares hashing a message's sticky key from its metadata, as Key_Shared dispatch does for each entry and
 * subscription: resolving the key into a byte array and hashing it, or hashing it where it is. The metadata is
 * parsed from a pooled direct buffer, as the broker has it. Run it with {@code -prof gc}.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
// java.lang open, as bin/pulsar runs the broker: without it, the string keys are encoded or decoded into arrays
@Fork(value = 2, jvmArgsAppend = "--add-opens=java.base/java.lang=ALL-UNNAMED")
public class StickyKeyHashBenchmark {
    /**
     * The sticky key: a plain partition key, the Base64 partition key of 16 or 8 binary bytes, as a producer sends a
     * byte[] key, or an ordering key of 16 bytes.
     */
    @Param({"plain", "base64-16", "base64-8", "ordering-16"})
    private String keyType;
    private MessageMetadata metadata;
    private ByteBuf buffer;

    @Setup
    public void setup() {
        Random random = new Random(1);
        byte[] binary = new byte[keyType.endsWith("-8") ? 8 : 16];
        random.nextBytes(binary);
        MessageMetadata written = new MessageMetadata().setProducerName("producer").setSequenceId(1)
                .setPublishTime(1);
        switch (keyType) {
            case "plain" -> written.setPartitionKey("device-123456");
            case "base64-16", "base64-8" -> written.setPartitionKey(Base64.getEncoder().encodeToString(binary))
                    .setPartitionKeyB64Encoded(true);
            case "ordering-16" -> written.setOrderingKey(binary);
            default -> throw new IllegalArgumentException(keyType);
        }
        // a pooled direct buffer, as the broker reads entries into
        buffer = PooledByteBufAllocator.DEFAULT.directBuffer();
        written.writeTo(buffer);
        metadata = new MessageMetadata();
        metadata.parseFrom(buffer, buffer.readableBytes());
        // reads the partition key's string, which the metadata then keeps
        metadata.getPartitionKey();
    }

    @TearDown
    public void tearDown() {
        buffer.release();
    }

    @Benchmark
    public int resolveStickyKeyThenHash() {
        return Murmur3_32Hash.getInstance().makeHash(Commands.resolveStickyKey(metadata));
    }

    @Benchmark
    public int resolveStickyKeyHash() {
        return Commands.resolveStickyKeyHash(metadata);
    }
}
