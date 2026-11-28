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
package org.apache.pulsar.client.impl;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
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
 * Measures key construction, hashing and lookup for key-based batch grouping. Message creation and map population
 * are outside the measurement; this does not measure sending a batch or end-to-end producer throughput.
 * Run with {@code -prof gc} to include allocated bytes per lookup.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(2)
public class KeyBasedBatchGroupingBenchmark {
    private static final int KEY_COUNT = 1024;

    @Param({"TEXT_ASCII", "TEXT_UTF8", "BYTES", "ORDERING"})
    public String keyType;

    @Param({"16", "64"})
    public int keyLength;

    private final MessageImpl<?>[] messages = new MessageImpl<?>[KEY_COUNT];
    private final Map<String, Integer> originalBatches = new HashMap<>();
    private final Map<String, Integer> normalizedBatches = new HashMap<>();
    private final Map<String, Integer> latin1Batches = new HashMap<>();
    private final Map<String, Integer> candidateBatches = new HashMap<>();
    private int index;

    @Setup
    public void setup() {
        Random random = new Random(1);
        for (int i = 0; i < KEY_COUNT; i++) {
            MessageMetadata metadata = new MessageMetadata().setProducerName("producer")
                    .setSequenceId(i).setPublishTime(1);
            byte[] key = new byte[keyLength];
            random.nextBytes(key);
            ByteBuffer.wrap(key).putInt(i);
            switch (keyType) {
                case "TEXT_ASCII" -> metadata.setPartitionKey(String.format("%08x", i)
                        + "a".repeat(keyLength - 8));
                case "TEXT_UTF8" -> metadata.setPartitionKey(String.format("%08x", i)
                        + "\u00e9".repeat((keyLength - 8) / 2));
                case "BYTES" -> metadata.setPartitionKey(Base64.getEncoder().encodeToString(key))
                        .setPartitionKeyB64Encoded(true);
                case "ORDERING" -> metadata.setOrderingKey(key);
                default -> throw new IllegalArgumentException(keyType);
            }
            MessageImpl<?> message = MessageImpl.create(metadata, ByteBuffer.wrap(new byte[1]),
                    Schema.BYTES, "topic");
            messages[i] = message;
            if (Commands.resolveStickyKey(message.getMessageBuilder()).length != keyLength) {
                throw new IllegalStateException("Incorrect raw key length");
            }
            originalBatches.put(originalKey(message), i);
            normalizedBatches.put(normalizedKey(message), i);
            latin1Batches.put(latin1Key(message), i);
            candidateBatches.put(candidateKey(message), i);
        }
        if (originalBatches.size() != KEY_COUNT || normalizedBatches.size() != KEY_COUNT
                || latin1Batches.size() != KEY_COUNT || candidateBatches.size() != KEY_COUNT) {
            throw new IllegalStateException("Keys must be distinct");
        }
        for (int i = 0; i < KEY_COUNT; i++) {
            Integer expected = i;
            if (!expected.equals(originalBatches.get(originalKey(messages[i])))
                    || !expected.equals(normalizedBatches.get(normalizedKey(messages[i])))
                    || !expected.equals(latin1Batches.get(latin1Key(messages[i])))
                    || !expected.equals(candidateBatches.get(candidateKey(messages[i])))) {
                throw new IllegalStateException("Grouping lookup changed the batch");
            }
        }
    }

    @TearDown
    public void tearDown() {
        for (MessageImpl<?> message : messages) {
            if (message != null) {
                message.getDataBuffer().release();
                message.recycle();
            }
        }
    }

    @Benchmark
    public Integer originalGroupingLookup() {
        return originalBatches.get(originalKey(nextMessage()));
    }

    @Benchmark
    public Integer normalizedGroupingLookup() {
        return normalizedBatches.get(normalizedKey(nextMessage()));
    }

    @Benchmark
    public Integer latin1GroupingLookup() {
        return latin1Batches.get(latin1Key(nextMessage()));
    }

    @Benchmark
    public Integer candidateGroupingLookup() {
        return candidateBatches.get(candidateKey(nextMessage()));
    }

    private MessageImpl<?> nextMessage() {
        index = (index + 1) & (KEY_COUNT - 1);
        return messages[index];
    }

    private static String originalKey(MessageImpl<?> message) {
        if (message.hasOrderingKey()) {
            return Base64.getEncoder().encodeToString(message.getOrderingKey());
        }
        return message.getKey();
    }

    private static String normalizedKey(MessageImpl<?> message) {
        if (message.hasOrderingKey() || message.hasKey()) {
            return Base64.getEncoder().encodeToString(Commands.resolveStickyKey(message.getMessageBuilder()));
        }
        return null;
    }

    private static String latin1Key(MessageImpl<?> message) {
        if (message.hasOrderingKey() || message.hasKey()) {
            return new String(Commands.resolveStickyKey(message.getMessageBuilder()), StandardCharsets.ISO_8859_1);
        }
        return null;
    }

    private static String candidateKey(MessageImpl<?> message) {
        if (!message.hasOrderingKey() && message.hasKey() && !message.hasBase64EncodedKey()) {
            String key = message.getKey();
            if (isAscii(key)) {
                return key;
            }
        }
        return latin1Key(message);
    }

    private static boolean isAscii(String key) {
        for (int i = 0; i < key.length(); i++) {
            if (key.charAt(i) > 0x7f) {
                return false;
            }
        }
        return true;
    }
}
