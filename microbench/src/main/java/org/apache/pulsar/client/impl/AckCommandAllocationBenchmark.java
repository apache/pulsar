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

import io.netty.buffer.ByteBuf;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.collections.ConcurrentBitSet;
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
import org.openjdk.jmh.annotations.Warmup;

/** Measures ACK entry assembly and actual wire serialization; excludes pending-set draining and network writes. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class AckCommandAllocationBenchmark {
    @Param({"1", "100", "1000"})
    public int entries;

    @Param({"false", "true"})
    public boolean batchIndexes;

    private MessageIdImpl[] messageIds;
    private ConcurrentBitSet bitSet;
    private List<Map.Entry<MessageIdAdv, ConcurrentBitSet>> pendingBatchAcks;

    @Setup
    public void setup() {
        messageIds = new MessageIdImpl[entries];
        for (int i = 0; i < entries; i++) {
            // IDs above the Long cache expose the allocation cost of both boxed coordinates.
            messageIds[i] = new MessageIdImpl(10000 + i / 100, 100000 + i, 0);
        }
        bitSet = new ConcurrentBitSet(100);
        bitSet.set(5, 100);
        pendingBatchAcks = new ArrayList<>(entries);
        for (MessageIdImpl id : messageIds) {
            // The pending map already supplies entries to the tracker; map draining is outside this benchmark.
            pendingBatchAcks.add(new AbstractMap.SimpleImmutableEntry<>(id, bitSet));
        }
    }

    @Benchmark
    public int assembleAndSerialize() {
        List<MessageIdAdv> acks;
        List<Map.Entry<MessageIdAdv, ConcurrentBitSet>> batchAcks;
        if (batchIndexes) {
            acks = Collections.emptyList();
            batchAcks = new ArrayList<>(entries);
            for (Map.Entry<MessageIdAdv, ConcurrentBitSet> entry : pendingBatchAcks) {
                batchAcks.add(entry);
            }
        } else {
            acks = new ArrayList<>(entries);
            for (MessageIdImpl id : messageIds) {
                acks.add(id);
            }
            batchAcks = Collections.emptyList();
        }
        ByteBuf command = Commands.newMultiMessageAck(1, acks, batchAcks, -1);
        try {
            return command.readableBytes();
        } finally {
            command.release();
        }
    }
}
