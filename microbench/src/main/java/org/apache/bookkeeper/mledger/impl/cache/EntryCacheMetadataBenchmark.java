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
package org.apache.bookkeeper.mledger.impl.cache;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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
 * Measures real cache-entry copy, insertion, metadata initialization and fanout read/release costs.
 * Eager initialization emulates parsing before insertion; deferred initialization uses CachedEntries.
 * This serial lifecycle benchmark measures total work, not the benefit of moving work between threads.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class EntryCacheMetadataBenchmark {
    @Param({"true", "false"})
    public boolean eagerMetadata;

    @Param({"0", "1", "20"})
    public int readers;

    private ByteBuf serialized;
    private RangeCache cache;
    private RangeCacheRemovalQueue removalQueue;
    private final Position position = PositionFactory.create(1, 0);

    @Setup
    public void setup() {
        MessageMetadata metadata = new MessageMetadata().setProducerName("producer-123")
                .setSequenceId(7).setPublishTime(123456789L).setPartitionKey("random-key-123");
        ByteBuf payload = Unpooled.buffer(128).writeZero(128);
        try {
            serialized = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, payload);
        } finally {
            payload.release();
        }
        removalQueue = new RangeCacheRemovalQueue(0, false);
        cache = new RangeCache(removalQueue);
    }

    @Benchmark
    public void insertReadAndEvict(Blackhole blackhole) {
        ByteBuf copied = serialized.copy();
        EntryImpl cached = EntryImpl.createWithRetainedDuplicate(position, copied, null, null);
        copied.release();
        if (eagerMetadata) {
            cached.initializeMessageMetadataIfNeeded("benchmark");
        }
        if (!cache.put(position, cached)) {
            cached.release();
            throw new IllegalStateException("Failed to insert entry");
        }
        for (int i = 0; i < readers; i++) {
            RangeEntryCacheImpl.CachedEntries result = new RangeEntryCacheImpl.CachedEntries(0, 1, "benchmark");
            cache.forEachInRange(position, position, result);
            Entry entry = result.entries.get(0);
            blackhole.consume(entry.getMessageMetadata().getSequenceId());
            blackhole.consume(entry.getMessageMetadata().getPartitionKey());
            entry.release();
        }
        // Clearing only the map leaves wrappers queued for eviction and makes the fixture grow
        // throughout the trial. Exercise the real removal queue so each operation is a full cycle.
        removalQueue.evictLeastAccessedEntries(Long.MAX_VALUE);
    }

    @TearDown(Level.Iteration)
    public void verifyEmpty() {
        if (cache.getSize() != 0 || !removalQueue.isEmpty()) {
            throw new IllegalStateException("Cache lifecycle did not finish eviction");
        }
    }

    @TearDown
    public void tearDown() {
        cache.clear();
        serialized.release();
    }
}
