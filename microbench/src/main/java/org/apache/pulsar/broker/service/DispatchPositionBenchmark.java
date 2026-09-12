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
package org.apache.pulsar.broker.service;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
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
 * Models dispatch lookups in ManagedCursorImpl's batch-deleted-index map using cached ledger entries.
 * Entry positions are already initialized by cache insertion, as in the broker cache-hit path.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class DispatchPositionBenchmark {
    private static final int ENTRY_COUNT = 10000;

    @Param({"0", "128", "10000"})
    public int acknowledgedEntries;

    private Entry[] entries;
    private ConcurrentSkipListMap<Position, long[]> ackSets;
    private int index;

    @Setup
    public void setup() {
        entries = new Entry[ENTRY_COUNT];
        ackSets = new ConcurrentSkipListMap<>();
        MessageMetadata metadata = new MessageMetadata().setProducerName("producer").setSequenceId(1)
                .setPublishTime(1);
        for (int i = 0; i < ENTRY_COUNT; i++) {
            EntryImpl entry = EntryImpl.create(i / 1000, i % 1000, new byte[0]);
            Position position = entry.getPosition();
            entries[i] = EntryAndMetadata.create(entry, metadata);
            if (i < acknowledgedEntries) {
                ackSets.put(position, new long[] {1});
            }
        }
    }

    private Entry nextEntry() {
        Entry entry = entries[index++];
        if (index == entries.length) {
            index = 0;
        }
        return entry;
    }

    @Benchmark
    public long[] recreatedPosition() {
        Entry entry = nextEntry();
        return ackSets.get(PositionFactory.create(entry.getLedgerId(), entry.getEntryId()));
    }

    @Benchmark
    public long[] cachedPosition() {
        return ackSets.get(nextEntry().getPosition());
    }

    @Benchmark
    public long[] skipPositionForEmptyMap() {
        Entry entry = nextEntry();
        if (ackSets.isEmpty()) {
            return null;
        }
        return ackSets.get(PositionFactory.create(entry.getLedgerId(), entry.getEntryId()));
    }

    @TearDown
    public void tearDown() {
        for (Entry entry : entries) {
            entry.release();
        }
    }
}
