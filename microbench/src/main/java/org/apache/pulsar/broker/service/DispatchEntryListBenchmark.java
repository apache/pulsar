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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
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
 * Models a cursor read's list handoff, metadata wrapping and four-consumer dispatch preparation.
 * The input list and wrappers are fresh per invocation; cached entries and their metadata are reused.
 * This isolates list preparation rather than measuring a broker's throughput or exception recovery.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class DispatchEntryListBenchmark {
    @Param({"32", "100"})
    public int batchSize;

    @Param({"Shared", "Key_Shared"})
    public SubscriptionType subscriptionType;

    private Entry[] entries;

    @Setup
    public void setup() {
        entries = new Entry[batchSize];
        for (int i = 0; i < batchSize; i++) {
            EntryImpl entry = EntryImpl.create(1, i, new byte[0]);
            entry.setMessageMetadata(new MessageMetadata().setProducerName("producer").setSequenceId(i)
                    .setPublishTime(1));
            entries[i] = entry;
        }
    }

    @Benchmark
    public List<Entry> copyOwnershipAndDispatchLists(Blackhole blackhole) {
        return prepareDispatch(true, blackhole);
    }

    @Benchmark
    public List<Entry> reuseReadList(Blackhole blackhole) {
        return prepareDispatch(false, blackhole);
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    private List<Entry> cursorRead() {
        // Preserve the read/callback boundary: the cursor allocates the input list independently of dispatch.
        List<Entry> readEntries = new ArrayList<>();
        for (Entry entry : entries) {
            readEntries.add(entry);
        }
        return readEntries;
    }

    private List<Entry> prepareDispatch(boolean copyOwnership, Blackhole blackhole) {
        List<Entry> readEntries = cursorRead();
        List<Entry> ownedEntries = readEntries;
        if (copyOwnership || ownedEntries.getClass() != ArrayList.class) {
            ownedEntries = new ArrayList<>(ownedEntries);
        }
        for (int i = 0; i < ownedEntries.size(); i++) {
            ownedEntries.set(i, EntryAndMetadata.create(ownedEntries.get(i)));
        }
        List<Entry> dispatchEntries = copyOwnership || subscriptionType == SubscriptionType.Shared
                ? new ArrayList<>(ownedEntries) : ownedEntries;
        if (subscriptionType == SubscriptionType.Shared) {
            int consumerBatchSize = (batchSize + 3) / 4;
            for (int start = 0; start < batchSize; start += consumerBatchSize) {
                List<Entry> consumerEntries = dispatchEntries.subList(start,
                        Math.min(batchSize, start + consumerBatchSize));
                // Filtering replaces a slot in a Shared consumer's sublist.
                consumerEntries.set(0, null);
                blackhole.consume(consumerEntries);
            }
        } else {
            Map<Integer, List<Entry>> consumerEntries = new HashMap<>();
            for (int i = 0; i < dispatchEntries.size(); i++) {
                consumerEntries.computeIfAbsent(i % 4, ignored -> new ArrayList<>()).add(dispatchEntries.get(i));
            }
            consumerEntries.values().forEach(blackhole::consume);
        }
        return ownedEntries;
    }

    @TearDown
    public void tearDown() {
        for (Entry entry : entries) {
            entry.release();
        }
    }
}
