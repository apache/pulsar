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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.EntryReadCountHandlerImpl;
import org.apache.commons.lang3.tuple.Pair;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(1)
@State(Scope.Thread)
@Warmup(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
@Measurement(time = 10, timeUnit = TimeUnit.SECONDS, iterations = 1)
@Fork(1)
public class RangeCacheEvictionBenchmark {
    private final int numberOfEntries = 1_000_000;
    private final double propabitilyOfExpectedReadsRemaining = 0.10d;
    private final double propabitilyOfAccessedEntries = 0.10d;
    @Param({"true", "false"})
    private boolean useRequeuing;
    private List<EntryImpl> entries;
    private Random random = new Random(1);
    private RangeCacheRemovalQueue removalQueue;
    private RangeCache rangeCache;
    private long timestampNanosStart;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        entries = new ArrayList<>(numberOfEntries);
        for (int i = 0; i < numberOfEntries; i++) {
            EntryImpl entry = EntryImpl.create(0, i, new byte[0], useRequeuing ? 1 : 0);
            entries.add(entry);
        }
    }

    @Setup(Level.Invocation)
    public void setupIteration() {
        removalQueue = new RangeCacheRemovalQueue(5, true);
        rangeCache = new RangeCache(removalQueue);
        timestampNanosStart = System.nanoTime();
        for (int i = 0; i < numberOfEntries; i++) {
            EntryImpl entry = entries.get(i);
            entry.retain();
            rangeCache.put(entry.getPosition(), entry);
            if (useRequeuing) {
                ((EntryReadCountHandlerImpl) entry.getReadCountHandler()).setExpectedReadCount(1);
                if (random.nextDouble() > propabitilyOfExpectedReadsRemaining) {
                    entry.getReadCountHandler().markRead();
                }
                if (random.nextDouble() < propabitilyOfAccessedEntries) {
                    rangeCache.get(entry.getPosition());
                }
            }
        }
    }

    @TearDown(Level.Invocation)
    public void tearDownIteration() {
        Pair<Integer, Long> counters = rangeCache.clear();
        if (counters.getLeft() > 0 || counters.getRight() > 0) {
            throw new IllegalStateException("Cache wasn't empty after iteration: "
                    + "entries=" + counters.getLeft() + ", bytes=" + counters.getRight());
        }
    }

    @Benchmark
    public boolean evictAllEntries() {
        long evictBefore = timestampNanosStart;
        while (!removalQueue.isEmpty()) {
            timestampNanosStart = System.nanoTime();
            removalQueue.evictLEntriesBeforeTimestamp(evictBefore);
            evictBefore = timestampNanosStart;
        }
        return removalQueue.isEmpty();
    }
}