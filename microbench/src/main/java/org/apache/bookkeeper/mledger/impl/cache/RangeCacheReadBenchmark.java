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

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.ReferenceCountedEntry;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
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

/** Measures range reads, including retaining and releasing hits, for dense, partial and empty cache ranges. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class RangeCacheReadBenchmark {
    private static final int RANGE_COUNT = 16;

    @Param({"100", "1000"})
    public int batchSize;

    @Param({"0", "10", "100"})
    public int hitPercent;

    @Param({"false", "true"})
    public boolean visit;

    private RangeCache cache;
    private Position[] firstPositions;
    private Position[] lastPositions;
    private int range;

    @Setup
    public void setup() {
        cache = new RangeCache(new RangeCacheRemovalQueue(0, false));
        firstPositions = new Position[RANGE_COUNT];
        lastPositions = new Position[RANGE_COUNT];
        for (int r = 0; r < RANGE_COUNT; r++) {
            firstPositions[r] = PositionFactory.create(r, 0);
            lastPositions[r] = PositionFactory.create(r, batchSize - 1);
            for (int i = 0; i < batchSize; i++) {
                if (i % 100 < hitPercent) {
                    EntryImpl entry = EntryImpl.create(r, i, new byte[0]);
                    if (!cache.put(entry.getPosition(), entry)) {
                        entry.release();
                        throw new IllegalStateException("Failed to populate range cache");
                    }
                }
            }
        }
    }

    @Benchmark
    public int readRange() {
        int currentRange = range++;
        if (range == RANGE_COUNT) {
            range = 0;
        }
        CountingVisitor visitor = new CountingVisitor();
        if (visit) {
            cache.forEachInRange(firstPositions[currentRange], lastPositions[currentRange], visitor);
        } else {
            Collection<ReferenceCountedEntry> entries =
                    cache.getRange(firstPositions[currentRange], lastPositions[currentRange]);
            for (ReferenceCountedEntry entry : entries) {
                visitor.accept(entry);
                entry.release();
            }
        }
        return visitor.count;
    }

    @Benchmark
    public int readAndCopyRange() {
        int currentRange = range++;
        if (range == RANGE_COUNT) {
            range = 0;
        }
        RangeEntryCacheImpl.CachedEntries result = new RangeEntryCacheImpl.CachedEntries(0, batchSize);
        if (visit) {
            cache.forEachInRange(firstPositions[currentRange], lastPositions[currentRange], result);
        } else {
            Collection<ReferenceCountedEntry> entries =
                    cache.getRange(firstPositions[currentRange], lastPositions[currentRange]);
            for (ReferenceCountedEntry entry : entries) {
                result.accept(entry);
                entry.release();
            }
        }
        int count = 0;
        if (result.entries != null) {
            for (Entry entry : result.entries) {
                if (entry != null) {
                    count++;
                    entry.release();
                }
            }
        }
        return count;
    }

    private static class CountingVisitor implements Consumer<ReferenceCountedEntry> {
        private int count;

        @Override
        public void accept(ReferenceCountedEntry entry) {
            count++;
        }
    }

    @TearDown
    public void tearDown() {
        cache.clear();
    }
}
