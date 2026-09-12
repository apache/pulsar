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

import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.commons.lang3.tuple.Pair;
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

/** Measures a complete insertion/eviction cycle, including allocation, using retained real entries. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class CacheEvictionCycleBenchmark {
    @Param({"100", "1000"})
    public int batchSize;

    @Param({"false", "true"})
    public boolean requeue;

    private RangeCacheRemovalQueue queue;
    private RangeCache cache;
    private EntryImpl[] entries;

    @Setup
    public void setup() {
        queue = new RangeCacheRemovalQueue(1, false);
        cache = new RangeCache(queue);
        entries = new EntryImpl[batchSize];
        for (int i = 0; i < batchSize; i++) {
            entries[i] = EntryImpl.create(0, i, new byte[1], requeue ? 1 : 0);
        }
    }

    @Benchmark
    public long insertAndEvict() {
        for (EntryImpl entry : entries) {
            entry.retain();
            if (!cache.put(entry.getPosition(), entry)) {
                entry.release();
                throw new IllegalStateException("Duplicate cache entry");
            }
        }
        Pair<Integer, Long> removed = queue.evictLeastAccessedEntries(batchSize);
        if (removed.getLeft() != batchSize || !queue.isEmpty()) {
            throw new IllegalStateException("Incomplete eviction");
        }
        return removed.getRight();
    }

    @TearDown
    public void tearDown() {
        cache.clear();
        for (EntryImpl entry : entries) {
            entry.release();
        }
    }
}
