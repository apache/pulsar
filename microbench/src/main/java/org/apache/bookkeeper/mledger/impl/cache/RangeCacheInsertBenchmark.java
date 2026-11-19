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
import org.apache.bookkeeper.mledger.PositionFactory;
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

/**
 * Measures how a managed ledger's thread caches the entries that it adds: each insert goes to the tail of the cache,
 * the cursors' progress trims its head in batches, the eviction recycles the removed entries' wrappers, and the
 * ledger rolls over after a number of entries.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(2)
public class RangeCacheInsertBenchmark {
    private static final int TRIM_BATCH = 100;
    private static final int ENTRIES_PER_LEDGER = 50_000;

    @Param({"1000", "100000"})
    public int cachedEntries;

    private final ByteBuf payload = Unpooled.wrappedBuffer(new byte[0]);
    private RangeCacheRemovalQueue removalQueue;
    private RangeCache cache;
    private RangeCache.Inserter inserter;
    private long ledgerId;
    private long entryId;
    private long sequence;

    @Setup
    public void setup() {
        removalQueue = new RangeCacheRemovalQueue(0, false);
        cache = new RangeCache(removalQueue);
        // the managed ledger inserts its added entries with one inserter
        inserter = cache.newInserter();
        for (int i = 0; i < cachedEntries; i++) {
            insert();
        }
    }

    @Benchmark
    public boolean insertAtTail() {
        boolean inserted = insert();
        if (sequence % TRIM_BATCH == 0) {
            // the cursors moved past the oldest entries, as invalidateEntries does
            long first = sequence - cachedEntries;
            cache.removeRange(PositionFactory.create(-1, 0),
                    PositionFactory.create(first / ENTRIES_PER_LEDGER, first % ENTRIES_PER_LEDGER), false);
            // the eviction drops the removed entries' wrappers from the removal queue, so that they're recycled;
            // the cached entries are newer than the timestamp, which stops it at the first of them
            removalQueue.evictLEntriesBeforeTimestamp(Long.MIN_VALUE);
        }
        return inserted;
    }

    private boolean insert() {
        EntryImpl entry = EntryImpl.create(ledgerId, entryId, payload);
        boolean inserted = inserter.put(entry.getPosition(), entry, entry.getLength());
        if (!inserted) {
            entry.release();
        }
        sequence++;
        if (++entryId == ENTRIES_PER_LEDGER) {
            ledgerId++;
            entryId = 0;
        }
        return inserted;
    }

    @TearDown
    public void tearDown() {
        cache.clear();
    }
}
