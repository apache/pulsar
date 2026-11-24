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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures the managed ledger's tail inserts while a catch-up read inserts the entries that it read from storage into
 * the same cache: the tail thread adds entries to the current ledger, and the catch-up thread inserts runs of 100
 * consecutive entries at random positions of an earlier ledger and removes them again, as the eviction would.
 */
@State(Scope.Group)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(2)
public class RangeCacheCatchUpInsertBenchmark {
    private static final int CACHED_TAIL_ENTRIES = 10_000;
    private static final int TRIM_BATCH = 100;
    private static final int CATCH_UP_RUN = 100;
    private static final long CATCH_UP_LEDGER = 1;
    private static final long TAIL_LEDGER = 2;
    private static final int CATCH_UP_LEDGER_ENTRIES = 50_000;

    private final ByteBuf payload = Unpooled.wrappedBuffer(new byte[0]);
    private RangeCacheRemovalQueue removalQueue;
    private RangeCache cache;
    private RangeCache.Inserter tailInserter;
    private long tailEntryId;

    @Setup(Level.Iteration)
    public void setup() {
        removalQueue = new RangeCacheRemovalQueue(0, false);
        cache = new RangeCache(removalQueue);
        tailInserter = cache.newInserter();
        tailEntryId = 0;
        for (int i = 0; i < CACHED_TAIL_ENTRIES; i++) {
            insert(tailInserter, TAIL_LEDGER, tailEntryId++);
        }
    }

    @Benchmark
    @Group("catchUp")
    public void tailInsert() {
        insert(tailInserter, TAIL_LEDGER, tailEntryId++);
        if (tailEntryId % TRIM_BATCH == 0) {
            cache.removeRange(PositionFactory.create(TAIL_LEDGER, 0),
                    PositionFactory.create(TAIL_LEDGER, tailEntryId - CACHED_TAIL_ENTRIES), false);
            // the eviction drops the removed entries' wrappers from the removal queue, so that they're recycled
            removalQueue.evictLEntriesBeforeTimestamp(Long.MIN_VALUE);
        }
    }

    @Benchmark
    @Group("catchUp")
    public void catchUpInsertRun() {
        long first = ThreadLocalRandom.current().nextInt(CATCH_UP_LEDGER_ENTRIES - CATCH_UP_RUN);
        // a read from storage inserts its entries with an inserter of its own
        RangeCache.Inserter inserter = cache.newInserter();
        for (int i = 0; i < CATCH_UP_RUN; i++) {
            insert(inserter, CATCH_UP_LEDGER, first + i);
        }
        cache.removeRange(PositionFactory.create(CATCH_UP_LEDGER, first),
                PositionFactory.create(CATCH_UP_LEDGER, first + CATCH_UP_RUN), false);
    }

    private void insert(RangeCache.Inserter inserter, long ledgerId, long entryId) {
        EntryImpl entry = EntryImpl.create(ledgerId, entryId, payload);
        if (!inserter.put(entry.getPosition(), entry, entry.getLength())) {
            entry.release();
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        cache.clear();
        // drops the removed entries' wrappers, so that they're recycled
        removalQueue.evictLEntriesBeforeTimestamp(Long.MAX_VALUE);
    }
}
