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
package org.apache.bookkeeper.mledger.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.ThreadBoundExecutor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.impl.cache.EntryCache;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheManager;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.ThreadParams;

/** Measures lifecycle calls on real cursors sharing a real ledger and active-cursor container, without storage I/O. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class ManagedCursorLifecycleBenchmark {
    /** Both repeated-call benchmarks share one cursor across all JMH worker threads. */
    @State(Scope.Benchmark)
    public static class SharedState {
        private BookKeeper bookKeeper;
        private ManagedLedgerImpl ledger;
        private ManagedCursorImpl active;
        private ManagedCursorImpl inactive;

        @Setup(Level.Trial)
        public void setup() {
            bookKeeper = mock(BookKeeper.class, withSettings().stubOnly());
            OrderedExecutor workerPool = mock(OrderedExecutor.class, withSettings().stubOnly());
            when(bookKeeper.getMainWorkerPool()).thenReturn(workerPool);
            when(workerPool.chooseThread(any(Object.class)))
                    .thenReturn(mock(ThreadBoundExecutor.class, withSettings().stubOnly()));
            ManagedLedgerFactoryImpl factory = mock(ManagedLedgerFactoryImpl.class, withSettings().stubOnly());
            EntryCacheManager cacheManager = mock(EntryCacheManager.class, withSettings().stubOnly());
            EntryCache entryCache = mock(EntryCache.class, withSettings().stubOnly());
            // Empty cache prevents deactivation from scheduling eviction; stubOnly avoids recording benchmark calls.
            when(entryCache.getSize()).thenReturn(0L);
            when(factory.getEntryCacheManager()).thenReturn(cacheManager);
            when(cacheManager.getEntryCache(any())).thenReturn(entryCache);
            ManagedLedgerConfig config = new ManagedLedgerConfig();
            config.setCacheEvictionByExpectedReadCount(true);
            ledger = new ManagedLedgerImpl(factory, bookKeeper, null, config, null,
                    "public/default/persistent/cursor-lifecycle-benchmark");
            active = new ManagedCursorImpl(bookKeeper, ledger, "shared-active");
            inactive = new ManagedCursorImpl(bookKeeper, ledger, "shared-inactive");
            active.setActive();
        }

        @TearDown(Level.Trial)
        public void verify() {
            if (!active.isActive() || !ledger.isCursorActive(active)
                    || inactive.isActive() || ledger.isCursorActive(inactive)) {
                throw new AssertionError("Repeated lifecycle calls changed the established activity state");
            }
        }
    }

    /** Distinct cursors contend on the same ledger's lifecycle synchronization when their state changes. */
    @State(Scope.Thread)
    public static class ThreadState {
        private ManagedCursorImpl cursor;
        private ManagedLedgerImpl ledger;

        @Setup(Level.Trial)
        public void setup(SharedState shared, ThreadParams thread) {
            ledger = shared.ledger;
            cursor = new ManagedCursorImpl(shared.bookKeeper, ledger, "thread-" + thread.getThreadIndex());
        }

        @TearDown(Level.Trial)
        public void verify() {
            if (cursor.isActive() || ledger.isCursorActive(cursor)) {
                throw new AssertionError("A completed activation/deactivation pair must leave its cursor inactive");
            }
        }
    }

    @Benchmark
    public boolean repeatSetActive(SharedState state) {
        state.active.setActive();
        return state.active.isActive();
    }

    @Benchmark
    public boolean repeatSetInactive(SharedState state) {
        state.inactive.setInactive();
        return state.inactive.isActive();
    }

    @Benchmark
    @OperationsPerInvocation(2)
    public boolean activateThenDeactivate(ThreadState state) {
        state.cursor.setActive();
        state.cursor.setInactive();
        return state.cursor.isActive();
    }
}
