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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.ThreadBoundExecutor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.cache.EntryCache;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheManager;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/** Measures real backlog estimators over a stationary metadata set, without BookKeeper or metadata-store I/O. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class ManagedLedgerBacklogSizeBenchmark {
    public enum Estimator {
        PER_POSITION,
        BATCH
    }

    public enum Placement {
        CLUSTERED_TAIL,
        OPPOSITE_ENDS
    }

    @State(Scope.Benchmark)
    public static class SharedState {
        @Param({"100", "10000"})
        public int ledgerCount;

        @Param({"2", "100"})
        public int subscriptionCount;

        @Param
        public Estimator estimator;

        @Param
        public Placement placement;

        private ManagedLedgerImpl ledger;
        private List<Position> positions;
        private long monitorSequence;

        @Setup(Level.Trial)
        public void setup() {
            BookKeeper bookKeeper = mock(BookKeeper.class, withSettings().stubOnly());
            OrderedExecutor workerPool = mock(OrderedExecutor.class, withSettings().stubOnly());
            when(bookKeeper.getMainWorkerPool()).thenReturn(workerPool);
            when(workerPool.chooseThread(any(Object.class)))
                    .thenReturn(mock(ThreadBoundExecutor.class, withSettings().stubOnly()));
            ManagedLedgerFactoryImpl factory = mock(ManagedLedgerFactoryImpl.class, withSettings().stubOnly());
            EntryCacheManager cacheManager = mock(EntryCacheManager.class, withSettings().stubOnly());
            when(factory.getEntryCacheManager()).thenReturn(cacheManager);
            when(cacheManager.getEntryCache(any())).thenReturn(mock(EntryCache.class, withSettings().stubOnly()));
            ledger = new ManagedLedgerImpl(factory, bookKeeper, null, new ManagedLedgerConfig(), null,
                    "public/default/persistent/backlog-size-benchmark");

            // Closed-ledger metadata keeps timed estimator calls independent of mock behavior and storage I/O.
            long totalSize = 0;
            for (long ledgerId = 1; ledgerId <= ledgerCount; ledgerId++) {
                long size = 1024 * 1024 + ledgerId % 17;
                ledger.getLedgersInfo().put(ledgerId,
                        new LedgerInfo().setLedgerId(ledgerId).setEntries(1024).setSize(size));
                totalSize += size;
            }
            ManagedLedgerImpl.TOTAL_SIZE_UPDATER.set(ledger, totalSize);

            positions = new ArrayList<>(subscriptionCount);
            for (int i = 0; i < subscriptionCount; i++) {
                long ledgerId = switch (placement) {
                    case CLUSTERED_TAIL -> ledgerCount - i % Math.min(4, ledgerCount);
                    case OPPOSITE_ENDS -> i % 2 == 0 ? 1 : ledgerCount;
                };
                positions.add(PositionFactory.create(ledgerId, 512 + i % 512));
            }
            Map<Position, Long> expected = estimatePerPosition();
            Map<Position, Long> actual = ManagedLedgerBacklogSizeEstimator.estimate(ledger, positions);
            if (!expected.equals(actual)) {
                throw new AssertionError("Batch estimates differ from the per-position estimates");
            }
        }

        private Map<Position, Long> estimatePerPosition() {
            Map<Position, Long> estimates = new HashMap<>();
            for (Position position : positions) {
                estimates.put(position, ledger.getEstimatedBacklogSize(position));
            }
            return estimates;
        }

        private long estimate() {
            long totalBacklogSize = 0;
            if (estimator == Estimator.PER_POSITION) {
                for (Position position : positions) {
                    totalBacklogSize += ledger.getEstimatedBacklogSize(position);
                }
            } else {
                Map<Position, Long> estimates = ManagedLedgerBacklogSizeEstimator.estimate(ledger, positions);
                for (Position position : positions) {
                    totalBacklogSize += estimates.get(position);
                }
            }
            return totalBacklogSize;
        }
    }

    /** The poll clock belongs only to the stats thread and never holds the managed-ledger monitor while waiting. */
    @State(Scope.Thread)
    public static class PollState {
        @Param({"20000"})
        public long pollPeriodMicros;

        private long periodNanos;
        private long nextPollNanos;

        @Setup(Level.Iteration)
        public void setup() {
            if (pollPeriodMicros <= 0) {
                throw new IllegalArgumentException("pollPeriodMicros must be positive");
            }
            periodNanos = TimeUnit.MICROSECONDS.toNanos(pollPeriodMicros);
            nextPollNanos = System.nanoTime() + periodNanos;
        }

        private void awaitNextPoll(PollCounters counters) {
            long now = System.nanoTime();
            if (now > nextPollNanos) {
                // An overrun drops missed slots instead of issuing a burst of catch-up stats requests.
                long skipped = (now - nextPollNanos) / periodNanos + 1;
                counters.skippedPolls += skipped;
                nextPollNanos += skipped * periodNanos;
            }
            long remaining;
            while ((remaining = nextPollNanos - System.nanoTime()) > 0) {
                LockSupport.parkNanos(remaining);
            }
            nextPollNanos += periodNanos;
        }
    }

    /** Raw counts let the caller verify that each estimator sustained the requested polling cadence. */
    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class PollCounters {
        public long polls;
        public long skippedPolls;
    }

    /** One invocation estimates all subscriptions; use the GC profiler to measure allocation per stats request. */
    @Benchmark
    public long estimate(SharedState state) {
        return state.estimate();
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @Group("contended")
    @GroupThreads(1)
    public long stats(SharedState state) {
        return state.estimate();
    }

    /** A synthetic probe of the monitor also used by writes; this does not execute addEntry or storage I/O. */
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @Group("contended")
    @GroupThreads(1)
    public long monitorProbe(SharedState state) {
        synchronized (state.ledger) {
            return ++state.monitorSequence;
        }
    }

    /** This score includes pacing delay and is a polling-cycle latency, not the estimator latency. */
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @Group("periodic")
    @GroupThreads(1)
    public long periodicStats(SharedState state, PollState pollState, PollCounters counters) {
        pollState.awaitNextPoll(counters);
        counters.polls++;
        return state.estimate();
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @Group("periodic")
    @GroupThreads(1)
    public long periodicMonitorProbe(SharedState state) {
        synchronized (state.ledger) {
            return ++state.monitorSequence;
        }
    }
}
