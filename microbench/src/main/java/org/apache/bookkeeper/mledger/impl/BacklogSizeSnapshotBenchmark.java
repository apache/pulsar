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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheManager;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
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
import org.openjdk.jmh.annotations.Warmup;

/** Compares per-subscription backlog estimates, including snapshot construction. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class BacklogSizeSnapshotBenchmark {
    @Param({"50", "500"})
    int ledgerCount;

    @Param({"1", "100", "10000"})
    int subscriptionCount;

    ManagedLedgerImpl ledger;
    Position[] positions;

    @Setup(Level.Trial)
    public void setup() {
        BookKeeper bookKeeper = mock(BookKeeper.class);
        when(bookKeeper.getMainWorkerPool()).thenReturn(mock(OrderedExecutor.class));
        ManagedLedgerFactoryImpl factory = mock(ManagedLedgerFactoryImpl.class);
        when(factory.getEntryCacheManager()).thenReturn(mock(EntryCacheManager.class));
        ledger = new ManagedLedgerImpl(factory, bookKeeper, null, new ManagedLedgerConfig(), null, "benchmark");

        long totalSize = 0;
        for (int i = 0; i < ledgerCount; i++) {
            long size = 400_000L + (i % 11) * 20_000L;
            long entries = 800 + (i % 7) * 50L;
            // The last ledger is active: its live counters, not its stored metadata, contain its size.
            boolean current = i == ledgerCount - 1;
            ledger.ledgers.put((long) i, new LedgerInfo().setLedgerId(i)
                    .setSize(current ? 0 : size).setEntries(current ? 0 : entries));
            totalSize += size;
            if (current) {
                ledger.currentLedger = mock(LedgerHandle.class);
                when(ledger.currentLedger.getId()).thenReturn((long) i);
                ledger.currentLedgerSize = size;
                ledger.currentLedgerEntries = entries;
                ledger.lastConfirmedEntry = PositionFactory.create(i, entries - 1);
            }
        }
        ManagedLedgerImpl.TOTAL_SIZE_UPDATER.set(ledger, totalSize);

        positions = new Position[subscriptionCount];
        for (int i = 0; i < positions.length; i++) {
            // Spread subscriptions across old and current ledgers, including partial consumption.
            positions[i] = PositionFactory.create((long) i * ledgerCount / positions.length, i % 800);
        }

        ToLongFunction<Position> snapshot = ledger.getEstimatedBacklogSizeSnapshot();
        for (Position position : positions) {
            if (snapshot.applyAsLong(position) != ledger.getEstimatedBacklogSize(position)) {
                throw new IllegalStateException("Snapshot differs from the existing estimator at " + position);
            }
        }
    }

    @Benchmark
    public long perSubscription() {
        long result = 0;
        for (Position position : positions) {
            result += ledger.getEstimatedBacklogSize(position);
        }
        return result;
    }

    @Benchmark
    public long snapshot() {
        ToLongFunction<Position> snapshot = ledger.getEstimatedBacklogSizeSnapshot();
        long result = 0;
        for (Position position : positions) {
            result += snapshot.applyAsLong(position);
        }
        return result;
    }
}
