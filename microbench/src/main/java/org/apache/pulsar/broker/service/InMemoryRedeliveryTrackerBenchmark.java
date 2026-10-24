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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
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
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures the tracker used once per dispatched entry, including when no entry has been redelivered.
 *
 * <p>Run the same benchmark against each tracker implementation, with {@code -t 1}, {@code -t 4},
 * and {@code -t 8}; all threads share one tracker, as consumers of one subscription do. Use
 * {@code -prof gc} to measure allocation as well as throughput. For example:
 * <pre>{@code
 * ./gradlew :microbench:shadowJar
 * java -jar microbench/build/libs/microbench-*-benchmarks.jar \
 *     InMemoryRedeliveryTrackerBenchmark.readOnly -t 8 -prof gc
 * }</pre>
 *
 * <p>The mixed workload updates existing positions, so it does not resize either map or allocate
 * positions during measurement. Its write percentages are operation ratios, rather than ratios of
 * reader and writer threads. Map expansion correctness belongs in the concurrent regression tests;
 * the original optimistic implementation is unsafe during structural changes.
 *
 * <p>The acknowledge workload interleaves an empty-tracker lookup for every entry with one
 * {@code removeBatch} for every {@code batchSize} entries. This includes the normal acknowledgement
 * path even when there are no redeliveries. One measured operation is either a lookup or a batch
 * acknowledgement, so compare throughput only at the same batch size.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(3)
public class InMemoryRedeliveryTrackerBenchmark {
    private static final int LEDGERS = 16;
    private static final int ENTRIES_PER_LEDGER = 256;
    private static final int POSITIONS = LEDGERS * ENTRIES_PER_LEDGER;

    @State(Scope.Benchmark)
    public static class ReadState {
        @Param({"empty", "hit", "innerMiss", "outerMiss"})
        public String lookup;

        private InMemoryRedeliveryTracker tracker;
        private long[] ledgerIds;
        private long[] entryIds;

        @Setup
        public void setup() {
            tracker = new InMemoryRedeliveryTracker();
            Position[] positions = positions();
            ledgerIds = new long[POSITIONS];
            entryIds = new long[POSITIONS];
            for (int i = 0; i < POSITIONS; i++) {
                Position position = positions[i];
                if (!"empty".equals(lookup)) {
                    tracker.incrementAndGetRedeliveryCount(position);
                }
                ledgerIds[i] = position.getLedgerId() + ("outerMiss".equals(lookup) ? LEDGERS : 0);
                entryIds[i] = position.getEntryId() + ("innerMiss".equals(lookup) ? ENTRIES_PER_LEDGER : 0);
            }
        }
    }

    @State(Scope.Benchmark)
    public static class MixedState {
        @Param({"1", "10"})
        public int writePercent;

        private InMemoryRedeliveryTracker tracker;
        private Position[] positions;

        @Setup
        public void setup() {
            tracker = new InMemoryRedeliveryTracker();
            positions = positions();
            for (Position position : positions) {
                tracker.incrementAndGetRedeliveryCount(position);
            }
        }
    }

    @State(Scope.Benchmark)
    public static class AckState {
        @Param({"1", "100"})
        public int batchSize;

        private InMemoryRedeliveryTracker tracker;
        private Position[] positions;
        private List<Position> ackBatch;

        @Setup
        public void setup() {
            tracker = new InMemoryRedeliveryTracker();
            positions = positions();
            ackBatch = List.copyOf(Arrays.asList(positions).subList(0, batchSize));
        }
    }

    @State(Scope.Thread)
    public static class Cursor {
        private int index;
        private int operation;

        int next() {
            return index++ & (POSITIONS - 1);
        }

        boolean write(int percent) {
            // Spread writes across each 100 operations rather than emitting them in a burst.
            operation += percent;
            if (operation >= 100) {
                operation -= 100;
                return true;
            }
            return false;
        }

        boolean acknowledge(int batchSize) {
            if (++operation > batchSize) {
                operation = 0;
                return true;
            }
            return false;
        }
    }

    @Benchmark
    public int readOnly(ReadState state, Cursor cursor) {
        int index = cursor.next();
        return state.tracker.getRedeliveryCount(state.ledgerIds[index], state.entryIds[index]);
    }

    @Benchmark
    public int mixed(MixedState state, Cursor cursor) {
        Position position = state.positions[cursor.next()];
        if (cursor.write(state.writePercent)) {
            return state.tracker.incrementAndGetRedeliveryCount(position);
        }
        return state.tracker.getRedeliveryCount(position.getLedgerId(), position.getEntryId());
    }

    @Benchmark
    public int acknowledge(AckState state, Cursor cursor) {
        if (cursor.acknowledge(state.batchSize)) {
            state.tracker.removeBatch(state.ackBatch);
            return 0;
        }
        Position position = state.positions[cursor.next()];
        return state.tracker.getRedeliveryCount(position.getLedgerId(), position.getEntryId());
    }

    private static Position[] positions() {
        Position[] positions = new Position[POSITIONS];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = PositionFactory.create(i / ENTRIES_PER_LEDGER, i % ENTRIES_PER_LEDGER);
        }
        return positions;
    }
}
