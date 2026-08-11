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

import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Compares the old span-based and new bitmap-based individual-ack checks on the managed-ledger read path.
 * A returned BookKeeper batch is a contiguous range from one ledger, although one cursor read can issue
 * several such reads while moving across ledgers.
 *
 * <p>The {@code *Decision} benchmarks isolate the fast-path lookup. The {@code *ThenFilter} benchmarks
 * include the existing per-entry membership checks and intermediate list allocation whenever the lookup
 * reports a possible individual ack. Run with {@code -prof gc} to compare allocation rates.
 *
 * <p>Run with:
 * <pre>{@code
 * ./gradlew :microbench:shadowJar
 * java -jar microbench/build/libs/microbench-*-benchmarks.jar \
 *   IndividualAckReadFilterBenchmark -prof gc
 * }</pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(time = 1, iterations = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(time = 1, iterations = 5, timeUnit = TimeUnit.SECONDS)
@Fork(2)
public class IndividualAckReadFilterBenchmark {

    private static final long TARGET_LEDGER_ID = 100;
    private static final long ENTRIES_PER_LEDGER = 100_000;

    @Param({"1", "10", "100", "1000"})
    public int batchSize;

    @Param({"SPARSE_GAP", "HIT_FIRST", "HIT_LAST", "HIT_EVERY_TEN", "HIT_ALL",
            "OUTSIDE_SPAN", "MISSING_LEDGER", "EMPTY_SET"})
    public String scenario;

    private PositionRangeSet individuallyDeletedMessages;
    private List<Position> entries;
    private Range<Position> entriesRange;
    private long firstEntryId;
    private long lastEntryId;

    @Setup(Level.Trial)
    public void setup() {
        individuallyDeletedMessages = new PositionRangeSet(PositionFactory::create, false);
        firstEntryId = ENTRIES_PER_LEDGER / 2;
        lastEntryId = firstEntryId + batchSize - 1;

        switch (scenario) {
            case "SPARSE_GAP" -> {
                addDeletedEntry(TARGET_LEDGER_ID, 0);
                addDeletedEntry(TARGET_LEDGER_ID, ENTRIES_PER_LEDGER - 1);
            }
            case "HIT_FIRST" -> addDeletedEntry(TARGET_LEDGER_ID, firstEntryId);
            case "HIT_LAST" -> addDeletedEntry(TARGET_LEDGER_ID, lastEntryId);
            case "HIT_EVERY_TEN" -> {
                for (long entryId = firstEntryId; entryId <= lastEntryId; entryId += 10) {
                    addDeletedEntry(TARGET_LEDGER_ID, entryId);
                }
            }
            case "HIT_ALL" -> individuallyDeletedMessages.addOpenClosed(
                    TARGET_LEDGER_ID, firstEntryId - 1, TARGET_LEDGER_ID, lastEntryId);
            case "OUTSIDE_SPAN" -> addDeletedEntry(TARGET_LEDGER_ID, 0);
            case "MISSING_LEDGER" -> {
                addDeletedEntry(TARGET_LEDGER_ID - 1, ENTRIES_PER_LEDGER - 1);
                addDeletedEntry(TARGET_LEDGER_ID + 1, 0);
            }
            case "EMPTY_SET" -> {
                // No individually deleted entries.
            }
            default -> throw new IllegalArgumentException("Unknown scenario: " + scenario);
        }

        entries = new ArrayList<>(batchSize);
        for (long entryId = firstEntryId; entryId <= lastEntryId; entryId++) {
            entries.add(PositionFactory.create(TARGET_LEDGER_ID, entryId));
        }
        entriesRange = Range.closed(entries.get(0), entries.get(entries.size() - 1));

        List<Position> oldResult = filterWithSpan();
        List<Position> newResult = filterWithContainsAny();
        if (!oldResult.equals(newResult)) {
            throw new IllegalStateException(
                    "Old and new filters disagree for " + scenario + ", batchSize=" + batchSize);
        }
    }

    @Benchmark
    @Threads(1)
    public boolean oldSpanDecision() {
        Range<Position> span = individuallyDeletedMessages.isEmpty() ? null : individuallyDeletedMessages.span();
        return span != null && entriesRange.isConnected(span);
    }

    @Benchmark
    @Threads(1)
    public boolean newContainsAnyDecision() {
        return individuallyDeletedMessages.containsAny(TARGET_LEDGER_ID, firstEntryId, lastEntryId);
    }

    @Benchmark
    @Threads(1)
    public List<Position> oldSpanThenFilter() {
        return filterWithSpan();
    }

    @Benchmark
    @Threads(1)
    public List<Position> newContainsAnyThenFilter() {
        return filterWithContainsAny();
    }

    private List<Position> filterWithSpan() {
        Range<Position> span = individuallyDeletedMessages.isEmpty() ? null : individuallyDeletedMessages.span();
        if (span == null || !entriesRange.isConnected(span)) {
            return entries;
        }
        return filterIndividuallyDeletedEntries();
    }

    private List<Position> filterWithContainsAny() {
        if (!individuallyDeletedMessages.containsAny(TARGET_LEDGER_ID, firstEntryId, lastEntryId)) {
            return entries;
        }
        return filterIndividuallyDeletedEntries();
    }

    private List<Position> filterIndividuallyDeletedEntries() {
        return Lists.newArrayList(Collections2.filter(entries, position ->
                !individuallyDeletedMessages.contains(position.getLedgerId(), position.getEntryId())));
    }

    private void addDeletedEntry(long ledgerId, long entryId) {
        individuallyDeletedMessages.addOpenClosed(ledgerId, entryId - 1, ledgerId, entryId);
    }
}
