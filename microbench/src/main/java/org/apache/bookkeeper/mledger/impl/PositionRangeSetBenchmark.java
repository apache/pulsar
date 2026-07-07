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

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * Baseline throughput numbers for {@link PositionRangeSet} on realistic cursor workloads. Hot paths
 * exercised: {@code addOpenClosed} (per individual ack), {@code contains} (per message read),
 * {@code forEachRawRange} (per persistence flush), {@code removeAtMost} (per mark-delete), and
 * {@code toRanges} (serialization for the cursor ledger).
 *
 * <p>Run with:
 * <pre>{@code
 * ./gradlew :microbench:shadowJar
 * java -jar microbench/build/libs/microbench-*-benchmarks.jar PositionRangeSetBenchmark
 * }</pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(time = 2, iterations = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(time = 3, iterations = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class PositionRangeSetBenchmark {

    @Param({"10", "100", "1000"})
    public int ledgerCount;

    @Param({"100", "10000"})
    public int entriesPerLedger;

    private PositionRangeSet set;
    private final AtomicLong cursor = new AtomicLong();
    private long totalEntries;

    @Setup(Level.Trial)
    public void setup() {
        set = new PositionRangeSet(PositionFactory::create, false);
        // Pre-populate: every other entry in every ledger is "individually deleted" — models the
        // typical Shared/Key_Shared ack pattern that produces fragmented deleted-message ranges.
        for (int l = 0; l < ledgerCount; l++) {
            for (int e = 1; e < entriesPerLedger; e += 2) {
                set.addOpenClosed(l, e - 1, l, e);
            }
        }
        totalEntries = (long) ledgerCount * entriesPerLedger;
        cursor.set(0);
    }

    @Benchmark
    @Threads(1)
    public boolean addOpenClosedSameLedger() {
        // Simulate a new individual ack arriving on an existing ledger.
        long l = (cursor.getAndIncrement() % ledgerCount);
        long e = entriesPerLedger + (cursor.getAndIncrement() & 0xFF);
        set.addOpenClosed(l, e, l, e + 1);
        return true;
    }

    @Benchmark
    @Threads(1)
    public boolean containsHit() {
        // Read-path check: is this (already-deleted) entry in the set?
        long l = (cursor.getAndIncrement() & Long.MAX_VALUE) % ledgerCount;
        long e = 1 + 2 * ((cursor.getAndIncrement() & Long.MAX_VALUE) % (entriesPerLedger / 2));
        return set.contains(l, e);
    }

    @Benchmark
    @Threads(1)
    public boolean containsMiss() {
        // Read-path check: entry that was NOT individually deleted (still in the backlog).
        long l = (cursor.getAndIncrement() & Long.MAX_VALUE) % ledgerCount;
        long e = 2 * ((cursor.getAndIncrement() & Long.MAX_VALUE) % (entriesPerLedger / 2));
        return set.contains(l, e);
    }

    @Benchmark
    @Threads(1)
    public int forEachRawRange(Blackhole bh) {
        // Persistence path: walk every deleted range once per mark-delete flush.
        int[] count = {0};
        set.forEachRawRange((lowerKey, lowerValue, upperKey, upperValue) -> {
            count[0]++;
            return true;
        });
        return count[0];
    }

    @Benchmark
    @Threads(1)
    public Map<Long, long[]> toRanges() {
        // Serialization: builds the dense long[] map written to the cursor ledger.
        return set.toRanges(Integer.MAX_VALUE);
    }

    @Benchmark
    @Threads(1)
    public int size() {
        return set.size();
    }
}
