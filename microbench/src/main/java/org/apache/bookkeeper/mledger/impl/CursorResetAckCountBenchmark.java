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

import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.commons.lang3.mutable.MutableLong;
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
 * Counts the individual acknowledgements remaining after reset trims entries through the new mark-delete position.
 * Excludes persistence, locking, trimming and clearing; it measures only the counting step on the real range set.
 * The original loop is left intact to preserve the JVM's opportunity to optimize it.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class CursorResetAckCountBenchmark {

    @Param({"SINGLE_ENTRY", "SINGLE_RANGE_1K", "SINGLE_RANGE_1M", "MANY_LEDGERS", "MANY_SHORT_RANGES",
            "MANY_LONG_RANGES"})
    public Layout layout;

    private PositionRangeSet ranges;

    @Setup
    public void setup() {
        ranges = new PositionRangeSet(PositionFactory::create, false);
        for (long ledgerId = 1; ledgerId <= layout.ledgerCount; ledgerId++) {
            for (int rangeIndex = 0; rangeIndex < layout.rangesPerLedger; rangeIndex++) {
                // Leave one unacknowledged entry between ranges so they do not merge.
                long firstEntryId = rangeIndex * (layout.entriesPerRange + 1L);
                ranges.addOpenClosed(ledgerId, firstEntryId - 1, ledgerId,
                        firstEntryId + layout.entriesPerRange - 1);
            }
        }
        long expectedCount = (long) layout.ledgerCount * layout.rangesPerLedger * layout.entriesPerRange;
        if (perEntry() != expectedCount || perRange() != expectedCount) {
            throw new IllegalStateException("Acknowledgement count does not match layout " + layout);
        }
    }

    @Benchmark
    public long perEntry() {
        MutableLong count = new MutableLong();
        ranges.forEach(range -> {
            for (long entryId = range.lowerEndpoint().getEntryId() + 1;
                    entryId <= range.upperEndpoint().getEntryId(); entryId++) {
                count.incrementAndGet();
            }
            return true;
        });
        return count.longValue();
    }

    @Benchmark
    public long perRange() {
        MutableLong count = new MutableLong();
        ranges.forEachRawRange((lowerLedgerId, lowerEntryId, upperLedgerId, upperEntryId) -> {
            count.add(upperEntryId - lowerEntryId);
            return true;
        });
        return count.longValue();
    }

    public enum Layout {
        SINGLE_ENTRY(1, 1, 1),
        SINGLE_RANGE_1K(1, 1, 1024),
        SINGLE_RANGE_1M(1, 1, 1_048_576),
        MANY_LEDGERS(16, 1, 65_536),
        MANY_SHORT_RANGES(1, 1024, 1),
        MANY_LONG_RANGES(16, 64, 1024);

        private final int ledgerCount;
        private final int rangesPerLedger;
        private final int entriesPerRange;

        Layout(int ledgerCount, int rangesPerLedger, int entriesPerRange) {
            this.ledgerCount = ledgerCount;
            this.rangesPerLedger = rangesPerLedger;
            this.entriesPerRange = entriesPerRange;
        }
    }
}
