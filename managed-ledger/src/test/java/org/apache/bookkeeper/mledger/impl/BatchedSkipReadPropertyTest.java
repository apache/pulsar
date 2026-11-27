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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import it.unimi.dsi.fastutil.longs.LongLongPair;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongPredicate;
import org.testng.annotations.Test;

/**
 * Property tests of the range scan of a batched skip-read, over random skip patterns and parameters.
 */
public class BatchedSkipReadPropertyTest {

    private static final int MAX_RANGES = 4;

    /**
     * The same scan as {@code buildRanges}, written as a straight-line reference over a skip set
     * that is fixed for the whole scan: every entry is classified once, before the scan starts, so
     * that the reference cannot disagree with itself about an entry. It builds the ranges and the
     * holes only; where the scan stops is checked separately, by what the continuation read needs.
     */
    private static Scan referenceScan(long firstEntry, long lastEntry, BitSet skipped, long maxHoleDistance,
                                      long maxRangeEntries, int maxRanges) {
        Scan scan = new Scan();
        long rangeStart = -1;
        long rangeEnd = -1;
        for (long entryId = firstEntry; entryId <= lastEntry; entryId++) {
            if (rangeStart == -1 && scan.ranges.size() == maxRanges) {
                return scan;
            }
            if (skipped.get((int) entryId)) {
                if (rangeStart == -1) {
                    continue;
                }
                long next = entryId;
                while (next <= lastEntry && skipped.get((int) next)) {
                    next++;
                }
                if (next <= lastEntry && next - rangeEnd <= maxHoleDistance
                        && next - rangeStart < maxRangeEntries) {
                    scan.holes.add(LongLongPair.of(rangeEnd + 1, next - 1));
                    entryId = next - 1;
                    rangeEnd = next;
                    continue;
                }
                scan.ranges.add(LongLongPair.of(rangeStart, rangeEnd));
                rangeStart = -1;
                entryId = next - 2;
                continue;
            }
            if (rangeStart == -1) {
                rangeStart = entryId;
            }
            rangeEnd = entryId;
        }
        if (rangeStart != -1) {
            scan.ranges.add(LongLongPair.of(rangeStart, rangeEnd));
        }
        return scan;
    }

    private static final class Scan {
        final List<LongLongPair> ranges = new ArrayList<>();
        final List<LongLongPair> holes = new ArrayList<>();
        long stoppedAt = -1;
    }

    private static Scan actualScan(long firstEntry, long lastEntry, BitSet skipped, long maxHoleDistance,
                                   long maxRangeEntries, int maxRanges, AtomicInteger tests, Set<Long> tested) {
        Scan scan = new Scan();
        LongPredicate shouldSkip = entryId -> {
            tests.incrementAndGet();
            tested.add(entryId);
            return skipped.get((int) entryId);
        };
        scan.stoppedAt = ManagedLedgerImpl.buildRanges(firstEntry, lastEntry, shouldSkip, maxRanges, scan.ranges,
                maxHoleDistance, maxRangeEntries, scan.holes);
        return scan;
    }

    /**
     * For every skip pattern and merge parameters, the scan equals the reference scan, and it tests
     * no entry more than once.
     */
    @Test
    public void testScanMatchesReferenceAndTestsEachEntryOnce() {
        Random random = new Random(22560);
        for (int iteration = 0; iteration < 50_000; iteration++) {
            int firstEntry = random.nextInt(0, 3);
            int lastEntry = firstEntry + random.nextInt(0, 24);
            BitSet skipped = new BitSet();
            double skipProbability = random.nextDouble();
            for (int entryId = firstEntry; entryId <= lastEntry; entryId++) {
                if (random.nextDouble() < skipProbability) {
                    skipped.set(entryId);
                }
            }
            long maxHoleDistance = random.nextInt(0, 10);
            long maxRangeEntries = random.nextInt(1, 30);
            int maxRanges = random.nextInt(1, MAX_RANGES + 2);

            Scan expected = referenceScan(firstEntry, lastEntry, skipped, maxHoleDistance, maxRangeEntries,
                    maxRanges);
            AtomicInteger tests = new AtomicInteger();
            Set<Long> testedEntries = new HashSet<>();
            Scan actual = actualScan(firstEntry, lastEntry, skipped, maxHoleDistance, maxRangeEntries, maxRanges,
                    tests, testedEntries);
            String context = "first=" + firstEntry + " last=" + lastEntry + " skipped=" + skipped + " maxHoleDistance="
                    + maxHoleDistance + " maxRangeEntries=" + maxRangeEntries + " maxRanges=" + maxRanges;
            assertEquals(actual.ranges, expected.ranges, context);
            assertEquals(actual.holes, expected.holes, context);
            // Where the scan stops: nowhere when the whole window was scanned; otherwise on the first
            // readable entry that is not in a range - the one the continuation read has to start at,
            // so that no readable entry is read twice or left out
            // Where the scan stops: nowhere when the whole window was scanned with fewer ranges
            // than the cap allows; otherwise on the first readable entry that is not in a range -
            // the one the continuation read has to start at, so that no readable entry is read
            // twice or left out
            if (actual.stoppedAt != -1) {
                assertEquals(actual.ranges.size(), maxRanges, context);
                assertTrue(!skipped.get((int) actual.stoppedAt), context);
                for (LongLongPair range : actual.ranges) {
                    assertTrue(actual.stoppedAt > range.rightLong(), context);
                }
                // ... and no readable entry lies between the last range and it, so that the
                // continuation read leaves no readable entry out
                long afterLastRange = actual.ranges.isEmpty()
                        ? firstEntry : actual.ranges.get(actual.ranges.size() - 1).rightLong() + 1;
                for (long entryId = afterLastRange; entryId < actual.stoppedAt; entryId++) {
                    assertTrue(skipped.get((int) entryId),
                            "the continuation read must start at the first readable entry left: " + context);
                }
            }
            assertEquals(actual.ranges.size() <= maxRanges, true, context);
            if (actual.stoppedAt == -1) {
                // the whole window was scanned: its readable entries are exactly the ranges' non-hole ones
                Set<Long> readable = new TreeSet<>();
                for (long entryId = firstEntry; entryId <= lastEntry; entryId++) {
                    if (!skipped.get((int) entryId)) {
                        readable.add(entryId);
                    }
                }
                Set<Long> nonHole = new TreeSet<>();
                for (LongLongPair range : actual.ranges) {
                    for (long entryId = range.leftLong(); entryId <= range.rightLong(); entryId++) {
                        if (!inHoles(actual, entryId)) {
                            nonHole.add(entryId);
                        }
                    }
                }
                assertEquals(nonHole, readable, context);
            }
            // The scan tests no entry twice: as many tests as distinct entries tested
            assertEquals(tests.get(), testedEntries.size(),
                    "an entry must be tested at most once per scan: " + context);
        }
    }

    /**
     * The structural invariants that the batched read relies on, checked without a reference: the
     * ranges are ascending, disjoint and cover exactly the entries that were not skipped up to the
     * point where the scan stopped; the holes are ascending and disjoint and lie inside the ranges;
     * and the entries of the ranges without the holes are exactly the readable ones.
     */
    @Test
    public void testScanInvariants() {
        Random random = new Random(22561);
        for (int iteration = 0; iteration < 50_000; iteration++) {
            int firstEntry = random.nextInt(0, 5);
            int lastEntry = firstEntry + random.nextInt(0, 40);
            BitSet skipped = new BitSet();
            double skipProbability = random.nextDouble() * random.nextDouble();
            for (int entryId = firstEntry; entryId <= lastEntry; entryId++) {
                if (random.nextDouble() < skipProbability) {
                    skipped.set(entryId);
                }
            }
            long maxHoleDistance = random.nextInt(0, 12);
            long maxRangeEntries = random.nextInt(1, 40);
            int maxRanges = random.nextInt(1, MAX_RANGES + 1);

            Scan scan = actualScan(firstEntry, lastEntry, skipped, maxHoleDistance, maxRangeEntries, maxRanges,
                    new AtomicInteger(), new HashSet<>());
            String context = "first=" + firstEntry + " last=" + lastEntry + " skipped=" + skipped + " maxHoleDistance="
                    + maxHoleDistance + " maxRangeEntries=" + maxRangeEntries + " maxRanges=" + maxRanges;
            long scanEnd = scan.stoppedAt == -1 ? lastEntry : scan.stoppedAt - 1;

            // ranges: ascending, disjoint, and at most maxRanges of them
            assertTrue(scan.ranges.size() <= maxRanges, context);
            for (int i = 0; i < scan.ranges.size(); i++) {
                LongLongPair range = scan.ranges.get(i);
                assertTrue(range.leftLong() <= range.rightLong(), context);
                assertTrue(range.leftLong() >= firstEntry && range.rightLong() <= scanEnd, context);
                if (i > 0) {
                    assertTrue(range.leftLong() > scan.ranges.get(i - 1).rightLong() + 1,
                            "ranges must be separated by a non-merged hole: " + context);
                }
                // the range starts and ends on a readable entry
                assertTrue(!skipped.get((int) range.leftLong()) && !skipped.get((int) range.rightLong()), context);
            }

            // holes: ascending, disjoint, each inside one range, made of skipped entries only
            long coveredHoles = 0;
            for (int i = 0; i < scan.holes.size(); i++) {
                LongLongPair hole = scan.holes.get(i);
                assertTrue(hole.leftLong() <= hole.rightLong(), context);
                if (i > 0) {
                    assertTrue(hole.leftLong() > scan.holes.get(i - 1).rightLong(),
                            "holes must be ascending and disjoint: " + context);
                }
                boolean insideOneRange = false;
                for (LongLongPair range : scan.ranges) {
                    if (range.leftLong() <= hole.leftLong() && hole.rightLong() <= range.rightLong()) {
                        insideOneRange = true;
                    }
                }
                assertTrue(insideOneRange, "a hole must lie inside one range: " + context);
                for (long entryId = hole.leftLong(); entryId <= hole.rightLong(); entryId++) {
                    assertTrue(skipped.get((int) entryId), "a hole must be made of skipped entries: " + context);
                }
                coveredHoles += hole.rightLong() - hole.leftLong() + 1;
            }

            // ranges minus holes == exactly the readable entries of the scanned part
            Set<Long> delivered = new TreeSet<>();
            long rangeEntries = 0;
            for (LongLongPair range : scan.ranges) {
                for (long entryId = range.leftLong(); entryId <= range.rightLong(); entryId++) {
                    rangeEntries++;
                    if (!skipped.get((int) entryId)) {
                        delivered.add(entryId);
                    }
                }
            }
            Set<Long> readable = new TreeSet<>();
            for (long entryId = firstEntry; entryId <= scanEnd; entryId++) {
                if (!skipped.get((int) entryId)) {
                    readable.add(entryId);
                }
            }
            assertEquals(delivered, readable, context);
            // the expected count of the batched read: every entry that the ranges read and that is not a hole
            long expectedCount = rangeEntries - coveredHoles;
            assertEquals(expectedCount, readable.size(), context);
        }
    }

    /**
     * The scan decides about each entry once, and what it decides is what counts: a predicate that
     * changes its answer between the scan and the delivery cannot make the batch deliver an entry that
     * the scan counted out, nor reach the expected count with a range still in flight. The reference
     * here is the scan's own view of the skip set.
     */
    @Test
    public void testScanIsStableUnderAConditionThatChangesAfterTheScan() {
        Random random = new Random(22562);
        for (int iteration = 0; iteration < 20_000; iteration++) {
            int firstEntry = random.nextInt(0, 3);
            int lastEntry = firstEntry + random.nextInt(0, 20);
            BitSet skippedAtScan = randomSkipSet(random, firstEntry, lastEntry);
            // the skip set as it looks when the entries arrive: entries flip with a small probability
            BitSet skippedOnArrival = randomFlip(random, skippedAtScan, firstEntry, lastEntry, 0.2);

            long maxHoleDistance = random.nextInt(0, 10);
            long maxRangeEntries = random.nextInt(1, 30);
            AtomicInteger tests = new AtomicInteger();
            Scan scan = actualScan(firstEntry, lastEntry, skippedAtScan, maxHoleDistance, maxRangeEntries,
                    MAX_RANGES, tests, new HashSet<>());
            String context = "first=" + firstEntry + " last=" + lastEntry + " skippedAtScan=" + skippedAtScan
                    + " skippedOnArrival=" + skippedOnArrival + " maxHoleDistance=" + maxHoleDistance;

            long expectedCount = 0;
            for (LongLongPair range : scan.ranges) {
                expectedCount += range.rightLong() - range.leftLong() + 1;
            }
            for (LongLongPair hole : scan.holes) {
                expectedCount -= hole.rightLong() - hole.leftLong() + 1;
            }

            // Every entry of a range was readable at scan time, unless the scan read it through as a hole
            for (LongLongPair range : scan.ranges) {
                for (long entryId = range.leftLong(); entryId <= range.rightLong(); entryId++) {
                    if (skippedAtScan.get((int) entryId)) {
                        assertTrue(inHoles(scan, entryId),
                                "an entry skipped at scan time is in a range only as a hole: " + context);
                    }
                }
            }
            // What a full arrival of every range delivers: the non-hole entries not skipped on arrival
            Set<Long> delivered = new TreeSet<>();
            for (long entryId : nonHoleEntries(scan)) {
                if (!skippedOnArrival.get((int) entryId)) {
                    delivered.add(entryId);
                }
            }
            // The condition can only lose entries after the scan: a non-hole entry that is skipped on
            // arrival is dropped, a hole entry is released whatever the condition says now. So the
            // delivery reaches the expected count only by delivering every non-hole entry of the
            // ranges, i.e. only once every range has reported its entries.
            assertTrue(delivered.size() <= expectedCount, context);
            if (delivered.size() == expectedCount) {
                assertEquals(delivered, nonHoleEntries(scan), context);
            }
        }
    }

    /**
     * The condition can change while the scan runs. The real one only changes in one direction, from
     * readable to skipped: an entry is acknowledged at some moment, and the condition says it is
     * skipped from then on. This gives every entry such a moment - before the scan, during it at a
     * random point of the predicate's call sequence, or never - and checks the scan against the
     * reference scan over the entries that were skipped from the start: the entries acknowledged
     * during the scan are the ones the scan may or may not have seen as skipped, so the scan must
     * match the reference once those are treated as skipped, up to the double test below.
     */
    @Test
    public void testScanWithEntriesAcknowledgedDuringTheScan() {
        Random random = new Random(22565);
        for (int iteration = 0; iteration < 50_000; iteration++) {
            int firstEntry = random.nextInt(0, 3);
            int lastEntry = firstEntry + random.nextInt(0, 24);
            int entryCount = lastEntry - firstEntry + 1;
            BitSet skippedFromStart = randomSkipSet(random, firstEntry, lastEntry);
            // the call number at which each entry gets acknowledged; never for most of them
            int[] acknowledgedAtCall = new int[entryCount];
            for (int i = 0; i < entryCount; i++) {
                acknowledgedAtCall[i] = random.nextInt(10) == 0 ? random.nextInt(entryCount + 2) : Integer.MAX_VALUE;
            }
            long maxHoleDistance = random.nextInt(0, 10);
            long maxRangeEntries = random.nextInt(1, 30);

            AtomicInteger calls = new AtomicInteger();
            List<String> decisions = new ArrayList<>();
            // A delayed message leaves the tracker when its delivery time passes: the condition can
            // also change from skipped to readable, at a random call.
            int[] deliveredAtCall = new int[entryCount];
            for (int i = 0; i < entryCount; i++) {
                deliveredAtCall[i] = random.nextInt(10) == 0 ? random.nextInt(entryCount + 2) : Integer.MAX_VALUE;
            }
            LongPredicate condition = entryId -> {
                int call = calls.getAndIncrement();
                int index = (int) entryId - firstEntry;
                boolean skip = skippedFromStart.get((int) entryId) || call >= acknowledgedAtCall[index];
                if (!skippedFromStart.get((int) entryId) && call >= deliveredAtCall[index]) {
                    skip = false;
                }
                decisions.add(entryId + (skip ? "S" : "R"));
                return skip;
            };
            Scan scan = new Scan();
            scan.stoppedAt = ManagedLedgerImpl.buildRanges(firstEntry, lastEntry, condition, MAX_RANGES,
                    scan.ranges, maxHoleDistance, maxRangeEntries, scan.holes);

            // Every entry is tested at most once, with the condition changing during the scan too.
            // The decisions carry the answer as their last character: strip it, as the same entry
            // answered differently twice is still tested twice.
            Set<Long> testedEntries = new HashSet<>();
            for (String decision : decisions) {
                testedEntries.add(Long.parseLong(decision.substring(0, decision.length() - 1)));
            }
            assertEquals(testedEntries.size(), decisions.size(),
                    "an entry must be tested at most once per scan: " + decisions);

            // The expected count of the batch, and the entries that a full arrival delivers: not a
            // hole, and not skipped by the last decision the scan made about it.
            long expectedCount = 0;
            for (LongLongPair range : scan.ranges) {
                expectedCount += range.rightLong() - range.leftLong() + 1;
            }
            for (LongLongPair hole : scan.holes) {
                expectedCount -= hole.rightLong() - hole.leftLong() + 1;
            }
            Set<Long> delivered = new TreeSet<>();
            for (LongLongPair range : scan.ranges) {
                for (long entryId = range.leftLong(); entryId <= range.rightLong(); entryId++) {
                    if (!inHoles(scan, entryId) && lastDecision(decisions, entryId).equals("R")) {
                        delivered.add(entryId);
                    }
                }
            }
            String context = "first=" + firstEntry + " last=" + lastEntry + " decisions=" + decisions
                    + " ranges=" + scan.ranges + " holes=" + scan.holes + " expectedCount=" + expectedCount
                    + " delivered=" + delivered;
            // Holes must be ascending and disjoint: the binary search and the expected count rely on it
            for (int i = 1; i < scan.holes.size(); i++) {
                assertTrue(scan.holes.get(i).leftLong() > scan.holes.get(i - 1).rightLong(), context);
            }
            // The range entries that the scan last saw as readable are exactly the non-hole ones
            for (LongLongPair range : scan.ranges) {
                for (long entryId = range.leftLong(); entryId <= range.rightLong(); entryId++) {
                    assertEquals(inHoles(scan, entryId), lastDecision(decisions, entryId).equals("S"), context);
                }
            }
            // So a full arrival delivers exactly the expected count
            assertEquals(delivered.size(), expectedCount, context);
        }
    }

    /** The decision the predicate gave the last time it was asked about the entry. */
    private static String lastDecision(List<String> decisions, long entryId) {
        String last = null;
        for (String decision : decisions) {
            if (decision.equals(entryId + "S") || decision.equals(entryId + "R")) {
                last = decision;
            }
        }
        return last.substring(last.length() - 1);
    }

    private static BitSet randomSkipSet(Random random, int firstEntry, int lastEntry) {
        BitSet skipped = new BitSet();
        double skipProbability = random.nextDouble() * random.nextDouble();
        for (int entryId = firstEntry; entryId <= lastEntry; entryId++) {
            if (random.nextDouble() < skipProbability) {
                skipped.set(entryId);
            }
        }
        return skipped;
    }

    private static BitSet randomFlip(Random random, BitSet skipped, int firstEntry, int lastEntry,
                                     double flipProbability) {
        BitSet flipped = (BitSet) skipped.clone();
        for (int entryId = firstEntry; entryId <= lastEntry; entryId++) {
            if (random.nextDouble() < flipProbability) {
                flipped.flip(entryId);
            }
        }
        return flipped;
    }

    private static boolean inHoles(Scan scan, long entryId) {
        return scan.holes.stream()
                .anyMatch(hole -> hole.leftLong() <= entryId && entryId <= hole.rightLong());
    }

    private static Set<Long> nonHoleEntries(Scan scan) {
        Set<Long> entries = new TreeSet<>();
        for (LongLongPair range : scan.ranges) {
            for (long entryId = range.leftLong(); entryId <= range.rightLong(); entryId++) {
                if (!inHoles(scan, entryId)) {
                    entries.add(entryId);
                }
            }
        }
        return entries;
    }
}
