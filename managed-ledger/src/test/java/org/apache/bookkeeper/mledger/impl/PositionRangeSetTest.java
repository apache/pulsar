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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.pulsar.common.util.collections.LongPairRangeSet.LongPairConsumer;
import org.apache.pulsar.common.util.collections.OpenLongPairRangeSet;
import org.roaringbitmap.RoaringBitSet;
import org.testng.annotations.Test;

/**
 * Tests for {@link PositionRangeSet}, the Position-specific bitmap-backed range set that replaces the
 * previous generic {@code RangeSetWrapper<Position>} + {@code OpenLongPairRangeSet} stack.
 *
 * <p>Originally {@code RangeSetWrapperTest} (parameterized over {@code unackedRangesOpenCacheSetEnabled}).
 * The {@code DefaultRangeSet} code path that the {@code false} axis exercised no longer exists, so the
 * {@code testAddForDifferentKey2} test and the {@code false} half of {@code testAddForSameKey} /
 * {@code testDeleteWithAtMost2} were removed; the remaining assertions reproduce the original
 * bitmap-mode ({@code openCacheSet=true}) behavior verbatim.
 */
public class PositionRangeSetTest {

    static final LongPairConsumer<Position> CONSUMER = PositionFactory::create;

    private static Position pos(long ledgerId, long entryId) {
        return PositionFactory.create(ledgerId, entryId);
    }

    // Standard fixture: multi-entry (dirty tracking) enabled — matches ManagedCursorImpl's production wiring.
    private static PositionRangeSet newSet() {
        return new PositionRangeSet(CONSUMER, true);
    }

    @Test
    public void testDirtyLedger() {
        PositionRangeSet rangeSet = newSet();
        rangeSet.addOpenClosed(10, 0, 20, 0);
        assertEquals(rangeSet.size(), 1);
        // addOpenClosed(10,0,20,0) marks ledgers in (10, 20] dirty per the original
        // dirtyLedgers.addOpenClosed(k,0,k',0) LongPair-ordering semantics — ledger 10 is open-lower
        // and therefore not dirty.
        assertFalse(rangeSet.isDirtyLedgers(10L));
        for (long i = 11; i <= 20; i++) {
            assertTrue(rangeSet.isDirtyLedgers(i));
        }

        rangeSet.removeAtMost(11, 0);
        assertEquals(rangeSet.size(), 1);
        assertFalse(rangeSet.isDirtyLedgers(11L));
        for (long i = 12; i <= 20; i++) {
            assertTrue(rangeSet.isDirtyLedgers(i));
        }
    }

    @Test
    public void testDirtyLedgerDisabledWhenMultiEntryOff() {
        PositionRangeSet rangeSet = new PositionRangeSet(CONSUMER, false);
        rangeSet.addOpenClosed(10, 0, 20, 0);
        for (long i = 0; i <= 20; i++) {
            assertFalse(rangeSet.isDirtyLedgers(i));
        }
    }

    @Test
    public void testAddForSameKey() {
        PositionRangeSet set = newSet();
        set.addOpenClosed(0, 0, 0, 5);
        set.addOpenClosed(0, 8, 0, 8);
        set.addOpenClosed(0, 9, 0, 9);
        set.addOpenClosed(0, 10, 0, 10);
        set.addOpenClosed(0, 98, 0, 99);
        set.addOpenClosed(0, 102, 0, 106);

        List<Range<Position>> ranges = new ArrayList<>(set.asRanges());
        int count = 0;
        assertEquals(ranges.get(count++), Range.openClosed(pos(0, 0), pos(0, 5)));
        assertEquals(ranges.get(count++), Range.openClosed(pos(0, 98), pos(0, 99)));
        assertEquals(ranges.get(count), Range.openClosed(pos(0, 102), pos(0, 106)));
    }

    @Test
    public void testAddForDifferentKey() {
        PositionRangeSet set = newSet();
        set.addOpenClosed(0, 98, 0, 99);
        set.addOpenClosed(0, 100, 1, 5);
        set.addOpenClosed(1, 10, 1, 15);
        set.addOpenClosed(1, 20, 2, 10);

        // bitmap-mode normalization: cross-ledger addOpenClosed into a ledger that did not previously
        // exist reports the lower endpoint as (upper, -1) because the bitmap starts at index 0
        // (= entry 0) with the open lower bound at -1.
        List<Range<Position>> ranges = new ArrayList<>(set.asRanges());
        int count = 0;
        assertEquals(ranges.get(count++), Range.openClosed(pos(0, 98), pos(0, 99)));
        assertEquals(ranges.get(count++), Range.openClosed(pos(1, -1), pos(1, 5)));
        assertEquals(ranges.get(count++), Range.openClosed(pos(1, 10), pos(1, 15)));
        assertEquals(ranges.get(count), Range.openClosed(pos(2, -1), pos(2, 10)));
    }

    @Test
    public void testAddCompareCompareWithGuava() {
        PositionRangeSet set = newSet();
        RangeSet<Position> gSet = TreeRangeSet.create();

        int totalInsert = 10_000;
        for (int i = 0; i < totalInsert; i++) {
            if (i % 3 == 0 || i % 6 == 0 || i % 8 == 0) {
                Position lower = pos(0, i - 1);
                Position upper = pos(0, i);
                set.addOpenClosed(lower.getLedgerId(), lower.getEntryId(), upper.getLedgerId(), upper.getEntryId());
                gSet.add(Range.openClosed(lower, upper));
            }
        }
        for (int i = totalInsert; i < (totalInsert * 2); i++) {
            if (i % 5 == 0) {
                Position lower = pos(0, i - 3 - 1);
                Position upper = pos(0, i + 3);
                set.addOpenClosed(lower.getLedgerId(), lower.getEntryId(), upper.getLedgerId(), upper.getEntryId());
                gSet.add(Range.openClosed(lower, upper));
            }
        }
        List<Range<Position>> ranges = new ArrayList<>(set.asRanges());
        Set<Range<Position>> gRanges = gSet.asRanges();

        List<Range<Position>> gRangeConnected = getConnectedRange(gRanges);
        assertEquals(gRangeConnected.size(), ranges.size());
        int i = 0;
        for (Range<Position> range : gRangeConnected) {
            assertEquals(range, ranges.get(i));
            i++;
        }
    }

    @Test
    public void testDeleteCompareWithGuava() throws Exception {
        PositionRangeSet set = newSet();
        RangeSet<Position> gSet = TreeRangeSet.create();

        int totalInsert = 10_000;
        List<Range<Position>> removedRanges = new ArrayList<>();
        for (int i = 0; i < totalInsert; i++) {
            if (i % 3 == 0 || i % 7 == 0 || i % 11 == 0) {
                continue;
            }
            Position lower = pos(0, i - 1);
            Position upper = pos(0, i);
            Range<Position> range = Range.openClosed(lower, upper);
            set.addOpenClosed(lower.getLedgerId(), lower.getEntryId(), upper.getLedgerId(), upper.getEntryId());
            gSet.add(range);
            if (i % 4 == 0) {
                removedRanges.add(range);
            }
        }
        for (int i = totalInsert; i < (totalInsert * 2); i++) {
            Position lower = pos(0, i - 3 - 1);
            Position upper = pos(0, i + 3);
            Range<Position> range = Range.openClosed(lower, upper);
            if (i % 5 != 0) {
                set.addOpenClosed(lower.getLedgerId(), lower.getEntryId(), upper.getLedgerId(), upper.getEntryId());
                gSet.add(range);
            }
            if (i % 4 == 0) {
                removedRanges.add(range);
            }
        }
        // remove records
        for (Range<Position> range : removedRanges) {
            set.remove(range);
            gSet.remove(range);
        }

        List<Range<Position>> ranges = new ArrayList<>(set.asRanges());
        Set<Range<Position>> gRanges = gSet.asRanges();
        List<Range<Position>> gRangeConnected = getConnectedRange(gRanges);
        assertEquals(gRangeConnected.size(), ranges.size());
        int i = 0;
        for (Range<Position> range : gRangeConnected) {
            assertEquals(range, ranges.get(i));
            i++;
        }
    }

    @Test
    public void testSpanWithGuava() {
        PositionRangeSet set = newSet();
        RangeSet<Position> gSet = TreeRangeSet.create();
        set.addOpenClosed(0, 97, 0, 99);
        gSet.add(Range.openClosed(pos(0, 97), pos(0, 99)));
        set.addOpenClosed(0, 99, 1, 5);
        gSet.add(Range.openClosed(pos(0, 99), pos(1, 5)));
        assertEquals(set.span(), gSet.span());
        assertEquals(set.span(), Range.openClosed(pos(0, 97), pos(1, 5)));

        set.addOpenClosed(1, 9, 1, 15);
        set.addOpenClosed(1, 19, 2, 10);
        set.addOpenClosed(2, 24, 2, 28);
        set.addOpenClosed(3, 11, 3, 20);
        set.addOpenClosed(4, 11, 4, 20);
        gSet.add(Range.openClosed(pos(1, 9), pos(1, 15)));
        gSet.add(Range.openClosed(pos(1, 19), pos(2, 10)));
        gSet.add(Range.openClosed(pos(2, 24), pos(2, 28)));
        gSet.add(Range.openClosed(pos(3, 11), pos(3, 20)));
        gSet.add(Range.openClosed(pos(4, 11), pos(4, 20)));
        assertEquals(set.span(), gSet.span());
        assertEquals(set.span(), Range.openClosed(pos(0, 97), pos(4, 20)));
    }

    @Test
    public void testFirstRange() {
        PositionRangeSet set = newSet();
        assertNull(set.firstRange());
        set.addOpenClosed(0, 97, 0, 99);
        assertEquals(set.firstRange(), Range.openClosed(pos(0, 97), pos(0, 99)));
        assertEquals(set.size(), 1);
        set.addOpenClosed(0, 98, 0, 105);
        assertEquals(set.firstRange(), Range.openClosed(pos(0, 97), pos(0, 105)));
        assertEquals(set.size(), 1);
        set.addOpenClosed(0, 5, 0, 75);
        assertEquals(set.firstRange(), Range.openClosed(pos(0, 5), pos(0, 75)));
        assertEquals(set.size(), 2);
    }

    @Test
    public void testLastRange() {
        PositionRangeSet set = newSet();
        assertNull(set.lastRange());
        Range<Position> range = Range.openClosed(pos(0, 97), pos(0, 99));
        set.addOpenClosed(0, 97, 0, 99);
        assertEquals(set.lastRange(), range);
        assertEquals(set.size(), 1);
        set.addOpenClosed(0, 98, 0, 105);
        assertEquals(set.lastRange(), Range.openClosed(pos(0, 97), pos(0, 105)));
        assertEquals(set.size(), 1);
        range = Range.openClosed(pos(1, 5), pos(1, 75));
        set.addOpenClosed(1, 5, 1, 75);
        assertEquals(set.lastRange(), range);
        assertEquals(set.size(), 2);
        range = Range.openClosed(pos(1, 80), pos(1, 120));
        set.addOpenClosed(1, 80, 1, 120);
        assertEquals(set.lastRange(), range);
        assertEquals(set.size(), 3);
    }

    @Test
    public void testToString() {
        PositionRangeSet set = newSet();
        set.addOpenClosed(0, 97, 0, 99);
        assertEquals(set.toString(), "[(0:97..0:99]]");
        set.addOpenClosed(0, 98, 0, 105);
        assertEquals(set.toString(), "[(0:97..0:105]]");
        set.addOpenClosed(0, 5, 0, 75);
        assertEquals(set.toString(), "[(0:5..0:75],(0:97..0:105]]");
    }

    @Test
    public void testDeleteForDifferentKey() {
        PositionRangeSet set = newSet();
        set.addOpenClosed(0, 97, 0, 99);
        set.addOpenClosed(0, 99, 1, 5);
        set.addOpenClosed(1, 9, 1, 15);
        set.addOpenClosed(1, 19, 2, 10);
        set.addOpenClosed(2, 24, 2, 28);
        set.addOpenClosed(3, 11, 3, 20);
        set.addOpenClosed(4, 11, 4, 20);

        // delete only (0,100)
        set.remove(Range.open(pos(0, 99), pos(0, 105)));

        /**
         * delete all keys from [2,27]->[4,15] : remaining [2,25..26,28], [4,16..20]
         */
        set.remove(Range.closed(pos(2, 27), pos(4, 15)));

        List<Range<Position>> ranges = new ArrayList<>(set.asRanges());
        int count = 0;
        assertEquals(ranges.get(count++), Range.openClosed(pos(0, 97), pos(0, 99)));
        assertEquals(ranges.get(count++), Range.openClosed(pos(1, -1), pos(1, 5)));
        assertEquals(ranges.get(count++), Range.openClosed(pos(1, 9), pos(1, 15)));
        assertEquals(ranges.get(count++), Range.openClosed(pos(2, -1), pos(2, 10)));

        assertEquals(ranges.get(count++), Range.openClosed(pos(2, 24), pos(2, 26)));
        assertEquals(ranges.get(count++), Range.openClosed(pos(2, 27), pos(2, 28)));
        assertEquals(ranges.get(count++), Range.openClosed(pos(4, 15), pos(4, 20)));
    }

    @Test
    public void testDeleteWithAtMost() {
        PositionRangeSet set = newSet();
        set.addOpenClosed(0, 98, 0, 99);
        set.addOpenClosed(0, 100, 1, 5);
        set.addOpenClosed(1, 10, 1, 15);
        set.addOpenClosed(1, 20, 2, 10);
        set.addOpenClosed(2, 25, 2, 28);
        set.addOpenClosed(3, 12, 3, 20);
        set.addOpenClosed(4, 12, 4, 20);

        // delete only (0,100)
        set.remove(Range.open(pos(0, 99), pos(0, 105)));

        /**
         * delete all keys from [2,27]->[4,15] : remaining [2,25..26,28], [4,16..20]
         */
        set.remove(Range.atMost(pos(2, 27)));

        List<Range<Position>> ranges = new ArrayList<>(set.asRanges());
        int count = 0;
        assertEquals(ranges.get(count++), Range.openClosed(pos(2, 27), pos(2, 28)));
        assertEquals(ranges.get(count++), Range.openClosed(pos(3, 12), pos(3, 20)));
        assertEquals(ranges.get(count++), Range.openClosed(pos(4, 12), pos(4, 20)));
    }

    @Test
    public void testDeleteWithAtMost2() {
        // Originally this test ran twice — once with openCacheSet=true and once with =false.
        // The =false (DefaultRangeSet) variant is dropped because PositionRangeSet has a single
        // bitmap-backed implementation. The remaining assertions reproduce the bitmap-mode behavior.
        PositionRangeSet set = newSet();
        set.addOpenClosed(0, 98, 0, 99);
        set.addOpenClosed(0, 100, 1, 5);
        set.addOpenClosed(1, 10, 1, 15);
        set.addOpenClosed(1, 20, 2, 10);
        set.addOpenClosed(2, 25, 2, 28);
        set.addOpenClosed(3, 12, 3, 20);
        set.addOpenClosed(4, 12, 4, 20);

        // delete entire ledger 0 (closed-closed within the same ledger id)
        set.remove(Range.closed(pos(0, 0), pos(0, Integer.MAX_VALUE - 1)));

        List<Range<Position>> ranges = new ArrayList<>(set.asRanges());
        int count = 0;
        assertEquals(ranges.get(count++), Range.openClosed(pos(1, -1), pos(1, 5)));
        assertEquals(ranges.get(count++), Range.openClosed(pos(1, 10), pos(1, 15)));
        assertEquals(ranges.get(count++), Range.openClosed(pos(2, -1), pos(2, 10)));
        assertEquals(ranges.get(count), Range.openClosed(pos(2, 25), pos(2, 28)));
    }

    @Test
    public void testDeleteWithLeastMost() {
        PositionRangeSet set = newSet();
        set.addOpenClosed(0, 98, 0, 99);
        set.addOpenClosed(0, 100, 1, 5);
        set.addOpenClosed(1, 10, 1, 15);
        set.addOpenClosed(1, 20, 2, 10);
        set.addOpenClosed(2, 25, 2, 28);
        set.addOpenClosed(2, 12, 3, 20);
        set.addOpenClosed(4, 12, 4, 20);

        // delete only (0,100)
        set.remove(Range.open(pos(0, 99), pos(0, 105)));

        /**
         * delete all keys from [2,27]->[4,15] : remaining [2,25..26,28], [4,16..20]
         */
        set.remove(Range.atLeast(pos(2, 27)));

        List<Range<Position>> ranges = new ArrayList<>(set.asRanges());
        int count = 0;
        assertEquals(ranges.get(count++), Range.openClosed(pos(0, 98), pos(0, 99)));
        assertEquals(ranges.get(count++), Range.openClosed(pos(1, -1), pos(1, 5)));
        assertEquals(ranges.get(count++), Range.openClosed(pos(1, 10), pos(1, 15)));
        assertEquals(ranges.get(count++), Range.openClosed(pos(2, -1), pos(2, 10)));
        assertEquals(ranges.get(count), Range.openClosed(pos(2, 12), pos(2, 26)));
    }

    @Test
    public void testRangeContaining() {
        PositionRangeSet set = newSet();
        set.add(Range.closed(pos(0, 98), pos(0, 99)));
        set.add(Range.closed(pos(0, 100), pos(1, 5)));
        RangeSet<Position> gSet = TreeRangeSet.create();
        gSet.add(Range.closed(pos(0, 98), pos(0, 100)));
        gSet.add(Range.closed(pos(0, 101), pos(1, 5)));
        set.add(Range.closed(pos(1, 10), pos(1, 15)));
        set.add(Range.closed(pos(1, 20), pos(2, 10)));
        set.add(Range.closed(pos(2, 25), pos(2, 28)));
        set.add(Range.closed(pos(3, 12), pos(3, 20)));
        set.add(Range.closed(pos(4, 12), pos(4, 20)));
        gSet.add(Range.closed(pos(1, 10), pos(1, 15)));
        gSet.add(Range.closed(pos(1, 20), pos(2, 10)));
        gSet.add(Range.closed(pos(2, 25), pos(2, 28)));
        gSet.add(Range.closed(pos(3, 12), pos(3, 20)));
        gSet.add(Range.closed(pos(4, 12), pos(4, 20)));

        Position position = pos(0, 99);
        assertEquals(set.rangeContaining(position.getLedgerId(), position.getEntryId()),
                Range.closed(pos(0, 98), pos(0, 100)));
        assertEquals(set.rangeContaining(position.getLedgerId(), position.getEntryId()),
                gSet.rangeContaining(position));

        position = pos(2, 30);
        assertNull(set.rangeContaining(position.getLedgerId(), position.getEntryId()));
        assertEquals(set.rangeContaining(position.getLedgerId(), position.getEntryId()),
                gSet.rangeContaining(position));

        position = pos(3, 13);
        assertEquals(set.rangeContaining(position.getLedgerId(), position.getEntryId()),
                Range.closed(pos(3, 12), pos(3, 20)));
        assertEquals(set.rangeContaining(position.getLedgerId(), position.getEntryId()),
                gSet.rangeContaining(position));

        position = pos(3, 22);
        assertNull(set.rangeContaining(position.getLedgerId(), position.getEntryId()));
        assertEquals(set.rangeContaining(position.getLedgerId(), position.getEntryId()),
                gSet.rangeContaining(position));
    }

    @Test
    public void testWireFormatRoundTrip() {
        // Verify the dense-long[] contract used by ManagedCursorImpl persistence: serialize via
        // toRanges, deserialize via build, and confirm the resulting range set is identical.
        PositionRangeSet original = newSet();
        original.addOpenClosed(0, 0, 0, 5);
        original.addOpenClosed(0, 10, 0, 100);
        original.addOpenClosed(1, 0, 1, 50);
        original.addOpenClosed(3, 200, 3, 300);

        Map<Long, long[]> serialized = original.toRanges(Integer.MAX_VALUE);
        PositionRangeSet restored = newSet();
        restored.build(serialized);

        assertEquals(restored.asRanges(), original.asRanges());
        assertEquals(restored.cardinality(0, 0, 3, 300), original.cardinality(0, 0, 3, 300));
        for (Range<Position> r : original.asRanges()) {
            assertTrue(restored.contains(r.lowerEndpoint().getLedgerId(), r.lowerEndpoint().getEntryId() + 1));
        }
    }

    @Test
    public void testCompatibilityWithLegacyOpenLongPairRangeSet() {
        // Simulate 4.x cursor persistence: serialize individual deleted messages using the legacy
        // OpenLongPairRangeSet (still present in pulsar-common) backed by RoaringBitSet, then
        // deserialize via PositionRangeSet. This proves on-disk byte compatibility — a cursor
        // persisted by 4.x can be recovered by 5.0 without conversion.
        OpenLongPairRangeSet<Position> legacy = new OpenLongPairRangeSet<>(
                PositionFactory::create, RoaringBitSet::new);
        legacy.addOpenClosed(0, -1, 0, 9);
        legacy.addOpenClosed(0, 50, 0, 99);
        legacy.addOpenClosed(1, 10, 1, 20);
        legacy.addOpenClosed(3, 0, 4, 5);

        Map<Long, long[]> legacySerialized = legacy.toRanges(Integer.MAX_VALUE);
        PositionRangeSet restored = newSet();
        restored.build(legacySerialized);

        assertEquals(restored.asRanges(), legacy.asRanges());
        assertEquals(restored.size(), legacy.size());
        assertEquals(restored.cardinality(0, 0, 4, 5), legacy.cardinality(0, 0, 4, 5));
    }

    @Test
    public void testCardinality() {
        PositionRangeSet set = newSet();
        // half-open lower: addOpenClosed(k, -1, k, N) sets entries 0..N inclusive.
        set.addOpenClosed(0, -1, 0, 9);   // 10 entries: 0..9
        set.addOpenClosed(1, -1, 1, 19);  // 20 entries: 0..19
        set.addOpenClosed(2, -1, 2, 4);   // 5 entries: 0..4

        // full span — cardinality bounds are inclusive on both ends
        assertEquals(set.cardinality(0, 0, 2, 4), 35);
        // partial ledger 0
        assertEquals(set.cardinality(0, 3, 0, 7), 5);
        // partial across ledgers 0 and 1
        assertEquals(set.cardinality(0, 5, 1, 5), 5 + 6);
        // single ledger interior
        assertEquals(set.cardinality(1, 5, 1, 14), 10);
    }

    @Test
    public void testClear() {
        PositionRangeSet set = newSet();
        // cross-ledger adds mark dirty (single-ledger adds do not, matching the original
        // RangeSetWrapper/DefaultRangeSet semantics where addOpenClosed(k,0,k,0) is an empty range).
        set.addOpenClosed(0, 5, 1, 5);
        set.addOpenClosed(2, 5, 3, 5);
        assertFalse(set.isEmpty());
        assertEquals(set.size(), 2);
        assertTrue(set.isDirtyLedgers(1));
        assertTrue(set.isDirtyLedgers(3));
        set.clear();
        assertTrue(set.isEmpty());
        assertEquals(set.size(), 0);
        // clear also resets dirty tracker
        assertFalse(set.isDirtyLedgers(1));
        assertFalse(set.isDirtyLedgers(3));
    }

    @Test
    public void testRemoveAtMostClearsEmptyLedgers() {
        // Cross-ledger removeAtMost must drop now-empty ledgers from the underlying TreeMap so
        // the structure does not accumulate stale empty bitmaps over the cursor's lifetime.
        PositionRangeSet set = newSet();
        set.addOpenClosed(0, -1, 0, 9);   // ledger 0: entries 0..9
        set.addOpenClosed(1, -1, 1, 9);   // ledger 1: entries 0..9
        set.addOpenClosed(2, -1, 2, 9);   // ledger 2: entries 0..9

        // removeAtMost(2, -1) deletes ledgers 0 and 1 wholesale (positions <= (2, -1)) and leaves
        // ledger 2 untouched (no entry <= -1 in ledger 2).
        set.removeAtMost(2, -1);

        // Only ledger 2 should remain in the underlying map.
        List<Range<Position>> ranges = new ArrayList<>(set.asRanges());
        assertEquals(ranges.size(), 1);
        assertEquals(ranges.get(0), Range.openClosed(pos(2, -1), pos(2, 9)));
        // The two cleared ledgers must not linger as empty bitmaps — verify via forEachRawRange,
        // which would otherwise skip them silently and hide the leak.
        MutableInt ledgerCount = new MutableInt(0);
        set.forEachRawRange((lowerKey, lowerValue, upperKey, upperValue) -> {
            ledgerCount.increment();
            return true;
        });
        assertEquals(ledgerCount.intValue(), 1);
    }


    private List<Range<Position>> getConnectedRange(Set<Range<Position>> gRanges) {
        List<Range<Position>> gRangeConnected = new ArrayList<>();
        Range<Position> lastRange = null;
        for (Range<Position> range : gRanges) {
            if (lastRange == null) {
                lastRange = range;
                continue;
            }
            Position previousUpper = lastRange.upperEndpoint();
            Position currentLower = range.lowerEndpoint();
            int previousUpperValue = (int) (lastRange.upperBoundType().equals(BoundType.CLOSED)
                    ? previousUpper.getEntryId()
                    : previousUpper.getEntryId() - 1);
            int currentLowerValue = (int) (range.lowerBoundType().equals(BoundType.CLOSED)
                    ? currentLower.getEntryId()
                    : currentLower.getEntryId() + 1);
            boolean connected = previousUpper.getLedgerId() == currentLower.getLedgerId()
                    && (previousUpperValue >= currentLowerValue);
            if (connected) {
                lastRange = Range.closed(lastRange.lowerEndpoint(), range.upperEndpoint());
            } else {
                gRangeConnected.add(lastRange);
                lastRange = range;
            }
        }
        if (lastRange != null) {
            int lowerOpenValue = (int) (lastRange.lowerBoundType().equals(BoundType.CLOSED)
                    ? (lastRange.lowerEndpoint().getEntryId() - 1)
                    : lastRange.lowerEndpoint().getEntryId());
            lastRange = Range.openClosed(pos(lastRange.lowerEndpoint().getLedgerId(), lowerOpenValue),
                    lastRange.upperEndpoint());
            gRangeConnected.add(lastRange);
        }
        return gRangeConnected;
    }
}
