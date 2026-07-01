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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import io.github.merlimat.slog.Logger;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.pulsar.common.util.collections.LongBitmap;
import org.apache.pulsar.common.util.collections.LongBitmaps;
import org.apache.pulsar.common.util.collections.LongPairRangeSet;
import org.roaringbitmap.RoaringBitSet;

/**
 * Tracks deleted-message positions as ranges of {@link Position}s.
 *
 * <p>The implementation stores positions in a two-level structure:
 * the ledger id is used as the map key, and the corresponding entry ids are stored in a
 * {@link RoaringBitSet}. Bit {@code n} in the bitmap of ledger {@code L} represents the position
 * {@code (L, n)}.
 *
 * <p>This implementation replaces the previous {@code RangeSetWrapper<Position>} backed by
 * {@code OpenLongPairRangeSet}. Keeping the storage layout ledger-oriented avoids the overhead of
 * creating and comparing {@link Position} range objects while preserving the same
 * {@link LongPairRangeSet} semantics.
 *
 * <h2>Thread Safety</h2>
 *
 * <p>This class is not thread-safe. All methods require the caller to provide external
 * synchronization. In normal usage, callers must hold the owning {@link ManagedCursorImpl}'s
 * cursor lock before accessing this class.
 *
 * <p>The class intentionally does not maintain internal locking. The cursor lock is the single
 * synchronization boundary for both this structure and related cursor state.
 *
 * <h2>Persistence Compatibility</h2>
 *
 * <p>The persisted representation remains compatible with the existing format:
 * {@link #toRanges(int)} and {@link #build(Map)} use the {@link BitSet#toLongArray()} /
 * {@link BitSet#valueOf(long[])} contract.
 *
 * <p>The internal {@link RoaringBitSet} representation is an implementation detail and is not used
 * for serialization, avoiding any wire-format change.
 *
 * <p>Entry ids are stored as bitmap indexes and therefore use {@code int} values. This is safe
 * because {@code managedLedgerMaxEntriesPerLedger} is an {@code int}, so valid entry ids are within
 * {@code [0, Integer.MAX_VALUE]}.
 */
class PositionRangeSet implements LongPairRangeSet<Position> {

    private static final Logger log = Logger.get(PositionRangeSet.class);

    private static final long EARLIEST_KEY = -1L;
    private static final long EARLIEST_VALUE = -1L;
    private static final long LATEST_KEY = Long.MAX_VALUE;
    private static final long LATEST_VALUE = Long.MAX_VALUE;

    /**
     * Maps ledger ID (key) to a bitmap of deleted entry IDs (value) within that ledger.
     * Bit {@code n} in the bitmap represents entry {@code n} in the ledger.
     * Uses fastutil's primitive long map to avoid Long boxing overhead.
     */
    private final Long2ObjectSortedMap<RoaringBitSet> rangeBitSetMap = new Long2ObjectRBTreeMap<>();
    private final LongPairConsumer<Position> consumer;
    private final boolean enableMultiEntry;

    private LongBitmap dirtyLedgers = LongBitmaps.create();

    private int cachedSize = 0;
    private String cachedToString = "[]";
    private boolean updatedAfterCachedForSize = true;
    private boolean updatedAfterCachedForToString = true;

    PositionRangeSet(LongPairConsumer<Position> consumer, boolean enableMultiEntry) {
        this.consumer = consumer;
        this.enableMultiEntry = enableMultiEntry;
    }

    private static int lastSetBit(RoaringBitSet bitSet) {
        return bitSet.isEmpty() ? -1 : bitSet.previousSetBit(bitSet.length() - 1);
    }

    @Override
    public void addOpenClosed(long lowerKey, long lowerValueOpen, long upperKey, long upperValue) {
        if (enableMultiEntry) {
            markDirty(lowerKey, upperKey);
        }
        long lowerValue = lowerValueOpen + 1;
        if (lowerKey != upperKey) {
            // Extend lower-key's bitmap only if it already exists and has bits at/after lowerValue;
            // otherwise we'd invent acknowledgements that never happened (e.g. (2:10..4:10] must not
            // touch 2:10 if ledger 2 was empty).
            if (isValid(lowerKey, lowerValue)) {
                RoaringBitSet rangeBitSet = rangeBitSetMap.get(lowerKey);
                if (rangeBitSet != null && (lastSetBit(rangeBitSet) > lowerValueOpen)) {
                    int lastValue = lastSetBit(rangeBitSet);
                    rangeBitSet.set((int) lowerValue, (int) Math.max(lastValue, lowerValue) + 1);
                }
            }
            if (isValid(upperKey, upperValue)) {
                RoaringBitSet rangeBitSet = rangeBitSetMap.computeIfAbsent(upperKey, k -> new RoaringBitSet());
                rangeBitSet.set(0, (int) upperValue + 1);
            }
        } else {
            RoaringBitSet rangeBitSet = rangeBitSetMap.computeIfAbsent(lowerKey, k -> new RoaringBitSet());
            rangeBitSet.set((int) lowerValue, (int) upperValue + 1);
        }
        invalidateCaches();
    }

    @Override
    public boolean contains(long key, long value) {
        RoaringBitSet rangeBitSet = rangeBitSetMap.get(key);
        if (rangeBitSet != null) {
            return rangeBitSet.get(getSafeEntry(value));
        }
        return false;
    }

    @Override
    public Range<Position> rangeContaining(long key, long value) {
        RoaringBitSet rangeBitSet = rangeBitSetMap.get(key);
        if (rangeBitSet == null || !rangeBitSet.get(getSafeEntry(value))) {
            return null;
        }
        int entry = getSafeEntry(value);
        int lowerValue = rangeBitSet.previousClearBit(entry) + 1;
        Position lower = consumer.apply(key, lowerValue);
        Position upper = consumer.apply(key, Math.max(rangeBitSet.nextClearBit(entry) - 1, lowerValue));
        return Range.closed(lower, upper);
    }

    @Override
    public void removeAtMost(long key, long value) {
        if (enableMultiEntry && key >= 0) {
            long end = Math.min(key + 1L, (long) Integer.MAX_VALUE + 1);
            dirtyLedgers.remove(0, end);
        }
        remove(Range.atMost(PositionFactory.create(key, value)));
    }

    @Override
    public boolean isEmpty() {
        if (rangeBitSetMap.isEmpty()) {
            return true;
        }
        for (RoaringBitSet bitSet : rangeBitSetMap.values()) {
            if (!bitSet.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void clear() {
        rangeBitSetMap.clear();
        resetDirtyKeys();
        invalidateCaches();
    }

    @Override
    public Range<Position> span() {
        if (rangeBitSetMap.isEmpty()) {
            return null;
        }
        long firstKey = rangeBitSetMap.firstLongKey();
        long lastKey = rangeBitSetMap.lastLongKey();
        RoaringBitSet firstSet = rangeBitSetMap.get(firstKey);
        RoaringBitSet lastSet = rangeBitSetMap.get(lastKey);
        int first = firstSet.nextSetBit(0);
        int last = lastSetBit(lastSet);
        return Range.openClosed(consumer.apply(firstKey, first - 1),
                consumer.apply(lastKey, last));
    }

    @Override
    public List<Range<Position>> asRanges() {
        List<Range<Position>> ranges = new ArrayList<>();
        forEach(range -> {
            ranges.add(range);
            return true;
        });
        return ranges;
    }

    @Override
    public void forEach(RangeProcessor<Position> action) {
        forEach(action, consumer);
    }

    @Override
    public void forEach(RangeProcessor<Position> action, LongPairConsumer<? extends Position> consumerParam) {
        forEachRawRange((lowerKey, lowerValue, upperKey, upperValue) -> {
            Range<Position> range = Range.openClosed(
                    consumerParam.apply(lowerKey, lowerValue),
                    consumerParam.apply(upperKey, upperValue));
            return action.process(range);
        });
    }

    @Override
    public void forEachRawRange(RawRangeProcessor processor) {
        AtomicBoolean completed = new AtomicBoolean(false);
        rangeBitSetMap.forEach((key, set) -> {
            if (completed.get() || set.isEmpty()) {
                return;
            }
            int first = set.nextSetBit(0);
            int last = lastSetBit(set);
            int currentClosedMark = first;
            while (currentClosedMark != -1 && currentClosedMark <= last) {
                long nextOpenMarkLong = set.nextClearBit(currentClosedMark);
                if (nextOpenMarkLong > Integer.MAX_VALUE) {
                    if (!processor.processRawRange(key, currentClosedMark - 1, key, last)) {
                        completed.set(true);
                    }
                    break;
                }
                int nextOpenMark = (int) nextOpenMarkLong;
                if (!processor.processRawRange(key, currentClosedMark - 1, key, nextOpenMark - 1)) {
                    completed.set(true);
                    break;
                }
                currentClosedMark = set.nextSetBit(nextOpenMark);
            }
        });
    }

    @Override
    public Range<Position> firstRange() {
        if (rangeBitSetMap.isEmpty()) {
            return null;
        }
        long firstKey = rangeBitSetMap.firstLongKey();
        RoaringBitSet firstSet = rangeBitSetMap.get(firstKey);
        int lower = firstSet.nextSetBit(0);
        int upper = Math.max(lower, firstSet.nextClearBit(lower) - 1);
        return Range.openClosed(consumer.apply(firstKey, lower - 1),
                consumer.apply(firstKey, upper));
    }

    @Override
    public Range<Position> lastRange() {
        if (rangeBitSetMap.isEmpty()) {
            return null;
        }
        long lastKey = rangeBitSetMap.lastLongKey();
        RoaringBitSet lastSet = rangeBitSetMap.get(lastKey);
        int upper = lastSetBit(lastSet);
        int lower = Math.min(lastSet.previousClearBit(upper), upper);
        return Range.openClosed(consumer.apply(lastKey, lower),
                consumer.apply(lastKey, upper));
    }

    @Override
    public Map<Long, long[]> toRanges(int maxRanges) {
        Map<Long, long[]> internalBitSetMap = new HashMap<>();
        MutableInt rangeCount = new MutableInt();
        rangeBitSetMap.forEach((id, bmap) -> {
            if (rangeCount.addAndGet(bmap.cardinality()) > maxRanges) {
                return;
            }
            internalBitSetMap.put(id, bmap.toLongArray());
        });
        return internalBitSetMap;
    }

    @Override
    public void build(Map<Long, long[]> internalRange) {
        rangeBitSetMap.clear();

        internalRange.forEach((id, ranges) -> {
            RoaringBitSet bitSet = new RoaringBitSet();
            fromLongArray(bitSet, ranges);
            rangeBitSetMap.put(id, bitSet);
        });
        invalidateCaches();
    }

    /**
     * Populates a RoaringBitSet from a long[] array in the format produced by BitSet.toLongArray().
     * This avoids creating a temporary ordinary java.util.BitSet via BitSet.valueOf().
     */
    private static void fromLongArray(RoaringBitSet bitSet, long[] words) {
        for (int wordIndex = 0; wordIndex < words.length; wordIndex++) {
            long word = words[wordIndex];
            if (word != 0) {
                int bitIndex = wordIndex * Long.SIZE;
                for (int i = 0; i < Long.SIZE; i++) {
                    if ((word & (1L << i)) != 0) {
                        bitSet.set(bitIndex + i);
                    }
                }
            }
        }
    }

    @Override
    public int cardinality(long lowerKey, long lowerValue, long upperKey, long upperValue) {
        Long2ObjectSortedMap<RoaringBitSet> subMap = rangeBitSetMap.subMap(lowerKey, upperKey + 1);
        MutableInt v = new MutableInt(0);
        subMap.forEach((key, bitSet) -> {
            if (key == lowerKey || key == upperKey) {
                RoaringBitSet temp = (RoaringBitSet) bitSet.clone();
                if (key == lowerKey) {
                    temp.clear(0, (int) Math.max(0, lowerValue));
                }
                if (key == upperKey) {
                    temp.clear((int) Math.min(upperValue + 1, temp.length()), temp.length());
                }
                v.add(temp.cardinality());
            } else {
                v.add(bitSet.cardinality());
            }
        });
        return v.intValue();
    }

    @Override
    public int size() {
        if (updatedAfterCachedForSize) {
            MutableInt size = new MutableInt(0);
            forEachRawRange((lowerKey, lowerValue, upperKey, upperValue) -> {
                size.increment();
                return true;
            });
            cachedSize = size.intValue();
            updatedAfterCachedForSize = false;
        }
        return cachedSize;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(rangeBitSetMap);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PositionRangeSet)) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        PositionRangeSet other = (PositionRangeSet) obj;
        return this.rangeBitSetMap.equals(other.rangeBitSetMap);
    }

    @Override
    public String toString() {
        if (updatedAfterCachedForToString) {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            AtomicBoolean first = new AtomicBoolean(true);
            forEach(range -> {
                if (!first.get()) {
                    sb.append(",");
                }
                sb.append(range);
                first.set(false);
                return true;
            });
            sb.append("]");
            cachedToString = sb.toString();
            updatedAfterCachedForToString = false;
        }
        return cachedToString;
    }

    /**
     * Adds a Guava {@link Range} of {@link Position} endpoints. Test-only.
     *
     * <p>Pre-creates the lower-key bitmap and seeds one bit so that {@link #addOpenClosed}'s
     * "skip when lower bitmap is empty" branch does not drop the lower endpoint.
     */
    @VisibleForTesting
    void add(Range<Position> range) {
        Position lowerEndpoint = range.hasLowerBound() ? range.lowerEndpoint()
                : PositionFactory.create(EARLIEST_KEY, EARLIEST_VALUE);
        Position upperEndpoint = range.hasUpperBound() ? range.upperEndpoint()
                : PositionFactory.create(LATEST_KEY, LATEST_VALUE);

        long lowerValueOpen = (range.hasLowerBound() && range.lowerBoundType().equals(BoundType.CLOSED))
                ? getSafeEntry(lowerEndpoint) - 1
                : getSafeEntry(lowerEndpoint);
        long upperValueClosed = (range.hasUpperBound() && range.upperBoundType().equals(BoundType.CLOSED))
                ? getSafeEntry(upperEndpoint)
                : getSafeEntry(upperEndpoint) + 1;

        rangeBitSetMap.computeIfAbsent(lowerEndpoint.getLedgerId(), k -> new RoaringBitSet())
                .set((int) lowerValueOpen + 1);
        addOpenClosed(lowerEndpoint.getLedgerId(), lowerValueOpen,
                upperEndpoint.getLedgerId(), upperValueClosed);
    }

    /**
     * Removes a Guava {@link Range} of {@link Position} endpoints. Test-only; does not touch the
     * dirty-ledger tracker.
     */
    @VisibleForTesting
    void remove(Range<Position> range) {
        Position lowerEndpoint = range.hasLowerBound() ? range.lowerEndpoint()
                : PositionFactory.create(EARLIEST_KEY, EARLIEST_VALUE);
        Position upperEndpoint = range.hasUpperBound() ? range.upperEndpoint()
                : PositionFactory.create(LATEST_KEY, LATEST_VALUE);

        long lower = (range.hasLowerBound() && range.lowerBoundType().equals(BoundType.CLOSED))
                ? getSafeEntry(lowerEndpoint)
                : getSafeEntry(lowerEndpoint) + 1;
        long upper = (range.hasUpperBound() && range.upperBoundType().equals(BoundType.CLOSED))
                ? getSafeEntry(upperEndpoint)
                : getSafeEntry(upperEndpoint) - 1;

        long lowerLedgerId = lowerEndpoint.getLedgerId();
        long upperLedgerId = upperEndpoint.getLedgerId();
        boolean lowerIsEarliest = lowerLedgerId == EARLIEST_KEY && lowerEndpoint.getEntryId() == EARLIEST_VALUE;
        boolean upperIsLatest = upperLedgerId == LATEST_KEY && upperEndpoint.getEntryId() == LATEST_VALUE;
        boolean sameLedger = lowerLedgerId == upperLedgerId;

        if (lowerIsEarliest) {
            rangeBitSetMap.headMap(upperLedgerId).clear();
        }
        if (upperIsLatest) {
            rangeBitSetMap.tailMap(lowerLedgerId + 1).clear();
        }
        if (!sameLedger && !lowerIsEarliest && !upperIsLatest) {
            rangeBitSetMap.subMap(lowerLedgerId + 1, upperLedgerId).clear();
        }

        RoaringBitSet lowerSet = lowerIsEarliest ? null : rangeBitSetMap.get(lowerLedgerId);
        RoaringBitSet upperSet = upperIsLatest ? null
                : (sameLedger ? lowerSet : rangeBitSetMap.get(upperLedgerId));

        if (sameLedger && lowerSet != null) {
            lowerSet.clear((int) lower, (int) upper + 1);
        } else {
            if (lowerSet != null) {
                // Preserves a long-standing behavior of the previous OpenLongPairRangeSet.remove:
                // BitSet.clear(lower, previousSetBit(length)) is half-open, so the highest set bit
                // itself survives. Existing cursor recovery and RangeSetWrapperTest rely on this;
                // do not "fix" the apparent off-by-one without a separate PIP.
                lowerSet.clear((int) lower, lastSetBit(lowerSet));
            }
            if (upperSet != null) {
                // upper+1 must not overflow to Integer.MIN_VALUE; entry ids never approach
                // Integer.MAX_VALUE in practice (managedLedgerMaxEntriesPerLedger is int and far
                // smaller), but the same latent overflow exists in the original
                // OpenLongPairRangeSet.remove — kept for behavioral parity until a separate fix.
                upperSet.clear(0, (int) upper + 1);
            }
        }

        if (lowerSet != null && lowerSet.isEmpty()) {
            rangeBitSetMap.remove(lowerLedgerId);
        }
        if (!sameLedger && upperSet != null && upperSet.isEmpty()) {
            rangeBitSetMap.remove(upperLedgerId);
        }

        invalidateCaches();
    }

    /** Resets the dirty-ledger tracker; called after persistence has flushed all dirty segments. */
    void resetDirtyKeys() {
        dirtyLedgers.clear();
    }

    /** Whether {@code ledgerId} has been modified since the last {@link #resetDirtyKeys()}. */
    boolean isDirtyLedgers(long ledgerId) {
        return ledgerId >= 0 && ledgerId <= Integer.MAX_VALUE && dirtyLedgers.contains(ledgerId);
    }

    private void markDirty(long lowerKey, long upperKey) {
        // Original semantics: dirtyLedgers.addOpenClosed(k1, 0, k2, 0), which in LongPair ordering
        // is (k1, k2] on ledger ids. LongBitmap.add(from, to) is half-open [from, to), so shift both
        // bounds. Same-ledger or inverted range is a no-op.
        //
        // Note: Ledger IDs are 64-bit longs, but LongBitmap supports unsigned 32-bit range [0, 2^32-1].
        // In practice, BookKeeper ledger IDs rarely exceed Integer.MAX_VALUE. If upperKey exceeds
        // this limit, we skip tracking to avoid overflow. This is acceptable because:
        // 1. The dirty tracker is an optimization hint for selective persistence
        // 2. Missing a dirty mark means conservative full-ledger write (safe, just slower)
        // 3. Real-world ledger IDs stay well within 32-bit range
        if (upperKey <= lowerKey || lowerKey < 0) {
            return;
        }
        if (lowerKey >= Integer.MAX_VALUE || upperKey > Integer.MAX_VALUE) {
            log.warn()
                    .attr("lowerKey", lowerKey)
                    .attr("upperKey", upperKey)
                    .log("Skipping dirty tracking for ledger ID at/exceeding Integer.MAX_VALUE");
            return;
        }
        dirtyLedgers.add(lowerKey + 1, upperKey + 1);
    }

    private boolean isValid(long key, long value) {
        return key != EARLIEST_KEY && value != EARLIEST_VALUE && key != LATEST_KEY && value != LATEST_VALUE;
    }

    private int getSafeEntry(Position position) {
        return getSafeEntry(position.getEntryId());
    }

    private int getSafeEntry(long value) {
        return (int) Math.max(value, -1);
    }

    private void invalidateCaches() {
        updatedAfterCachedForSize = true;
        updatedAfterCachedForToString = true;
    }
}
