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

/**
 * Tracks deleted-message positions as ranges of {@link Position}s.
 *
 * <p>The implementation stores positions in a two-level structure:
 * the ledger id is used as the map key, and the corresponding entry ids are stored in a
 * {@link LongBitmap}. Bit {@code n} in the bitmap of ledger {@code L} represents the position
 * {@code (L, n)}.
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
 * <p>The persisted representation remains compatible with the existing format using
 * {@link LongBitmap#serializeToLongArray()} and {@link LongBitmap#deserializeFromLongArray(long[])},
 * which are compatible with the BitSet long[] format.
 *
 * <p>Entry ids are stored as bitmap indexes and therefore use {@code int} values. This is safe
 * because {@code managedLedgerMaxEntriesPerLedger} is an {@code int}, so valid entry ids are within
 * {@code [0, Integer.MAX_VALUE]}.
 */
class PositionRangeSet implements LongPairRangeSet<Position> {

    private static final Logger log = Logger.get(PositionRangeSet.class);

    private static final long EARLIEST_LEDGER_ID = -1L;
    private static final long EARLIEST_ENTRY_ID = -1L;
    private static final long LATEST_LEDGER_ID = Long.MAX_VALUE;
    private static final long LATEST_ENTRY_ID = Long.MAX_VALUE;

    /**
     * Maps ledger ID to a bitmap of deleted entry IDs within that ledger.
     * Bit {@code n} in the bitmap represents entry {@code n} in the ledger.
     */
    private final Long2ObjectSortedMap<LongBitmap> rangeBitmapMap = new Long2ObjectRBTreeMap<>();
    private final LongPairConsumer<Position> consumer;
    private final boolean enableMultiEntry;

    private final LongBitmap dirtyLedgers = LongBitmaps.create();

    private int cachedSize = 0;
    private String cachedToString = "[]";
    private boolean updatedAfterCachedForSize = true;
    private boolean updatedAfterCachedForToString = true;

    PositionRangeSet(LongPairConsumer<Position> consumer, boolean enableMultiEntry) {
        this.consumer = consumer;
        this.enableMultiEntry = enableMultiEntry;
    }

    private static long lastPresentValue(LongBitmap bitmap) {
        return bitmap.lastPresentValue();
    }

    @Override
    public void addOpenClosed(long lowerLedgerId, long lowerEntryIdOpen, long upperLedgerId, long upperEntryId) {
        if (enableMultiEntry) {
            markDirty(lowerLedgerId, upperLedgerId);
        }
        long lowerEntryId = lowerEntryIdOpen + 1;
        if (lowerLedgerId != upperLedgerId) {
            // Extend lower ledger's bitmap only if it already exists and has bits at/after lowerEntryId;
            // otherwise we'd invent acknowledgements that never happened (e.g. (2:10..4:10] must not
            // touch 2:10 if ledger 2 was empty).
            if (isValid(lowerLedgerId, lowerEntryId)) {
                LongBitmap rangeBitmap = rangeBitmapMap.get(lowerLedgerId);
                if (rangeBitmap != null) {
                    long lastEntryId = rangeBitmap.lastPresentValue();
                    if (lastEntryId > lowerEntryIdOpen) {
                        rangeBitmap.add(lowerEntryId, Math.max(lastEntryId, lowerEntryId) + 1);
                    }
                }
            }
            if (isValid(upperLedgerId, upperEntryId)) {
                LongBitmap rangeBitmap = rangeBitmapMap.computeIfAbsent(upperLedgerId, k -> LongBitmaps.create());
                rangeBitmap.add(0, upperEntryId + 1);
            }
        } else {
            LongBitmap rangeBitmap = rangeBitmapMap.computeIfAbsent(lowerLedgerId, k -> LongBitmaps.create());
            rangeBitmap.add(lowerEntryId, upperEntryId + 1);
        }
        invalidateCaches();
    }

    @Override
    public boolean contains(long ledgerId, long entryId) {
        LongBitmap rangeBitmap = rangeBitmapMap.get(ledgerId);
        if (rangeBitmap != null) {
            return rangeBitmap.contains(getSafeEntry(entryId));
        }
        return false;
    }

    @Override
    public Range<Position> rangeContaining(long ledgerId, long entryId) {
        LongBitmap rangeBitmap = rangeBitmapMap.get(ledgerId);
        if (rangeBitmap == null || !rangeBitmap.contains(getSafeEntry(entryId))) {
            return null;
        }
        long safeEntryId = getSafeEntry(entryId);
        long lowerEntryId = rangeBitmap.previousAbsentValue(safeEntryId) + 1;
        Position lower = consumer.apply(ledgerId, lowerEntryId);
        long nextAbsentEntryId = rangeBitmap.nextAbsentValue(safeEntryId);
        Position upper = consumer.apply(ledgerId, Math.max(nextAbsentEntryId - 1, lowerEntryId));
        return Range.closed(lower, upper);
    }

    @Override
    public void removeAtMost(long ledgerId, long entryId) {
        if (enableMultiEntry && ledgerId >= 0) {
            long end = Math.min(ledgerId + 1L, (long) Integer.MAX_VALUE + 1);
            dirtyLedgers.remove(0, end);
        }
        remove(Range.atMost(PositionFactory.create(ledgerId, entryId)));
    }

    @Override
    public boolean isEmpty() {
        if (rangeBitmapMap.isEmpty()) {
            return true;
        }
        for (LongBitmap bitmap : rangeBitmapMap.values()) {
            if (!bitmap.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void clear() {
        rangeBitmapMap.clear();
        resetDirtyKeys();
        invalidateCaches();
    }

    @Override
    public Range<Position> span() {
        if (rangeBitmapMap.isEmpty()) {
            return null;
        }
        long firstLedgerId = rangeBitmapMap.firstLongKey();
        long lastLedgerId = rangeBitmapMap.lastLongKey();
        LongBitmap firstBitmap = rangeBitmapMap.get(firstLedgerId);
        LongBitmap lastBitmap = rangeBitmapMap.get(lastLedgerId);
        long firstEntryId = firstBitmap.nextPresentValue(0);
        long lastEntryId = lastBitmap.lastPresentValue();
        return Range.openClosed(consumer.apply(firstLedgerId, firstEntryId - 1),
                consumer.apply(lastLedgerId, lastEntryId));
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
        forEachRawRange((lowerLedgerId, lowerEntryId, upperLedgerId, upperEntryId) -> {
            Range<Position> range = Range.openClosed(
                    consumerParam.apply(lowerLedgerId, lowerEntryId),
                    consumerParam.apply(upperLedgerId, upperEntryId));
            return action.process(range);
        });
    }

    @Override
    public void forEachRawRange(RawRangeProcessor processor) {
        AtomicBoolean completed = new AtomicBoolean(false);
        rangeBitmapMap.forEach((ledgerId, bitmap) -> {
            if (completed.get() || bitmap.isEmpty()) {
                return;
            }
            long firstEntryId = bitmap.nextPresentValue(0);
            long lastEntryId = bitmap.lastPresentValue();
            long currentEntryId = firstEntryId;
            while (currentEntryId != -1 && currentEntryId <= lastEntryId) {
                long nextAbsentEntryId = bitmap.nextAbsentValue(currentEntryId);
                if (!processor.processRawRange(ledgerId, currentEntryId - 1, ledgerId, nextAbsentEntryId - 1)) {
                    completed.set(true);
                    break;
                }
                if (nextAbsentEntryId > Integer.MAX_VALUE) {
                    break;
                }
                currentEntryId = bitmap.nextPresentValue(nextAbsentEntryId);
            }
        });
    }

    @Override
    public Range<Position> firstRange() {
        if (rangeBitmapMap.isEmpty()) {
            return null;
        }
        long firstLedgerId = rangeBitmapMap.firstLongKey();
        LongBitmap firstBitmap = rangeBitmapMap.get(firstLedgerId);
        long lowerEntryId = firstBitmap.nextPresentValue(0);
        long upperEntryId = Math.max(lowerEntryId, firstBitmap.nextAbsentValue(lowerEntryId) - 1);
        return Range.openClosed(consumer.apply(firstLedgerId, lowerEntryId - 1),
                consumer.apply(firstLedgerId, upperEntryId));
    }

    @Override
    public Range<Position> lastRange() {
        if (rangeBitmapMap.isEmpty()) {
            return null;
        }
        long lastLedgerId = rangeBitmapMap.lastLongKey();
        LongBitmap lastBitmap = rangeBitmapMap.get(lastLedgerId);
        long upperEntryId = lastBitmap.lastPresentValue();
        long lowerEntryId = Math.min(lastBitmap.previousAbsentValue(upperEntryId), upperEntryId);
        return Range.openClosed(consumer.apply(lastLedgerId, lowerEntryId),
                consumer.apply(lastLedgerId, upperEntryId));
    }

    @Override
    public Map<Long, long[]> toRanges(int maxRanges) {
        Map<Long, long[]> internalBitSetMap = new HashMap<>();
        MutableInt rangeCount = new MutableInt();
        rangeBitmapMap.forEach((ledgerId, bitmap) -> {
            if (rangeCount.addAndGet((int) bitmap.cardinality()) > maxRanges) {
                return;
            }
            internalBitSetMap.put(ledgerId, bitmap.serializeToLongArray());
        });
        return internalBitSetMap;
    }

    @Override
    public void build(Map<Long, long[]> internalRange) {
        rangeBitmapMap.clear();
        resetDirtyKeys();

        internalRange.forEach((ledgerId, ranges) -> {
            rangeBitmapMap.put(ledgerId.longValue(), LongBitmaps.deserializeFromLongArray(ranges));
        });
        invalidateCaches();
    }

    @Override
    public int cardinality(long lowerLedgerId, long lowerEntryId, long upperLedgerId, long upperEntryId) {
        Long2ObjectSortedMap<LongBitmap> subMap = rangeBitmapMap.subMap(lowerLedgerId, upperLedgerId + 1);
        MutableInt v = new MutableInt(0);
        subMap.forEach((ledgerId, bitmap) -> {
            if (ledgerId == lowerLedgerId && ledgerId == upperLedgerId) {
                long count = bitmap.rank(upperEntryId + 1) - bitmap.rank(lowerEntryId);
                v.add(Math.toIntExact(count));
            } else if (ledgerId == lowerLedgerId) {
                long count = bitmap.cardinality() - bitmap.rank(lowerEntryId);
                v.add(Math.toIntExact(count));
            } else if (ledgerId == upperLedgerId) {
                long count = bitmap.rank(upperEntryId + 1);
                v.add(Math.toIntExact(count));
            } else {
                v.add(Math.toIntExact(bitmap.cardinality()));
            }
        });
        return v.intValue();
    }

    @Override
    public int size() {
        if (updatedAfterCachedForSize) {
            MutableInt size = new MutableInt(0);
            forEachRawRange((lowerLedgerId, lowerEntryId, upperLedgerId, upperEntryId) -> {
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
        return Objects.hashCode(rangeBitmapMap);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PositionRangeSet other)) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        return this.rangeBitmapMap.equals(other.rangeBitmapMap);
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

    @VisibleForTesting
    void add(Range<Position> range) {
        Position lowerEndpoint = range.hasLowerBound() ? range.lowerEndpoint()
                : PositionFactory.create(EARLIEST_LEDGER_ID, EARLIEST_ENTRY_ID);
        Position upperEndpoint = range.hasUpperBound() ? range.upperEndpoint()
                : PositionFactory.create(LATEST_LEDGER_ID, LATEST_ENTRY_ID);

        long lowerEntryIdOpen = (range.hasLowerBound() && range.lowerBoundType().equals(BoundType.CLOSED))
                ? getSafeEntry(lowerEndpoint) - 1
                : getSafeEntry(lowerEndpoint);
        long upperEntryIdClosed = (range.hasUpperBound() && range.upperBoundType().equals(BoundType.CLOSED))
                ? getSafeEntry(upperEndpoint)
                : getSafeEntry(upperEndpoint) + 1;

        rangeBitmapMap.computeIfAbsent(lowerEndpoint.getLedgerId(), k -> LongBitmaps.create())
                .add(lowerEntryIdOpen + 1);
        addOpenClosed(lowerEndpoint.getLedgerId(), lowerEntryIdOpen,
                upperEndpoint.getLedgerId(), upperEntryIdClosed);
    }

    @VisibleForTesting
    void remove(Range<Position> range) {
        Position lowerEndpoint = range.hasLowerBound() ? range.lowerEndpoint()
                : PositionFactory.create(EARLIEST_LEDGER_ID, EARLIEST_ENTRY_ID);
        Position upperEndpoint = range.hasUpperBound() ? range.upperEndpoint()
                : PositionFactory.create(LATEST_LEDGER_ID, LATEST_ENTRY_ID);

        long lowerEntryId = (range.hasLowerBound() && range.lowerBoundType().equals(BoundType.CLOSED))
                ? getSafeEntry(lowerEndpoint)
                : getSafeEntry(lowerEndpoint) + 1;
        long upperEntryId = (range.hasUpperBound() && range.upperBoundType().equals(BoundType.CLOSED))
                ? getSafeEntry(upperEndpoint)
                : getSafeEntry(upperEndpoint) - 1;

        long lowerLedgerId = lowerEndpoint.getLedgerId();
        long upperLedgerId = upperEndpoint.getLedgerId();
        boolean lowerIsEarliest = lowerLedgerId == EARLIEST_LEDGER_ID
                && lowerEndpoint.getEntryId() == EARLIEST_ENTRY_ID;
        boolean upperIsLatest = upperLedgerId == LATEST_LEDGER_ID
                && upperEndpoint.getEntryId() == LATEST_ENTRY_ID;
        boolean sameLedger = lowerLedgerId == upperLedgerId;

        if (lowerIsEarliest) {
            rangeBitmapMap.headMap(upperLedgerId).clear();
        }
        if (upperIsLatest) {
            rangeBitmapMap.tailMap(lowerLedgerId + 1).clear();
        }
        if (!sameLedger && !lowerIsEarliest && !upperIsLatest) {
            rangeBitmapMap.subMap(lowerLedgerId + 1, upperLedgerId).clear();
        }

        LongBitmap lowerSet = lowerIsEarliest ? null : rangeBitmapMap.get(lowerLedgerId);
        LongBitmap upperSet = upperIsLatest ? null
                : (sameLedger ? lowerSet : rangeBitmapMap.get(upperLedgerId));

        if (sameLedger && lowerSet != null) {
            lowerSet.remove(lowerEntryId, upperEntryId + 1);
        } else {
            if (lowerSet != null) {
                lowerSet.remove(lowerEntryId, lastPresentValue(lowerSet));
            }
            if (upperSet != null) {
                upperSet.remove(0, upperEntryId + 1);
            }
        }

        if (lowerSet != null && lowerSet.isEmpty()) {
            rangeBitmapMap.remove(lowerLedgerId);
        }
        if (!sameLedger && upperSet != null && upperSet.isEmpty()) {
            rangeBitmapMap.remove(upperLedgerId);
        }

        invalidateCaches();
    }

    void resetDirtyKeys() {
        dirtyLedgers.clear();
    }

    boolean isDirtyLedgers(long ledgerId) {
        return ledgerId >= 0 && ledgerId <= Integer.MAX_VALUE && dirtyLedgers.contains(ledgerId);
    }

    private void markDirty(long lowerLedgerId, long upperLedgerId) {
        // Original semantics: dirtyLedgers.addOpenClosed(k1, 0, k2, 0), which in LongPair ordering
        // is (k1, k2] on ledger ids. LongBitmap.add(from, to) is half-open [from, to), so shift both
        // bounds. Same-ledger or inverted range is a no-op.
        //
        // Note: Ledger IDs are 64-bit longs, but LongBitmap supports unsigned 32-bit range [0, 2^32-1].
        // In practice, BookKeeper ledger IDs rarely exceed Integer.MAX_VALUE. If upperLedgerId exceeds
        // this limit, we skip tracking to avoid overflow. This is acceptable because:
        // 1. The dirty tracker is an optimization hint for selective persistence
        // 2. Missing a dirty mark means conservative full-ledger write (safe, just slower)
        // 3. Real-world ledger IDs stay well within 32-bit range
        if (upperLedgerId <= lowerLedgerId || lowerLedgerId < 0) {
            return;
        }
        if (lowerLedgerId >= Integer.MAX_VALUE || upperLedgerId > Integer.MAX_VALUE) {
            log.warn()
                    .attr("lowerLedgerId", lowerLedgerId)
                    .attr("upperLedgerId", upperLedgerId)
                    .log("Skipping dirty tracking for ledger ID at/exceeding Integer.MAX_VALUE");
            return;
        }
        dirtyLedgers.add(lowerLedgerId + 1, upperLedgerId + 1);
    }

    private boolean isValid(long ledgerId, long entryId) {
        return ledgerId != EARLIEST_LEDGER_ID && entryId != EARLIEST_ENTRY_ID
                && ledgerId != LATEST_LEDGER_ID && entryId != LATEST_ENTRY_ID;
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
