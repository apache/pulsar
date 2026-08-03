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
import com.google.common.collect.Range;
import io.github.merlimat.slog.Logger;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import java.util.function.IntFunction;
import java.util.function.Supplier;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.BatchedEntryDeletionIndexInfo;
import org.apache.bookkeeper.mledger.proto.MessageRange;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.proto.NestedPositionInfo;
import org.apache.pulsar.common.util.collections.LongPairRangeSet;
import org.jspecify.annotations.Nullable;

/**
 * In-memory ack state backed by {@link PositionRangeSet} for individual-ack
 * tracking and a {@link ConcurrentSkipListMap} for batch-ack tracking.
 *
 * <p>This corresponds to the "old" / PositionSet implementation described in the design notes.
 * The two data structures are always accessed under the owning cursor's read or write lock.
 */
class BitmapAckState {

    private static final Logger log = Logger.get(BitmapAckState.class);

    /** Individual-ack ranges (position ranges of deleted entries). */
    private final PositionRangeSet individualDeletedMessages;

    /**
     * Partial batch-ack state: (ledgerId, entryId) → BitSet of un-acknowledged batch indexes.
     * {@code null} when {@code deletionAtBatchIndexLevelEnabled} is {@code false}.
     */
    @Nullable
    private final ConcurrentSkipListMap<Position, BitSet> batchDeletedIndexes;

    /**
     * @param positionRangeConverter factory for creating {@link Position} from (ledgerId, entryId)
     * @param enableDirtyTracking    {@code true} when PIP-81/488 multi-entry or DE+CM persistence
     *                               is enabled and per-ledger dirty tracking is needed
     * @param batchIndexAckEnabled   {@code true} when deletion-at-batch-index-level is enabled
     */
    BitmapAckState(LongPairRangeSet.LongPairConsumer<Position> positionRangeConverter,
                             boolean enableDirtyTracking,
                             boolean batchIndexAckEnabled) {
        this.individualDeletedMessages = new PositionRangeSet(positionRangeConverter, enableDirtyTracking);
        this.batchDeletedIndexes = batchIndexAckEnabled ? new ConcurrentSkipListMap<>() : null;
    }

    // =========================================================
    // Individual-ack mutations
    // =========================================================

    public void addOpenClosed(long prevLedgerId, long prevEntryId, long ledgerId, long entryId) {
        individualDeletedMessages.addOpenClosed(prevLedgerId, prevEntryId, ledgerId, entryId);
    }

    public void removeAtMost(long ledgerId, long entryId) {
        individualDeletedMessages.removeAtMost(ledgerId, entryId);
    }

    public void clear() {
        individualDeletedMessages.clear();
        if (batchDeletedIndexes != null) {
            batchDeletedIndexes.clear();
        }
    }

    public void markExternalDirty(long ledgerId) {
        individualDeletedMessages.markExternalDirty(ledgerId);
    }

    public void markAllDirty() {
        individualDeletedMessages.markAllDirty();
    }

    // =========================================================
    // Batch-ack mutations
    // =========================================================

    public boolean putBatchAck(Position position, long[] ackSet) {
        if (batchDeletedIndexes == null) {
            return false;
        }
        final var givenBitSet = BitSet.valueOf(ackSet);
        final var stored = batchDeletedIndexes.compute(position, (k, v) -> {
            if (v == null || givenBitSet.nextSetBit(0) > v.nextSetBit(0)) {
                return givenBitSet;
            }
            return v;
        });
        return stored.isEmpty();
    }

    public boolean mergeBatchAck(Position position, long[] ackSet) {
        if (batchDeletedIndexes == null) {
            return false;
        }
        final var givenBitSet = BitSet.valueOf(ackSet);
        final var merged = batchDeletedIndexes.computeIfAbsent(position, __ -> givenBitSet);
        if (merged != givenBitSet) {
            merged.and(givenBitSet);
        }
        return merged.isEmpty();
    }

    public void removeBatchAck(Position position) {
        if (batchDeletedIndexes != null) {
            batchDeletedIndexes.remove(position);
        }
    }

    public void removeBatchAcksBefore(Position exclusive) {
        if (batchDeletedIndexes != null) {
            batchDeletedIndexes.subMap(PositionFactory.EARLIEST, exclusive).clear();
        }
    }

    public void removeBatchAcksAtOrBefore(Position inclusive) {
        if (batchDeletedIndexes != null) {
            batchDeletedIndexes.subMap(PositionFactory.EARLIEST, false, inclusive, true).clear();
        }
    }

    // =========================================================
    // Queries
    // =========================================================

    public boolean contains(long ledgerId, long entryId) {
        return individualDeletedMessages.contains(ledgerId, entryId);
    }

    @Nullable
    public Range<Position> rangeContaining(long ledgerId, long entryId) {
        return individualDeletedMessages.rangeContaining(ledgerId, entryId);
    }

    @Nullable
    public Range<Position> firstRange() {
        return individualDeletedMessages.firstRange();
    }

    @Nullable
    public Range<Position> lastRange() {
        return individualDeletedMessages.lastRange();
    }

    public boolean isEmpty() {
        return individualDeletedMessages.isEmpty();
    }

    public int size() {
        return individualDeletedMessages.size();
    }

    @Nullable
    public Range<Position> span() {
        return individualDeletedMessages.span();
    }

    public long cardinality(long lowerLedgerId, long lowerEntryId,
                            long upperLedgerId, long upperEntryId) {
        return individualDeletedMessages.cardinality(lowerLedgerId, lowerEntryId,
                upperLedgerId, upperEntryId);
    }

    public void forEach(LongPairRangeSet.RangeProcessor<Position> action) {
        individualDeletedMessages.forEach(action);
    }

    public void forEach(LongPairRangeSet.RangeProcessor<Position> action,
                        LongPairRangeSet.LongPairConsumer<? extends Position> consumer) {
        individualDeletedMessages.forEach(action, consumer);
    }

    public void forEachRawRange(LongPairRangeSet.RawRangeProcessor action) {
        individualDeletedMessages.forEachRawRange(action);
    }

    public void forEachBatchDeletedIndex(java.util.function.BiConsumer<Position, long[]> action) {
        if (batchDeletedIndexes != null) {
            batchDeletedIndexes.forEach((pos, bitSet) -> action.accept(pos, bitSet.toLongArray()));
        }
    }

    @Nullable
    public long[] getBatchPositionAckSet(Position position) {
        if (batchDeletedIndexes == null) {
            return null;
        }
        final var bitSet = batchDeletedIndexes.get(position);
        return bitSet == null ? null : bitSet.toLongArray();
    }

    public boolean isBatchIndexAckEnabled() {
        return batchDeletedIndexes != null;
    }

    public int getBatchDeletedIndexesSize() {
        return batchDeletedIndexes == null ? 0 : batchDeletedIndexes.size();
    }

    // =========================================================
    // Recovery
    // =========================================================

    public void recoverFromMessageRanges(int count, IntFunction<MessageRange> accessor,
                                  Supplier<NavigableMap<Long, LedgerInfo>> ledgerInfoMapSupplier) {
        individualDeletedMessages.clear();
        for (int i = 0; i < count; i++) {
            MessageRange messageRange = accessor.apply(i);
            NestedPositionInfo lowerEndpoint = messageRange.getLowerEndpoint();
            NestedPositionInfo upperEndpoint = messageRange.getUpperEndpoint();

            if (lowerEndpoint.getLedgerId() == upperEndpoint.getLedgerId()) {
                individualDeletedMessages.addOpenClosed(
                        lowerEndpoint.getLedgerId(), lowerEndpoint.getEntryId(),
                        upperEndpoint.getLedgerId(), upperEndpoint.getEntryId());
            } else {
                NavigableMap<Long, LedgerInfo> ledgersInfo = ledgerInfoMapSupplier.get();
                LedgerInfo lowerLedgerInfo = ledgersInfo.get(lowerEndpoint.getLedgerId());
                if (lowerLedgerInfo != null) {
                    individualDeletedMessages.addOpenClosed(
                            lowerEndpoint.getLedgerId(), lowerEndpoint.getEntryId(),
                            lowerEndpoint.getLedgerId(), lowerLedgerInfo.getEntries() - 1);
                } else {
                    log.warn()
                            .attr("ledgerId", lowerEndpoint.getLedgerId())
                            .attr("entryId", lowerEndpoint.getEntryId())
                            .log("No ledger info of lower endpoint");
                }

                for (LedgerInfo li : ledgersInfo.subMap(
                        lowerEndpoint.getLedgerId(), false,
                        upperEndpoint.getLedgerId(), false).values()) {
                    individualDeletedMessages.addOpenClosed(
                            li.getLedgerId(), -1, li.getLedgerId(), li.getEntries() - 1);
                }

                individualDeletedMessages.addOpenClosed(
                        upperEndpoint.getLedgerId(), -1,
                        upperEndpoint.getLedgerId(), upperEndpoint.getEntryId());
            }
        }
    }

    public void recoverFromBitmaps(Map<Long, byte[]> bitmaps) {
        individualDeletedMessages.buildFromSerializedBitmaps(bitmaps);
    }

    public void recoverFromRanges(Map<Long, long[]> rangeMap) {
        individualDeletedMessages.build(rangeMap);
    }

    public void recoverBatchDeletedIndexes(int count, IntFunction<BatchedEntryDeletionIndexInfo> accessor) {
        if (batchDeletedIndexes == null) {
            return;
        }
        batchDeletedIndexes.clear();
        for (int i = 0; i < count; i++) {
            BatchedEntryDeletionIndexInfo info = accessor.apply(i);
            if (info.getDeleteSetsCount() > 0) {
                long[] array = new long[info.getDeleteSetsCount()];
                for (int j = 0; j < array.length; j++) {
                    array[j] = info.getDeleteSetAt(j);
                }
                batchDeletedIndexes.put(
                        PositionFactory.create(
                                info.getPosition().getLedgerId(),
                                info.getPosition().getEntryId()),
                        BitSet.valueOf(array));
            }
        }
    }

    // =========================================================
    // Serialization
    // =========================================================

    public Map<Long, byte[]> toSerializedBitmaps() {
        return individualDeletedMessages.toSerializedBitmaps();
    }

    public Map<Long, long[]> toRanges(int maxRanges) {
        return individualDeletedMessages.toRanges(maxRanges);
    }

    public List<BatchedEntryDeletionIndexInfo> buildBatchEntryDeletionIndexInfoList(int maxIndexes) {
        if (batchDeletedIndexes == null || batchDeletedIndexes.isEmpty()) {
            return Collections.emptyList();
        }
        List<BatchedEntryDeletionIndexInfo> result = new ArrayList<>();
        for (var entry : batchDeletedIndexes.entrySet()) {
            if (result.size() >= maxIndexes) {
                break;
            }
            BatchedEntryDeletionIndexInfo info = new BatchedEntryDeletionIndexInfo();
            info.setPosition()
                    .setLedgerId(entry.getKey().getLedgerId())
                    .setEntryId(entry.getKey().getEntryId());
            for (long l : entry.getValue().toLongArray()) {
                info.addDeleteSet(l);
            }
            result.add(info);
        }
        return result;
    }

    public void resetDirtyKeys() {
        individualDeletedMessages.resetDirtyKeys();
    }

    // =========================================================
    // Dirty tracking
    // =========================================================

    public Set<Long> snapshotAndClearDirtyLedgers() {
        return individualDeletedMessages.snapshotAndClearDirtyLedgers();
    }

    public void restoreDirtyLedgers(Set<Long> ledgerIds) {
        individualDeletedMessages.restoreDirtyLedgers(ledgerIds);
    }

    @Nullable
    public byte[] bitmapOf(long ledgerId) {
        return individualDeletedMessages.bitmapOf(ledgerId);
    }

    // =========================================================
    // Testing access
    // =========================================================

    /**
     * Returns the underlying {@link PositionRangeSet} for testing and introspection.
     * Production code should use the {@link AckState} interface instead.
     */
    @VisibleForTesting
    PositionRangeSet getIndividualDeletedMessages() {
        return individualDeletedMessages;
    }

    /**
     * Returns the underlying batch-ack map for testing and introspection, or {@code null}
     * when batch-index ack is disabled.
     */
    @VisibleForTesting
    @Nullable
    ConcurrentSkipListMap<Position, BitSet> getBatchDeletedIndexes() {
        return batchDeletedIndexes;
    }

    public String toString() {
        return individualDeletedMessages.toString();
    }
}
