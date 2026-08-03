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

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.BiConsumer;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.BatchedEntryDeletionIndexInfo;
import org.apache.bookkeeper.mledger.proto.MessageRange;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.apache.commons.lang3.mutable.MutableBoolean;

/**
 * per-msgLedger {@link AckPersistence}: per-msgLedger Data Entry (DE) + Checkpoint Marker (CM).
 * Every flush writes DE × N + CM. Last entry is always CM, so recovery is always fast path.
 */
public class IndividualAckStatePersistence implements AckPersistence {

    private static final io.github.merlimat.slog.Logger log =
            io.github.merlimat.slog.Logger.get(IndividualAckStatePersistence.class);

    private final CursorWal wal;
    private final ReadWriteLock lock;
    private final BiConsumer<Long, Long> cmAckedCallback;
    private final OpenTelemetryManagedCursorStats otelStats;
    private final ManagedCursor cursor;

    private final Map<Long, Position> lastAppendedDEPos = new HashMap<>();
    private final Map<Long, byte[]> lastAppendedBitmap = new HashMap<>();

    public IndividualAckStatePersistence(CursorWal wal, ReadWriteLock lock,
                                         BiConsumer<Long, Long> cmAckedCallback,
                                         OpenTelemetryManagedCursorStats otelStats,
                                         ManagedCursor cursor) {
        this.wal = wal;
        this.lock = lock;
        this.cmAckedCallback = cmAckedCallback;
        this.otelStats = otelStats;
        this.cursor = cursor;
    }

    @Override
    public CompletableFuture<PersistResult> persist(
            LedgerHandle lh, Position mdPos, Map<String, Long> properties,
            BitmapAckState ackState) {

        var batchAckList = ackState.buildBatchEntryDeletionIndexInfoList(Integer.MAX_VALUE);
        final Set<Long> dirtyLedgers;
        final Map<Long, byte[]> newBitmaps = new HashMap<>();
        final Map<Long, List<BatchedEntryDeletionIndexInfo>> batchAcksByLedger = new HashMap<>();

        lock.readLock().lock();
        try {
            dirtyLedgers = ackState.snapshotAndClearDirtyLedgers();
            for (long msgLedgerId : dirtyLedgers) {
                var bitmap = ackState.bitmapOf(msgLedgerId);
                var batchAcks = filterBatchAckForLedger(batchAckList, msgLedgerId);
                if (bitmap == null && batchAcks.isEmpty()) continue;
                if (bitmap != null && batchAcks.isEmpty()) {
                    var last = lastAppendedBitmap.get(msgLedgerId);
                    if (last != null && Arrays.equals(last, bitmap)) continue;
                }
                newBitmaps.put(msgLedgerId, bitmap);
                batchAcksByLedger.put(msgLedgerId, batchAcks);
            }
        } finally {
            lock.readLock().unlock();
        }

        var msgLedgerOrder = List.copyOf(newBitmaps.keySet());
        var deFutures = msgLedgerOrder.stream()
                .map(id -> wal.append(lh, buildDe(id, newBitmaps.get(id), batchAcksByLedger.get(id)))
                        .thenApply(CursorWal.AppendResult::commitEntryId))
                .toList();

        var allDEs = deFutures.isEmpty()
                ? CompletableFuture.<Void>completedFuture(null)
                : CompletableFuture.allOf(deFutures.toArray(CompletableFuture[]::new));

        return allDEs.thenCompose(ignored -> {
            lock.writeLock().lock();
            try {
                for (int i = 0; i < msgLedgerOrder.size(); i++) {
                    long id = msgLedgerOrder.get(i);
                    lastAppendedDEPos.put(id, PositionFactory.create(lh.getId(), deFutures.get(i).join()));
                    var bm = newBitmaps.get(id);
                    if (bm != null) lastAppendedBitmap.put(id, bm);
                }
            } finally {
                lock.writeLock().unlock();
            }
            return wal.append(lh, buildCm(mdPos, properties, lastAppendedDEPos)).thenApply(result -> {
                if (cmAckedCallback != null) cmAckedCallback.accept(lh.getId(), result.commitEntryId());
                return new PersistResult(result.totalBytes(), result.commitEntryId());
            });
        }).exceptionally(error -> {
            lock.writeLock().lock();
            try { ackState.restoreDirtyLedgers(dirtyLedgers); }
            finally { lock.writeLock().unlock(); }
            throw new java.util.concurrent.CompletionException(error);
        });
    }

    @Override
    public CompletableFuture<RecoveredState> recover(LedgerHandle lh) {
        return wal.readLatestDeCmInstance(lh).thenApply(s ->
                new RecoveredState(s.positionInfo(), s.commitEntryId(), s.stateSize()));
    }

    @Override
    public void onLedgerRollover() {
        // Keep lastAppendedDEPos as-is — CM references DEs in old cursor ledger via cross-ledger reads.
    }

    /**
     * Called when mark-delete advances. Removes DE references for message-ledgers fully covered
     * by the mark-delete position (ledgerId < mdLedgerId).
     */
    @Override
    public void onMarkDeleteAdvance(long mdLedgerId) {
        if (mdLedgerId <= 0) return;
        lock.writeLock().lock();
        try {
            lastAppendedDEPos.keySet().removeIf(id -> id < mdLedgerId);
            lastAppendedBitmap.keySet().removeIf(id -> id < mdLedgerId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void setZkCmHint(long cursorLedgerId, long entryId) { }

    @Override
    public boolean shouldGcOldCursorLedgerOnRollover() {
        return false; // DE+CM keeps cross-ledger references to old DEs; GC is deferred.
    }

    /**
     * Returns the minimum cursorLedgerId referenced by any DE in the current lastAppendedDEPos.
     * Used by ManagedCursorImpl to GC old cursor ledgers below this threshold.
     */
    @Override
    public long getOldestReferencedCursorLedgerId() {
        lock.readLock().lock();
        try {
            return lastAppendedDEPos.values().stream()
                    .mapToLong(Position::getLedgerId)
                    .min()
                    .orElse(-1);
        } finally {
            lock.readLock().unlock();
        }
    }

    // ---- AckPersistence ZK-MetaStore build helpers ----

    @Override
    public List<MessageRange> buildIndividualDeletedMessageRanges(BitmapAckState ackState, int maxRanges) {
        lock.writeLock().lock();
        try {
            if (ackState.isEmpty()) {
                return Collections.emptyList();
            }
            List<MessageRange> rangeList = new ArrayList<>();
            final MutableBoolean truncated = new MutableBoolean(false);

            ackState.forEachRawRange((lk, lv, uk, uv) -> {
                if (rangeList.size() >= maxRanges) {
                    truncated.setTrue();
                    return false;
                }
                MessageRange mr = new MessageRange();
                mr.setLowerEndpoint().setLedgerId(lk).setEntryId(lv);
                mr.setUpperEndpoint().setLedgerId(uk).setEntryId(uv);
                rangeList.add(mr);
                return true;
            });

            ackState.resetDirtyKeys();

            if (truncated.booleanValue()) {
                if (otelStats != null) otelStats.incrementPersistUnackedRangesTruncated(cursor);
                log.warn()
                    .attr("totalRanges", ackState.size())
                    .attr("maxRanges", maxRanges)
                    .log("Individually deleted message ranges truncated during DE+CM MetaStore write.");
            }
            return rangeList;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public List<BatchedEntryDeletionIndexInfo> buildBatchEntryDeletionIndexInfoList(
            BitmapAckState ackState, int maxIndexes) {
        lock.readLock().lock();
        try {
            List<BatchedEntryDeletionIndexInfo> result =
                    ackState.buildBatchEntryDeletionIndexInfoList(maxIndexes);
            int totalIndexes = ackState.getBatchDeletedIndexesSize();
            if (result.size() < totalIndexes) {
                if (otelStats != null) otelStats.incrementPersistBatchDeletedIndexesTruncated(cursor);
                log.warn()
                    .attr("totalIndexes", totalIndexes)
                    .attr("maxIndexes", maxIndexes)
                    .log("Batch deleted indexes truncated during DE+CM MetaStore write.");
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    // ---- builders ----

    private static List<BatchedEntryDeletionIndexInfo> filterBatchAckForLedger(
            List<BatchedEntryDeletionIndexInfo> list, long msgLedgerId) {
        if (list == null || list.isEmpty()) return List.of();
        return list.stream()
                .filter(i -> i.getPosition().getLedgerId() == msgLedgerId)
                .collect(toList());
    }

    private static PositionInfo buildDe(long msgLedgerId, byte[] bitmapBytes,
                                        List<BatchedEntryDeletionIndexInfo> batchAcks) {
        var de = new PositionInfo().setLedgerId(0L).setEntryId(0L);
        var range = de.addIndividualDeletedMessageRange();
        range.setKey(msgLedgerId);
        if (bitmapBytes != null) range.setBitmap(bitmapBytes);
        if (batchAcks != null && !batchAcks.isEmpty()) de.addAllBatchedEntryDeletionIndexInfos(batchAcks);
        return markType(de, CursorWal.ENTRY_TYPE_DE);
    }

    private static PositionInfo buildCm(Position mdPos, Map<String, Long> properties,
                                        Map<Long, Position> dePositions) {
        var cm = new PositionInfo().setLedgerId(mdPos.getLedgerId()).setEntryId(mdPos.getEntryId());
        if (properties != null) {
            properties.forEach((k, v) -> {
                var p = cm.addProperty();
                p.setName(k);
                p.setValue(v);
            });
        }
        dePositions.forEach((id, pos) -> {
            var entry = cm.addIndividualDeletedMessageRange();
            entry.setKey(id);
            entry.addValue(pos.getLedgerId());
            entry.addValue(pos.getEntryId());
        });
        return markType(cm, CursorWal.ENTRY_TYPE_CM);
    }

    private static PositionInfo markType(PositionInfo pi, long type) {
        var p = pi.addProperty();
        p.setName(CursorWal.ENTRY_TYPE_PROPERTY);
        p.setValue(type);
        return pi;
    }
}
