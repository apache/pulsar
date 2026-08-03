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

import io.github.merlimat.slog.Logger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.proto.BatchedEntryDeletionIndexInfo;
import org.apache.bookkeeper.mledger.proto.LongListMap;
import org.apache.bookkeeper.mledger.proto.LongProperty;
import org.apache.bookkeeper.mledger.proto.MessageRange;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 * Legacy {@link AckPersistence}: writes a single {@link PositionInfo} entry per flush
 * directly to the cursor ledger. No chunking — if serialized data exceeds BK's entry
 * size limit, ack ranges are truncated by {@code managedLedgerMaxUnackedRangesToPersist}.
 *
 * <p>Serialization format depends on config:
 * <ul>
 *   <li>{@code persistentUnackedRangesWithMultipleEntriesEnabled=true}: per-ledger RoaringBitmap bytes.</li>
 *   <li>{@code persistIndividualAckAsLongArray=true} (default): compact packed-long representation.</li>
 *   <li>Fallback: classic flat {@link MessageRange} list.</li>
 * </ul>
 */
class LegacyAckPersistence implements AckPersistence {

    static final Logger log = Logger.get(LegacyAckPersistence.class);

    private final ManagedLedgerConfig config;
    private final ReadWriteLock lock;
    private final OpenTelemetryManagedCursorStats otelStats;
    private final ManagedCursor cursor;

    private final PositionInfo reusablePositionInfo = new PositionInfo();
    private volatile int serializedSize;
    private final AtomicBoolean lastCursorDataFullyPersistable = new AtomicBoolean(true);
    private final AtomicBoolean lastBatchDeletedIndexFullyPersistable = new AtomicBoolean(true);

    LegacyAckPersistence(ManagedLedgerConfig config, ReadWriteLock lock,
                         OpenTelemetryManagedCursorStats otelStats, ManagedCursor cursor) {
        this.config = config;
        this.lock = lock;
        this.otelStats = otelStats;
        this.cursor = cursor;
    }

    @Override
    public CompletableFuture<PersistResult> persist(
            LedgerHandle lh, Position mdPos, Map<String, Long> properties,
            BitmapAckState ackState) {
        var pi = reusablePositionInfo;
        pi.clear();
        pi.setLedgerId(mdPos.getLedgerId())
                .setEntryId(mdPos.getEntryId())
                .addAllBatchedEntryDeletionIndexInfos(
                        buildBatchEntryDeletionIndexInfoList(ackState, config.getMaxBatchDeletedIndexToPersist()))
                .addAllProperties(buildPropertiesMap(properties));

        if (config.isPersistentUnackedRangesWithMultipleEntriesEnabled()) {
            lock.readLock().lock();
            try {
                pi.addAllIndividualDeletedMessageRanges(
                        buildBitmapPropertiesMap(ackState.toSerializedBitmaps()));
            } catch (Exception e) {
                log.warn().exception(e).log("Failed to serialize individualDeletedMessages");
            } finally {
                lock.readLock().unlock();
            }
        } else {
            Map<Long, long[]> internalRanges = null;
            if (config.isPersistIndividualAckAsLongArray()) {
                lock.readLock().lock();
                try {
                    internalRanges = ackState.toRanges(config.getMaxUnackedRangesToPersist());
                } catch (Exception e) {
                    log.warn().exception(e).log("Failed to serialize individualDeletedMessages");
                } finally {
                    lock.readLock().unlock();
                }
            }
            if (internalRanges != null && !internalRanges.isEmpty()) {
                pi.addAllIndividualDeletedMessageRanges(buildLongPropertiesMap(internalRanges));
            } else {
                pi.addAllIndividualDeletedMessages(
                        buildIndividualDeletedMessageRanges(ackState, config.getMaxUnackedRangesToPersist()));
            }
        }

        var future = new CompletableFuture<PersistResult>();
        byte[] data = pi.toByteArray();
        lh.asyncAddEntry(data, (rc, lh1, entryId, ctx) -> {
            if (rc == BKException.Code.OK) {
                future.complete(new PersistResult(data.length, entryId));
            } else {
                future.completeExceptionally(BKException.create(rc));
            }
        }, null);
        return future;
    }

    @Override
    public CompletableFuture<RecoveredState> recover(LedgerHandle lh) {
        long lac = lh.getLastAddConfirmed();
        if (lac < 0) {
            return CompletableFuture.failedFuture(
                    new org.apache.bookkeeper.mledger.ManagedLedgerException("Cursor ledger has no entries"));
        }
        var future = new CompletableFuture<RecoveredState>();
        lh.asyncReadEntries(lac, lac, (rc, handle, seq, ctx) -> {
            if (rc != BKException.Code.OK) {
                future.completeExceptionally(BKException.create(rc));
                return;
            }
            if (!seq.hasMoreElements()) {
                future.completeExceptionally(
                        new org.apache.bookkeeper.mledger.ManagedLedgerException("Empty read result"));
                return;
            }
            LedgerEntry entry = seq.nextElement();
            byte[] bytes = entry.getEntry();
            PositionInfo pi = new PositionInfo();
            pi.parseFrom(bytes);
            future.complete(new RecoveredState(pi, lac, bytes.length));
        }, null);
        return future;
    }

    @Override
    public int getSerializedSize() {
        return serializedSize;
    }

    // ---- helpers ----

    private static List<LongProperty> buildPropertiesMap(Map<String, Long> properties) {
        if (properties == null || properties.isEmpty()) return Collections.emptyList();
        var props = new ArrayList<LongProperty>(properties.size());
        properties.forEach((k, v) -> props.add(new LongProperty().setName(k).setValue(v)));
        return props;
    }

    private List<LongListMap> buildBitmapPropertiesMap(Map<Long, byte[]> bitmaps) {
        var result = new ArrayList<LongListMap>(bitmaps.size());
        var size = new MutableInt();
        bitmaps.forEach((ledgerId, bitmap) -> {
            var entry = new LongListMap().setKey(ledgerId).setBitmap(bitmap);
            result.add(entry);
            size.add(entry.getSerializedSize());
        });
        this.serializedSize = size.toInteger();
        return result;
    }

    private List<LongListMap> buildLongPropertiesMap(Map<Long, long[]> properties) {
        if (properties.isEmpty()) return Collections.emptyList();
        var result = new ArrayList<LongListMap>();
        var size = new MutableInt();
        properties.forEach((id, ranges) -> {
            if (ranges == null || ranges.length == 0) return;
            var lm = new LongListMap().setKey(id);
            for (long range : ranges) lm.addValue(range);
            result.add(lm);
            size.add(lm.getSerializedSize());
        });
        this.serializedSize = size.toInteger();
        return result;
    }

    @Override
    public List<MessageRange> buildIndividualDeletedMessageRanges(BitmapAckState ackState, int maxRanges) {
        lock.writeLock().lock();
        try {
            if (ackState.isEmpty()) {
                this.serializedSize = 0;
                return Collections.emptyList();
            }
            var acksSerializedSize = new AtomicInteger(0);
            var rangeList = new ArrayList<MessageRange>();
            var truncated = new MutableBoolean(false);
            ackState.forEachRawRange((lowerKey, lowerValue, upperKey, upperValue) -> {
                if (rangeList.size() >= maxRanges) { truncated.setTrue(); return false; }
                var mr = new MessageRange();
                mr.setLowerEndpoint().setLedgerId(lowerKey).setEntryId(lowerValue);
                mr.setUpperEndpoint().setLedgerId(upperKey).setEntryId(upperValue);
                acksSerializedSize.addAndGet(mr.getSerializedSize());
                rangeList.add(mr);
                return true;
            });
            this.serializedSize = acksSerializedSize.get();
            ackState.resetDirtyKeys();
            if (truncated.booleanValue()) {
                if (otelStats != null) otelStats.incrementPersistUnackedRangesTruncated(cursor);
                if (lastCursorDataFullyPersistable.compareAndSet(true, false)) {
                    log.warn().attr("totalRanges", ackState.size()).attr("maxRanges", maxRanges)
                            .log("Individually deleted message ranges exceed managedLedgerMaxUnackedRangesToPersist.");
                }
            } else {
                lastCursorDataFullyPersistable.compareAndSet(false, true);
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
            var result = ackState.buildBatchEntryDeletionIndexInfoList(maxIndexes);
            int total = ackState.getBatchDeletedIndexesSize();
            if (result.size() < total) {
                if (otelStats != null) otelStats.incrementPersistBatchDeletedIndexesTruncated(cursor);
                if (lastBatchDeletedIndexFullyPersistable.compareAndSet(true, false)) {
                    log.warn().attr("totalIndexes", total).attr("maxIndexes", maxIndexes)
                            .log("Batch deleted indexes exceed managedLedgerMaxBatchDeletedIndexToPersist.");
                }
            } else {
                lastBatchDeletedIndexFullyPersistable.compareAndSet(false, true);
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }
}
