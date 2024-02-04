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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ScanCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ScanOutcome;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.PositionBound;

@Slf4j
class OpScan implements ReadEntriesCallback {
    private final ManagedCursorImpl cursor;
    private final ManagedLedgerImpl ledger;
    private final ScanCallback callback;
    private final Predicate<Entry> condition;
    private final Object ctx;
    private final AtomicLong remainingEntries = new AtomicLong();
    private final long timeOutMs;
    private final long startTime = System.currentTimeMillis();
    private final int batchSize;

    PositionImpl searchPosition;
    Position lastSeenPosition = null;

    public OpScan(ManagedCursorImpl cursor, int batchSize,
                  PositionImpl startPosition, Predicate<Entry> condition,
                  ScanCallback callback, Object ctx, long maxEntries, long timeOutMs) {
        this.batchSize = batchSize;
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize " + batchSize);
        }
        this.cursor = Objects.requireNonNull(cursor);
        this.ledger = cursor.ledger;
        this.callback = callback;
        this.condition = condition;
        this.ctx = ctx;
        this.searchPosition = startPosition;
        this.remainingEntries.set(maxEntries);
        this.timeOutMs = timeOutMs;
    }

    @Override
    public void readEntriesComplete(List<Entry> entries, Object ctx) {
        try {
            Position lastPositionForBatch = entries.get(entries.size() - 1).getPosition();
            lastSeenPosition = lastPositionForBatch;
            // filter out the entry if it has been already deleted
            // filterReadEntries will call entry.release if the entry is filtered out
            List<Entry> entriesFiltered = this.cursor.filterReadEntries(entries);
            int skippedEntries = entries.size() - entriesFiltered.size();
            remainingEntries.addAndGet(-skippedEntries);
            if (!entriesFiltered.isEmpty()) {
                for (Entry entry : entriesFiltered) {
                    if (remainingEntries.decrementAndGet() <= 0) {
                        log.warn("[{}] Scan abort after reading too many entries", OpScan.this.cursor);
                        callback.scanComplete(lastSeenPosition, ScanOutcome.ABORTED, OpScan.this.ctx);
                        return;
                    }
                    if (!condition.test(entry)) {
                        log.warn("[{}] Scan abort due to user code", OpScan.this.cursor);
                        callback.scanComplete(lastSeenPosition, ScanOutcome.USER_INTERRUPTED, OpScan.this.ctx);
                        return;
                    }
                }
            }
            searchPosition = ledger.getPositionAfterN((PositionImpl) lastPositionForBatch, 1,
                    PositionBound.startExcluded);
            if (log.isDebugEnabled()) {
                log.debug("readEntryComplete {} at {} next is {}", lastPositionForBatch, searchPosition);
            }

            if (searchPosition.compareTo((PositionImpl) lastPositionForBatch) == 0) {
                // we have reached the end of the ledger, as we are not doing progress
                callback.scanComplete(lastSeenPosition, ScanOutcome.COMPLETED, OpScan.this.ctx);
                return;
            }
        } catch (Throwable t) {
            log.error("Unhandled error", t);
            callback.scanFailed(ManagedLedgerException.getManagedLedgerException(t),
                    Optional.ofNullable(lastSeenPosition), OpScan.this.ctx);
            return;
        } finally {
            entries.forEach(Entry::release);
        }
        find();
    }

    @Override
    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
        callback.scanFailed(exception, Optional.ofNullable(searchPosition), OpScan.this.ctx);
    }

    public void find() {
        if (remainingEntries.get() <= 0) {
            log.warn("[{}] Scan abort after reading too many entries", OpScan.this.cursor);
            callback.scanComplete(lastSeenPosition, ScanOutcome.ABORTED, OpScan.this.ctx);
            return;
        }
        if (System.currentTimeMillis() - startTime > timeOutMs) {
            log.warn("[{}] Scan abort after hitting the deadline", OpScan.this.cursor);
            callback.scanComplete(lastSeenPosition, ScanOutcome.ABORTED, OpScan.this.ctx);
            return;
        }
        if (cursor.hasMoreEntries(searchPosition)) {
            OpReadEntry opReadEntry = OpReadEntry.create(cursor, searchPosition, batchSize,
            this, OpScan.this.ctx, null, null, false);
            ledger.asyncReadEntries(opReadEntry);
        } else {
            callback.scanComplete(lastSeenPosition, ScanOutcome.COMPLETED, OpScan.this.ctx);
        }
    }
}
