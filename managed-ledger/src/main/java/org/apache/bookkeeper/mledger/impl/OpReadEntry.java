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

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NonRecoverableLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.TooManyRequestsException;
import org.apache.bookkeeper.mledger.Position;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OpReadEntry implements ReadEntriesCallback {
    static final OpReadEntry WAITING_READ_OP_FOR_CLOSED_CURSOR = new OpReadEntry();
    private static final AtomicInteger opReadIdGenerator = new AtomicInteger(1);
    /**
     * id for this read operation. Value can be negative when integer value overflow happens.
     * Used for waitingReadOp consistency so the the correct instance is handled after the instance has already been
     * recycled.
     */
    int id;
    ManagedCursorImpl cursor;
    PositionImpl readPosition;
    private int count;
    private ReadEntriesCallback callback;
    Object ctx;

    // Results
    private List<Entry> entries;
    private PositionImpl nextReadPosition;
    PositionImpl maxPosition;

    Predicate<PositionImpl> skipCondition;
    boolean skipOpenLedgerFullyAcked = false;

    public static OpReadEntry create(ManagedCursorImpl cursor, PositionImpl readPositionRef, int count,
                                     ReadEntriesCallback callback, Object ctx, PositionImpl maxPosition,
                                     Predicate<PositionImpl> skipCondition,
                                     boolean skipOpenLedgerFullyAcked) {
        OpReadEntry op = RECYCLER.get();
        op.id = opReadIdGenerator.getAndIncrement();
        op.readPosition = cursor.ledger.startReadOperationOnLedger(readPositionRef);
        op.cursor = cursor;
        op.count = count;
        op.callback = callback;
        op.entries = new ArrayList<>();
        if (maxPosition == null) {
            maxPosition = PositionImpl.LATEST;
        }
        op.maxPosition = maxPosition;
        op.skipCondition = skipCondition;
        op.skipOpenLedgerFullyAcked = skipOpenLedgerFullyAcked;
        op.ctx = ctx;
        op.nextReadPosition = PositionImpl.get(op.readPosition);
        return op;
    }

    private void internalReadEntriesComplete(List<Entry> returnedEntries) {
        if (returnedEntries.isEmpty()) {
            log.warn("[{}] Read no entries unexpectedly", this);
            checkReadCompletion();
            return;
        }
        // Filter the returned entries for individual deleted messages
        int entriesCount = returnedEntries.size();
        long entriesSize = 0;
        for (int i = 0; i < entriesCount; i++) {
            entriesSize += returnedEntries.get(i).getLength();
        }
        cursor.updateReadStats(entriesCount, entriesSize);

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Read entries succeeded batch_size={} cumulative_size={} requested_count={}",
                    cursor.ledger.getName(), cursor.getName(), returnedEntries.size(), entries.size(), count);
        }

        // Entries might be released after `filterReadEntries`, so retrieve the last position before that
        final var lastPosition = returnedEntries.get(entriesCount - 1).getPosition();
        final var filteredEntries = cursor.filterReadEntries(returnedEntries);
        entries.addAll(filteredEntries);

        // if entries have been filtered out then try to skip reading of already deletedMessages in that range
        final Position nexReadPosition = entriesCount != filteredEntries.size()
                ? cursor.getNextAvailablePosition((PositionImpl) lastPosition) : lastPosition.getNext();
        updateReadPosition(nexReadPosition);
        checkReadCompletion();
    }

    @Override
    public void readEntriesComplete(List<Entry> returnedEntries, Object ctx) {
        try {
            internalReadEntriesComplete(returnedEntries);
        } catch (Throwable throwable) {
            log.error("[{}] Fallback to readEntriesFailed for exception in readEntriesComplete", this, throwable);
            readEntriesFailed(ManagedLedgerException.getManagedLedgerException(throwable), ctx);
        }
    }

    @Override
    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
        try {
            internalReadEntriesFailed(exception, ctx);
        } catch (Throwable throwable) {
            // At least we should complete the callback
            fail(ManagedLedgerException.getManagedLedgerException(throwable), ctx);
        }
    }

    private void internalReadEntriesFailed(ManagedLedgerException exception, Object ctx) {
        cursor.readOperationCompleted();

        if (!entries.isEmpty()) {
            // There were already some entries that were read before, we can return them
            complete(ctx);
        } else if (!cursor.isClosed() && cursor.getConfig().isAutoSkipNonRecoverableData()
                && exception instanceof NonRecoverableLedgerException) {
            log.warn("[{}][{}] read failed from ledger at position:{} : {}", cursor.ledger.getName(), cursor.getName(),
                    readPosition, exception.getMessage());
            final ManagedLedgerImpl ledger = (ManagedLedgerImpl) cursor.getManagedLedger();
            Position nexReadPosition;
            Long lostLedger = null;
            if (exception instanceof ManagedLedgerException.LedgerNotExistException) {
                // try to find and move to next valid ledger
                nexReadPosition = cursor.getNextLedgerPosition(readPosition.getLedgerId());
                lostLedger = readPosition.ledgerId;
            } else {
                // Skip this read operation
                nexReadPosition = ledger.getValidPositionAfterSkippedEntries(readPosition, count);
            }
            // fail callback if it couldn't find next valid ledger
            if (nexReadPosition == null) {
                fail(exception, ctx);
                return;
            }
            updateReadPosition(nexReadPosition);
            if (lostLedger != null) {
                cursor.getManagedLedger().skipNonRecoverableLedger(lostLedger);
            }
            checkReadCompletion();
        } else {
            if (!(exception instanceof TooManyRequestsException)) {
                log.warn("[{}][{}] read failed from ledger at position:{}", cursor.ledger.getName(),
                        cursor.getName(), readPosition, exception);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] read throttled failed from ledger at position:{}", cursor.ledger.getName(),
                            cursor.getName(), readPosition);
                }
            }

            fail(exception, ctx);
        }
    }

    void updateReadPosition(Position newReadPosition) {
        nextReadPosition = (PositionImpl) newReadPosition;
        cursor.setReadPosition(nextReadPosition);
    }

    void checkReadCompletion() {
        // op readPosition is smaller or equals maxPosition then can read again
        if (entries.size() < count && cursor.hasMoreEntries()
                && maxPosition.compareTo(readPosition) > 0) {

            // We still have more entries to read from the next ledger, schedule a new async operation
            cursor.ledger.getExecutor().execute(() -> {
                readPosition = cursor.ledger.startReadOperationOnLedger(nextReadPosition);
                cursor.ledger.asyncReadEntries(OpReadEntry.this);
            });
        } else {
            // The reading was already completed, release resources and trigger callback
            try {
                cursor.readOperationCompleted();
            } finally {
                complete(ctx);
            }
        }
    }

    public int getNumberOfEntriesToRead() {
        return count - entries.size();
    }

    private final Handle<OpReadEntry> recyclerHandle;

    private OpReadEntry(Handle<OpReadEntry> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    // no-op constructor for EMPTY instance
    private OpReadEntry() {
        this.recyclerHandle = null;
        this.callback = new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                // no-op
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                // no-op
            }
        };
    }

    private static final Recycler<OpReadEntry> RECYCLER = new Recycler<OpReadEntry>() {
        @Override
        protected OpReadEntry newObject(Recycler.Handle<OpReadEntry> recyclerHandle) {
            return new OpReadEntry(recyclerHandle);
        }
    };

    public void recycle() {
        if (recyclerHandle == null) {
            // This is the no-op instance, do not recycle
            return;
        }
        id = -1;
        count = 0;
        cursor = null;
        readPosition = null;
        callback = null;
        ctx = null;
        entries = null;
        nextReadPosition = null;
        maxPosition = null;
        skipCondition = null;
        skipOpenLedgerFullyAcked = false;
        recyclerHandle.recycle(this);
    }

    private void complete(Object ctx) {
        cursor.ledger.getExecutor().execute(() -> {
            try {
                callback.readEntriesComplete(entries, ctx);
                recycle();
            } catch (Throwable throwable) {
                log.error("[{}] readEntriesComplete failed (last position: {})", this, lastEntryPosition(), throwable);
            }
        });
    }

    private void fail(ManagedLedgerException e, Object ctx) {
        try {
            callback.readEntriesFailed(e, ctx);
            cursor.ledger.mbean.recordReadEntriesError();
            recycle();
        } catch (Throwable throwable) {
            log.error("[{}] readEntriesFailed failed (exception: {})", this, e.getMessage(), throwable);
        }
    }

    @Override
    public String toString() {
        final var cursor = this.cursor;
        final var readPosition = this.readPosition;
        final var nextReadPosition = this.nextReadPosition;
        final var entries = this.entries;
        final var maxPosition = this.maxPosition;
        final var count = this.count;
        if (cursor != null) {
            return cursor.ledger.getName() + " " + cursor.getName() + "{ readPosition: "
                    + (readPosition != null ? readPosition : "(null)") + ", nextReadPosition: "
                    + (nextReadPosition != null ? nextReadPosition : "(null)") + ", maxPosition: "
                    + (maxPosition != null ? maxPosition : "(null)") + ", entries count: "
                    + (entries != null ? entries.size() : "(null)") + ", count: " + count + " }";
        } else {
            return "(null)";
        }
    }

    private String lastEntryPosition() {
        final var entries = this.entries;
        if (entries != null) {
            return entries.isEmpty() ? "(empty)" : entries.get(entries.size() - 1).getPosition().toString();
        } else {
            return "(null)";
        }
    }

    private static final Logger log = LoggerFactory.getLogger(OpReadEntry.class);
}
