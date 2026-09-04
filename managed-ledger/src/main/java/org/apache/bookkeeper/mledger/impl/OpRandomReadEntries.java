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

import static java.lang.Math.min;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.CustomLog;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFencedException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.State;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.pulsar.common.util.FutureUtil;

@CustomLog
class OpRandomReadEntries implements ReadEntriesCallback {
    private final ManagedLedgerImpl ledger;
    private final Position maxPosition;
    private final int count;
    private final CompletableFuture<List<Entry>> promise = new CompletableFuture<>();
    private final List<Entry> entries = new ArrayList<>();
    private final AtomicBoolean terminal = new AtomicBoolean();
    private Position readPosition;
    private Position nextReadPosition;
    private final boolean populateCache;

    private OpRandomReadEntries(ManagedLedgerImpl ledger, Position readPosition, int count, Position maxPosition,
                                boolean populateCache) {
        this.ledger = ledger;
        this.readPosition = ledger.startReadOperationOnLedger(readPosition);
        this.count = count;
        this.maxPosition = maxPosition;
        this.nextReadPosition = this.readPosition;
        this.populateCache = populateCache;
    }

    static CompletableFuture<List<Entry>> read(ManagedLedgerImpl ledger, Position readPosition, int count,
                                               Position maxPosition) {
        return read(ledger, readPosition, count, maxPosition, true);
    }

    static CompletableFuture<List<Entry>> read(ManagedLedgerImpl ledger, Position readPosition, int count,
                                               Position maxPosition, boolean populateCache) {
        OpRandomReadEntries op = new OpRandomReadEntries(ledger, readPosition, count, maxPosition, populateCache);
        op.readEntries();
        return op.promise;
    }

    void readEntries() {
        if (terminal.get()) {
            return;
        }
        final State state = ManagedLedgerImpl.STATE_UPDATER.get(ledger);
        if (state.isFenced() || state == State.Closed) {
            readEntriesFailed(new ManagedLedgerFencedException(), null);
            return;
        }

        if (readPosition.compareTo(maxPosition) > 0) {
            checkReadCompletion();
            return;
        }

        long ledgerId = readPosition.getLedgerId();
        LedgerHandle currentLedger = ledger.currentLedger;

        if (currentLedger != null && ledgerId == currentLedger.getId()) {
            // Current writing ledger is not in the cache (since we don't want
            // it to be automatically evicted), and we cannot use 2 different
            // ledger handles (read & write)for the same ledger.
            internalReadFromLedger(currentLedger);
        } else {
            LedgerInfo ledgerInfo = ledger.ledgers.get(ledgerId);
            if (ledgerInfo == null || ledgerInfo.getEntries() == 0) {
                updateReadPosition(getNextLedgerPosition(ledgerId));
                checkReadCompletion();
                return;
            }

            ledger.getLedgerHandle(ledgerId).thenAccept(this::internalReadFromLedger)
                    .exceptionally(ex -> {
                        ledger.log.error().attr("position", readPosition).exceptionMessage(ex)
                                .log("Error opening ledger for reading");
                        readEntriesFailed(ManagedLedgerException.getManagedLedgerException(
                                FutureUtil.unwrapCompletionException(ex)), null);
                        return null;
                    });
        }
    }

    private void internalReadFromLedger(ReadHandle readHandle) {
        long firstEntry = readPosition.getEntryId();
        long lastEntryInLedger;

        Position lastPosition = ledger.lastConfirmedEntry;

        if (readHandle.getId() == lastPosition.getLedgerId()) {
            // For the current ledger, we only give read visibility to the last entry we have received a confirmation in
            // the managed ledger layer
            lastEntryInLedger = lastPosition.getEntryId();
        } else {
            // For other ledgers, already closed the BK lastAddConfirmed is appropriate
            lastEntryInLedger = readHandle.getLastAddConfirmed();
        }

        if (readHandle.getId() == maxPosition.getLedgerId()) {
            lastEntryInLedger = min(maxPosition.getEntryId(), lastEntryInLedger);
        }

        if (firstEntry > lastEntryInLedger) {
            log.debug().attr("ledgerId", readHandle.getId())
                    .attr("lastEntry", lastEntryInLedger)
                    .attr("readEntry", firstEntry)
                    .log("No more messages to read from ledger");

            LedgerHandle currentLedger = ledger.currentLedger;
            if (currentLedger == null || readHandle.getId() != currentLedger.getId()) {
                updateReadPosition(getNextLedgerPosition(readHandle.getId()));
            } else {
                updateReadPosition(readPosition);
            }

            checkReadCompletion();
            return;
        }

        long lastEntry = min(firstEntry + getNumberOfEntriesToRead() - 1, lastEntryInLedger);

        log.debug().attr("ledgerId", readHandle.getId())
                .attr("firstEntry", firstEntry)
                .attr("lastEntry", lastEntry)
                .log("Reading entries from ledger");
        if (populateCache) {
            ledger.asyncReadEntryForRandomReader(readHandle, firstEntry, lastEntry,
                    ledger::getActiveCachePopulatingRandomReaderCount, this);
        } else {
            // Cache-bypass: do not write back misses (expectedReadCount = 0).
            ledger.asyncReadEntryForRandomReader(readHandle, firstEntry, lastEntry, () -> 0, this);
        }
    }

    private Position getNextLedgerPosition(long ledgerId) {
        Long nextLedgerId = ledger.ledgers.ceilingKey(ledgerId + 1);
        return PositionFactory.create(nextLedgerId != null ? nextLedgerId : ledgerId + 1, 0);
    }

    @Override
    public void readEntriesComplete(List<Entry> returnedEntries, Object ctx) {
        if (!populateCache) {
            returnedEntries.forEach(entry -> ((EntryImpl) entry).setDecreaseReadCountOnRelease(false));
        }
        if (terminal.get()) {
            returnedEntries.forEach(Entry::release);
            return;
        }
        try {
            internalReadEntriesComplete(returnedEntries);
        } catch (Throwable throwable) {
            log.error().attr("op", this).exception(throwable)
                    .log("Fallback to readEntriesFailed for exception in readEntriesComplete");
            readEntriesFailed(ManagedLedgerException.getManagedLedgerException(throwable), ctx);
        }
    }

    private void internalReadEntriesComplete(List<Entry> returnedEntries) {
        if (returnedEntries.isEmpty()) {
            log.warn().attr("op", this).log("Read no entries unexpectedly");
            checkReadCompletion();
            return;
        }

        log.debug()
                .attr("managedLedger", ledger.getName())
                .attr("batchSize", returnedEntries.size())
                .attr("cumulativeSize", entries.size())
                .attr("requestedCount", count)
                .log("Read entries succeeded");

        entries.addAll(returnedEntries);
        updateReadPosition(returnedEntries.get(returnedEntries.size() - 1).getPosition().getNext());
        checkReadCompletion();
    }

    @Override
    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
        try {
            internalReadEntriesFailed(exception);
        } catch (Throwable throwable) {
            failPromise(ManagedLedgerException.getManagedLedgerException(throwable));
        }
    }

    private void internalReadEntriesFailed(ManagedLedgerException exception) {
        if (!entries.isEmpty()) {
            log.warn()
                    .attr("managedLedger", ledger.getName())
                    .attr("readPosition", readPosition)
                    .attr("partialEntries", entries.size())
                    .exception(exception)
                    .log("Read failed after partial result");
            completeWithEntries();
            return;
        }

        log.warn()
                .attr("managedLedger", ledger.getName())
                .attr("readPosition", readPosition)
                .exception(exception)
                .log("Read failed from ledger");
        failPromise(exception);
    }

    private void updateReadPosition(Position newReadPosition) {
        nextReadPosition = newReadPosition;
    }

    private void checkReadCompletion() {
        if (entries.size() < count
                && ledger.hasMoreEntries(nextReadPosition)
                && maxPosition.compareTo(readPosition) > 0) {
            ledger.getExecutor().execute(() -> {
                readPosition = ledger.startReadOperationOnLedger(nextReadPosition);
                readEntries();
            });
        } else {
            completeWithEntries();
        }
    }

    private void completeWithEntries() {
        if (terminal.compareAndSet(false, true) && !promise.complete(entries)) {
            // CompletableFuture is mutable: cancellation or external completion must not leak owned entries.
            entries.forEach(Entry::release);
        }
    }

    private void failPromise(ManagedLedgerException exception) {
        if (terminal.compareAndSet(false, true)) {
            promise.completeExceptionally(exception);
        }
    }

    private int getNumberOfEntriesToRead() {
        return count - entries.size();
    }


    @Override
    public String toString() {
        return ledger.getName() + "{ readPosition: " + readPosition + ", maxPosition: " + maxPosition
                + ", nextReadPosition: " + nextReadPosition + ", entries count: " + entries.size()
                + ", count: " + count + " }";
    }
}
