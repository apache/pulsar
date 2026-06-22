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
import java.util.concurrent.RejectedExecutionException;
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

@CustomLog
class OpReadEntries implements ReadEntriesCallback {

    private final ManagedLedgerImpl ledger;
    private final Position maxPosition;
    private final int count;
    private final CompletableFuture<List<Entry>> promise;
    private final List<Entry> entries;

    private Position readPosition;
    private Position nextReadPosition;

    static OpReadEntries create(ManagedLedgerImpl ledger, Position readPosition, int count,
                                Position maxPosition, CompletableFuture<List<Entry>> promise) {
        return new OpReadEntries(ledger, readPosition, count, maxPosition, promise);
    }

    private OpReadEntries(ManagedLedgerImpl ledger, Position readPosition, int count,
                          Position maxPosition, CompletableFuture<List<Entry>> promise) {
        this.ledger = ledger;
        this.readPosition = readPosition;
        this.count = count;
        this.maxPosition = maxPosition;
        this.promise = promise;
        this.entries = new ArrayList<>(Math.min(count, 16));
        this.nextReadPosition = readPosition;
    }

    void readEntries() {
        State state = ManagedLedgerImpl.STATE_UPDATER.get(ledger);
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
            internalReadFromLedger(currentLedger);
            return;
        }

        LedgerInfo ledgerInfo = ledger.ledgers.get(ledgerId);
        if (ledgerInfo == null || ledgerInfo.getEntries() == 0) {
            updateReadPosition(getNextLedgerPosition(ledgerId));
            checkReadCompletion();
            return;
        }

        ledger.getLedgerHandle(ledgerId)
                .thenAccept(this::internalReadFromLedger)
                .exceptionally(ex -> {
                    log.error().attr("position", readPosition).exceptionMessage(ex)
                            .log("Error opening ledger for reading");
                    readEntriesFailed(ManagedLedgerException.getManagedLedgerException(ex.getCause()), null);
                    return null;
                });
    }

    private void internalReadFromLedger(ReadHandle readHandle) {
        long firstEntry = readPosition.getEntryId();
        Position lastPosition = ledger.lastConfirmedEntry;
        long lastEntryInLedger;
        if (readHandle.getId() == lastPosition.getLedgerId()) {
            lastEntryInLedger = lastPosition.getEntryId();
        } else {
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
        ledger.asyncReadEntry(readHandle, firstEntry, lastEntry, this, null);
    }

    private Position getNextLedgerPosition(long ledgerId) {
        Long nextLedgerId = ledger.ledgers.ceilingKey(ledgerId + 1);
        return PositionFactory.create(nextLedgerId != null ? nextLedgerId : ledgerId + 1, 0);
    }

    @Override
    public void readEntriesComplete(List<Entry> returnedEntries, Object ctx) {
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
            // Cache returned nothing for a range validated as non-empty. Retrying would spin on the
            // same range; terminate with whatever we have so far (contract permits returning fewer).
            log.warn().attr("op", this).log("Read no entries unexpectedly; completing read");
            completeSuccessfully();
            return;
        }

        log.debug().attr("managedLedger", ledger.getName())
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
            completeExceptionally(ManagedLedgerException.getManagedLedgerException(throwable));
        }
    }

    private void internalReadEntriesFailed(ManagedLedgerException exception) {
        if (!entries.isEmpty()) {
            completeSuccessfully();
            return;
        }
        if (ledger.getConfig().isAutoSkipNonRecoverableData()
                && exception instanceof ManagedLedgerException.NonRecoverableLedgerException) {
            Position skippedTo = exception instanceof ManagedLedgerException.LedgerNotExistException
                    ? nextLedgerStart(readPosition.getLedgerId())
                    : ledger.getValidPositionAfterSkippedEntries(readPosition, count);
            if (skippedTo == null) {
                log.warn().attr("managedLedger", ledger.getName())
                        .attr("readPosition", readPosition)
                        .exceptionMessage(exception)
                        .log("Read failed; cannot find a position to skip to");
                completeExceptionally(exception);
                return;
            }
            log.warn().attr("managedLedger", ledger.getName())
                    .attr("readPosition", readPosition)
                    .attr("nextReadPosition", skippedTo)
                    .exceptionMessage(exception)
                    .log("Read failed; skipping non-recoverable data and continuing");
            updateReadPosition(skippedTo);
            checkReadCompletion();
            return;
        }
        log.warn().attr("managedLedger", ledger.getName())
                .attr("readPosition", readPosition)
                .exception(exception)
                .log("Read failed from ledger");
        completeExceptionally(exception);
    }

    private Position nextLedgerStart(long ledgerId) {
        Long next = ledger.getNextValidLedger(ledgerId);
        return next != null ? PositionFactory.create(next, 0) : null;
    }

    private void updateReadPosition(Position newPosition) {
        nextReadPosition = newPosition;
    }

    private void checkReadCompletion() {
        if (entries.size() < count
                && ledger.hasMoreEntries(nextReadPosition)
                && maxPosition.compareTo(readPosition) > 0) {
            executeOnLedgerExecutor(() -> {
                readPosition = ledger.startReadOperationOnLedger(nextReadPosition);
                readEntries();
            });
        } else {
            completeSuccessfully();
        }
    }

    // Route completion through the ML executor so caller callbacks don't run on BK I/O threads.
    // If the executor is shut down (ML closing), fall back to inline completion to avoid leaking
    // the promise and accumulated ByteBufs.
    private void completeSuccessfully() {
        executeOnLedgerExecutor(() -> promise.complete(entries));
    }

    private void completeExceptionally(Throwable cause) {
        executeOnLedgerExecutor(() -> promise.completeExceptionally(cause));
    }

    private void executeOnLedgerExecutor(Runnable task) {
        try {
            ledger.getExecutor().execute(task);
        } catch (RejectedExecutionException e) {
            log.warn().attr("managedLedger", ledger.getName()).exceptionMessage(e)
                    .log("ML executor rejected task; running inline");
            task.run();
        }
    }

    private int getNumberOfEntriesToRead() {
        return count - entries.size();
    }

    @Override
    public String toString() {
        return ledger == null ? "(null)" : ledger.getName() + "{ readPosition: " + readPosition
                + ", maxPosition: " + maxPosition
                + ", nextReadPosition: " + nextReadPosition
                + ", entries: " + (entries != null ? entries.size() : 0)
                + ", count: " + count + " }";
    }
}
