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

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.collect.Range;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.AsyncCallbacks.FindEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionBound;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.common.util.FutureUtil;

@CustomLog
class OpFindNewest implements ReadEntryCallback {
    private final ManagedCursorImpl cursor;
    private final ManagedLedgerImpl ledger;
    private final Position startPosition;
    private final FindEntryCallback callback;
    private final Predicate<Entry> condition;
    private final Object ctx;

    enum State {
        checkFirst, checkLast, searching
    }

    Position searchPosition;
    long min;
    long max;
    long mid;
    Position lastMatchedPosition = null;
    State state;

    public OpFindNewest(ManagedCursorImpl cursor, Position startPosition, Predicate<Entry> condition,
            long numberOfEntries, FindEntryCallback callback, Object ctx) {
        this.cursor = cursor;
        this.ledger = cursor.ledger;
        this.startPosition = startPosition;
        this.callback = new OnceFindEntryCallback(callback);
        this.condition = condition;
        this.ctx = ctx;

        this.min = 0;
        this.max = numberOfEntries;
        this.mid = mid();

        this.searchPosition = startPosition;
        this.state = State.checkFirst;
    }

    public OpFindNewest(ManagedLedgerImpl ledger, Position startPosition, Predicate<Entry> condition,
                        long numberOfEntries, FindEntryCallback callback, Object ctx) {
        this.cursor = null;
        this.ledger = ledger;
        this.startPosition = startPosition;
        this.callback = new OnceFindEntryCallback(callback);
        this.condition = condition;
        this.ctx = ctx;

        this.min = 0;
        this.max = numberOfEntries;

        this.searchPosition = startPosition;
        this.state = State.checkFirst;
    }

    @Override
    public void readEntryComplete(Entry entry, Object ctx) {
        final Position position = entry.getPosition();
        switch (state) {
        case checkFirst:
            if (!condition.test(entry)) {
                // If no entry is found that matches the condition, it is expected to pass null to the callback.
                // Otherwise, a message before the expiration date will be deleted due to message TTL.
                // cf. https://github.com/apache/pulsar/issues/5579
                callback.findEntryComplete(null, OpFindNewest.this.ctx);
                return;
            } else {
                lastMatchedPosition = position;
                // check last entry
                state = State.checkLast;
                searchPosition = ledger.getPositionAfterN(searchPosition, max, PositionBound.startExcluded);
                Position lastPosition = ledger.getLastPosition();
                if (lastPosition.compareTo(searchPosition) < 0) {
                    log.debug().attr("firstPosition", position)
                            .attr("expectedLastPosition", searchPosition)
                            .attr("actualLastPosition", lastPosition)
                            .log("First position matches, but moving to lastPos");
                    searchPosition = lastPosition;
                }
                find();
            }
            break;
        case checkLast:
            if (condition.test(entry)) {
                callback.findEntryComplete(position, OpFindNewest.this.ctx);
                return;
            } else {
                // start binary search
                state = State.searching;
                this.mid = mid();
                moveToMidAndFind();
            }
            break;
        case searching:
            if (condition.test(entry)) {
                // mid - last
                lastMatchedPosition = position;
                min = mid;
            } else {
                // start - mid
                max = mid - 1;
            }
            this.mid = mid();

            if (max <= min) {
                callback.findEntryComplete(lastMatchedPosition, OpFindNewest.this.ctx);
                return;
            }
            moveToMidAndFind();
        }
    }

    @Override
    public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
        if (exception instanceof ManagedLedgerException.NonRecoverableLedgerException
            && ledger.getConfig().isAutoSkipNonRecoverableData()) {
            try {
                log.info().attr("ledgerName", ledger.getName())
                        .attr("searchPosition", searchPosition)
                        .attr("state", state)
                        .log("Ledger is not recoverable, skip non-recoverable data");
                checkArgument(state == State.checkFirst || state == State.checkLast || state == State.searching);
                if (state == State.checkFirst) {
                    // If we failed to read the first entry, try next valid position
                    Position nextPosition = findNextValidPosition(searchPosition, exception);
                    if (nextPosition != null && nextPosition.getEntryId() != -1) {
                        long numberOfEntries =
                            ledger.getNumberOfEntries(Range.closedOpen(searchPosition, nextPosition));
                        searchPosition = nextPosition;
                        min += numberOfEntries;
                        find();
                        return;
                    }
                } else if (state == State.checkLast) {
                    Position prevPosition = findPreviousValidPosition(searchPosition, exception);
                    if (prevPosition != null && prevPosition.getEntryId() != -1) {
                        long numberOfEntries =
                            ledger.getNumberOfEntries(Range.openClosed(prevPosition, searchPosition));
                        searchPosition = prevPosition;
                        max -= numberOfEntries;
                        find();
                        return;
                    }
                } else if (state == State.searching) {
                    // In searching state, if we failed to read the mid entry, try next valid position
                    Position nextPosition = findNextValidPosition(searchPosition, exception);
                    if (nextPosition != null && nextPosition.getEntryId() != -1) {
                        searchPosition = nextPosition;
                        find();
                        return;
                    } else {
                        // If we can't find next valid position, try previous valid position
                        Position prevPosition = findPreviousValidPosition(searchPosition, exception);
                        if (prevPosition != null && prevPosition.getEntryId() != -1) {
                            searchPosition = prevPosition;
                            find();
                            return;
                        }
                    }
                }

                // If don't find any entry, return the last matched position
                log.warn().attr("ledgerName", ledger.getName())
                        .attr("lastMatchedPosition", lastMatchedPosition)
                        .log("Failed to find next valid entry. Returning last matched position");
                callback.findEntryComplete(lastMatchedPosition, OpFindNewest.this.ctx);
                return;
            } catch (Exception e) {
                callback.findEntryFailed(
                    new ManagedLedgerException("Failed to skip non-recoverable data during search position", e),
                    Optional.ofNullable(searchPosition), OpFindNewest.this.ctx);
                return;
            }
        }

        callback.findEntryFailed(exception, Optional.ofNullable(searchPosition), OpFindNewest.this.ctx);
    }

    private Position findPreviousValidPosition(Position searchPosition, ManagedLedgerException exception) {
        Position prevPosition;
        if (exception instanceof ManagedLedgerException.LedgerNotExistException) {
            prevPosition =
                ledger.getPreviousPosition(PositionFactory.create(searchPosition.getLedgerId(), -1L));
        } else {
            prevPosition = ledger.getPreviousPosition(searchPosition);
        }
        if (prevPosition.getEntryId() != -1) {
            var minPosition = ledger.getPositionAfterN(startPosition, min, PositionBound.startExcluded);
            if (minPosition.compareTo(prevPosition) > 0) {
                // If the previous position is out of the min position, an invalid position is returned
                prevPosition = null;
            }
        }
        return prevPosition;
    }

    private Position findNextValidPosition(Position searchPosition, Exception exception) {
        Position nextPosition = null;
        if (exception instanceof ManagedLedgerException.LedgerNotExistException) {
            Long nextLedgerId = ledger.getNextValidLedger(searchPosition.getLedgerId());
            if (nextLedgerId != null) {
                Boolean nonEmptyLedger = ledger.getOptionalLedgerInfo(nextLedgerId)
                    .map(ledgerInfo -> ledgerInfo.getEntries() > 0)
                    .orElse(false);
                if (nonEmptyLedger) {
                    nextPosition = PositionFactory.create(nextLedgerId, 0);
                }
            }
        } else {
            nextPosition = ledger.getNextValidPosition(searchPosition);
        }

        if (nextPosition != null) {
            var maxPosition = ledger.getPositionAfterN(startPosition, max, PositionBound.startExcluded);
            if (maxPosition.compareTo(nextPosition) < 0) {
                // If the next position is out of the max position, an invalid position is returned
                nextPosition = null;
            }
        }
        return nextPosition;
    }

    /**
     * Find the largest entry that matches the given predicate.
     */
    public void find() {
        if (hasMoreEntries(searchPosition)) {
            ledger.asyncReadEntry(searchPosition, this, null);
        } else {
            callback.findEntryComplete(lastMatchedPosition, OpFindNewest.this.ctx);
        }
    }

    /**
     * Moves the search position to the entry at {@link #mid}, or to a cheaper entry of the same ledger close to it,
     * and reads it.
     *
     * <p>Reading an entry of an offloaded ledger requires scanning the offloaded data from the nearest entry whose
     * location is known, e.g. the start of its data block, which can take many reads from the tiered storage. The
     * nearest such entries below and above {@link #mid} are probed instead when they are close enough to it: any
     * probe within the search range keeps the binary search correct, and staying in the middle half of the range
     * between {@link #min} (excluded) and {@link #max} keeps discarding at least a quarter of it with each probe.
     */
    private void moveToMidAndFind() {
        Position midPosition = ledger.getPositionAfterN(startPosition, mid, PositionBound.startExcluded);
        if (!hasMoreEntries(midPosition)) {
            // Completes the search without reading, and without opening the read handle of the ledger
            searchPosition = midPosition;
            find();
            return;
        }
        CompletableFuture<long[]> indexedEntryIdsFuture = ledger.getIndexedEntryIdsAround(midPosition);
        if (indexedEntryIdsFuture.isDone()) {
            // Keep reading on the current thread, as before, when the read handle is already opened
            findIndexedEntryOrMid(midPosition, indexedEntryIdsFuture);
        } else {
            // The outcome is read from the future itself, the same way whether it completed before or after
            indexedEntryIdsFuture.whenComplete((ignoredIndexedEntryIds, ignoredException) -> {
                try {
                    findIndexedEntryOrMid(midPosition, indexedEntryIdsFuture);
                } catch (Throwable t) {
                    // Would be lost in the future otherwise. Ignored if the search already completed, e.g. when the
                    // failure came from the callback itself.
                    callback.findEntryFailed(ManagedLedgerException.getManagedLedgerException(t),
                            Optional.ofNullable(searchPosition), OpFindNewest.this.ctx);
                }
            });
        }
    }

    private void findIndexedEntryOrMid(Position midPosition, CompletableFuture<long[]> indexedEntryIdsFuture) {
        searchPosition = midPosition;
        long[] indexedEntryIds;
        try {
            indexedEntryIds = indexedEntryIdsFuture.getNow(null);
        } catch (CompletionException | CancellationException e) {
            // Opening the read handle of the ledger failed, as reading the entry would, so handle it the same way
            Throwable cause = FutureUtil.unwrapCompletionException(e);
            log.error().attr("position", midPosition).exceptionMessage(cause)
                    .log("Error opening ledger for reading at position");
            readEntryFailed(ManagedLedgerException.getManagedLedgerException(cause), null);
            return;
        }
        if (indexedEntryIds != null) {
            moveToIndexedEntry(midPosition, indexedEntryIds[0], indexedEntryIds[1]);
        }
        find();
    }

    private void moveToIndexedEntry(Position midPosition, long floorEntryId, long ceilingEntryId) {
        if (mid >= max) {
            // "midPosition" may have been clamped to the last position, so it may not be the entry at "mid", which
            // the shifts below assume. Below "max", it is always the entry at "mid".
            return;
        }
        long midEntryId = midPosition.getEntryId();
        // Entries of a ledger are contiguous, so the offset from the start position shifts by the same amount
        long lowestMid = Math.max(min + 1, min + (mid - min) / 2);
        long highestMid = mid + (max - mid) / 2;
        long bestMid = mid;
        long bestEntryId = midEntryId;
        long bestDistance = Long.MAX_VALUE;
        if (floorEntryId >= 0 && mid - (midEntryId - floorEntryId) >= lowestMid) {
            bestDistance = midEntryId - floorEntryId;
            bestMid = mid - bestDistance;
            bestEntryId = floorEntryId;
        }
        if (ceilingEntryId > midEntryId && mid + (ceilingEntryId - midEntryId) <= highestMid
                && ceilingEntryId - midEntryId < bestDistance) {
            bestMid = mid + (ceilingEntryId - midEntryId);
            bestEntryId = ceilingEntryId;
        }
        if (bestEntryId == midEntryId) {
            return;
        }
        mid = bestMid;
        searchPosition = PositionFactory.create(midPosition.getLedgerId(), bestEntryId);
    }

    private boolean hasMoreEntries(Position position) {
        return cursor != null ? cursor.hasMoreEntries(position) : ledger.hasMoreEntries(position);
    }

    private long mid() {
        return min + Math.max((max - min) / 2, 1);
    }

    /**
     * Calls the search callback at most once, so that a failure raised after the search completed, e.g. by the
     * callback itself, is not reported as a second outcome.
     */
    private static final class OnceFindEntryCallback implements FindEntryCallback {
        private final FindEntryCallback delegate;
        private final AtomicBoolean called = new AtomicBoolean();

        OnceFindEntryCallback(FindEntryCallback delegate) {
            this.delegate = delegate;
        }

        @Override
        public void findEntryComplete(Position position, Object ctx) {
            if (called.compareAndSet(false, true)) {
                delegate.findEntryComplete(position, ctx);
            } else {
                log.debug().attr("position", position).log("Ignoring the completion of an already completed search");
            }
        }

        @Override
        public void findEntryFailed(ManagedLedgerException exception, Optional<Position> failedReadPosition,
                                    Object ctx) {
            if (called.compareAndSet(false, true)) {
                delegate.findEntryFailed(exception, failedReadPosition, ctx);
            } else {
                log.debug().exception(exception).log("Ignoring the failure of an already completed search");
            }
        }
    }
}
