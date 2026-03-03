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
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks.FindEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionBound;
import org.apache.bookkeeper.mledger.PositionFactory;

@Slf4j
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
        this.callback = callback;
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
        this.callback = callback;
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
                    if (log.isDebugEnabled()) {
                        log.debug("first position {} matches, last should be {}, but moving to lastPos {}", position,
                                searchPosition, lastPosition);
                    }
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
                searchPosition = ledger.getPositionAfterN(startPosition, this.mid, PositionBound.startExcluded);
                find();
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
            searchPosition = ledger.getPositionAfterN(startPosition, this.mid, PositionBound.startExcluded);
            find();
        }
    }

    @Override
    public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
        if (exception instanceof ManagedLedgerException.NonRecoverableLedgerException
            && ledger.getConfig().isAutoSkipNonRecoverableData()) {
            try {
                log.info("[{}] Ledger {} is not recoverable, skip non-recoverable data, state:{}", ledger.getName(),
                    searchPosition, state);
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
                log.warn("[{}] Failed to find next valid entry. Returning last matched position: {}", ledger.getName(),
                    lastMatchedPosition);
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
        if (cursor != null ? cursor.hasMoreEntries(searchPosition) : ledger.hasMoreEntries(searchPosition)) {
            ledger.asyncReadEntry(searchPosition, this, null);
        } else {
            callback.findEntryComplete(lastMatchedPosition, OpFindNewest.this.ctx);
        }
    }

    private long mid() {
        return min + Math.max((max - min) / 2, 1);
    }
}
