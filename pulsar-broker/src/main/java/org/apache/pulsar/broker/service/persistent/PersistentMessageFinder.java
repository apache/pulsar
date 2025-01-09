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
package org.apache.pulsar.broker.service.persistent;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * given a timestamp find the first message (position) (published) at or before the timestamp.
 */
public class PersistentMessageFinder implements AsyncCallbacks.FindEntryCallback {
    private final ManagedCursor cursor;
    private final String subName;
    private final String topicName;
    private long timestamp = 0;

    private static final int FALSE = 0;
    private static final int TRUE = 1;
    @SuppressWarnings("unused")
    private volatile int messageFindInProgress = FALSE;
    private static final AtomicIntegerFieldUpdater<PersistentMessageFinder> messageFindInProgressUpdater =
            AtomicIntegerFieldUpdater
                    .newUpdater(PersistentMessageFinder.class, "messageFindInProgress");

    public PersistentMessageFinder(String topicName, ManagedCursor cursor) {
        this.topicName = topicName;
        this.cursor = cursor;
        this.subName = Codec.decode(cursor.getName());
    }

    public void findMessages(final long timestamp, AsyncCallbacks.FindEntryCallback callback) {
        if (messageFindInProgressUpdater.compareAndSet(this, FALSE, TRUE)) {
            this.timestamp = timestamp;
            if (log.isDebugEnabled()) {
                log.debug("[{}] Starting message position find at timestamp {}", subName, timestamp);
            }
            Pair<Position, Position> range =
                    getFindPositionRange(cursor.getManagedLedger().getLedgersInfo().values(),
                            cursor.getManagedLedger().getLastConfirmedEntry(), timestamp);
            cursor.asyncFindNewestMatching(ManagedCursor.FindPositionConstraint.SearchAllAvailableEntries, entry -> {
                try {
                    long entryTimestamp = Commands.getEntryTimestamp(entry.getDataBuffer());
                    return MessageImpl.isEntryPublishedEarlierThan(entryTimestamp, timestamp);
                } catch (Exception e) {
                    log.error("[{}][{}] Error deserializing message for message position find", topicName, subName, e);
                } finally {
                    entry.release();
                }
                return false;
            }, range.getLeft(), range.getRight(), this, callback, true);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Ignore message position find scheduled task, last find is still running", topicName,
                        subName);
            }
            callback.findEntryFailed(
                    new ManagedLedgerException.ConcurrentFindCursorPositionException("last find is still running"),
                    Optional.empty(), null);
        }
    }

    public static Pair<Position, Position> getFindPositionRange(
            Iterable<MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgerInfos, Position lastConfirmedEntry,
            long timestamp) {
        Position start = null;
        Position end = null;

        long previousCloseTimestamp = -1;
        for (MLDataFormats.ManagedLedgerInfo.LedgerInfo info : ledgerInfos) {
            if (!info.hasTimestamp()) {
                // unexpected case, don't set start and end
                return Pair.of(null, null);
            }
            long closeTimestamp = info.getTimestamp();
            // For an open ledger, closeTimestamp is 0
            if (closeTimestamp == 0) {
                // if the previous ledger close timestamp is less than the timestamp, then start from the first entry
                // when there are confirmed entries in the ledger
                if (previousCloseTimestamp < timestamp) {
                    Position firstPositionInLedger = PositionFactory.create(info.getLedgerId(), 0);
                    if (lastConfirmedEntry != null
                            && lastConfirmedEntry.compareTo(firstPositionInLedger) >= 0) {
                        start = firstPositionInLedger;
                    } else {
                        start = lastConfirmedEntry;
                    }
                }
                return Pair.of(start, null);
            }
            if (closeTimestamp <= timestamp) {
                start = PositionFactory.create(info.getLedgerId(), 0);
            } else {
                // If the close timestamp is greater than the timestamp
                end = PositionFactory.create(info.getLedgerId(), info.getEntries() - 1);
                break;
            }
            previousCloseTimestamp = closeTimestamp;
        }
        return Pair.of(start, end);
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentMessageFinder.class);

    @Override
    public void findEntryComplete(Position position, Object ctx) {
        checkArgument(ctx instanceof AsyncCallbacks.FindEntryCallback);
        AsyncCallbacks.FindEntryCallback callback = (AsyncCallbacks.FindEntryCallback) ctx;
        if (position != null) {
            log.info("[{}][{}] Found position {} closest to provided timestamp {}", topicName, subName, position,
                    timestamp);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] No position found closest to provided timestamp {}", topicName, subName, timestamp);
            }
        }
        messageFindInProgress = FALSE;
        callback.findEntryComplete(position, null);
    }

    @Override
    public void findEntryFailed(ManagedLedgerException exception, Optional<Position> failedReadPosition, Object ctx) {
        checkArgument(ctx instanceof AsyncCallbacks.FindEntryCallback);
        AsyncCallbacks.FindEntryCallback callback = (AsyncCallbacks.FindEntryCallback) ctx;
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] message position find operation failed for provided timestamp {}", topicName, subName,
                    timestamp, exception);
        }
        messageFindInProgress = FALSE;
        callback.findEntryFailed(exception, failedReadPosition, null);
    }
}
