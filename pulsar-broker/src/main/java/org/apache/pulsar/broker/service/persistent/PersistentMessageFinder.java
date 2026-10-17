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
import com.google.common.annotations.VisibleForTesting;
import io.github.merlimat.slog.Logger;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.stats.OpenTelemetryMessageFinderStats;
import org.apache.pulsar.broker.stats.OpenTelemetryMessageFinderStats.EntryStorage;
import org.apache.pulsar.broker.stats.OpenTelemetryMessageFinderStats.FindReason;
import org.apache.pulsar.broker.stats.OpenTelemetryMessageFinderStats.FindResult;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.util.Codec;

/**
 * given a timestamp find the first message (position) (published) at or before the timestamp.
 */
public class PersistentMessageFinder implements AsyncCallbacks.FindEntryCallback {

    private static final Logger LOG = Logger.get(PersistentMessageFinder.class);
    protected final Logger log;

    protected final ManagedCursor cursor;
    protected final String subName;
    protected final int ledgerCloseTimestampMaxClockSkewMillis;
    protected final String topicName;
    protected long timestamp = 0;
    private final OpenTelemetryMessageFinderStats stats;
    private final FindReason findReason;
    private volatile FindProgress currentFind;

    protected static final int FALSE = 0;
    protected static final int TRUE = 1;
    @SuppressWarnings("unused")
    protected volatile int messageFindInProgress = FALSE;
    protected static final AtomicIntegerFieldUpdater<PersistentMessageFinder> MESSAGE_FIND_IN_PROGRESS =
            AtomicIntegerFieldUpdater
                    .newUpdater(PersistentMessageFinder.class, "messageFindInProgress");

    @VisibleForTesting
    public PersistentMessageFinder(String topicName, ManagedCursor cursor, int ledgerCloseTimestampMaxClockSkewMillis) {
        this(topicName, cursor, ledgerCloseTimestampMaxClockSkewMillis, null, FindReason.SEEK);
    }

    public PersistentMessageFinder(String topicName, ManagedCursor cursor, int ledgerCloseTimestampMaxClockSkewMillis,
                                   OpenTelemetryMessageFinderStats stats, FindReason findReason) {
        this.topicName = topicName;
        this.cursor = cursor;
        this.subName = Codec.decode(cursor.getName());
        this.ledgerCloseTimestampMaxClockSkewMillis = ledgerCloseTimestampMaxClockSkewMillis;
        this.stats = stats;
        this.findReason = findReason;
        this.log = LOG.with()
                .attr("topic", topicName)
                .attr("subscription", subName)
                .build();
    }

    public void findMessages(final long timestamp, AsyncCallbacks.FindEntryCallback callback) {
        if (MESSAGE_FIND_IN_PROGRESS.compareAndSet(this, FALSE, TRUE)) {
            this.timestamp = timestamp;
            log.debug()
                    .attr("timestamp", timestamp)
                    .log("Starting message position find at timestamp");
            Pair<Position, Position> range =
                    getFindPositionRange(cursor.getManagedLedger().getLedgersInfo().values(),
                            cursor.getManagedLedger().getLastConfirmedEntry(), timestamp,
                            ledgerCloseTimestampMaxClockSkewMillis);
            FindProgress progress = new FindProgress(range.getLeft() != null || range.getRight() != null);
            currentFind = progress;
            cursor.asyncFindNewestMatching(ManagedCursor.FindPositionConstraint.SearchAllAvailableEntries, entry -> {
                try {
                    recordEntryRead(progress, entry.getLedgerId(), entry.getLength());
                    // Find the latest entry that is earlier than the target timestamp.
                    long entryTimestamp = entry.getEntryTimestamp();
                    return MessageImpl.isEntryPublishedEarlierThan(entryTimestamp, timestamp);
                } catch (Exception e) {
                    log.error()
                            .exception(e)
                            .log("Error deserializing message for message position find");
                } finally {
                    entry.release();
                }
                return false;
            }, range.getLeft(), range.getRight(), this, callback, true);
        } else {
            log.debug("Ignore message position find scheduled task, last find is still running");
            callback.findEntryFailed(
                    new ManagedLedgerException.ConcurrentFindCursorPositionException("last find is still running"),
                    Optional.empty(), null);
        }
    }

    /**
     * The range may be across multi ledgers:
     *   - start: the latest ledger that closed before {@param targetTimestamp}.
     *     - only the latest entry is useful.
     *   - end: the earliest ledger that is larger than the target timestamp.
     */
    @VisibleForTesting
    public static Pair<Position, Position> getFindPositionRange(Iterable<LedgerInfo> ledgerInfos,
                                                                Position lastConfirmedEntry, long targetTimestamp,
                                                                int ledgerCloseTimestampMaxClockSkewMillis) {
        if (ledgerCloseTimestampMaxClockSkewMillis < 0) {
            // this feature is disabled when the value is negative
            return Pair.of(null, null);
        }

        long targetTimestampMin = targetTimestamp - ledgerCloseTimestampMaxClockSkewMillis;
        long targetTimestampMax = targetTimestamp + ledgerCloseTimestampMaxClockSkewMillis;

        Position start = null;
        Position end = null;

        // We do not use binary search here:
        // Since "managedLedger.ledgers" os a map, we can hardly use a binary search except to copy items to an array,
        // which causes frequently young GC. And "collection.toArray()" also loops the collection once, which does not
        // benefit performance anymore.
        for (LedgerInfo info : ledgerInfos) {
            if (!info.hasTimestamp()) {
                // unexpected case, don't set start and end
                return Pair.of(null, null);
            }
            long closeTimestamp = info.getTimestamp();
            // For an open ledger, closeTimestamp is 0
            if (closeTimestamp == 0) {
                end = null;
                break;
            }
            if (closeTimestamp <= targetTimestampMin) {
                // Since we have "broker.conf -> managedLedgerCursorResetLedgerCloseTimestampMaxClockSkewMillis", which
                // already expanded the scope for searching, the entries before the latest one is not useful.
                start = PositionFactory.create(info.getLedgerId(), info.getEntries() - 1);
            } else if (closeTimestamp > targetTimestampMax) {
                // If the close timestamp is greater than the timestamp
                end = PositionFactory.create(info.getLedgerId(), info.getEntries() - 1);
                break;
            }
        }
        return Pair.of(start, end);
    }

    private void recordEntryRead(FindProgress progress, long ledgerId, int length) {
        // The storage the entry's ledger is read from: an offloaded ledger may still be read from BookKeeper,
        // depending on the offloaded read priority
        EntryStorage storage = cursor.getManagedLedger() instanceof ManagedLedgerImpl ml
                && ml.isReadFromOffloadedLedgerHandle(ledgerId) ? EntryStorage.OFFLOADED : EntryStorage.BOOKKEEPER;
        progress.entriesRead++;
        progress.bytesRead += length;
        if (storage == EntryStorage.OFFLOADED) {
            progress.offloadedEntriesRead++;
        }
        if (stats != null) {
            stats.recordEntryRead(findReason, storage, length);
        }
    }

    private long recordFindCompleted(FindResult result) {
        FindProgress progress = currentFind;
        currentFind = null;
        if (progress == null) {
            return 0;
        }
        long durationNanos = System.nanoTime() - progress.startNanos;
        if (stats != null) {
            stats.recordFindCompleted(findReason, result, durationNanos);
        }
        return durationNanos;
    }

    @Override
    public void findEntryComplete(Position position, Object ctx) {
        checkArgument(ctx instanceof AsyncCallbacks.FindEntryCallback);
        AsyncCallbacks.FindEntryCallback callback = (AsyncCallbacks.FindEntryCallback) ctx;
        FindProgress progress = currentFind;
        long durationNanos = recordFindCompleted(position != null ? FindResult.FOUND : FindResult.NOT_FOUND);
        if (position != null) {
            log.info()
                    .attr("position", position)
                    .attr("timestamp", timestamp)
                    .attr("reason", findReason)
                    .attr("durationMs", TimeUnit.NANOSECONDS.toMillis(durationNanos))
                    .attr("entriesRead", progress != null ? progress.entriesRead : 0)
                    .attr("offloadedEntriesRead", progress != null ? progress.offloadedEntriesRead : 0)
                    .attr("bytesRead", progress != null ? progress.bytesRead : 0)
                    .attr("rangeNarrowed", progress != null && progress.rangeNarrowed)
                    .log("Found position closest to provided timestamp");
        } else {
            log.debug()
                    .attr("timestamp", timestamp)
                    .attr("reason", findReason)
                    .attr("durationMs", TimeUnit.NANOSECONDS.toMillis(durationNanos))
                    .log("No position found closest to provided timestamp");
        }
        messageFindInProgress = FALSE;
        callback.findEntryComplete(position, null);
    }

    @Override
    public void findEntryFailed(ManagedLedgerException exception, Optional<Position> failedReadPosition, Object ctx) {
        checkArgument(ctx instanceof AsyncCallbacks.FindEntryCallback);
        AsyncCallbacks.FindEntryCallback callback = (AsyncCallbacks.FindEntryCallback) ctx;
        long durationNanos = recordFindCompleted(FindResult.FAILURE);
        log.debug()
                .attr("timestamp", timestamp)
                .attr("reason", findReason)
                .attr("durationMs", TimeUnit.NANOSECONDS.toMillis(durationNanos))
                .exception(exception)
                .log("Message position find operation failed for provided timestamp");
        messageFindInProgress = FALSE;
        callback.findEntryFailed(exception, failedReadPosition, null);
    }

    /**
     * Progress of a single find. Entry reads of a find are strictly sequential: the next read is only issued from
     * the completion of the previous one, so these fields are never updated concurrently.
     */
    private static final class FindProgress {
        final long startNanos = System.nanoTime();
        final boolean rangeNarrowed;
        long entriesRead;
        long offloadedEntriesRead;
        long bytesRead;

        FindProgress(boolean rangeNarrowed) {
            this.rangeNarrowed = rangeNarrowed;
        }
    }
}
