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
package org.apache.pulsar.compaction;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.mledger.util.ManagedLedgerUtils.readEntries;
import static org.apache.bookkeeper.mledger.util.ManagedLedgerUtils.readEntriesWithSkipOrWait;
import com.google.common.annotations.Beta;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.commons.collections4.CollectionUtils;
import org.jspecify.annotations.Nullable;

public class CompactedTopicUtils {

    @Beta
    public static CompletableFuture<List<Entry>> asyncReadCompactedEntries(
            TopicCompactionService topicCompactionService, ManagedCursor cursor, int maxEntries,
            long bytesToRead, Position maxReadPosition, boolean readFromEarliest, boolean wait) {
        return asyncReadCompactedEntries(topicCompactionService, cursor, maxEntries, bytesToRead, maxReadPosition,
                readFromEarliest, wait, null, null);
    }

    /**
     * Read the next entries of a compacted-topic reader.
     *
     * <p>Positions up to the compaction horizon are read from the compacted ledger, later positions from the
     * original ledger. A reader that has already moved on to the original ledger must stay there even when a later
     * compaction moves the horizon past its read position, or it would skip the messages compaction removed since,
     * tombstones included.
     *
     * @param originalLedgerReadPosition the position from which this reader has been reading the original ledger,
     *                                   or {@code null}. Positions at or after it are read from the original ledger
     *                                   while it is still retained there.
     * @param onOriginalLedgerRead called with the position from which the reader moves on to the original ledger,
     *                             when a read finishes the compacted snapshot or reads the original ledger; may be
     *                             {@code null}
     */
    @Beta
    public static CompletableFuture<List<Entry>> asyncReadCompactedEntries(
            TopicCompactionService topicCompactionService, ManagedCursor cursor, int maxEntries,
            long bytesToRead, Position maxReadPosition, boolean readFromEarliest, boolean wait,
            @Nullable Position originalLedgerReadPosition, @Nullable Consumer<Position> onOriginalLedgerRead) {
        Objects.requireNonNull(topicCompactionService);
        Objects.requireNonNull(cursor);
        checkArgument(maxEntries > 0);

        final Position readPosition;
        if (readFromEarliest) {
            readPosition = PositionFactory.EARLIEST;
        } else {
            readPosition = cursor.getReadPosition();
        }

        // Once retention has trimmed the original ledger at that position, only the compacted ledger has the data.
        final Position originalLedgerLimit = originalLedgerReadPosition != null
                && isRetained(cursor, originalLedgerReadPosition) ? originalLedgerReadPosition : null;
        if (originalLedgerLimit != null && readPosition.compareTo(originalLedgerLimit) >= 0) {
            return readOriginalLedger(cursor, maxEntries, bytesToRead, maxReadPosition, wait);
        }

        CompletableFuture<Position> lastCompactedPositionFuture = topicCompactionService.getLastCompactedPosition();

        return lastCompactedPositionFuture.thenCompose(lastCompactedPosition -> {
            if (lastCompactedPosition == null
                    || readPosition.compareTo(
                    lastCompactedPosition.getLedgerId(), lastCompactedPosition.getEntryId()) > 0) {
                if (onOriginalLedgerRead != null) {
                    onOriginalLedgerRead.accept(cursor.getReadPosition());
                }
                return readOriginalLedger(cursor, maxEntries, bytesToRead, maxReadPosition, wait);
            }

            int numberOfEntriesToRead = cursor.applyMaxSizeCap(maxEntries, bytesToRead);

            return topicCompactionService.readCompactedEntries(readPosition, numberOfEntriesToRead)
                    .thenApply(entries -> {
                        if (CollectionUtils.isEmpty(entries)) {
                            Position seekToPosition = lastCompactedPosition.getNext();
                            if (originalLedgerLimit != null && seekToPosition.compareTo(originalLedgerLimit) > 0) {
                                seekToPosition = originalLedgerLimit;
                            }
                            if (readPosition.compareTo(seekToPosition.getLedgerId(), seekToPosition.getEntryId()) > 0) {
                                seekToPosition = readPosition;
                            }
                            cursor.seek(seekToPosition);
                            notifyIfPastHorizon(seekToPosition, lastCompactedPosition, onOriginalLedgerRead);
                            return entries;
                        }

                        List<Entry> result = entries;
                        if (originalLedgerLimit != null
                                && entries.get(entries.size() - 1).getPosition().compareTo(originalLedgerLimit) >= 0) {
                            // The reader already went past this point on the original ledger: continue there.
                            result = new ArrayList<>(entries.size());
                            for (Entry entry : entries) {
                                if (entry.getPosition().compareTo(originalLedgerLimit) < 0) {
                                    result.add(entry);
                                } else {
                                    entry.release();
                                }
                            }
                            if (result.isEmpty()) {
                                cursor.seek(originalLedgerLimit, true);
                                return result;
                            }
                        }

                        long entriesSize = 0;
                        for (Entry entry : result) {
                            entriesSize += entry.getLength();
                        }
                        cursor.updateReadStats(result.size(), entriesSize);

                        Position nextPosition = result.get(result.size() - 1).getPosition().getNext();
                        cursor.seek(nextPosition, true);
                        notifyIfPastHorizon(nextPosition, lastCompactedPosition, onOriginalLedgerRead);
                        return result;
                    });
        });
    }

    private static CompletableFuture<List<Entry>> readOriginalLedger(ManagedCursor cursor, int maxEntries,
                                                                     long bytesToRead, Position maxReadPosition,
                                                                     boolean wait) {
        if (wait) {
            return readEntriesWithSkipOrWait(cursor, maxEntries, bytesToRead, maxReadPosition, null);
        } else {
            return readEntries(cursor, maxEntries, bytesToRead, maxReadPosition);
        }
    }

    // The snapshot ends when the cursor moves past the horizon: the next read goes to the original ledger.
    private static void notifyIfPastHorizon(Position nextReadPosition, Position lastCompactedPosition,
                                            @Nullable Consumer<Position> onOriginalLedgerRead) {
        if (onOriginalLedgerRead != null && nextReadPosition.compareTo(lastCompactedPosition) > 0) {
            onOriginalLedgerRead.accept(nextReadPosition);
        }
    }

    private static boolean isRetained(ManagedCursor cursor, Position position) {
        Position firstPosition = cursor.getManagedLedger().getFirstPosition();
        return firstPosition == null || position.compareTo(firstPosition) > 0;
    }
}
