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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.collections4.CollectionUtils;

public class CompactedTopicUtils {

    @Beta
    public static CompletableFuture<List<Entry>> asyncReadCompactedEntries(
            TopicCompactionService topicCompactionService, ManagedCursor cursor, int maxEntries,
            long bytesToRead, PositionImpl maxReadPosition, boolean readFromEarliest, boolean wait) {
        Objects.requireNonNull(topicCompactionService);
        Objects.requireNonNull(cursor);
        checkArgument(maxEntries > 0);

        final PositionImpl readPosition;
        if (readFromEarliest) {
            readPosition = PositionImpl.EARLIEST;
        } else {
            readPosition = (PositionImpl) cursor.getReadPosition();
        }

        CompletableFuture<Position> lastCompactedPositionFuture = topicCompactionService.getLastCompactedPosition();

        return lastCompactedPositionFuture.thenCompose(lastCompactedPosition -> {
            if (lastCompactedPosition == null
                    || readPosition.compareTo(
                    lastCompactedPosition.getLedgerId(), lastCompactedPosition.getEntryId()) > 0) {
                if (wait) {
                    return readEntriesWithSkipOrWait(cursor, maxEntries, bytesToRead, maxReadPosition, null);
                } else {
                    return readEntries(cursor, maxEntries, bytesToRead, maxReadPosition);
                }
            }

            ManagedCursorImpl managedCursor = (ManagedCursorImpl) cursor;
            int numberOfEntriesToRead = managedCursor.applyMaxSizeCap(maxEntries, bytesToRead);

            return topicCompactionService.readCompactedEntries(readPosition, numberOfEntriesToRead)
                    .thenApply(entries -> {
                        if (CollectionUtils.isEmpty(entries)) {
                            Position seekToPosition = lastCompactedPosition.getNext();
                            if (readPosition.compareTo(seekToPosition.getLedgerId(), seekToPosition.getEntryId()) > 0) {
                                seekToPosition = readPosition;
                            }
                            cursor.seek(seekToPosition);
                            return entries;
                        }

                        long entriesSize = 0;
                        for (Entry entry : entries) {
                            entriesSize += entry.getLength();
                        }
                        managedCursor.updateReadStats(entries.size(), entriesSize);

                        Entry lastEntry = entries.get(entries.size() - 1);
                        cursor.seek(lastEntry.getPosition().getNext(), true);
                        return entries;
                    });
        });
    }
}
