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
import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import com.google.common.annotations.Beta;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherSingleActiveConsumer;
import org.apache.pulsar.common.util.FutureUtil;

public class CompactedTopicUtils {

    @Beta
    public static void asyncReadCompactedEntries(TopicCompactionService topicCompactionService,
                                                 ManagedCursor cursor, int maxEntries,
                                                 long bytesToRead, Position maxReadPosition,
                                                 boolean readFromEarliest, AsyncCallbacks.ReadEntriesCallback callback,
                                                 boolean wait, @Nullable Consumer consumer) {
        Objects.requireNonNull(topicCompactionService);
        Objects.requireNonNull(cursor);
        checkArgument(maxEntries > 0);
        Objects.requireNonNull(callback);

        final Position readPosition;
        if (readFromEarliest) {
            readPosition = PositionFactory.EARLIEST;
        } else {
            readPosition = cursor.getReadPosition();
        }

        // TODO: redeliver epoch link https://github.com/apache/pulsar/issues/13690
        PersistentDispatcherSingleActiveConsumer.ReadEntriesCtx readEntriesCtx =
                PersistentDispatcherSingleActiveConsumer.ReadEntriesCtx.create(consumer, DEFAULT_CONSUMER_EPOCH);

        CompletableFuture<Position> lastCompactedPositionFuture = topicCompactionService.getLastCompactedPosition();

        lastCompactedPositionFuture.thenCompose(lastCompactedPosition -> {
            if (lastCompactedPosition == null
                    || readPosition.compareTo(
                    lastCompactedPosition.getLedgerId(), lastCompactedPosition.getEntryId()) > 0) {
                if (wait) {
                    cursor.asyncReadEntriesOrWait(maxEntries, bytesToRead, callback, readEntriesCtx, maxReadPosition);
                } else {
                    cursor.asyncReadEntries(maxEntries, bytesToRead, callback, readEntriesCtx, maxReadPosition);
                }
                return CompletableFuture.completedFuture(null);
            }

            int numberOfEntriesToRead = cursor.applyMaxSizeCap(maxEntries, bytesToRead);

            return topicCompactionService.readCompactedEntries(readPosition, numberOfEntriesToRead)
                    .thenAccept(entries -> {
                        if (CollectionUtils.isEmpty(entries)) {
                            Position seekToPosition = lastCompactedPosition.getNext();
                            if (readPosition.compareTo(seekToPosition.getLedgerId(), seekToPosition.getEntryId()) > 0) {
                                seekToPosition = readPosition;
                            }
                            cursor.seek(seekToPosition);
                            callback.readEntriesComplete(Collections.emptyList(), readEntriesCtx);
                            return;
                        }

                        long entriesSize = 0;
                        for (Entry entry : entries) {
                            entriesSize += entry.getLength();
                        }
                        cursor.updateReadStats(entries.size(), entriesSize);

                        Entry lastEntry = entries.get(entries.size() - 1);
                        cursor.seek(lastEntry.getPosition().getNext(), true);
                        callback.readEntriesComplete(entries, readEntriesCtx);
                    });
        }).exceptionally((exception) -> {
            exception = FutureUtil.unwrapCompletionException(exception);
            callback.readEntriesFailed(ManagedLedgerException.getManagedLedgerException(exception), readEntriesCtx);
            return null;
        });
    }
}
