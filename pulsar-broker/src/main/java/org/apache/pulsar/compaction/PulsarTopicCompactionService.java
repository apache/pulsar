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
import static org.apache.pulsar.compaction.CompactedTopicImpl.COMPACT_LEDGER_EMPTY;
import static org.apache.pulsar.compaction.CompactedTopicImpl.NEWER_THAN_COMPACTED;
import static org.apache.pulsar.compaction.CompactedTopicImpl.findStartPoint;
import static org.apache.pulsar.compaction.CompactedTopicImpl.readEntries;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;


public class PulsarTopicCompactionService implements TopicCompactionService {

    private final String topic;

    private final CompactedTopicImpl compactedTopic;

    private final Supplier<Compactor> compactorSupplier;

    public PulsarTopicCompactionService(String topic, BookKeeper bookKeeper,
                                        Supplier<Compactor> compactorSupplier) {
        this.topic = topic;
        this.compactedTopic = new CompactedTopicImpl(bookKeeper);
        this.compactorSupplier = compactorSupplier;
    }

    @Override
    public CompletableFuture<Void> compact() {
        Compactor compactor;
        try {
            compactor = compactorSupplier.get();
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
        return compactor.compact(topic).thenApply(x -> null);
    }

    @Override
    public CompletableFuture<List<Entry>> readCompactedEntries(@Nonnull Position startPosition,
                                                               int numberOfEntriesToRead) {
        Objects.requireNonNull(startPosition);
        checkArgument(numberOfEntriesToRead > 0);

        CompletableFuture<List<Entry>> resultFuture = new CompletableFuture<>();

        Objects.requireNonNull(compactedTopic.getCompactedTopicContextFuture()).thenCompose(
                (context) -> findStartPoint((PositionImpl) startPosition, context.ledger.getLastAddConfirmed(),
                        context.cache).thenCompose((startPoint) -> {
                    if (startPoint == COMPACT_LEDGER_EMPTY || startPoint == NEWER_THAN_COMPACTED) {
                        return CompletableFuture.completedFuture(Collections.emptyList());
                    }
                    long endPoint =
                            Math.min(context.ledger.getLastAddConfirmed(), startPoint + (numberOfEntriesToRead - 1));
                    return CompactedTopicImpl.readEntries(context.ledger, startPoint, endPoint);
                })).whenComplete((result, ex) -> {
                    if (ex == null) {
                        resultFuture.complete(result);
                    } else {
                        ex = FutureUtil.unwrapCompletionException(ex);
                        if (ex instanceof NoSuchElementException) {
                            resultFuture.complete(Collections.emptyList());
                        } else {
                            resultFuture.completeExceptionally(ex);
                        }
                    }
                });

        return resultFuture;
    }

    @Override
    public CompletableFuture<Entry> readLastCompactedEntry() {
        return compactedTopic.readLastEntryOfCompactedLedger();
    }

    @Override
    public CompletableFuture<Position> getLastCompactedPosition() {
        return CompletableFuture.completedFuture(compactedTopic.getCompactionHorizon().orElse(null));
    }

    @Override
    public CompletableFuture<Entry> findEntryByPublishTime(long publishTime) {
        final Predicate<Entry> predicate = entry -> {
            try {
                return Commands.parseMessageMetadata(entry.getDataBuffer()).getPublishTime() >= publishTime;
            } finally {
                entry.release();
            }
        };
        return findFirstMatchEntry(predicate);
    }

    @Override
    public CompletableFuture<Entry> findEntryByEntryIndex(long entryIndex) {
        final Predicate<Entry> predicate = entry -> {
            try {
                BrokerEntryMetadata brokerEntryMetadata =
                        Commands.parseBrokerEntryMetadataIfExist(entry.getDataBuffer());
                if (brokerEntryMetadata == null || !brokerEntryMetadata.hasIndex()) {
                    return false;
                }
                return brokerEntryMetadata.getIndex() >= entryIndex;
            } finally {
                entry.release();
            }
        };
        return findFirstMatchEntry(predicate);
    }

    private CompletableFuture<Entry> findFirstMatchEntry(final Predicate<Entry> predicate) {
        var compactedTopicContextFuture = compactedTopic.getCompactedTopicContextFuture();

        if (compactedTopicContextFuture == null) {
            return CompletableFuture.completedFuture(null);
        }
        return compactedTopicContextFuture.thenCompose(compactedTopicContext -> {
            LedgerHandle lh = compactedTopicContext.getLedger();
            CompletableFuture<Long> promise = new CompletableFuture<>();
            findFirstMatchIndexLoop(predicate, 0L, lh.getLastAddConfirmed(), promise, null, lh);
            return promise.thenCompose(index -> {
                if (index == null) {
                    return CompletableFuture.completedFuture(null);
                }
                return readEntries(lh, index, index).thenApply(entries -> entries.get(0));
            });
        });
    }

    private static void findFirstMatchIndexLoop(final Predicate<Entry> predicate,
                                           final long start, final long end,
                                           final CompletableFuture<Long> promise,
                                           final Long lastMatchIndex,
                                           final LedgerHandle lh) {
        if (start > end) {
            promise.complete(lastMatchIndex);
            return;
        }

        long mid = (start + end) / 2;
        CompactedTopicImpl.readEntries(lh, mid, mid).thenAccept(entries -> {
            Entry entry = entries.get(0);
            if (predicate.test(entry)) {
                findFirstMatchIndexLoop(predicate, start, mid - 1, promise, mid, lh);
            } else {
                findFirstMatchIndexLoop(predicate, mid + 1, end, promise, lastMatchIndex, lh);
            }
        }).exceptionally(ex -> {
            promise.completeExceptionally(ex);
            return null;
        });
    }

    public CompactedTopicImpl getCompactedTopic() {
        return compactedTopic;
    }

    @Override
    public void close() throws IOException {
        // noop
    }
}
