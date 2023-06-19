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

import static org.apache.pulsar.compaction.CompactedTopicImpl.COMPACT_LEDGER_EMPTY;
import static org.apache.pulsar.compaction.CompactedTopicImpl.NEWER_THAN_COMPACTED;
import static org.apache.pulsar.compaction.CompactedTopicImpl.findStartPoint;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.PositionImpl;


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
        return compactorSupplier.get().compact(topic).thenApply(x -> null);
    }

    @Override
    public CompletableFuture<Optional<List<Entry>>> readCompactedEntries(PositionImpl startPosition,
                                                                         int numberOfEntriesToRead) {
        CompletableFuture<Optional<List<Entry>>> resultFuture = new CompletableFuture<>();

        CompletableFuture<Optional<List<Entry>>> optionalCompletableFuture =
                compactedTopic.getCompactedTopicContextFuture().thenCompose(
                        (context) -> findStartPoint((PositionImpl) startPosition, context.ledger.getLastAddConfirmed(),
                                context.cache).thenCompose((startPoint) -> {
                            if (startPoint == COMPACT_LEDGER_EMPTY || startPoint == NEWER_THAN_COMPACTED) {
                                return CompletableFuture.completedFuture(Optional.empty());
                            }
                            long endPoint =
                                    Math.min(context.ledger.getLastAddConfirmed(), startPoint + numberOfEntriesToRead);
                            return CompactedTopicImpl.readEntries(context.ledger, startPoint, endPoint)
                                    .thenApply(Optional::of);
                        }));

        optionalCompletableFuture.whenComplete((result, ex) -> {
            if (ex == null) {
                resultFuture.complete(result);
            } else {
                if (ex instanceof NoSuchElementException) {
                    resultFuture.complete(Optional.empty());
                } else {
                    resultFuture.completeExceptionally(ex);
                }
            }
        });

        return resultFuture;
    }

    @Override
    public CompletableFuture<Optional<Entry>> readCompactedLastEntry() {
        return compactedTopic.readLastEntryOfCompactedLedger().thenApply(Optional::ofNullable);
    }

    @Override
    public CompletableFuture<Optional<PositionImpl>> getCompactedLastPosition() {
        return CompletableFuture.completedFuture(compactedTopic.getCompactionHorizon());
    }

    public CompactedTopicImpl getCompactedTopic() {
        return compactedTopic;
    }
}
