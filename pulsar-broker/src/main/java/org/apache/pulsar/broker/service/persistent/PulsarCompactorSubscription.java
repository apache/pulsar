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
import static org.apache.pulsar.broker.service.AbstractBaseDispatcher.checkAndApplyReachedEndOfTopicOrTopicMigration;
import io.github.merlimat.slog.Logger;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.util.Errors;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.compaction.CompactedTopic;
import org.apache.pulsar.compaction.CompactedTopicContext;
import org.apache.pulsar.compaction.CompactedTopicImpl;
import org.apache.pulsar.compaction.Compactor;

public class PulsarCompactorSubscription extends PersistentSubscription {

    private static final Logger LOG = Logger.get(PulsarCompactorSubscription.class);
    protected final Logger log;

    private final CompactedTopic compactedTopic;

    public PulsarCompactorSubscription(PersistentTopic topic, CompactedTopic compactedTopic,
                                       String subscriptionName, ManagedCursor cursor) {
        super(topic, subscriptionName, cursor, false);
        this.log = LOG.with().ctx(super.log).build();
        checkArgument(subscriptionName.equals(Compactor.COMPACTION_SUBSCRIPTION));
        this.compactedTopic = compactedTopic;

        // Avoid compactor cursor to cause entries to be cached
        this.cursor.setAlwaysInactive();

        Map<String, Long> properties = cursor.getProperties();
        if (properties.containsKey(Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY)) {
            long compactedLedgerId = properties.get(Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY);
            Position markDeletedPosition = cursor.getMarkDeletedPosition();
            compactedTopic.newCompactedLedger(markDeletedPosition, compactedLedgerId)
                    .thenAccept(previousContext -> {
                        // The mark-delete is already durable (loaded from cursor metadata), so promote
                        // the compacted ledger immediately so in-memory state matches persistence.
                        compactedTopic.promoteNewCompactedLedger(markDeletedPosition);
                        if (previousContext != null) {
                            compactedTopic.deleteCompactedLedger(previousContext.getLedger().getId());
                        }
                    });
        }
    }

    /**
     * The compactor owns this cursor, so it can never be replicated. Disabling is still delegated, so that a
     * replicated status that was set before is removed from the cursor properties.
     */
    @Override
    public boolean setReplicated(boolean replicated) {
        if (replicated) {
            return false;
        }
        return super.setReplicated(false);
    }

    @Override
    public CompletableFuture<Void> acknowledgeMessageAsync(List<Position> positions, AckType ackType,
                                                           Map<String, Long> properties) {
        checkArgument(ackType == AckType.Cumulative);
        checkArgument(positions.size() == 1);
        checkArgument(properties.containsKey(Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY));
        long compactedLedgerId = properties.get(Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY);
        CompletableFuture<Void> completionFuture = new CompletableFuture<>();

        Position position = positions.get(0);

        log.debug()
                .attr("position", position)
                .log("Cumulative ack on compactor subscription");

        // Open the new compacted ledger first. This does NOT yet install the compaction horizon
        // or set the active context — those are only promoted after the mark-delete below is
        // durably committed. Otherwise a failed mark-delete would leave the in-memory state
        // ahead of the persisted cursor metadata (see issue #26486).
        compactedTopic.newCompactedLedger(position, compactedLedgerId).thenAccept(previousContext -> {
            cursor.asyncMarkDelete(position, properties, new MarkDeleteCallback() {
                @Override
                public void markDeleteComplete(Object ctx) {
                    log.debug()
                            .attr("position", position)
                            .log("Mark deleted messages until position on compactor subscription");

                    // Mark-delete is now durable — promote the compacted ledger and horizon so reads
                    // reflect what was actually persisted.
                    compactedTopic.promoteNewCompactedLedger(position);

                    if (previousContext != null) {
                        compactedTopic.deleteCompactedLedger(previousContext.getLedger().getId());
                    }
                    completionFuture.complete(null);
                }

                @Override
                public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                    // The mark-delete did not commit. The compacted ledger was opened above but never
                    // promoted, so compactionHorizon and compactedTopicContext are unchanged and
                    // remain consistent with the cursor's durable state.
                    // TODO: cut consumer connection on markDeleteFailed
                    log.debug()
                            .exception(exception)
                            .log("Failed to mark delete for position on compactor subscription");
                    completionFuture.completeExceptionally(exception);
                }
            }, null);
        }).exceptionally(ex -> {
            completionFuture.completeExceptionally(ex);
            return null;
        });

        if (topic.getManagedLedger().isTerminated() && !cursor.hasBacklog(false)) {
            // Notify all consumer that the end of topic was reached
            checkAndApplyReachedEndOfTopicOrTopicMigration(topic, dispatcher.getConsumers());
        }

        return completionFuture;
    }

    CompletableFuture<Void> cleanCompactedLedger() {
        final CompletableFuture<CompactedTopicContext> compactedTopicContextFuture;
        synchronized (compactedTopic) {
            compactedTopicContextFuture = ((CompactedTopicImpl) compactedTopic).getCompactedTopicContextFuture();
            // The subscription was explicitly deleted. Clear its registration even if the ledger
            // could not be opened, before another compaction can install a replacement context.
            ((CompactedTopicImpl) compactedTopic).reset();
        }
        if (compactedTopicContextFuture != null) {
            return compactedTopicContextFuture.thenCompose(context -> {
                long compactedLedgerId = context.getLedger().getId();
                return compactedTopic.deleteCompactedLedger(compactedLedgerId);
            }).exceptionallyCompose(ex -> {
                Throwable cause = FutureUtil.unwrapCompletionException(ex);
                if (cause instanceof BKException bkException
                        && Errors.isNoSuchLedgerExistsException(bkException.getCode())) {
                    return CompletableFuture.completedFuture(null);
                }
                return CompletableFuture.failedFuture(cause);
            });
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }
}
