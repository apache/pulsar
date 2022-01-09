/**
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

import com.google.common.collect.ImmutableMap;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.NullLedgerOffloader;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.compaction.CompactedTopic;
import org.apache.pulsar.compaction.CompactedTopicContext;
import org.apache.pulsar.compaction.CompactedTopicImpl;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.compaction.OffloadCompactedTopicImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.google.common.base.Preconditions.checkArgument;

public class CompactorSubscription extends PersistentSubscription {
    private volatile CompactedTopic compactedTopic;

    static final AtomicReferenceFieldUpdater<CompactorSubscription, CompactedTopic> COMPACTED_TOPIC_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(CompactorSubscription.class, CompactedTopic.class,
                    "compactedTopic");

    public CompactorSubscription(PersistentTopic topic, CompactedTopic compactedTopic,
                                 String subscriptionName, ManagedCursor cursor) {
        super(topic, subscriptionName, cursor, false);
        checkArgument(subscriptionName.equals(Compactor.COMPACTION_SUBSCRIPTION));
        this.compactedTopic = compactedTopic;

        // Avoid compactor cursor to cause entries to be cached
        this.cursor.setAlwaysInactive();

        Map<String, Long> properties = cursor.getProperties();
        if (properties.containsKey(Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY)) {
            long compactedLedgerId = properties.get(Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY);
            compactedTopic.newCompactedLedger(cursor.getMarkDeletedPosition(), compactedLedgerId)
                    .thenAccept(previousContext -> {
                        if (previousContext != null) {
                            compactedTopic.deleteCompactedLedger(previousContext.getLedger().getId());
                        }
                    });
        }
    }

    @Override
    public void acknowledgeMessage(List<Position> positions, AckType ackType, Map<String, Long> properties) {
        checkArgument(ackType == AckType.Cumulative);
        checkArgument(positions.size() == 1);
        checkArgument(properties.containsKey(Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY));
        long compactedLedgerId = properties.get(Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY);

        Position position = positions.get(0);

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Cumulative ack on compactor subscription {}", topicName, subName, position);
        }

        CompletableFuture<Void> future;

        // The newCompactedLedger must be called at the first step because we need to ensure the reader can read
        // complete data from compacted Ledger, otherwise, if the original ledger been deleted the reader cursor
        // might move to a subsequent original ledger if `compactionHorizon` have not updated, this will lead to
        // the reader skips compacted data at that time, after the `compactionHorizon` updated, the reader able
        // to read the complete compacted data again.
        // And we can only delete the previous ledger after the mark delete succeed, otherwise we will loss the
        // compacted data if mark delete failed.
        if (compactedTopic instanceof OffloadCompactedTopicImpl) {
            BookKeeper bk = topic.getBrokerService().getPulsar().getBookKeeperClient();
            final CompactedTopic nextCt = new CompactedTopicImpl(bk);
            future = nextCt.newCompactedLedger(position, compactedLedgerId).thenAccept(__ -> {
                final CompactedTopic oldTopic = COMPACTED_TOPIC_UPDATER.getAndSet(this, nextCt);
                CompactedTopicContext oldContext = oldTopic.getCompactedTopicContext().isPresent() ?
                        oldTopic.getCompactedTopicContext().get() : null;

                cursor.asyncMarkDelete(position, properties, deleteCallback(oldContext, position,
                        oldTopic), null);
            });
        } else {
            future = compactedTopic.newCompactedLedger(position, compactedLedgerId)
                    .thenAccept(previousContext -> cursor.asyncMarkDelete(position, properties,
                            deleteCallback(previousContext, position, compactedTopic), null));
        }

        if (topic.getManagedLedger().isTerminated() && cursor.getNumberOfEntriesInBacklog(false) == 0) {
            // Notify all consumer that the end of topic was reached
            dispatcher.getConsumers().forEach(Consumer::reachedEndOfTopic);
        }

        future.whenComplete((__, ex) -> {
            if (ex == null) {
                ManagedLedgerConfig config = topic.ledger.getConfig();

                boolean isOffload = false;
                // Topic is open offload,compactedTopic ledger auto offload
                for (MLDataFormats.ManagedLedgerInfo.LedgerInfo info : ((ManagedLedgerImpl) topic.ledger).getLedgersInfoAsList()) {
                    if(info.hasOffloadContext()){
                        isOffload = true;
                        break;
                    }
                }

                if (isOffload && config.getLedgerOffloader() != null
                        && config.getLedgerOffloader() != NullLedgerOffloader.INSTANCE
                        && config.getLedgerOffloader().getOffloadPolicies() != null) {
                    UUID uuid = UUID.randomUUID();
                    String name = TopicName.get(topicName).getPersistenceNamingEncoding();
                    Map<String, String> offloadDriverMetadata = config.getLedgerOffloader().getOffloadDriverMetadata();
                    Map<String, String> extraMetadata = ImmutableMap.of("ManagedLedgerName", name);

                    if (compactedTopic.getCompactedTopicContext().isPresent()
                            && compactedTopic.getCompactionHorizon().isPresent()) {
                        final CompactedTopicContext oldContext = compactedTopic.getCompactedTopicContext().get();
                        config.getLedgerOffloader().offload(oldContext.getLedger(), uuid, extraMetadata).thenAccept(res -> {
                            OffloadCompactedTopicImpl newCompactionTopic = new OffloadCompactedTopicImpl(
                                    config.getLedgerOffloader(), uuid, offloadDriverMetadata);
                            newCompactionTopic.newCompactedLedger(position, compactedLedgerId).thenAccept(ctc -> {
                                CompactedTopic oldCompactionTopic = COMPACTED_TOPIC_UPDATER.getAndSet(this, newCompactionTopic);
                                Map<String, Long> newProp = new HashMap<>(properties);
                                newProp.put(OffloadCompactedTopicImpl.UUID_LSB_NAME, uuid.getLeastSignificantBits());
                                newProp.put(OffloadCompactedTopicImpl.UUID_MSB_NAME, uuid.getMostSignificantBits());
                                cursor.asyncMarkDelete(position, newProp, deleteCallback(oldContext, position, oldCompactionTopic), null);
                            });
                        }).exceptionally(e -> {
                            log.warn("Failed offload compactedTopic:{}", topicName, e);
                            return null;
                        });
                    }
                }
            } else {
                log.warn("Failed mark delete topic:{}", topicName, ex);
            }

        });
    }

    private MarkDeleteCallback deleteCallback(CompactedTopicContext ctc, Position position, CompactedTopic compactedTopic){
        return new MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Mark deleted messages until position on compactor subscription {}",
                            topicName, subName, position);
                }
                if (ctc != null) {
                    compactedTopic.deleteCompactedLedger(ctc.getLedger().getId());
                }
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                // TODO: cut consumer connection on markDeleteFailed
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Failed to mark delete for position on compactor subscription {}",
                            topicName, subName, ctx, exception);
                }
            }
        };
    }

    private static final Logger log = LoggerFactory.getLogger(CompactorSubscription.class);
}
