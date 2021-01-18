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

import static com.google.common.base.Preconditions.checkArgument;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.compaction.CompactedTopic;
import org.apache.pulsar.compaction.Compactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactorSubscription extends PersistentSubscription {
    private CompactedTopic compactedTopic;

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
            compactedTopic.newCompactedLedger(cursor.getMarkDeletedPosition(),
                                              compactedLedgerId);
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
        CompletableFuture<Void> future = new CompletableFuture<>();
        cursor.asyncMarkDelete(position, properties, new MarkDeleteCallback() {
                @Override
                public void markDeleteComplete(Object ctx) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}][{}] Mark deleted messages until position on compactor subscription {}",
                                  topicName, subName, position);
                    }
                    future.complete(null);
                }

                @Override
                public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                    // TODO: cut consumer connection on markDeleteFailed
                    if (log.isDebugEnabled()) {
                        log.debug("[{}][{}] Failed to mark delete for position on compactor subscription {}",
                                  topicName, subName, ctx, exception);
                    }
                }
            }, null);

        if (topic.getManagedLedger().isTerminated() && cursor.getNumberOfEntriesInBacklog(false) == 0) {
            // Notify all consumer that the end of topic was reached
            dispatcher.getConsumers().forEach(Consumer::reachedEndOfTopic);
        }

        // Once properties have been persisted, we can notify the compacted topic to use
        // the new ledger
        future.thenAccept((v) -> compactedTopic.newCompactedLedger(position, compactedLedgerId));
    }

    private static final Logger log = LoggerFactory.getLogger(CompactorSubscription.class);
}
