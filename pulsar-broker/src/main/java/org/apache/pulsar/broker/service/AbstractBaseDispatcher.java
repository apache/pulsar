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

package org.apache.pulsar.broker.service;

import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import io.netty.buffer.ByteBuf;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshot;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;

@Slf4j
public abstract class AbstractBaseDispatcher implements Dispatcher {

    protected final Subscription subscription;

    protected AbstractBaseDispatcher(Subscription subscription) {
        this.subscription = subscription;
    }

    /**
     * Filter messages that are being sent to a consumers.
     * <p>
     * Messages can be filtered out for multiple reasons:
     * <ul>
     * <li>Checksum or metadata corrupted
     * <li>Message is an internal marker
     * <li>Message is not meant to be delivered immediately
     * </ul>
     *
     * @param entries
     *            a list of entries as read from storage
     *
     * @param batchSizes
     *            an array where the batch size for each entry (the number of messages within an entry) is stored. This
     *            array needs to be of at least the same size as the entries list
     *
     * @param sendMessageInfo
     *            an object where the total size in messages and bytes will be returned back to the caller
     */
    public void filterEntriesForConsumer(List<Entry> entries, EntryBatchSizes batchSizes,
                                         SendMessageInfo sendMessageInfo, EntryBatchIndexesAcks indexesAcks,
                                         ManagedCursor cursor, boolean isReplayRead) {
        int totalMessages = 0;
        long totalBytes = 0;
        int totalChunkedMessages = 0;
        for (int i = 0, entriesSize = entries.size(); i < entriesSize; i++) {
            Entry entry = entries.get(i);
            if (entry == null) {
                continue;
            }

            ByteBuf metadataAndPayload = entry.getDataBuffer();

            MessageMetadata msgMetadata = Commands.peekMessageMetadata(metadataAndPayload, subscription.toString(), -1);

            if (!isReplayRead && msgMetadata != null
                    && msgMetadata.hasTxnidMostBits() && msgMetadata.hasTxnidLeastBits()) {
                if (Markers.isTxnMarker(msgMetadata)) {
                    entries.set(i, null);
                    entry.release();
                    continue;
                } else if (((PersistentTopic) subscription.getTopic()).isTxnAborted(
                        new TxnID(msgMetadata.getTxnidMostBits(), msgMetadata.getTxnidLeastBits()))) {
                    subscription.acknowledgeMessage(Collections.singletonList(entry.getPosition()), AckType.Individual,
                            Collections.emptyMap());
                    entries.set(i, null);
                    entry.release();
                    continue;
                }
            } else if (msgMetadata == null || Markers.isServerOnlyMarker(msgMetadata)) {
                PositionImpl pos = (PositionImpl) entry.getPosition();
                // Message metadata was corrupted or the messages was a server-only marker

                if (Markers.isReplicatedSubscriptionSnapshotMarker(msgMetadata)) {
                    processReplicatedSubscriptionSnapshot(pos, metadataAndPayload);
                }

                entries.set(i, null);
                entry.release();
                subscription.acknowledgeMessage(Collections.singletonList(pos), AckType.Individual,
                        Collections.emptyMap());
                continue;
            } else if (msgMetadata.hasDeliverAtTime()
                    && trackDelayedDelivery(entry.getLedgerId(), entry.getEntryId(), msgMetadata)) {
                // The message is marked for delayed delivery. Ignore for now.
                entries.set(i, null);
                entry.release();
                continue;
            }

            int batchSize = msgMetadata.getNumMessagesInBatch();
            totalMessages += batchSize;
            totalBytes += metadataAndPayload.readableBytes();
            totalChunkedMessages += msgMetadata.hasChunkId() ? 1 : 0;
            batchSizes.setBatchSize(i, batchSize);
            long[] ackSet = null;
            if (indexesAcks != null && cursor != null) {
                ackSet = cursor.getDeletedBatchIndexesAsLongArray(
                        PositionImpl.get(entry.getLedgerId(), entry.getEntryId()));
                if (ackSet != null) {
                    indexesAcks.setIndexesAcks(i, Pair.of(batchSize, ackSet));
                } else {
                    indexesAcks.setIndexesAcks(i, null);
                }
            }

            BrokerInterceptor interceptor = subscription.interceptor();
            if (null != interceptor) {
                interceptor.beforeSendMessage(
                    subscription,
                    entry,
                    ackSet,
                    msgMetadata
                );
            }
        }

        sendMessageInfo.setTotalMessages(totalMessages);
        sendMessageInfo.setTotalBytes(totalBytes);
        sendMessageInfo.setTotalChunkedMessages(totalChunkedMessages);
    }

    /**
     * Determine whether the number of consumers on the subscription reaches the threshold.
     * @return
     */
    protected abstract boolean isConsumersExceededOnSubscription();

    protected boolean isConsumersExceededOnSubscription(BrokerService brokerService,
                                                        String topic, int consumerSize) {
        Policies policies = null;
        Integer maxConsumersPerSubscription = null;
        try {
            maxConsumersPerSubscription = Optional.ofNullable(brokerService
                    .getTopicPolicies(TopicName.get(topic)))
                    .map(TopicPolicies::getMaxConsumersPerSubscription)
                    .orElse(null);
            if (maxConsumersPerSubscription == null) {
                // Use getDataIfPresent from zk cache to make the call non-blocking and prevent deadlocks in addConsumer
                policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                        .getDataIfPresent(AdminResource.path(POLICIES, TopicName.get(topic).getNamespace()));
            }
        } catch (Exception e) {
            log.debug("Get topic or namespace policies fail", e);
        }

        if (maxConsumersPerSubscription == null) {
            maxConsumersPerSubscription = policies != null
                    && policies.max_consumers_per_subscription != null
                    && policies.max_consumers_per_subscription >= 0
                    ? policies.max_consumers_per_subscription :
                    brokerService.pulsar().getConfiguration().getMaxConsumersPerSubscription();
        }

        return maxConsumersPerSubscription > 0 && maxConsumersPerSubscription <= consumerSize;
    }

    private void processReplicatedSubscriptionSnapshot(PositionImpl pos, ByteBuf headersAndPayload) {
        // Remove the protobuf headers
        Commands.skipMessageMetadata(headersAndPayload);

        try {
            ReplicatedSubscriptionsSnapshot snapshot = Markers.parseReplicatedSubscriptionsSnapshot(headersAndPayload);
            subscription.processReplicatedSubscriptionSnapshot(snapshot);
        } catch (Throwable t) {
            log.warn("Failed to process replicated subscription snapshot at {} -- {}", pos, t.getMessage(), t);
            return;
        }
    }

    public void resetCloseFuture() {
        // noop
    }

    protected byte[] peekStickyKey(ByteBuf metadataAndPayload) {
        return Commands.peekStickyKey(metadataAndPayload, subscription.getTopicName(), subscription.getName());
    }

    protected void addMessageToReplay(long ledgerId, long entryId) {
        // No-op
    }
}
