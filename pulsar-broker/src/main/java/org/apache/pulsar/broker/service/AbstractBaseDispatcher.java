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
package org.apache.pulsar.broker.service;

import io.netty.buffer.ByteBuf;
import io.prometheus.client.Gauge;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.service.persistent.CompactorSubscription;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshot;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;

@Slf4j
public abstract class AbstractBaseDispatcher extends EntryFilterSupport implements Dispatcher {

    private static final Gauge PENDING_BYTES_TO_DISPATCH = Gauge
            .build()
            .name("pulsar_broker_pending_bytes_to_dispatch")
            .help("Amount of bytes loaded in memory to be dispatched to Consumers")
            .register();

    protected final ServiceConfiguration serviceConfig;
    protected final boolean dispatchThrottlingOnBatchMessageEnabled;
    private final LongAdder filterProcessedMsgs = new LongAdder();
    private final LongAdder filterAcceptedMsgs = new LongAdder();
    private final LongAdder filterRejectedMsgs = new LongAdder();
    private final LongAdder filterRescheduledMsgs = new LongAdder();

    protected AbstractBaseDispatcher(Subscription subscription, ServiceConfiguration serviceConfig) {
        super(subscription);
        this.serviceConfig = serviceConfig;
        this.dispatchThrottlingOnBatchMessageEnabled = serviceConfig.isDispatchThrottlingOnBatchMessageEnabled();
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
    public int filterEntriesForConsumer(List<? extends Entry> entries, EntryBatchSizes batchSizes,
            SendMessageInfo sendMessageInfo, EntryBatchIndexesAcks indexesAcks,
            ManagedCursor cursor, boolean isReplayRead, Consumer consumer) {
        return filterEntriesForConsumer(Optional.empty(), 0, entries, batchSizes, sendMessageInfo, indexesAcks, cursor,
                isReplayRead, consumer);
    }

    /**
     * Filter entries with prefetched message metadata range so that there is no need to peek metadata from Entry.
     *
     * @param optMetadataArray the optional message metadata array
     * @param startOffset the index in `optMetadataArray` of the first Entry's message metadata
     *
     * @see AbstractBaseDispatcher#filterEntriesForConsumer(List, EntryBatchSizes, SendMessageInfo,
     *   EntryBatchIndexesAcks, ManagedCursor, boolean, Consumer)
     */
    public int filterEntriesForConsumer(Optional<MessageMetadata[]> optMetadataArray, int startOffset,
             List<? extends Entry> entries, EntryBatchSizes batchSizes, SendMessageInfo sendMessageInfo,
             EntryBatchIndexesAcks indexesAcks, ManagedCursor cursor, boolean isReplayRead, Consumer consumer) {
        int totalMessages = 0;
        long totalBytes = 0;
        int totalChunkedMessages = 0;
        int totalEntries = 0;
        int filteredMessageCount = 0;
        int filteredEntryCount = 0;
        long filteredBytesCount = 0;
        List<Position> entriesToFiltered = hasFilter ? new ArrayList<>() : null;
        List<PositionImpl> entriesToRedeliver = hasFilter ? new ArrayList<>() : null;
        for (int i = 0, entriesSize = entries.size(); i < entriesSize; i++) {
            final Entry entry = entries.get(i);
            if (entry == null) {
                continue;
            }
            ByteBuf metadataAndPayload = entry.getDataBuffer();
            final int metadataIndex = i + startOffset;
            final MessageMetadata msgMetadata = optMetadataArray.map(metadataArray -> metadataArray[metadataIndex])
                    .orElseGet(() -> (entry instanceof EntryAndMetadata)
                            ? ((EntryAndMetadata) entry).getMetadata()
                            : Commands.peekAndCopyMessageMetadata(metadataAndPayload, subscription.toString(), -1)
                    );

            int entryMsgCnt = msgMetadata == null ? 1 : msgMetadata.getNumMessagesInBatch();
            if (hasFilter) {
                this.filterProcessedMsgs.add(entryMsgCnt);
            }

            EntryFilter.FilterResult filterResult = runFiltersForEntry(entry, msgMetadata, consumer);
            if (filterResult == EntryFilter.FilterResult.REJECT) {
                entriesToFiltered.add(entry.getPosition());
                entries.set(i, null);
                // FilterResult will be always `ACCEPTED` when there is No Filter
                // dont need to judge whether `hasFilter` is true or not.
                this.filterRejectedMsgs.add(entryMsgCnt);
                filteredEntryCount++;
                filteredMessageCount += entryMsgCnt;
                filteredBytesCount += metadataAndPayload.readableBytes();
                entry.release();
                continue;
            } else if (filterResult == EntryFilter.FilterResult.RESCHEDULE) {
                entriesToRedeliver.add((PositionImpl) entry.getPosition());
                entries.set(i, null);
                // FilterResult will be always `ACCEPTED` when there is No Filter
                // dont need to judge whether `hasFilter` is true or not.
                this.filterRescheduledMsgs.add(entryMsgCnt);
                filteredEntryCount++;
                filteredMessageCount += entryMsgCnt;
                filteredBytesCount += metadataAndPayload.readableBytes();
                entry.release();
                continue;
            }
            if (!isReplayRead && msgMetadata != null && msgMetadata.hasTxnidMostBits()
                    && msgMetadata.hasTxnidLeastBits()) {
                if (Markers.isTxnMarker(msgMetadata)) {
                    // because consumer can receive message is smaller than maxReadPosition,
                    // so this marker is useless for this subscription
                    individualAcknowledgeMessageIfNeeded(entry.getPosition(), Collections.emptyMap());
                    entries.set(i, null);
                    entry.release();
                    continue;
                } else if (((PersistentTopic) subscription.getTopic())
                        .isTxnAborted(new TxnID(msgMetadata.getTxnidMostBits(), msgMetadata.getTxnidLeastBits()),
                                (PositionImpl) entry.getPosition())) {
                    individualAcknowledgeMessageIfNeeded(entry.getPosition(), Collections.emptyMap());
                    entries.set(i, null);
                    entry.release();
                    continue;
                }
            }

            if (msgMetadata == null || Markers.isServerOnlyMarker(msgMetadata)) {
                PositionImpl pos = (PositionImpl) entry.getPosition();
                // Message metadata was corrupted or the messages was a server-only marker

                if (Markers.isReplicatedSubscriptionSnapshotMarker(msgMetadata)) {
                    processReplicatedSubscriptionSnapshot(pos, metadataAndPayload);
                }

                entries.set(i, null);
                entry.release();
                individualAcknowledgeMessageIfNeeded(pos, Collections.emptyMap());
                continue;
            } else if (trackDelayedDelivery(entry.getLedgerId(), entry.getEntryId(), msgMetadata)) {
                // The message is marked for delayed delivery. Ignore for now.
                entries.set(i, null);
                entry.release();
                continue;
            }

            if (hasFilter) {
                this.filterAcceptedMsgs.add(entryMsgCnt);
            }

            totalEntries++;
            int batchSize = msgMetadata.getNumMessagesInBatch();
            totalMessages += batchSize;
            totalBytes += metadataAndPayload.readableBytes();
            totalChunkedMessages += msgMetadata.hasChunkId() ? 1 : 0;
            batchSizes.setBatchSize(i, batchSize);
            long[] ackSet = null;
            if (indexesAcks != null && cursor != null) {
                ackSet = cursor
                        .getDeletedBatchIndexesAsLongArray(PositionImpl.get(entry.getLedgerId(), entry.getEntryId()));
                if (ackSet != null) {
                    indexesAcks.setIndexesAcks(i, Pair.of(batchSize, ackSet));
                } else {
                    indexesAcks.setIndexesAcks(i, null);
                }
            }

            BrokerInterceptor interceptor = subscription.interceptor();
            if (null != interceptor) {
                // keep for compatibility if users has implemented the old interface
                interceptor.beforeSendMessage(subscription, entry, ackSet, msgMetadata);
                interceptor.beforeSendMessage(subscription, entry, ackSet, msgMetadata, consumer);
            }
        }
        if (CollectionUtils.isNotEmpty(entriesToFiltered)) {
            subscription.acknowledgeMessage(entriesToFiltered, AckType.Individual,
                    Collections.emptyMap());

            int filtered = entriesToFiltered.size();
            Topic topic = subscription.getTopic();
            if (topic instanceof AbstractTopic) {
                ((AbstractTopic) topic).addFilteredEntriesCount(filtered);
            }
        }
        if (CollectionUtils.isNotEmpty(entriesToRedeliver)) {
            this.subscription.getTopic().getBrokerService().getPulsar().getExecutor()
                    .schedule(() -> {
                        // simulate the Consumer rejected the message
                        subscription
                                .redeliverUnacknowledgedMessages(consumer, entriesToRedeliver);
                    }, serviceConfig.getDispatcherEntryFilterRescheduledMessageDelay(), TimeUnit.MILLISECONDS);

        }

        if (serviceConfig.isDispatchThrottlingForFilteredEntriesEnabled()) {
            acquirePermitsForDeliveredMessages(subscription.getTopic(), cursor, filteredEntryCount,
                    filteredMessageCount, filteredBytesCount);
        }

        sendMessageInfo.setTotalMessages(totalMessages);
        sendMessageInfo.setTotalBytes(totalBytes);
        sendMessageInfo.setTotalChunkedMessages(totalChunkedMessages);
        return totalEntries;
    }

    private void individualAcknowledgeMessageIfNeeded(Position position, Map<String, Long> properties) {
        if (!(subscription instanceof CompactorSubscription)) {
            subscription.acknowledgeMessage(Collections.singletonList(position), AckType.Individual, properties);
        }
    }

    protected void acquirePermitsForDeliveredMessages(Topic topic, ManagedCursor cursor, long totalEntries,
                                                      long totalMessagesSent, long totalBytesSent) {
        if (serviceConfig.isDispatchThrottlingOnNonBacklogConsumerEnabled()
                || (cursor != null && !cursor.isActive())) {
            long permits = dispatchThrottlingOnBatchMessageEnabled ? totalEntries : totalMessagesSent;
            topic.getBrokerDispatchRateLimiter().ifPresent(rateLimiter ->
                    rateLimiter.tryDispatchPermit(permits, totalBytesSent));
            topic.getDispatchRateLimiter().ifPresent(rateLimter ->
                    rateLimter.tryDispatchPermit(permits, totalBytesSent));
            getRateLimiter().ifPresent(rateLimiter -> rateLimiter.tryDispatchPermit(permits, totalBytesSent));
        }
    }

    /**
     * Determine whether the number of consumers on the subscription reaches the threshold.
     * @return
     */
    protected abstract boolean isConsumersExceededOnSubscription();

    protected boolean isConsumersExceededOnSubscription(AbstractTopic topic, int consumerSize) {
        if (topic.isSystemTopic()) {
            return false;
        }
        Integer maxConsumersPerSubscription = topic.getHierarchyTopicPolicies().getMaxConsumersPerSubscription().get();
        return maxConsumersPerSubscription != null && maxConsumersPerSubscription > 0
                && maxConsumersPerSubscription <= consumerSize;
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

    protected abstract void reScheduleRead();

    protected boolean reachDispatchRateLimit(DispatchRateLimiter dispatchRateLimiter) {
        if (dispatchRateLimiter.isDispatchRateLimitingEnabled()) {
            if (!dispatchRateLimiter.hasMessageDispatchPermit()) {
                reScheduleRead();
                return true;
            }
        }
        return false;
    }

    protected Pair<Integer, Long> updateMessagesToRead(DispatchRateLimiter dispatchRateLimiter,
                                                       int messagesToRead, long bytesToRead) {
        // update messagesToRead according to available dispatch rate limit.
        return computeReadLimits(messagesToRead,
                (int) dispatchRateLimiter.getAvailableDispatchRateLimitOnMsg(),
                bytesToRead, dispatchRateLimiter.getAvailableDispatchRateLimitOnByte());
    }

    protected static Pair<Integer, Long> computeReadLimits(int messagesToRead, int availablePermitsOnMsg,
                                                           long bytesToRead, long availablePermitsOnByte) {
        if (availablePermitsOnMsg > 0) {
            messagesToRead = Math.min(messagesToRead, availablePermitsOnMsg);
        }

        if (availablePermitsOnByte > 0) {
            bytesToRead = Math.min(bytesToRead, availablePermitsOnByte);
        }

        return Pair.of(messagesToRead, bytesToRead);
    }

    protected byte[] peekStickyKey(ByteBuf metadataAndPayload) {
        return Commands.peekStickyKey(metadataAndPayload, subscription.getTopicName(), subscription.getName());
    }

    protected String getSubscriptionName() {
        return subscription == null ? null : subscription.getName();
    }

    protected void checkAndApplyReachedEndOfTopicOrTopicMigration(List<Consumer> consumers) {
        PersistentTopic topic = (PersistentTopic) subscription.getTopic();
        checkAndApplyReachedEndOfTopicOrTopicMigration(topic, consumers);
    }

    public static void checkAndApplyReachedEndOfTopicOrTopicMigration(PersistentTopic topic, List<Consumer> consumers) {
        if (topic.isMigrated()) {
            consumers.forEach(c -> c.topicMigrated(topic.getMigratedClusterUrl()));
        } else {
            consumers.forEach(Consumer::reachedEndOfTopic);
        }
    }

    @Override
    public long getFilterProcessedMsgCount() {
        return this.filterProcessedMsgs.longValue();
    }

    @Override
    public long getFilterAcceptedMsgCount() {
        return this.filterAcceptedMsgs.longValue();
    }

    @Override
    public long getFilterRejectedMsgCount() {
        return this.filterRejectedMsgs.longValue();
    }

    @Override
    public long getFilterRescheduledMsgCount() {
        return this.filterRescheduledMsgs.longValue();
    }

    protected final void updatePendingBytesToDispatch(long size) {
        PENDING_BYTES_TO_DISPATCH.inc(size);
    }
}
