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

import static org.apache.bookkeeper.mledger.util.PositionAckSetUtil.andAckSet;
import static org.apache.bookkeeper.mledger.util.PositionAckSetUtil.isAckSetEmpty;
import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.service.persistent.CompactorSubscription;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.EntryFilterWithClassLoader;
import org.apache.pulsar.broker.service.plugin.FilterContext;
import org.apache.pulsar.broker.transaction.pendingack.impl.PendingAckHandleImpl;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshot;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public abstract class AbstractBaseDispatcher implements Dispatcher {

    protected final Subscription subscription;

    protected final ServiceConfiguration serviceConfig;
    protected final boolean dispatchThrottlingOnBatchMessageEnabled;
    /**
     * Entry filters in Broker.
     * Not set to final, for the convenience of testing mock.
     */
    protected ImmutableList<EntryFilterWithClassLoader> entryFilters;
    protected final FilterContext filterContext;

    protected AbstractBaseDispatcher(Subscription subscription, ServiceConfiguration serviceConfig) {
        this.subscription = subscription;
        this.serviceConfig = serviceConfig;
        this.dispatchThrottlingOnBatchMessageEnabled = serviceConfig.isDispatchThrottlingOnBatchMessageEnabled();
        if (subscription != null && subscription.getTopic() != null && MapUtils.isNotEmpty(subscription.getTopic()
                .getBrokerService().getEntryFilters())) {
            this.entryFilters = subscription.getTopic().getBrokerService().getEntryFilters().values().asList();
            this.filterContext = new FilterContext();
        } else {
            this.entryFilters = ImmutableList.of();
            this.filterContext = FilterContext.FILTER_CONTEXT_DISABLED;
        }
    }

    /**
     * Update Entries with the metadata of each entry.
     *
     * @param entries
     * @return
     */
    protected int updateEntryWrapperWithMetadata(EntryWrapper[] entryWrappers, List<Entry> entries) {
        int totalMessages = 0;
        for (int i = 0, entriesSize = entries.size(); i < entriesSize; i++) {
            Entry entry = entries.get(i);
            if (entry == null) {
                continue;
            }

            ByteBuf metadataAndPayload = entry.getDataBuffer();
            MessageMetadata msgMetadata = Commands.peekMessageMetadata(metadataAndPayload, subscription.toString(), -1);
            EntryWrapper entryWrapper = EntryWrapper.get(entry, msgMetadata);
            entryWrappers[i] = entryWrapper;
            int batchSize = msgMetadata.getNumMessagesInBatch();
            totalMessages += batchSize;
        }
        return totalMessages;
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
    public int filterEntriesForConsumer(List<Entry> entries, EntryBatchSizes batchSizes,
            SendMessageInfo sendMessageInfo, EntryBatchIndexesAcks indexesAcks,
            ManagedCursor cursor, boolean isReplayRead, Consumer consumer) {
        return filterEntriesForConsumer(null, 0, entries, batchSizes,
                sendMessageInfo, indexesAcks, cursor,
                isReplayRead, consumer);
    }

    /**
     * Filter entries with prefetched message metadata range so that there is no need to peek metadata from Entry.
     *
     * @param entryWrapper the optional message metadata array. need check if null pass.
     * @param entryWrapperOffset the index in `optMetadataArray` of the first Entry's message metadata
     *
     * @see AbstractBaseDispatcher#filterEntriesForConsumer(List, EntryBatchSizes, SendMessageInfo,
     *   EntryBatchIndexesAcks, ManagedCursor, boolean, Consumer)
     */
    public int filterEntriesForConsumer(@Nullable EntryWrapper[] entryWrapper, int entryWrapperOffset,
                                        List<? extends Entry> entries, EntryBatchSizes batchSizes,
                                        SendMessageInfo sendMessageInfo,
                                        EntryBatchIndexesAcks indexesAcks, ManagedCursor cursor,
                                        boolean isReplayRead, Consumer consumer) {
        int totalMessages = 0;
        long totalBytes = 0;
        int totalChunkedMessages = 0;
        int totalEntries = 0;
        List<Position> entriesToFiltered = CollectionUtils.isNotEmpty(entryFilters) ? new ArrayList<>() : null;
        List<PositionImpl> entriesToRedeliver = CollectionUtils.isNotEmpty(entryFilters) ? new ArrayList<>() : null;
        for (int i = 0, entriesSize = entries.size(); i < entriesSize; i++) {
            Entry entry = entries.get(i);
            if (entry == null) {
                continue;
            }
            ByteBuf metadataAndPayload = entry.getDataBuffer();
            int entryWrapperIndex = i + entryWrapperOffset;
            MessageMetadata msgMetadata = entryWrapper != null  && entryWrapper[entryWrapperIndex] != null
                    ? entryWrapper[entryWrapperIndex].getMetadata()
                    : null;
            msgMetadata = msgMetadata == null
                    ? Commands.peekMessageMetadata(metadataAndPayload, subscription.toString(), -1)
                    : msgMetadata;
            EntryFilter.FilterResult filterResult = EntryFilter.FilterResult.ACCEPT;
            if (CollectionUtils.isNotEmpty(entryFilters)) {
                fillContext(filterContext, msgMetadata, subscription, consumer);
                filterResult = getFilterResult(filterContext, entry, entryFilters);
                if (filterResult == EntryFilter.FilterResult.REJECT) {
                    entriesToFiltered.add(entry.getPosition());
                    entries.set(i, null);
                    entry.release();
                    continue;
                } else if (filterResult == EntryFilter.FilterResult.RESCHEDULE) {
                    entriesToRedeliver.add((PositionImpl) entry.getPosition());
                    entries.set(i, null);
                    entry.release();
                    continue;
                }
            }
            if (msgMetadata != null && msgMetadata.hasTxnidMostBits()
                    && msgMetadata.hasTxnidLeastBits()) {
                if (Markers.isTxnMarker(msgMetadata)) {
                    // because consumer can receive message is smaller than maxReadPosition,
                    // so this marker is useless for this subscription
                    individualAcknowledgeMessageIfNeeded(entry.getPosition(), Collections.emptyMap());
                    entries.set(i, null);
                    entry.release();
                    continue;
                } else if (((PersistentTopic) subscription.getTopic())
                        .isTxnAborted(new TxnID(msgMetadata.getTxnidMostBits(), msgMetadata.getTxnidLeastBits()))) {
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

            totalEntries++;
            int batchSize = msgMetadata.getNumMessagesInBatch();
            totalMessages += batchSize;
            totalBytes += metadataAndPayload.readableBytes();
            totalChunkedMessages += msgMetadata.hasChunkId() ? 1 : 0;
            batchSizes.setBatchSize(i, batchSize);
            long[] ackSet = null;
            if (indexesAcks != null && cursor != null) {
                PositionImpl position = PositionImpl.get(entry.getLedgerId(), entry.getEntryId());
                ackSet = cursor
                        .getDeletedBatchIndexesAsLongArray(position);
                // some batch messages ack bit sit will be in pendingAck state, so don't send all bit sit to consumer
                if (subscription instanceof PersistentSubscription
                        && ((PersistentSubscription) subscription)
                        .getPendingAckHandle() instanceof PendingAckHandleImpl) {
                    PositionImpl positionInPendingAck =
                            ((PersistentSubscription) subscription).getPositionInPendingAck(position);
                    // if this position not in pendingAck state, don't need to do any op
                    if (positionInPendingAck != null) {
                        if (positionInPendingAck.hasAckSet()) {
                            // need to or ackSet in pendingAck state and cursor ackSet which bit sit has been acked
                            if (ackSet != null) {
                                ackSet = andAckSet(ackSet, positionInPendingAck.getAckSet());
                            } else {
                                // if actSet is null, use pendingAck ackSet
                                ackSet = positionInPendingAck.getAckSet();
                            }
                            // if the result of pendingAckSet(in pendingAckHandle) AND the ackSet(in cursor) is empty
                            // filter this entry
                            if (isAckSetEmpty(ackSet)) {
                                entries.set(i, null);
                                entry.release();
                                continue;
                            }
                        } else {
                            // filter non-batch message in pendingAck state
                            entries.set(i, null);
                            entry.release();
                            continue;
                        }
                    }
                }
                if (ackSet != null) {
                    indexesAcks.setIndexesAcks(i, Pair.of(batchSize, ackSet));
                } else {
                    indexesAcks.setIndexesAcks(i, null);
                }
            }

            BrokerInterceptor interceptor = subscription.interceptor();
            if (null != interceptor) {
                interceptor.beforeSendMessage(subscription, entry, ackSet, msgMetadata);
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

    private static EntryFilter.FilterResult getFilterResult(FilterContext filterContext, Entry entry,
                                                            ImmutableList<EntryFilterWithClassLoader> entryFilters) {
        for (EntryFilter entryFilter : entryFilters) {
            EntryFilter.FilterResult filterResult =
                    entryFilter.filterEntry(entry, filterContext);
            if (filterResult == null) {
                filterResult = EntryFilter.FilterResult.ACCEPT;
            }
            if (filterResult != EntryFilter.FilterResult.ACCEPT) {
                return filterResult;
            }
        }
        return EntryFilter.FilterResult.ACCEPT;
    }

    private void fillContext(FilterContext context, MessageMetadata msgMetadata,
                             Subscription subscription, Consumer consumer) {
        context.reset();
        context.setMsgMetadata(msgMetadata);
        context.setSubscription(subscription);
        context.setConsumer(consumer);
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
}
