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
package org.apache.pulsar.broker.service.nonpersistent;

import com.google.common.base.MoreObjects;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.service.AbstractSubscription;
import org.apache.pulsar.broker.service.AnalyzeBacklogResult;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.BrokerServiceException.ServerMetadataException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionFencedException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.GetStatsOptions;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.policies.data.stats.NonPersistentSubscriptionStatsImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NonPersistentSubscription extends AbstractSubscription {
    private final NonPersistentTopic topic;
    private volatile NonPersistentDispatcher dispatcher;
    private final String topicName;
    private final String subName;
    private final String fullName;

    private static final int FALSE = 0;
    private static final int TRUE = 1;
    private static final AtomicIntegerFieldUpdater<NonPersistentSubscription> IS_FENCED_UPDATER =
            AtomicIntegerFieldUpdater
                    .newUpdater(NonPersistentSubscription.class, "isFenced");
    @SuppressWarnings("unused")
    private volatile int isFenced = FALSE;

    private volatile Map<String, String> subscriptionProperties;

    private KeySharedMode keySharedMode = null;

    public NonPersistentSubscription(NonPersistentTopic topic, String subscriptionName,
                                     Map<String, String> properties) {
        this.topic = topic;
        this.topicName = topic.getName();
        this.subName = subscriptionName;
        this.fullName = MoreObjects.toStringHelper(this).add("topic", topicName).add("name", subName).toString();
        IS_FENCED_UPDATER.set(this, FALSE);
        this.subscriptionProperties = properties != null
                ? Collections.unmodifiableMap(properties) : Collections.emptyMap();
    }

    @Override
    public BrokerInterceptor interceptor() {
        return this.topic.getBrokerService().getInterceptor();
    }

    @Override
    public String getName() {
        return this.subName;
    }

    @Override
    public Topic getTopic() {
        return topic;
    }

    @Override
    public boolean isReplicated() {
        return false;
    }

    @Override
    public synchronized CompletableFuture<Void> addConsumer(Consumer consumer) {
        if (IS_FENCED_UPDATER.get(this) == TRUE) {
            log.warn("Attempting to add consumer {} on a fenced subscription", consumer);
            return FutureUtil.failedFuture(new SubscriptionFencedException("Subscription is fenced"));
        }

        if (dispatcher == null || !dispatcher.isConsumerConnected()) {
            Dispatcher previousDispatcher = null;

            switch (consumer.subType()) {
            case Exclusive:
                if (dispatcher == null || dispatcher.getType() != SubType.Exclusive) {
                    previousDispatcher = dispatcher;
                    dispatcher = new NonPersistentDispatcherSingleActiveConsumer(SubType.Exclusive, 0, topic, this);
                }
                break;
            case Shared:
                if (dispatcher == null || dispatcher.getType() != SubType.Shared) {
                    previousDispatcher = dispatcher;
                    dispatcher = new NonPersistentDispatcherMultipleConsumers(topic, this);
                }
                break;
            case Failover:
                int partitionIndex = TopicName.getPartitionIndex(topicName);
                if (partitionIndex < 0) {
                    // For non partition topics, assume index 0 to pick a predictable consumer
                    partitionIndex = 0;
                }

                if (dispatcher == null || dispatcher.getType() != SubType.Failover) {
                    previousDispatcher = dispatcher;
                    dispatcher = new NonPersistentDispatcherSingleActiveConsumer(SubType.Failover, partitionIndex,
                            topic, this);
                }
                break;
            case Key_Shared:
                KeySharedMeta ksm = consumer.getKeySharedMeta();
                if (dispatcher == null || dispatcher.getType() != SubType.Key_Shared
                        || !((NonPersistentStickyKeyDispatcherMultipleConsumers) dispatcher)
                                .hasSameKeySharedPolicy(ksm)) {
                    previousDispatcher = dispatcher;
                    this.dispatcher = new NonPersistentStickyKeyDispatcherMultipleConsumers(topic, this, ksm);
                }
                break;
            default:
                return FutureUtil.failedFuture(new ServerMetadataException("Unsupported subscription type"));
            }

            if (previousDispatcher != null) {
                previousDispatcher.close().thenRun(() -> {
                    log.info("[{}][{}] Successfully closed previous dispatcher", topicName, subName);
                }).exceptionally(ex -> {
                    log.error("[{}][{}] Failed to close previous dispatcher", topicName, subName, ex);
                    return null;
                });
            }
        } else {
            Optional<CompletableFuture<Void>> compatibilityError =
                    checkForConsumerCompatibilityErrorWithDispatcher(dispatcher, consumer);
            if (compatibilityError.isPresent()) {
                return compatibilityError.get();
            }
        }

        return dispatcher.addConsumer(consumer);
    }

    @Override
    public synchronized void removeConsumer(Consumer consumer, boolean isResetCursor) throws BrokerServiceException {
        if (dispatcher != null) {
            dispatcher.removeConsumer(consumer);
        }
        // preserve accumulative stats form removed consumer
        ConsumerStatsImpl stats = consumer.getStats();
        bytesOutFromRemovedConsumers.add(stats.bytesOutCounter);
        msgOutFromRemovedConsumer.add(stats.msgOutCounter);
        // Unsubscribe when all the consumers disconnected.
        if (dispatcher != null && CollectionUtils.isEmpty(dispatcher.getConsumers())) {
            topic.unsubscribe(subName);
        }

        // invalid consumer remove will throw an exception
        // decrement usage is triggered only for valid consumer close
        topic.decrementUsageCount();
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] [{}] Removed consumer -- count: {}", topic.getName(), subName, consumer.consumerName(),
                    topic.currentUsageCount());
        }
    }

    @Override
    public void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {
        dispatcher.consumerFlow(consumer, additionalNumberOfMessages);
    }

    @Override
    public void acknowledgeMessage(List<Position> position, AckType ackType, Map<String, Long> properties) {
        // No-op
    }

    @Override
    public String toString() {
        return fullName;
    }

    @Override
    public String getTopicName() {
        return this.topicName;
    }

    @Override
    public SubType getType() {
        return dispatcher != null ? dispatcher.getType() : null;
    }

    @Override
    public String getTypeString() {
        SubType type = getType();
        if (type == null) {
            return "None";
        }

        switch (type) {
        case Exclusive:
            return "Exclusive";
        case Failover:
            return "Failover";
        case Shared:
            return "Shared";
        case Key_Shared:
            return "Key_Shared";
        }

        return "Null";
    }

    @Override
    public CompletableFuture<Void> clearBacklog() {
        // No-op
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> skipMessages(int numMessagesToSkip) {
        // No-op
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> resetCursor(long timestamp) {
        // No-op
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Entry> peekNthMessage(int messagePosition) {
        // No-op
        return CompletableFuture.completedFuture(null); // TODO: throw exception
    }

    @Override
    public long getNumberOfEntriesInBacklog(boolean getPreciseBacklog) {
        // No-op
        return 0;
    }

    @Override
    public NonPersistentDispatcher getDispatcher() {
        return this.dispatcher;
    }

    @Override
    public boolean isSubscriptionMigrated() {
        return topic.isMigrated();
    }

    /**
     * Disconnect all consumers from this subscription.
     *
     * @return CompletableFuture indicating the completion of the operation.
     */
    @Override
    public synchronized CompletableFuture<Void> disconnect(Optional<BrokerLookupData> assignedBrokerLookupData) {
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        (dispatcher != null
                ? dispatcher.disconnectAllConsumers(false, assignedBrokerLookupData)
                : CompletableFuture.completedFuture(null))
                .thenRun(() -> {
                    log.info("[{}][{}] Successfully disconnected subscription consumers", topicName, subName);
                    closeFuture.complete(null);
                }).exceptionally(exception -> {
                    log.error("[{}][{}] Error disconnecting subscription consumers", topicName, subName, exception);
                    closeFuture.completeExceptionally(exception);
                    return null;
                });

        return closeFuture;

    }

    private CompletableFuture<Void> fence() {
        IS_FENCED_UPDATER.set(this, TRUE);
        return CompletableFuture.completedFuture(null);
    }


    /**
     * Fence this subscription and optionally disconnect all consumers.
     *
     * @return CompletableFuture indicating the completion of the operation.
     */
    @Override
    public synchronized CompletableFuture<Void> close(boolean disconnectConsumers,
                                                      Optional<BrokerLookupData> assignedBrokerLookupData) {
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        // block any further consumers on this subscription
        IS_FENCED_UPDATER.set(this, TRUE);

        (dispatcher != null
                ? dispatcher.close(disconnectConsumers, assignedBrokerLookupData)
                : CompletableFuture.completedFuture(null))
                .thenRun(() -> {
                    log.info("[{}][{}] Successfully closed subscription", topicName, subName);
                    closeFuture.complete(null);
                }).exceptionally(exception -> {
                    IS_FENCED_UPDATER.set(this, FALSE);
                    if (dispatcher != null) {
                        dispatcher.reset();
                    }
                    log.error("[{}][{}] Error closing subscription", topicName, subName, exception);
                    closeFuture.completeExceptionally(exception);
                    return null;
                });

        return closeFuture;
    }

    /**
     * Delete the subscription by closing and deleting its managed cursor if no consumers are connected to it. Handle
     * unsubscribe call from admin layer.
     *
     * @return CompletableFuture indicating the completion of delete operation
     */
    @Override
    public CompletableFuture<Void> delete() {
        return delete(false);
    }

    /**
     * Forcefully close all consumers and deletes the subscription.
     * @return
     */
    @Override
    public CompletableFuture<Void> deleteForcefully() {
        return delete(true);
    }

    /**
     * Delete the subscription by closing and deleting its managed cursor. Handle unsubscribe call from admin layer.
     *
     * @param closeIfConsumersConnected
     *            Flag indicate whether explicitly close connected consumers before trying to delete subscription. If
     *            any consumer is connected to it and if this flag is disable then this operation fails.
     * @return CompletableFuture indicating the completion of delete operation
     */
    private CompletableFuture<Void> delete(boolean closeIfConsumersConnected) {
        CompletableFuture<Void> deleteFuture = new CompletableFuture<>();

        log.info("[{}][{}] Unsubscribing", topicName, subName);

        CompletableFuture<Void> closeSubscriptionFuture = new CompletableFuture<>();

        if (closeIfConsumersConnected) {
            this.close(true, Optional.empty()).thenRun(() -> {
                closeSubscriptionFuture.complete(null);
            }).exceptionally(ex -> {
                log.error("[{}][{}] Error disconnecting and closing subscription", topicName, subName, ex);
                closeSubscriptionFuture.completeExceptionally(ex);
                return null;
            });
        } else {
            this.fence().thenRun(() -> {
                closeSubscriptionFuture.complete(null);
            }).exceptionally(exception -> {
                log.error("[{}][{}] Error closing subscription", topicName, subName, exception);
                closeSubscriptionFuture.completeExceptionally(exception);
                return null;
            });
        }

        // cursor close handles pending delete (ack) operations
        closeSubscriptionFuture.thenCompose(v -> topic.unsubscribe(subName)).thenAccept(v -> {
            synchronized (this) {
                (dispatcher != null ? dispatcher.close() : CompletableFuture.completedFuture(null)).thenRun(() -> {
                    log.info("[{}][{}] Successfully deleted subscription", topicName, subName);
                    deleteFuture.complete(null);
                }).exceptionally(ex -> {
                    IS_FENCED_UPDATER.set(this, FALSE);
                    if (dispatcher != null) {
                        dispatcher.reset();
                    }
                    log.error("[{}][{}] Error deleting subscription", topicName, subName, ex);
                    deleteFuture.completeExceptionally(ex);
                    return null;
                });
            }
        }).exceptionally(exception -> {
            IS_FENCED_UPDATER.set(this, FALSE);
            log.error("[{}][{}] Error deleting subscription", topicName, subName, exception);
            deleteFuture.completeExceptionally(exception);
            return null;
        });

        return deleteFuture;
    }

    /**
     * Handle unsubscribe command from the client API Check with the dispatcher is this consumer can proceed with
     * unsubscribe.
     *
     * @param consumer consumer object that is initiating the unsubscribe operation
     * @return CompletableFuture indicating the completion of ubsubscribe operation
     */
    @Override
    public CompletableFuture<Void> doUnsubscribe(Consumer consumer) {
        return doUnsubscribe(consumer, false);
    }

    /**
     * Handle unsubscribe command from the client API Check with the dispatcher is this consumer can proceed with
     * unsubscribe.
     *
     * @param consumer consumer object that is initiating the unsubscribe operation
     * @param force unsubscribe forcefully by disconnecting consumers and closing subscription
     * @return CompletableFuture indicating the completion of unsubscribe operation
     */
    @Override
    public CompletableFuture<Void> doUnsubscribe(Consumer consumer, boolean force) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            if (force || dispatcher.canUnsubscribe(consumer)) {
                consumer.close();
                return delete(force);
            }
            future.completeExceptionally(
                    new ServerMetadataException("Unconnected or shared consumer attempting to unsubscribe"));
        } catch (BrokerServiceException e) {
            log.warn("Error removing consumer {}", consumer);
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public List<Consumer> getConsumers() {
        Dispatcher dispatcher = this.dispatcher;
        if (dispatcher != null) {
            return dispatcher.getConsumers();
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public boolean expireMessages(int messageTTLInSeconds) {
        throw new UnsupportedOperationException("Expire message by timestamp is not supported for"
                + " non-persistent topic.");
    }

    @Override
    public CompletableFuture<Boolean> expireMessagesAsync(int messageTTLInSeconds) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Expire message by timestamp is not"
                + " supported for non-persistent topic."));
    }

    @Override
    public boolean expireMessages(Position position) {
        throw new UnsupportedOperationException("Expire message by position is not supported for"
                + " non-persistent topic.");
    }

    public NonPersistentSubscriptionStatsImpl getStats(GetStatsOptions getStatsOptions) {
        NonPersistentSubscriptionStatsImpl subStats = new NonPersistentSubscriptionStatsImpl();
        subStats.bytesOutCounter = bytesOutFromRemovedConsumers.longValue();
        subStats.msgOutCounter = msgOutFromRemovedConsumer.longValue();

        NonPersistentDispatcher dispatcher = this.dispatcher;
        if (dispatcher != null) {
            dispatcher.getConsumers().forEach(consumer -> {
                ConsumerStatsImpl consumerStats = consumer.getStats();
                if (!getStatsOptions.isExcludeConsumers()) {
                    subStats.consumers.add(consumerStats);
                }
                subStats.msgRateOut += consumerStats.msgRateOut;
                subStats.messageAckRate += consumerStats.messageAckRate;
                subStats.msgThroughputOut += consumerStats.msgThroughputOut;
                subStats.bytesOutCounter += consumerStats.bytesOutCounter;
                subStats.msgOutCounter += consumerStats.msgOutCounter;
                subStats.msgRateRedeliver += consumerStats.msgRateRedeliver;
            });

            subStats.filterProcessedMsgCount = dispatcher.getFilterProcessedMsgCount();
            subStats.filterAcceptedMsgCount = dispatcher.getFilterAcceptedMsgCount();
            subStats.filterRejectedMsgCount = dispatcher.getFilterRejectedMsgCount();
            subStats.filterRescheduledMsgCount = dispatcher.getFilterRescheduledMsgCount();
            subStats.dispatchThrottledMsgEventsBySubscriptionLimit =
                    dispatcher.getDispatchThrottledMsgEventsBySubscriptionLimit();
            subStats.dispatchThrottledBytesEventsBySubscriptionLimit =
                    dispatcher.getDispatchThrottledBytesBySubscriptionLimit();
            subStats.dispatchThrottledMsgEventsByBrokerLimit =
                    dispatcher.getDispatchThrottledMsgEventsByBrokerLimit();
            subStats.dispatchThrottledBytesEventsByBrokerLimit =
                    dispatcher.getDispatchThrottledBytesEventsByBrokerLimit();
            subStats.dispatchThrottledMsgEventsByTopicLimit =
                    dispatcher.getDispatchThrottledMsgEventsByTopicLimit();
            subStats.dispatchThrottledBytesEventsByTopicLimit =
                    dispatcher.getDispatchThrottledBytesEventsByTopicLimit();
        }

        subStats.type = getTypeString();
        subStats.msgDropRate = dispatcher.getMessageDropRate().getValueRate();

        KeySharedMode keySharedMode = this.keySharedMode;
        if (getType() == SubType.Key_Shared && keySharedMode != null) {
            subStats.keySharedMode = keySharedMode.toString();
        }

        return subStats;
    }

    @Override
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer, long consumerEpoch) {
     // No-op
    }

    @Override
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer, List<Position> positions) {
        // No-op
    }

    @Override
    public void addUnAckedMessages(int unAckMessages) {
        // No-op
    }

    @Override
    public double getExpiredMessageRate() {
        // No-op
        return 0;
    }

    @Override
    public void markTopicWithBatchMessagePublished() {
        topic.markBatchMessagePublished();
    }

    @Override
    public CompletableFuture<Void> resetCursor(Position position) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> endTxn(long txnidMostBits, long txnidLeastBits, int txnAction, long lowWaterMark) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(
                new Exception("Unsupported operation end txn for NonPersistentSubscription"));
        return completableFuture;
    }

    @Override
    public CompletableFuture<AnalyzeBacklogResult> analyzeBacklog(Optional<Position> position) {
        CompletableFuture<AnalyzeBacklogResult> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(
                new Exception("Unsupported operation analyzeBacklog for NonPersistentSubscription"));
        return completableFuture;
    }

    private static final Logger log = LoggerFactory.getLogger(NonPersistentSubscription.class);

    public Map<String, String> getSubscriptionProperties() {
        return subscriptionProperties;
    }

    @Override
    public CompletableFuture<Void> updateSubscriptionProperties(Map<String, String> subscriptionProperties) {
        if (subscriptionProperties == null || subscriptionProperties.isEmpty()) {
          this.subscriptionProperties = Collections.emptyMap();
        } else {
           this.subscriptionProperties = Collections.unmodifiableMap(subscriptionProperties);
        }
        return CompletableFuture.completedFuture(null);
    }

}
