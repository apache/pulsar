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

import static org.apache.pulsar.common.events.EventsTopicNames.checkTopicIsEventsNames;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.stream.Collectors;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ClearBacklogCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.IndividualDeletedEntries;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ConcurrentFindCursorPositionException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.InvalidCursorPositionException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.service.AbstractSubscription;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.BrokerServiceException.NotAllowedException;
import org.apache.pulsar.broker.service.BrokerServiceException.ServerMetadataException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionFencedException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionInvalidCursorPosition;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckHandle;
import org.apache.pulsar.broker.transaction.pendingack.impl.PendingAckHandleDisabled;
import org.apache.pulsar.broker.transaction.pendingack.impl.PendingAckHandleImpl;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshot;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TransactionInPendingAckStats;
import org.apache.pulsar.common.policies.data.TransactionPendingAckStats;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.policies.data.stats.SubscriptionStatsImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentSubscription extends AbstractSubscription implements Subscription {
    protected final PersistentTopic topic;
    protected final ManagedCursor cursor;
    protected volatile Dispatcher dispatcher;
    protected final String topicName;
    protected final String subName;
    protected final String fullName;

    private static final int FALSE = 0;
    private static final int TRUE = 1;
    private static final AtomicIntegerFieldUpdater<PersistentSubscription> IS_FENCED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PersistentSubscription.class, "isFenced");
    private volatile int isFenced = FALSE;
    private PersistentMessageExpiryMonitor expiryMonitor;

    private long lastExpireTimestamp = 0L;
    private long lastConsumedFlowTimestamp = 0L;
    private long lastMarkDeleteAdvancedTimestamp = 0L;

    // for connected subscriptions, message expiry will be checked if the backlog is greater than this threshold
    private static final int MINIMUM_BACKLOG_FOR_EXPIRY_CHECK = 1000;

    private static final String REPLICATED_SUBSCRIPTION_PROPERTY = "pulsar.replicated.subscription";

    // Map of properties that is used to mark this subscription as "replicated".
    // Since this is the only field at this point, we can just keep a static
    // instance of the map.
    private static final Map<String, Long> REPLICATED_SUBSCRIPTION_CURSOR_PROPERTIES = new TreeMap<>();
    private static final Map<String, Long> NON_REPLICATED_SUBSCRIPTION_CURSOR_PROPERTIES = Collections.emptyMap();

    private volatile ReplicatedSubscriptionSnapshotCache replicatedSubscriptionSnapshotCache;
    private final PendingAckHandle pendingAckHandle;
    private volatile Map<String, String> subscriptionProperties;
    private volatile CompletableFuture<Void> fenceFuture;

    static {
        REPLICATED_SUBSCRIPTION_CURSOR_PROPERTIES.put(REPLICATED_SUBSCRIPTION_PROPERTY, 1L);
    }

    static Map<String, Long> getBaseCursorProperties(boolean isReplicated) {
        return isReplicated ? REPLICATED_SUBSCRIPTION_CURSOR_PROPERTIES : NON_REPLICATED_SUBSCRIPTION_CURSOR_PROPERTIES;
    }

    static boolean isCursorFromReplicatedSubscription(ManagedCursor cursor) {
        return cursor.getProperties().containsKey(REPLICATED_SUBSCRIPTION_PROPERTY);
    }

    public PersistentSubscription(PersistentTopic topic, String subscriptionName, ManagedCursor cursor,
                                                         boolean replicated) {
        this(topic, subscriptionName, cursor, replicated, Collections.emptyMap());
    }

    public PersistentSubscription(PersistentTopic topic, String subscriptionName, ManagedCursor cursor,
                                  boolean replicated, Map<String, String> subscriptionProperties) {
        this.topic = topic;
        this.cursor = cursor;
        this.topicName = topic.getName();
        this.subName = subscriptionName;
        this.fullName = MoreObjects.toStringHelper(this).add("topic", topicName).add("name", subName).toString();
        this.expiryMonitor = new PersistentMessageExpiryMonitor(topic, subscriptionName, cursor, this);
        this.setReplicated(replicated);
        this.subscriptionProperties = MapUtils.isEmpty(subscriptionProperties)
                ? Collections.emptyMap() : Collections.unmodifiableMap(subscriptionProperties);
        if (topic.getBrokerService().getPulsar().getConfig().isTransactionCoordinatorEnabled()
                && !checkTopicIsEventsNames(TopicName.get(topicName))) {
            this.pendingAckHandle = new PendingAckHandleImpl(this);
        } else {
            this.pendingAckHandle = new PendingAckHandleDisabled();
        }
        IS_FENCED_UPDATER.set(this, FALSE);
    }

    public void updateLastMarkDeleteAdvancedTimestamp() {
        this.lastMarkDeleteAdvancedTimestamp =
            Math.max(this.lastMarkDeleteAdvancedTimestamp, System.currentTimeMillis());
    }

    @Override
    public BrokerInterceptor interceptor() {
        return topic.getBrokerService().getInterceptor();
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
        return replicatedSubscriptionSnapshotCache != null;
    }

    public boolean setReplicated(boolean replicated) {
        ServiceConfiguration config = topic.getBrokerService().getPulsar().getConfig();

        if (!replicated || !config.isEnableReplicatedSubscriptions()) {
            this.replicatedSubscriptionSnapshotCache = null;
        } else if (this.replicatedSubscriptionSnapshotCache == null) {
            this.replicatedSubscriptionSnapshotCache = new ReplicatedSubscriptionSnapshotCache(subName,
                    config.getReplicatedSubscriptionsSnapshotMaxCachedPerSubscription());
        }

        if (this.cursor != null) {
            if (replicated) {
                if (!config.isEnableReplicatedSubscriptions()) {
                    log.warn("[{}][{}] Failed set replicated subscription status to {}, please enable the "
                            + "configuration enableReplicatedSubscriptions", topicName, subName, replicated);
                } else {
                    return this.cursor.putProperty(REPLICATED_SUBSCRIPTION_PROPERTY, 1L);
                }
            } else {
                return this.cursor.removeProperty(REPLICATED_SUBSCRIPTION_PROPERTY);
            }
        }

        return false;
    }

    @Override
    public CompletableFuture<Void> addConsumer(Consumer consumer) {
        return pendingAckHandle.pendingAckHandleFuture().thenCompose(future -> {
            synchronized (PersistentSubscription.this) {
                cursor.updateLastActive();
                if (IS_FENCED_UPDATER.get(this) == TRUE) {
                    log.warn("Attempting to add consumer {} on a fenced subscription", consumer);
                    return FutureUtil.failedFuture(new SubscriptionFencedException("Subscription is fenced"));
                }

                if (dispatcher == null || !dispatcher.isConsumerConnected()) {
                    Dispatcher previousDispatcher = null;
                    boolean useStreamingDispatcher = topic.getBrokerService().getPulsar()
                            .getConfiguration().isStreamingDispatch();
                    switch (consumer.subType()) {
                        case Exclusive:
                            if (dispatcher == null || dispatcher.getType() != SubType.Exclusive) {
                                previousDispatcher = dispatcher;
                                dispatcher = useStreamingDispatcher
                                        ? new PersistentStreamingDispatcherSingleActiveConsumer(
                                                cursor, SubType.Exclusive, 0, topic, this)
                                        : new PersistentDispatcherSingleActiveConsumer(
                                                cursor, SubType.Exclusive, 0, topic, this);
                            }
                            break;
                        case Shared:
                            if (dispatcher == null || dispatcher.getType() != SubType.Shared) {
                                previousDispatcher = dispatcher;
                                dispatcher = useStreamingDispatcher
                                        ? new PersistentStreamingDispatcherMultipleConsumers(
                                                topic, cursor, this)
                                        : new PersistentDispatcherMultipleConsumers(topic, cursor, this);
                            }
                            break;
                        case Failover:
                            int partitionIndex = TopicName.getPartitionIndex(topicName);
                            if (partitionIndex < 0) {
                                // For non partition topics, use a negative index so
                                // dispatcher won't sort consumers before picking
                                // an active consumer for the topic.
                                partitionIndex = -1;
                            }

                            if (dispatcher == null || dispatcher.getType() != SubType.Failover) {
                                previousDispatcher = dispatcher;
                                dispatcher = useStreamingDispatcher
                                        ? new PersistentStreamingDispatcherSingleActiveConsumer(
                                                cursor, SubType.Failover, partitionIndex, topic, this) :
                                        new PersistentDispatcherSingleActiveConsumer(cursor, SubType.Failover,
                                                partitionIndex, topic, this);
                            }
                            break;
                        case Key_Shared:
                            KeySharedMeta ksm = consumer.getKeySharedMeta();
                            if (dispatcher == null || dispatcher.getType() != SubType.Key_Shared
                                    || !((PersistentStickyKeyDispatcherMultipleConsumers) dispatcher)
                                            .hasSameKeySharedPolicy(ksm)) {
                                previousDispatcher = dispatcher;
                                dispatcher = new PersistentStickyKeyDispatcherMultipleConsumers(topic, cursor, this,
                                        topic.getBrokerService().getPulsar().getConfiguration(), ksm);
                            }
                            break;
                        default:
                            return FutureUtil.failedFuture(
                                    new ServerMetadataException("Unsupported subscription type"));
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
                    if (consumer.subType() != dispatcher.getType()) {
                        return FutureUtil.failedFuture(
                                new SubscriptionBusyException("Subscription is of different type"));
                    }
                }

                try {
                    dispatcher.addConsumer(consumer);
                    return CompletableFuture.completedFuture(null);
                } catch (BrokerServiceException brokerServiceException) {
                    return FutureUtil.failedFuture(brokerServiceException);
                }
            }
        });
    }

    @Override
    public synchronized void removeConsumer(Consumer consumer, boolean isResetCursor) throws BrokerServiceException {
        cursor.updateLastActive();
        if (dispatcher != null) {
            dispatcher.removeConsumer(consumer);
        }

        // preserve accumulative stats form removed consumer
        ConsumerStatsImpl stats = consumer.getStats();
        bytesOutFromRemovedConsumers.add(stats.bytesOutCounter);
        msgOutFromRemovedConsumer.add(stats.msgOutCounter);

        if (dispatcher != null && dispatcher.getConsumers().isEmpty()) {
            deactivateCursor();
            topic.getManagedLedger().removeWaitingCursor(cursor);

            if (!cursor.isDurable()) {
                // If cursor is not durable, we need to clean up the subscription as well
                this.close().thenRun(() -> {
                    synchronized (this) {
                        if (dispatcher != null) {
                            dispatcher.close().thenRun(() -> {
                                log.info("[{}][{}] Successfully closed dispatcher for reader", topicName, subName);
                            }).exceptionally(ex -> {
                                log.error("[{}][{}] Failed to close dispatcher for reader", topicName, subName, ex);
                                return null;
                            });
                        }
                    }
                }).exceptionally(exception -> {
                    log.error("[{}][{}] Failed to close subscription for reader", topicName, subName, exception);
                    return null;
                });

                // when topic closes: it iterates through concurrent-subscription map to close each subscription. so,
                // topic.remove again try to access same map which creates deadlock. so, execute it in different thread.
                topic.getBrokerService().pulsar().getExecutor().submit(() ->{
                    topic.removeSubscription(subName);
                    // Also need remove the cursor here, otherwise the data deletion will not work well.
                    // Because data deletion depends on the mark delete position of all cursors.
                    if (!isResetCursor) {
                        try {
                            topic.getManagedLedger().deleteCursor(cursor.getName());
                        } catch (InterruptedException | ManagedLedgerException e) {
                            log.warn("[{}] [{}] Failed to remove non durable cursor", topic.getName(), subName, e);
                        }
                    }
                });
            }
        }

        // invalid consumer remove will throw an exception
        // decrement usage is triggered only for valid consumer close
        topic.decrementUsageCount();
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] [{}] Removed consumer -- count: {}", topic.getName(), subName, consumer.consumerName(),
                    topic.currentUsageCount());
        }
    }

    public void deactivateCursor() {
        this.cursor.setInactive();
    }

    @Override
    public void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {
        this.lastConsumedFlowTimestamp = System.currentTimeMillis();
        dispatcher.consumerFlow(consumer, additionalNumberOfMessages);
    }

    @Override
    public void acknowledgeMessage(List<Position> positions, AckType ackType, Map<String, Long> properties) {
        Position previousMarkDeletePosition = cursor.getMarkDeletedPosition();

        if (ackType == AckType.Cumulative) {

            if (positions.size() != 1) {
                log.warn("[{}][{}] Invalid cumulative ack received with multiple message ids.", topicName, subName);
                return;
            }

            Position position = positions.get(0);
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Cumulative ack on {}", topicName, subName, position);
            }
            cursor.asyncMarkDelete(position, mergeCursorProperties(properties),
                    markDeleteCallback, previousMarkDeletePosition);

        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Individual acks on {}", topicName, subName, positions);
            }
            cursor.asyncDelete(positions, deleteCallback, previousMarkDeletePosition);
            if (topic.getBrokerService().getPulsar().getConfig().isTransactionCoordinatorEnabled()) {
                positions.forEach(position -> {
                    if (((ManagedCursorImpl) cursor).isMessageDeleted(position)) {
                        pendingAckHandle.clearIndividualPosition(position);
                    }
                });
            }

            if (dispatcher != null) {
                dispatcher.getRedeliveryTracker().removeBatch(positions);
            }
        }

        if (!cursor.getMarkDeletedPosition().equals(previousMarkDeletePosition)) {
            this.updateLastMarkDeleteAdvancedTimestamp();

            // Mark delete position advance
            ReplicatedSubscriptionSnapshotCache snapshotCache  = this.replicatedSubscriptionSnapshotCache;
            if (snapshotCache != null) {
                ReplicatedSubscriptionsSnapshot snapshot = snapshotCache
                        .advancedMarkDeletePosition((PositionImpl) cursor.getMarkDeletedPosition());
                if (snapshot != null) {
                    topic.getReplicatedSubscriptionController()
                            .ifPresent(c -> c.localSubscriptionUpdated(subName, snapshot));
                }
            }
        }

        if (topic.getManagedLedger().isTerminated() && cursor.getNumberOfEntriesInBacklog(false) == 0) {
            // Notify all consumer that the end of topic was reached
            if (dispatcher != null) {
                dispatcher.getConsumers().forEach(Consumer::reachedEndOfTopic);
            }
        }
    }

    public CompletableFuture<Void> transactionIndividualAcknowledge(
            TxnID txnId,
            List<MutablePair<PositionImpl, Integer>> positions) {
        return pendingAckHandle.individualAcknowledgeMessage(txnId, positions);
    }

    public CompletableFuture<Void> transactionCumulativeAcknowledge(TxnID txnId, List<PositionImpl> positions) {
        return pendingAckHandle.cumulativeAcknowledgeMessage(txnId, positions);
    }

    private final MarkDeleteCallback markDeleteCallback = new MarkDeleteCallback() {
        @Override
        public void markDeleteComplete(Object ctx) {
            PositionImpl oldMD = (PositionImpl) ctx;
            PositionImpl newMD = (PositionImpl) cursor.getMarkDeletedPosition();
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Mark deleted messages to position {} from position {}",
                        topicName, subName, newMD, oldMD);
            }
            // Signal the dispatchers to give chance to take extra actions
            notifyTheMarkDeletePositionMoveForwardIfNeeded(oldMD);
        }

        @Override
        public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
            // TODO: cut consumer connection on markDeleteFailed
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Failed to mark delete for position {}: {}", topicName, subName, ctx, exception);
            }
        }
    };

    private final DeleteCallback deleteCallback = new DeleteCallback() {
        @Override
        public void deleteComplete(Object position) {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Deleted message at {}", topicName, subName, position);
            }
            // Signal the dispatchers to give chance to take extra actions
            notifyTheMarkDeletePositionMoveForwardIfNeeded((PositionImpl) position);
        }

        @Override
        public void deleteFailed(ManagedLedgerException exception, Object ctx) {
            log.warn("[{}][{}] Failed to delete message at {}: {}", topicName, subName, ctx, exception);
        }
    };

    private void notifyTheMarkDeletePositionMoveForwardIfNeeded(Position oldPosition) {
        PositionImpl oldMD = (PositionImpl) oldPosition;
        PositionImpl newMD = (PositionImpl) cursor.getMarkDeletedPosition();
        if (dispatcher != null && newMD.compareTo(oldMD) > 0) {
            dispatcher.markDeletePositionMoveForward();
        }
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
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Backlog size before clearing: {}", topicName, subName,
                    cursor.getNumberOfEntriesInBacklog(false));
        }

        cursor.asyncClearBacklog(new ClearBacklogCallback() {
            @Override
            public void clearBacklogComplete(Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Backlog size after clearing: {}", topicName, subName,
                            cursor.getNumberOfEntriesInBacklog(false));
                }
                if (dispatcher != null) {
                    dispatcher.clearDelayedMessages();
                }
                future.complete(null);
            }

            @Override
            public void clearBacklogFailed(ManagedLedgerException exception, Object ctx) {
                log.error("[{}][{}] Failed to clear backlog", topicName, subName, exception);
                future.completeExceptionally(exception);
            }
        }, null);

        return future;
    }

    @Override
    public CompletableFuture<Void> skipMessages(int numMessagesToSkip) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Skipping {} messages, current backlog {}", topicName, subName, numMessagesToSkip,
                    cursor.getNumberOfEntriesInBacklog(false));
        }
        cursor.asyncSkipEntries(numMessagesToSkip, IndividualDeletedEntries.Exclude,
                new AsyncCallbacks.SkipEntriesCallback() {
                    @Override
                    public void skipEntriesComplete(Object ctx) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}][{}] Skipped {} messages, new backlog {}", topicName, subName,
                                    numMessagesToSkip, cursor.getNumberOfEntriesInBacklog(false));
                        }
                        future.complete(null);
                    }

                    @Override
                    public void skipEntriesFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("[{}][{}] Failed to skip {} messages", topicName, subName, numMessagesToSkip,
                                exception);
                        future.completeExceptionally(exception);
                    }
                }, null);

        return future;
    }

    @Override
    public CompletableFuture<Void> resetCursor(long timestamp) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        PersistentMessageFinder persistentMessageFinder = new PersistentMessageFinder(topicName, cursor);

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Resetting subscription to timestamp {}", topicName, subName, timestamp);
        }
        persistentMessageFinder.findMessages(timestamp, new AsyncCallbacks.FindEntryCallback() {
            @Override
            public void findEntryComplete(Position position, Object ctx) {
                final Position finalPosition;
                if (position == null) {
                    // this should not happen ideally unless a reset is requested for a time
                    // that spans beyond the retention limits (time/size)
                    finalPosition = cursor.getFirstPosition();
                    if (finalPosition == null) {
                        log.warn("[{}][{}] Unable to find position for timestamp {}."
                                        + " Unable to reset cursor to first position",
                                topicName, subName, timestamp);
                        future.completeExceptionally(
                                new SubscriptionInvalidCursorPosition(
                                        "Unable to find position for specified timestamp"));
                        return;
                    }
                    log.info(
                            "[{}][{}] Unable to find position for timestamp {}."
                                    + " Resetting cursor to first position {} in ledger",
                            topicName, subName, timestamp, finalPosition);
                } else {
                    finalPosition = position.getNext();
                }
                resetCursor(finalPosition, future);
            }

            @Override
            public void findEntryFailed(ManagedLedgerException exception,
                                        Optional<Position> failedReadPosition, Object ctx) {
                // todo - what can go wrong here that needs to be retried?
                if (exception instanceof ConcurrentFindCursorPositionException) {
                    future.completeExceptionally(new SubscriptionBusyException(exception.getMessage()));
                } else {
                    future.completeExceptionally(new BrokerServiceException(exception));
                }
            }
        });

        return future;
    }

    @Override
    public CompletableFuture<Void> resetCursor(Position position) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        resetCursor(position, future);
        return future;
    }

    private void resetCursor(Position finalPosition, CompletableFuture<Void> future) {
        if (!IS_FENCED_UPDATER.compareAndSet(PersistentSubscription.this, FALSE, TRUE)) {
            future.completeExceptionally(new SubscriptionBusyException("Failed to fence subscription"));
            return;
        }

        final CompletableFuture<Void> disconnectFuture;

        // Lock the Subscription object before locking the Dispatcher object to avoid deadlocks
        synchronized (this) {
            if (dispatcher != null && dispatcher.isConsumerConnected()) {
                disconnectFuture = dispatcher.disconnectActiveConsumers(true);
            } else {
                disconnectFuture = CompletableFuture.completedFuture(null);
            }
        }

        disconnectFuture.whenComplete((aVoid, throwable) -> {
            if (dispatcher != null) {
                dispatcher.resetCloseFuture();
            }

            if (throwable != null) {
                log.error("[{}][{}] Failed to disconnect consumer from subscription", topicName, subName, throwable);
                IS_FENCED_UPDATER.set(PersistentSubscription.this, FALSE);
                future.completeExceptionally(
                        new SubscriptionBusyException("Failed to disconnect consumers from subscription"));
                return;
            }

            log.info("[{}][{}] Successfully disconnected consumers from subscription, proceeding with cursor reset",
                    topicName, subName);

            try {
                boolean forceReset = false;
                if (topic.getCompactedTopic() != null && topic.getCompactedTopic().getCompactionHorizon().isPresent()) {
                    PositionImpl horizon = (PositionImpl) topic.getCompactedTopic().getCompactionHorizon().get();
                    PositionImpl resetTo = (PositionImpl) finalPosition;
                    if (horizon.compareTo(resetTo) >= 0) {
                        forceReset = true;
                    }
                }
                cursor.asyncResetCursor(finalPosition, forceReset, new AsyncCallbacks.ResetCursorCallback() {
                    @Override
                    public void resetComplete(Object ctx) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}][{}] Successfully reset subscription to position {}", topicName, subName,
                                    finalPosition);
                        }
                        if (dispatcher != null) {
                            dispatcher.cursorIsReset();
                        }
                        IS_FENCED_UPDATER.set(PersistentSubscription.this, FALSE);
                        future.complete(null);
                    }

                    @Override
                    public void resetFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("[{}][{}] Failed to reset subscription to position {}", topicName, subName,
                                finalPosition, exception);
                        IS_FENCED_UPDATER.set(PersistentSubscription.this, FALSE);
                        // todo - retry on InvalidCursorPositionException
                        // or should we just ask user to retry one more time?
                        if (exception instanceof InvalidCursorPositionException) {
                            future.completeExceptionally(new SubscriptionInvalidCursorPosition(exception.getMessage()));
                        } else if (exception instanceof ConcurrentFindCursorPositionException) {
                            future.completeExceptionally(new SubscriptionBusyException(exception.getMessage()));
                        } else {
                            future.completeExceptionally(new BrokerServiceException(exception));
                        }
                    }
                });
            } catch (Exception e) {
                log.error("[{}][{}] Error while resetting cursor", topicName, subName, e);
                IS_FENCED_UPDATER.set(PersistentSubscription.this, FALSE);
                future.completeExceptionally(new BrokerServiceException(e));
            }
        });
    }

    @Override
    public CompletableFuture<Entry> peekNthMessage(int messagePosition) {
        CompletableFuture<Entry> future = new CompletableFuture<>();

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Getting message at position {}", topicName, subName, messagePosition);
        }

        cursor.asyncGetNthEntry(messagePosition, IndividualDeletedEntries.Exclude, new ReadEntryCallback() {

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }

            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                future.complete(entry);
            }

            @Override
            public String toString() {
                return String.format("Subscription [%s-%s] async replay entries", PersistentSubscription.this.topicName,
                        PersistentSubscription.this.subName);
            }
        }, null);

        return future;
    }

    @Override
    public long getNumberOfEntriesInBacklog(boolean getPreciseBacklog) {
        return cursor.getNumberOfEntriesInBacklog(getPreciseBacklog);
    }

    @Override
    public synchronized Dispatcher getDispatcher() {
        return this.dispatcher;
    }

    public long getNumberOfEntriesSinceFirstNotAckedMessage() {
        return cursor.getNumberOfEntriesSinceFirstNotAckedMessage();
    }

    public int getTotalNonContiguousDeletedMessagesRange() {
        return cursor.getTotalNonContiguousDeletedMessagesRange();
    }

    /**
     * Close the cursor ledger for this subscription. Requires that there are no active consumers on the dispatcher
     *
     * @return CompletableFuture indicating the completion of delete operation
     */
    @Override
    public CompletableFuture<Void> close() {
        synchronized (this) {
            if (dispatcher != null && dispatcher.isConsumerConnected()) {
                return FutureUtil.failedFuture(new SubscriptionBusyException("Subscription has active consumers"));
            }
            return this.pendingAckHandle.closeAsync().thenAccept(v -> {
                IS_FENCED_UPDATER.set(this, TRUE);
                log.info("[{}][{}] Successfully closed subscription [{}]", topicName, subName, cursor);
            });
        }
    }

    /**
     * Disconnect all consumers attached to the dispatcher and close this subscription.
     *
     * @return CompletableFuture indicating the completion of disconnect operation
     */
    @Override
    public synchronized CompletableFuture<Void> disconnect() {
        if (fenceFuture != null){
            return fenceFuture;
        }
        fenceFuture = new CompletableFuture<>();

        // block any further consumers on this subscription
        IS_FENCED_UPDATER.set(this, TRUE);

        (dispatcher != null ? dispatcher.close() : CompletableFuture.completedFuture(null))
                .thenCompose(v -> close()).thenRun(() -> {
                    log.info("[{}][{}] Successfully disconnected and closed subscription", topicName, subName);
                    fenceFuture.complete(null);
                }).exceptionally(exception -> {
                    log.error("[{}][{}] Error disconnecting consumers from subscription", topicName, subName,
                            exception);
                    fenceFuture.completeExceptionally(exception);
                    resumeAfterFence();
                    return null;
                });
        return fenceFuture;
    }

    /**
     * Resume subscription after topic deletion or close failure.
     */
    public synchronized void resumeAfterFence() {
        // If "fenceFuture" is null, it means that "disconnect" has never been called.
        if (fenceFuture != null) {
            fenceFuture.whenComplete((ignore, ignoreEx) -> {
                synchronized (PersistentSubscription.this) {
                    try {
                        if (IS_FENCED_UPDATER.compareAndSet(this, TRUE, FALSE)) {
                            if (dispatcher != null) {
                                dispatcher.reset();
                            }
                        }
                        fenceFuture = null;
                    } catch (Exception ex) {
                        log.error("[{}] Resume subscription [{}] failure", topicName, subName, ex);
                    }
                }
            });
        }
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
            this.disconnect().thenRun(() -> {
                closeSubscriptionFuture.complete(null);
            }).exceptionally(ex -> {
                log.error("[{}][{}] Error disconnecting and closing subscription", topicName, subName, ex);
                closeSubscriptionFuture.completeExceptionally(ex);
                return null;
            });
        } else {
            this.close().thenRun(() -> {
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
     * @return CompletableFuture indicating the completion of unsubscribe operation
     */
    @Override
    public CompletableFuture<Void> doUnsubscribe(Consumer consumer) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            if (dispatcher.canUnsubscribe(consumer)) {
                consumer.close();
                return delete();
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
        long backlog = getNumberOfEntriesInBacklog(false);
        if (backlog == 0 || (dispatcher != null && dispatcher.isConsumerConnected()
                && backlog < MINIMUM_BACKLOG_FOR_EXPIRY_CHECK
                && !topic.isOldestMessageExpired(cursor, messageTTLInSeconds))) {
            // don't do anything for almost caught-up connected subscriptions
            return false;
        }
        this.lastExpireTimestamp = System.currentTimeMillis();
        return expiryMonitor.expireMessages(messageTTLInSeconds);
    }

    @Override
    public boolean expireMessages(Position position) {
        this.lastExpireTimestamp = System.currentTimeMillis();
        return expiryMonitor.expireMessages(position);
    }

    public double getExpiredMessageRate() {
        return expiryMonitor.getMessageExpiryRate();
    }

    public PersistentMessageExpiryMonitor getExpiryMonitor() {
        return expiryMonitor;
    }

    public long estimateBacklogSize() {
        return cursor.getEstimatedSizeSinceMarkDeletePosition();
    }

    public SubscriptionStatsImpl getStats(Boolean getPreciseBacklog, boolean subscriptionBacklogSize,
                                          boolean getEarliestTimeInBacklog) {
        SubscriptionStatsImpl subStats = new SubscriptionStatsImpl();
        subStats.lastExpireTimestamp = lastExpireTimestamp;
        subStats.lastConsumedFlowTimestamp = lastConsumedFlowTimestamp;
        subStats.lastMarkDeleteAdvancedTimestamp = lastMarkDeleteAdvancedTimestamp;
        subStats.bytesOutCounter = bytesOutFromRemovedConsumers.longValue();
        subStats.msgOutCounter = msgOutFromRemovedConsumer.longValue();
        Dispatcher dispatcher = this.dispatcher;
        if (dispatcher != null) {
            Map<Consumer, List<Range>> consumerKeyHashRanges = getType() == SubType.Key_Shared
                    ? ((PersistentStickyKeyDispatcherMultipleConsumers) dispatcher).getConsumerKeyHashRanges() : null;
            dispatcher.getConsumers().forEach(consumer -> {
                ConsumerStatsImpl consumerStats = consumer.getStats();
                subStats.consumers.add(consumerStats);
                subStats.msgRateOut += consumerStats.msgRateOut;
                subStats.msgThroughputOut += consumerStats.msgThroughputOut;
                subStats.bytesOutCounter += consumerStats.bytesOutCounter;
                subStats.msgOutCounter += consumerStats.msgOutCounter;
                subStats.msgRateRedeliver += consumerStats.msgRateRedeliver;
                subStats.messageAckRate += consumerStats.messageAckRate;
                subStats.chunkedMessageRate += consumerStats.chunkedMessageRate;
                subStats.unackedMessages += consumerStats.unackedMessages;
                subStats.lastConsumedTimestamp =
                        Math.max(subStats.lastConsumedTimestamp, consumerStats.lastConsumedTimestamp);
                subStats.lastAckedTimestamp = Math.max(subStats.lastAckedTimestamp, consumerStats.lastAckedTimestamp);
                if (consumerKeyHashRanges != null && consumerKeyHashRanges.containsKey(consumer)) {
                    consumerStats.keyHashRanges = consumerKeyHashRanges.get(consumer).stream()
                            .map(Range::toString)
                            .collect(Collectors.toList());
                }
            });
        }

        SubType subType = getType();
        subStats.type = getTypeString();
        if (dispatcher instanceof PersistentDispatcherSingleActiveConsumer) {
            Consumer activeConsumer = ((PersistentDispatcherSingleActiveConsumer) dispatcher).getActiveConsumer();
            if (activeConsumer != null) {
                subStats.activeConsumerName = activeConsumer.consumerName();
            }
        }
        if (Subscription.isIndividualAckMode(subType)) {
            if (dispatcher instanceof PersistentDispatcherMultipleConsumers) {
                PersistentDispatcherMultipleConsumers d = (PersistentDispatcherMultipleConsumers) dispatcher;
                subStats.unackedMessages = d.getTotalUnackedMessages();
                subStats.blockedSubscriptionOnUnackedMsgs = d.isBlockedDispatcherOnUnackedMsgs();
                subStats.msgDelayed = d.getNumberOfDelayedMessages();
            }
        }
        subStats.msgBacklog = getNumberOfEntriesInBacklog(getPreciseBacklog);
        if (subscriptionBacklogSize) {
            subStats.backlogSize = ((ManagedLedgerImpl) topic.getManagedLedger())
                    .getEstimatedBacklogSize((PositionImpl) cursor.getMarkDeletedPosition());
        }
        if (getEarliestTimeInBacklog && subStats.msgBacklog > 0) {
            ManagedLedgerImpl managedLedger = ((ManagedLedgerImpl) cursor.getManagedLedger());
            PositionImpl markDeletedPosition = (PositionImpl) cursor.getMarkDeletedPosition();
            long result = 0;
            try {
                result = managedLedger.getEarliestMessagePublishTimeOfPos(markDeletedPosition).get();
            } catch (InterruptedException | ExecutionException e) {
                result = -1;
            }
            subStats.earliestMsgPublishTimeInBacklog = result;
        }
        subStats.msgBacklogNoDelayed = subStats.msgBacklog - subStats.msgDelayed;
        subStats.msgRateExpired = expiryMonitor.getMessageExpiryRate();
        subStats.totalMsgExpired = expiryMonitor.getTotalMessageExpired();
        subStats.isReplicated = isReplicated();
        subStats.subscriptionProperties = subscriptionProperties;
        subStats.isDurable = cursor.isDurable();
        if (getType() == SubType.Key_Shared && dispatcher instanceof PersistentStickyKeyDispatcherMultipleConsumers) {
            PersistentStickyKeyDispatcherMultipleConsumers keySharedDispatcher =
                    (PersistentStickyKeyDispatcherMultipleConsumers) dispatcher;

            subStats.allowOutOfOrderDelivery = keySharedDispatcher.isAllowOutOfOrderDelivery();
            subStats.keySharedMode = keySharedDispatcher.getKeySharedMode().toString();

            LinkedHashMap<Consumer, PositionImpl> recentlyJoinedConsumers = keySharedDispatcher
                    .getRecentlyJoinedConsumers();
            if (recentlyJoinedConsumers != null && recentlyJoinedConsumers.size() > 0) {
                recentlyJoinedConsumers.forEach((k, v) -> {
                    subStats.consumersAfterMarkDeletePosition.put(k.consumerName(), v.toString());
                });
            }
        }
        subStats.nonContiguousDeletedMessagesRanges = cursor.getTotalNonContiguousDeletedMessagesRange();
        subStats.nonContiguousDeletedMessagesRangesSerializedSize =
                cursor.getNonContiguousDeletedMessagesRangeSerializedSize();
        return subStats;
    }

    @Override
    public void redeliverUnacknowledgedMessages(Consumer consumer, long consumerEpoch) {
        Dispatcher dispatcher = getDispatcher();
        if (dispatcher != null) {
            dispatcher.redeliverUnacknowledgedMessages(consumer, consumerEpoch);
        }
    }

    @Override
    public void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {
        Dispatcher dispatcher = getDispatcher();
        if (dispatcher != null) {
            dispatcher.redeliverUnacknowledgedMessages(consumer, positions);
        }
    }

    private void trimByMarkDeletePosition(List<PositionImpl> positions) {
        positions.removeIf(position -> cursor.getMarkDeletedPosition() != null
                && position.compareTo((PositionImpl) cursor.getMarkDeletedPosition()) <= 0);
    }

    @Override
    public void addUnAckedMessages(int unAckMessages) {
        dispatcher.addUnAckedMessages(unAckMessages);
    }

    @Override
    public synchronized long getNumberOfEntriesDelayed() {
        if (dispatcher != null) {
            return dispatcher.getNumberOfDelayedMessages();
        } else {
            return 0;
        }
    }

    @Override
    public void markTopicWithBatchMessagePublished() {
        topic.markBatchMessagePublished();
    }

    void topicTerminated() {
        if (cursor.getNumberOfEntriesInBacklog(false) == 0) {
            // notify the consumers if there are consumers connected to this topic.
            if (null != dispatcher) {
                // Immediately notify the consumer that there are no more available messages
                dispatcher.getConsumers().forEach(Consumer::reachedEndOfTopic);
            }
        }
    }

    @Override
    public Map<String, String> getSubscriptionProperties() {
        return subscriptionProperties;
    }

    public PositionImpl getPositionInPendingAck(PositionImpl position) {
        return pendingAckHandle.getPositionInPendingAck(position);
    }
    @Override
    public CompletableFuture<Void> updateSubscriptionProperties(Map<String, String> subscriptionProperties) {
        Map<String, String> newSubscriptionProperties;
        if (subscriptionProperties == null || subscriptionProperties.isEmpty()) {
            newSubscriptionProperties = Collections.emptyMap();
        } else {
            newSubscriptionProperties = Collections.unmodifiableMap(subscriptionProperties);
        }
        return cursor.setCursorProperties(newSubscriptionProperties)
                .thenRun(() -> {
                    this.subscriptionProperties = newSubscriptionProperties;
                });
    }
    /**
     * Return a merged map that contains the cursor properties specified by used
     * (eg. when using compaction subscription) and the subscription properties.
     */
    protected Map<String, Long> mergeCursorProperties(Map<String, Long> userProperties) {
        Map<String, Long> baseProperties = getBaseCursorProperties(isReplicated());

        if (userProperties.isEmpty()) {
            // Use only the static instance in the common case
            return baseProperties;
        } else {
            Map<String, Long> merged = new TreeMap<>();
            merged.putAll(userProperties);
            merged.putAll(baseProperties);
            return merged;
        }

    }

    @Override
    public void processReplicatedSubscriptionSnapshot(ReplicatedSubscriptionsSnapshot snapshot) {
        ReplicatedSubscriptionSnapshotCache snapshotCache = this.replicatedSubscriptionSnapshotCache;
        if (snapshotCache != null) {
            snapshotCache.addNewSnapshot(new ReplicatedSubscriptionsSnapshot().copyFrom(snapshot));
        }
    }

    @Override
    public CompletableFuture<Void> endTxn(long txnidMostBits, long txnidLeastBits, int txnAction, long lowWaterMark) {
        TxnID txnID = new TxnID(txnidMostBits, txnidLeastBits);
        if (TxnAction.COMMIT.getValue() == txnAction) {
            return pendingAckHandle.commitTxn(txnID, Collections.emptyMap(), lowWaterMark);
        } else if (TxnAction.ABORT.getValue() == txnAction) {
            Consumer redeliverConsumer = null;
            if (getDispatcher() instanceof PersistentDispatcherSingleActiveConsumer) {
                redeliverConsumer = ((PersistentDispatcherSingleActiveConsumer)
                        getDispatcher()).getActiveConsumer();
            }
            return pendingAckHandle.abortTxn(txnID, redeliverConsumer, lowWaterMark);
        } else {
            return FutureUtil.failedFuture(new NotAllowedException("Unsupported txnAction " + txnAction));
        }
    }

    @VisibleForTesting
    public ManagedCursor getCursor() {
        return cursor;
    }

    public void syncBatchPositionBitSetForPendingAck(PositionImpl position) {
        this.pendingAckHandle.syncBatchPositionAckSetForTransaction(position);
    }

    public boolean checkIsCanDeleteConsumerPendingAck(PositionImpl position) {
        return this.pendingAckHandle.checkIsCanDeleteConsumerPendingAck(position);
    }

    public TransactionPendingAckStats getTransactionPendingAckStats() {
        return this.pendingAckHandle.getStats();
    }

    public boolean checkAndUnblockIfStuck() {
        return dispatcher != null ? dispatcher.checkAndUnblockIfStuck() : false;
    }

    public TransactionInPendingAckStats getTransactionInPendingAckStats(TxnID txnID) {
        return this.pendingAckHandle.getTransactionInPendingAckStats(txnID);
    }

    public CompletableFuture<ManagedLedger> getPendingAckManageLedger() {
        if (this.pendingAckHandle instanceof PendingAckHandleImpl) {
            return ((PendingAckHandleImpl) this.pendingAckHandle).getStoreManageLedger();
        } else {
            return FutureUtil.failedFuture(new NotAllowedException("Pending ack handle don't use managedLedger!"));
        }
    }

    public boolean checkIfPendingAckStoreInit() {
        return this.pendingAckHandle.checkIfPendingAckStoreInit();
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentSubscription.class);

    @VisibleForTesting
    public PendingAckHandle getPendingAckHandle() {
        return pendingAckHandle;
    }
}
