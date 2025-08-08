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

import static org.apache.pulsar.broker.service.AbstractBaseDispatcher.checkAndApplyReachedEndOfTopicOrTopicMigration;
import static org.apache.pulsar.common.naming.SystemTopicNames.isEventSystemTopic;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Getter;
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
import org.apache.bookkeeper.mledger.ScanOutcome;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.service.AbstractSubscription;
import org.apache.pulsar.broker.service.AnalyzeBacklogResult;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.BrokerServiceException.NotAllowedException;
import org.apache.pulsar.broker.service.BrokerServiceException.ServerMetadataException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionFencedException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionInvalidCursorPosition;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.EntryFilterSupport;
import org.apache.pulsar.broker.service.GetStatsOptions;
import org.apache.pulsar.broker.service.StickyKeyDispatcher;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckHandle;
import org.apache.pulsar.broker.transaction.pendingack.impl.PendingAckHandleDisabled;
import org.apache.pulsar.broker.transaction.pendingack.impl.PendingAckHandleImpl;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshot;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TransactionInPendingAckStats;
import org.apache.pulsar.common.policies.data.TransactionPendingAckStats;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.policies.data.stats.SubscriptionStatsImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.stats.PositionInPendingAckStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentSubscription extends AbstractSubscription {
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

    private volatile long lastExpireTimestamp = 0L;
    private volatile long lastConsumedFlowTimestamp = 0L;
    private volatile long lastMarkDeleteAdvancedTimestamp = 0L;

    // for connected subscriptions, message expiry will be checked if the backlog is greater than this threshold
    private static final int MINIMUM_BACKLOG_FOR_EXPIRY_CHECK = 1000;

    private static final String REPLICATED_SUBSCRIPTION_PROPERTY = "pulsar.replicated.subscription";

    // Map of properties that is used to mark this subscription as "replicated".
    // Since this is the only field at this point, we can just keep a static
    // instance of the map.
    private static final Map<String, Long> REPLICATED_SUBSCRIPTION_CURSOR_PROPERTIES =
            Map.of(REPLICATED_SUBSCRIPTION_PROPERTY, 1L);
    private static final Map<String, Long> NON_REPLICATED_SUBSCRIPTION_CURSOR_PROPERTIES = Map.of();

    private volatile ReplicatedSubscriptionSnapshotCache replicatedSubscriptionSnapshotCache;
    @Getter
    private final PendingAckHandle pendingAckHandle;
    private volatile Map<String, String> subscriptionProperties;
    private volatile CompletableFuture<Void> fenceFuture;
    private volatile CompletableFuture<Void> inProgressResetCursorFuture;
    private volatile Boolean replicatedControlled;
    private final ServiceConfiguration config;

    static Map<String, Long> getBaseCursorProperties(Boolean isReplicated) {
        return isReplicated != null && isReplicated ? REPLICATED_SUBSCRIPTION_CURSOR_PROPERTIES :
                NON_REPLICATED_SUBSCRIPTION_CURSOR_PROPERTIES;
    }

    static boolean isCursorFromReplicatedSubscription(ManagedCursor cursor) {
        return cursor.getProperties().containsKey(REPLICATED_SUBSCRIPTION_PROPERTY);
    }

    public PersistentSubscription(PersistentTopic topic, String subscriptionName, ManagedCursor cursor,
                                  Boolean replicated) {
        this(topic, subscriptionName, cursor, replicated, Collections.emptyMap());
    }

    public PersistentSubscription(PersistentTopic topic, String subscriptionName, ManagedCursor cursor,
                                  Boolean replicated, Map<String, String> subscriptionProperties) {
        this.topic = topic;
        this.config = topic.getBrokerService().getPulsar().getConfig();
        this.cursor = cursor;
        this.topicName = topic.getName();
        this.subName = subscriptionName;
        this.fullName = MoreObjects.toStringHelper(this).add("topic", topicName).add("name", subName).toString();
        this.expiryMonitor = new PersistentMessageExpiryMonitor(topic, subscriptionName, cursor, this);
        if (replicated != null) {
            this.setReplicated(replicated);
        }
        this.subscriptionProperties = MapUtils.isEmpty(subscriptionProperties)
                ? Collections.emptyMap() : Collections.unmodifiableMap(subscriptionProperties);
        if (config.isTransactionCoordinatorEnabled()
                && !isEventSystemTopic(TopicName.get(topicName))
                && !ExtensibleLoadManagerImpl.isInternalTopic(topicName)) {
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
        replicatedControlled = replicated;

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
        CompletableFuture<Void> inProgressResetCursorFuture = this.inProgressResetCursorFuture;
        if (inProgressResetCursorFuture != null) {
            return inProgressResetCursorFuture.handle((ignore, ignoreEx) -> null)
                    .thenCompose(ignore -> addConsumerInternal(consumer));
        } else {
            return addConsumerInternal(consumer);
        }
    }

    private CompletableFuture<Void> addConsumerInternal(Consumer consumer) {
        return pendingAckHandle.pendingAckHandleFuture().thenCompose(future -> {
            synchronized (PersistentSubscription.this) {
                cursor.updateLastActive();
                if (IS_FENCED_UPDATER.get(this) == TRUE) {
                    log.warn("Attempting to add consumer {} on a fenced subscription", consumer);
                    return FutureUtil.failedFuture(new SubscriptionFencedException("Subscription is fenced"));
                }

                if (dispatcher == null || !dispatcher.isConsumerConnected()) {
                    if (consumer.subType() == null) {
                        return FutureUtil.failedFuture(new ServerMetadataException("Unsupported subscription type"));
                    }
                    dispatcher = reuseOrCreateDispatcher(dispatcher, consumer);
                } else {
                    Optional<CompletableFuture<Void>> compatibilityError =
                            checkForConsumerCompatibilityErrorWithDispatcher(dispatcher, consumer);
                    if (compatibilityError.isPresent()) {
                        return compatibilityError.get();
                    }
                }

                return dispatcher.addConsumer(consumer);
            }
        });
    }

    /**
     * Create a new dispatcher or reuse the existing one when it's compatible with the new consumer.
     * This protected method can be overridded for testing purpose for injecting test dispatcher instances with
     * special behaviors.
     * @param dispatcher the existing dispatcher
     * @param consumer the new consumer
     * @return the dispatcher to use, either the existing one or a new one
     */
    protected Dispatcher reuseOrCreateDispatcher(Dispatcher dispatcher, Consumer consumer) {
        Dispatcher previousDispatcher = null;
        switch (consumer.subType()) {
            case Exclusive:
                if (dispatcher == null || dispatcher.getType() != SubType.Exclusive) {
                    previousDispatcher = dispatcher;
                    dispatcher = new PersistentDispatcherSingleActiveConsumer(
                            cursor, SubType.Exclusive, 0, topic, this);
                }
                break;
            case Shared:
                if (dispatcher == null || dispatcher.getType() != SubType.Shared) {
                    previousDispatcher = dispatcher;
                    if (config.isSubscriptionSharedUseClassicPersistentImplementation()) {
                        dispatcher = new PersistentDispatcherMultipleConsumersClassic(topic, cursor, this);
                    } else {
                        dispatcher = new PersistentDispatcherMultipleConsumers(topic, cursor, this);
                    }
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
                    dispatcher = new PersistentDispatcherSingleActiveConsumer(cursor, SubType.Failover,
                            partitionIndex, topic, this);
                }
                break;
            case Key_Shared:
                KeySharedMeta ksm = consumer.getKeySharedMeta();
                if (dispatcher == null || dispatcher.getType() != SubType.Key_Shared
                        || !((StickyKeyDispatcher) dispatcher)
                        .hasSameKeySharedPolicy(ksm)) {
                    previousDispatcher = dispatcher;
                    if (config.isSubscriptionKeySharedUseClassicPersistentImplementation()) {
                        dispatcher =
                                new PersistentStickyKeyDispatcherMultipleConsumersClassic(topic, cursor,
                                        this, config, ksm);
                    } else {
                        dispatcher = new PersistentStickyKeyDispatcherMultipleConsumers(topic, cursor, this,
                                config, ksm);
                    }
                }
                break;
        }

        if (previousDispatcher != null) {
            previousDispatcher.close().thenRun(() -> {
                log.info("[{}][{}] Successfully closed previous dispatcher", topicName, subName);
            }).exceptionally(ex -> {
                log.error("[{}][{}] Failed to close previous dispatcher", topicName, subName, ex);
                return null;
            });
        }

        return dispatcher;
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
            // Remove the cursor from the waiting cursors list.
            // For durable cursors, we should *not* cancel the pending read with cursor.cancelPendingReadRequest.
            // This is because internally, in the dispatcher implementations, there is a "havePendingRead" flag
            // that is not reset. If the pending read is cancelled, the dispatcher will not continue reading from
            // the managed ledger when a new consumer is added to the dispatcher since based on the "havePendingRead"
            // state, it will continue to expect that a read is pending and will not submit a new read.
            // For non-durable cursors, there's no difference since the cursor is not expected to be used again.

            // remove waiting cursor from the managed ledger, this applies to both durable and non-durable cursors.
            topic.getManagedLedger().removeWaitingCursor(cursor);

            if (!cursor.isDurable()) {
                // If cursor is not durable, we need to clean up the subscription as well. No need to check for active
                // consumers since we already validated that there are no consumers on this dispatcher.
                this.closeCursor(false).thenRun(() -> {
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
                topic.getBrokerService().pulsar().getExecutor().execute(() -> {
                    topic.removeSubscription(subName).thenRunAsync(() -> {
                        // Also need remove the cursor here, otherwise the data deletion will not work well.
                        // Because data deletion depends on the mark delete position of all cursors.
                        if (!isResetCursor) {
                            try {
                                topic.getManagedLedger().deleteCursor(cursor.getName());
                            } catch (InterruptedException | ManagedLedgerException e) {
                                log.warn("[{}] [{}] Failed to remove non durable cursor", topic.getName(), subName, e);
                            }
                        }
                    }, topic.getBrokerService().pulsar().getExecutor());
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
        cursor.updateLastActive();
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
            if (config.isTransactionCoordinatorEnabled()) {
                positions.forEach(position -> {
                    if ((cursor.isMessageDeleted(position))) {
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
            ReplicatedSubscriptionSnapshotCache snapshotCache = this.replicatedSubscriptionSnapshotCache;
            if (snapshotCache != null) {
                ReplicatedSubscriptionsSnapshot snapshot = snapshotCache
                        .advancedMarkDeletePosition(cursor.getMarkDeletedPosition());
                if (snapshot != null) {
                    topic.getReplicatedSubscriptionController()
                            .ifPresent(c -> c.localSubscriptionUpdated(subName, snapshot));
                }
            }
        }

        if (topic.getManagedLedger().isTerminated() && cursor.getNumberOfEntriesInBacklog(false) == 0) {
            // Notify all consumer that the end of topic was reached
            if (dispatcher != null) {
                checkAndApplyReachedEndOfTopicOrTopicMigration(topic, dispatcher.getConsumers());
            }
        }
    }

    public CompletableFuture<Void> transactionIndividualAcknowledge(
            TxnID txnId,
            List<MutablePair<Position, Integer>> positions) {
        return pendingAckHandle.individualAcknowledgeMessage(txnId, positions);
    }

    public CompletableFuture<Void> transactionCumulativeAcknowledge(TxnID txnId, List<Position> positions) {
        return pendingAckHandle.cumulativeAcknowledgeMessage(txnId, positions);
    }

    private final MarkDeleteCallback markDeleteCallback = new MarkDeleteCallback() {
        @Override
        public void markDeleteComplete(Object ctx) {
            Position oldMD = (Position) ctx;
            Position newMD = cursor.getMarkDeletedPosition();
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Mark deleted messages to position {} from position {}",
                        topicName, subName, newMD, oldMD);
            }
            // Signal the dispatchers to give chance to take extra actions
            if (dispatcher != null) {
                dispatcher.afterAckMessages(null, ctx);
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
            // Signal the dispatchers to give chance to take extra actions
            if (dispatcher != null) {
                dispatcher.afterAckMessages(null, ctx);
            }
        }
    };

    private final DeleteCallback deleteCallback = new DeleteCallback() {
        @Override
        public void deleteComplete(Object context) {
            if (log.isDebugEnabled()) {
                // The value of the param "context" is a position.
                log.debug("[{}][{}] Deleted message at {}", topicName, subName, context);
            }
            // Signal the dispatchers to give chance to take extra actions
            if (dispatcher != null) {
                dispatcher.afterAckMessages(null, context);
            }
            notifyTheMarkDeletePositionMoveForwardIfNeeded((Position) context);
        }

        @Override
        public void deleteFailed(ManagedLedgerException exception, Object ctx) {
            log.warn("[{}][{}] Failed to delete message at {}: {}", topicName, subName, ctx, exception);
            // Signal the dispatchers to give chance to take extra actions
            if (dispatcher != null) {
                dispatcher.afterAckMessages(exception, ctx);
            }
        }
    };

    private void notifyTheMarkDeletePositionMoveForwardIfNeeded(Position oldPosition) {
        Position oldMD = oldPosition;
        Position newMD = cursor.getMarkDeletedPosition();
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

    public CompletableFuture<AnalyzeBacklogResult> analyzeBacklog(Optional<Position> position) {
        final ManagedLedger managedLedger = topic.getManagedLedger();
        final String newNonDurableCursorName = "analyze-backlog-" + UUID.randomUUID();
        ManagedCursor newNonDurableCursor;
        try {
            newNonDurableCursor = cursor.duplicateNonDurableCursor(newNonDurableCursorName);
        } catch (ManagedLedgerException e) {
            return CompletableFuture.failedFuture(e);
        }
        long start = System.currentTimeMillis();
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Starting to analyze backlog", topicName, subName);
        }

        AtomicLong entries = new AtomicLong();
        AtomicLong accepted = new AtomicLong();
        AtomicLong rejected = new AtomicLong();
        AtomicLong rescheduled = new AtomicLong();
        AtomicLong messages = new AtomicLong();
        AtomicLong acceptedMessages = new AtomicLong();
        AtomicLong rejectedMessages = new AtomicLong();
        AtomicLong rescheduledMessages = new AtomicLong();

        Position currentPosition = newNonDurableCursor.getMarkDeletedPosition();

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] currentPosition {}",
                    topicName, subName, currentPosition);
        }
        final EntryFilterSupport entryFilterSupport = dispatcher != null
                ? (EntryFilterSupport) dispatcher : new EntryFilterSupport(this);
        // we put some hard limits on the scan, in order to prevent denial of services
        long maxEntries = config.getSubscriptionBacklogScanMaxEntries();
        long timeOutMs = config.getSubscriptionBacklogScanMaxTimeMs();
        int batchSize = config.getDispatcherMaxReadBatchSize();
        AtomicReference<Position> firstPosition = new AtomicReference<>();
        AtomicReference<Position> lastPosition = new AtomicReference<>();
        final Predicate<Entry> condition = entry -> {
            if (log.isDebugEnabled()) {
                log.debug("found {}", entry);
            }
            Position entryPosition = entry.getPosition();
            firstPosition.compareAndSet(null, entryPosition);
            lastPosition.set(entryPosition);
            ByteBuf metadataAndPayload = entry.getDataBuffer();
            MessageMetadata messageMetadata = Commands.peekMessageMetadata(metadataAndPayload, "", -1);
            int numMessages = 1;
            if (messageMetadata.hasNumMessagesInBatch()) {
                numMessages = messageMetadata.getNumMessagesInBatch();
            }
            EntryFilter.FilterResult filterResult = entryFilterSupport
                    .runFiltersForEntry(entry, messageMetadata, null);

            if (filterResult == null) {
                filterResult = EntryFilter.FilterResult.ACCEPT;
            }
            switch (filterResult) {
                case REJECT:
                    rejected.incrementAndGet();
                    rejectedMessages.addAndGet(numMessages);
                    break;
                case RESCHEDULE:
                    rescheduled.incrementAndGet();
                    rescheduledMessages.addAndGet(numMessages);
                    break;
                default:
                    accepted.incrementAndGet();
                    acceptedMessages.addAndGet(numMessages);
                    break;
            }
            long num = entries.incrementAndGet();
            messages.addAndGet(numMessages);

            if (num % 1000 == 0) {
                long end = System.currentTimeMillis();
                log.info(
                        "[{}][{}] scan running since {} ms - scanned {} entries",
                        topicName, subName, end - start, num);
            }

            return true;
        };
        CompletableFuture<AnalyzeBacklogResult> res = newNonDurableCursor.scan(
                position,
                condition,
                batchSize,
                maxEntries,
                timeOutMs
        ).thenApply((ScanOutcome outcome) -> {
            long end = System.currentTimeMillis();
            AnalyzeBacklogResult result = new AnalyzeBacklogResult();
            result.setFirstPosition(firstPosition.get());
            result.setLastPosition(lastPosition.get());
            result.setEntries(entries.get());
            result.setMessages(messages.get());
            result.setFilterAcceptedEntries(accepted.get());
            result.setFilterAcceptedMessages(acceptedMessages.get());
            result.setFilterRejectedEntries(rejected.get());
            result.setFilterRejectedMessages(rejectedMessages.get());
            result.setFilterRescheduledEntries(rescheduled.get());
            result.setFilterRescheduledMessages(rescheduledMessages.get());
            // sometimes we abort the execution due to a timeout or
            // when we reach a maximum number of entries
            result.setScanOutcome(outcome);
            log.info(
                    "[{}][{}] scan took {} ms - {}",
                    topicName, subName, end - start, result);
            return result;
        });
        res.whenComplete((__, ex) -> {
            managedLedger.asyncDeleteCursor(newNonDurableCursorName,
                new AsyncCallbacks.DeleteCursorCallback(){
                    @Override
                    public void deleteCursorComplete(Object ctx) {
                        // Nothing to do.
                    }

                    @Override
                    public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                        log.warn("[{}][{}] Delete non-durable cursor[{}] failed when analyze backlog.",
                                topicName, subName, newNonDurableCursor.getName());
                    }
                }, null);
        });
        return res;
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
                    dispatcher.clearDelayedMessages().whenComplete((__, ex) -> {
                        if (ex != null) {
                            future.completeExceptionally(ex);
                        } else {
                            future.complete(null);
                        }
                    });
                    dispatcher.afterAckMessages(null, ctx);
                } else {
                    future.complete(null);
                }
            }

            @Override
            public void clearBacklogFailed(ManagedLedgerException exception, Object ctx) {
                log.error("[{}][{}] Failed to clear backlog", topicName, subName, exception);
                future.completeExceptionally(exception);
                if (dispatcher != null) {
                    dispatcher.afterAckMessages(exception, ctx);
                }
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
                        if (dispatcher != null) {
                            dispatcher.afterAckMessages(null, ctx);
                        }
                    }

                    @Override
                    public void skipEntriesFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("[{}][{}] Failed to skip {} messages", topicName, subName, numMessagesToSkip,
                                exception);
                        future.completeExceptionally(exception);
                        if (dispatcher != null) {
                            dispatcher.afterAckMessages(exception, ctx);
                        }
                    }
                }, null);

        return future;
    }

    @Override
    public CompletableFuture<Void> resetCursor(long timestamp) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        PersistentMessageFinder persistentMessageFinder = new PersistentMessageFinder(topicName, cursor,
                config.getManagedLedgerCursorResetLedgerCloseTimestampMaxClockSkewMillis());

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
                CompletableFuture<Void> resetCursorFuture = resetCursor(finalPosition);
                FutureUtil.completeAfter(future, resetCursorFuture);
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
    public CompletableFuture<Void> resetCursor(Position finalPosition) {
        if (!IS_FENCED_UPDATER.compareAndSet(PersistentSubscription.this, FALSE, TRUE)) {
            return CompletableFuture.failedFuture(new SubscriptionBusyException("Failed to fence subscription"));
        }

        final CompletableFuture<Void> future = new CompletableFuture<>();
        inProgressResetCursorFuture = future;
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
                inProgressResetCursorFuture = null;
                future.completeExceptionally(
                        new SubscriptionBusyException("Failed to disconnect consumers from subscription"));
                return;
            }

            log.info("[{}][{}] Successfully disconnected consumers from subscription, proceeding with cursor reset",
                    topicName, subName);

            CompletableFuture<Boolean> forceReset = new CompletableFuture<>();
            if (topic.getTopicCompactionService() == null) {
                forceReset.complete(false);
            } else {
                topic.getTopicCompactionService().getLastCompactedPosition().thenAccept(lastCompactedPosition -> {
                    Position resetTo = finalPosition;
                    if (lastCompactedPosition != null && resetTo.compareTo(lastCompactedPosition.getLedgerId(),
                            lastCompactedPosition.getEntryId()) <= 0) {
                        forceReset.complete(true);
                    } else {
                        forceReset.complete(false);
                    }
                }).exceptionally(ex -> {
                    forceReset.completeExceptionally(ex);
                    return null;
                });
            }

            forceReset.thenAccept(forceResetValue -> {
                cursor.asyncResetCursor(finalPosition, forceResetValue, new AsyncCallbacks.ResetCursorCallback() {
                    @Override
                    public void resetComplete(Object ctx) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}][{}] Successfully reset subscription to position {}", topicName, subName,
                                    finalPosition);
                        }
                        if (dispatcher != null) {
                            dispatcher.cursorIsReset();
                            dispatcher.afterAckMessages(null, finalPosition);
                        }
                        IS_FENCED_UPDATER.set(PersistentSubscription.this, FALSE);
                        inProgressResetCursorFuture = null;
                        future.complete(null);
                    }

                    @Override
                    public void resetFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("[{}][{}] Failed to reset subscription to position {}", topicName, subName,
                                finalPosition, exception);
                        IS_FENCED_UPDATER.set(PersistentSubscription.this, FALSE);
                        inProgressResetCursorFuture = null;
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
            }).exceptionally((e) -> {
                log.error("[{}][{}] Error while resetting cursor", topicName, subName, e);
                IS_FENCED_UPDATER.set(PersistentSubscription.this, FALSE);
                inProgressResetCursorFuture = null;
                future.completeExceptionally(new BrokerServiceException(e));
                return null;
            });
        });
        return future;
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
     * Close the cursor ledger for this subscription. Optionally verifies that there are no active consumers on the
     * dispatcher.
     *
     * @return CompletableFuture indicating the completion of close operation
     */
    private synchronized CompletableFuture<Void> closeCursor(boolean checkActiveConsumers) {
        if (checkActiveConsumers && dispatcher != null && dispatcher.isConsumerConnected()) {
            return FutureUtil.failedFuture(new SubscriptionBusyException("Subscription has active consumers"));
        }
        return this.pendingAckHandle.closeAsync().thenAccept(v -> {
            IS_FENCED_UPDATER.set(this, TRUE);
            log.info("[{}][{}] Successfully closed subscription [{}]", topicName, subName, cursor);
        });
    }


    /**
     * Disconnect all consumers from this subscription.
     *
     * @return CompletableFuture indicating the completion of the operation.
     */
    @Override
    public synchronized CompletableFuture<Void> disconnect(Optional<BrokerLookupData> assignedBrokerLookupData) {
        CompletableFuture<Void> disconnectFuture = new CompletableFuture<>();

        (dispatcher != null
                ? dispatcher.disconnectAllConsumers(false, assignedBrokerLookupData)
                : CompletableFuture.completedFuture(null))
                .thenRun(() -> {
                    log.info("[{}][{}] Successfully disconnected subscription consumers", topicName, subName);
                    disconnectFuture.complete(null);
                }).exceptionally(exception -> {
                    log.error("[{}][{}] Error disconnecting subscription consumers", topicName, subName, exception);
                    disconnectFuture.completeExceptionally(exception);
                    return null;
                });

        return disconnectFuture;
    }

    /**
     * Fence this subscription and optionally disconnect all consumers.
     *
     * @return CompletableFuture indicating the completion of the operation.
     */
    @Override
    public synchronized CompletableFuture<Void> close(boolean disconnectConsumers,
                                                      Optional<BrokerLookupData> assignedBrokerLookupData) {
        if (fenceFuture != null) {
            return fenceFuture;
        }

        fenceFuture = new CompletableFuture<>();

        // block any further consumers on this subscription
        IS_FENCED_UPDATER.set(this, TRUE);

        (dispatcher != null
                ? dispatcher.close(disconnectConsumers, assignedBrokerLookupData)
                : CompletableFuture.completedFuture(null))
                // checkActiveConsumers is false since we just closed all of them if we wanted.
                .thenCompose(__ -> closeCursor(false)).thenRun(() -> {
                    log.info("[{}][{}] Successfully closed the subscription", topicName, subName);
                    fenceFuture.complete(null);
                }).exceptionally(exception -> {
                    log.error("[{}][{}] Error closing the subscription", topicName, subName, exception);
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
        // If "fenceFuture" is null, it means that "close" has never been called.
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
     *
     * @return
     */
    @Override
    public CompletableFuture<Void> deleteForcefully() {
        return delete(true);
    }

    /**
     * Delete the subscription by closing and deleting its managed cursor. Handle unsubscribe call from admin layer.
     *
     * @param closeIfConsumersConnected Flag indicate whether explicitly close connected consumers before trying to
     *                                  delete subscription. If
     *                                  any consumer is connected to it and if this flag is disable then this operation
     *                                  fails.
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
            this.closeCursor(true).thenRun(() -> {
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
                if (log.isDebugEnabled()) {
                    log.debug("[{}] unsubscribing forcefully {}-{}", topicName, subName, consumer.consumerName());
                }
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
    public CompletableFuture<Boolean> expireMessagesAsync(int messageTTLInSeconds) {
        long backlog = getNumberOfEntriesInBacklog(false);
        if (backlog == 0) {
            return CompletableFuture.completedFuture(false);
        }
        if (dispatcher != null && dispatcher.isConsumerConnected() && backlog < MINIMUM_BACKLOG_FOR_EXPIRY_CHECK) {
            return topic.isOldestMessageExpiredAsync(cursor, messageTTLInSeconds)
                .thenCompose(oldestMsgExpired -> {
                    if (oldestMsgExpired) {
                        this.lastExpireTimestamp = System.currentTimeMillis();
                        return expiryMonitor.expireMessagesAsync(messageTTLInSeconds);
                    } else {
                        return CompletableFuture.completedFuture(false);
                    }
            });
        }
        return expiryMonitor.expireMessagesAsync(messageTTLInSeconds);
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

    public CompletableFuture<SubscriptionStatsImpl> getStatsAsync(GetStatsOptions getStatsOptions) {
        SubscriptionStatsImpl subStats = new SubscriptionStatsImpl();
        subStats.lastExpireTimestamp = lastExpireTimestamp;
        subStats.lastConsumedFlowTimestamp = lastConsumedFlowTimestamp;
        subStats.lastMarkDeleteAdvancedTimestamp = lastMarkDeleteAdvancedTimestamp;
        subStats.bytesOutCounter = bytesOutFromRemovedConsumers.longValue();
        subStats.msgOutCounter = msgOutFromRemovedConsumer.longValue();

        Dispatcher dispatcher = this.dispatcher;
        if (dispatcher != null) {
            Map<Consumer, List<Range>> consumerKeyHashRanges = getType() == SubType.Key_Shared
                    ? ((StickyKeyDispatcher) dispatcher).getConsumerKeyHashRanges() : null;
            dispatcher.getConsumers().forEach(consumer -> {
                ConsumerStatsImpl consumerStats = consumer.getStats();
                if (!getStatsOptions.isExcludeConsumers()) {
                    subStats.consumers.add(consumerStats);
                }
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
                List<Range> keyRanges = consumerKeyHashRanges != null ? consumerKeyHashRanges.get(consumer) : null;
                if (keyRanges != null) {
                    if (((StickyKeyDispatcher) dispatcher).isClassic()) {
                        // Use string representation for classic mode
                        consumerStats.keyHashRanges = keyRanges.stream()
                                .map(Range::toString)
                                .collect(Collectors.toList());
                    } else {
                        // Use array representation for PIP-379 stats
                        consumerStats.keyHashRangeArrays = keyRanges.stream()
                                .map(range -> new int[]{range.getStart(), range.getEnd()})
                                .collect(Collectors.toList());
                    }
                }
                subStats.drainingHashesCount += consumerStats.drainingHashesCount;
                subStats.drainingHashesClearedTotal += consumerStats.drainingHashesClearedTotal;
                subStats.drainingHashesUnackedMessages += consumerStats.drainingHashesUnackedMessages;
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

        SubType subType = getType();
        subStats.type = getTypeString();
        if (dispatcher instanceof PersistentDispatcherSingleActiveConsumer) {
            Consumer activeConsumer = ((PersistentDispatcherSingleActiveConsumer) dispatcher).getActiveConsumer();
            if (activeConsumer != null) {
                subStats.activeConsumerName = activeConsumer.consumerName();
            }
        }

        if (dispatcher instanceof AbstractPersistentDispatcherMultipleConsumers) {
            subStats.delayedMessageIndexSizeInBytes =
                    ((AbstractPersistentDispatcherMultipleConsumers) dispatcher).getDelayedTrackerMemoryUsage();

            subStats.bucketDelayedIndexStats =
                    ((AbstractPersistentDispatcherMultipleConsumers) dispatcher).getBucketDelayedIndexStats();
        }

        if (Subscription.isIndividualAckMode(subType)) {
            if (dispatcher instanceof AbstractPersistentDispatcherMultipleConsumers) {
                AbstractPersistentDispatcherMultipleConsumers d =
                        (AbstractPersistentDispatcherMultipleConsumers) dispatcher;
                subStats.unackedMessages = d.getTotalUnackedMessages();
                subStats.blockedSubscriptionOnUnackedMsgs = d.isBlockedDispatcherOnUnackedMsgs();
                subStats.msgDelayed = d.getNumberOfDelayedMessages();
                subStats.msgInReplay = d.getNumberOfMessagesInReplay();
            }
        }
        subStats.msgBacklog = getNumberOfEntriesInBacklog(getStatsOptions.isGetPreciseBacklog());
        if (getStatsOptions.isSubscriptionBacklogSize()) {
            subStats.backlogSize = topic.getManagedLedger()
                    .getEstimatedBacklogSize(cursor.getMarkDeletedPosition());
        } else {
            subStats.backlogSize = -1;
        }
        subStats.msgBacklogNoDelayed = subStats.msgBacklog - subStats.msgDelayed;
        subStats.msgRateExpired = expiryMonitor.getMessageExpiryRate();
        subStats.totalMsgExpired = expiryMonitor.getTotalMessageExpired();
        subStats.isReplicated = isReplicated();
        subStats.subscriptionProperties = subscriptionProperties;
        subStats.isDurable = cursor.isDurable();
        if (getType() == SubType.Key_Shared && dispatcher instanceof StickyKeyDispatcher) {
            StickyKeyDispatcher keySharedDispatcher = (StickyKeyDispatcher) dispatcher;
            subStats.allowOutOfOrderDelivery = keySharedDispatcher.isAllowOutOfOrderDelivery();
            subStats.keySharedMode = keySharedDispatcher.getKeySharedMode().toString();

            LinkedHashMap<Consumer, Position> recentlyJoinedConsumers = keySharedDispatcher
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
        if (!getStatsOptions.isGetEarliestTimeInBacklog()) {
            return CompletableFuture.completedFuture(subStats);
        }
        if (subStats.msgBacklog > 0) {
            ManagedLedger managedLedger = cursor.getManagedLedger();
            Position markDeletedPosition = cursor.getMarkDeletedPosition();
            return getEarliestMessagePublishTimeOfPos(managedLedger, markDeletedPosition).thenApply(v -> {
                subStats.earliestMsgPublishTimeInBacklog = v;
                return subStats;
            });
        } else {
            subStats.earliestMsgPublishTimeInBacklog = -1;
            return CompletableFuture.completedFuture(subStats);
        }
    }

    private CompletableFuture<Long> getEarliestMessagePublishTimeOfPos(ManagedLedger ml, Position pos) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        if (pos == null) {
            future.complete(0L);
            return future;
        }
        Position nextPos = ml.getNextValidPosition(pos);

        if (nextPos.compareTo(ml.getLastConfirmedEntry()) > 0) {
            return CompletableFuture.completedFuture(-1L);
        }

        ml.asyncReadEntry(nextPos, new ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                try {
                    long entryTimestamp = Commands.getEntryTimestamp(entry.getDataBuffer());
                    future.complete(entryTimestamp);
                } catch (IOException e) {
                    log.error("Error deserializing message for message position {}", nextPos, e);
                    future.completeExceptionally(e);
                } finally {
                    entry.release();
                }
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                log.error("Error read entry for position {}", nextPos, exception);
                future.completeExceptionally(exception);
            }

            @Override
            public String toString() {
                return String.format("ML [%s] get earliest message publish time of pos",
                        ml.getName());
            }
        }, null);

        return future;
    }

    @Override
    public void redeliverUnacknowledgedMessages(Consumer consumer, long consumerEpoch) {
        Dispatcher dispatcher = getDispatcher();
        if (dispatcher != null) {
            dispatcher.redeliverUnacknowledgedMessages(consumer, consumerEpoch);
        }
    }

    @Override
    public void redeliverUnacknowledgedMessages(Consumer consumer, List<Position> positions) {
        Dispatcher dispatcher = getDispatcher();
        if (dispatcher != null) {
            dispatcher.redeliverUnacknowledgedMessages(consumer, positions);
        }
    }

    private void trimByMarkDeletePosition(List<Position> positions) {
        positions.removeIf(position -> cursor.getMarkDeletedPosition() != null
                && position.compareTo(cursor.getMarkDeletedPosition()) <= 0);
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
                checkAndApplyReachedEndOfTopicOrTopicMigration(topic, dispatcher.getConsumers());
            }
        }
    }

    @Override
    public boolean isSubscriptionMigrated() {
        log.info("backlog for {} - {}", topicName, cursor.getNumberOfEntriesInBacklog(true));
        return topic.isMigrated() && cursor.getNumberOfEntriesInBacklog(true) <= 0;
    }

    @Override
    public Map<String, String> getSubscriptionProperties() {
        return subscriptionProperties;
    }

    public Position getPositionInPendingAck(Position position) {
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

    public void syncBatchPositionBitSetForPendingAck(Position position) {
        this.pendingAckHandle.syncBatchPositionAckSetForTransaction(position);
    }

    public boolean checkIsCanDeleteConsumerPendingAck(Position position) {
        return this.pendingAckHandle.checkIsCanDeleteConsumerPendingAck(position);
    }

    public TransactionPendingAckStats getTransactionPendingAckStats(boolean lowWaterMarks) {
        return this.pendingAckHandle.getStats(lowWaterMarks);
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

    public PositionInPendingAckStats checkPositionInPendingAckState(Position position, Integer batchIndex) {
        return pendingAckHandle.checkPositionInPendingAckState(position, batchIndex);
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentSubscription.class);

    @VisibleForTesting
    public Boolean getReplicatedControlled() {
        return replicatedControlled;
    }
}
