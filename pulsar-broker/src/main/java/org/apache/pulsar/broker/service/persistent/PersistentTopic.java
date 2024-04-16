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

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.pulsar.broker.service.persistent.SubscribeRateLimiter.isSubscribeRateEnabled;
import static org.apache.pulsar.common.naming.SystemTopicNames.isEventSystemTopic;
import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.apache.pulsar.compaction.Compactor.COMPACTION_SUBSCRIPTION;
import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Value;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OffloadCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.TerminateCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.UpdatePropertiesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.IndividualDeletedEntries;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFencedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerTerminatedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetadataNotFoundException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NonRecoverableLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorContainer;
import org.apache.bookkeeper.mledger.impl.ManagedCursorContainer.CursorInfo;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.impl.ShadowManagedLedgerImpl;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.bookkeeper.net.BookieId;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.delayed.BucketDelayedDeliveryTrackerFactory;
import org.apache.pulsar.broker.delayed.DelayedDeliveryTrackerFactory;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateCompactionStrategy;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.NamespaceResources.PartitionedTopicResources;
import org.apache.pulsar.broker.service.AbstractReplicator;
import org.apache.pulsar.broker.service.AbstractTopic;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.BrokerServiceException.AlreadyRunningException;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.NamingException;
import org.apache.pulsar.broker.service.BrokerServiceException.NotAllowedException;
import org.apache.pulsar.broker.service.BrokerServiceException.PersistenceException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionConflictUnloadException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionNotFoundException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicBacklogQuotaExceededException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicClosedException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicFencedException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicMigratedException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicTerminatedException;
import org.apache.pulsar.broker.service.BrokerServiceException.UnsupportedSubscriptionException;
import org.apache.pulsar.broker.service.BrokerServiceException.UnsupportedVersionException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.GetStatsOptions;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.Replicator;
import org.apache.pulsar.broker.service.StreamingStats;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.SubscriptionOption;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter.Type;
import org.apache.pulsar.broker.service.schema.BookkeeperSchemaStorage;
import org.apache.pulsar.broker.stats.ClusterReplicationMetrics;
import org.apache.pulsar.broker.stats.NamespaceStats;
import org.apache.pulsar.broker.stats.ReplicationMetrics;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.impl.TransactionBufferDisable;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStore;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.OffloadProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterPolicies.ClusterUrl;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats.CursorStats;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats.LedgerInfo;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.TransactionBufferStats;
import org.apache.pulsar.common.policies.data.TransactionInBufferStats;
import org.apache.pulsar.common.policies.data.TransactionInPendingAckStats;
import org.apache.pulsar.common.policies.data.TransactionPendingAckStats;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.policies.data.stats.PublisherStatsImpl;
import org.apache.pulsar.common.policies.data.stats.ReplicatorStatsImpl;
import org.apache.pulsar.common.policies.data.stats.SubscriptionStatsImpl;
import org.apache.pulsar.common.policies.data.stats.TopicMetricBean;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.topics.TopicCompactionStrategy;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.compaction.CompactedTopicContext;
import org.apache.pulsar.compaction.CompactedTopicImpl;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.compaction.CompactorMXBean;
import org.apache.pulsar.compaction.PulsarTopicCompactionService;
import org.apache.pulsar.compaction.TopicCompactionService;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.utils.StatsOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PersistentTopic extends AbstractTopic implements Topic, AddEntryCallback {

    // Managed ledger associated with the topic
    protected final ManagedLedger ledger;

    // Subscriptions to this topic
    private final ConcurrentOpenHashMap<String, PersistentSubscription> subscriptions;

    private final ConcurrentOpenHashMap<String/*RemoteCluster*/, Replicator> replicators;
    private final ConcurrentOpenHashMap<String/*ShadowTopic*/, Replicator> shadowReplicators;
    @Getter
    private volatile List<String> shadowTopics;
    private final TopicName shadowSourceTopic;

    public static final String DEDUPLICATION_CURSOR_NAME = "pulsar.dedup";

    public static boolean isDedupCursorName(String name) {
        return DEDUPLICATION_CURSOR_NAME.equals(name);
    }
    private static final String TOPIC_EPOCH_PROPERTY_NAME = "pulsar.topic.epoch";

    private static final double MESSAGE_EXPIRY_THRESHOLD = 1.5;

    private static final long POLICY_UPDATE_FAILURE_RETRY_TIME_SECONDS = 60;

    private static final String MIGRATION_CLUSTER_NAME = "migration-cluster";
    private volatile boolean migrationSubsCreated = false;

    // topic has every published chunked message since topic is loaded
    public boolean msgChunkPublished;

    private Optional<DispatchRateLimiter> dispatchRateLimiter = Optional.empty();
    private final Object dispatchRateLimiterLock = new Object();
    private Optional<SubscribeRateLimiter> subscribeRateLimiter = Optional.empty();
    private final long backloggedCursorThresholdEntries;
    public static final int MESSAGE_RATE_BACKOFF_MS = 1000;

    protected final MessageDeduplication messageDeduplication;

    private static final Long COMPACTION_NEVER_RUN = -0xfebecffeL;
    private volatile CompletableFuture<Long> currentCompaction = CompletableFuture.completedFuture(
            COMPACTION_NEVER_RUN);
    private TopicCompactionService topicCompactionService;

    // TODO: Create compaction strategy from topic policy when exposing strategic compaction to users.
    private static Map<String, TopicCompactionStrategy> strategicCompactionMap = Map.of(
            ServiceUnitStateChannelImpl.TOPIC,
            new ServiceUnitStateCompactionStrategy());

    private CompletableFuture<MessageIdImpl> currentOffload = CompletableFuture.completedFuture(
            (MessageIdImpl) MessageId.earliest);

    private volatile Optional<ReplicatedSubscriptionsController> replicatedSubscriptionsController = Optional.empty();

    private static final FastThreadLocal<TopicStatsHelper> threadLocalTopicStats =
            new FastThreadLocal<TopicStatsHelper>() {
                @Override
                protected TopicStatsHelper initialValue() {
                    return new TopicStatsHelper();
                }
            };

    private final AtomicLong pendingWriteOps = new AtomicLong(0);
    private volatile double lastUpdatedAvgPublishRateInMsg = 0;
    private volatile double lastUpdatedAvgPublishRateInByte = 0;

    private volatile boolean isClosingOrDeleting = false;

    private ScheduledFuture<?> fencedTopicMonitoringTask = null;

    @Getter
    protected final TransactionBuffer transactionBuffer;

    // Record the last time a data message (ie: not an internal Pulsar marker) is published on the topic
    @Getter
    private volatile long lastDataMessagePublishedTimestamp = 0;
    @Getter
    private final ExecutorService orderedExecutor;

    @Getter
    private final PersistentTopicMetrics persistentTopicMetrics = new PersistentTopicMetrics();

    private volatile TimeBasedBacklogQuotaCheckResult timeBasedBacklogQuotaCheckResult;
    private static final AtomicReferenceFieldUpdater<PersistentTopic, TimeBasedBacklogQuotaCheckResult>
            TIME_BASED_BACKLOG_QUOTA_CHECK_RESULT_UPDATER = AtomicReferenceFieldUpdater.newUpdater(
            PersistentTopic.class,
            TimeBasedBacklogQuotaCheckResult.class,
            "timeBasedBacklogQuotaCheckResult");
    @Value
    private static class TimeBasedBacklogQuotaCheckResult {
        PositionImpl oldestCursorMarkDeletePosition;
        String cursorName;
        long positionPublishTimestampInMillis;
        long dataVersion;
    }

    @Value
    private static class EstimateTimeBasedBacklogQuotaCheckResult {
        boolean truncateBacklogToMatchQuota;
        Long estimatedOldestUnacknowledgedMessageTimestamp;
    }

    private static class TopicStatsHelper {
        public double averageMsgSize;
        public double aggMsgRateIn;
        public double aggMsgThroughputIn;
        public double aggMsgThrottlingFailure;
        public double aggMsgRateOut;
        public double aggMsgThroughputOut;
        public final ObjectObjectHashMap<String, PublisherStatsImpl> remotePublishersStats;

        public TopicStatsHelper() {
            remotePublishersStats = new ObjectObjectHashMap<>();
            reset();
        }

        public void reset() {
            averageMsgSize = 0;
            aggMsgRateIn = 0;
            aggMsgThroughputIn = 0;
            aggMsgRateOut = 0;
            aggMsgThrottlingFailure = 0;
            aggMsgThroughputOut = 0;
            remotePublishersStats.clear();
        }
    }

    public PersistentTopic(String topic, ManagedLedger ledger, BrokerService brokerService) {
        super(topic, brokerService);
        // null check for backwards compatibility with tests which mock the broker service
        this.orderedExecutor = brokerService.getTopicOrderedExecutor() != null
                ? brokerService.getTopicOrderedExecutor().chooseThread(topic)
                : null;
        this.ledger = ledger;
        this.subscriptions = ConcurrentOpenHashMap.<String, PersistentSubscription>newBuilder()
                        .expectedItems(16)
                        .concurrencyLevel(1)
                        .build();
        this.replicators = ConcurrentOpenHashMap.<String, Replicator>newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();
        this.shadowReplicators = ConcurrentOpenHashMap.<String, Replicator>newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();
        this.backloggedCursorThresholdEntries =
                brokerService.pulsar().getConfiguration().getManagedLedgerCursorBackloggedThreshold();
        this.messageDeduplication = new MessageDeduplication(brokerService.pulsar(), this, ledger);
        if (ledger.getProperties().containsKey(TOPIC_EPOCH_PROPERTY_NAME)) {
            topicEpoch = Optional.of(Long.parseLong(ledger.getProperties().get(TOPIC_EPOCH_PROPERTY_NAME)));
        }

        TopicName topicName = TopicName.get(topic);
        if (brokerService.getPulsar().getConfiguration().isTransactionCoordinatorEnabled()
                && !isEventSystemTopic(topicName)
                && !NamespaceService.isHeartbeatNamespace(topicName.getNamespaceObject())
                && !ExtensibleLoadManagerImpl.isInternalTopic(topic)) {
            this.transactionBuffer = brokerService.getPulsar()
                    .getTransactionBufferProvider().newTransactionBuffer(this);
        } else {
            this.transactionBuffer = new TransactionBufferDisable(this);
        }
        transactionBuffer.syncMaxReadPositionForNormalPublish((PositionImpl) ledger.getLastConfirmedEntry());
        if (ledger instanceof ShadowManagedLedgerImpl) {
            shadowSourceTopic = TopicName.get(ledger.getConfig().getShadowSource());
        } else {
            shadowSourceTopic = null;
        }
    }

    @Override
    public CompletableFuture<Void> initialize() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        futures.add(brokerService.getPulsar().newTopicCompactionService(topic).thenAccept(service -> {
            PersistentTopic.this.topicCompactionService = service;
            this.createPersistentSubscriptions();
        }));

        for (ManagedCursor cursor : ledger.getCursors()) {
            if (cursor.getName().startsWith(replicatorPrefix)) {
                String localCluster = brokerService.pulsar().getConfiguration().getClusterName();
                String remoteCluster = PersistentReplicator.getRemoteCluster(cursor.getName());
                futures.add(addReplicationCluster(remoteCluster, cursor, localCluster));
            }
        }
        return FutureUtil.waitForAll(futures).thenCompose(__ ->
            brokerService.pulsar().getPulsarResources().getNamespaceResources()
                .getPoliciesAsync(TopicName.get(topic).getNamespaceObject())
                .thenAcceptAsync(optPolicies -> {
                    if (!optPolicies.isPresent()) {
                        isEncryptionRequired = false;
                        updatePublishRateLimiter();
                        updateResourceGroupLimiter(new Policies());
                        initializeDispatchRateLimiterIfNeeded();
                        updateSubscribeRateLimiter();
                        return;
                    }

                    Policies policies = optPolicies.get();

                    this.updateTopicPolicyByNamespacePolicy(policies);

                    initializeDispatchRateLimiterIfNeeded();

                    updateSubscribeRateLimiter();

                    updatePublishRateLimiter();

                    updateResourceGroupLimiter(policies);

                    this.isEncryptionRequired = policies.encryption_required;

                    isAllowAutoUpdateSchema = policies.is_allow_auto_update_schema;
                }, getOrderedExecutor())
                .thenCompose(ignore -> initTopicPolicy())
                .exceptionally(ex -> {
                    log.warn("[{}] Error getting policies {} and isEncryptionRequired will be set to false",
                            topic, ex.getMessage());
                    isEncryptionRequired = false;
                    return null;
                }));
    }

    // for testing purposes
    @VisibleForTesting
    PersistentTopic(String topic, BrokerService brokerService, ManagedLedger ledger,
                    MessageDeduplication messageDeduplication) {
        super(topic, brokerService);
        // null check for backwards compatibility with tests which mock the broker service
        this.orderedExecutor = brokerService.getTopicOrderedExecutor() != null
                ? brokerService.getTopicOrderedExecutor().chooseThread(topic)
                : null;
        this.ledger = ledger;
        this.messageDeduplication = messageDeduplication;
        this.subscriptions = ConcurrentOpenHashMap.<String, PersistentSubscription>newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();
        this.replicators = ConcurrentOpenHashMap.<String, Replicator>newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();
        this.shadowReplicators = ConcurrentOpenHashMap.<String, Replicator>newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();
        this.backloggedCursorThresholdEntries =
                brokerService.pulsar().getConfiguration().getManagedLedgerCursorBackloggedThreshold();

        if (brokerService.pulsar().getConfiguration().isTransactionCoordinatorEnabled()) {
            this.transactionBuffer = brokerService.getPulsar()
                    .getTransactionBufferProvider().newTransactionBuffer(this);
        } else {
            this.transactionBuffer = new TransactionBufferDisable(this);
        }
        shadowSourceTopic = null;
    }

    private void initializeDispatchRateLimiterIfNeeded() {
        synchronized (dispatchRateLimiterLock) {
            // dispatch rate limiter for topic
            if (!dispatchRateLimiter.isPresent()
                && DispatchRateLimiter.isDispatchRateEnabled(topicPolicies.getDispatchRate().get())) {
                this.dispatchRateLimiter = Optional.of(new DispatchRateLimiter(this, Type.TOPIC));
            }
        }
    }

    @VisibleForTesting
    public AtomicLong getPendingWriteOps() {
        return pendingWriteOps;
    }

    private void createPersistentSubscriptions() {
        for (ManagedCursor cursor : ledger.getCursors()) {
                if (cursor.getName().equals(DEDUPLICATION_CURSOR_NAME)
                        || cursor.getName().startsWith(replicatorPrefix)) {
                    // This is not a regular subscription, we are going to
                    // ignore it for now and let the message dedup logic to take care of it
                } else {
                    final String subscriptionName = Codec.decode(cursor.getName());
                    subscriptions.put(subscriptionName, createPersistentSubscription(subscriptionName, cursor,
                            PersistentSubscription.isCursorFromReplicatedSubscription(cursor),
                            cursor.getCursorProperties()));
                    // subscription-cursor gets activated by default: deactivate as there is no active subscription
                    // right now
                    subscriptions.get(subscriptionName).deactivateCursor();
                }
        }
        checkReplicatedSubscriptionControllerState();
    }

    /**
     * Unload a subscriber.
     * @throws SubscriptionNotFoundException If subscription not founded.
     * @throws UnsupportedSubscriptionException If the subscription is typed compaction.
     * @throws SubscriptionConflictUnloadException Conflict topic-close, topic-delete, another-subscribe-unload,
     *     cannot unload subscription now
     */
    public CompletableFuture<Void> unloadSubscription(@Nonnull String subName) {
        final PersistentSubscription sub = subscriptions.get(subName);
        if (sub == null) {
            return CompletableFuture.failedFuture(
                    new SubscriptionNotFoundException(String.format("Subscription %s not found", subName)));
        }
        if (Compactor.COMPACTION_SUBSCRIPTION.equals(sub.getName())){
            return CompletableFuture.failedFuture(
                    new UnsupportedSubscriptionException(String.format("Unsupported subscription: %s", subName)));
        }
        // Fence old subscription -> Rewind cursor -> Replace with a new subscription.
        return sub.close(true, Optional.empty()).thenCompose(ignore -> {
            if (!lock.writeLock().tryLock()) {
                return CompletableFuture.failedFuture(new SubscriptionConflictUnloadException(String.format("Conflict"
                        + " topic-close, topic-delete, another-subscribe-unload, cannot unload subscription %s now",
                        subName)));
            }
            try {
                if (isFenced) {
                    return CompletableFuture.failedFuture(new TopicFencedException(String.format(
                            "Topic[%s] is fenced, can not unload subscription %s now", topic, subName)));
                }
                if (sub != subscriptions.get(subName)) {
                    // Another task already finished.
                    return CompletableFuture.failedFuture(new SubscriptionConflictUnloadException(String.format(
                            "Another unload subscriber[%s] has been finished, do not repeat call.", subName)));
                }
                sub.getCursor().rewind();
                PersistentSubscription subNew = PersistentTopic.this.createPersistentSubscription(sub.getName(),
                        sub.getCursor(), sub.isReplicated(), sub.getSubscriptionProperties());
                subscriptions.put(subName, subNew);
                return CompletableFuture.completedFuture(null);
            } finally {
                lock.writeLock().unlock();
            }
        });
    }

    private PersistentSubscription createPersistentSubscription(String subscriptionName, ManagedCursor cursor,
            boolean replicated, Map<String, String> subscriptionProperties) {
        requireNonNull(topicCompactionService);
        if (isCompactionSubscription(subscriptionName)
                && topicCompactionService instanceof PulsarTopicCompactionService pulsarTopicCompactionService) {
            CompactedTopicImpl compactedTopic = pulsarTopicCompactionService.getCompactedTopic();
            return new PulsarCompactorSubscription(this, compactedTopic, subscriptionName, cursor);
        } else {
            return new PersistentSubscription(this, subscriptionName, cursor, replicated, subscriptionProperties);
        }
    }

    public static boolean isCompactionSubscription(String subscriptionName) {
        return COMPACTION_SUBSCRIPTION.equals(subscriptionName);
    }

    @Override
    public void publishMessage(ByteBuf headersAndPayload, PublishContext publishContext) {
        pendingWriteOps.incrementAndGet();
        if (isFenced) {
            publishContext.completed(new TopicFencedException("fenced"), -1, -1);
            decrementPendingWriteOpsAndCheck();
            return;
        }
        if (isExceedMaximumMessageSize(headersAndPayload.readableBytes(), publishContext)) {
            publishContext.completed(new NotAllowedException("Exceed maximum message size"), -1, -1);
            decrementPendingWriteOpsAndCheck();
            return;
        }
        if (isExceedMaximumDeliveryDelay(headersAndPayload)) {
            publishContext.completed(
                    new NotAllowedException(
                            String.format("Exceeds max allowed delivery delay of %s milliseconds",
                                    getDelayedDeliveryMaxDelayInMillis())), -1, -1);
            decrementPendingWriteOpsAndCheck();
            return;
        }

        MessageDeduplication.MessageDupStatus status =
                messageDeduplication.isDuplicate(publishContext, headersAndPayload);
        switch (status) {
            case NotDup:
                asyncAddEntry(headersAndPayload, publishContext);
                break;
            case Dup:
                // Immediately acknowledge duplicated message
                publishContext.completed(null, -1, -1);
                decrementPendingWriteOpsAndCheck();
                break;
            default:
                publishContext.completed(new MessageDeduplication.MessageDupUnknownException(), -1, -1);
                decrementPendingWriteOpsAndCheck();

        }
    }

    public void updateSubscribeRateLimiter() {
        SubscribeRate subscribeRate = getSubscribeRate();
        synchronized (subscribeRateLimiter) {
            if (isSubscribeRateEnabled(subscribeRate)) {
                if (subscribeRateLimiter.isPresent()) {
                    this.subscribeRateLimiter.get().onSubscribeRateUpdate(subscribeRate);
                } else {
                    this.subscribeRateLimiter = Optional.of(new SubscribeRateLimiter(this));
                }
            } else {
                if (subscribeRateLimiter.isPresent()) {
                    subscribeRateLimiter.get().close();
                    subscribeRateLimiter = Optional.empty();
                }
            }
        }
    }

    private void asyncAddEntry(ByteBuf headersAndPayload, PublishContext publishContext) {
        if (brokerService.isBrokerEntryMetadataEnabled()) {
            ledger.asyncAddEntry(headersAndPayload,
                    (int) publishContext.getNumberOfMessages(), this, publishContext);
        } else {
            ledger.asyncAddEntry(headersAndPayload, this, publishContext);
        }
    }

    public void asyncReadEntry(PositionImpl position, AsyncCallbacks.ReadEntryCallback callback, Object ctx) {
        if (ledger instanceof ManagedLedgerImpl) {
            ((ManagedLedgerImpl) ledger).asyncReadEntry(position, callback, ctx);
        } else {
            callback.readEntryFailed(new ManagedLedgerException(
                    "Unexpected managedledger implementation, doesn't support "
                            + "direct read entry operation."), ctx);
        }
    }

    public PositionImpl getPositionAfterN(PositionImpl startPosition, long n) throws ManagedLedgerException {
        if (ledger instanceof ManagedLedgerImpl) {
            return ((ManagedLedgerImpl) ledger).getPositionAfterN(startPosition, n,
                    ManagedLedgerImpl.PositionBound.startExcluded);
        } else {
            throw new ManagedLedgerException("Unexpected managedledger implementation, doesn't support "
                    + "getPositionAfterN operation.");
        }
    }

    public PositionImpl getFirstPosition() throws ManagedLedgerException {
        if (ledger instanceof ManagedLedgerImpl) {
            return ((ManagedLedgerImpl) ledger).getFirstPosition();
        } else {
            throw new ManagedLedgerException("Unexpected managedledger implementation, doesn't support "
                    + "getFirstPosition operation.");
        }
    }

    public long getNumberOfEntries() {
        return ledger.getNumberOfEntries();
    }

    private void decrementPendingWriteOpsAndCheck() {
        long pending = pendingWriteOps.decrementAndGet();
        if (pending == 0 && isFenced && !isClosingOrDeleting) {
            synchronized (this) {
                if (isFenced && !isClosingOrDeleting) {
                    messageDeduplication.resetHighestSequenceIdPushed();
                    log.info("[{}] Un-fencing topic...", topic);
                    // signal to managed ledger that we are ready to resume by creating a new ledger
                    ledger.readyToCreateNewLedger();

                    unfence();
                }

            }
        }
    }

    @Override
    public void addComplete(Position pos, ByteBuf entryData, Object ctx) {
        PublishContext publishContext = (PublishContext) ctx;
        PositionImpl position = (PositionImpl) pos;

        // Message has been successfully persisted
        messageDeduplication.recordMessagePersisted(publishContext, position);

        if (!publishContext.isMarkerMessage()) {
            lastDataMessagePublishedTimestamp = Clock.systemUTC().millis();
        }

        // in order to sync the max position when cursor read entries
        transactionBuffer.syncMaxReadPositionForNormalPublish((PositionImpl) ledger.getLastConfirmedEntry());
        publishContext.setMetadataFromEntryData(entryData);
        publishContext.completed(null, position.getLedgerId(), position.getEntryId());
        decrementPendingWriteOpsAndCheck();
    }

    @Override
    public synchronized void addFailed(ManagedLedgerException exception, Object ctx) {
        /* If the topic is being transferred(in the Releasing bundle state),
         we don't want to forcefully close topic here.
         Instead, we will rely on the service unit state channel's bundle(topic) transfer protocol.
         At the end of the transfer protocol, at Owned state, the source broker should close the topic properly.
         */
        if (transferring) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed to persist msg in store: {} while transferring.",
                        topic, exception.getMessage(), exception);
            }
            return;
        }

        PublishContext callback = (PublishContext) ctx;
        if (exception instanceof ManagedLedgerFencedException) {
            // If the managed ledger has been fenced, we cannot continue using it. We need to close and reopen
            close();
        } else {
            // fence topic when failed to write a message to BK
            fence();
            // close all producers
            CompletableFuture<Void> disconnectProducersFuture;
            if (producers.size() > 0) {
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                // send migration url metadata to producers before disconnecting them
                if (isMigrated()) {
                    if (!shouldProducerMigrate()) {
                        log.info("Topic {} is migrated but replication-backlog exists or "
                                + "subs not created. Closing producers.", topic);
                    } else {
                        producers.forEach((__, producer) -> producer.topicMigrated(getMigratedClusterUrl()));
                    }
                }
                producers.forEach((__, producer) -> futures.add(producer.disconnect()));
                disconnectProducersFuture = FutureUtil.waitForAll(futures);
            } else {
                disconnectProducersFuture = CompletableFuture.completedFuture(null);
            }
            disconnectProducersFuture.handle((BiFunction<Void, Throwable, Void>) (aVoid, throwable) -> {
                decrementPendingWriteOpsAndCheck();
                return null;
            });

            if (exception instanceof ManagedLedgerAlreadyClosedException) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Failed to persist msg in store: {}", topic, exception.getMessage());
                }

                callback.completed(new TopicClosedException(exception), -1, -1);
                return;

            } else {
                log.warn("[{}] Failed to persist msg in store: {}", topic, exception.getMessage());
            }

            if (exception instanceof ManagedLedgerTerminatedException && !isMigrated()) {
                // Signal the producer that this topic is no longer available
                callback.completed(new TopicTerminatedException(exception), -1, -1);
            } else {
                // Use generic persistence exception
                callback.completed(new PersistenceException(exception), -1, -1);
            }
        }
    }

    @Override
    public CompletableFuture<Optional<Long>> addProducer(Producer producer,
            CompletableFuture<Void> producerQueuedFuture) {
        return super.addProducer(producer, producerQueuedFuture).thenCompose(topicEpoch -> {
            messageDeduplication.producerAdded(producer.getProducerName());

            // Start replication producers if not already
            return startReplProducers().thenApply(__ -> topicEpoch);
        });
    }

    @Override
    public CompletableFuture<Void> checkIfTransactionBufferRecoverCompletely(boolean isTxnEnabled) {
        return getTransactionBuffer().checkIfTBRecoverCompletely(isTxnEnabled);
    }

    @Override
    protected CompletableFuture<Long> incrementTopicEpoch(Optional<Long> currentEpoch) {
        long newEpoch = currentEpoch.orElse(-1L) + 1;
        return setTopicEpoch(newEpoch);
    }

    @Override
    protected CompletableFuture<Long> setTopicEpoch(long newEpoch) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        ledger.asyncSetProperty(TOPIC_EPOCH_PROPERTY_NAME, String.valueOf(newEpoch), new UpdatePropertiesCallback() {
            @Override
            public void updatePropertiesComplete(Map<String, String> properties, Object ctx) {
                log.info("[{}] Updated topic epoch to {}", getName(), newEpoch);
                future.complete(newEpoch);
            }

            @Override
            public void updatePropertiesFailed(ManagedLedgerException exception, Object ctx) {
                log.warn("[{}] Failed to update topic epoch to {}: {}", getName(), newEpoch, exception.getMessage());
                future.completeExceptionally(exception);
            }
        }, null);

        return future;
    }

    private boolean hasRemoteProducers() {
        if (producers.isEmpty()) {
            return false;
        }
        for (Producer producer : producers.values()) {
            if (producer.isRemote()) {
                return true;
            }
        }
        return false;
    }

    public CompletableFuture<Void> startReplProducers() {
        // read repl-cluster from policies to avoid restart of replicator which are in process of disconnect and close
        return brokerService.pulsar().getPulsarResources().getNamespaceResources()
                .getPoliciesAsync(TopicName.get(topic).getNamespaceObject())
                .thenAcceptAsync(optPolicies -> {
                    if (optPolicies.isPresent()) {
                        if (optPolicies.get().replication_clusters != null) {
                            Set<String> configuredClusters = Sets.newTreeSet(optPolicies.get().replication_clusters);
                            replicators.forEach((region, replicator) -> {
                                if (configuredClusters.contains(region)) {
                                    replicator.startProducer();
                                }
                            });
                        }
                    } else {
                        replicators.forEach((region, replicator) -> replicator.startProducer());
                    }
                }, getOrderedExecutor()).exceptionally(ex -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Error getting policies while starting repl-producers {}", topic, ex.getMessage());
            }
            replicators.forEach((region, replicator) -> replicator.startProducer());
            return null;
        });
    }

    public CompletableFuture<Void> stopReplProducers() {
        List<CompletableFuture<Void>> closeFutures = new ArrayList<>();
        replicators.forEach((region, replicator) -> closeFutures.add(replicator.disconnect()));
        shadowReplicators.forEach((__, replicator) -> closeFutures.add(replicator.disconnect()));
        return FutureUtil.waitForAll(closeFutures);
    }

    private synchronized CompletableFuture<Void> closeReplProducersIfNoBacklog() {
        List<CompletableFuture<Void>> closeFutures = new ArrayList<>();
        replicators.forEach((region, replicator) -> closeFutures.add(replicator.disconnect(true)));
        shadowReplicators.forEach((__, replicator) -> closeFutures.add(replicator.disconnect(true)));
        return FutureUtil.waitForAll(closeFutures);
    }

    @Override
    protected void handleProducerRemoved(Producer producer) {
        super.handleProducerRemoved(producer);
        messageDeduplication.producerRemoved(producer.getProducerName());
    }

    @Override
    public CompletableFuture<Consumer> subscribe(SubscriptionOption option) {
        return internalSubscribe(option.getCnx(), option.getSubscriptionName(), option.getConsumerId(),
                option.getSubType(), option.getPriorityLevel(), option.getConsumerName(), option.isDurable(),
                option.getStartMessageId(), option.getMetadata(), option.isReadCompacted(),
                option.getInitialPosition(), option.getStartMessageRollbackDurationSec(),
                option.isReplicatedSubscriptionStateArg(), option.getKeySharedMeta(),
                option.getSubscriptionProperties().orElse(Collections.emptyMap()),
                option.getConsumerEpoch(), option.getSchemaType());
    }

    private CompletableFuture<Consumer> internalSubscribe(final TransportCnx cnx, String subscriptionName,
                                                          long consumerId, SubType subType, int priorityLevel,
                                                          String consumerName, boolean isDurable,
                                                          MessageId startMessageId,
                                                          Map<String, String> metadata, boolean readCompacted,
                                                          InitialPosition initialPosition,
                                                          long startMessageRollbackDurationSec,
                                                          boolean replicatedSubscriptionStateArg,
                                                          KeySharedMeta keySharedMeta,
                                                          Map<String, String> subscriptionProperties,
                                                          long consumerEpoch,
                                                          SchemaType schemaType) {
        if (readCompacted && !(subType == SubType.Failover || subType == SubType.Exclusive)) {
            return FutureUtil.failedFuture(new NotAllowedException(
                    "readCompacted only allowed on failover or exclusive subscriptions"));
        }

        return brokerService.checkTopicNsOwnership(getName()).thenCompose(__ -> {
            boolean replicatedSubscriptionState = replicatedSubscriptionStateArg;

            if (replicatedSubscriptionState
                    && !brokerService.pulsar().getConfiguration().isEnableReplicatedSubscriptions()) {
                log.warn("[{}] Replicated Subscription is disabled by broker.", getName());
                replicatedSubscriptionState = false;
            }

            if (subType == SubType.Key_Shared
                    && !brokerService.pulsar().getConfiguration().isSubscriptionKeySharedEnable()) {
                return FutureUtil.failedFuture(
                        new NotAllowedException("Key_Shared subscription is disabled by broker."));
            }

            try {
                if (!SystemTopicNames.isTopicPoliciesSystemTopic(topic)
                        && !checkSubscriptionTypesEnable(subType)) {
                    return FutureUtil.failedFuture(
                            new NotAllowedException("Topic[{" + topic + "}] doesn't support "
                                    + subType.name() + " sub type!"));
                }
            } catch (Exception e) {
                return FutureUtil.failedFuture(e);
            }

            if (isBlank(subscriptionName)) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Empty subscription name", topic);
                }
                return FutureUtil.failedFuture(new NamingException("Empty subscription name"));
            }

            if (hasBatchMessagePublished && !cnx.isBatchMessageCompatibleVersion()) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Consumer doesn't support batch-message {}", topic, subscriptionName);
                }
                return FutureUtil.failedFuture(
                        new UnsupportedVersionException("Consumer doesn't support batch-message"));
            }

            if (subscriptionName.startsWith(replicatorPrefix)
                    || subscriptionName.equals(DEDUPLICATION_CURSOR_NAME)) {
                log.warn("[{}] Failed to create subscription for {}", topic, subscriptionName);
                return FutureUtil.failedFuture(
                        new NamingException("Subscription with reserved subscription name attempted"));
            }

            if (cnx.clientAddress() != null && cnx.clientAddress().toString().contains(":")
                    && subscribeRateLimiter.isPresent()) {
                SubscribeRateLimiter.ConsumerIdentifier consumer = new SubscribeRateLimiter.ConsumerIdentifier(
                        cnx.clientAddress().toString().split(":")[0], consumerName, consumerId);
                if (!subscribeRateLimiter.get().subscribeAvailable(consumer)
                        || !subscribeRateLimiter.get().tryAcquire(consumer)) {
                    log.warn("[{}] Failed to create subscription for {} {} limited by {}, available {}",
                            topic, subscriptionName, consumer, subscribeRateLimiter.get().getSubscribeRate(),
                            subscribeRateLimiter.get().getAvailableSubscribeRateLimit(consumer));
                    return FutureUtil.failedFuture(
                            new NotAllowedException("Subscribe limited by subscribe rate limit per consumer."));
                }
            }

            lock.readLock().lock();
            try {
                if (isFenced) {
                    log.warn("[{}] Attempting to subscribe to a fenced topic", topic);
                    return FutureUtil.failedFuture(new TopicFencedException("Topic is temporarily unavailable"));
                }
                handleConsumerAdded(subscriptionName, consumerName);
            } finally {
                lock.readLock().unlock();
            }

            CompletableFuture<? extends Subscription> subscriptionFuture = isDurable
                    ? getDurableSubscription(subscriptionName, initialPosition, startMessageRollbackDurationSec,
                            replicatedSubscriptionState, subscriptionProperties)
                    : getNonDurableSubscription(subscriptionName, startMessageId, initialPosition,
                    startMessageRollbackDurationSec, readCompacted, subscriptionProperties);

            CompletableFuture<Consumer> future = subscriptionFuture.thenCompose(subscription -> {
                Consumer consumer = new Consumer(subscription, subType, topic, consumerId, priorityLevel,
                        consumerName, isDurable, cnx, cnx.getAuthRole(), metadata,
                        readCompacted, keySharedMeta, startMessageId, consumerEpoch, schemaType);

                return addConsumerToSubscription(subscription, consumer).thenCompose(v -> {
                    if (subscription instanceof PersistentSubscription persistentSubscription) {
                        checkBackloggedCursor(persistentSubscription);
                    }
                    if (!cnx.isActive()) {
                        try {
                            consumer.close();
                        } catch (BrokerServiceException e) {
                            if (e instanceof ConsumerBusyException) {
                                log.warn("[{}][{}] Consumer {} {} already connected: {}",
                                        topic, subscriptionName, consumerId, consumerName, e.getMessage());
                            } else if (e instanceof SubscriptionBusyException) {
                                log.warn("[{}][{}] {}", topic, subscriptionName, e.getMessage());
                            }

                            decrementUsageCount();
                            return FutureUtil.failedFuture(e);
                        }
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] [{}] [{}] Subscribe failed -- count: {}", topic, subscriptionName,
                                    consumer.consumerName(), currentUsageCount());
                        }

                        decrementUsageCount();
                        return FutureUtil.failedFuture(
                                new BrokerServiceException("Connection was closed while the opening the cursor "));
                    } else {
                        checkReplicatedSubscriptionControllerState();
                        if (log.isDebugEnabled()) {
                            log.debug("[{}][{}] Created new subscription for {}", topic, subscriptionName, consumerId);
                        }
                        return CompletableFuture.completedFuture(consumer);
                    }
                });
            });

            future.exceptionally(ex -> {
                decrementUsageCount();

                if (ex.getCause() instanceof ConsumerBusyException) {
                    log.warn("[{}][{}] Consumer {} {} already connected: {}", topic, subscriptionName, consumerId,
                            consumerName, ex.getCause().getMessage());
                    Consumer consumer = null;
                    try {
                        consumer = subscriptionFuture.isDone() ? getActiveConsumer(subscriptionFuture.get()) : null;
                        // cleanup consumer if connection is already closed
                        if (consumer != null && !consumer.cnx().isActive()) {
                            consumer.close();
                        }
                    } catch (Exception be) {
                        log.error("Failed to clean up consumer on closed connection {}, {}", consumer, be.getMessage());
                    }
                } else if (ex.getCause() instanceof SubscriptionBusyException) {
                    log.warn("[{}][{}] {}", topic, subscriptionName, ex.getMessage());
                } else if (ex.getCause() instanceof BrokerServiceException.SubscriptionFencedException
                        && isCompactionSubscription(subscriptionName)) {
                    log.warn("[{}] Failed to create compaction subscription: {}", topic, ex.getMessage());
                } else {
                    log.error("[{}] Failed to create subscription: {}", topic, subscriptionName, ex);
                }
                return null;
            });
            return future;
        });
    }

    @Override
    public CompletableFuture<Consumer> subscribe(final TransportCnx cnx, String subscriptionName, long consumerId,
                                                 SubType subType, int priorityLevel, String consumerName,
                                                 boolean isDurable, MessageId startMessageId,
                                                 Map<String, String> metadata, boolean readCompacted,
                                                 InitialPosition initialPosition,
                                                 long startMessageRollbackDurationSec,
                                                 boolean replicatedSubscriptionStateArg,
                                                 KeySharedMeta keySharedMeta) {
        return internalSubscribe(cnx, subscriptionName, consumerId, subType, priorityLevel, consumerName,
                isDurable, startMessageId, metadata, readCompacted, initialPosition, startMessageRollbackDurationSec,
                replicatedSubscriptionStateArg, keySharedMeta, null, DEFAULT_CONSUMER_EPOCH, null);
    }

    private CompletableFuture<Subscription> getDurableSubscription(String subscriptionName,
                                                                   InitialPosition initialPosition,
                                                                   long startMessageRollbackDurationSec,
                                                                   boolean replicated,
                                                                   Map<String, String> subscriptionProperties) {
        CompletableFuture<Subscription> subscriptionFuture = new CompletableFuture<>();
        if (checkMaxSubscriptionsPerTopicExceed(subscriptionName)) {
            subscriptionFuture.completeExceptionally(new NotAllowedException(
                    "Exceed the maximum number of subscriptions of the topic: " + topic));
            return subscriptionFuture;
        }

        Map<String, Long> properties = PersistentSubscription.getBaseCursorProperties(replicated);
        ledger.asyncOpenCursor(Codec.encode(subscriptionName), initialPosition, properties, subscriptionProperties,
                new OpenCursorCallback() {
            @Override
            public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Opened cursor", topic, subscriptionName);
                }

                PersistentSubscription subscription = subscriptions.get(subscriptionName);
                if (subscription == null) {
                    subscription = subscriptions.computeIfAbsent(subscriptionName,
                                  name -> createPersistentSubscription(subscriptionName, cursor,
                                          replicated, subscriptionProperties));
                } else {
                    // if subscription exists, check if it's a non-durable subscription
                    if (subscription.getCursor() != null && !subscription.getCursor().isDurable()) {
                        subscriptionFuture.completeExceptionally(
                                new NotAllowedException("NonDurable subscription with the same name already exists."));
                        return;
                    }
                }
                if (replicated && !subscription.isReplicated()) {
                    // Flip the subscription state
                    subscription.setReplicated(replicated);
                }

                if (startMessageRollbackDurationSec > 0) {
                    resetSubscriptionCursor(subscription, subscriptionFuture, startMessageRollbackDurationSec);
                } else {
                    subscriptionFuture.complete(subscription);
                }
            }

            @Override
            public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                log.warn("[{}] Failed to create subscription for {}: {}", topic, subscriptionName,
                        exception.getMessage());
                decrementUsageCount();
                subscriptionFuture.completeExceptionally(new PersistenceException(exception));
                if (exception instanceof ManagedLedgerFencedException) {
                    // If the managed ledger has been fenced, we cannot continue using it. We need to close and reopen
                    close();
                }
            }
        }, null);
        return subscriptionFuture;
    }

    private CompletableFuture<? extends Subscription> getNonDurableSubscription(String subscriptionName,
            MessageId startMessageId, InitialPosition initialPosition, long startMessageRollbackDurationSec,
            boolean isReadCompacted, Map<String, String> subscriptionProperties) {
        log.info("[{}][{}] Creating non-durable subscription at msg id {} - {}",
                topic, subscriptionName, startMessageId, subscriptionProperties);

        CompletableFuture<Subscription> subscriptionFuture = new CompletableFuture<>();
        if (checkMaxSubscriptionsPerTopicExceed(subscriptionName)) {
            subscriptionFuture.completeExceptionally(new NotAllowedException(
                    "Exceed the maximum number of subscriptions of the topic: " + topic));
            return subscriptionFuture;
        }

        synchronized (ledger) {
            // Create a new non-durable cursor only for the first consumer that connects
            PersistentSubscription subscription = subscriptions.get(subscriptionName);

            if (subscription == null) {
                MessageIdImpl msgId = startMessageId != null ? (MessageIdImpl) startMessageId
                        : (MessageIdImpl) MessageId.latest;

                long ledgerId = msgId.getLedgerId();
                long entryId = msgId.getEntryId();
                // Ensure that the start message id starts from a valid entry.
                if (ledgerId >= 0 && entryId >= 0
                        && msgId instanceof BatchMessageIdImpl) {
                    // When the start message is relative to a batch, we need to take one step back on the previous
                    // message,
                    // because the "batch" might not have been consumed in its entirety.
                    // The client will then be able to discard the first messages if needed.
                    entryId = msgId.getEntryId() - 1;
                }

                Position startPosition = new PositionImpl(ledgerId, entryId);
                ManagedCursor cursor = null;
                try {
                    cursor = ledger.newNonDurableCursor(startPosition, subscriptionName, initialPosition,
                            isReadCompacted);
                } catch (ManagedLedgerException e) {
                    return FutureUtil.failedFuture(e);
                }

                subscription = new PersistentSubscription(this, subscriptionName, cursor, false,
                        subscriptionProperties);
                subscriptions.put(subscriptionName, subscription);
            } else {
                // if subscription exists, check if it's a durable subscription
                if (subscription.getCursor() != null && subscription.getCursor().isDurable()) {
                    return FutureUtil.failedFuture(
                            new NotAllowedException("Durable subscription with the same name already exists."));
                }
            }

            if (startMessageRollbackDurationSec > 0) {
                resetSubscriptionCursor(subscription, subscriptionFuture, startMessageRollbackDurationSec);
                return subscriptionFuture;
            } else {
                return CompletableFuture.completedFuture(subscription);
            }
        }
    }

    private void resetSubscriptionCursor(Subscription subscription, CompletableFuture<Subscription> subscriptionFuture,
                                         long startMessageRollbackDurationSec) {
        long timestamp = System.currentTimeMillis()
                - SECONDS.toMillis(startMessageRollbackDurationSec);
        final Subscription finalSubscription = subscription;
        subscription.resetCursor(timestamp).handle((s, ex) -> {
            if (ex != null) {
                log.warn("[{}] Failed to reset cursor {} position at timestamp {}, caused by {}", topic,
                        subscription.getName(), startMessageRollbackDurationSec, ex.getMessage());
            }
            subscriptionFuture.complete(finalSubscription);
            return null;
        });
    }

    @Override
    public CompletableFuture<Subscription> createSubscription(String subscriptionName, InitialPosition initialPosition,
                                                              boolean replicateSubscriptionState,
                                                              Map<String, String> subscriptionProperties) {
        return getDurableSubscription(subscriptionName, initialPosition,
                0 /*avoid reseting cursor*/, replicateSubscriptionState, subscriptionProperties);
    }

    /**
     * Delete the cursor ledger for a given subscription.
     *
     * @param subscriptionName Subscription for which the cursor ledger is to be deleted
     * @return Completable future indicating completion of unsubscribe operation Completed exceptionally with:
     *         ManagedLedgerException if cursor ledger delete fails
     */
    @Override
    public CompletableFuture<Void> unsubscribe(String subscriptionName) {
        CompletableFuture<Void> unsubscribeFuture = new CompletableFuture<>();

        TopicName tn = TopicName.get(MLPendingAckStore
                .getTransactionPendingAckStoreSuffix(topic,
                        Codec.encode(subscriptionName)));
        if (brokerService.pulsar().getConfiguration().isTransactionCoordinatorEnabled()) {
            getBrokerService().getManagedLedgerFactory().asyncDelete(tn.getPersistenceNamingEncoding(),
                    getBrokerService().getManagedLedgerConfig(tn),
                    new AsyncCallbacks.DeleteLedgerCallback() {
                        @Override
                        public void deleteLedgerComplete(Object ctx) {
                            asyncDeleteCursorWithClearDelayedMessage(subscriptionName, unsubscribeFuture);
                        }

                        @Override
                        public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                            if (exception instanceof MetadataNotFoundException) {
                                asyncDeleteCursorWithClearDelayedMessage(subscriptionName, unsubscribeFuture);
                                return;
                            }

                            unsubscribeFuture.completeExceptionally(exception);
                            log.error("[{}][{}] Error deleting subscription pending ack store",
                                    topic, subscriptionName, exception);
                        }
                    }, null);
        } else {
            asyncDeleteCursorWithClearDelayedMessage(subscriptionName, unsubscribeFuture);
        }

        return unsubscribeFuture;
    }

    private void asyncDeleteCursorWithClearDelayedMessage(String subscriptionName,
                                                          CompletableFuture<Void> unsubscribeFuture) {
        PersistentSubscription persistentSubscription = subscriptions.get(subscriptionName);
        if (persistentSubscription == null) {
            log.warn("[{}][{}] Can't find subscription, skip delete cursor", topic, subscriptionName);
            unsubscribeFuture.complete(null);
            return;
        }

        if (!isDelayedDeliveryEnabled()
                || !(brokerService.getDelayedDeliveryTrackerFactory() instanceof BucketDelayedDeliveryTrackerFactory)) {
            asyncDeleteCursorWithCleanCompactionLedger(persistentSubscription, unsubscribeFuture);
            return;
        }

        Dispatcher dispatcher = persistentSubscription.getDispatcher();
        if (dispatcher == null) {
            DelayedDeliveryTrackerFactory delayedDeliveryTrackerFactory =
                    brokerService.getDelayedDeliveryTrackerFactory();
            if (delayedDeliveryTrackerFactory instanceof BucketDelayedDeliveryTrackerFactory
                    bucketDelayedDeliveryTrackerFactory) {
                ManagedCursor cursor = persistentSubscription.getCursor();
                bucketDelayedDeliveryTrackerFactory.cleanResidualSnapshots(cursor).whenComplete((__, ex) -> {
                    if (ex != null) {
                        unsubscribeFuture.completeExceptionally(ex);
                    } else {
                        asyncDeleteCursorWithCleanCompactionLedger(persistentSubscription, unsubscribeFuture);
                    }
                });
            }
            return;
        }

        dispatcher.clearDelayedMessages().whenComplete((__, ex) -> {
            if (ex != null) {
                unsubscribeFuture.completeExceptionally(ex);
            } else {
                asyncDeleteCursorWithCleanCompactionLedger(persistentSubscription, unsubscribeFuture);
            }
        });
    }

    private void asyncDeleteCursorWithCleanCompactionLedger(PersistentSubscription subscription,
                                                            CompletableFuture<Void> unsubscribeFuture) {
        final String subscriptionName = subscription.getName();
        if ((!isCompactionSubscription(subscriptionName)) || !(subscription instanceof PulsarCompactorSubscription)) {
            asyncDeleteCursor(subscriptionName, unsubscribeFuture);
            return;
        }

        currentCompaction.handle((__, e) -> {
            if (e != null) {
                log.warn("[{}][{}] Last compaction task failed", topic, subscriptionName);
            }
            return ((PulsarCompactorSubscription) subscription).cleanCompactedLedger();
        }).whenComplete((__, ex) -> {
            if (ex != null) {
                log.error("[{}][{}] Error cleaning compacted ledger", topic, subscriptionName, ex);
                unsubscribeFuture.completeExceptionally(ex);
            } else {
                asyncDeleteCursor(subscriptionName, unsubscribeFuture);
            }
        });
    }

    private void asyncDeleteCursor(String subscriptionName, CompletableFuture<Void> unsubscribeFuture) {
        ledger.asyncDeleteCursor(Codec.encode(subscriptionName), new DeleteCursorCallback() {
            @Override
            public void deleteCursorComplete(Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Cursor deleted successfully", topic, subscriptionName);
                }
                removeSubscription(subscriptionName);
                unsubscribeFuture.complete(null);
                lastActive = System.nanoTime();
            }

            @Override
            public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Error deleting cursor for subscription",
                            topic, subscriptionName, exception);
                }
                if (exception instanceof ManagedLedgerException.ManagedLedgerNotFoundException) {
                    unsubscribeFuture.complete(null);
                    lastActive = System.nanoTime();
                    return;
                }
                unsubscribeFuture.completeExceptionally(new PersistenceException(exception));
            }
        }, null);
    }

    void removeSubscription(String subscriptionName) {
        PersistentSubscription sub = subscriptions.remove(subscriptionName);
        if (sub != null) {
            // preserve accumulative stats form removed subscription
            SubscriptionStatsImpl stats = sub.getStats(new GetStatsOptions(false, false, false, false, false));
            bytesOutFromRemovedSubscriptions.add(stats.bytesOutCounter);
            msgOutFromRemovedSubscriptions.add(stats.msgOutCounter);
        }
    }

    /**
     * Delete the managed ledger associated with this topic.
     *
     * @return Completable future indicating completion of delete operation Completed exceptionally with:
     *         IllegalStateException if topic is still active ManagedLedgerException if ledger delete operation fails
     */
    @Override
    public CompletableFuture<Void> delete() {
        return delete(false, false, false);
    }

    /**
     * Forcefully close all producers/consumers/replicators and deletes the topic. this function is used when local
     * cluster is removed from global-namespace replication list. Because broker doesn't allow lookup if local cluster
     * is not part of replication cluster list.
     *
     * @return
     */
    @Override
    public CompletableFuture<Void> deleteForcefully() {
        return delete(false, false, true);
    }

    /**
     * Delete the managed ledger associated with this topic.
     *
     * @param failIfHasSubscriptions
     *            Flag indicating whether delete should succeed if topic still has unconnected subscriptions. Set to
     *            false when called from admin API (it will delete the subs too), and set to true when called from GC
     *            thread
     * @param failIfHasBacklogs
     *            Flag indicating whether delete should succeed if topic has backlogs. Set to false when called from
     *            admin API (it will delete the subs too), and set to true when called from GC thread
     * @param closeIfClientsConnected
     *            Flag indicate whether explicitly close connected
     *            producers/consumers/replicators before trying to delete topic.
     *            If any client is connected to a topic and if this flag is disable then this operation fails.
     *
     * @return Completable future indicating completion of delete operation Completed exceptionally with:
     *         IllegalStateException if topic is still active ManagedLedgerException if ledger delete operation fails
     */
    private CompletableFuture<Void> delete(boolean failIfHasSubscriptions,
                                           boolean failIfHasBacklogs,
                                           boolean closeIfClientsConnected) {

        lock.writeLock().lock();
        try {
            if (isClosingOrDeleting) {
                log.warn("[{}] Topic is already being closed or deleted", topic);
                return FutureUtil.failedFuture(new TopicFencedException("Topic is already fenced"));
            }
            // We can proceed with the deletion if either:
            //  1. No one is connected and no subscriptions
            //  2. The topic have subscriptions but no backlogs for all subscriptions
            //     if delete_when_no_subscriptions is applied
            //  3. We want to kick out everyone and forcefully delete the topic.
            //     In this case, we shouldn't care if the usageCount is 0 or not, just proceed
            if (!closeIfClientsConnected) {
                if (failIfHasSubscriptions && !subscriptions.isEmpty()) {
                    return FutureUtil.failedFuture(
                            new TopicBusyException("Topic has subscriptions: " + subscriptions.keys()));
                } else if (failIfHasBacklogs) {
                    if (hasBacklogs()) {
                        List<String> backlogSubs =
                                subscriptions.values().stream()
                                        .filter(sub -> sub.getNumberOfEntriesInBacklog(false) > 0)
                                        .map(PersistentSubscription::getName).toList();
                        return FutureUtil.failedFuture(
                                new TopicBusyException("Topic has subscriptions did not catch up: " + backlogSubs));
                    } else if (!producers.isEmpty()) {
                        return FutureUtil.failedFuture(new TopicBusyException(
                                "Topic has " + producers.size() + " connected producers"));
                    }
                } else if (currentUsageCount() > 0) {
                    return FutureUtil.failedFuture(new TopicBusyException(
                            "Topic has " + currentUsageCount() + " connected producers/consumers"));
                }
            }

            fenceTopicToCloseOrDelete(); // Avoid clients reconnections while deleting

            return getBrokerService().getPulsar().getPulsarResources().getNamespaceResources()
                        .getPartitionedTopicResources().runWithMarkDeleteAsync(TopicName.get(topic), () -> {
                CompletableFuture<Void> deleteFuture = new CompletableFuture<>();

                CompletableFuture<Void> closeClientFuture = new CompletableFuture<>();
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                subscriptions.forEach((s, sub) -> futures.add(sub.close(true, Optional.empty())));
                if (closeIfClientsConnected) {
                    replicators.forEach((cluster, replicator) -> futures.add(replicator.disconnect()));
                    shadowReplicators.forEach((__, replicator) -> futures.add(replicator.disconnect()));
                    producers.values().forEach(producer -> futures.add(producer.disconnect()));
                }
                FutureUtil.waitForAll(futures).thenRunAsync(() -> {
                    closeClientFuture.complete(null);
                }, getOrderedExecutor()).exceptionally(ex -> {
                    log.error("[{}] Error closing clients", topic, ex);
                    unfenceTopicToResume();
                    closeClientFuture.completeExceptionally(ex);
                    return null;
                });

                closeClientFuture.thenAccept(__ -> {
                    CompletableFuture<Void> deleteTopicAuthenticationFuture = new CompletableFuture<>();
                    brokerService.deleteTopicAuthenticationWithRetry(topic, deleteTopicAuthenticationFuture, 5);

                        deleteTopicAuthenticationFuture.thenCompose(ignore -> deleteSchema())
                                .thenCompose(ignore -> {
                                    if (!SystemTopicNames.isTopicPoliciesSystemTopic(topic)
                                            && brokerService.getPulsar().getConfiguration().isSystemTopicEnabled()) {
                                        return deleteTopicPolicies();
                                    } else {
                                        return CompletableFuture.completedFuture(null);
                                    }
                                })
                                .thenCompose(ignore -> transactionBufferCleanupAndClose())
                                .whenComplete((v, ex) -> {
                                    if (ex != null) {
                                        log.error("[{}] Error deleting topic", topic, ex);
                                        unfenceTopicToResume();
                                        deleteFuture.completeExceptionally(ex);
                                    } else {
                                        List<CompletableFuture<Void>> subsDeleteFutures = new ArrayList<>();
                                        subscriptions.forEach((sub, p) -> subsDeleteFutures.add(unsubscribe(sub)));

                                    FutureUtil.waitForAll(subsDeleteFutures).whenComplete((f, e) -> {
                                        if (e != null) {
                                            log.error("[{}] Error deleting topic", topic, e);
                                            unfenceTopicToResume();
                                            deleteFuture.completeExceptionally(e);
                                        } else {
                                            ledger.asyncDelete(new AsyncCallbacks.DeleteLedgerCallback() {
                                                @Override
                                                public void deleteLedgerComplete(Object ctx) {
                                                    brokerService.removeTopicFromCache(PersistentTopic.this);

                                                    dispatchRateLimiter.ifPresent(DispatchRateLimiter::close);

                                                    subscribeRateLimiter.ifPresent(SubscribeRateLimiter::close);

                                                    unregisterTopicPolicyListener();

                                                    log.info("[{}] Topic deleted", topic);
                                                    deleteFuture.complete(null);
                                                }

                                                @Override
                                                public void
                                                deleteLedgerFailed(ManagedLedgerException exception,
                                                                   Object ctx) {
                                                    if (exception.getCause()
                                                            instanceof MetadataStoreException.NotFoundException) {
                                                        log.info("[{}] Topic is already deleted {}",
                                                                topic, exception.getMessage());
                                                        deleteLedgerComplete(ctx);
                                                    } else {
                                                        log.error("[{}] Error deleting topic",
                                                                topic, exception);
                                                        unfenceTopicToResume();
                                                        deleteFuture.completeExceptionally(
                                                                new PersistenceException(exception));
                                                    }
                                                }
                                            }, null);

                                        }
                                    });
                                }
                            });
                }).exceptionally(ex->{
                    unfenceTopicToResume();
                    deleteFuture.completeExceptionally(
                            new TopicBusyException("Failed to close clients before deleting topic."));
                    return null;
                });

                return deleteFuture;
                }).whenComplete((value, ex) -> {
                    if (ex != null) {
                        log.error("[{}] Error deleting topic", topic, ex);
                        unfenceTopicToResume();
                    }
                });
        } finally {
            lock.writeLock().unlock();
        }

    }

    public CompletableFuture<Void> close() {
        return close(true, false);
    }

    @Override
    public CompletableFuture<Void> close(boolean closeWithoutWaitingClientDisconnect) {
        return close(true, closeWithoutWaitingClientDisconnect);
    }

    /**
     * Close this topic - close all producers and subscriptions associated with this topic.
     *
     * @param disconnectClients disconnect clients
     * @param closeWithoutWaitingClientDisconnect don't wait for client disconnect and forcefully close managed-ledger
     * @return Completable future indicating completion of close operation
     */
    @Override
    public CompletableFuture<Void> close(
            boolean disconnectClients, boolean closeWithoutWaitingClientDisconnect) {
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        lock.writeLock().lock();
        try {
            if (!disconnectClients) {
                transferring = true;
            }
            // closing managed-ledger waits until all producers/consumers/replicators get closed. Sometimes, broker
            // forcefully wants to close managed-ledger without waiting all resources to be closed.
            if (!isClosingOrDeleting || closeWithoutWaitingClientDisconnect) {
                fenceTopicToCloseOrDelete();
            } else {
                log.warn("[{}] Topic is already being closed or deleted", topic);
                closeFuture.completeExceptionally(new TopicFencedException("Topic is already fenced"));
                return closeFuture;
            }
        } finally {
            lock.writeLock().unlock();
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        futures.add(transactionBuffer.closeAsync());
        replicators.forEach((cluster, replicator) -> futures.add(replicator.disconnect()));
        shadowReplicators.forEach((__, replicator) -> futures.add(replicator.disconnect()));
        if (disconnectClients) {
            futures.add(ExtensibleLoadManagerImpl.getAssignedBrokerLookupData(
                brokerService.getPulsar(), topic).thenAccept(lookupData -> {
                    producers.values().forEach(producer -> futures.add(producer.disconnect(lookupData)));
                    // Topics unloaded due to the ExtensibleLoadManager undergo closing twice: first with
                    // disconnectClients = false, second with disconnectClients = true. The check below identifies the
                    // cases when Topic.close is called outside the scope of the ExtensibleLoadManager. In these
                    // situations, we must pursue the regular Subscription.close, as Topic.close is invoked just once.
                    if (isTransferring()) {
                        subscriptions.forEach((s, sub) -> futures.add(sub.disconnect(lookupData)));
                    } else {
                        subscriptions.forEach((s, sub) -> futures.add(sub.close(true, lookupData)));
                    }
                }
            ));
        } else {
            subscriptions.forEach((s, sub) -> futures.add(sub.close(false, Optional.empty())));
        }

        //close entry filters
        if (entryFilters != null) {
            entryFilters.getRight().forEach((filter) -> {
                try {
                    filter.close();
                } catch (Throwable e) {
                    log.warn("Error shutting down entry filter {}", filter, e);
                }
            });
        }

        if (topicCompactionService != null) {
            try {
                topicCompactionService.close();
            } catch (Exception e) {
                log.warn("Error close topicCompactionService ", e);
            }
        }

        CompletableFuture<Void> clientCloseFuture = closeWithoutWaitingClientDisconnect
                ? CompletableFuture.completedFuture(null)
                : FutureUtil.waitForAll(futures);

        clientCloseFuture.thenRun(() -> {
            // After having disconnected all producers/consumers, close the managed ledger
            ledger.asyncClose(new CloseCallback() {
                @Override
                public void closeComplete(Object ctx) {
                    if (disconnectClients) {
                        // Everything is now closed, remove the topic from map
                        disposeTopic(closeFuture);
                    } else {
                        closeFuture.complete(null);
                    }
                }

                @Override
                public void closeFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("[{}] Failed to close managed ledger, proceeding anyway.", topic, exception);
                    if (disconnectClients) {
                        disposeTopic(closeFuture);
                    } else {
                        closeFuture.complete(null);
                    }
                }
            }, null);
        }).exceptionally(exception -> {
            log.error("[{}] Error closing topic", topic, exception);
            unfenceTopicToResume();
            closeFuture.completeExceptionally(exception);
            return null;
        });

        return closeFuture;
    }

    private void disposeTopic(CompletableFuture<?> closeFuture) {
        brokerService.removeTopicFromCache(PersistentTopic.this)
                .thenRun(() -> {
                    replicatedSubscriptionsController.ifPresent(ReplicatedSubscriptionsController::close);

                    dispatchRateLimiter.ifPresent(DispatchRateLimiter::close);

                    subscribeRateLimiter.ifPresent(SubscribeRateLimiter::close);

                    unregisterTopicPolicyListener();
                    log.info("[{}] Topic closed", topic);
                    cancelFencedTopicMonitoringTask();
                    closeFuture.complete(null);
                })
                .exceptionally(ex -> {
                    closeFuture.completeExceptionally(ex);
                    return null;
                });
    }

    @VisibleForTesting
    CompletableFuture<Void> checkReplicationAndRetryOnFailure() {
        CompletableFuture<Void> result = new CompletableFuture<Void>();
        checkReplication().thenAccept(res -> {
            result.complete(null);
        }).exceptionally(th -> {
            log.error("[{}] Policies update failed {}, scheduled retry in {} seconds", topic, th.getMessage(),
                    POLICY_UPDATE_FAILURE_RETRY_TIME_SECONDS, th);
            if (!(th.getCause() instanceof TopicFencedException)) {
                // retriable exception
                brokerService.executor().schedule(this::checkReplicationAndRetryOnFailure,
                        POLICY_UPDATE_FAILURE_RETRY_TIME_SECONDS, SECONDS);
            }
            result.completeExceptionally(th);
            return null;
        });
        return result;
    }

    public CompletableFuture<Void> checkDeduplicationStatus() {
        return messageDeduplication.checkStatus();
    }

    @VisibleForTesting
    CompletableFuture<Void> checkPersistencePolicies() {
        TopicName topicName = TopicName.get(topic);
        CompletableFuture<Void> future = new CompletableFuture<>();
        brokerService.getManagedLedgerConfig(topicName).thenAccept(config -> {
            // update managed-ledger config and managed-cursor.markDeleteRate
            this.ledger.setConfig(config);
            future.complete(null);
        }).exceptionally(ex -> {
            log.warn("[{}] Failed to update persistence-policies {}", topic, ex.getMessage());
            future.completeExceptionally(ex);
            return null;
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> checkReplication() {
        TopicName name = TopicName.get(topic);
        if (!name.isGlobal() || NamespaceService.isHeartbeatNamespace(name)
                || ExtensibleLoadManagerImpl.isInternalTopic(topic)) {
            return CompletableFuture.completedFuture(null);
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Checking replication status", name);
        }

        List<String> configuredClusters = topicPolicies.getReplicationClusters().get();
        if (CollectionUtils.isEmpty(configuredClusters)) {
            log.warn("[{}] No replication clusters configured", name);
            return CompletableFuture.completedFuture(null);
        }

        int newMessageTTLInSeconds = topicPolicies.getMessageTTLInSeconds().get();

        String localCluster = brokerService.pulsar().getConfiguration().getClusterName();

        // if local cluster is removed from global namespace cluster-list : then delete topic forcefully
        // because pulsar doesn't serve global topic without local repl-cluster configured.
        if (TopicName.get(topic).isGlobal() && !configuredClusters.contains(localCluster)) {
            log.info("Deleting topic [{}] because local cluster is not part of "
                    + " global namespace repl list {}", topic, configuredClusters);
            return deleteForcefully();
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // Check for missing replicators
        for (String cluster : configuredClusters) {
            if (cluster.equals(localCluster)) {
                continue;
            }
            if (!replicators.containsKey(cluster)) {
                futures.add(startReplicator(cluster));
            }
        }

        // Check for replicators to be stopped
        replicators.forEach((cluster, replicator) -> {
            // Update message TTL
            ((PersistentReplicator) replicator).updateMessageTTL(newMessageTTLInSeconds);
            if (!cluster.equals(localCluster)) {
                if (!configuredClusters.contains(cluster)) {
                    futures.add(removeReplicator(cluster));
                }
            }
        });

        futures.add(checkShadowReplication());

        return FutureUtil.waitForAll(futures);
    }

    private CompletableFuture<Void> checkShadowReplication() {
        if (CollectionUtils.isEmpty(shadowTopics)) {
            return CompletableFuture.completedFuture(null);
        }
        List<String> configuredShadowTopics = shadowTopics;
        int newMessageTTLInSeconds = topicPolicies.getMessageTTLInSeconds().get();

        if (log.isDebugEnabled()) {
            log.debug("[{}] Checking shadow replication status, shadowTopics={}", topic, configuredShadowTopics);
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // Check for missing replicators
        for (String shadowTopic : configuredShadowTopics) {
            if (!shadowReplicators.containsKey(shadowTopic)) {
                futures.add(startShadowReplicator(shadowTopic));
            }
        }

        // Check for replicators to be stopped
        shadowReplicators.forEach((shadowTopic, replicator) -> {
            // Update message TTL
            ((PersistentReplicator) replicator).updateMessageTTL(newMessageTTLInSeconds);
            if (!configuredShadowTopics.contains(shadowTopic)) {
                futures.add(removeShadowReplicator(shadowTopic));
            }
        });
        return FutureUtil.waitForAll(futures);
    }

    @Override
    public void checkMessageExpiry() {
        int messageTtlInSeconds = topicPolicies.getMessageTTLInSeconds().get();
        if (messageTtlInSeconds != 0) {
            subscriptions.forEach((__, sub) -> {
                if (!isCompactionSubscription(sub.getName())) {
                   sub.expireMessages(messageTtlInSeconds);
                }
            });
        }
    }

    @Override
    public void checkMessageDeduplicationInfo() {
        messageDeduplication.purgeInactiveProducers();
    }

    public boolean isCompactionEnabled() {
        Long compactionThreshold = topicPolicies.getCompactionThreshold().get();
        return compactionThreshold != null && compactionThreshold > 0;
    }

    public void checkCompaction() {
        TopicName name = TopicName.get(topic);
        try {
            long compactionThreshold = topicPolicies.getCompactionThreshold().get();
            if (isCompactionEnabled() && currentCompaction.isDone()) {

                long backlogEstimate = 0;

                PersistentSubscription compactionSub = subscriptions.get(COMPACTION_SUBSCRIPTION);
                if (compactionSub != null) {
                    backlogEstimate = compactionSub.estimateBacklogSize();
                } else {
                    // compaction has never run, so take full backlog size,
                    // or total size if we have no durable subs yet.
                    backlogEstimate = subscriptions.isEmpty() || subscriptions.values().stream()
                                .noneMatch(sub -> sub.getCursor().isDurable())
                            ? ledger.getTotalSize()
                            : ledger.getEstimatedBacklogSize();
                }

                if (backlogEstimate > compactionThreshold) {
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "topic:{} backlogEstimate:{} is bigger than compactionThreshold:{}. Triggering "
                                        + "compaction", topic, backlogEstimate, compactionThreshold);
                    }
                    try {
                        triggerCompaction();
                    } catch (AlreadyRunningException are) {
                        log.debug("[{}] Compaction already running, so don't trigger again, "
                                  + "even though backlog({}) is over threshold({})",
                                  name, backlogEstimate, compactionThreshold);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("[{}] Error getting policies and skipping compaction check", topic, e);
        }
    }

    public CompletableFuture<Void> preCreateSubscriptionForCompactionIfNeeded() {
        if (subscriptions.containsKey(COMPACTION_SUBSCRIPTION)) {
            // The compaction cursor is already there, nothing to do
            return CompletableFuture.completedFuture(null);
        }

        return isCompactionEnabled()
                // If a topic has a compaction policy setup, we must make sure that the compaction cursor
                // is pre-created, in order to ensure all the data will be seen by the compactor.
                ? createSubscription(COMPACTION_SUBSCRIPTION, CommandSubscribe.InitialPosition.Earliest, false, null)
                        .thenCompose(__ -> CompletableFuture.completedFuture(null))
                : CompletableFuture.completedFuture(null);
    }

    CompletableFuture<Void> startReplicator(String remoteCluster) {
        log.info("[{}] Starting replicator to remote: {}", topic, remoteCluster);
        final CompletableFuture<Void> future = new CompletableFuture<>();

        String name = PersistentReplicator.getReplicatorName(replicatorPrefix, remoteCluster);
        ledger.asyncOpenCursor(name, new OpenCursorCallback() {
            @Override
            public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                String localCluster = brokerService.pulsar().getConfiguration().getClusterName();
                addReplicationCluster(remoteCluster, cursor, localCluster).whenComplete((__, ex) -> {
                    if (ex == null) {
                        future.complete(null);
                    } else {
                        future.completeExceptionally(ex);
                    }
                });
            }

            @Override
            public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(new PersistenceException(exception));
            }

        }, null);

        return future;
    }

    private CompletableFuture<Boolean> checkReplicationCluster(String remoteCluster) {
        return brokerService.getPulsar().getPulsarResources().getNamespaceResources()
                .getPoliciesAsync(TopicName.get(topic).getNamespaceObject())
                .thenApply(optPolicies -> optPolicies.map(policies -> policies.replication_clusters)
                        .orElse(Collections.emptySet()).contains(remoteCluster)
                        || topicPolicies.getReplicationClusters().get().contains(remoteCluster));
    }

    protected CompletableFuture<Void> addReplicationCluster(String remoteCluster, ManagedCursor cursor,
            String localCluster) {
        return AbstractReplicator.validatePartitionedTopicAsync(PersistentTopic.this.getName(), brokerService)
                .thenCompose(__ -> checkReplicationCluster(remoteCluster))
                .thenCompose(clusterExists -> {
                    if (!clusterExists) {
                        log.warn("Remove the replicator because the cluster '{}' does not exist", remoteCluster);
                        return removeReplicator(remoteCluster).thenApply(__ -> null);
                    }
                    return brokerService.pulsar().getPulsarResources().getClusterResources()
                            .getClusterAsync(remoteCluster)
                            .thenApply(clusterData ->
                                    brokerService.getReplicationClient(remoteCluster, clusterData));
                })
                .thenAccept(replicationClient -> {
                    if (replicationClient == null) {
                        return;
                    }
                    Replicator replicator = replicators.computeIfAbsent(remoteCluster, r -> {
                        try {
                            return new GeoPersistentReplicator(PersistentTopic.this, cursor, localCluster,
                                    remoteCluster, brokerService, (PulsarClientImpl) replicationClient);
                        } catch (PulsarServerException e) {
                            log.error("[{}] Replicator startup failed {}", topic, remoteCluster, e);
                        }
                        return null;
                    });

                    // clean up replicator if startup is failed
                    if (replicator == null) {
                        replicators.removeNullValue(remoteCluster);
                    }
                });
    }

    CompletableFuture<Void> removeReplicator(String remoteCluster) {
        log.info("[{}] Removing replicator to {}", topic, remoteCluster);
        final CompletableFuture<Void> future = new CompletableFuture<>();

        String name = PersistentReplicator.getReplicatorName(replicatorPrefix, remoteCluster);

        Optional.ofNullable(replicators.get(remoteCluster)).map(Replicator::disconnect)
                .orElse(CompletableFuture.completedFuture(null)).thenRun(() -> {
            ledger.asyncDeleteCursor(name, new DeleteCursorCallback() {
                @Override
                public void deleteCursorComplete(Object ctx) {
                    replicators.remove(remoteCluster);
                    future.complete(null);
                }

                @Override
                public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("[{}] Failed to delete cursor {} {}", topic, name, exception.getMessage(), exception);
                    future.completeExceptionally(new PersistenceException(exception));
                }
            }, null);

        }).exceptionally(e -> {
            log.error("[{}] Failed to close replication producer {} {}", topic, name, e.getMessage(), e);
            future.completeExceptionally(e);
            return null;
        });

        return future;
    }

    CompletableFuture<Void> startShadowReplicator(String shadowTopic) {
        log.info("[{}] Starting shadow topic replicator to remote: {}", topic, shadowTopic);

        String name = ShadowReplicator.getShadowReplicatorName(replicatorPrefix, shadowTopic);
        ManagedCursor cursor;
        try {
            cursor = ledger.newNonDurableCursor(PositionImpl.LATEST, name);
        } catch (ManagedLedgerException e) {
            log.error("[{}]Open non-durable cursor for shadow replicator failed, name={}", topic, name, e);
            return FutureUtil.failedFuture(e);
        }
        CompletableFuture<Void> future = addShadowReplicationCluster(shadowTopic, cursor);
        future.exceptionally(ex -> {
            log.error("[{}] Add shadow replication cluster failed, shadowTopic={}", topic, shadowTopic, ex);
            return null;
        });
        return future;
    }

    protected CompletableFuture<Void> addShadowReplicationCluster(String shadowTopic, ManagedCursor cursor) {
        String localCluster = brokerService.pulsar().getConfiguration().getClusterName();
        return AbstractReplicator.validatePartitionedTopicAsync(PersistentTopic.this.getName(), brokerService)
                .thenCompose(__ -> brokerService.pulsar().getPulsarResources().getClusterResources()
                        .getClusterAsync(localCluster)
                        .thenApply(clusterData -> brokerService.getReplicationClient(localCluster, clusterData)))
                .thenAccept(replicationClient -> {
                    Replicator replicator = shadowReplicators.computeIfAbsent(shadowTopic, r -> {
                        try {
                            return new ShadowReplicator(shadowTopic, PersistentTopic.this, cursor, brokerService,
                                    (PulsarClientImpl) replicationClient);
                        } catch (PulsarServerException e) {
                            log.error("[{}] ShadowReplicator startup failed {}", topic, shadowTopic, e);
                        }
                        return null;
                    });

                    // clean up replicator if startup is failed
                    if (replicator == null) {
                        shadowReplicators.removeNullValue(shadowTopic);
                    }
                });
    }

    CompletableFuture<Void> removeShadowReplicator(String shadowTopic) {
        log.info("[{}] Removing shadow topic replicator to {}", topic, shadowTopic);
        final CompletableFuture<Void> future = new CompletableFuture<>();
        String name = ShadowReplicator.getShadowReplicatorName(replicatorPrefix, shadowTopic);
        shadowReplicators.get(shadowTopic).disconnect().thenRun(() -> {

            ledger.asyncDeleteCursor(name, new DeleteCursorCallback() {
                @Override
                public void deleteCursorComplete(Object ctx) {
                    shadowReplicators.remove(shadowTopic);
                    future.complete(null);
                }

                @Override
                public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("[{}] Failed to delete shadow topic replication cursor {} {}",
                            topic, name, exception.getMessage(), exception);
                    future.completeExceptionally(new PersistenceException(exception));
                }
            }, null);

        }).exceptionally(e -> {
            log.error("[{}] Failed to close shadow topic replication producer {} {}", topic, name, e.getMessage(), e);
            future.completeExceptionally(e);
            return null;
        });

        return future;
    }

    public boolean isDeduplicationEnabled() {
        return messageDeduplication.isEnabled();
    }

    @Override
    public int getNumberOfConsumers() {
        int count = 0;
        for (PersistentSubscription subscription : subscriptions.values()) {
            count += subscription.getConsumers().size();
        }
        return count;
    }

    @Override
    public int getNumberOfSameAddressConsumers(final String clientAddress) {
        return getNumberOfSameAddressConsumers(clientAddress, subscriptions.values());
    }

    @Override
    protected String getSchemaId() {
        if (shadowSourceTopic == null) {
            return super.getSchemaId();
        } else {
            //reuse schema from shadow source.
            String base = shadowSourceTopic.getPartitionedTopicName();
            return TopicName.get(base).getSchemaName();
        }
    }

    @Override
    public ConcurrentOpenHashMap<String, PersistentSubscription> getSubscriptions() {
        return subscriptions;
    }

    @Override
    public PersistentSubscription getSubscription(String subscriptionName) {
        return subscriptions.get(subscriptionName);
    }

    @Override
    public ConcurrentOpenHashMap<String, Replicator> getReplicators() {
        return replicators;
    }

    @Override
    public ConcurrentOpenHashMap<String, Replicator> getShadowReplicators() {
        return shadowReplicators;
    }

    public Replicator getPersistentReplicator(String remoteCluster) {
        return replicators.get(remoteCluster);
    }

    public ManagedLedger getManagedLedger() {
        return ledger;
    }

    @Override
    public void updateRates(NamespaceStats nsStats, NamespaceBundleStats bundleStats,
                            StatsOutputStream topicStatsStream,
                            ClusterReplicationMetrics replStats, String namespace, boolean hydratePublishers) {
        this.publishRateLimitedTimes = 0;
        TopicStatsHelper topicStatsHelper = threadLocalTopicStats.get();
        topicStatsHelper.reset();

        replicators.forEach((region, replicator) -> replicator.updateRates());

        nsStats.producerCount += producers.size();
        bundleStats.producerCount += producers.size();
        topicStatsStream.startObject(topic);

        // start publisher stats
        topicStatsStream.startList("publishers");
        producers.values().forEach(producer -> {
            producer.updateRates();
            PublisherStatsImpl publisherStats = producer.getStats();

            topicStatsHelper.aggMsgRateIn += publisherStats.msgRateIn;
            topicStatsHelper.aggMsgThroughputIn += publisherStats.msgThroughputIn;

            if (producer.isRemote()) {
                topicStatsHelper.remotePublishersStats.put(producer.getRemoteCluster(), publisherStats);
            }

            // Populate consumer specific stats here
            if (hydratePublishers) {
                StreamingStats.writePublisherStats(topicStatsStream, publisherStats);
            }
        });
        topicStatsStream.endList();
        // if publish-rate increases (eg: 0 to 1K) then pick max publish-rate and if publish-rate decreases then keep
        // average rate.
        lastUpdatedAvgPublishRateInMsg = topicStatsHelper.aggMsgRateIn > lastUpdatedAvgPublishRateInMsg
                ? topicStatsHelper.aggMsgRateIn
                : (topicStatsHelper.aggMsgRateIn + lastUpdatedAvgPublishRateInMsg) / 2;
        lastUpdatedAvgPublishRateInByte = topicStatsHelper.aggMsgThroughputIn > lastUpdatedAvgPublishRateInByte
                ? topicStatsHelper.aggMsgThroughputIn
                : (topicStatsHelper.aggMsgThroughputIn + lastUpdatedAvgPublishRateInByte) / 2;
        // Start replicator stats
        topicStatsStream.startObject("replication");
        nsStats.replicatorCount += topicStatsHelper.remotePublishersStats.size();
        replicators.forEach((cluster, replicator) -> {
            // Update replicator cursor state
            try {
                ((PersistentReplicator) replicator).updateCursorState();
            } catch (Exception e) {
                log.warn("[{}] Failed to update cursor state ", topic, e);
            }

            // Update replicator stats
            ReplicatorStatsImpl rStat = replicator.getStats();

            // Add incoming msg rates
            PublisherStatsImpl pubStats = topicStatsHelper.remotePublishersStats.get(replicator.getRemoteCluster());
            rStat.msgRateIn = pubStats != null ? pubStats.msgRateIn : 0;
            rStat.msgThroughputIn = pubStats != null ? pubStats.msgThroughputIn : 0;
            rStat.inboundConnection = pubStats != null ? pubStats.getAddress() : null;
            rStat.inboundConnectedSince = pubStats != null ? pubStats.getConnectedSince() : null;

            topicStatsHelper.aggMsgRateOut += rStat.msgRateOut;
            topicStatsHelper.aggMsgThroughputOut += rStat.msgThroughputOut;

            // Populate replicator specific stats here
            topicStatsStream.startObject(cluster);
            topicStatsStream.writePair("connected", rStat.connected);
            topicStatsStream.writePair("msgRateExpired", rStat.msgRateExpired);
            topicStatsStream.writePair("msgRateIn", rStat.msgRateIn);
            topicStatsStream.writePair("msgRateOut", rStat.msgRateOut);
            topicStatsStream.writePair("msgThroughputIn", rStat.msgThroughputIn);
            topicStatsStream.writePair("msgThroughputOut", rStat.msgThroughputOut);
            topicStatsStream.writePair("replicationBacklog", rStat.replicationBacklog);
            topicStatsStream.writePair("replicationDelayInSeconds", rStat.replicationDelayInSeconds);
            topicStatsStream.writePair("inboundConnection", rStat.inboundConnection);
            topicStatsStream.writePair("inboundConnectedSince", rStat.inboundConnectedSince);
            topicStatsStream.writePair("outboundConnection", rStat.outboundConnection);
            topicStatsStream.writePair("outboundConnectedSince", rStat.outboundConnectedSince);
            topicStatsStream.endObject();

            nsStats.msgReplBacklog += rStat.replicationBacklog;

            if (replStats.isMetricsEnabled()) {
                String namespaceClusterKey = replStats.getKeyName(namespace, cluster);
                ReplicationMetrics replicationMetrics = replStats.get(namespaceClusterKey);
                boolean update = false;
                if (replicationMetrics == null) {
                    replicationMetrics = ReplicationMetrics.get();
                    update = true;
                }
                replicationMetrics.connected += rStat.connected ? 1 : 0;
                replicationMetrics.msgRateOut += rStat.msgRateOut;
                replicationMetrics.msgThroughputOut += rStat.msgThroughputOut;
                replicationMetrics.msgReplBacklog += rStat.replicationBacklog;
                if (update) {
                    replStats.put(namespaceClusterKey, replicationMetrics);
                }
                // replication delay for a namespace is the max repl-delay among all the topics under this namespace
                if (rStat.replicationDelayInSeconds > replicationMetrics.maxMsgReplDelayInSeconds) {
                    replicationMetrics.maxMsgReplDelayInSeconds = rStat.replicationDelayInSeconds;
                }
            }
        });

        // Close replication
        topicStatsStream.endObject();

        // Start subscription stats
        topicStatsStream.startObject("subscriptions");
        nsStats.subsCount += subscriptions.size();

        subscriptions.forEach((subscriptionName, subscription) -> {
            double subMsgRateOut = 0;
            double subMsgThroughputOut = 0;
            double subMsgRateRedeliver = 0;
            double subMsgAckRate = 0;

            // Start subscription name & consumers
            try {
                topicStatsStream.startObject(subscriptionName);
                topicStatsStream.startList("consumers");

                for (Consumer consumer : subscription.getConsumers()) {
                    ++nsStats.consumerCount;
                    ++bundleStats.consumerCount;
                    consumer.updateRates();

                    ConsumerStatsImpl consumerStats = consumer.getStats();
                    subMsgRateOut += consumerStats.msgRateOut;
                    subMsgAckRate += consumerStats.messageAckRate;
                    subMsgThroughputOut += consumerStats.msgThroughputOut;
                    subMsgRateRedeliver += consumerStats.msgRateRedeliver;

                    StreamingStats.writeConsumerStats(topicStatsStream, subscription.getType(), consumerStats);
                }

                // Close Consumer stats
                topicStatsStream.endList();

                // Populate subscription specific stats here
                topicStatsStream.writePair("msgBacklog",
                        subscription.getNumberOfEntriesInBacklog(true));
                subscription.getExpiryMonitor().updateRates();
                topicStatsStream.writePair("msgRateExpired", subscription.getExpiredMessageRate());
                topicStatsStream.writePair("msgRateOut", subMsgRateOut);
                topicStatsStream.writePair("messageAckRate", subMsgAckRate);
                topicStatsStream.writePair("msgThroughputOut", subMsgThroughputOut);
                topicStatsStream.writePair("msgRateRedeliver", subMsgRateRedeliver);
                topicStatsStream.writePair("numberOfEntriesSinceFirstNotAckedMessage",
                        subscription.getNumberOfEntriesSinceFirstNotAckedMessage());
                topicStatsStream.writePair("totalNonContiguousDeletedMessagesRange",
                        subscription.getTotalNonContiguousDeletedMessagesRange());
                topicStatsStream.writePair("type", subscription.getTypeString());

                Dispatcher dispatcher0 = subscription.getDispatcher();
                if (null != dispatcher0) {
                    topicStatsStream.writePair("filterProcessedMsgCount",
                            dispatcher0.getFilterProcessedMsgCount());
                    topicStatsStream.writePair("filterAcceptedMsgCount",
                            dispatcher0.getFilterAcceptedMsgCount());
                    topicStatsStream.writePair("filterRejectedMsgCount",
                            dispatcher0.getFilterRejectedMsgCount());
                    topicStatsStream.writePair("filterRescheduledMsgCount",
                            dispatcher0.getFilterRescheduledMsgCount());
                }

                if (Subscription.isIndividualAckMode(subscription.getType())) {
                    if (subscription.getDispatcher() instanceof PersistentDispatcherMultipleConsumers) {
                        PersistentDispatcherMultipleConsumers dispatcher =
                                (PersistentDispatcherMultipleConsumers) subscription.getDispatcher();
                        topicStatsStream.writePair("blockedSubscriptionOnUnackedMsgs",
                                dispatcher.isBlockedDispatcherOnUnackedMsgs());
                        topicStatsStream.writePair("unackedMessages",
                                dispatcher.getTotalUnackedMessages());
                    }
                }

                // Close consumers
                topicStatsStream.endObject();

                topicStatsHelper.aggMsgRateOut += subMsgRateOut;
                topicStatsHelper.aggMsgThroughputOut += subMsgThroughputOut;
                nsStats.msgBacklog += subscription.getNumberOfEntriesInBacklog(false);
                // check stuck subscription
                if (brokerService.getPulsar().getConfig().isUnblockStuckSubscriptionEnabled()) {
                    subscription.checkAndUnblockIfStuck();
                }
            } catch (Exception e) {
                log.error("Got exception when creating consumer stats for subscription {}: {}", subscriptionName,
                        e.getMessage(), e);
            }
        });

        // Close subscription
        topicStatsStream.endObject();

        // Remaining dest stats.
        topicStatsHelper.averageMsgSize = topicStatsHelper.aggMsgRateIn == 0.0 ? 0.0
                : (topicStatsHelper.aggMsgThroughputIn / topicStatsHelper.aggMsgRateIn);
        topicStatsStream.writePair("producerCount", producers.size());
        topicStatsStream.writePair("averageMsgSize", topicStatsHelper.averageMsgSize);
        topicStatsStream.writePair("msgRateIn", topicStatsHelper.aggMsgRateIn);
        topicStatsStream.writePair("msgRateOut", topicStatsHelper.aggMsgRateOut);
        topicStatsStream.writePair("msgInCount", getMsgInCounter());
        topicStatsStream.writePair("bytesInCount", getBytesInCounter());
        topicStatsStream.writePair("msgOutCount", getMsgOutCounter());
        topicStatsStream.writePair("bytesOutCount", getBytesOutCounter());
        topicStatsStream.writePair("msgThroughputIn", topicStatsHelper.aggMsgThroughputIn);
        topicStatsStream.writePair("msgThroughputOut", topicStatsHelper.aggMsgThroughputOut);
        topicStatsStream.writePair("storageSize", ledger.getTotalSize());
        topicStatsStream.writePair("backlogSize", ledger.getEstimatedBacklogSize());
        topicStatsStream.writePair("pendingAddEntriesCount", ((ManagedLedgerImpl) ledger).getPendingAddEntriesCount());
        topicStatsStream.writePair("filteredEntriesCount", getFilteredEntriesCount());

        nsStats.msgRateIn += topicStatsHelper.aggMsgRateIn;
        nsStats.msgRateOut += topicStatsHelper.aggMsgRateOut;
        nsStats.msgThroughputIn += topicStatsHelper.aggMsgThroughputIn;
        nsStats.msgThroughputOut += topicStatsHelper.aggMsgThroughputOut;
        nsStats.storageSize += ledger.getEstimatedBacklogSize();

        bundleStats.msgRateIn += topicStatsHelper.aggMsgRateIn;
        bundleStats.msgRateOut += topicStatsHelper.aggMsgRateOut;
        bundleStats.msgThroughputIn += topicStatsHelper.aggMsgThroughputIn;
        bundleStats.msgThroughputOut += topicStatsHelper.aggMsgThroughputOut;
        bundleStats.cacheSize += ((ManagedLedgerImpl) ledger).getCacheSize();

        // Close topic object
        topicStatsStream.endObject();

        // add publish-latency metrics
        this.addEntryLatencyStatsUsec.refresh();
        NamespaceStats.add(this.addEntryLatencyStatsUsec.getBuckets(), nsStats.addLatencyBucket);
        this.addEntryLatencyStatsUsec.reset();
    }

    public double getLastUpdatedAvgPublishRateInMsg() {
        return lastUpdatedAvgPublishRateInMsg;
    }

    public double getLastUpdatedAvgPublishRateInByte() {
        return lastUpdatedAvgPublishRateInByte;
    }

    @Override
    public TopicStatsImpl getStats(boolean getPreciseBacklog, boolean subscriptionBacklogSize,
                                   boolean getEarliestTimeInBacklog) {
        try {
            return asyncGetStats(getPreciseBacklog, subscriptionBacklogSize, getEarliestTimeInBacklog).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("[{}] Fail to get stats", topic, e);
            return null;
        }
    }

    @Override
    public TopicStatsImpl getStats(GetStatsOptions getStatsOptions) {
        try {
            return asyncGetStats(getStatsOptions).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("[{}] Fail to get stats", topic, e);
            return null;
        }
    }

    @Override
    public CompletableFuture<TopicStatsImpl> asyncGetStats(boolean getPreciseBacklog, boolean subscriptionBacklogSize,
                                                           boolean getEarliestTimeInBacklog) {
        GetStatsOptions getStatsOptions = new GetStatsOptions(getPreciseBacklog, subscriptionBacklogSize,
                getEarliestTimeInBacklog, false, false);
        return (CompletableFuture<TopicStatsImpl>) asyncGetStats(getStatsOptions);
    }

    @Override
    public CompletableFuture<? extends TopicStatsImpl> asyncGetStats(GetStatsOptions getStatsOptions) {

        CompletableFuture<TopicStatsImpl> statsFuture = new CompletableFuture<>();
        TopicStatsImpl stats = new TopicStatsImpl();

        ObjectObjectHashMap<String, PublisherStatsImpl> remotePublishersStats = new ObjectObjectHashMap<>();

        producers.values().forEach(producer -> {
            PublisherStatsImpl publisherStats = producer.getStats();
            stats.msgRateIn += publisherStats.msgRateIn;
            stats.msgThroughputIn += publisherStats.msgThroughputIn;

            if (producer.isRemote()) {
                remotePublishersStats.put(producer.getRemoteCluster(), publisherStats);
            }
            if (!getStatsOptions.isExcludePublishers()){
                stats.addPublisher(publisherStats);
            }
        });

        stats.averageMsgSize = stats.msgRateIn == 0.0 ? 0.0 : (stats.msgThroughputIn / stats.msgRateIn);
        stats.msgInCounter = getMsgInCounter();
        stats.bytesInCounter = getBytesInCounter();
        stats.msgChunkPublished = this.msgChunkPublished;
        stats.waitingPublishers = getWaitingProducersCount();
        stats.bytesOutCounter = bytesOutFromRemovedSubscriptions.longValue();
        stats.msgOutCounter = msgOutFromRemovedSubscriptions.longValue();
        stats.publishRateLimitedTimes = publishRateLimitedTimes;
        TransactionBuffer txnBuffer = getTransactionBuffer();
        stats.ongoingTxnCount = txnBuffer.getOngoingTxnCount();
        stats.abortedTxnCount = txnBuffer.getAbortedTxnCount();
        stats.committedTxnCount = txnBuffer.getCommittedTxnCount();

        subscriptions.forEach((name, subscription) -> {
            SubscriptionStatsImpl subStats = subscription.getStats(getStatsOptions);

            stats.msgRateOut += subStats.msgRateOut;
            stats.msgThroughputOut += subStats.msgThroughputOut;
            stats.bytesOutCounter += subStats.bytesOutCounter;
            stats.msgOutCounter += subStats.msgOutCounter;
            stats.subscriptions.put(name, subStats);
            stats.nonContiguousDeletedMessagesRanges += subStats.nonContiguousDeletedMessagesRanges;
            stats.nonContiguousDeletedMessagesRangesSerializedSize +=
                    subStats.nonContiguousDeletedMessagesRangesSerializedSize;
            stats.delayedMessageIndexSizeInBytes += subStats.delayedMessageIndexSizeInBytes;

            subStats.bucketDelayedIndexStats.forEach((k, v) -> {
                TopicMetricBean topicMetricBean =
                        stats.bucketDelayedIndexStats.computeIfAbsent(k, __ -> new TopicMetricBean());
                topicMetricBean.name = v.name;
                topicMetricBean.labelsAndValues = v.labelsAndValues;
                topicMetricBean.value += v.value;
            });
        });

        replicators.forEach((cluster, replicator) -> {
            ReplicatorStatsImpl replicatorStats = replicator.getStats();

            // Add incoming msg rates
            PublisherStatsImpl pubStats = remotePublishersStats.get(replicator.getRemoteCluster());
            if (pubStats != null) {
                replicatorStats.msgRateIn = pubStats.msgRateIn;
                replicatorStats.msgThroughputIn = pubStats.msgThroughputIn;
                replicatorStats.inboundConnection = pubStats.getAddress();
                replicatorStats.inboundConnectedSince = pubStats.getConnectedSince();
            }

            stats.msgRateOut += replicatorStats.msgRateOut;
            stats.msgThroughputOut += replicatorStats.msgThroughputOut;

            stats.replication.put(replicator.getRemoteCluster(), replicatorStats);
        });

        stats.storageSize = ledger.getTotalSize();
        stats.backlogSize = ledger.getEstimatedBacklogSize();
        stats.deduplicationStatus = messageDeduplication.getStatus().toString();
        stats.topicEpoch = topicEpoch.orElse(null);
        stats.ownerBroker = brokerService.pulsar().getBrokerId();
        stats.offloadedStorageSize = ledger.getOffloadedSize();
        stats.lastOffloadLedgerId = ledger.getLastOffloadedLedgerId();
        stats.lastOffloadSuccessTimeStamp = ledger.getLastOffloadedSuccessTimestamp();
        stats.lastOffloadFailureTimeStamp = ledger.getLastOffloadedFailureTimestamp();
        Optional<CompactorMXBean> mxBean = getCompactorMXBean();

        stats.backlogQuotaLimitSize = getBacklogQuota(BacklogQuotaType.destination_storage).getLimitSize();
        stats.backlogQuotaLimitTime = getBacklogQuota(BacklogQuotaType.message_age).getLimitTime();

        TimeBasedBacklogQuotaCheckResult backlogQuotaCheckResult = timeBasedBacklogQuotaCheckResult;
        stats.oldestBacklogMessageAgeSeconds = (backlogQuotaCheckResult == null)
            ? (long) -1
                : TimeUnit.MILLISECONDS.toSeconds(
                Clock.systemUTC().millis() - backlogQuotaCheckResult.getPositionPublishTimestampInMillis());

        stats.oldestBacklogMessageSubscriptionName = (backlogQuotaCheckResult == null)
            ? null
            : backlogQuotaCheckResult.getCursorName();

        stats.compaction.reset();
        mxBean.flatMap(bean -> bean.getCompactionRecordForTopic(topic)).map(compactionRecord -> {
            stats.compaction.lastCompactionRemovedEventCount = compactionRecord.getLastCompactionRemovedEventCount();
            stats.compaction.lastCompactionSucceedTimestamp = compactionRecord.getLastCompactionSucceedTimestamp();
            stats.compaction.lastCompactionFailedTimestamp = compactionRecord.getLastCompactionFailedTimestamp();
            stats.compaction.lastCompactionDurationTimeInMills =
                    compactionRecord.getLastCompactionDurationTimeInMills();
            return compactionRecord;
        });

        if (getStatsOptions.isGetEarliestTimeInBacklog() && stats.backlogSize != 0) {
            ledger.getEarliestMessagePublishTimeInBacklog().whenComplete((earliestTime, e) -> {
                if (e != null) {
                    log.error("[{}] Failed to get earliest message publish time in backlog", topic, e);
                    statsFuture.completeExceptionally(e);
                } else {
                    stats.earliestMsgPublishTimeInBacklogs = earliestTime;
                    statsFuture.complete(stats);
                }
            });
        } else {
            statsFuture.complete(stats);
        }

        return statsFuture;
    }

    private Optional<CompactorMXBean> getCompactorMXBean() {
        Compactor compactor = brokerService.pulsar().getNullableCompactor();
        return Optional.ofNullable(compactor).map(c -> c.getStats());
    }

    @Override
    public CompletableFuture<SchemaVersion> deleteSchema() {
        if (TopicName.get(getName()).isPartitioned()) {
            // Only delete schema when partitioned metadata is deleting.
            return CompletableFuture.completedFuture(null);
        }
        return brokerService.deleteSchema(TopicName.get(getName()));
    }

    @Override
    public CompletableFuture<PersistentTopicInternalStats> getInternalStats(boolean includeLedgerMetadata) {

        CompletableFuture<PersistentTopicInternalStats> statFuture = new CompletableFuture<>();
        PersistentTopicInternalStats stats = new PersistentTopicInternalStats();

        ManagedLedgerImpl ml = (ManagedLedgerImpl) ledger;
        stats.entriesAddedCounter = ml.getEntriesAddedCounter();
        stats.numberOfEntries = ml.getNumberOfEntries();
        stats.totalSize = ml.getTotalSize();
        stats.currentLedgerEntries = ml.getCurrentLedgerEntries();
        stats.currentLedgerSize = ml.getCurrentLedgerSize();
        stats.lastLedgerCreatedTimestamp = DateFormatter.format(ml.getLastLedgerCreatedTimestamp());
        if (ml.getLastLedgerCreationFailureTimestamp() != 0) {
            stats.lastLedgerCreationFailureTimestamp = DateFormatter.format(ml.getLastLedgerCreationFailureTimestamp());
        }

        stats.waitingCursorsCount = ml.getWaitingCursorsCount();
        stats.pendingAddEntriesCount = ml.getPendingAddEntriesCount();

        stats.lastConfirmedEntry = ml.getLastConfirmedEntry().toString();
        stats.state = ml.getState().toString();

        stats.ledgers = new ArrayList<>();
        Set<CompletableFuture<?>> futures = Sets.newConcurrentHashSet();
        CompletableFuture<Set<String>> availableBookiesFuture =
                brokerService.pulsar().getPulsarResources().getBookieResources().listAvailableBookiesAsync();
        futures.add(
            availableBookiesFuture
                .whenComplete((bookies, e) -> {
                    if (e != null) {
                        log.error("[{}] Failed to fetch available bookies.", topic, e);
                        statFuture.completeExceptionally(e);
                    } else {
                        ml.getLedgersInfo().forEach((id, li) -> {
                            LedgerInfo info = new LedgerInfo();
                            info.ledgerId = li.getLedgerId();
                            info.entries = li.getEntries();
                            info.size = li.getSize();
                            info.offloaded = li.hasOffloadContext() && li.getOffloadContext().getComplete();
                            info.bookkeeperDeleted =
                                    li.hasOffloadContext() && li.getOffloadContext().getBookkeeperDeleted();
                            stats.ledgers.add(info);
                            if (includeLedgerMetadata) {
                                futures.add(ml.getLedgerMetadata(li.getLedgerId()).handle((lMetadata, ex) -> {
                                    if (ex == null) {
                                        info.metadata = lMetadata;
                                    }
                                    return null;
                                }));
                                futures.add(ml.getEnsemblesAsync(li.getLedgerId()).handle((ensembles, ex) -> {
                                    if (ex == null) {
                                        info.underReplicated =
                                            !bookies.containsAll(ensembles.stream().map(BookieId::toString)
                                                .collect(Collectors.toList()));
                                    }
                                    return null;
                                }));
                            }
                        });
                    }
                })
        );

        // Add ledger info for compacted topic ledger if exist.
        LedgerInfo info = new LedgerInfo();
        info.ledgerId = -1;
        info.entries = -1;
        info.size = -1;

        Optional<CompactedTopicContext> compactedTopicContext = getCompactedTopicContext();
        if (compactedTopicContext.isPresent()) {
            CompactedTopicContext ledgerContext = compactedTopicContext.get();
            info.ledgerId = ledgerContext.getLedger().getId();
            info.entries = ledgerContext.getLedger().getLastAddConfirmed() + 1;
            info.size = ledgerContext.getLedger().getLength();
        }

        stats.compactedLedger = info;

        stats.cursors = new HashMap<>();
        ml.getCursors().forEach(c -> {
            ManagedCursorImpl cursor = (ManagedCursorImpl) c;
            CursorStats cs = new CursorStats();
            cs.markDeletePosition = cursor.getMarkDeletedPosition().toString();
            cs.readPosition = cursor.getReadPosition().toString();
            cs.waitingReadOp = cursor.hasPendingReadRequest();
            cs.pendingReadOps = cursor.getPendingReadOpsCount();
            cs.messagesConsumedCounter = cursor.getMessagesConsumedCounter();
            cs.cursorLedger = cursor.getCursorLedger();
            cs.cursorLedgerLastEntry = cursor.getCursorLedgerLastEntry();
            cs.individuallyDeletedMessages = cursor.getIndividuallyDeletedMessages();
            cs.lastLedgerSwitchTimestamp = DateFormatter.format(cursor.getLastLedgerSwitchTimestamp());
            cs.state = cursor.getState();
            cs.active = cursor.isActive();
            cs.numberOfEntriesSinceFirstNotAckedMessage = cursor.getNumberOfEntriesSinceFirstNotAckedMessage();
            cs.totalNonContiguousDeletedMessagesRange = cursor.getTotalNonContiguousDeletedMessagesRange();
            cs.properties = cursor.getProperties();
            // subscription metrics
            PersistentSubscription sub = subscriptions.get(Codec.decode(c.getName()));
            if (sub != null) {
                if (sub.getDispatcher() instanceof PersistentDispatcherMultipleConsumers) {
                    PersistentDispatcherMultipleConsumers dispatcher = (PersistentDispatcherMultipleConsumers) sub
                            .getDispatcher();
                    cs.subscriptionHavePendingRead = dispatcher.havePendingRead;
                    cs.subscriptionHavePendingReplayRead = dispatcher.havePendingReplayRead;
                } else if (sub.getDispatcher() instanceof PersistentDispatcherSingleActiveConsumer) {
                    PersistentDispatcherSingleActiveConsumer dispatcher = (PersistentDispatcherSingleActiveConsumer) sub
                            .getDispatcher();
                    cs.subscriptionHavePendingRead = dispatcher.havePendingRead;
                }
            }
            stats.cursors.put(cursor.getName(), cs);
        });

        //Schema store ledgers
        String schemaId;
        try {
            schemaId = TopicName.get(topic).getSchemaName();
        } catch (Throwable t) {
            statFuture.completeExceptionally(t);
            return statFuture;
        }


        CompletableFuture<Void> schemaStoreLedgersFuture = new CompletableFuture<>();
        stats.schemaLedgers = Collections.synchronizedList(new ArrayList<>());
        if (brokerService.getPulsar().getSchemaStorage() != null
                && brokerService.getPulsar().getSchemaStorage() instanceof BookkeeperSchemaStorage) {
            ((BookkeeperSchemaStorage) brokerService.getPulsar().getSchemaStorage())
                    .getStoreLedgerIdsBySchemaId(schemaId)
                    .thenAccept(ledgers -> {
                        List<CompletableFuture<Void>> getLedgerMetadataFutures = new ArrayList<>();
                        ledgers.forEach(ledgerId -> {
                            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                            getLedgerMetadataFutures.add(completableFuture);
                            CompletableFuture<LedgerMetadata> metadataFuture = null;
                            try {
                                metadataFuture = brokerService.getPulsar().getBookKeeperClient()
                                    .getLedgerMetadata(ledgerId);
                            } catch (NullPointerException e) {
                                // related to bookkeeper issue https://github.com/apache/bookkeeper/issues/2741
                                if (log.isDebugEnabled()) {
                                    log.debug("{{}} Failed to get ledger metadata for the schema ledger {}",
                                            topic, ledgerId, e);
                                }
                            }
                            if (metadataFuture != null) {
                                metadataFuture.thenAccept(metadata -> {
                                    LedgerInfo schemaLedgerInfo = new LedgerInfo();
                                    schemaLedgerInfo.ledgerId = metadata.getLedgerId();
                                    schemaLedgerInfo.entries = metadata.getLastEntryId() + 1;
                                    schemaLedgerInfo.size = metadata.getLength();
                                    if (includeLedgerMetadata) {
                                        info.metadata = metadata.toSafeString();
                                    }
                                    stats.schemaLedgers.add(schemaLedgerInfo);
                                    completableFuture.complete(null);
                                }).exceptionally(e -> {
                                    log.error("[{}] Failed to get ledger metadata for the schema ledger {}",
                                            topic, ledgerId, e);
                                    completableFuture.completeExceptionally(e);
                                    return null;
                                });
                            } else {
                                completableFuture.complete(null);
                            }
                        });
                        FutureUtil.waitForAll(getLedgerMetadataFutures).thenRun(() -> {
                            schemaStoreLedgersFuture.complete(null);
                        }).exceptionally(e -> {
                            schemaStoreLedgersFuture.completeExceptionally(e);
                            return null;
                        });
                    }).exceptionally(e -> {
                schemaStoreLedgersFuture.completeExceptionally(e);
                return null;
            });
        } else {
            schemaStoreLedgersFuture.complete(null);
        }
        schemaStoreLedgersFuture.thenRun(() ->
            FutureUtil.waitForAll(futures).handle((res, ex) -> {
                statFuture.complete(stats);
                return null;
            })).exceptionally(e -> {
            statFuture.completeExceptionally(e);
            return null;
        });
        return statFuture;
    }

    public Optional<CompactedTopicContext> getCompactedTopicContext() {
        try {
            if (topicCompactionService instanceof PulsarTopicCompactionService pulsarCompactedService) {
                return pulsarCompactedService.getCompactedTopic().getCompactedTopicContext();
            }
        } catch (ExecutionException | InterruptedException e) {
            log.warn("[{}]Fail to get ledger information for compacted topic.", topic);
        }
        return Optional.empty();
    }

    public long getBacklogSize() {
        return ledger.getEstimatedBacklogSize();
    }

    public boolean isActive(InactiveTopicDeleteMode deleteMode) {
        switch (deleteMode) {
            case delete_when_no_subscriptions:
                if (!subscriptions.isEmpty()) {
                    return true;
                }
                break;
            case delete_when_subscriptions_caught_up:
                if (hasBacklogs()) {
                    return true;
                }
                break;
        }
        if (TopicName.get(topic).isGlobal()) {
            // no local producers
            return hasLocalProducers();
        } else {
            return currentUsageCount() != 0;
        }
    }

    private boolean hasBacklogs() {
        return subscriptions.values().stream().anyMatch(sub -> sub.getNumberOfEntriesInBacklog(false) > 0);
    }

    @Override
    public CompletableFuture<Void> checkClusterMigration() {
        if (ExtensibleLoadManagerImpl.isInternalTopic(topic)) {
            return CompletableFuture.completedFuture(null);
        }

        Optional<ClusterUrl> clusterUrl = getMigratedClusterUrl();

        if (!clusterUrl.isPresent()) {
            return CompletableFuture.completedFuture(null);
        }

        if (isReplicated()) {
            if (isReplicationBacklogExist()) {
                if (!ledger.isMigrated()) {
                    log.info("{} applying migration with replication backlog", topic);
                    ledger.asyncMigrate();
                }
                if (log.isDebugEnabled()) {
                    log.debug("{} has replication backlog and applied migration", topic);
                }
                return CompletableFuture.completedFuture(null);
            }
        }

        return initMigration().thenCompose(subCreated -> {
            migrationSubsCreated = true;
            CompletableFuture<?> migrated = !isMigrated() ? ledger.asyncMigrate()
                    : CompletableFuture.completedFuture(null);
            return migrated.thenApply(__ -> {
                subscriptions.forEach((name, sub) -> {
                    if (sub.isSubscriptionMigrated()) {
                        sub.getConsumers().forEach(Consumer::checkAndApplyTopicMigration);
                    }
                });
                return null;
            }).thenCompose(__ -> checkAndDisconnectReplicators())
                    .thenCompose(__ -> checkAndUnsubscribeSubscriptions())
                    .thenCompose(__ -> checkAndDisconnectProducers());
        });
    }

    /**
     * Initialize migration for a topic by creating topic's resources at migration cluster.
     */
    private CompletableFuture<Void> initMigration() {
        if (migrationSubsCreated) {
            return CompletableFuture.completedFuture(null);
        }
        log.info("{} initializing subscription created at migration cluster", topic);
        return getMigratedClusterUrlAsync(getBrokerService().getPulsar(), topic).thenCompose(clusterUrl -> {
            if (!brokerService.getPulsar().getConfig().isClusterMigrationAutoResourceCreation()) {
                return CompletableFuture.completedFuture(null);
            }
            if (!clusterUrl.isPresent()) {
                return FutureUtil
                        .failedFuture(new TopicMigratedException("cluster migration service-url is not configured"));
            }
            ClusterUrl url = clusterUrl.get();
            ClusterData clusterData = ClusterData.builder().serviceUrl(url.getServiceUrl())
                    .serviceUrlTls(url.getServiceUrlTls()).brokerServiceUrl(url.getBrokerServiceUrl())
                    .brokerServiceUrlTls(url.getBrokerServiceUrlTls()).build();
            PulsarAdmin admin = getBrokerService().getClusterPulsarAdmin(MIGRATION_CLUSTER_NAME,
                    Optional.of(clusterData));

            // namespace creation
            final String tenant = TopicName.get(topic).getTenant();
            final NamespaceName ns = TopicName.get(topic).getNamespaceObject();
            List<CompletableFuture<Void>> subResults = new ArrayList<>();

            return brokerService.getPulsar().getPulsarResources().getTenantResources().getTenantAsync(tenant)
                    .thenCompose(tenantInfo -> {
                        if (!tenantInfo.isPresent()) {
                            return CompletableFuture.completedFuture(null);
                        }
                        CompletableFuture<Void> ts = new CompletableFuture<>();
                        admin.tenants().createTenantAsync(tenant, tenantInfo.get()).handle((__, ex) -> {
                            if (ex == null || ex instanceof ConflictException) {
                                log.info("[{}] successfully created tenant {} for migration", topic, tenant);
                                ts.complete(null);
                                return null;
                            }
                            log.warn("[{}] Failed to create tenant {} on migration cluster {}", topic, tenant,
                                    ex.getCause().getMessage());
                            ts.completeExceptionally(ex.getCause());
                            return null;
                        });
                        return ts;
                    }).thenCompose(t -> {
                        return brokerService.getPulsar().getPulsarResources().getNamespaceResources()
                                .getPoliciesAsync(ns).thenCompose(policies -> {
                                    if (!policies.isPresent()) {
                                        return CompletableFuture.completedFuture(null);
                                    }
                                    CompletableFuture<Void> nsFuture = new CompletableFuture<>();
                                    admin.namespaces().createNamespaceAsync(ns.toString(), policies.get())
                                            .handle((__, ex) -> {
                                                if (ex == null || ex instanceof ConflictException) {
                                                    log.info("[{}] successfully created namespace {} for migration",
                                                            topic, ns);
                                                    nsFuture.complete(null);
                                                    return null;
                                                }
                                                log.warn("[{}] Failed to create namespace {} on migration cluster {}",
                                                        topic, ns, ex.getCause().getMessage());
                                                nsFuture.completeExceptionally(ex.getCause());
                                                return null;
                                            });
                                    return nsFuture;
                                }).thenCompose(p -> {
                                    subscriptions.forEach((subName, sub) -> {
                                        CompletableFuture<Void> subResult = new CompletableFuture<>();
                                        subResults.add(subResult);
                                        admin.topics().createSubscriptionAsync(topic, subName, MessageId.earliest)
                                                .handle((__, ex) -> {
                                                    if (ex == null || ex instanceof ConflictException) {
                                                        log.info("[{}] successfully created sub {} for migration",
                                                                topic, subName);
                                                        subResult.complete(null);
                                                        return null;
                                                    }
                                                    log.warn("[{}] Failed to create sub {} on migration cluster, {}",
                                                            topic, subName, ex.getCause().getMessage());
                                                    subResult.completeExceptionally(ex.getCause());
                                                    return null;
                                                });
                                    });
                                    return Futures.waitForAll(subResults);
                                });
                    });
        });
    }

    private CompletableFuture<Void> checkAndUnsubscribeSubscriptions() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        subscriptions.forEach((s, subscription) -> {
            if (subscription.getNumberOfEntriesInBacklog(true) == 0
                    && subscription.getConsumers().isEmpty()) {
                futures.add(subscription.delete());
            }
        });

        return FutureUtil.waitForAll(futures);
    }

    private CompletableFuture<Void> checkAndDisconnectProducers() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        producers.forEach((name, producer) -> {
            futures.add(producer.disconnect());
        });

        return FutureUtil.waitForAll(futures);
    }

    private CompletableFuture<Void> checkAndDisconnectReplicators() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        ConcurrentOpenHashMap<String, Replicator> replicators = getReplicators();
        replicators.forEach((r, replicator) -> {
            if (replicator.getNumberOfEntriesInBacklog() <= 0) {
                futures.add(replicator.disconnect());
            }
        });
        return FutureUtil.waitForAll(futures);
    }

    public boolean shouldProducerMigrate() {
        return !isReplicationBacklogExist() && migrationSubsCreated;
    }

    @Override
    public boolean isReplicationBacklogExist() {
        ConcurrentOpenHashMap<String, Replicator> replicators = getReplicators();
        if (replicators != null) {
            for (Replicator replicator : replicators.values()) {
                if (replicator.getNumberOfEntriesInBacklog() > 0) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void checkGC() {
        if (!isDeleteWhileInactive()) {
            // This topic is not included in GC
            return;
        }
        InactiveTopicDeleteMode deleteMode =
                topicPolicies.getInactiveTopicPolicies().get().getInactiveTopicDeleteMode();
        int maxInactiveDurationInSec = topicPolicies.getInactiveTopicPolicies().get().getMaxInactiveDurationSeconds();
        if (isActive(deleteMode)) {
            lastActive = System.nanoTime();
        } else if (System.nanoTime() - lastActive < SECONDS.toNanos(maxInactiveDurationInSec)) {
            // Gc interval did not expire yet
            return;
        } else if (shouldTopicBeRetained()) {
            // Topic activity is still within the retention period
            return;
        } else {
            CompletableFuture<Void> replCloseFuture = new CompletableFuture<>();

            if (TopicName.get(topic).isGlobal()) {
                // For global namespace, close repl producers first.
                // Once all repl producers are closed, we can delete the topic,
                // provided no remote producers connected to the broker.
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Global topic inactive for {} seconds, closing repl producers.", topic,
                        maxInactiveDurationInSec);
                }
                closeReplProducersIfNoBacklog().thenRun(() -> {
                    if (hasRemoteProducers()) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Global topic has connected remote producers. Not a candidate for GC",
                                    topic);
                        }
                        replCloseFuture
                                .completeExceptionally(new TopicBusyException("Topic has connected remote producers"));
                    } else {
                        log.info("[{}] Global topic inactive for {} seconds, closed repl producers", topic,
                            maxInactiveDurationInSec);
                        replCloseFuture.complete(null);
                    }
                }).exceptionally(e -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Global topic has replication backlog. Not a candidate for GC", topic);
                    }
                    replCloseFuture.completeExceptionally(e.getCause());
                    return null;
                });
            } else {
                replCloseFuture.complete(null);
            }

            replCloseFuture.thenCompose(v -> delete(deleteMode == InactiveTopicDeleteMode.delete_when_no_subscriptions,
                deleteMode == InactiveTopicDeleteMode.delete_when_subscriptions_caught_up, false))
                    .thenCompose((res) -> tryToDeletePartitionedMetadata())
                    .thenRun(() -> log.info("[{}] Topic deleted successfully due to inactivity", topic))
                    .exceptionally(e -> {
                        if (e.getCause() instanceof TopicBusyException) {
                            // topic became active again
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Did not delete busy topic: {}", topic, e.getCause().getMessage());
                            }
                        } else if (e.getCause() instanceof UnsupportedOperationException) {
                            log.info("[{}] Skip to delete partitioned topic: {}", topic, e.getCause().getMessage());
                        } else {
                            log.warn("[{}] Inactive topic deletion failed", topic, e);
                        }
                        return null;
                    });
        }
    }

    private CompletableFuture<Void> tryToDeletePartitionedMetadata() {
        if (TopicName.get(topic).isPartitioned() && !deletePartitionedTopicMetadataWhileInactive()) {
            return CompletableFuture.completedFuture(null);
        }
        TopicName topicName = TopicName.get(TopicName.get(topic).getPartitionedTopicName());
        PartitionedTopicResources partitionedTopicResources = getBrokerService().pulsar().getPulsarResources()
                .getNamespaceResources()
                .getPartitionedTopicResources();
        return partitionedTopicResources.partitionedTopicExistsAsync(topicName)
                .thenCompose(partitionedTopicExist -> {
                    if (!partitionedTopicExist) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return getBrokerService().pulsar().getPulsarResources().getNamespaceResources()
                                .getPartitionedTopicResources().runWithMarkDeleteAsync(topicName, () ->
                            getBrokerService()
                                .fetchPartitionedTopicMetadataAsync(topicName)
                                .thenCompose((metadata -> {
                                    List<CompletableFuture<Boolean>> persistentTopicExists =
                                            new ArrayList<>(metadata.partitions);
                                    for (int i = 0; i < metadata.partitions; i++) {
                                        persistentTopicExists.add(brokerService.getPulsar()
                                                .getPulsarResources().getTopicResources()
                                                .persistentTopicExists(topicName.getPartition(i)));
                                    }
                                    List<CompletableFuture<Boolean>> unmodifiablePersistentTopicExists =
                                            Collections.unmodifiableList(persistentTopicExists);
                                    return FutureUtil.waitForAll(unmodifiablePersistentTopicExists)
                                            .thenCompose(unused -> {
                                                // make sure all sub partitions were deleted after all future complete
                                                Optional<Boolean> anyExistPartition = unmodifiablePersistentTopicExists
                                                        .stream()
                                                        .map(CompletableFuture::join)
                                                        .filter(topicExist -> topicExist)
                                                        .findAny();
                                                if (anyExistPartition.isPresent()) {
                                                    log.info("[{}] Delete topic metadata failed because"
                                                            + " another partition exist.", topicName);
                                                    throw new UnsupportedOperationException(
                                                            String.format("Another partition exists for [%s].",
                                                                    topicName));
                                                } else {
                                                    return partitionedTopicResources
                                                            .deletePartitionedTopicAsync(topicName);
                                                }
                                            });
                                }))
                            );
                    }
                });
    }

    @Override
    public void checkInactiveSubscriptions() {
        TopicName name = TopicName.get(topic);
        try {
            Policies policies = brokerService.pulsar().getPulsarResources().getNamespaceResources()
                    .getPolicies(name.getNamespaceObject())
                    .orElseThrow(() -> new MetadataStoreException.NotFoundException());
            final int defaultExpirationTime = brokerService.pulsar().getConfiguration()
                    .getSubscriptionExpirationTimeMinutes();
            final Integer nsExpirationTime = policies.subscription_expiration_time_minutes;
            final long expirationTimeMillis = TimeUnit.MINUTES
                    .toMillis(nsExpirationTime == null ? defaultExpirationTime : nsExpirationTime);
            if (expirationTimeMillis > 0) {
                subscriptions.forEach((subName, sub) -> {
                    if (sub.dispatcher != null && sub.dispatcher.isConsumerConnected()
                            || sub.isReplicated()
                            || isCompactionSubscription(subName)) {
                        return;
                    }
                    if (System.currentTimeMillis() - sub.cursor.getLastActive() > expirationTimeMillis) {
                        sub.delete().thenAccept(v -> log.info("[{}][{}] The subscription was deleted due to expiration "
                                + "with last active [{}]", topic, subName, sub.cursor.getLastActive()));
                    }
                });
            }
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Error getting policies", topic);
            }
        }
    }

    @Override
    public void checkBackloggedCursors() {
        subscriptions.forEach((subName, subscription) -> {
            checkBackloggedCursor(subscription);
        });
    }

    private void checkBackloggedCursor(PersistentSubscription subscription) {
        // activate caught up cursor which include consumers
        if (!subscription.getConsumers().isEmpty()
                && subscription.getCursor().getNumberOfEntries() < backloggedCursorThresholdEntries) {
            subscription.getCursor().setActive();
        } else {
            subscription.getCursor().setInactive();
        }
    }

    public void checkInactiveLedgers() {
        ledger.checkInactiveLedgerAndRollOver();
    }

    @Override
    public void checkCursorsToCacheEntries() {
        try {
            ledger.checkCursorsToCacheEntries();
        } catch (Exception e) {
            log.warn("Failed to check cursors to cache entries", e);
        }
    }

    @Override
    public void checkDeduplicationSnapshot() {
        messageDeduplication.takeSnapshot();
    }

    /**
     * Check whether the topic should be retained (based on time), even tough there are no producers/consumers and it's
     * marked as inactive.
     */
    private boolean shouldTopicBeRetained() {
        RetentionPolicies retentionPolicies = topicPolicies.getRetentionPolicies().get();
        long retentionTime = TimeUnit.MINUTES.toNanos(retentionPolicies.getRetentionTimeInMinutes());
        // Negative retention time means the topic should be retained indefinitely,
        // because its own data has to be retained
        return retentionTime < 0 || (System.nanoTime() - lastActive) < retentionTime;
    }

    public CompletableFuture<Void> onLocalPoliciesUpdate() {
        return checkPersistencePolicies();
    }

    @Override
    public void updateDispatchRateLimiter() {
        initializeDispatchRateLimiterIfNeeded();
        dispatchRateLimiter.ifPresent(DispatchRateLimiter::updateDispatchRate);
    }

    @Override
    public CompletableFuture<Void> onPoliciesUpdate(@Nonnull Policies data) {
        requireNonNull(data);
        if (log.isDebugEnabled()) {
            log.debug("[{}] isEncryptionRequired changes: {} -> {}", topic, isEncryptionRequired,
                    data.encryption_required);
        }
        if (data.deleted) {
            log.debug("Ignore the update because it has been deleted : {}", data);
            return CompletableFuture.completedFuture(null);
        }

        // Update props.
        // The component "EntryFilters" is update in the method "updateTopicPolicyByNamespacePolicy(data)".
        //   see more detail: https://github.com/apache/pulsar/pull/19364.
        updateTopicPolicyByNamespacePolicy(data);
        checkReplicatedSubscriptionControllerState();
        isEncryptionRequired = data.encryption_required;
        isAllowAutoUpdateSchema = data.is_allow_auto_update_schema;

        // Apply policies for components.
        List<CompletableFuture<Void>> applyPolicyTasks = applyUpdatedTopicPolicies();
        applyPolicyTasks.add(applyUpdatedNamespacePolicies(data));
        return FutureUtil.waitForAll(applyPolicyTasks)
            .thenAccept(__ -> log.info("[{}] namespace-level policies updated successfully", topic))
            .exceptionally(ex -> {
                log.error("[{}] update namespace polices : {} error", this.getName(), data, ex);
                throw FutureUtil.wrapToCompletionException(ex);
            });
    }

    private CompletableFuture<Void> applyUpdatedNamespacePolicies(Policies namespaceLevelPolicies) {
        return FutureUtil.runWithCurrentThread(() -> updateResourceGroupLimiter(namespaceLevelPolicies));
    }

    private List<CompletableFuture<Void>> applyUpdatedTopicPolicies() {
        List<CompletableFuture<Void>> applyPoliciesFutureList = new ArrayList<>();

        // Client permission check.
        subscriptions.forEach((subName, sub) -> {
            sub.getConsumers().forEach(consumer -> applyPoliciesFutureList.add(consumer.checkPermissionsAsync()));
        });
        producers.values().forEach(producer -> applyPoliciesFutureList.add(
                producer.checkPermissionsAsync().thenRun(producer::checkEncryption)));
        // Check message expiry.
        applyPoliciesFutureList.add(FutureUtil.runWithCurrentThread(() -> checkMessageExpiry()));

        // Update rate limiters.
        applyPoliciesFutureList.add(FutureUtil.runWithCurrentThread(() -> updateDispatchRateLimiter()));
        applyPoliciesFutureList.add(FutureUtil.runWithCurrentThread(() -> updateSubscribeRateLimiter()));
        applyPoliciesFutureList.add(FutureUtil.runWithCurrentThread(() -> updatePublishRateLimiter()));

        applyPoliciesFutureList.add(FutureUtil.runWithCurrentThread(() -> updateSubscriptionsDispatcherRateLimiter()));
        applyPoliciesFutureList.add(FutureUtil.runWithCurrentThread(
                () -> replicators.forEach((name, replicator) -> replicator.updateRateLimiter())));
        applyPoliciesFutureList.add(FutureUtil.runWithCurrentThread(
                () -> shadowReplicators.forEach((name, replicator) -> replicator.updateRateLimiter())));

        // Other components.
        applyPoliciesFutureList.add(FutureUtil.runWithCurrentThread(() -> checkReplicationAndRetryOnFailure()));
        applyPoliciesFutureList.add(FutureUtil.runWithCurrentThread(() -> checkDeduplicationStatus()));
        applyPoliciesFutureList.add(FutureUtil.runWithCurrentThread(() -> checkPersistencePolicies()));
        applyPoliciesFutureList.add(FutureUtil.runWithCurrentThread(
                () -> preCreateSubscriptionForCompactionIfNeeded()));
        applyPoliciesFutureList.add(FutureUtil.runWithCurrentThread(
                () -> updateBrokerDispatchPauseOnAckStatePersistentEnabled()));

        return applyPoliciesFutureList;
    }

    /**
     *
     * @return Backlog quota for topic
     */
    @Override
    public BacklogQuota getBacklogQuota(BacklogQuotaType backlogQuotaType) {
        return this.topicPolicies.getBackLogQuotaMap().get(backlogQuotaType).get();
    }

    /**
     *
     * @return quota exceeded status for blocking producer creation
     */
    @Override
    public CompletableFuture<Void> checkBacklogQuotaExceeded(String producerName, BacklogQuotaType backlogQuotaType) {
        BacklogQuota backlogQuota = getBacklogQuota(backlogQuotaType);
        if (backlogQuota != null) {
            BacklogQuota.RetentionPolicy retentionPolicy = backlogQuota.getPolicy();
            if ((retentionPolicy == BacklogQuota.RetentionPolicy.producer_request_hold
                    || retentionPolicy == BacklogQuota.RetentionPolicy.producer_exception)) {
                if (backlogQuotaType == BacklogQuotaType.destination_storage && isSizeBacklogExceeded()) {
                    log.debug("[{}] Size backlog quota exceeded. Cannot create producer [{}]", this.getName(),
                            producerName);
                    return FutureUtil.failedFuture(new TopicBacklogQuotaExceededException(retentionPolicy));
                }
                if (backlogQuotaType == BacklogQuotaType.message_age) {
                    return checkTimeBacklogExceeded().thenCompose(isExceeded -> {
                        if (isExceeded) {
                            log.debug("[{}] Time backlog quota exceeded. Cannot create producer [{}]", this.getName(),
                                    producerName);
                            return FutureUtil.failedFuture(new TopicBacklogQuotaExceededException(retentionPolicy));
                        } else {
                            return CompletableFuture.completedFuture(null);
                        }
                    });
                }
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * @return determine if backlog quota enforcement needs to be done for topic based on size limit
     */
    public boolean isSizeBacklogExceeded() {
        long backlogQuotaLimitInBytes = getBacklogQuota(BacklogQuotaType.destination_storage).getLimitSize();
        if (backlogQuotaLimitInBytes < 0) {
            return false;
        }

        // check if backlog exceeded quota
        long storageSize = getBacklogSize();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Storage size = [{}], backlog quota limit [{}]",
                    getName(), storageSize, backlogQuotaLimitInBytes);
        }

        return (storageSize >= backlogQuotaLimitInBytes);
    }

    @Override
    public long getBestEffortOldestUnacknowledgedMessageAgeSeconds() {
        TimeBasedBacklogQuotaCheckResult result = timeBasedBacklogQuotaCheckResult;
        if (result == null) {
            return -1;
        } else {
            return TimeUnit.MILLISECONDS.toSeconds(
                    Clock.systemUTC().millis() - result.getPositionPublishTimestampInMillis());
        }
    }

    private void updateResultIfNewer(TimeBasedBacklogQuotaCheckResult updatedResult) {
        TIME_BASED_BACKLOG_QUOTA_CHECK_RESULT_UPDATER.updateAndGet(this,
                existingResult -> {
                    if (existingResult == null
                            || ManagedCursorContainer.DataVersion.compareVersions(
                                    updatedResult.getDataVersion(), existingResult.getDataVersion()) > 0) {
                        return updatedResult;
                    } else {
                        return existingResult;
                    }
                });

    }

    /**
     * @return determine if backlog quota enforcement needs to be done for topic based on time limit
     */
    public CompletableFuture<Boolean> checkTimeBacklogExceeded() {
        TopicName topicName = TopicName.get(getName());
        int backlogQuotaLimitInSecond = getBacklogQuota(BacklogQuotaType.message_age).getLimitTime();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Time backlog quota = [{}]. Checking if exceeded.", topicName, backlogQuotaLimitInSecond);
        }

        // If backlog quota by time is not set
        if (backlogQuotaLimitInSecond <= 0) {
            return CompletableFuture.completedFuture(false);
        }

        ManagedCursorContainer managedCursorContainer = (ManagedCursorContainer) ledger.getCursors();
        CursorInfo oldestMarkDeleteCursorInfo = managedCursorContainer.getCursorWithOldestPosition();

        // If we have no durable cursor since `ledger.getCursors()` only managed durable cursors
        if (oldestMarkDeleteCursorInfo == null
                || oldestMarkDeleteCursorInfo.getPosition() == null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] No durable cursor found. Skipping time based backlog quota check."
                        + " Oldest mark-delete cursor info: {}", topicName, oldestMarkDeleteCursorInfo);
            }
            return CompletableFuture.completedFuture(false);
        }

        PositionImpl oldestMarkDeletePosition = oldestMarkDeleteCursorInfo.getPosition();

        TimeBasedBacklogQuotaCheckResult lastCheckResult = timeBasedBacklogQuotaCheckResult;
        if (lastCheckResult != null
            && oldestMarkDeletePosition.compareTo(lastCheckResult.getOldestCursorMarkDeletePosition()) == 0) {

            // Same position, but the cursor causing it has changed?
            if (!lastCheckResult.getCursorName().equals(oldestMarkDeleteCursorInfo.getCursor().getName())) {
                final TimeBasedBacklogQuotaCheckResult updatedResult = new TimeBasedBacklogQuotaCheckResult(
                        lastCheckResult.getOldestCursorMarkDeletePosition(),
                        oldestMarkDeleteCursorInfo.getCursor().getName(),
                        lastCheckResult.getPositionPublishTimestampInMillis(),
                        oldestMarkDeleteCursorInfo.getVersion());

                updateResultIfNewer(updatedResult);
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Time-based backlog quota check. Updating cached result for position {}, "
                        + "since cursor causing it has changed from {} to {}",
                            topicName,
                            oldestMarkDeletePosition,
                            lastCheckResult.getCursorName(),
                            oldestMarkDeleteCursorInfo.getCursor().getName());
                }
            }

            long entryTimestamp = lastCheckResult.getPositionPublishTimestampInMillis();
            boolean expired = MessageImpl.isEntryExpired(backlogQuotaLimitInSecond, entryTimestamp);
            if (log.isDebugEnabled()) {
                log.debug("[{}] Time based backlog quota check. Using cache result for position {}. "
                        + "Entry timestamp: {}, expired: {}",
                        topicName, oldestMarkDeletePosition, entryTimestamp, expired);
            }
            return CompletableFuture.completedFuture(expired);
        }

        if (brokerService.pulsar().getConfiguration().isPreciseTimeBasedBacklogQuotaCheck()) {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            // Check if first unconsumed message(first message after mark delete position)
            // for slowest cursor's has expired.
            PositionImpl position = ((ManagedLedgerImpl) ledger).getNextValidPosition(oldestMarkDeletePosition);
            ((ManagedLedgerImpl) ledger).asyncReadEntry(position,
                    new AsyncCallbacks.ReadEntryCallback() {
                        @Override
                        public void readEntryComplete(Entry entry, Object ctx) {
                            try {
                                long entryTimestamp = Commands.getEntryTimestamp(entry.getDataBuffer());

                                updateResultIfNewer(
                                        new TimeBasedBacklogQuotaCheckResult(
                                            oldestMarkDeleteCursorInfo.getPosition(),
                                            oldestMarkDeleteCursorInfo.getCursor().getName(),
                                            entryTimestamp,
                                            oldestMarkDeleteCursorInfo.getVersion()));

                                boolean expired = MessageImpl.isEntryExpired(backlogQuotaLimitInSecond, entryTimestamp);
                                if (log.isDebugEnabled()) {
                                    log.debug("[{}] Time based backlog quota check. Oldest unacked entry read from BK. "
                                                    + "Oldest entry in cursor {}'s backlog: {}. "
                                                    + "Oldest mark-delete position: {}. "
                                                    + "Quota {}. Last check result position [{}]. "
                                                    + "Expired: {}, entryTimestamp: {}",
                                            topicName,
                                            oldestMarkDeleteCursorInfo.getCursor().getName(),
                                            position,
                                            oldestMarkDeletePosition,
                                            backlogQuotaLimitInSecond,
                                            lastCheckResult.getOldestCursorMarkDeletePosition(),
                                            expired,
                                            entryTimestamp);
                                }
                                future.complete(expired);
                            } catch (Exception e) {
                                log.error("[{}][{}] Error deserializing message for backlog check", topicName, e);
                                future.complete(false);
                            } finally {
                                entry.release();
                            }
                        }

                        @Override
                        public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                            log.error("[{}][{}] Error reading entry for precise time based  backlog check",
                                    topicName, exception);
                            future.complete(false);
                        }
                    }, null);
            return future;
        } else {
            try {
                EstimateTimeBasedBacklogQuotaCheckResult checkResult =
                        estimatedTimeBasedBacklogQuotaCheck(oldestMarkDeletePosition);
                if (checkResult.getEstimatedOldestUnacknowledgedMessageTimestamp() != null) {
                    updateResultIfNewer(
                            new TimeBasedBacklogQuotaCheckResult(
                                oldestMarkDeleteCursorInfo.getPosition(),
                                oldestMarkDeleteCursorInfo.getCursor().getName(),
                                checkResult.getEstimatedOldestUnacknowledgedMessageTimestamp(),
                                oldestMarkDeleteCursorInfo.getVersion()));
                }

                return CompletableFuture.completedFuture(checkResult.isTruncateBacklogToMatchQuota());
            } catch (Exception e) {
                log.error("[{}][{}] Error reading entry for precise time based backlog check", topicName, e);
                return CompletableFuture.completedFuture(false);
            }
        }
    }

    private EstimateTimeBasedBacklogQuotaCheckResult estimatedTimeBasedBacklogQuotaCheck(
            PositionImpl markDeletePosition)
            throws ExecutionException, InterruptedException {
        int backlogQuotaLimitInSecond = getBacklogQuota(BacklogQuotaType.message_age).getLimitTime();
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) ledger;

        // The ledger timestamp is only known when ledger is closed, hence when the mark-delete
        // is at active ledger (open) we can't estimate it.
        if (managedLedger.getLedgersInfo().lastKey().equals(markDeletePosition.getLedgerId())) {
            return new EstimateTimeBasedBacklogQuotaCheckResult(false, null);
        }

        org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo
                markDeletePositionLedgerInfo = ledger.getLedgerInfo(markDeletePosition.getLedgerId()).get();

        org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo positionToCheckLedgerInfo =
                markDeletePositionLedgerInfo;

        // if the mark-delete position is the last entry it means all entries for
        // that ledger are acknowledged
        if (markDeletePosition.getEntryId() == markDeletePositionLedgerInfo.getEntries() - 1) {
            PositionImpl positionToCheck = managedLedger.getNextValidPosition(markDeletePosition);
            positionToCheckLedgerInfo = ledger.getLedgerInfo(positionToCheck.getLedgerId()).get();
        }

        if (positionToCheckLedgerInfo != null
                && positionToCheckLedgerInfo.hasTimestamp()
                && positionToCheckLedgerInfo.getTimestamp() > 0) {
            long estimateMsgAgeMs = managedLedger.getClock().millis() - positionToCheckLedgerInfo.getTimestamp();
            boolean shouldTruncateBacklog = estimateMsgAgeMs > SECONDS.toMillis(backlogQuotaLimitInSecond);
            if (log.isDebugEnabled()) {
                log.debug("Time based backlog quota exceeded, quota {}[ms], age of ledger "
                                + "slowest cursor currently on {}[ms]", backlogQuotaLimitInSecond * 1000,
                        estimateMsgAgeMs);
            }

            return new EstimateTimeBasedBacklogQuotaCheckResult(
                    shouldTruncateBacklog,
                    positionToCheckLedgerInfo.getTimestamp());
        } else {
            return new EstimateTimeBasedBacklogQuotaCheckResult(false, null);
        }
    }

    @Override
    public boolean isReplicated() {
        return !replicators.isEmpty();
    }

    @Override
    public boolean isShadowReplicated() {
        return !shadowReplicators.isEmpty();
    }

    public CompletableFuture<MessageId> terminate() {
        CompletableFuture<MessageId> future = new CompletableFuture<>();
        ledger.asyncTerminate(new TerminateCallback() {
            @Override
            public void terminateComplete(Position lastCommittedPosition, Object ctx) {
                producers.values().forEach(Producer::disconnect);
                subscriptions.forEach((name, sub) -> sub.topicTerminated());

                PositionImpl lastPosition = (PositionImpl) lastCommittedPosition;
                MessageId messageId = new MessageIdImpl(lastPosition.getLedgerId(), lastPosition.getEntryId(), -1);

                log.info("[{}] Topic terminated at {}", getName(), messageId);
                future.complete(messageId);
            }

            @Override
            public void terminateFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);

        return future;
    }

    public boolean isOldestMessageExpired(ManagedCursor cursor, int messageTTLInSeconds) {
        Entry entry = null;
        boolean isOldestMessageExpired = false;
        try {
            entry = cursor.getNthEntry(1, IndividualDeletedEntries.Include);
            if (entry != null) {
                long entryTimestamp = Commands.getEntryTimestamp(entry.getDataBuffer());
                isOldestMessageExpired = MessageImpl.isEntryExpired(
                        (int) (messageTTLInSeconds * MESSAGE_EXPIRY_THRESHOLD), entryTimestamp);
            }
        } catch (Exception e) {
            if (brokerService.pulsar().getConfiguration().isAutoSkipNonRecoverableData()
                    && e instanceof NonRecoverableLedgerException) {
                // NonRecoverableLedgerException means the ledger or entry can't be read anymore.
                // if AutoSkipNonRecoverableData is set to true, just return true here.
                return true;
            } else {
                log.warn("[{}] [{}] Error while getting the oldest message", topic, cursor.toString(), e);
            }
        } finally {
            if (entry != null) {
                entry.release();
            }
        }

        return isOldestMessageExpired;
    }

    /**
     * Clears backlog for all cursors in the topic.
     *
     * @return
     */
    public CompletableFuture<Void> clearBacklog() {
        log.info("[{}] Clearing backlog on all cursors in the topic.", topic);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        List<String> cursors = getSubscriptions().keys();
        cursors.addAll(getReplicators().keys());
        cursors.addAll(getShadowReplicators().keys());
        for (String cursor : cursors) {
            futures.add(clearBacklog(cursor));
        }
        return FutureUtil.waitForAll(futures);
    }

    /**
     * Clears backlog for a given cursor in the topic.
     * <p>
     * Note: For a replication cursor, just provide the remote cluster name
     * </p>
     *
     * @param cursorName
     * @return
     */
    public CompletableFuture<Void> clearBacklog(String cursorName) {
        log.info("[{}] Clearing backlog for cursor {} in the topic.", topic, cursorName);
        PersistentSubscription sub = getSubscription(cursorName);
        if (sub != null) {
            return sub.clearBacklog();
        }

        PersistentReplicator repl = (PersistentReplicator) getPersistentReplicator(cursorName);
        if (repl != null) {
            return repl.clearBacklog();
        }

        repl = (PersistentReplicator) shadowReplicators.get(cursorName);
        if (repl != null) {
            return repl.clearBacklog();
        }

        return FutureUtil.failedFuture(new BrokerServiceException("Cursor not found"));
    }

    @Override
    public Optional<DispatchRateLimiter> getDispatchRateLimiter() {
        return this.dispatchRateLimiter;
    }

    @Override
    public Optional<DispatchRateLimiter> getBrokerDispatchRateLimiter() {
        return Optional.ofNullable(this.brokerService.getBrokerDispatchRateLimiter());
    }

    public Optional<SubscribeRateLimiter> getSubscribeRateLimiter() {
        return this.subscribeRateLimiter;
    }

    public long getLastPublishedSequenceId(String producerName) {
        return messageDeduplication.getLastPublishedSequenceId(producerName);
    }

    @Override
    public Position getLastPosition() {
        return ledger.getLastConfirmedEntry();
    }

    @Override
    public CompletableFuture<MessageId> getLastMessageId() {
        CompletableFuture<MessageId> completableFuture = new CompletableFuture<>();
        PositionImpl position = (PositionImpl) ledger.getLastConfirmedEntry();
        String name = getName();
        int partitionIndex = TopicName.getPartitionIndex(name);
        if (log.isDebugEnabled()) {
            log.debug("getLastMessageId {}, partitionIndex{}, position {}", name, partitionIndex, position);
        }
        if (position.getEntryId() == -1) {
            completableFuture
                    .complete(new MessageIdImpl(position.getLedgerId(), position.getEntryId(), partitionIndex));
            return completableFuture;
        }
        ManagedLedgerImpl ledgerImpl = (ManagedLedgerImpl) ledger;
        if (!ledgerImpl.ledgerExists(position.getLedgerId())) {
            completableFuture
                    .complete(MessageId.earliest);
            return completableFuture;
        }
        ledgerImpl.asyncReadEntry(position, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                try {
                    MessageMetadata metadata = Commands.parseMessageMetadata(entry.getDataBuffer());
                    if (metadata.hasNumMessagesInBatch()) {
                        completableFuture.complete(new BatchMessageIdImpl(position.getLedgerId(), position.getEntryId(),
                                partitionIndex, metadata.getNumMessagesInBatch() - 1));
                    } else {
                        completableFuture
                                .complete(new MessageIdImpl(position.getLedgerId(), position.getEntryId(),
                                        partitionIndex));
                    }
                } finally {
                    entry.release();
                }
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                completableFuture.completeExceptionally(exception);
            }
        }, null);
        return completableFuture;
    }

    public synchronized void triggerCompaction()
            throws PulsarServerException, AlreadyRunningException {
        if (currentCompaction.isDone()) {
            if (!lock.readLock().tryLock()) {
                log.info("[{}] Conflict topic-close, topic-delete, skip triggering compaction", topic);
                return;
            }
            try {
                if (isClosingOrDeleting) {
                    log.info("[{}] Topic is closing or deleting, skip triggering compaction", topic);
                    return;
                }

                if (strategicCompactionMap.containsKey(topic)) {
                    currentCompaction = brokerService.pulsar().getStrategicCompactor()
                            .compact(topic, strategicCompactionMap.get(topic));
                } else {
                    currentCompaction = topicCompactionService.compact().thenApply(x -> null);
                }
            } finally {
                lock.readLock().unlock();
            }
            currentCompaction.whenComplete((ignore, ex) -> {
                if (ex != null) {
                    log.warn("[{}] Compaction failure.", topic, ex);
                }
            });
        } else {
            throw new AlreadyRunningException("Compaction already in progress");
        }
    }

    public synchronized LongRunningProcessStatus compactionStatus() {
        final CompletableFuture<Long> current;
        synchronized (this) {
            current = currentCompaction;
        }
        if (!current.isDone()) {
            return LongRunningProcessStatus.forStatus(LongRunningProcessStatus.Status.RUNNING);
        } else {
            try {
                if (Objects.equals(current.join(), COMPACTION_NEVER_RUN)) {
                    return LongRunningProcessStatus.forStatus(LongRunningProcessStatus.Status.NOT_RUN);
                } else {
                    return LongRunningProcessStatus.forStatus(LongRunningProcessStatus.Status.SUCCESS);
                }
            } catch (CancellationException | CompletionException e) {
                return LongRunningProcessStatus.forError(e.getMessage());
            }
        }
    }

    public synchronized void triggerOffload(MessageIdImpl messageId) throws AlreadyRunningException {
        if (currentOffload.isDone()) {
            CompletableFuture<MessageIdImpl> promise = currentOffload = new CompletableFuture<>();
            log.info("[{}] Starting offload operation at messageId {}", topic, messageId);
            getManagedLedger().asyncOffloadPrefix(
                    PositionImpl.get(messageId.getLedgerId(), messageId.getEntryId()),
                    new OffloadCallback() {
                        @Override
                        public void offloadComplete(Position pos, Object ctx) {
                            PositionImpl impl = (PositionImpl) pos;
                            log.info("[{}] Completed successfully offload operation at messageId {}", topic, messageId);
                            promise.complete(new MessageIdImpl(impl.getLedgerId(), impl.getEntryId(), -1));
                        }

                        @Override
                        public void offloadFailed(ManagedLedgerException exception, Object ctx) {
                            log.warn("[{}] Failed offload operation at messageId {}", topic, messageId, exception);
                            promise.completeExceptionally(exception);
                        }
                    }, null);
        } else {
            throw new AlreadyRunningException("Offload already in progress");
        }
    }

    public synchronized OffloadProcessStatus offloadStatus() {
        if (!currentOffload.isDone()) {
            return OffloadProcessStatus.forStatus(LongRunningProcessStatus.Status.RUNNING);
        } else {
            try {
                if (currentOffload.join() == MessageId.earliest) {
                    return OffloadProcessStatus.forStatus(LongRunningProcessStatus.Status.NOT_RUN);
                } else {
                    return OffloadProcessStatus.forSuccess(currentOffload.join());
                }
            } catch (CancellationException | CompletionException e) {
                log.warn("Failed to offload", e.getCause());
                return OffloadProcessStatus.forError(e.getMessage());
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentTopic.class);

    @Override
    public CompletableFuture<Void> addSchemaIfIdleOrCheckCompatible(SchemaData schema) {
        return hasSchema().thenCompose((hasSchema) -> {
            int numActiveConsumersWithoutAutoSchema = subscriptions.values().stream()
                    .mapToInt(subscription -> subscription.getConsumers().stream()
                            .filter(consumer -> consumer.getSchemaType() != SchemaType.AUTO_CONSUME)
                            .toList().size())
                    .sum();
            if (hasSchema
                    || (userCreatedProducerCount > 0)
                    || (numActiveConsumersWithoutAutoSchema != 0)
                    || (ledger.getTotalSize() != 0)) {
                return checkSchemaCompatibleForConsumer(schema);
            } else {
                return addSchema(schema).thenCompose(schemaVersion ->
                        CompletableFuture.completedFuture(null));
            }
        });
    }

    public synchronized void checkReplicatedSubscriptionControllerState() {
        AtomicBoolean shouldBeEnabled = new AtomicBoolean(false);
        subscriptions.forEach((name, subscription) -> {
            if (subscription.isReplicated()) {
                shouldBeEnabled.set(true);
            }
        });

        if (!shouldBeEnabled.get()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] There are no replicated subscriptions on the topic", topic);
            }
        }

        checkReplicatedSubscriptionControllerState(shouldBeEnabled.get());
    }

    private synchronized void checkReplicatedSubscriptionControllerState(boolean shouldBeEnabled) {
        boolean isCurrentlyEnabled = replicatedSubscriptionsController.isPresent();
        boolean isEnableReplicatedSubscriptions =
                brokerService.pulsar().getConfiguration().isEnableReplicatedSubscriptions();
        boolean replicationEnabled = this.topicPolicies.getReplicationClusters().get().size() > 1;

        if (shouldBeEnabled && !isCurrentlyEnabled && isEnableReplicatedSubscriptions && replicationEnabled) {
            log.info("[{}] Enabling replicated subscriptions controller", topic);
            replicatedSubscriptionsController = Optional.of(new ReplicatedSubscriptionsController(this,
                    brokerService.pulsar().getConfiguration().getClusterName()));
        } else if (isCurrentlyEnabled && !shouldBeEnabled || !isEnableReplicatedSubscriptions
                || !replicationEnabled) {
            log.info("[{}] Disabled replicated subscriptions controller", topic);
            replicatedSubscriptionsController.ifPresent(ReplicatedSubscriptionsController::close);
            replicatedSubscriptionsController = Optional.empty();
        }
    }

    void receivedReplicatedSubscriptionMarker(Position position, int markerType, ByteBuf payload) {
        ReplicatedSubscriptionsController ctrl = replicatedSubscriptionsController.orElse(null);
        if (ctrl == null) {
            // Force to start the replication controller
            checkReplicatedSubscriptionControllerState(true /* shouldBeEnabled */);
            ctrl = replicatedSubscriptionsController.get();
        }

        ctrl.receivedReplicatedSubscriptionMarker(position, markerType, payload);
     }

    public Optional<ReplicatedSubscriptionsController> getReplicatedSubscriptionController() {
        return replicatedSubscriptionsController;
    }

    public TopicCompactionService getTopicCompactionService() {
        return this.topicCompactionService;
    }

    @Override
    public boolean isSystemTopic() {
        return false;
    }

    @Override
    public boolean isPersistent() {
        return true;
    }

    private synchronized void cancelFencedTopicMonitoringTask() {
        ScheduledFuture<?> monitoringTask = this.fencedTopicMonitoringTask;
        if (monitoringTask != null && !monitoringTask.isDone()) {
            monitoringTask.cancel(false);
        }
    }

    private synchronized void fence() {
        isFenced = true;
        ScheduledFuture<?> monitoringTask = this.fencedTopicMonitoringTask;
        if (monitoringTask == null || monitoringTask.isDone()) {
            final int timeout = brokerService.pulsar().getConfiguration().getTopicFencingTimeoutSeconds();
            if (timeout > 0) {
                this.fencedTopicMonitoringTask = brokerService.executor().schedule(this::closeFencedTopicForcefully,
                        timeout, SECONDS);
            }
        }
    }

    private synchronized void unfence() {
        isFenced = false;
        cancelFencedTopicMonitoringTask();
    }

    private void closeFencedTopicForcefully() {
        if (isFenced) {
            final int timeout = brokerService.pulsar().getConfiguration().getTopicFencingTimeoutSeconds();
            if (isClosingOrDeleting) {
                log.warn("[{}] Topic remained fenced for {} seconds and is already closed (pendingWriteOps: {})", topic,
                        timeout, pendingWriteOps.get());
            } else {
                log.error("[{}] Topic remained fenced for {} seconds, so close it (pendingWriteOps: {})", topic,
                        timeout, pendingWriteOps.get());
                close();
            }
        }
    }

    private void fenceTopicToCloseOrDelete() {
        isClosingOrDeleting = true;
        isFenced = true;
    }

    private void unfenceTopicToResume() {
        subscriptions.values().forEach(sub -> sub.resumeAfterFence());
        isFenced = false;
        isClosingOrDeleting = false;
    }

    @Override
    public void publishTxnMessage(TxnID txnID, ByteBuf headersAndPayload, PublishContext publishContext) {
        pendingWriteOps.incrementAndGet();

        if (isFenced) {
            publishContext.completed(new TopicFencedException("fenced"), -1, -1);
            decrementPendingWriteOpsAndCheck();
            return;
        }
        if (isExceedMaximumMessageSize(headersAndPayload.readableBytes(), publishContext)) {
            publishContext.completed(new NotAllowedException("Exceed maximum message size"), -1, -1);
            decrementPendingWriteOpsAndCheck();
            return;
        }
        if (isExceedMaximumDeliveryDelay(headersAndPayload)) {
            publishContext.completed(
                    new NotAllowedException(
                            String.format("Exceeds max allowed delivery delay of %s milliseconds",
                                    getDelayedDeliveryMaxDelayInMillis())), -1, -1);
            decrementPendingWriteOpsAndCheck();
            return;
        }

        MessageDeduplication.MessageDupStatus status =
                messageDeduplication.isDuplicate(publishContext, headersAndPayload);
        switch (status) {
            case NotDup:
                transactionBuffer.appendBufferToTxn(txnID, publishContext.getSequenceId(), headersAndPayload)
                        .thenAccept(position -> {
                            // Message has been successfully persisted
                            messageDeduplication.recordMessagePersisted(publishContext,
                                    (PositionImpl) position);
                            publishContext.setProperty("txn_id", txnID.toString());
                            publishContext.completed(null, ((PositionImpl) position).getLedgerId(),
                                    ((PositionImpl) position).getEntryId());

                            decrementPendingWriteOpsAndCheck();
                        })
                        .exceptionally(throwable -> {
                            throwable = throwable.getCause();
                            if (throwable instanceof NotAllowedException) {
                              publishContext.completed((NotAllowedException) throwable, -1, -1);
                              decrementPendingWriteOpsAndCheck();
                              return null;
                            } else if (!(throwable instanceof ManagedLedgerException)) {
                                throwable = new ManagedLedgerException(throwable);
                            }
                            addFailed((ManagedLedgerException) throwable, publishContext);
                            return null;
                        });
                break;
            case Dup:
                // Immediately acknowledge duplicated message
                publishContext.completed(null, -1, -1);
                decrementPendingWriteOpsAndCheck();
                break;
            default:
                publishContext.completed(new MessageDeduplication.MessageDupUnknownException(), -1, -1);
                decrementPendingWriteOpsAndCheck();

        }

    }

    @Override
    public CompletableFuture<Void> endTxn(TxnID txnID, int txnAction, long lowWaterMark) {
        if (TxnAction.COMMIT_VALUE == txnAction) {
            return transactionBuffer.commitTxn(txnID, lowWaterMark);
        } else if (TxnAction.ABORT_VALUE == txnAction) {
            return transactionBuffer.abortTxn(txnID, lowWaterMark);
        } else {
            return FutureUtil.failedFuture(new NotAllowedException("Unsupported txnAction " + txnAction));
        }
    }

    @Override
    public CompletableFuture<Void> truncate() {
        return ledger.asyncTruncate();
    }

    public long getDelayedDeliveryTickTimeMillis() {
        return topicPolicies.getDelayedDeliveryTickTimeMillis().get();
    }

    public boolean isDelayedDeliveryEnabled() {
        return topicPolicies.getDelayedDeliveryEnabled().get();
    }

    public long getDelayedDeliveryMaxDelayInMillis() {
        return topicPolicies.getDelayedDeliveryMaxDelayInMillis().get();
    }

    public int getMaxUnackedMessagesOnSubscription() {
        return topicPolicies.getMaxUnackedMessagesOnSubscription().get();
    }

    public boolean isDispatcherPauseOnAckStatePersistentEnabled() {
        Boolean b = topicPolicies.getDispatcherPauseOnAckStatePersistentEnabled().get();
        return b == null ? false : b.booleanValue();
    }

    @Override
    public void updateBrokerDispatchPauseOnAckStatePersistentEnabled() {
        super.updateBrokerDispatchPauseOnAckStatePersistentEnabled();
        // Trigger new read if subscriptions has been paused before.
        if (!topicPolicies.getDispatcherPauseOnAckStatePersistentEnabled().get()) {
            getSubscriptions().forEach((sName, subscription) -> {
                if (subscription.getDispatcher() == null) {
                    return;
                }
                subscription.getDispatcher().checkAndResumeIfPaused();
            });
        }
    }

    @Override
    public void onUpdate(TopicPolicies policies) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] update topic policy: {}", topic, policies);
        }
        if (policies == null) {
            return;
        }
        // Update props.
        // The component "EntryFilters" is update in the method "updateTopicPolicy(data)".
        //   see more detail: https://github.com/apache/pulsar/pull/19364.
        updateTopicPolicy(policies);
        shadowTopics = policies.getShadowTopics();
        checkReplicatedSubscriptionControllerState();

        // Apply policies for components(not contains the specified policies which only defined in namespace policies).
        FutureUtil.waitForAll(applyUpdatedTopicPolicies())
            .thenAccept(__ -> log.info("[{}] topic-level policies updated successfully", topic))
            .exceptionally(e -> {
                Throwable t = FutureUtil.unwrapCompletionException(e);
                log.error("[{}] update topic-level policy error: {}", topic, t.getMessage(), t);
                return null;
            });
    }

    private void updateSubscriptionsDispatcherRateLimiter() {
        subscriptions.forEach((subName, sub) -> {
            Dispatcher dispatcher = sub.getDispatcher();
            if (dispatcher != null) {
                dispatcher.updateRateLimiter();
            }
        });
    }

    protected CompletableFuture<Void> initTopicPolicy() {
        if (brokerService.pulsar().getConfig().isSystemTopicAndTopicLevelPoliciesEnabled()) {
            brokerService.getPulsar().getTopicPoliciesService()
                    .registerListener(TopicName.getPartitionedTopicName(topic), this);
            return CompletableFuture.completedFuture(null).thenRunAsync(() -> onUpdate(
                            brokerService.getPulsar().getTopicPoliciesService()
                                    .getTopicPoliciesIfExists(TopicName.getPartitionedTopicName(topic))),
                    brokerService.getTopicOrderedExecutor());
        }
        return CompletableFuture.completedFuture(null);
    }

    @VisibleForTesting
    public MessageDeduplication getMessageDeduplication() {
        return messageDeduplication;
    }

    private boolean checkMaxSubscriptionsPerTopicExceed(String subscriptionName) {
        if (isSystemTopic()) {
            return false;
        }
        //Existing subscriptions are not affected
        if (StringUtils.isNotEmpty(subscriptionName) && getSubscription(subscriptionName) != null) {
            return false;
        }

        Integer maxSubsPerTopic  = topicPolicies.getMaxSubscriptionsPerTopic().get();

        if (maxSubsPerTopic != null && maxSubsPerTopic > 0) {
            return subscriptions != null && subscriptions.size() >= maxSubsPerTopic;
        }

        return false;
    }

    public boolean checkSubscriptionTypesEnable(SubType subType) {
        EnumSet<SubType> subTypesEnabled = topicPolicies.getSubscriptionTypesEnabled().get();
        return subTypesEnabled != null && subTypesEnabled.contains(subType);
    }

    public TransactionBufferStats getTransactionBufferStats(boolean lowWaterMarks) {
        return getTransactionBufferStats(lowWaterMarks, false);
    }

    public TransactionBufferStats getTransactionBufferStats(boolean lowWaterMarks, boolean segmentStats) {
        return this.transactionBuffer.getStats(lowWaterMarks, segmentStats);
    }

    public TransactionPendingAckStats getTransactionPendingAckStats(String subName, boolean lowWaterMarks) {
        return this.subscriptions.get(subName).getTransactionPendingAckStats(lowWaterMarks);
    }

    public PositionImpl getMaxReadPosition() {
        return this.transactionBuffer.getMaxReadPosition();
    }

    public boolean isTxnAborted(TxnID txnID, PositionImpl readPosition) {
        return this.transactionBuffer.isTxnAborted(txnID, readPosition);
    }

    public TransactionInBufferStats getTransactionInBufferStats(TxnID txnID) {
        return this.transactionBuffer.getTransactionInBufferStats(txnID);
    }

    @Override
    protected boolean isTerminated() {
        return ledger.isTerminated();
    }

    @Override
    public boolean isMigrated() {
        return ledger.isMigrated();
    }

    public TransactionInPendingAckStats getTransactionInPendingAckStats(TxnID txnID, String subName) {
        return this.subscriptions.get(subName).getTransactionInPendingAckStats(txnID);
    }

    public CompletableFuture<ManagedLedger> getPendingAckManagedLedger(String subName) {
        PersistentSubscription subscription = subscriptions.get(subName);
        if (subscription == null) {
            return FutureUtil.failedFuture(new SubscriptionNotFoundException((topic
                    + " not found subscription : " + subName)));
        }
        return subscription.getPendingAckManageLedger();
    }

    private CompletableFuture<Void> transactionBufferCleanupAndClose() {
        return transactionBuffer.clearSnapshot().thenCompose(__ -> transactionBuffer.closeAsync());
    }

    public Optional<TopicName> getShadowSourceTopic() {
        return Optional.ofNullable(shadowSourceTopic);
    }

    protected boolean isExceedMaximumDeliveryDelay(ByteBuf headersAndPayload) {
        if (isDelayedDeliveryEnabled()) {
            long maxDeliveryDelayInMs = getDelayedDeliveryMaxDelayInMillis();
            if (maxDeliveryDelayInMs > 0) {
                headersAndPayload.markReaderIndex();
                MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
                headersAndPayload.resetReaderIndex();
                return msgMetadata.hasDeliverAtTime()
                        && msgMetadata.getDeliverAtTime() - msgMetadata.getPublishTime() > maxDeliveryDelayInMs;
            }
        }
        return false;
    }
}
