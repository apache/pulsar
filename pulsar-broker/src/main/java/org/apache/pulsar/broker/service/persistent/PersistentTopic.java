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
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;

import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BiFunction;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OffloadCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.TerminateCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.IndividualDeletedEntries;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFencedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerTerminatedException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.service.AbstractTopic;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.BrokerServiceException.AlreadyRunningException;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.NamingException;
import org.apache.pulsar.broker.service.BrokerServiceException.NotAllowedException;
import org.apache.pulsar.broker.service.BrokerServiceException.PersistenceException;
import org.apache.pulsar.broker.service.BrokerServiceException.ServerMetadataException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicClosedException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicFencedException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicTerminatedException;
import org.apache.pulsar.broker.service.BrokerServiceException.UnsupportedVersionException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.PrecisPublishLimiter;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.PublishRateLimiter;
import org.apache.pulsar.broker.service.PublishRateLimiterImpl;
import org.apache.pulsar.broker.service.Replicator;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.StreamingStats;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TopicPolicyListener;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter.Type;
import org.apache.pulsar.broker.stats.ClusterReplicationMetrics;
import org.apache.pulsar.broker.stats.NamespaceStats;
import org.apache.pulsar.broker.stats.ReplicationMetrics;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.OffloadProcessStatus;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats.CursorStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats.LedgerInfo;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.policies.data.ReplicatorStats;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.compaction.CompactedTopic;
import org.apache.pulsar.compaction.CompactedTopicImpl;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.utils.StatsOutputStream;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentTopic extends AbstractTopic implements Topic, AddEntryCallback, TopicPolicyListener<TopicPolicies> {

    // Managed ledger associated with the topic
    protected final ManagedLedger ledger;

    // Subscriptions to this topic
    private final ConcurrentOpenHashMap<String, PersistentSubscription> subscriptions;

    private final ConcurrentOpenHashMap<String, Replicator> replicators;

    protected static final AtomicLongFieldUpdater<PersistentTopic> USAGE_COUNT_UPDATER =
            AtomicLongFieldUpdater.newUpdater(PersistentTopic.class, "usageCount");
    @SuppressWarnings("unused")
    private volatile long usageCount = 0;

    static final String DEDUPLICATION_CURSOR_NAME = "pulsar.dedup";

    private static final double MESSAGE_EXPIRY_THRESHOLD = 1.5;

    private static final long POLICY_UPDATE_FAILURE_RETRY_TIME_SECONDS = 60;

    // topic has every published chunked message since topic is loaded
    public boolean msgChunkPublished;

    private Optional<DispatchRateLimiter> dispatchRateLimiter = Optional.empty();
    private Optional<SubscribeRateLimiter> subscribeRateLimiter = Optional.empty();
    public volatile long delayedDeliveryTickTimeMillis = 1000;
    private final long backloggedCursorThresholdEntries;
    public volatile boolean delayedDeliveryEnabled = false;
    public static final int MESSAGE_RATE_BACKOFF_MS = 1000;

    protected final MessageDeduplication messageDeduplication;

    private static final long COMPACTION_NEVER_RUN = -0xfebecffeL;
    private CompletableFuture<Long> currentCompaction = CompletableFuture.completedFuture(COMPACTION_NEVER_RUN);
    private final CompactedTopic compactedTopic;

    private CompletableFuture<MessageIdImpl> currentOffload = CompletableFuture.completedFuture(
            (MessageIdImpl)MessageId.earliest);

    private volatile Optional<ReplicatedSubscriptionsController> replicatedSubscriptionsController = Optional.empty();

    private static final FastThreadLocal<TopicStatsHelper> threadLocalTopicStats = new FastThreadLocal<TopicStatsHelper>() {
        @Override
        protected TopicStatsHelper initialValue() {
            return new TopicStatsHelper();
        }
    };

    private final AtomicLong pendingWriteOps = new AtomicLong(0);
    private volatile double lastUpdatedAvgPublishRateInMsg = 0;
    private volatile double lastUpdatedAvgPublishRateInByte = 0;

    private volatile int maxUnackedMessagesOnSubscription = -1;
    private volatile boolean isClosingOrDeleting = false;

    private static class TopicStatsHelper {
        public double averageMsgSize;
        public double aggMsgRateIn;
        public double aggMsgThroughputIn;
        public double aggMsgThrottlingFailure;
        public double aggMsgRateOut;
        public double aggMsgThroughputOut;
        public final ObjectObjectHashMap<String, PublisherStats> remotePublishersStats;

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

    public PersistentTopic(String topic, ManagedLedger ledger, BrokerService brokerService) throws NamingException {
        super(topic, brokerService);
        this.ledger = ledger;
        this.subscriptions = new ConcurrentOpenHashMap<>(16, 1);
        this.replicators = new ConcurrentOpenHashMap<>(16, 1);
        USAGE_COUNT_UPDATER.set(this, 0);
        this.delayedDeliveryEnabled = brokerService.pulsar().getConfiguration().isDelayedDeliveryEnabled();
        this.delayedDeliveryTickTimeMillis = brokerService.pulsar().getConfiguration().getDelayedDeliveryTickTimeMillis();
        this.backloggedCursorThresholdEntries = brokerService.pulsar().getConfiguration().getManagedLedgerCursorBackloggedThreshold();

        initializeDispatchRateLimiterIfNeeded(Optional.empty());
        brokerService.getPulsar().getTopicPoliciesService().registerListener(TopicName.get(topic), this);

        this.compactedTopic = new CompactedTopicImpl(brokerService.pulsar().getBookKeeperClient());

        for (ManagedCursor cursor : ledger.getCursors()) {
            if (cursor.getName().startsWith(replicatorPrefix)) {
                String localCluster = brokerService.pulsar().getConfiguration().getClusterName();
                String remoteCluster = PersistentReplicator.getRemoteCluster(cursor.getName());
                boolean isReplicatorStarted = addReplicationCluster(remoteCluster, this, cursor.getName(), localCluster);
                if (!isReplicatorStarted) {
                    throw new NamingException(
                        PersistentTopic.this.getName() + " Failed to start replicator " + remoteCluster);
                }
            } else if (cursor.getName().equals(DEDUPLICATION_CURSOR_NAME)) {
                // This is not a regular subscription, we are going to ignore it for now and let the message dedup logic
                // to take care of it
            } else {
                final String subscriptionName = Codec.decode(cursor.getName());
                subscriptions.put(subscriptionName, createPersistentSubscription(subscriptionName, cursor,
                    PersistentSubscription.isCursorFromReplicatedSubscription(cursor)));
                // subscription-cursor gets activated by default: deactivate as there is no active subscription right
                // now
                subscriptions.get(subscriptionName).deactivateCursor();
            }
        }
        this.messageDeduplication = new MessageDeduplication(brokerService.pulsar(), this, ledger);

        try {
            Policies policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                    .get(AdminResource.path(POLICIES, TopicName.get(topic).getNamespace()))
                    .orElseThrow(() -> new KeeperException.NoNodeException());
            this.isEncryptionRequired = policies.encryption_required;

            setSchemaCompatibilityStrategy(policies);
            isAllowAutoUpdateSchema = policies.is_allow_auto_update_schema;

            schemaValidationEnforced = policies.schema_validation_enforced;
            if (policies.inactive_topic_policies != null) {
                inactiveTopicPolicies = policies.inactive_topic_policies;
            }

            maxUnackedMessagesOnConsumer = unackedMessagesExceededOnConsumer(policies);
            maxUnackedMessagesOnSubscription = unackedMessagesExceededOnSubscription(policies);
        } catch (Exception e) {
            log.warn("[{}] Error getting policies {} and isEncryptionRequired will be set to false", topic, e.getMessage());
            isEncryptionRequired = false;
        }

        checkReplicatedSubscriptionControllerState();
    }
    // for testing purposes
    @VisibleForTesting
    PersistentTopic(String topic, BrokerService brokerService, ManagedLedger ledger, MessageDeduplication messageDeduplication) {
        super(topic, brokerService);
        this.ledger = ledger;
        this.messageDeduplication = messageDeduplication;
        this.subscriptions = new ConcurrentOpenHashMap<>(16, 1);
        this.replicators = new ConcurrentOpenHashMap<>(16, 1);
        this.compactedTopic = new CompactedTopicImpl(brokerService.pulsar().getBookKeeperClient());
        this.backloggedCursorThresholdEntries = brokerService.pulsar().getConfiguration().getManagedLedgerCursorBackloggedThreshold();
    }

    private void initializeDispatchRateLimiterIfNeeded(Optional<Policies> policies) {
        synchronized (dispatchRateLimiter) {
            // dispatch rate limiter for topic
            if (!dispatchRateLimiter.isPresent() && DispatchRateLimiter
                    .isDispatchRateNeeded(brokerService, policies, topic, Type.TOPIC)) {
                this.dispatchRateLimiter = Optional.of(new DispatchRateLimiter(this, Type.TOPIC));
            }
            if (!subscribeRateLimiter.isPresent() && SubscribeRateLimiter
                    .isDispatchRateNeeded(brokerService, policies, topic)) {
                this.subscribeRateLimiter = Optional.of(new SubscribeRateLimiter(this));
            }

            // dispatch rate limiter for each subscription
            subscriptions.forEach((name, subscription) -> {
                Dispatcher dispatcher = subscription.getDispatcher();
                if (dispatcher != null) {
                    dispatcher.initializeDispatchRateLimiterIfNeeded(policies);
                }
            });

            // dispatch rate limiter for each replicator
            replicators.forEach((name, replicator) ->
                replicator.initializeDispatchRateLimiterIfNeeded(policies));
        }
    }

    private PersistentSubscription createPersistentSubscription(String subscriptionName, ManagedCursor cursor,
            boolean replicated) {
        checkNotNull(compactedTopic);
        if (subscriptionName.equals(Compactor.COMPACTION_SUBSCRIPTION)) {
            return new CompactorSubscription(this, compactedTopic, subscriptionName, cursor);
        } else {
            return new PersistentSubscription(this, subscriptionName, cursor, replicated);
        }
    }

    @Override
    public void publishMessage(ByteBuf headersAndPayload, PublishContext publishContext) {
        pendingWriteOps.incrementAndGet();
        if (isFenced) {
            publishContext.completed(new TopicFencedException("fenced"), -1, -1);
            decrementPendingWriteOpsAndCheck();
            return;
        }

        MessageDeduplication.MessageDupStatus status = messageDeduplication.isDuplicate(publishContext, headersAndPayload);
        switch (status) {
            case NotDup:
                ledger.asyncAddEntry(headersAndPayload, this, publishContext);
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

    private void decrementPendingWriteOpsAndCheck() {
        long pending = pendingWriteOps.decrementAndGet();
        if (pending == 0 && isFenced && !isClosingOrDeleting) {
            synchronized (this) {
                if (isFenced && !isClosingOrDeleting) {
                    messageDeduplication.resetHighestSequenceIdPushed();
                    log.info("[{}] Un-fencing topic...", topic);
                    // signal to managed ledger that we are ready to resume by creating a new ledger
                    ledger.readyToCreateNewLedger();

                    isFenced = false;
                }

            }
        }
    }

    @Override
    public void addComplete(Position pos, Object ctx) {
        PublishContext publishContext = (PublishContext) ctx;
        PositionImpl position = (PositionImpl) pos;

        // Message has been successfully persisted
        messageDeduplication.recordMessagePersisted(publishContext, position);
        publishContext.completed(null, position.getLedgerId(), position.getEntryId());

        decrementPendingWriteOpsAndCheck();
    }

    @Override
    public synchronized void addFailed(ManagedLedgerException exception, Object ctx) {
        if (exception instanceof ManagedLedgerFencedException) {
            // If the managed ledger has been fenced, we cannot continue using it. We need to close and reopen
            close();
        } else {

            // fence topic when failed to write a message to BK
            isFenced = true;
            // close all producers
            List<CompletableFuture<Void>> futures = Lists.newArrayList();
            producers.values().forEach(producer -> futures.add(producer.disconnect()));
            FutureUtil.waitForAll(futures).handle((BiFunction<Void, Throwable, Void>) (aVoid, throwable) -> {
                decrementPendingWriteOpsAndCheck();
                return null;
            });

            PublishContext callback = (PublishContext) ctx;

            if (exception instanceof ManagedLedgerAlreadyClosedException) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Failed to persist msg in store: {}", topic, exception.getMessage());
                }

                callback.completed(new TopicClosedException(exception), -1, -1);
                return;

            } else {
                log.warn("[{}] Failed to persist msg in store: {}", topic, exception.getMessage());
            }

            if (exception instanceof ManagedLedgerTerminatedException) {
                // Signal the producer that this topic is no longer available
                callback.completed(new TopicTerminatedException(exception), -1, -1);
            } else {
                // Use generic persistence exception
                callback.completed(new PersistenceException(exception), -1, -1);
            }
        }
    }

    @Override
    public void addProducer(Producer producer) throws BrokerServiceException {
        checkArgument(producer.getTopic() == this);

        lock.readLock().lock();
        try {
            brokerService.checkTopicNsOwnership(getName());

            checkTopicFenced();

            if (ledger.isTerminated()) {
                log.warn("[{}] Attempting to add producer to a terminated topic", topic);
                throw new TopicTerminatedException("Topic was already terminated");
            }

            internalAddProducer(producer);

            USAGE_COUNT_UPDATER.incrementAndGet(this);
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Added producer -- count: {}", topic, producer.getProducerName(), USAGE_COUNT_UPDATER.get(this));
            }
            messageDeduplication.producerAdded(producer.getProducerName());

            // Start replication producers if not already
            startReplProducers();
        } finally {
            lock.readLock().unlock();
        }
    }

    private boolean hasRemoteProducers() {
        AtomicBoolean foundRemote = new AtomicBoolean(false);
        producers.values().forEach(producer -> {
            if (producer.isRemote()) {
                foundRemote.set(true);
            }
        });

        return foundRemote.get();
    }

    public void startReplProducers() {
        // read repl-cluster from policies to avoid restart of replicator which are in process of disconnect and close
        try {
            Policies policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                    .get(AdminResource.path(POLICIES, TopicName.get(topic).getNamespace()))
                    .orElseThrow(() -> new KeeperException.NoNodeException());
            if (policies.replication_clusters != null) {
                Set<String> configuredClusters = Sets.newTreeSet(policies.replication_clusters);
                replicators.forEach((region, replicator) -> {
                    if (configuredClusters.contains(region)) {
                        replicator.startProducer();
                    }
                });
            }
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Error getting policies while starting repl-producers {}", topic, e.getMessage());
            }
            replicators.forEach((region, replicator) -> replicator.startProducer());
        }
    }

    public CompletableFuture<Void> stopReplProducers() {
        List<CompletableFuture<Void>> closeFutures = Lists.newArrayList();
        replicators.forEach((region, replicator) -> closeFutures.add(replicator.disconnect()));
        return FutureUtil.waitForAll(closeFutures);
    }

    private synchronized CompletableFuture<Void> closeReplProducersIfNoBacklog() {
        List<CompletableFuture<Void>> closeFutures = Lists.newArrayList();
        replicators.forEach((region, replicator) -> closeFutures.add(replicator.disconnect(true)));
        return FutureUtil.waitForAll(closeFutures);
    }

    @Override
    public void removeProducer(Producer producer) {
        checkArgument(producer.getTopic() == this);

        if (producers.remove(producer.getProducerName(), producer)) {
            handleProducerRemoved(producer);
        }
    }

    @Override
    protected void handleProducerRemoved(Producer producer) {
        // decrement usage only if this was a valid producer close
        USAGE_COUNT_UPDATER.decrementAndGet(this);
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Removed producer -- count: {}", topic, producer.getProducerName(),
                    USAGE_COUNT_UPDATER.get(this));
        }
        lastActive = System.nanoTime();

        messageDeduplication.producerRemoved(producer.getProducerName());
    }

    @Override
    public CompletableFuture<Consumer> subscribe(final ServerCnx cnx, String subscriptionName, long consumerId,
            SubType subType, int priorityLevel, String consumerName, boolean isDurable, MessageId startMessageId,
            Map<String, String> metadata, boolean readCompacted, InitialPosition initialPosition,
            long startMessageRollbackDurationSec, boolean replicatedSubscriptionState, PulsarApi.KeySharedMeta keySharedMeta) {

        final CompletableFuture<Consumer> future = new CompletableFuture<>();

        try {
            brokerService.checkTopicNsOwnership(getName());
        } catch (Exception e) {
            future.completeExceptionally(e);
            return future;
        }

        if (readCompacted && !(subType == SubType.Failover || subType == SubType.Exclusive)) {
            future.completeExceptionally(
                    new NotAllowedException("readCompacted only allowed on failover or exclusive subscriptions"));
            return future;
        }

        if (subType == SubType.Key_Shared
            && !brokerService.pulsar().getConfiguration().isSubscriptionKeySharedEnable()) {
            future.completeExceptionally(
                new NotAllowedException("Key_Shared subscription is disabled by broker.")
            );
            return future;
        }
        if (isBlank(subscriptionName)) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Empty subscription name", topic);
            }
            future.completeExceptionally(new NamingException("Empty subscription name"));
            return future;
        }

        if (hasBatchMessagePublished && !cnx.isBatchMessageCompatibleVersion()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Consumer doesn't support batch-message {}", topic, subscriptionName);
            }
            future.completeExceptionally(new UnsupportedVersionException("Consumer doesn't support batch-message"));
            return future;
        }

        if (subscriptionName.startsWith(replicatorPrefix) || subscriptionName.equals(DEDUPLICATION_CURSOR_NAME)) {
            log.warn("[{}] Failed to create subscription for {}", topic, subscriptionName);
            future.completeExceptionally(new NamingException("Subscription with reserved subscription name attempted"));
            return future;
        }

        if (cnx.getRemoteAddress() != null && cnx.getRemoteAddress().toString().contains(":")) {
            SubscribeRateLimiter.ConsumerIdentifier consumer = new SubscribeRateLimiter.ConsumerIdentifier(
                    cnx.getRemoteAddress().toString().split(":")[0], consumerName, consumerId);
            if (subscribeRateLimiter.isPresent() && !subscribeRateLimiter.get().subscribeAvailable(consumer) || !subscribeRateLimiter.get().tryAcquire(consumer)) {
                log.warn("[{}] Failed to create subscription for {} {} limited by {}, available {}",
                        topic, subscriptionName, consumer, subscribeRateLimiter.get().getSubscribeRate(),
                        subscribeRateLimiter.get().getAvailableSubscribeRateLimit(consumer));
                future.completeExceptionally(new NotAllowedException("Subscribe limited by subscribe rate limit per consumer."));
                return future;
            }
        }

        lock.readLock().lock();
        try {
            if (isFenced) {
                log.warn("[{}] Attempting to subscribe to a fenced topic", topic);
                future.completeExceptionally(new TopicFencedException("Topic is temporarily unavailable"));
                return future;
            }
            USAGE_COUNT_UPDATER.incrementAndGet(this);
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] [{}] Added consumer -- count: {}", topic, subscriptionName, consumerName,
                        USAGE_COUNT_UPDATER.get(this));
            }
        } finally {
            lock.readLock().unlock();
        }

        CompletableFuture<? extends Subscription> subscriptionFuture = isDurable ? //
                getDurableSubscription(subscriptionName, initialPosition, startMessageRollbackDurationSec, replicatedSubscriptionState) //
                : getNonDurableSubscription(subscriptionName, startMessageId, initialPosition, startMessageRollbackDurationSec);

        int maxUnackedMessages = isDurable
                ? getMaxUnackedMessagesOnConsumer()
                : 0;

        subscriptionFuture.thenAccept(subscription -> {
            try {
                Consumer consumer = new Consumer(subscription, subType, topic, consumerId, priorityLevel, consumerName,
                                                 maxUnackedMessages, cnx, cnx.getRole(), metadata, readCompacted, initialPosition, keySharedMeta);
                addConsumerToSubscription(subscription, consumer);

                checkBackloggedCursors();

                if (!cnx.isActive()) {
                    consumer.close();
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] [{}] Subscribe failed -- count: {}", topic, subscriptionName,
                                consumer.consumerName(), USAGE_COUNT_UPDATER.get(PersistentTopic.this));
                    }
                    USAGE_COUNT_UPDATER.decrementAndGet(PersistentTopic.this);
                    future.completeExceptionally(
                            new BrokerServiceException("Connection was closed while the opening the cursor "));
                } else {
                    checkReplicatedSubscriptionControllerState();
                    log.info("[{}][{}] Created new subscription for {}", topic, subscriptionName, consumerId);
                    future.complete(consumer);
                }
            } catch (BrokerServiceException e) {
                if (e instanceof ConsumerBusyException) {
                    log.warn("[{}][{}] Consumer {} {} already connected", topic, subscriptionName, consumerId,
                            consumerName);
                } else if (e instanceof SubscriptionBusyException) {
                    log.warn("[{}][{}] {}", topic, subscriptionName, e.getMessage());
                }

                USAGE_COUNT_UPDATER.decrementAndGet(PersistentTopic.this);
                future.completeExceptionally(e);
            }
        }).exceptionally(ex -> {
            log.error("[{}] Failed to create subscription: {} error: {}", topic, subscriptionName, ex);
            USAGE_COUNT_UPDATER.decrementAndGet(PersistentTopic.this);
            future.completeExceptionally(new PersistenceException(ex));
            return null;
        });

        return future;
    }

    private int unackedMessagesExceededOnSubscription(Policies data) {
        final int maxUnackedMessages = data.max_unacked_messages_per_subscription > -1 ?
                data.max_unacked_messages_per_subscription :
                brokerService.pulsar().getConfiguration().getMaxUnackedMessagesPerSubscription();

        return maxUnackedMessages;
    }

    private int unackedMessagesExceededOnConsumer(Policies data) {
        final int maxUnackedMessages = data.max_unacked_messages_per_consumer > -1 ?
                data.max_unacked_messages_per_consumer :
                brokerService.pulsar().getConfiguration().getMaxUnackedMessagesPerConsumer();

        return maxUnackedMessages;
    }

    private CompletableFuture<Subscription> getDurableSubscription(String subscriptionName,
            InitialPosition initialPosition, long startMessageRollbackDurationSec, boolean replicated) {
        CompletableFuture<Subscription> subscriptionFuture = new CompletableFuture<>();

        Map<String, Long> properties = PersistentSubscription.getBaseCursorProperties(replicated);

        ledger.asyncOpenCursor(Codec.encode(subscriptionName), initialPosition, properties, new OpenCursorCallback() {
            @Override
            public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Opened cursor", topic, subscriptionName);
                }

                PersistentSubscription subscription = subscriptions.computeIfAbsent(subscriptionName,
                        name -> createPersistentSubscription(subscriptionName, cursor, replicated));

                if (replicated && !subscription.isReplicated()) {
                    // Flip the subscription state
                    subscription.setReplicated(replicated);
                }
                subscriptionFuture.complete(subscription);
            }

            @Override
            public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                log.warn("[{}] Failed to create subscription for {}: {}", topic, subscriptionName, exception.getMessage());
                USAGE_COUNT_UPDATER.decrementAndGet(PersistentTopic.this);
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
            MessageId startMessageId, InitialPosition initialPosition, long startMessageRollbackDurationSec) {
        log.info("[{}][{}] Creating non-durable subscription at msg id {}", topic, subscriptionName, startMessageId);

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
                    cursor = ledger.newNonDurableCursor(startPosition, subscriptionName, initialPosition);
                } catch (ManagedLedgerException e) {
                    return FutureUtil.failedFuture(e);
                }

                subscription = new PersistentSubscription(this, subscriptionName, cursor, false);
                subscriptions.put(subscriptionName, subscription);
            }

            if (startMessageRollbackDurationSec > 0) {
                long timestamp = System.currentTimeMillis()
                        - TimeUnit.SECONDS.toMillis(startMessageRollbackDurationSec);
                CompletableFuture<Subscription> subscriptionFuture = new CompletableFuture<>();
                final Subscription finalSubscription = subscription;
                subscription.resetCursor(timestamp).handle((s, ex) -> {
                    if (ex != null) {
                        log.warn("[{}] Failed to reset cursor {} position at timestamp {}", topic, subscriptionName,
                                startMessageRollbackDurationSec);
                    }
                    subscriptionFuture.complete(finalSubscription);
                    return null;
                });
                return subscriptionFuture;
            } else {
                return CompletableFuture.completedFuture(subscription);
            }
        }
    }

    @Override
    public CompletableFuture<Subscription> createSubscription(String subscriptionName, InitialPosition initialPosition, boolean replicateSubscriptionState) {
        return getDurableSubscription(subscriptionName, initialPosition, 0 /*avoid reseting cursor*/, replicateSubscriptionState);
    }

    /**
     * Delete the cursor ledger for a given subscription
     *
     * @param subscriptionName
     *            Subscription for which the cursor ledger is to be deleted
     * @return Completable future indicating completion of unsubscribe operation Completed exceptionally with:
     *         ManagedLedgerException if cursor ledger delete fails
     */
    @Override
    public CompletableFuture<Void> unsubscribe(String subscriptionName) {
        CompletableFuture<Void> unsubscribeFuture = new CompletableFuture<>();

        ledger.asyncDeleteCursor(Codec.encode(subscriptionName), new DeleteCursorCallback() {
            @Override
            public void deleteCursorComplete(Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Cursor deleted successfully", topic, subscriptionName);
                }
                subscriptions.remove(subscriptionName);
                unsubscribeFuture.complete(null);
                lastActive = System.nanoTime();
            }

            @Override
            public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Error deleting cursor for subscription", topic, subscriptionName, exception);
                }
                unsubscribeFuture.completeExceptionally(new PersistenceException(exception));
            }
        }, null);

        return unsubscribeFuture;
    }

    void removeSubscription(String subscriptionName) {
        subscriptions.remove(subscriptionName);
    }

    /**
     * Delete the managed ledger associated with this topic
     *
     * @return Completable future indicating completion of delete operation Completed exceptionally with:
     *         IllegalStateException if topic is still active ManagedLedgerException if ledger delete operation fails
     */
    @Override
    public CompletableFuture<Void> delete() {
        return delete(false, false, false);
    }

    private CompletableFuture<Void> delete(boolean failIfHasSubscriptions, boolean failIfHasBacklogs, boolean deleteSchema) {
        return delete(failIfHasSubscriptions, failIfHasBacklogs, false, deleteSchema);
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
        return delete(false, false, true, false);
    }

    /**
     * Delete the managed ledger associated with this topic
     *
     * @param failIfHasSubscriptions
     *            Flag indicating whether delete should succeed if topic still has unconnected subscriptions. Set to
     *            false when called from admin API (it will delete the subs too), and set to true when called from GC
     *            thread
     * @param closeIfClientsConnected
     *            Flag indicate whether explicitly close connected producers/consumers/replicators before trying to delete topic. If
     *            any client is connected to a topic and if this flag is disable then this operation fails.
     * @param deleteSchema
     *            Flag indicating whether delete the schema defined for topic if exist.
     *
     * @return Completable future indicating completion of delete operation Completed exceptionally with:
     *         IllegalStateException if topic is still active ManagedLedgerException if ledger delete operation fails
     */
    private CompletableFuture<Void> delete(boolean failIfHasSubscriptions,
                                           boolean failIfHasBacklogs,
                                           boolean closeIfClientsConnected,
                                           boolean deleteSchema) {
        CompletableFuture<Void> deleteFuture = new CompletableFuture<>();

        lock.writeLock().lock();
        try {
            if (isClosingOrDeleting) {
                log.warn("[{}] Topic is already being closed or deleted", topic);
                return FutureUtil.failedFuture(new TopicFencedException("Topic is already fenced"));
            } else if (failIfHasSubscriptions && !subscriptions.isEmpty()) {
                return FutureUtil.failedFuture(new TopicBusyException("Topic has subscriptions"));
            } else if (failIfHasBacklogs && hasBacklogs()) {
                return FutureUtil.failedFuture(new TopicBusyException("Topic has subscriptions did not catch up"));
            }

            fenceTopicToCloseOrDelete(); // Avoid clients reconnections while deleting
            CompletableFuture<Void> closeClientFuture = new CompletableFuture<>();
            if (closeIfClientsConnected) {
                List<CompletableFuture<Void>> futures = Lists.newArrayList();
                replicators.forEach((cluster, replicator) -> futures.add(replicator.disconnect()));
                producers.values().forEach(producer -> futures.add(producer.disconnect()));
                subscriptions.forEach((s, sub) -> futures.add(sub.disconnect()));
                FutureUtil.waitForAll(futures).thenRun(() -> {
                    closeClientFuture.complete(null);
                }).exceptionally(ex -> {
                    log.error("[{}] Error closing clients", topic, ex);
                    unfenceTopicToResume();
                    closeClientFuture.completeExceptionally(ex);
                    return null;
                });
            } else {
                closeClientFuture.complete(null);
            }

            closeClientFuture.thenAccept(delete -> {
                // We can proceed with the deletion if either:
                //  1. No one is connected
                //  2. We want to kick out everyone and forcefully delete the topic.
                //     In this case, we shouldn't care if the usageCount is 0 or not, just proceed
                if (USAGE_COUNT_UPDATER.get(this) == 0 || (closeIfClientsConnected && !failIfHasSubscriptions)) {
                    CompletableFuture<SchemaVersion> deleteSchemaFuture = deleteSchema ?
                            deleteSchema()
                            : CompletableFuture.completedFuture(null);

                    deleteSchemaFuture.whenComplete((v, ex) -> {
                        if (ex != null) {
                            log.error("[{}] Error deleting topic", topic, ex);
                            unfenceTopicToResume();
                            deleteFuture.completeExceptionally(ex);
                        } else {
                            ledger.asyncDelete(new AsyncCallbacks.DeleteLedgerCallback() {
                                @Override
                                public void deleteLedgerComplete(Object ctx) {
                                    brokerService.removeTopicFromCache(topic);

                                    dispatchRateLimiter.ifPresent(DispatchRateLimiter::close);

                                    subscribeRateLimiter.ifPresent(SubscribeRateLimiter::close);

                                    brokerService.pulsar().getTopicPoliciesService().unregisterListener(TopicName.get(topic), getPersistentTopic());
                                    log.info("[{}] Topic deleted", topic);
                                    deleteFuture.complete(null);
                                }

                                @Override
                                public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                                    if (exception.getCause() instanceof KeeperException.NoNodeException) {
                                        log.info("[{}] Topic is already deleted {}", topic, exception.getMessage());
                                        deleteLedgerComplete(ctx);
                                    } else {
                                        unfenceTopicToResume();
                                        log.error("[{}] Error deleting topic", topic, exception);
                                        deleteFuture.completeExceptionally(new PersistenceException(exception));
                                    }
                                }
                            }, null);
                        }
                    });
                } else {
                    unfenceTopicToResume();
                    deleteFuture.completeExceptionally(new TopicBusyException(
                            "Topic has " + USAGE_COUNT_UPDATER.get(this) + " connected producers/consumers"));
                }
            }).exceptionally(ex->{
                unfenceTopicToResume();
                deleteFuture.completeExceptionally(
                        new TopicBusyException("Failed to close clients before deleting topic."));
                return null;
            });
        } finally {
            lock.writeLock().unlock();
        }

        return deleteFuture;
    }

    public CompletableFuture<Void> close() {
        return close(false);
    }

    /**
     * Close this topic - close all producers and subscriptions associated with this topic
     *
     * @param closeWithoutWaitingClientDisconnect don't wait for client disconnect and forcefully close managed-ledger
     * @return Completable future indicating completion of close operation
     */
    @Override
    public CompletableFuture<Void> close(boolean closeWithoutWaitingClientDisconnect) {
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        lock.writeLock().lock();
        try {
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

        List<CompletableFuture<Void>> futures = Lists.newArrayList();

        replicators.forEach((cluster, replicator) -> futures.add(replicator.disconnect()));
        producers.values().forEach(producer -> futures.add(producer.disconnect()));
        subscriptions.forEach((s, sub) -> futures.add(sub.disconnect()));

        CompletableFuture<Void> clientCloseFuture = closeWithoutWaitingClientDisconnect ? CompletableFuture.completedFuture(null)
                : FutureUtil.waitForAll(futures);

        clientCloseFuture.thenRun(() -> {
            // After having disconnected all producers/consumers, close the managed ledger
            ledger.asyncClose(new CloseCallback() {
                @Override
                public void closeComplete(Object ctx) {
                    // Everything is now closed, remove the topic from map
                    brokerService.removeTopicFromCache(topic);

                    replicatedSubscriptionsController.ifPresent(ReplicatedSubscriptionsController::close);

                    dispatchRateLimiter.ifPresent(DispatchRateLimiter::close);

                    subscribeRateLimiter.ifPresent(SubscribeRateLimiter::close);

                    brokerService.pulsar().getTopicPoliciesService().unregisterListener(TopicName.get(topic), getPersistentTopic());
                    log.info("[{}] Topic closed", topic);
                    closeFuture.complete(null);
                }

                @Override
                public void closeFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("[{}] Failed to close managed ledger, proceeding anyway.", topic, exception);
                    brokerService.removeTopicFromCache(topic);
                    closeFuture.complete(null);
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

    private CompletableFuture<Void> checkReplicationAndRetryOnFailure() {
        CompletableFuture<Void> result = new CompletableFuture<Void>();
        checkReplication().thenAccept(res -> {
            log.info("[{}] Policies updated successfully", topic);
            result.complete(null);
        }).exceptionally(th -> {
            log.error("[{}] Policies update failed {}, scheduled retry in {} seconds", topic, th.getMessage(),
                    POLICY_UPDATE_FAILURE_RETRY_TIME_SECONDS, th);
            if (!(th.getCause() instanceof TopicFencedException)) {
                // retriable exception
                brokerService.executor().schedule(this::checkReplicationAndRetryOnFailure,
                        POLICY_UPDATE_FAILURE_RETRY_TIME_SECONDS, TimeUnit.SECONDS);
            }
            result.completeExceptionally(th);
            return null;
        });
        return result;
    }

    public CompletableFuture<Void> checkDeduplicationStatus() {
        return messageDeduplication.checkStatus();
    }

    private CompletableFuture<Void> checkPersistencePolicies() {
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
        if (!name.isGlobal()) {
            return CompletableFuture.completedFuture(null);
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Checking replication status", name);
        }

        Policies policies = null;
        try {
            policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                    .get(AdminResource.path(POLICIES, name.getNamespace()))
                    .orElseThrow(() -> new KeeperException.NoNodeException());
        } catch (Exception e) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new ServerMetadataException(e));
            return future;
        }

        //Ignore current broker's config for messageTTL for replication.
        final int newMessageTTLinSeconds = getMessageTTL(getTopicPolicies(name), policies, -1);

        Set<String> configuredClusters;
        if (policies.replication_clusters != null) {
            configuredClusters = Sets.newTreeSet(policies.replication_clusters);
        } else {
            configuredClusters = Collections.emptySet();
        }

        String localCluster = brokerService.pulsar().getConfiguration().getClusterName();

        // if local cluster is removed from global namespace cluster-list : then delete topic forcefully because pulsar
        // doesn't serve global topic without local repl-cluster configured.
        if (TopicName.get(topic).isGlobal() && !configuredClusters.contains(localCluster)) {
            log.info("Deleting topic [{}] because local cluster is not part of global namespace repl list {}",
                    topic, configuredClusters);
            return deleteForcefully();
        }

        List<CompletableFuture<Void>> futures = Lists.newArrayList();

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
            ((PersistentReplicator) replicator).updateMessageTTL(newMessageTTLinSeconds);

            if (!cluster.equals(localCluster)) {
                if (!configuredClusters.contains(cluster)) {
                    futures.add(removeReplicator(cluster));
                }
            }

        });

        return FutureUtil.waitForAll(futures);
    }

    @Override
    public void checkMessageExpiry() {
        TopicName name = TopicName.get(topic);
        Policies policies;
        try {
            policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                    .get(AdminResource.path(POLICIES, name.getNamespace()))
                    .orElseThrow(() -> new KeeperException.NoNodeException());
            int defaultTTL = brokerService.pulsar().getConfiguration().getTtlDurationDefaultInSeconds();
            TopicPolicies topicPolicies = getTopicPolicies(name);
            //If topic level policy or message ttl is not set, fall back to namespace level config.
            int message_ttl_in_seconds = getMessageTTL(topicPolicies, policies, defaultTTL);

            if (message_ttl_in_seconds != 0) {
                subscriptions.forEach((subName, sub) -> sub.expireMessages(message_ttl_in_seconds));
                replicators.forEach((region, replicator) -> ((PersistentReplicator)replicator).expireMessages(message_ttl_in_seconds));
            }
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Error getting policies", topic);
            }
        }
    }

    @Override
    public void checkMessageDeduplicationInfo() {
        messageDeduplication.purgeInactiveProducers();
    }

    public void checkCompaction() {
        TopicName name = TopicName.get(topic);
        try {
            Long compactionThreshold = Optional.ofNullable(getTopicPolicies(name))
                .map(TopicPolicies::getCompactionThreshold)
                .orElse(null);
            if (compactionThreshold == null) {
                Policies policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                    .get(AdminResource.path(POLICIES, name.getNamespace()))
                    .orElseThrow(() -> new KeeperException.NoNodeException());
                compactionThreshold = policies.compaction_threshold;
            }


            if (isSystemTopic() || compactionThreshold != 0
                && currentCompaction.isDone()) {

                long backlogEstimate = 0;

                PersistentSubscription compactionSub = subscriptions.get(Compactor.COMPACTION_SUBSCRIPTION);
                if (compactionSub != null) {
                    backlogEstimate = compactionSub.estimateBacklogSize();
                } else {
                    // compaction has never run, so take full backlog size
                    backlogEstimate = ledger.getEstimatedBacklogSize();
                }

                if (backlogEstimate > compactionThreshold) {
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
            log.debug("[{}] Error getting policies", topic);
        }
    }

    CompletableFuture<Void> startReplicator(String remoteCluster) {
        log.info("[{}] Starting replicator to remote: {}", topic, remoteCluster);
        final CompletableFuture<Void> future = new CompletableFuture<>();

        String replicatorName = PersistentReplicator.getReplicatorName(replicatorPrefix, remoteCluster);
        String localCluster = brokerService.pulsar().getConfiguration().getClusterName();
        boolean isReplicatorStarted = addReplicationCluster(remoteCluster, PersistentTopic.this, replicatorName, localCluster);
        if (isReplicatorStarted) {
            future.complete(null);
        } else {
            future.completeExceptionally(new NamingException(
                PersistentTopic.this.getName() + " Failed to start replicator " + remoteCluster));
        }

        return future;
    }

    protected boolean addReplicationCluster(String remoteCluster, PersistentTopic persistentTopic, String replicatorName,
            String localCluster) {
        AtomicBoolean isReplicatorStarted = new AtomicBoolean(true);
        replicators.computeIfAbsent(remoteCluster, r -> {
            try {
                return new PersistentReplicator(PersistentTopic.this, replicatorName, localCluster, remoteCluster,
                        brokerService, ledger);
            } catch (NamingException e) {
                isReplicatorStarted.set(false);
                log.error("[{}] Replicator startup failed due to partitioned-topic {}", topic, remoteCluster);
            }
            return null;
        });
        // clean up replicator if startup is failed
        if (!isReplicatorStarted.get()) {
            replicators.remove(remoteCluster);
        }
        return isReplicatorStarted.get();
    }

    CompletableFuture<Void> removeReplicator(String remoteCluster) {
        log.info("[{}] Removing replicator to {}", topic, remoteCluster);
        final CompletableFuture<Void> future = new CompletableFuture<>();

        String name = PersistentReplicator.getReplicatorName(replicatorPrefix, remoteCluster);

        replicators.get(remoteCluster).disconnect().thenRun(() -> {

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

    public Replicator getPersistentReplicator(String remoteCluster) {
        return replicators.get(remoteCluster);
    }

    public ManagedLedger getManagedLedger() {
        return ledger;
    }

    @Override
    public void updateRates(NamespaceStats nsStats, NamespaceBundleStats bundleStats, StatsOutputStream topicStatsStream,
            ClusterReplicationMetrics replStats, String namespace, boolean hydratePublishers) {

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
            PublisherStats publisherStats = producer.getStats();

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
                log.warn("[{}] Failed to update cursro state ", topic, e);
            }


            // Update replicator stats
            ReplicatorStats rStat = replicator.getStats();

            // Add incoming msg rates
            PublisherStats pubStats = topicStatsHelper.remotePublishersStats.get(replicator.getRemoteCluster());
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
            // replication delay for a namespace is the max repl-delay among all the topics under this namespace
            if (rStat.replicationDelayInSeconds > nsStats.maxMsgReplDelayInSeconds) {
                nsStats.maxMsgReplDelayInSeconds = rStat.replicationDelayInSeconds;
            }

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

            // Start subscription name & consumers
            try {
                topicStatsStream.startObject(subscriptionName);
                topicStatsStream.startList("consumers");

                for (Consumer consumer : subscription.getConsumers()) {
                    ++nsStats.consumerCount;
                    ++bundleStats.consumerCount;
                    consumer.updateRates();

                    ConsumerStats consumerStats = consumer.getStats();
                    subMsgRateOut += consumerStats.msgRateOut;
                    subMsgThroughputOut += consumerStats.msgThroughputOut;
                    subMsgRateRedeliver += consumerStats.msgRateRedeliver;

                    StreamingStats.writeConsumerStats(topicStatsStream, subscription.getType(), consumerStats);
                }

                // Close Consumer stats
                topicStatsStream.endList();

                // Populate subscription specific stats here
                topicStatsStream.writePair("msgBacklog", subscription.getNumberOfEntriesInBacklog(false));
                topicStatsStream.writePair("msgRateExpired", subscription.getExpiredMessageRate());
                topicStatsStream.writePair("msgRateOut", subMsgRateOut);
                topicStatsStream.writePair("msgThroughputOut", subMsgThroughputOut);
                topicStatsStream.writePair("msgRateRedeliver", subMsgRateRedeliver);
                topicStatsStream.writePair("numberOfEntriesSinceFirstNotAckedMessage", subscription.getNumberOfEntriesSinceFirstNotAckedMessage());
                topicStatsStream.writePair("totalNonContiguousDeletedMessagesRange", subscription.getTotalNonContiguousDeletedMessagesRange());
                topicStatsStream.writePair("type", subscription.getTypeString());
                if (Subscription.isIndividualAckMode(subscription.getType())) {
                    if(subscription.getDispatcher() instanceof PersistentDispatcherMultipleConsumers) {
                        PersistentDispatcherMultipleConsumers dispatcher = (PersistentDispatcherMultipleConsumers)subscription.getDispatcher();
                        topicStatsStream.writePair("blockedSubscriptionOnUnackedMsgs",  dispatcher.isBlockedDispatcherOnUnackedMsgs());
                        topicStatsStream.writePair("unackedMessages", dispatcher.getTotalUnackedMessages());
                    }
                }

                // Close consumers
                topicStatsStream.endObject();

                topicStatsHelper.aggMsgRateOut += subMsgRateOut;
                topicStatsHelper.aggMsgThroughputOut += subMsgThroughputOut;
                nsStats.msgBacklog += subscription.getNumberOfEntriesInBacklog(false);
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
        NamespaceStats.copy(this.addEntryLatencyStatsUsec.getBuckets(), nsStats.addLatencyBucket);
        this.addEntryLatencyStatsUsec.reset();
    }

    public double getLastUpdatedAvgPublishRateInMsg() {
        return lastUpdatedAvgPublishRateInMsg;
    }

    public double getLastUpdatedAvgPublishRateInByte() {
        return lastUpdatedAvgPublishRateInByte;
    }

    @Override
    public TopicStats getStats(boolean getPreciseBacklog) {

        TopicStats stats = new TopicStats();

        ObjectObjectHashMap<String, PublisherStats> remotePublishersStats = new ObjectObjectHashMap<String, PublisherStats>();

        producers.values().forEach(producer -> {
            PublisherStats publisherStats = producer.getStats();
            stats.msgRateIn += publisherStats.msgRateIn;
            stats.msgThroughputIn += publisherStats.msgThroughputIn;

            if (producer.isRemote()) {
                remotePublishersStats.put(producer.getRemoteCluster(), publisherStats);
            } else {
                stats.publishers.add(publisherStats);
            }
        });

        stats.averageMsgSize = stats.msgRateIn == 0.0 ? 0.0 : (stats.msgThroughputIn / stats.msgRateIn);
        stats.msgInCounter = getMsgInCounter();
        stats.bytesInCounter = getBytesInCounter();
        stats.msgChunkPublished = this.msgChunkPublished;

        subscriptions.forEach((name, subscription) -> {
            SubscriptionStats subStats = subscription.getStats(getPreciseBacklog);

            stats.msgRateOut += subStats.msgRateOut;
            stats.msgThroughputOut += subStats.msgThroughputOut;
            stats.bytesOutCounter += subStats.bytesOutCounter;
            stats.msgOutCounter += subStats.msgOutCounter;
            stats.subscriptions.put(name, subStats);
        });

        replicators.forEach((cluster, replicator) -> {
            ReplicatorStats replicatorStats = replicator.getStats();

            // Add incoming msg rates
            PublisherStats pubStats = remotePublishersStats.get(replicator.getRemoteCluster());
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

        return stats;
    }

    @Override
    public PersistentTopicInternalStats getInternalStats() {
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
        stats.state = ml.getState();

        stats.ledgers = Lists.newArrayList();
        ml.getLedgersInfo().forEach((id, li) -> {
            LedgerInfo info = new LedgerInfo();
            info.ledgerId = li.getLedgerId();
            info.entries = li.getEntries();
            info.size = li.getSize();
            info.offloaded = li.hasOffloadContext() && li.getOffloadContext().getComplete();
            stats.ledgers.add(info);
        });

        // Add ledger info for compacted topic ledger if exist.
        LedgerInfo info = new LedgerInfo();
        info.ledgerId = -1;
        info.entries = -1;
        info.size = -1;

        try {
            Optional<CompactedTopicImpl.CompactedTopicContext> compactedTopicContext =
                    ((CompactedTopicImpl)compactedTopic).getCompactedTopicContext();
            if (compactedTopicContext.isPresent()) {
                CompactedTopicImpl.CompactedTopicContext ledgerContext = compactedTopicContext.get();
                info.ledgerId = ledgerContext.getLedger().getId();
                info.entries = ledgerContext.getLedger().getLastAddConfirmed() + 1;
                info.size = ledgerContext.getLedger().getLength();
            }
        } catch (ExecutionException | InterruptedException e) {
            log.warn("[{}]Fail to get ledger information for compacted topic.", topic);
        }
        stats.compactedLedger = info;

        stats.cursors = Maps.newTreeMap();
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
            cs.numberOfEntriesSinceFirstNotAckedMessage = cursor.getNumberOfEntriesSinceFirstNotAckedMessage();
            cs.totalNonContiguousDeletedMessagesRange = cursor.getTotalNonContiguousDeletedMessagesRange();
            cs.properties = cursor.getProperties();
            stats.cursors.put(cursor.getName(), cs);
        });
        return stats;
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
            return USAGE_COUNT_UPDATER.get(this) != 0;
        }
    }

    private boolean hasBacklogs() {
        return subscriptions.values().stream().anyMatch(sub -> sub.getNumberOfEntriesInBacklog(false) > 0);
    }

    @Override
    public void checkGC() {
        if (!isDeleteWhileInactive()) {
            // This topic is not included in GC
            return;
        }
        InactiveTopicDeleteMode deleteMode = inactiveTopicPolicies.getInactiveTopicDeleteMode();
        int maxInactiveDurationInSec = inactiveTopicPolicies.getMaxInactiveDurationSeconds();
        if (isActive(deleteMode)) {
            lastActive = System.nanoTime();
        } else if (System.nanoTime() - lastActive < TimeUnit.SECONDS.toNanos(maxInactiveDurationInSec)) {
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
                deleteMode == InactiveTopicDeleteMode.delete_when_subscriptions_caught_up, true))
                    .thenRun(() -> log.info("[{}] Topic deleted successfully due to inactivity", topic))
                    .exceptionally(e -> {
                        if (e.getCause() instanceof TopicBusyException) {
                            // topic became active again
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Did not delete busy topic: {}", topic, e.getCause().getMessage());
                            }
                        } else {
                            log.warn("[{}] Inactive topic deletion failed", topic, e);
                        }
                        return null;
                    });

        }
    }

    @Override
    public void checkInactiveSubscriptions() {
        TopicName name = TopicName.get(topic);
        try {
            Policies policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                    .get(AdminResource.path(POLICIES, name.getNamespace()))
                    .orElseThrow(() -> new KeeperException.NoNodeException());
            final int defaultExpirationTime = brokerService.pulsar().getConfiguration()
                    .getSubscriptionExpirationTimeMinutes();
            final long expirationTimeMillis = TimeUnit.MINUTES
                    .toMillis((policies.subscription_expiration_time_minutes <= 0 && defaultExpirationTime > 0)
                            ? defaultExpirationTime
                            : policies.subscription_expiration_time_minutes);
            if (expirationTimeMillis > 0) {
                subscriptions.forEach((subName, sub) -> {
                    if (sub.dispatcher != null && sub.dispatcher.isConsumerConnected()) {
                        return;
                    }
                    if (System.currentTimeMillis() - sub.cursor.getLastActive() > expirationTimeMillis) {
                        sub.delete().thenAccept(v -> log.info("[{}][{}] The subscription was deleted due to expiration",
                                topic, subName));
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
        // activate caught up cursors which include consumers
        subscriptions.forEach((subName, subscription) -> {
            if (!subscription.getConsumers().isEmpty()
                && subscription.getCursor().getNumberOfEntries() < backloggedCursorThresholdEntries) {
                subscription.getCursor().setActive();
            } else {
                subscription.getCursor().setInactive();
            }
        });
    }

    /**
     * Check whether the topic should be retained (based on time), even tough there are no producers/consumers and it's
     * marked as inactive.
     */
    private boolean shouldTopicBeRetained() {
        TopicName name = TopicName.get(topic);
        RetentionPolicies retentionPolicies = null;
        try {
            retentionPolicies = Optional.ofNullable(getTopicPolicies(name))
                    .map(TopicPolicies::getRetentionPolicies)
                    .orElse(null);
            if (retentionPolicies == null){
                retentionPolicies = brokerService.pulsar().getConfigurationCache().policiesCache()
                        .get(AdminResource.path(POLICIES, name.getNamespace()))
                        .map(p -> p.retention_policies)
                        .orElse(null);
            }
            if (retentionPolicies == null){
                // If no policies, the default is to have no retention and delete the inactive topic
                retentionPolicies = new RetentionPolicies(
                        brokerService.pulsar().getConfiguration().getDefaultRetentionTimeInMinutes(),
                        brokerService.pulsar().getConfiguration().getDefaultRetentionSizeInMB());
            }
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Error getting policies", topic);
            }
            // Don't delete in case we cannot get the policies
            return true;
        }

        long retentionTime = TimeUnit.MINUTES.toNanos(retentionPolicies.getRetentionTimeInMinutes());
        // Negative retention time means the topic should be retained indefinitely,
        // because its own data has to be retained
        return retentionTime < 0 || (System.nanoTime() - lastActive) < retentionTime;
    }

    @Override
    public CompletableFuture<Void> onPoliciesUpdate(Policies data) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] isEncryptionRequired changes: {} -> {}", topic, isEncryptionRequired, data.encryption_required);
        }
        isEncryptionRequired = data.encryption_required;

        setSchemaCompatibilityStrategy(data);
        isAllowAutoUpdateSchema = data.is_allow_auto_update_schema;

        schemaValidationEnforced = data.schema_validation_enforced;

        maxUnackedMessagesOnConsumer = unackedMessagesExceededOnConsumer(data);
        maxUnackedMessagesOnSubscription = unackedMessagesExceededOnSubscription(data);

        if (data.delayed_delivery_policies != null) {
            delayedDeliveryTickTimeMillis = data.delayed_delivery_policies.getTickTime();
            delayedDeliveryEnabled = data.delayed_delivery_policies.isActive();
        }
        //If the topic-level policy already exists, the namespace-level policy cannot override the topic-level policy.
        TopicPolicies topicPolicies = getTopicPolicies(TopicName.get(topic));
        if (data.inactive_topic_policies != null) {
            if (topicPolicies == null || !topicPolicies.isInactiveTopicPoliciesSet()) {
                this.inactiveTopicPolicies = data.inactive_topic_policies;
            }
        } else {
            ServiceConfiguration cfg = brokerService.getPulsar().getConfiguration();
            resetInactiveTopicPolicies(cfg.getBrokerDeleteInactiveTopicsMode()
                    , cfg.getBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds(), cfg.isBrokerDeleteInactiveTopicsEnabled());
        }

        initializeDispatchRateLimiterIfNeeded(Optional.ofNullable(data));

        this.updateMaxPublishRate(data);

        producers.values().forEach(producer -> {
            producer.checkPermissions();
            producer.checkEncryption();
        });
        subscriptions.forEach((subName, sub) -> {
            sub.getConsumers().forEach(Consumer::checkPermissions);
            Dispatcher dispatcher = sub.getDispatcher();
            if (dispatcher != null) {
                dispatcher.getRateLimiter().ifPresent(rateLimiter -> rateLimiter.onPoliciesUpdate(data));
            }
        });
        replicators.forEach((name, replicator) ->
            replicator.getRateLimiter().ifPresent(rateLimiter -> rateLimiter.onPoliciesUpdate(data))
        );
        checkMessageExpiry();
        CompletableFuture<Void> replicationFuture = checkReplicationAndRetryOnFailure();
        CompletableFuture<Void> dedupFuture = checkDeduplicationStatus();
        CompletableFuture<Void> persistentPoliciesFuture = checkPersistencePolicies();
        // update rate-limiter if policies updated
        if (this.dispatchRateLimiter.isPresent()) {
            if (topicPolicies == null || !topicPolicies.isDispatchRateSet()) {
                dispatchRateLimiter.get().onPoliciesUpdate(data);
            }
        }
        if (this.subscribeRateLimiter.isPresent()) {
            subscribeRateLimiter.get().onPoliciesUpdate(data);
        }
        getManagedLedger().getConfig().setLedgerOffloader(
                brokerService.pulsar().getManagedLedgerOffloader(
                        TopicName.get(topic).getNamespaceObject(), data.offload_policies));

        return CompletableFuture.allOf(replicationFuture, dedupFuture, persistentPoliciesFuture);
    }

    /**
     *
     * @return Backlog quota for topic
     */
    @Override
    public BacklogQuota getBacklogQuota() {
        TopicName topicName = TopicName.get(this.getName());
        return brokerService.getBacklogQuotaManager().getBacklogQuota(topicName);
    }

    /**
     *
     * @return quota exceeded status for blocking producer creation
     */
    @Override
    public boolean isBacklogQuotaExceeded(String producerName) {
        BacklogQuota backlogQuota = getBacklogQuota();

        if (backlogQuota != null) {
            BacklogQuota.RetentionPolicy retentionPolicy = backlogQuota.getPolicy();

            if ((retentionPolicy == BacklogQuota.RetentionPolicy.producer_request_hold
                    || retentionPolicy == BacklogQuota.RetentionPolicy.producer_exception)
                    && isBacklogExceeded()) {
                log.info("[{}] Backlog quota exceeded. Cannot create producer [{}]", this.getName(), producerName);
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    /**
     * @return determine if quota enforcement needs to be done for topic
     */
    public boolean isBacklogExceeded() {
        TopicName topicName = TopicName.get(getName());
        long backlogQuotaLimitInBytes = brokerService.getBacklogQuotaManager().getBacklogQuotaLimit(topicName);
        if (backlogQuotaLimitInBytes < 0) {
            return false;
        }
        if (log.isDebugEnabled()) {
            log.debug("[{}] - backlog quota limit = [{}]", getName(), backlogQuotaLimitInBytes);
        }

        // check if backlog exceeded quota
        long storageSize = getBacklogSize();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Storage size = [{}], limit [{}]", getName(), storageSize, backlogQuotaLimitInBytes);
        }

        return (storageSize >= backlogQuotaLimitInBytes);
    }

    @Override
    public boolean isReplicated() {
        return !replicators.isEmpty();
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

    public boolean isOldestMessageExpired(ManagedCursor cursor, long messageTTLInSeconds) {
        MessageImpl msg = null;
        Entry entry = null;
        boolean isOldestMessageExpired = false;
        try {
            entry = cursor.getNthEntry(1, IndividualDeletedEntries.Include);
            if (entry != null) {
                msg = MessageImpl.deserialize(entry.getDataBuffer());
                isOldestMessageExpired = messageTTLInSeconds != 0 && System.currentTimeMillis() > (msg.getPublishTime()
                        + TimeUnit.SECONDS.toMillis((long) (messageTTLInSeconds * MESSAGE_EXPIRY_THRESHOLD)));
            }
        } catch (Exception e) {
            log.warn("[{}] Error while getting the oldest message", topic, e);
        } finally {
            if (entry != null) {
                entry.release();
            }
            if (msg != null) {
                msg.recycle();
            }
        }

        return isOldestMessageExpired;
    }

    /**
     * Clears backlog for all cursors in the topic
     *
     * @return
     */
    public CompletableFuture<Void> clearBacklog() {
        log.info("[{}] Clearing backlog on all cursors in the topic.", topic);
        List<CompletableFuture<Void>> futures = Lists.newArrayList();
        List<String> cursors = getSubscriptions().keys();
        cursors.addAll(getReplicators().keys());
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

        return FutureUtil.failedFuture(new BrokerServiceException("Cursor not found"));
    }

    @Override
    public Optional<DispatchRateLimiter> getDispatchRateLimiter() {
        return this.dispatchRateLimiter;
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
        int partitionIndex = TopicName.getPartitionIndex(getName());
        if (position.getEntryId() == -1) {
            completableFuture
                    .complete(new MessageIdImpl(position.getLedgerId(), position.getEntryId(), partitionIndex));
        }
        ((ManagedLedgerImpl) ledger).asyncReadEntry(position, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                PulsarApi.MessageMetadata metadata = Commands.parseMessageMetadata(entry.getDataBuffer());
                if (metadata.hasNumMessagesInBatch()) {
                    completableFuture.complete(new BatchMessageIdImpl(position.getLedgerId(), position.getEntryId(),
                            partitionIndex, metadata.getNumMessagesInBatch() - 1));
                } else {
                    completableFuture
                            .complete(new MessageIdImpl(position.getLedgerId(), position.getEntryId(), partitionIndex));
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
            currentCompaction = brokerService.pulsar().getCompactor().compact(topic);
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
                if (current.join() == COMPACTION_NEVER_RUN) {
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
                            PositionImpl impl = (PositionImpl)pos;
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

    /**
     * Get message TTL for this topic.
     * @param topicPolicies TopicPolicies
     * @param policies      NameSpace policy
     * @param brokerDefaultMessageTTL
     * @return Message TTL in second.
     */
    private int getMessageTTL(TopicPolicies topicPolicies, Policies policies, int brokerDefaultMessageTTL) {
        //Return Topic level message TTL if exist. If topic level policy or message ttl is not set,
        //fall back to namespace level message ttl then message ttl set for current broker.
        return (topicPolicies == null || topicPolicies.getMessageTTLInSeconds() == null) ?
                ((policies.message_ttl_in_seconds <= 0 && brokerDefaultMessageTTL > 0) ?
                        brokerDefaultMessageTTL :
                        policies.message_ttl_in_seconds) :
                topicPolicies.getMessageTTLInSeconds();
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentTopic.class);

    @Override
    public CompletableFuture<Void> addSchemaIfIdleOrCheckCompatible(SchemaData schema) {
        return hasSchema()
            .thenCompose((hasSchema) -> {
                    if (hasSchema || isActive(InactiveTopicDeleteMode.delete_when_no_subscriptions) || ledger.getTotalSize() != 0) {
                        return checkSchemaCompatibleForConsumer(schema);
                    } else {
                        return addSchema(schema).thenCompose(schemaVersion ->
                                CompletableFuture.completedFuture(null));
                    }
                });
    }

    private synchronized void checkReplicatedSubscriptionControllerState() {
        AtomicBoolean shouldBeEnabled = new AtomicBoolean(false);
        subscriptions.forEach((name, subscription) -> {
            if (subscription.isReplicated()) {
                shouldBeEnabled.set(true);
            }
        });

        if (shouldBeEnabled.get() == false) {
            log.info("[{}] There are no replicated subscriptions on the topic", topic);
        }

        checkReplicatedSubscriptionControllerState(shouldBeEnabled.get());
    }

    private synchronized void checkReplicatedSubscriptionControllerState(boolean shouldBeEnabled) {
        boolean isCurrentlyEnabled = replicatedSubscriptionsController.isPresent();

        if (shouldBeEnabled && !isCurrentlyEnabled) {
            log.info("[{}] Enabling replicated subscriptions controller", topic);
            replicatedSubscriptionsController = Optional.of(new ReplicatedSubscriptionsController(this,
                    brokerService.pulsar().getConfiguration().getClusterName()));
        } else if (isCurrentlyEnabled && !shouldBeEnabled) {
            log.info("[{}] Disabled replicated subscriptions controller", topic);
            replicatedSubscriptionsController.get().close();
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

    Optional<ReplicatedSubscriptionsController> getReplicatedSubscriptionController() {
        return replicatedSubscriptionsController;
    }

    public CompactedTopic getCompactedTopic() {
        return compactedTopic;
    }

    @Override
    public boolean isSystemTopic() {
        return false;
    }

    private void fenceTopicToCloseOrDelete() {
        isClosingOrDeleting = true;
        isFenced = true;
    }

    private void unfenceTopicToResume() {
        isFenced = false;
        isClosingOrDeleting = false;
    }

    @Override
    public CompletableFuture<TransactionBuffer> getTransactionBuffer(boolean createIfMissing) {
        if (transactionBuffer == null && createIfMissing) {
            transactionBufferLock.lock();
            try {
                if (transactionBuffer == null) {
                    transactionBuffer = brokerService.getPulsar().getTransactionBufferProvider()
                            .newTransactionBuffer(this);
                }
            } finally {
                transactionBufferLock.unlock();
            }
        }
        return transactionBuffer;
    }

    @Override
    public void publishTxnMessage(TxnID txnID, ByteBuf headersAndPayload, long batchSize, PublishContext publishContext) {
        pendingWriteOps.incrementAndGet();
        if (isFenced) {
            publishContext.completed(new TopicFencedException("fenced"), -1, -1);
            decrementPendingWriteOpsAndCheck();
            return;
        }

        MessageDeduplication.MessageDupStatus status = messageDeduplication.isDuplicate(publishContext, headersAndPayload);
        switch (status) {
            case NotDup:
                getTransactionBuffer(true)
                        .thenCompose(txnBuffer -> txnBuffer.appendBufferToTxn(
                                txnID, publishContext.getSequenceId(), batchSize, headersAndPayload))
                        .thenAccept(position -> {
                            decrementPendingWriteOpsAndCheck();
                            publishContext.completed(null,
                                    ((PositionImpl)position).getLedgerId(), ((PositionImpl)position).getEntryId());
                        })
                        .exceptionally(throwable -> {
                            decrementPendingWriteOpsAndCheck();
                            publishContext.completed(new Exception(throwable), -1, -1);
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
    public CompletableFuture<Void> endTxn(TxnID txnID, int txnAction) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        getTransactionBuffer(false).thenAccept(tb -> {
            tb.endTxnOnPartition(txnID, txnAction).whenComplete((ignored, throwable) -> {
                if (throwable != null) {
                    completableFuture.completeExceptionally(throwable);
                    return;
                }
                completableFuture.complete(null);
            });
        }).exceptionally(tbThrowable -> {
            completableFuture.completeExceptionally(tbThrowable);
            return null;
        });
        return completableFuture;
    }

    public long getDelayedDeliveryTickTimeMillis() {
        TopicPolicies topicPolicies = getTopicPolicies(TopicName.get(topic));
        //Topic level setting has higher priority than namespace level
        if (topicPolicies != null && topicPolicies.isDelayedDeliveryTickTimeMillisSet()) {
            return topicPolicies.getDelayedDeliveryTickTimeMillis();
        }
        return delayedDeliveryTickTimeMillis;
    }

    public int getMaxUnackedMessagesOnConsumer() {
        TopicPolicies topicPolicies = getTopicPolicies(TopicName.get(topic));
        if (topicPolicies != null && topicPolicies.isMaxUnackedMessagesOnConsumerSet()) {
            return topicPolicies.getMaxUnackedMessagesOnConsumer();
        }
        return maxUnackedMessagesOnConsumer;
    }

    public boolean isDelayedDeliveryEnabled() {
        TopicPolicies topicPolicies = getTopicPolicies(TopicName.get(topic));
        //Topic level setting has higher priority than namespace level
        if (topicPolicies != null && topicPolicies.isDelayedDeliveryEnabledSet()) {
            return topicPolicies.getDelayedDeliveryEnabled();
        }
        return delayedDeliveryEnabled;
    }

    public int getMaxUnackedMessagesOnSubscription() {
        TopicPolicies topicPolicies = getTopicPolicies(TopicName.get(topic));
        //Topic level setting has higher priority than namespace level
        if (topicPolicies != null && topicPolicies.isMaxUnackedMessagesOnSubscriptionSet()) {
            return topicPolicies.getMaxUnackedMessagesOnSubscription();
        }
        return maxUnackedMessagesOnSubscription;
    }

    @Override
    public void onUpdate(TopicPolicies policies) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] update topic policy: {}", topic, policies);
        }
        if (policies == null) {
            return;
        }
        Optional<Policies> namespacePolicies = getNamespacePolicies();
        initializeTopicDispatchRateLimiterIfNeeded(policies);

        dispatchRateLimiter.ifPresent(limiter -> {
            if (policies.isDispatchRateSet()) {
                dispatchRateLimiter.get().updateDispatchRate(policies.getDispatchRate());
            } else {
                dispatchRateLimiter.get().updateDispatchRate();
            }
        });

        if (policies.getPublishRate() != null) {
            topicPolicyPublishRate = policies.getPublishRate();
            updateTopicPublishDispatcher();
        }

        if (policies.isInactiveTopicPoliciesSet()) {
            inactiveTopicPolicies = policies.getInactiveTopicPolicies();
        } else if (namespacePolicies.isPresent() && namespacePolicies.get().inactive_topic_policies != null) {
            //topic-level policies is null , so use namespace-level
            inactiveTopicPolicies = namespacePolicies.get().inactive_topic_policies;
        } else {
            //namespace-level policies is null , so use broker level
            ServiceConfiguration cfg = brokerService.getPulsar().getConfiguration();
            resetInactiveTopicPolicies(cfg.getBrokerDeleteInactiveTopicsMode()
                    , cfg.getBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds(), cfg.isBrokerDeleteInactiveTopicsEnabled());
        }
    }

    private Optional<Policies> getNamespacePolicies(){
        try {
            return Optional.ofNullable(brokerService.pulsar().getAdminClient().namespaces()
                    .getPolicies(TopicName.get(topic).getNamespace()));
        } catch (Exception e) {
            log.error("get namespace policies fail", e);
        }
        return Optional.empty();
    }

    private void initializeTopicDispatchRateLimiterIfNeeded(TopicPolicies policies) {
        synchronized (dispatchRateLimiter) {
            if (!dispatchRateLimiter.isPresent() && policies.getDispatchRate() != null) {
                this.dispatchRateLimiter = Optional.of(new DispatchRateLimiter(this, Type.TOPIC));
            }
        }
    }

    private PersistentTopic getPersistentTopic() {
        return this;
    }

    @Override
    protected void updateTopicPublishDispatcher() {
        if (topicPolicyPublishRate != null && (topicPolicyPublishRate.publishThrottlingRateInByte > 0
            || topicPolicyPublishRate.publishThrottlingRateInMsg > 0)) {
            log.info("Enabling topic policy publish rate limiting {} on topic {}", topicPolicyPublishRate, this.topic);
            if (!preciseTopicPublishRateLimitingEnable) {
                this.brokerService.setupBrokerPublishRateLimiterMonitor();
            }

            if (this.topicPublishRateLimiter == null
                || this.topicPublishRateLimiter == PublishRateLimiter.DISABLED_RATE_LIMITER) {
                // create new rateLimiter if rate-limiter is disabled
                if (preciseTopicPublishRateLimitingEnable) {
                    this.topicPublishRateLimiter = new PrecisPublishLimiter(topicPolicyPublishRate, ()-> this.enableCnxAutoRead());
                } else {
                    this.topicPublishRateLimiter = new PublishRateLimiterImpl(topicPolicyPublishRate);
                }
            } else {
                this.topicPublishRateLimiter.update(topicPolicyPublishRate);
            }
        } else {
            log.info("Disabling publish throttling for {}", this.topic);
            this.topicPublishRateLimiter = PublishRateLimiter.DISABLED_RATE_LIMITER;
            enableProducerReadForPublishRateLimiting();
        }
    }

    @VisibleForTesting
    public MessageDeduplication getMessageDeduplication() {
        return messageDeduplication;
    }
}
