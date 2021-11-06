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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.pulsar.common.events.EventsTopicNames.checkTopicIsEventsNames;
import static org.apache.pulsar.compaction.Compactor.COMPACTION_SUBSCRIPTION;
import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import lombok.Getter;
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
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorContainer;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.net.BookieId;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
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
import org.apache.pulsar.broker.service.BrokerServiceException.ServerMetadataException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionNotFoundException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicClosedException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicFencedException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicTerminatedException;
import org.apache.pulsar.broker.service.BrokerServiceException.UnsupportedVersionException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.Replicator;
import org.apache.pulsar.broker.service.StreamingStats;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TopicPolicyListener;
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
import org.apache.pulsar.common.events.EventsTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats.CursorStats;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats.LedgerInfo;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.TransactionBufferStats;
import org.apache.pulsar.common.policies.data.TransactionInBufferStats;
import org.apache.pulsar.common.policies.data.TransactionInPendingAckStats;
import org.apache.pulsar.common.policies.data.TransactionPendingAckStats;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.policies.data.stats.PublisherStatsImpl;
import org.apache.pulsar.common.policies.data.stats.ReplicatorStatsImpl;
import org.apache.pulsar.common.policies.data.stats.SubscriptionStatsImpl;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.compaction.CompactedTopic;
import org.apache.pulsar.compaction.CompactedTopicContext;
import org.apache.pulsar.compaction.CompactedTopicImpl;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.compaction.CompactorMXBean;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.utils.StatsOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentTopic extends AbstractTopic
        implements Topic, AddEntryCallback, TopicPolicyListener<TopicPolicies> {

    // Managed ledger associated with the topic
    protected final ManagedLedger ledger;

    // Subscriptions to this topic
    private final ConcurrentOpenHashMap<String, PersistentSubscription> subscriptions;

    private final ConcurrentOpenHashMap<String, Replicator> replicators;

    static final String DEDUPLICATION_CURSOR_NAME = "pulsar.dedup";
    private static final String TOPIC_EPOCH_PROPERTY_NAME = "pulsar.topic.epoch";

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

    private volatile int maxUnackedMessagesOnSubscriptionApplied;
    private volatile boolean isClosingOrDeleting = false;

    private ScheduledFuture<?> fencedTopicMonitoringTask = null;

    @Getter
    protected final TransactionBuffer transactionBuffer;

    private final LongAdder bytesOutFromRemovedSubscriptions = new LongAdder();
    private final LongAdder msgOutFromRemovedSubscriptions = new LongAdder();

    // Record the last time a data message (ie: not an internal Pulsar marker) is published on the topic
    private long lastDataMessagePublishedTimestamp = 0;

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

    public PersistentTopic(String topic, ManagedLedger ledger, BrokerService brokerService) throws NamingException {
        super(topic, brokerService);
        this.ledger = ledger;
        this.subscriptions = new ConcurrentOpenHashMap<>(16, 1);
        this.replicators = new ConcurrentOpenHashMap<>(16, 1);
        this.delayedDeliveryEnabled = brokerService.pulsar().getConfiguration().isDelayedDeliveryEnabled();
        this.delayedDeliveryTickTimeMillis =
                brokerService.pulsar().getConfiguration().getDelayedDeliveryTickTimeMillis();
        this.backloggedCursorThresholdEntries =
                brokerService.pulsar().getConfiguration().getManagedLedgerCursorBackloggedThreshold();
        initializeRateLimiterIfNeeded(Optional.empty());
        registerTopicPolicyListener();

        this.compactedTopic = new CompactedTopicImpl(brokerService.pulsar().getBookKeeperClient());

        for (ManagedCursor cursor : ledger.getCursors()) {
            if (cursor.getName().equals(DEDUPLICATION_CURSOR_NAME)
                    || cursor.getName().startsWith(replicatorPrefix)) {
                // This is not a regular subscription, we are going to
                // ignore it for now and let the message dedup logic to take care of it
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
        if (ledger.getProperties().containsKey(TOPIC_EPOCH_PROPERTY_NAME)) {
            topicEpoch = Optional.of(Long.parseLong(ledger.getProperties().get(TOPIC_EPOCH_PROPERTY_NAME)));
        }

        checkReplicatedSubscriptionControllerState();
        TopicName topicName = TopicName.get(topic);
        if (brokerService.getPulsar().getConfiguration().isTransactionCoordinatorEnabled()
                && !checkTopicIsEventsNames(topicName)) {
            this.transactionBuffer = brokerService.getPulsar()
                    .getTransactionBufferProvider().newTransactionBuffer(this);
        } else {
            this.transactionBuffer = new TransactionBufferDisable();
        }
        transactionBuffer.syncMaxReadPositionForNormalPublish((PositionImpl) ledger.getLastConfirmedEntry());
    }

    @Override
    public CompletableFuture<Void> initialize() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
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
                .thenAccept(optPolicies -> {
                    if (!optPolicies.isPresent()) {
                        isEncryptionRequired = false;
                        updateUnackedMessagesAppliedOnSubscription(null);
                        updateUnackedMessagesExceededOnConsumer(null);
                        return;
                    }

                    Policies policies = optPolicies.get();
                    this.isEncryptionRequired = policies.encryption_required;

                    setSchemaCompatibilityStrategy(policies);
                    isAllowAutoUpdateSchema = policies.is_allow_auto_update_schema;

                    schemaValidationEnforced = policies.schema_validation_enforced;
                    if (policies.inactive_topic_policies != null) {
                        inactiveTopicPolicies = policies.inactive_topic_policies;
                    }
                    updateUnackedMessagesAppliedOnSubscription(policies);
                    updateUnackedMessagesExceededOnConsumer(policies);
                }).exceptionally(ex -> {
                    log.warn("[{}] Error getting policies {} and isEncryptionRequired will be set to false",
                            topic, ex.getMessage());
                    isEncryptionRequired = false;
                    updateUnackedMessagesAppliedOnSubscription(null);
                    updateUnackedMessagesExceededOnConsumer(null);
                    return null;
                }));
    }

    // for testing purposes
    @VisibleForTesting
    PersistentTopic(String topic, BrokerService brokerService, ManagedLedger ledger,
                    MessageDeduplication messageDeduplication) {
        super(topic, brokerService);
        this.ledger = ledger;
        this.messageDeduplication = messageDeduplication;
        this.subscriptions = new ConcurrentOpenHashMap<>(16, 1);
        this.replicators = new ConcurrentOpenHashMap<>(16, 1);
        this.compactedTopic = new CompactedTopicImpl(brokerService.pulsar().getBookKeeperClient());
        this.backloggedCursorThresholdEntries =
                brokerService.pulsar().getConfiguration().getManagedLedgerCursorBackloggedThreshold();

        if (brokerService.pulsar().getConfiguration().isTransactionCoordinatorEnabled()) {
            this.transactionBuffer = brokerService.getPulsar()
                    .getTransactionBufferProvider().newTransactionBuffer(this);
        } else {
            this.transactionBuffer = new TransactionBufferDisable();
        }
    }

    private void initializeRateLimiterIfNeeded(Optional<Policies> policies) {
        synchronized (dispatchRateLimiter) {
            // dispatch rate limiter for topic
            if (!dispatchRateLimiter.isPresent() && DispatchRateLimiter
                    .isDispatchRateNeeded(brokerService, policies, topic, Type.TOPIC)) {
                this.dispatchRateLimiter = Optional.of(new DispatchRateLimiter(this, Type.TOPIC));
            }
            boolean isDispatchRateNeeded = SubscribeRateLimiter.isDispatchRateNeeded(brokerService, policies, topic);
            if (!subscribeRateLimiter.isPresent() && isDispatchRateNeeded) {
                this.subscribeRateLimiter = Optional.of(new SubscribeRateLimiter(this));
            } else if (!isDispatchRateNeeded) {
                this.subscribeRateLimiter = Optional.empty();
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
        if (subscriptionName.equals(COMPACTION_SUBSCRIPTION)) {
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
        if (isExceedMaximumMessageSize(headersAndPayload.readableBytes())) {
            publishContext.completed(new NotAllowedException("Exceed maximum message size")
                    , -1, -1);
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

        publishContext.setMetadataFromEntryData(entryData);
        publishContext.completed(null, position.getLedgerId(), position.getEntryId());
        // in order to sync the max position when cursor read entries
        transactionBuffer.syncMaxReadPositionForNormalPublish((PositionImpl) ledger.getLastConfirmedEntry());
        decrementPendingWriteOpsAndCheck();
    }

    @Override
    public synchronized void addFailed(ManagedLedgerException exception, Object ctx) {
        if (exception instanceof ManagedLedgerFencedException) {
            // If the managed ledger has been fenced, we cannot continue using it. We need to close and reopen
            close();
        } else {

            // fence topic when failed to write a message to BK
            fence();
            // close all producers
            CompletableFuture<Void> disconnectProducersFuture;
            if (producers.size() > 0) {
                List<CompletableFuture<Void>> futures = Lists.newArrayList();
                producers.forEach((__, producer) -> futures.add(producer.disconnect()));
                disconnectProducersFuture = FutureUtil.waitForAll(futures);
            } else {
                disconnectProducersFuture = CompletableFuture.completedFuture(null);
            }
            disconnectProducersFuture.handle((BiFunction<Void, Throwable, Void>) (aVoid, throwable) -> {
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
        AtomicBoolean foundRemote = new AtomicBoolean(false);
        producers.values().forEach(producer -> {
            if (producer.isRemote()) {
                foundRemote.set(true);
            }
        });

        return foundRemote.get();
    }

    public CompletableFuture<Void> startReplProducers() {
        // read repl-cluster from policies to avoid restart of replicator which are in process of disconnect and close
        return brokerService.pulsar().getPulsarResources().getNamespaceResources()
                .getPoliciesAsync(TopicName.get(topic).getNamespaceObject())
                .thenAccept(optPolicies -> {
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
                }).exceptionally(ex -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Error getting policies while starting repl-producers {}", topic, ex.getMessage());
            }
            replicators.forEach((region, replicator) -> replicator.startProducer());
            return null;
        });
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
    protected void handleProducerRemoved(Producer producer) {
        super.handleProducerRemoved(producer);
        messageDeduplication.producerRemoved(producer.getProducerName());
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
                if (!topic.endsWith(EventsTopicNames.NAMESPACE_EVENTS_LOCAL_NAME)
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

            if (cnx.clientAddress() != null && cnx.clientAddress().toString().contains(":")) {
                SubscribeRateLimiter.ConsumerIdentifier consumer = new SubscribeRateLimiter.ConsumerIdentifier(
                        cnx.clientAddress().toString().split(":")[0], consumerName, consumerId);
                if (subscribeRateLimiter.isPresent() && (!subscribeRateLimiter.get().subscribeAvailable(consumer)
                        || !subscribeRateLimiter.get().tryAcquire(consumer))) {
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

            CompletableFuture<? extends Subscription> subscriptionFuture = isDurable ? //
                    getDurableSubscription(subscriptionName, initialPosition, startMessageRollbackDurationSec,
                            replicatedSubscriptionState)
                    : getNonDurableSubscription(subscriptionName, startMessageId, initialPosition,
                    startMessageRollbackDurationSec);

            int maxUnackedMessages = isDurable
                    ? getMaxUnackedMessagesOnConsumer()
                    : 0;

            CompletableFuture<Consumer> future = subscriptionFuture.thenCompose(subscription -> {
                Consumer consumer = new Consumer(subscription, subType, topic, consumerId, priorityLevel,
                        consumerName, maxUnackedMessages, cnx, cnx.getAuthRole(), metadata,
                        readCompacted, initialPosition, keySharedMeta, startMessageId);
                return addConsumerToSubscription(subscription, consumer).thenCompose(v -> {
                    checkBackloggedCursors();
                    if (!cnx.isActive()) {
                        try {
                            consumer.close();
                        } catch (BrokerServiceException e) {
                            if (e instanceof ConsumerBusyException) {
                                log.warn("[{}][{}] Consumer {} {} already connected",
                                        topic, subscriptionName, consumerId, consumerName);
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
                        log.info("[{}][{}] Created new subscription for {}", topic, subscriptionName, consumerId);
                        return CompletableFuture.completedFuture(consumer);
                    }
                });
            });

            future.exceptionally(ex -> {
                decrementUsageCount();

                if (ex.getCause() instanceof ConsumerBusyException) {
                    log.warn("[{}][{}] Consumer {} {} already connected", topic, subscriptionName, consumerId,
                            consumerName);
                } else if (ex.getCause() instanceof SubscriptionBusyException) {
                    log.warn("[{}][{}] {}", topic, subscriptionName, ex.getMessage());
                } else {
                    log.error("[{}] Failed to create subscription: {}", topic, subscriptionName, ex);
                }
                return null;
            });
            return future;
        });
    }

    public void updateUnackedMessagesAppliedOnSubscription(Policies policies) {
        maxUnackedMessagesOnSubscriptionApplied = getTopicPolicies()
                .map(TopicPolicies::getMaxUnackedMessagesOnSubscription)
                .orElseGet(() ->
                        policies != null && policies.max_unacked_messages_per_subscription != null
                                ? policies.max_unacked_messages_per_subscription
                                : brokerService.pulsar().getConfiguration().getMaxUnackedMessagesPerSubscription()
                );
    }

    private void updateUnackedMessagesExceededOnConsumer(Policies data) {
        maxUnackedMessagesOnConsumerAppilied = getTopicPolicies()
                .map(TopicPolicies::getMaxUnackedMessagesOnConsumer)
                .orElseGet(() -> data != null && data.max_unacked_messages_per_consumer != null
                        ? data.max_unacked_messages_per_consumer
                        : brokerService.pulsar().getConfiguration().getMaxUnackedMessagesPerConsumer());
        getSubscriptions().forEach((name, sub) -> {
            if (sub != null) {
                sub.getConsumers().forEach(consumer -> {
                    if (consumer.getMaxUnackedMessages() != maxUnackedMessagesOnConsumerAppilied) {
                        consumer.setMaxUnackedMessages(maxUnackedMessagesOnConsumerAppilied);
                    }
                });
            }
        });

    }

    private CompletableFuture<Subscription> getDurableSubscription(String subscriptionName,
            InitialPosition initialPosition, long startMessageRollbackDurationSec, boolean replicated) {
        CompletableFuture<Subscription> subscriptionFuture = new CompletableFuture<>();
        if (checkMaxSubscriptionsPerTopicExceed(subscriptionName)) {
            subscriptionFuture.completeExceptionally(new NotAllowedException(
                    "Exceed the maximum number of subscriptions of the topic: " + topic));
            return subscriptionFuture;
        }

        Map<String, Long> properties = PersistentSubscription.getBaseCursorProperties(replicated);

        ledger.asyncOpenCursor(Codec.encode(subscriptionName), initialPosition, properties, new OpenCursorCallback() {
            @Override
            public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Opened cursor", topic, subscriptionName);
                }

                PersistentSubscription subscription = subscriptions.get(subscriptionName);
                if (subscription == null) {
                    subscription = subscriptions.computeIfAbsent(subscriptionName,
                                  name -> createPersistentSubscription(subscriptionName, cursor, replicated));
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
            MessageId startMessageId, InitialPosition initialPosition, long startMessageRollbackDurationSec) {
        log.info("[{}][{}] Creating non-durable subscription at msg id {}", topic, subscriptionName, startMessageId);

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
                    cursor = ledger.newNonDurableCursor(startPosition, subscriptionName, initialPosition);
                } catch (ManagedLedgerException e) {
                    return FutureUtil.failedFuture(e);
                }

                subscription = new PersistentSubscription(this, subscriptionName, cursor, false);
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
                - TimeUnit.SECONDS.toMillis(startMessageRollbackDurationSec);
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
                                                              boolean replicateSubscriptionState) {
        return getDurableSubscription(subscriptionName, initialPosition,
                0 /*avoid reseting cursor*/, replicateSubscriptionState);
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
        getBrokerService().getManagedLedgerFactory().asyncDelete(TopicName.get(MLPendingAckStore
                .getTransactionPendingAckStoreSuffix(topic,
                        Codec.encode(subscriptionName))).getPersistenceNamingEncoding(),
                new AsyncCallbacks.DeleteLedgerCallback() {
            @Override
            public void deleteLedgerComplete(Object ctx) {
                asyncDeleteCursor(subscriptionName, unsubscribeFuture);
            }

            @Override
            public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                if (exception instanceof MetadataNotFoundException) {
                    asyncDeleteCursor(subscriptionName, unsubscribeFuture);
                    return;
                }

                unsubscribeFuture.completeExceptionally(exception);
                log.error("[{}][{}] Error deleting subscription pending ack store",
                        topic, subscriptionName, exception);
            }
        }, null);

        return unsubscribeFuture;
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
                unsubscribeFuture.completeExceptionally(new PersistenceException(exception));
            }
        }, null);
    }

    void removeSubscription(String subscriptionName) {
        PersistentSubscription sub = subscriptions.remove(subscriptionName);
        // preserve accumulative stats form removed subscription
        SubscriptionStatsImpl stats = sub.getStats(false, false);
        bytesOutFromRemovedSubscriptions.add(stats.bytesOutCounter);
        msgOutFromRemovedSubscriptions.add(stats.msgOutCounter);
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

    private CompletableFuture<Void> delete(boolean failIfHasSubscriptions,
                                           boolean failIfHasBacklogs, boolean deleteSchema) {
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
     * Delete the managed ledger associated with this topic.
     *
     * @param failIfHasSubscriptions
     *            Flag indicating whether delete should succeed if topic still has unconnected subscriptions. Set to
     *            false when called from admin API (it will delete the subs too), and set to true when called from GC
     *            thread
     * @param closeIfClientsConnected
     *            Flag indicate whether explicitly close connected
     *            producers/consumers/replicators before trying to delete topic.
     *            If any client is connected to a topic and if this flag is disable then this operation fails.
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
                if (currentUsageCount() ==  0 || (closeIfClientsConnected && !failIfHasSubscriptions)) {
                    CompletableFuture<SchemaVersion> deleteSchemaFuture =
                            deleteSchema ? deleteSchema() : CompletableFuture.completedFuture(null);

                    deleteSchemaFuture.thenAccept(__ -> deleteTopicPolicies())
                            .thenCompose(__ -> transactionBuffer.clearSnapshot()).whenComplete((v, ex) -> {
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
                                            brokerService.removeTopicFromCache(topic);

                                            dispatchRateLimiter.ifPresent(DispatchRateLimiter::close);

                                            subscribeRateLimiter.ifPresent(SubscribeRateLimiter::close);

                                            brokerService.pulsar().getTopicPoliciesService()
                                                    .clean(TopicName.get(topic));
                                            log.info("[{}] Topic deleted", topic);
                                            deleteFuture.complete(null);
                                        }

                                        @Override
                                        public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                                            if (exception.getCause()
                                                    instanceof MetadataStoreException.NotFoundException) {
                                                log.info("[{}] Topic is already deleted {}",
                                                        topic, exception.getMessage());
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
                        }
                    });
                } else {
                    unfenceTopicToResume();
                    deleteFuture.completeExceptionally(new TopicBusyException(
                            "Topic has " + currentUsageCount() + " connected producers/consumers"));
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
     * Close this topic - close all producers and subscriptions associated with this topic.
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

        futures.add(transactionBuffer.closeAsync());
        replicators.forEach((cluster, replicator) -> futures.add(replicator.disconnect()));
        producers.values().forEach(producer -> futures.add(producer.disconnect()));
        if (topicPublishRateLimiter != null) {
            try {
                topicPublishRateLimiter.close();
            } catch (Exception e) {
                log.warn("Error closing topicPublishRateLimiter for topic {}", topic, e);
            }
        }
        subscriptions.forEach((s, sub) -> futures.add(sub.disconnect()));
        if (this.resourceGroupPublishLimiter != null) {
            this.resourceGroupPublishLimiter.unregisterRateLimitFunction(this.getName());
        }

        CompletableFuture<Void> clientCloseFuture = closeWithoutWaitingClientDisconnect
                ? CompletableFuture.completedFuture(null)
                : FutureUtil.waitForAll(futures);

        clientCloseFuture.thenRun(() -> {
            // After having disconnected all producers/consumers, close the managed ledger
            ledger.asyncClose(new CloseCallback() {
                @Override
                public void closeComplete(Object ctx) {
                    // Everything is now closed, remove the topic from map
                    brokerService.removeTopicFromCache(topic)
                            .thenRun(() -> {
                                replicatedSubscriptionsController.ifPresent(ReplicatedSubscriptionsController::close);

                                dispatchRateLimiter.ifPresent(DispatchRateLimiter::close);

                                subscribeRateLimiter.ifPresent(SubscribeRateLimiter::close);

                                brokerService.pulsar().getTopicPoliciesService().clean(TopicName.get(topic));
                                log.info("[{}] Topic closed", topic);
                                closeFuture.complete(null);
                            })
                    .exceptionally(ex -> {
                        closeFuture.completeExceptionally(ex);
                        return null;
                    });
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

    @VisibleForTesting
    CompletableFuture<Void> checkReplicationAndRetryOnFailure() {
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

        CompletableFuture<Policies> policiesFuture = brokerService.pulsar().getPulsarResources()
                .getNamespaceResources()
                .getPoliciesAsync(TopicName.get(topic).getNamespaceObject())
                .thenCompose(optPolicies -> {
                            if (!optPolicies.isPresent()) {
                                return FutureUtil.failedFuture(
                                        new ServerMetadataException(
                                                new MetadataStoreException.NotFoundException()));
                            }

                            return CompletableFuture.completedFuture(optPolicies.get());
                        });

        CompletableFuture<Integer> ttlFuture = getMessageTTL();

        return CompletableFuture.allOf(policiesFuture, ttlFuture)
                .thenCompose(__ -> {
                    Policies policies = policiesFuture.join();
                    int newMessageTTLinSeconds = ttlFuture.join();

                    Set<String> configuredClusters;
                    if (policies.replication_clusters != null) {
                        configuredClusters = Sets.newTreeSet(policies.replication_clusters);
                    } else {
                        configuredClusters = Collections.emptySet();
                    }

                    String localCluster = brokerService.pulsar().getConfiguration().getClusterName();

                    // if local cluster is removed from global namespace cluster-list : then delete topic forcefully
                    // because pulsar doesn't serve global topic without local repl-cluster configured.
                    if (TopicName.get(topic).isGlobal() && !configuredClusters.contains(localCluster)) {
                        log.info("Deleting topic [{}] because local cluster is not part of "
                                + " global namespace repl list {}", topic, configuredClusters);
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
                });
    }

    @Override
    public void checkMessageExpiry() {
        getMessageTTL().thenAccept(messageTtlInSeconds -> {
            //If topic level policy or message ttl is not set, fall back to namespace level config.

            if (messageTtlInSeconds != 0) {
                subscriptions.forEach((__, sub) -> sub.expireMessages(messageTtlInSeconds));
                replicators.forEach((__, replicator)
                        -> ((PersistentReplicator) replicator).expireMessages(messageTtlInSeconds));
            }
        });
    }

    @Override
    public void checkMessageDeduplicationInfo() {
        messageDeduplication.purgeInactiveProducers();
    }

    public CompletableFuture<Boolean> isCompactionEnabled() {
        Optional<Long> topicCompactionThreshold = getTopicPolicies()
                .map(TopicPolicies::getCompactionThreshold);
        if (topicCompactionThreshold.isPresent() && topicCompactionThreshold.get() > 0) {
            return CompletableFuture.completedFuture(true);
        }

        TopicName topicName = TopicName.get(topic);
        return brokerService.getPulsar().getPulsarResources().getNamespaceResources()
                .getPoliciesAsync(topicName.getNamespaceObject())
                .thenApply(policies -> {
                    if (policies.isPresent()) {
                        return policies.get().compaction_threshold != null
                                && policies.get().compaction_threshold > 0;
                    } else {
                        // Check broker default
                        return brokerService.pulsar().getConfiguration()
                                .getBrokerServiceCompactionThresholdInBytes() > 0;
                    }
                });
    }

    public void checkCompaction() {
        TopicName name = TopicName.get(topic);
        try {
            Long compactionThreshold = getTopicPolicies()
                .map(TopicPolicies::getCompactionThreshold)
                .orElse(null);
            if (compactionThreshold == null) {
                Policies policies = brokerService.pulsar().getPulsarResources().getNamespaceResources()
                        .getPolicies(name.getNamespaceObject())
                        .orElseThrow(() -> new MetadataStoreException.NotFoundException());
                compactionThreshold = policies.compaction_threshold;
            }
            if (compactionThreshold == null) {
                compactionThreshold = brokerService.pulsar().getConfiguration()
                        .getBrokerServiceCompactionThresholdInBytes();
            }

            if (isSystemTopic() || compactionThreshold != 0
                && currentCompaction.isDone()) {

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

    public CompletableFuture<Void> preCreateSubscriptionForCompactionIfNeeded() {
        if (subscriptions.containsKey(COMPACTION_SUBSCRIPTION)) {
            // The compaction cursor is already there, nothing to do
            return CompletableFuture.completedFuture(null);
        }

        return isCompactionEnabled()
                .thenCompose(enabled -> {
                    if (enabled) {
                        // If a topic has a compaction policy setup, we must make sure that the compaction cursor
                        // is pre-created, in order to ensure all the data will be seen by the compactor.
                        return createSubscription(COMPACTION_SUBSCRIPTION,
                                        CommandSubscribe.InitialPosition.Earliest, false)
                                .thenCompose(__ -> CompletableFuture.completedFuture(null));
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
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

    protected CompletableFuture<Void> addReplicationCluster(String remoteCluster, ManagedCursor cursor,
            String localCluster) {
        return AbstractReplicator.validatePartitionedTopicAsync(PersistentTopic.this.getName(), brokerService)
                .thenCompose(__ -> brokerService.pulsar().getPulsarResources().getClusterResources()
                        .getClusterAsync(remoteCluster)
                        .thenApply(clusterData ->
                                brokerService.getReplicationClient(remoteCluster, clusterData)))
                .thenAccept(replicationClient -> {
                    replicators.computeIfAbsent(remoteCluster, r -> {
                        try {
                            return new PersistentReplicator(PersistentTopic.this, cursor, localCluster,
                                    remoteCluster, brokerService, (PulsarClientImpl) replicationClient);
                        } catch (PulsarServerException e) {
                            log.error("[{}] Replicator startup failed {}", topic, remoteCluster, e);
                        }
                        return null;
                    });

                    // clean up replicator if startup is failed
                    if (replicators.containsKey(remoteCluster) && replicators.get(remoteCluster) == null) {
                        replicators.remove(remoteCluster);
                    }
                });
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
    public int getNumberOfSameAddressConsumers(final String clientAddress) {
        return getNumberOfSameAddressConsumers(clientAddress, subscriptions.values());
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
    public void updateRates(NamespaceStats nsStats, NamespaceBundleStats bundleStats,
                            StatsOutputStream topicStatsStream,
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
                log.warn("[{}] Failed to update cursro state ", topic, e);
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
                    subMsgThroughputOut += consumerStats.msgThroughputOut;
                    subMsgRateRedeliver += consumerStats.msgRateRedeliver;

                    StreamingStats.writeConsumerStats(topicStatsStream, subscription.getType(), consumerStats);
                }

                // Close Consumer stats
                topicStatsStream.endList();

                // Populate subscription specific stats here
                topicStatsStream.writePair("msgBacklog",
                        subscription.getNumberOfEntriesInBacklog(true));
                topicStatsStream.writePair("msgRateExpired", subscription.getExpiredMessageRate());
                topicStatsStream.writePair("msgRateOut", subMsgRateOut);
                topicStatsStream.writePair("msgThroughputOut", subMsgThroughputOut);
                topicStatsStream.writePair("msgRateRedeliver", subMsgRateRedeliver);
                topicStatsStream.writePair("numberOfEntriesSinceFirstNotAckedMessage",
                        subscription.getNumberOfEntriesSinceFirstNotAckedMessage());
                topicStatsStream.writePair("totalNonContiguousDeletedMessagesRange",
                        subscription.getTotalNonContiguousDeletedMessagesRange());
                topicStatsStream.writePair("type", subscription.getTypeString());
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
    public TopicStatsImpl getStats(boolean getPreciseBacklog, boolean subscriptionBacklogSize) {

        TopicStatsImpl stats = new TopicStatsImpl();

        ObjectObjectHashMap<String, PublisherStatsImpl> remotePublishersStats = new ObjectObjectHashMap<>();

        producers.values().forEach(producer -> {
            PublisherStatsImpl publisherStats = producer.getStats();
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
        stats.waitingPublishers = getWaitingProducersCount();
        stats.bytesOutCounter = bytesOutFromRemovedSubscriptions.longValue();
        stats.msgOutCounter = msgOutFromRemovedSubscriptions.longValue();

        subscriptions.forEach((name, subscription) -> {
            SubscriptionStatsImpl subStats = subscription.getStats(getPreciseBacklog, subscriptionBacklogSize);

            stats.msgRateOut += subStats.msgRateOut;
            stats.msgThroughputOut += subStats.msgThroughputOut;
            stats.bytesOutCounter += subStats.bytesOutCounter;
            stats.msgOutCounter += subStats.msgOutCounter;
            stats.subscriptions.put(name, subStats);
            stats.nonContiguousDeletedMessagesRanges += subStats.nonContiguousDeletedMessagesRanges;
            stats.nonContiguousDeletedMessagesRangesSerializedSize +=
                    subStats.nonContiguousDeletedMessagesRangesSerializedSize;
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
        stats.offloadedStorageSize = ledger.getOffloadedSize();
        stats.lastOffloadLedgerId = ledger.getLastOffloadedLedgerId();
        stats.lastOffloadSuccessTimeStamp = ledger.getLastOffloadedSuccessTimestamp();
        stats.lastOffloadFailureTimeStamp = ledger.getLastOffloadedFailureTimestamp();
        Optional<CompactorMXBean> mxBean = getCompactorMXBean();

        stats.compaction.reset();
        mxBean.flatMap(bean -> bean.getCompactionRecordForTopic(topic)).map(compactionRecord -> {
            stats.compaction.lastCompactionRemovedEventCount = compactionRecord.getLastCompactionRemovedEventCount();
            stats.compaction.lastCompactionSucceedTimestamp = compactionRecord.getLastCompactionSucceedTimestamp();
            stats.compaction.lastCompactionFailedTimestamp = compactionRecord.getLastCompactionFailedTimestamp();
            stats.compaction.lastCompactionDurationTimeInMills =
                    compactionRecord.getLastCompactionDurationTimeInMills();
            return compactionRecord;
        });
        return stats;
    }

    private Optional<CompactorMXBean> getCompactorMXBean() {
        Compactor compactor = null;
        try {
            compactor = brokerService.pulsar().getCompactor(false);
        } catch (PulsarServerException ex) {
            log.warn("get compactor error", ex);
        }
        return Optional.ofNullable(compactor).map(c -> c.getStats());
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

        stats.ledgers = Lists.newArrayList();
        List<CompletableFuture<String>> futures = includeLedgerMetadata ? Lists.newArrayList() : null;
        CompletableFuture<Set<String>> availableBookiesFuture =
                brokerService.pulsar().getPulsarResources().getBookieResources().listAvailableBookiesAsync();
        availableBookiesFuture.whenComplete((bookies, e) -> {
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
                    stats.ledgers.add(info);
                    if (futures != null) {
                        futures.add(ml.getLedgerMetadata(li.getLedgerId()).handle((lMetadata, ex) -> {
                            if (ex == null) {
                                info.metadata = lMetadata;
                            }
                            return null;
                        }));
                        futures.add(ml.getEnsemblesAsync(li.getLedgerId()).handle((ensembles, ex) -> {
                            if (ex == null) {
                                info.underReplicated = !bookies.containsAll(ensembles.stream().map(BookieId::toString)
                                        .collect(Collectors.toList()));
                            }
                            return null;
                        }));
                    }
                });
            }
        });

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
        schemaStoreLedgersFuture.thenRun(() -> {
            if (futures != null) {
                FutureUtil.waitForAll(futures).handle((res, ex) -> {
                    statFuture.complete(stats);
                    return null;
                });
            } else {
                statFuture.complete(stats);
            }
        }).exceptionally(e -> {
            statFuture.completeExceptionally(e);
            return null;
        });
        return statFuture;
    }

    public Optional<CompactedTopicContext> getCompactedTopicContext() {
        try {
            return ((CompactedTopicImpl) compactedTopic).getCompactedTopicContext();
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
                    .thenApply((res) -> tryToDeletePartitionedMetadata())
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

    private CompletableFuture<Void> tryToDeletePartitionedMetadata() {
        if (TopicName.get(topic).isPartitioned() && !deletePartitionedTopicMetadataWhileInactive()) {
            return CompletableFuture.completedFuture(null);
        }
        TopicName topicName = TopicName.get(TopicName.get(topic).getPartitionedTopicName());
        try {
            PartitionedTopicResources partitionedTopicResources = getBrokerService().pulsar().getPulsarResources()
                    .getNamespaceResources()
                    .getPartitionedTopicResources();
            if (topicName.isPartitioned() && !partitionedTopicResources.partitionedTopicExists(topicName)) {
                return CompletableFuture.completedFuture(null);
            }
            CompletableFuture<Void> deleteMetadataFuture = new CompletableFuture<>();
            getBrokerService().fetchPartitionedTopicMetadataAsync(TopicName.get(topicName.getPartitionedTopicName()))
                    .thenAccept((metadata -> {
                        // make sure all sub partitions were deleted
                        for (int i = 0; i < metadata.partitions; i++) {
                            if (brokerService.getPulsar().getPulsarResources().getTopicResources()
                                    .persistentTopicExists(topicName.getPartition(i)).join()) {
                                throw new UnsupportedOperationException();
                            }
                        }
                    }))
                    .thenAccept((res) -> partitionedTopicResources.deletePartitionedTopicAsync(topicName)
                            .thenAccept((r) -> {
                        deleteMetadataFuture.complete(null);
                    }).exceptionally(ex -> {
                        deleteMetadataFuture.completeExceptionally(ex.getCause());
                        return null;
                    }))
                    .exceptionally((e) -> {
                        if (!(e.getCause() instanceof UnsupportedOperationException)) {
                            log.error("delete metadata fail", e);
                        }
                        deleteMetadataFuture.complete(null);
                        return null;
                    });
            return deleteMetadataFuture;
        } catch (Exception e) {
            return FutureUtil.failedFuture(e);
        }
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
                    if (sub.dispatcher != null && sub.dispatcher.isConsumerConnected() || sub.isReplicated()) {
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

    @Override
    public void checkDeduplicationSnapshot() {
        messageDeduplication.takeSnapshot();
    }

    /**
     * Check whether the topic should be retained (based on time), even tough there are no producers/consumers and it's
     * marked as inactive.
     */
    private boolean shouldTopicBeRetained() {
        RetentionPolicies retentionPolicies = null;
        try {
            retentionPolicies = getTopicPolicies()
                    .map(TopicPolicies::getRetentionPolicies)
                    .orElse(null);
            if (retentionPolicies == null){
                TopicName name = TopicName.get(topic);
                retentionPolicies = brokerService.pulsar().getPulsarResources().getNamespaceResources()
                        .getPolicies(name.getNamespaceObject())
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
            log.debug("[{}] isEncryptionRequired changes: {} -> {}", topic, isEncryptionRequired,
                    data.encryption_required);
        }
        if (data.deleted) {
            log.debug("Ignore the update because it has been deleted : {}", data);
            return CompletableFuture.completedFuture(null);
        }
        isEncryptionRequired = data.encryption_required;

        setSchemaCompatibilityStrategy(data);
        isAllowAutoUpdateSchema = data.is_allow_auto_update_schema;

        schemaValidationEnforced = data.schema_validation_enforced;
        updateUnackedMessagesAppliedOnSubscription(data);
        updateUnackedMessagesExceededOnConsumer(data);
        maxSubscriptionsPerTopic = data.max_subscriptions_per_topic;

        if (data.delayed_delivery_policies != null) {
            delayedDeliveryTickTimeMillis = data.delayed_delivery_policies.getTickTime();
            delayedDeliveryEnabled = data.delayed_delivery_policies.isActive();
        }
        //If the topic-level policy already exists, the namespace-level policy cannot override the topic-level policy.
        Optional<TopicPolicies> topicPolicies = getTopicPolicies();
        if (data.inactive_topic_policies != null) {
            if (!topicPolicies.isPresent() || !topicPolicies.get().isInactiveTopicPoliciesSet()) {
                this.inactiveTopicPolicies = data.inactive_topic_policies;
            }
        } else {
            ServiceConfiguration cfg = brokerService.getPulsar().getConfiguration();
            resetInactiveTopicPolicies(cfg.getBrokerDeleteInactiveTopicsMode()
                    , cfg.getBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds(),
                    cfg.isBrokerDeleteInactiveTopicsEnabled());
        }

        initializeRateLimiterIfNeeded(Optional.ofNullable(data));

        this.updateMaxPublishRate(data);

        producers.values().forEach(producer -> {
            producer.checkPermissions();
            producer.checkEncryption();
        });
        subscriptions.forEach((subName, sub) -> {
            sub.getConsumers().forEach(Consumer::checkPermissions);
            Dispatcher dispatcher = sub.getDispatcher();
            // If the topic-level policy already exists, the namespace-level policy cannot override
            // the topic-level policy.
            if (dispatcher != null
                    && (!topicPolicies.isPresent() || !topicPolicies.get().isSubscriptionDispatchRateSet())) {
                dispatcher.getRateLimiter().ifPresent(rateLimiter -> rateLimiter.onPoliciesUpdate(data));
            }
        });
        replicators.forEach((name, replicator) ->
                replicator.getRateLimiter().ifPresent(DispatchRateLimiter::updateDispatchRate)
        );
        checkMessageExpiry();
        CompletableFuture<Void> replicationFuture = checkReplicationAndRetryOnFailure();
        CompletableFuture<Void> dedupFuture = checkDeduplicationStatus();
        CompletableFuture<Void> persistentPoliciesFuture = checkPersistencePolicies();
        // update rate-limiter if policies updated
        if (this.dispatchRateLimiter.isPresent()) {
            if (!topicPolicies.isPresent() || !topicPolicies.get().isDispatchRateSet()) {
                dispatchRateLimiter.get().onPoliciesUpdate(data);
            }
        }
        if (this.subscribeRateLimiter.isPresent()) {
            subscribeRateLimiter.get().onPoliciesUpdate(data);
        }


        return CompletableFuture.allOf(replicationFuture, dedupFuture, persistentPoliciesFuture,
                preCreateSubscriptionForCompactionIfNeeded());
    }

    /**
     *
     * @return Backlog quota for topic
     */
    @Override
    public BacklogQuota getBacklogQuota(BacklogQuota.BacklogQuotaType backlogQuotaType) {
        TopicName topicName = TopicName.get(this.getName());
        return brokerService.getBacklogQuotaManager().getBacklogQuota(topicName, backlogQuotaType);
    }

    /**
     *
     * @return quota exceeded status for blocking producer creation
     */
    @Override
    public boolean isBacklogQuotaExceeded(String producerName, BacklogQuota.BacklogQuotaType backlogQuotaType) {
        BacklogQuota backlogQuota = getBacklogQuota(backlogQuotaType);

        if (backlogQuota != null) {
            BacklogQuota.RetentionPolicy retentionPolicy = backlogQuota.getPolicy();

            if ((retentionPolicy == BacklogQuota.RetentionPolicy.producer_request_hold
                    || retentionPolicy == BacklogQuota.RetentionPolicy.producer_exception)) {
                if (backlogQuotaType == BacklogQuota.BacklogQuotaType.destination_storage && isSizeBacklogExceeded()
                || backlogQuotaType == BacklogQuota.BacklogQuotaType.message_age && isTimeBacklogExceeded()){
                    log.info("[{}] Backlog quota exceeded. Cannot create producer [{}]", this.getName(), producerName);
                    return true;
                }
            } else {
                return false;
            }
        }
        return false;
    }

    /**
     * @return determine if backlog quota enforcement needs to be done for topic based on size limit
     */
    public boolean isSizeBacklogExceeded() {
        TopicName topicName = TopicName.get(getName());
        long backlogQuotaLimitInBytes = brokerService.getBacklogQuotaManager().getBacklogQuotaLimitInSize(topicName);
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

    /**
     * @return determine if backlog quota enforcement needs to be done for topic based on time limit
     */
    public boolean isTimeBacklogExceeded() {
        TopicName topicName = TopicName.get(getName());
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        int backlogQuotaLimitInSecond = brokerService.getBacklogQuotaManager().getBacklogQuotaLimitInTime(topicName);

        // If backlog quota by time is not set and we have no durable cursor.
        if (backlogQuotaLimitInSecond <= 0
                || ((ManagedCursorContainer) ledger.getCursors()).getSlowestReaderPosition() == null) {
            return false;
        }

        if (brokerService.pulsar().getConfiguration().isPreciseTimeBasedBacklogQuotaCheck()) {
            // Check if first unconsumed message(first message after mark delete position)
            // for slowest cursor's has expired.
            PositionImpl position = ((ManagedLedgerImpl) ledger).getNextValidPosition(((ManagedCursorContainer)
                    ledger.getCursors()).getSlowestReaderPosition());
            ((ManagedLedgerImpl) ledger).asyncReadEntry(position,
                    new AsyncCallbacks.ReadEntryCallback() {
                        @Override
                        public void readEntryComplete(Entry entry, Object ctx) {
                            try {
                                long entryTimestamp = MessageImpl.getEntryTimestamp(entry.getDataBuffer());
                                boolean expired = MessageImpl.isEntryExpired(backlogQuotaLimitInSecond, entryTimestamp);
                                if (expired && log.isDebugEnabled()) {
                                    log.debug("Time based backlog quota exceeded, oldest entry in cursor {}'s backlog"
                                    + "exceeded quota {}", ((ManagedLedgerImpl) ledger).getSlowestConsumer().getName(),
                                            backlogQuotaLimitInSecond);
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

            try {
                return future.get();
            } catch (Exception e) {
                log.error("[{}][{}] Error reading entry for precise time based backlog check", topicName, e);
                return false;
            }
        } else {
            Long ledgerId = ((ManagedCursorContainer) ledger.getCursors()).getSlowestReaderPosition().getLedgerId();
            try {
                org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo
                        ledgerInfo = ledger.getLedgerInfo(ledgerId).get();
                if (ledgerInfo != null && ledgerInfo.hasTimestamp() && ledgerInfo.getTimestamp() > 0
                        && ((ManagedLedgerImpl) ledger).getClock().millis() - ledgerInfo.getTimestamp()
                        > backlogQuotaLimitInSecond * 1000) {
                    if (log.isDebugEnabled()) {
                        log.debug("Time based backlog quota exceeded, quota {}, age of ledger "
                                        + "slowest cursor currently on {}", backlogQuotaLimitInSecond * 1000,
                                ((ManagedLedgerImpl) ledger).getClock().millis() - ledgerInfo.getTimestamp());
                    }
                    return true;
                } else {
                    return false;
                }
            } catch (Exception e) {
                log.error("[{}][{}] Error reading entry for precise time based backlog check", topicName, e);
                return false;
            }
        }
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

    public boolean isOldestMessageExpired(ManagedCursor cursor, int messageTTLInSeconds) {
        Entry entry = null;
        boolean isOldestMessageExpired = false;
        try {
            entry = cursor.getNthEntry(1, IndividualDeletedEntries.Include);
            if (entry != null) {
                long entryTimestamp = MessageImpl.getEntryTimestamp(entry.getDataBuffer());
                isOldestMessageExpired = MessageImpl.isEntryExpired(
                        (int) (messageTTLInSeconds * MESSAGE_EXPIRY_THRESHOLD), entryTimestamp);
            }
        } catch (Exception e) {
            log.warn("[{}] Error while getting the oldest message", topic, e);
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

    /**
     * Get message TTL for this topic.
     * @return Message TTL in second.
     */
    private CompletableFuture<Integer> getMessageTTL() {
        //Return Topic level message TTL if exist. If topic level policy or message ttl is not set,
        //fall back to namespace level message ttl then message ttl set for current broker.
        Optional<Integer> messageTtl = getTopicPolicies().map(TopicPolicies::getMessageTTLInSeconds);
        if (messageTtl.isPresent()) {
            return CompletableFuture.completedFuture(messageTtl.get());
        }
        TopicName name = TopicName.get(topic);

        return brokerService.pulsar().getPulsarResources().getNamespaceResources()
                .getPoliciesAsync(TopicName.get(topic).getNamespaceObject())
                .thenApply(optPolicies -> {
                    if (optPolicies.isPresent()) {
                        if (optPolicies.get().message_ttl_in_seconds != null) {
                            return optPolicies.get().message_ttl_in_seconds;
                        }
                    }

                    return brokerService.getPulsar().getConfiguration().getTtlDurationDefaultInSeconds();
                });
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentTopic.class);

    @Override
    public CompletableFuture<Void> addSchemaIfIdleOrCheckCompatible(SchemaData schema) {
        return hasSchema()
            .thenCompose((hasSchema) -> {
                int numActiveConsumers = subscriptions.values().stream()
                        .mapToInt(subscription -> subscription.getConsumers().size())
                        .sum();
                if (hasSchema
                        || (!producers.isEmpty())
                        || (numActiveConsumers != 0)
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
            log.info("[{}] There are no replicated subscriptions on the topic", topic);
        }

        checkReplicatedSubscriptionControllerState(shouldBeEnabled.get());
    }

    private synchronized void checkReplicatedSubscriptionControllerState(boolean shouldBeEnabled) {
        boolean isCurrentlyEnabled = replicatedSubscriptionsController.isPresent();
        boolean isEnableReplicatedSubscriptions =
                brokerService.pulsar().getConfiguration().isEnableReplicatedSubscriptions();

        if (shouldBeEnabled && !isCurrentlyEnabled && isEnableReplicatedSubscriptions) {
            log.info("[{}] Enabling replicated subscriptions controller", topic);
            replicatedSubscriptionsController = Optional.of(new ReplicatedSubscriptionsController(this,
                    brokerService.pulsar().getConfiguration().getClusterName()));
        } else if (isCurrentlyEnabled && !shouldBeEnabled || !isEnableReplicatedSubscriptions) {
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

    public CompactedTopic getCompactedTopic() {
        return compactedTopic;
    }

    @Override
    public boolean isSystemTopic() {
        return false;
    }

    private synchronized void fence() {
        isFenced = true;
        ScheduledFuture<?> monitoringTask = this.fencedTopicMonitoringTask;
        if (monitoringTask == null || monitoringTask.isDone()) {
            final int timeout = brokerService.pulsar().getConfiguration().getTopicFencingTimeoutSeconds();
            if (timeout > 0) {
                this.fencedTopicMonitoringTask = brokerService.executor().schedule(this::closeFencedTopicForcefully,
                        timeout, TimeUnit.SECONDS);
            }
        }
    }

    private synchronized void unfence() {
        isFenced = false;
        ScheduledFuture<?> monitoringTask = this.fencedTopicMonitoringTask;
        if (monitoringTask != null && !monitoringTask.isDone()) {
            monitoringTask.cancel(false);
        }
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
        isFenced = false;
        isClosingOrDeleting = false;
    }

    @Override
    public void publishTxnMessage(TxnID txnID, ByteBuf headersAndPayload, PublishContext publishContext) {
        pendingWriteOps.incrementAndGet();
        // in order to avoid the opAddEntry retain

        // in order to promise the publish txn message orderly, we should change the transactionCompletableFuture

        if (isFenced) {
            publishContext.completed(new TopicFencedException("fenced"), -1, -1);
            decrementPendingWriteOpsAndCheck();
            return;
        }
        if (isExceedMaximumMessageSize(headersAndPayload.readableBytes())) {
            publishContext.completed(new NotAllowedException("Exceed maximum message size")
                    , -1, -1);
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
                            publishContext.completed(null, ((PositionImpl) position).getLedgerId(),
                                    ((PositionImpl) position).getEntryId());

                            decrementPendingWriteOpsAndCheck();
                        })
                        .exceptionally(throwable -> {
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
        //Topic level setting has higher priority than namespace level
        return getTopicPolicies()
                .map(TopicPolicies::getDelayedDeliveryTickTimeMillis)
                .orElse(delayedDeliveryTickTimeMillis);
    }

    public int getMaxUnackedMessagesOnConsumer() {
        return maxUnackedMessagesOnConsumerAppilied;
    }

    public boolean isDelayedDeliveryEnabled() {
        //Topic level setting has higher priority than namespace level
        return getTopicPolicies()
                .map(TopicPolicies::getDelayedDeliveryEnabled)
                .orElse(delayedDeliveryEnabled);
    }

    public int getMaxUnackedMessagesOnSubscription() {
        return maxUnackedMessagesOnSubscriptionApplied;
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

        subscriptions.forEach((subName, sub) -> {
            sub.getConsumers().forEach(Consumer::checkPermissions);
            Dispatcher dispatcher = sub.getDispatcher();
            dispatcher.updateRateLimiter(policies.getSubscriptionDispatchRate());
        });

        if (policies.getPublishRate() != null) {
            updatePublishDispatcher(policies.getPublishRate());
        } else {
            updateMaxPublishRate(namespacePolicies.orElse(null));
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
                    , cfg.getBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds(),
                    cfg.isBrokerDeleteInactiveTopicsEnabled());
        }
        updateUnackedMessagesAppliedOnSubscription(namespacePolicies.orElse(null));
        initializeTopicSubscribeRateLimiterIfNeeded(Optional.ofNullable(policies));
        if (this.subscribeRateLimiter.isPresent()) {
            subscribeRateLimiter.ifPresent(subscribeRateLimiter ->
                subscribeRateLimiter.onSubscribeRateUpdate(policies.getSubscribeRate()));
        }
        replicators.forEach((name, replicator) -> replicator.getRateLimiter()
                .ifPresent(DispatchRateLimiter::updateDispatchRate));
        updateUnackedMessagesExceededOnConsumer(namespacePolicies.orElse(null));

        checkDeduplicationStatus();

        preCreateSubscriptionForCompactionIfNeeded();

        // update managed ledger config
        checkPersistencePolicies();
    }

    private Optional<Policies> getNamespacePolicies() {
        return DispatchRateLimiter.getPolicies(brokerService, topic);
    }

    private void initializeTopicDispatchRateLimiterIfNeeded(TopicPolicies policies) {
        synchronized (dispatchRateLimiter) {
            if (!dispatchRateLimiter.isPresent() && policies.getDispatchRate() != null) {
                this.dispatchRateLimiter = Optional.of(new DispatchRateLimiter(this, Type.TOPIC));
            }
        }
    }

    private void initializeTopicSubscribeRateLimiterIfNeeded(Optional<TopicPolicies> policies) {
        if (!policies.isPresent()) {
            return;
        }
        synchronized (subscribeRateLimiter) {
            if (!subscribeRateLimiter.isPresent()
                    && policies.get().getSubscribeRate() != null
                    && policies.get().getSubscribeRate().subscribeThrottlingRatePerConsumer > 0) {
                this.subscribeRateLimiter = Optional.of(new SubscribeRateLimiter(this));
            } else if (!policies.get().isSubscribeRateSet()
                    || policies.get().getSubscribeRate().subscribeThrottlingRatePerConsumer <= 0) {
                this.subscribeRateLimiter = Optional.empty();
            }
        }
    }

    private PersistentTopic getPersistentTopic() {
        return this;
    }

    private void registerTopicPolicyListener() {
        if (brokerService.pulsar().getConfig().isSystemTopicEnabled()
                && brokerService.pulsar().getConfig().isTopicLevelPoliciesEnabled()) {
            TopicName topicName = TopicName.get(topic);
            TopicName cloneTopicName = topicName;
            if (topicName.isPartitioned()) {
                cloneTopicName = TopicName.get(topicName.getPartitionedTopicName());
            }

            brokerService.getPulsar().getTopicPoliciesService().registerListener(cloneTopicName, this);
        }
    }

    @VisibleForTesting
    public MessageDeduplication getMessageDeduplication() {
        return messageDeduplication;
    }

    private boolean checkMaxSubscriptionsPerTopicExceed(String subscriptionName) {
        //Existing subscriptions are not affected
        if (StringUtils.isNotEmpty(subscriptionName) && getSubscription(subscriptionName) != null) {
            return false;
        }
        Integer maxSubsPerTopic = getTopicPolicies()
                .map(TopicPolicies::getMaxSubscriptionsPerTopic)
                .orElseGet(() -> {
                    if (maxSubscriptionsPerTopic != null) {
                        return maxSubscriptionsPerTopic;
                    } else {
                        return brokerService.pulsar().getConfig().getMaxSubscriptionsPerTopic();
                    }
                });

        if (maxSubsPerTopic > 0) {
            if (subscriptions != null && subscriptions.size() >= maxSubsPerTopic) {
                return true;
            }
        }

        return false;
    }

    public boolean checkSubscriptionTypesEnable(SubType subType) throws Exception {
        TopicName topicName = TopicName.get(topic);
        if (brokerService.pulsar().getConfiguration().isTopicLevelPoliciesEnabled()) {
            try {
                TopicPolicies topicPolicies =
                        brokerService.pulsar().getTopicPoliciesService().getTopicPolicies(TopicName.get(topic));
                if (topicPolicies == null) {
                    return checkNsAndBrokerSubscriptionTypesEnable(topicName, subType);
                } else {
                    if (topicPolicies.getSubscriptionTypesEnabled().isEmpty()) {
                        return checkNsAndBrokerSubscriptionTypesEnable(topicName, subType);
                    }
                    return topicPolicies.getSubscriptionTypesEnabled().contains(subType);
                }
            } catch (BrokerServiceException.TopicPoliciesCacheNotInitException e) {
                return checkNsAndBrokerSubscriptionTypesEnable(topicName, subType);
            }
        } else {
            return checkNsAndBrokerSubscriptionTypesEnable(topicName, subType);
        }
    }

    private boolean checkNsAndBrokerSubscriptionTypesEnable(TopicName topicName, SubType subType) throws Exception {
        Optional<Policies> policies = brokerService.pulsar().getPulsarResources().getNamespaceResources()
                .getPolicies(topicName.getNamespaceObject());
        if (policies.isPresent()) {
            if (policies.get().subscription_types_enabled.isEmpty()) {
                return getBrokerService().getPulsar().getConfiguration()
                        .getSubscriptionTypesEnabled().contains(subType.name());
            } else {
                return policies.get().subscription_types_enabled.contains(subType.name());
            }
        } else {
            return getBrokerService().getPulsar().getConfiguration()
                    .getSubscriptionTypesEnabled().contains(subType.name());
        }
    }

    public TransactionBufferStats getTransactionBufferStats() {
        return this.transactionBuffer.getStats();
    }

    public TransactionPendingAckStats getTransactionPendingAckStats(String subName) {
        return this.subscriptions.get(subName).getTransactionPendingAckStats();
    }

    public PositionImpl getMaxReadPosition() {
        return this.transactionBuffer.getMaxReadPosition();
    }

    public boolean isTxnAborted(TxnID txnID) {
        return this.transactionBuffer.isTxnAborted(txnID);
    }

    public TransactionInBufferStats getTransactionInBufferStats(TxnID txnID) {
        return this.transactionBuffer.getTransactionInBufferStats(txnID);
    }

    @Override
    protected boolean isTerminated() {
        return ledger.isTerminated();
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

    public long getLastDataMessagePublishedTimestamp() {
        return lastDataMessagePublishedTimestamp;
    }
}
