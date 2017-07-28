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
package org.apache.pulsar.broker.service.nonpersistent;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.mledger.impl.EntryCacheManager.create;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.util.SafeRun;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.NamingException;
import org.apache.pulsar.broker.service.BrokerServiceException.ServerMetadataException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicFencedException;
import org.apache.pulsar.broker.service.BrokerServiceException.UnsupportedVersionException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.Replicator;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.stats.ClusterReplicationMetrics;
import org.apache.pulsar.broker.stats.NamespaceStats;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.util.FutureUtil;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.NonPersistentPublisherStats;
import org.apache.pulsar.common.policies.data.NonPersistentReplicatorStats;
import org.apache.pulsar.common.policies.data.NonPersistentSubscriptionStats;
import org.apache.pulsar.common.policies.data.NonPersistentTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats.CursorStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.utils.StatsOutputStream;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.internal.Lists;
import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.RecyclableDuplicateByteBuf;
import io.netty.util.concurrent.FastThreadLocal;

public class NonPersistentTopic implements Topic {
    private final String topic;

    // Producers currently connected to this topic
    private final ConcurrentOpenHashSet<Producer> producers;

    // Subscriptions to this topic
    private final ConcurrentOpenHashMap<String, NonPersistentSubscription> subscriptions;

    private final ConcurrentOpenHashMap<String, NonPersistentReplicator> replicators;

    private final BrokerService brokerService;

    private volatile boolean isFenced;

    // Prefix for replication cursors
    public final String replicatorPrefix;

    protected static final AtomicLongFieldUpdater<NonPersistentTopic> USAGE_COUNT_UPDATER = AtomicLongFieldUpdater
            .newUpdater(NonPersistentTopic.class, "usageCount");
    private volatile long usageCount = 0;

    private final OrderedSafeExecutor executor;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSZ")
            .withZone(ZoneId.systemDefault());

    // Timestamp of when this topic was last seen active
    private volatile long lastActive;

    // Flag to signal that producer of this topic has published batch-message so, broker should not allow consumer which
    // doesn't support batch-message
    private volatile boolean hasBatchMessagePublished = false;
    // Ever increasing counter of entries added
    static final AtomicLongFieldUpdater<NonPersistentTopic> ENTRIES_ADDED_COUNTER_UPDATER = AtomicLongFieldUpdater
            .newUpdater(NonPersistentTopic.class, "entriesAddedCounter");
    private volatile long entriesAddedCounter = 0;
    private static final long POLICY_UPDATE_FAILURE_RETRY_TIME_SECONDS = 60;

    private static final FastThreadLocal<TopicStats> threadLocalTopicStats = new FastThreadLocal<TopicStats>() {
        @Override
        protected TopicStats initialValue() {
            return new TopicStats();
        }
    };

    private static class TopicStats {
        public double averageMsgSize;
        public double aggMsgRateIn;
        public double aggMsgThroughputIn;
        public double aggMsgRateOut;
        public double aggMsgThroughputOut;
        public final ObjectObjectHashMap<String, PublisherStats> remotePublishersStats;

        public TopicStats() {
            remotePublishersStats = new ObjectObjectHashMap<String, PublisherStats>();
            reset();
        }

        public void reset() {
            averageMsgSize = 0;
            aggMsgRateIn = 0;
            aggMsgThroughputIn = 0;
            aggMsgRateOut = 0;
            aggMsgThroughputOut = 0;
            remotePublishersStats.clear();
        }
    }

    public NonPersistentTopic(String topic, BrokerService brokerService) {
        this.topic = topic;
        this.brokerService = brokerService;
        this.producers = new ConcurrentOpenHashSet<Producer>();
        this.subscriptions = new ConcurrentOpenHashMap<>();
        this.replicators = new ConcurrentOpenHashMap<>();
        this.isFenced = false;
        this.replicatorPrefix = brokerService.pulsar().getConfiguration().getReplicatorPrefix();
        this.executor = brokerService.getTopicOrderedExecutor();
        USAGE_COUNT_UPDATER.set(this, 0);

        this.lastActive = System.nanoTime();
    }

    @Override
    public void publishMessage(ByteBuf data, PublishCallback callback) {
        AtomicInteger msgDeliveryCount = new AtomicInteger(2);
        ENTRIES_ADDED_COUNTER_UPDATER.incrementAndGet(this);

        // retain data for sub/replication because io-thread will release actual payload
        data.retain(2);
        this.executor.submitOrdered(topic, SafeRun.safeRun(() -> {
            subscriptions.forEach((name, subscription) -> {
                ByteBuf duplicateBuffer = RecyclableDuplicateByteBuf.create(data);
                Entry entry = create(0L, 0L, duplicateBuffer);
                // entry internally retains data so, duplicateBuffer should be release here
                duplicateBuffer.release();
                if (subscription.getDispatcher() != null) {
                    subscription.getDispatcher().sendMessages(Lists.newArrayList(entry));
                } else {
                    // it happens when subscription is created but dispatcher is not created as consumer is not added
                    // yet
                    entry.release();
                }
            });
            data.release();
            if (msgDeliveryCount.decrementAndGet() == 0) {
                callback.completed(null, 0L, 0L);
            }
        }));

        this.executor.submitOrdered(topic, SafeRun.safeRun(() -> {
            replicators.forEach((name, replicator) -> {
                ByteBuf duplicateBuffer = RecyclableDuplicateByteBuf.create(data);
                Entry entry = create(0L, 0L, duplicateBuffer);
                // entry internally retains data so, duplicateBuffer should be release here
                duplicateBuffer.release();
                ((NonPersistentReplicator) replicator).sendMessage(entry);
            });
            data.release();
            if (msgDeliveryCount.decrementAndGet() == 0) {
                callback.completed(null, 0L, 0L);
            }
        }));
    }

    @Override
    public void addProducer(Producer producer) throws BrokerServiceException {
        checkArgument(producer.getTopic() == this);

        lock.readLock().lock();
        try {
            if (isFenced) {
                log.warn("[{}] Attempting to add producer to a fenced topic", topic);
                throw new TopicFencedException("Topic is temporarily unavailable");
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}] {} Got request to create producer ", topic, producer.getProducerName());
            }

            if (!producers.add(producer)) {
                throw new NamingException(
                        "Producer with name '" + producer.getProducerName() + "' is already connected to topic");
            }

            USAGE_COUNT_UPDATER.incrementAndGet(this);
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Added producer -- count: {}", topic, producer.getProducerName(),
                        USAGE_COUNT_UPDATER.get(this));
            }

        } finally {
            lock.readLock().unlock();
        }
    }

    private boolean hasLocalProducers() {
        AtomicBoolean foundLocal = new AtomicBoolean(false);
        producers.forEach(producer -> {
            if (!producer.isRemote()) {
                foundLocal.set(true);
            }
        });

        return foundLocal.get();
    }

    private boolean hasRemoteProducers() {
        AtomicBoolean foundRemote = new AtomicBoolean(false);
        producers.forEach(producer -> {
            if (producer.isRemote()) {
                foundRemote.set(true);
            }
        });

        return foundRemote.get();
    }

    @Override
    public void removeProducer(Producer producer) {
        checkArgument(producer.getTopic() == this);
        if (producers.remove(producer)) {
            // decrement usage only if this was a valid producer close
            USAGE_COUNT_UPDATER.decrementAndGet(this);
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Removed producer -- count: {}", topic, producer.getProducerName(),
                        USAGE_COUNT_UPDATER.get(this));
            }
            lastActive = System.nanoTime();
        }
    }

    @Override
    public CompletableFuture<Consumer> subscribe(final ServerCnx cnx, String subscriptionName, long consumerId,
            SubType subType, int priorityLevel, String consumerName, boolean isDurable, MessageId startMessageId) {

        final CompletableFuture<Consumer> future = new CompletableFuture<>();

        if (hasBatchMessagePublished && !cnx.isBatchMessageCompatibleVersion()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Consumer doesn't support batch-message {}", topic, subscriptionName);
            }
            future.completeExceptionally(new UnsupportedVersionException("Consumer doesn't support batch-message"));
            return future;
        }

        if (subscriptionName.startsWith(replicatorPrefix)) {
            log.warn("[{}] Failed to create subscription for {}", topic, subscriptionName);
            future.completeExceptionally(new NamingException("Subscription with reserved subscription name attempted"));
            return future;
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

        NonPersistentSubscription subscription = subscriptions.computeIfAbsent(subscriptionName,
                name -> new NonPersistentSubscription(this, subscriptionName));

        try {
            Consumer consumer = new Consumer(subscription, subType, consumerId, priorityLevel, consumerName, 0, cnx,
                    cnx.getRole());
            subscription.addConsumer(consumer);
            if (!cnx.isActive()) {
                consumer.close();
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] [{}] Subscribe failed -- count: {}", topic, subscriptionName,
                            consumer.consumerName(), USAGE_COUNT_UPDATER.get(NonPersistentTopic.this));
                }
                future.completeExceptionally(
                        new BrokerServiceException("Connection was closed while the opening the cursor "));
            } else {
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

            USAGE_COUNT_UPDATER.decrementAndGet(NonPersistentTopic.this);
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public CompletableFuture<Subscription> createSubscription(String subscriptionName) {
        return CompletableFuture.completedFuture(new NonPersistentSubscription(this, subscriptionName));
    }

    void removeSubscription(String subscriptionName) {
        subscriptions.remove(subscriptionName);
    }

    @Override
    public CompletableFuture<Void> delete() {
        return delete(false);
    }

    private CompletableFuture<Void> delete(boolean failIfHasSubscriptions) {
        CompletableFuture<Void> deleteFuture = new CompletableFuture<>();

        lock.writeLock().lock();
        try {
            if (isFenced) {
                log.warn("[{}] Topic is already being closed or deleted", topic);
                deleteFuture.completeExceptionally(new TopicFencedException("Topic is already fenced"));
                return deleteFuture;
            }
            if (USAGE_COUNT_UPDATER.get(this) == 0) {
                isFenced = true;

                List<CompletableFuture<Void>> futures = Lists.newArrayList();

                if (failIfHasSubscriptions) {
                    if (!subscriptions.isEmpty()) {
                        isFenced = false;
                        deleteFuture.completeExceptionally(new TopicBusyException("Topic has subscriptions"));
                        return deleteFuture;
                    }
                } else {
                    subscriptions.forEach((s, sub) -> futures.add(sub.delete()));
                }

                FutureUtil.waitForAll(futures).whenComplete((v, ex) -> {
                    if (ex != null) {
                        log.error("[{}] Error deleting topic", topic, ex);
                        isFenced = false;
                        deleteFuture.completeExceptionally(ex);
                    } else {
                        brokerService.removeTopicFromCache(topic);
                        log.info("[{}] Topic deleted", topic);
                        deleteFuture.complete(null);
                    }
                });
            } else {
                deleteFuture.completeExceptionally(new TopicBusyException(
                        "Topic has " + USAGE_COUNT_UPDATER.get(this) + " connected producers/consumers"));
            }
        } finally {
            lock.writeLock().unlock();
        }

        return deleteFuture;
    }

    /**
     * Close this topic - close all producers and subscriptions associated with this topic
     *
     * @return Completable future indicating completion of close operation
     */
    @Override
    public CompletableFuture<Void> close() {
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        lock.writeLock().lock();
        try {
            if (!isFenced) {
                isFenced = true;
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
        producers.forEach(producer -> futures.add(producer.disconnect()));
        subscriptions.forEach((s, sub) -> futures.add(sub.disconnect()));

        FutureUtil.waitForAll(futures).thenRun(() -> {
            log.info("[{}] Topic closed", topic);
            closeFuture.complete(null);
        }).exceptionally(exception -> {
            log.error("[{}] Error closing topic", topic, exception);
            isFenced = false;
            closeFuture.completeExceptionally(exception);
            return null;
        });

        return closeFuture;
    }

    public CompletableFuture<Void> stopReplProducers() {
        List<CompletableFuture<Void>> closeFutures = Lists.newArrayList();
        replicators.forEach((region, replicator) -> closeFutures.add(replicator.disconnect()));
        return FutureUtil.waitForAll(closeFutures);
    }

    @Override
    public CompletableFuture<Void> checkReplication() {
        DestinationName name = DestinationName.get(topic);
        if (!name.isGlobal()) {
            return CompletableFuture.completedFuture(null);
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Checking replication status", name);
        }

        Policies policies = null;
        try {
            policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                    .get(AdminResource.path("policies", name.getNamespace()))
                    .orElseThrow(() -> new KeeperException.NoNodeException());
        } catch (Exception e) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new ServerMetadataException(e));
            return future;
        }

        Set<String> configuredClusters;
        if (policies.replication_clusters != null) {
            configuredClusters = Sets.newTreeSet(policies.replication_clusters);
        } else {
            configuredClusters = Collections.emptySet();
        }

        String localCluster = brokerService.pulsar().getConfiguration().getClusterName();

        List<CompletableFuture<Void>> futures = Lists.newArrayList();

        // Check for missing replicators
        for (String cluster : configuredClusters) {
            if (cluster.equals(localCluster)) {
                continue;
            }

            if (!replicators.containsKey(cluster)) {
                startReplicator(cluster);
            }
        }

        // Check for replicators to be stopped
        replicators.forEach((cluster, replicator) -> {
            if (!cluster.equals(localCluster)) {
                if (!configuredClusters.contains(cluster)) {
                    futures.add(removeReplicator(cluster));
                }
            }
        });
        return FutureUtil.waitForAll(futures);
    }

    void startReplicator(String remoteCluster) {
        log.info("[{}] Starting replicator to remote: {}", topic, remoteCluster);
        String localCluster = brokerService.pulsar().getConfiguration().getClusterName();
        replicators.computeIfAbsent(remoteCluster,
                r -> new NonPersistentReplicator(NonPersistentTopic.this, localCluster, remoteCluster, brokerService));
    }

    CompletableFuture<Void> removeReplicator(String remoteCluster) {
        log.info("[{}] Removing replicator to {}", topic, remoteCluster);
        final CompletableFuture<Void> future = new CompletableFuture<>();

        String name = NonPersistentReplicator.getReplicatorName(replicatorPrefix, remoteCluster);

        replicators.get(remoteCluster).disconnect().thenRun(() -> {
            log.info("[{}] Successfully removed replicator {}", name, remoteCluster);

        }).exceptionally(e -> {
            log.error("[{}] Failed to close replication producer {} {}", topic, name, e.getMessage(), e);
            future.completeExceptionally(e);
            return null;
        });

        return future;
    }

    private CompletableFuture<Void> checkReplicationAndRetryOnFailure() {
        CompletableFuture<Void> result = new CompletableFuture<Void>();
        checkReplication().thenAccept(res -> {
            log.info("[{}] Policies updated successfully", topic);
            result.complete(null);
        }).exceptionally(th -> {
            log.error("[{}] Policies update failed {}, scheduled retry in {} seconds", topic, th.getMessage(),
                    POLICY_UPDATE_FAILURE_RETRY_TIME_SECONDS, th);
            brokerService.executor().schedule(this::checkReplicationAndRetryOnFailure,
                    POLICY_UPDATE_FAILURE_RETRY_TIME_SECONDS, TimeUnit.SECONDS);
            result.completeExceptionally(th);
            return null;
        });
        return result;
    }

    @Override
    public void checkMessageExpiry() {
        // No-op
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("topic", topic).toString();
    }

    @Override
    public ConcurrentOpenHashSet<Producer> getProducers() {
        return producers;
    }

    @Override
    public ConcurrentOpenHashMap<String, NonPersistentSubscription> getSubscriptions() {
        return subscriptions;
    }

    @Override
    public ConcurrentOpenHashMap<String, NonPersistentReplicator> getReplicators() {
        return replicators;
    }

    @Override
    public Subscription getSubscription(String subscription) {
        return subscriptions.get(subscription);
    }

    public Replicator getPersistentReplicator(String remoteCluster) {
        return replicators.get(remoteCluster);
    }

    public BrokerService getBrokerService() {
        return brokerService;
    }

    @Override
    public String getName() {
        return topic;
    }

    public void updateRates(NamespaceStats nsStats, NamespaceBundleStats bundleStats, StatsOutputStream destStatsStream,
            ClusterReplicationMetrics replStats, String namespace) {

        TopicStats topicStats = threadLocalTopicStats.get();
        topicStats.reset();

        replicators.forEach((region, replicator) -> replicator.updateRates());

        nsStats.producerCount += producers.size();
        bundleStats.producerCount += producers.size();
        destStatsStream.startObject(topic);

        producers.forEach(producer -> {
            producer.updateRates();
            PublisherStats PublisherStats = producer.getStats();

            topicStats.aggMsgRateIn += PublisherStats.msgRateIn;
            topicStats.aggMsgThroughputIn += PublisherStats.msgThroughputIn;

            if (producer.isRemote()) {
                topicStats.remotePublishersStats.put(producer.getRemoteCluster(), PublisherStats);
            }
        });

        // Creating publishers object for backward compatibility
        destStatsStream.startList("publishers");
        destStatsStream.endList();

        // Start replicator stats
        destStatsStream.startObject("replication");
        nsStats.replicatorCount += topicStats.remotePublishersStats.size();

        // Close replication
        destStatsStream.endObject();

        // Start subscription stats
        destStatsStream.startObject("subscriptions");
        nsStats.subsCount += subscriptions.size();

        subscriptions.forEach((subscriptionName, subscription) -> {
            double subMsgRateOut = 0;
            double subMsgThroughputOut = 0;
            double subMsgRateRedeliver = 0;

            // Start subscription name & consumers
            try {
                destStatsStream.startObject(subscriptionName);
                Object[] consumers = subscription.getConsumers().array();
                nsStats.consumerCount += consumers.length;
                bundleStats.consumerCount += consumers.length;

                destStatsStream.startList("consumers");

                subscription.getDispatcher().getMesssageDropRate().calculateRate();
                for (Object consumerObj : consumers) {
                    Consumer consumer = (Consumer) consumerObj;
                    consumer.updateRates();

                    ConsumerStats consumerStats = consumer.getStats();
                    subMsgRateOut += consumerStats.msgRateOut;
                    subMsgThroughputOut += consumerStats.msgThroughputOut;
                    subMsgRateRedeliver += consumerStats.msgRateRedeliver;

                    // Populate consumer specific stats here
                    destStatsStream.startObject();
                    destStatsStream.writePair("address", consumerStats.address);
                    destStatsStream.writePair("consumerName", consumerStats.consumerName);
                    destStatsStream.writePair("availablePermits", consumerStats.availablePermits);
                    destStatsStream.writePair("connectedSince", consumerStats.connectedSince);
                    destStatsStream.writePair("msgRateOut", consumerStats.msgRateOut);
                    destStatsStream.writePair("msgThroughputOut", consumerStats.msgThroughputOut);
                    destStatsStream.writePair("msgRateRedeliver", consumerStats.msgRateRedeliver);

                    if (SubType.Shared.equals(subscription.getType())) {
                        destStatsStream.writePair("unackedMessages", consumerStats.unackedMessages);
                        destStatsStream.writePair("blockedConsumerOnUnackedMsgs",
                                consumerStats.blockedConsumerOnUnackedMsgs);
                    }
                    if (consumerStats.clientVersion != null) {
                        destStatsStream.writePair("clientVersion", consumerStats.clientVersion);
                    }
                    destStatsStream.endObject();
                }

                // Close Consumer stats
                destStatsStream.endList();

                // Populate subscription specific stats here
                destStatsStream.writePair("msgBacklog", subscription.getNumberOfEntriesInBacklog());
                destStatsStream.writePair("msgRateExpired", subscription.getExpiredMessageRate());
                destStatsStream.writePair("msgRateOut", subMsgRateOut);
                destStatsStream.writePair("msgThroughputOut", subMsgThroughputOut);
                destStatsStream.writePair("msgRateRedeliver", subMsgRateRedeliver);
                destStatsStream.writePair("type", subscription.getTypeString());
                if (subscription.getDispatcher() != null) {
                    destStatsStream.writePair("msgDropRate",
                            subscription.getDispatcher().getMesssageDropRate().getRate());
                }

                // Close consumers
                destStatsStream.endObject();

                topicStats.aggMsgRateOut += subMsgRateOut;
                topicStats.aggMsgThroughputOut += subMsgThroughputOut;
                nsStats.msgBacklog += subscription.getNumberOfEntriesInBacklog();
            } catch (Exception e) {
                log.error("Got exception when creating consumer stats for subscription {}: {}", subscriptionName,
                        e.getMessage(), e);
            }
        });

        // Close subscription
        destStatsStream.endObject();

        // Remaining dest stats.
        topicStats.averageMsgSize = topicStats.aggMsgRateIn == 0.0 ? 0.0
                : (topicStats.aggMsgThroughputIn / topicStats.aggMsgRateIn);
        destStatsStream.writePair("producerCount", producers.size());
        destStatsStream.writePair("averageMsgSize", topicStats.averageMsgSize);
        destStatsStream.writePair("msgRateIn", topicStats.aggMsgRateIn);
        destStatsStream.writePair("msgRateOut", topicStats.aggMsgRateOut);
        destStatsStream.writePair("msgThroughputIn", topicStats.aggMsgThroughputIn);
        destStatsStream.writePair("msgThroughputOut", topicStats.aggMsgThroughputOut);

        nsStats.msgRateIn += topicStats.aggMsgRateIn;
        nsStats.msgRateOut += topicStats.aggMsgRateOut;
        nsStats.msgThroughputIn += topicStats.aggMsgThroughputIn;
        nsStats.msgThroughputOut += topicStats.aggMsgThroughputOut;

        bundleStats.msgRateIn += topicStats.aggMsgRateIn;
        bundleStats.msgRateOut += topicStats.aggMsgRateOut;
        bundleStats.msgThroughputIn += topicStats.aggMsgThroughputIn;
        bundleStats.msgThroughputOut += topicStats.aggMsgThroughputOut;

        // Close topic object
        destStatsStream.endObject();
    }

    public NonPersistentTopicStats getStats() {

        NonPersistentTopicStats stats = new NonPersistentTopicStats();

        ObjectObjectHashMap<String, PublisherStats> remotePublishersStats = new ObjectObjectHashMap<String, PublisherStats>();

        producers.forEach(producer -> {
            NonPersistentPublisherStats publisherStats = (NonPersistentPublisherStats) producer.getStats();
            stats.msgRateIn += publisherStats.msgRateIn;
            stats.msgThroughputIn += publisherStats.msgThroughputIn;

            if (producer.isRemote()) {
                remotePublishersStats.put(producer.getRemoteCluster(), publisherStats);
            } else {
                stats.getPublishers().add(publisherStats);
            }
        });

        stats.averageMsgSize = stats.msgRateIn == 0.0 ? 0.0 : (stats.msgThroughputIn / stats.msgRateIn);

        subscriptions.forEach((name, subscription) -> {
            NonPersistentSubscriptionStats subStats = subscription.getStats();

            stats.msgRateOut += subStats.msgRateOut;
            stats.msgThroughputOut += subStats.msgThroughputOut;
            stats.getSubscriptions().put(name, subStats);
        });

        replicators.forEach((cluster, replicator) -> {
            NonPersistentReplicatorStats ReplicatorStats = replicator.getStats();

            // Add incoming msg rates
            PublisherStats pubStats = remotePublishersStats.get(replicator.getRemoteCluster());
            if (pubStats != null) {
                ReplicatorStats.msgRateIn = pubStats.msgRateIn;
                ReplicatorStats.msgThroughputIn = pubStats.msgThroughputIn;
                ReplicatorStats.inboundConnection = pubStats.address;
                ReplicatorStats.inboundConnectedSince = pubStats.connectedSince;
            }

            stats.msgRateOut += ReplicatorStats.msgRateOut;
            stats.msgThroughputOut += ReplicatorStats.msgThroughputOut;

            stats.getReplication().put(replicator.getRemoteCluster(), ReplicatorStats);
        });

        return stats;
    }

    public PersistentTopicInternalStats getInternalStats() {

        PersistentTopicInternalStats stats = new PersistentTopicInternalStats();
        stats.entriesAddedCounter = ENTRIES_ADDED_COUNTER_UPDATER.get(this);

        stats.cursors = Maps.newTreeMap();
        subscriptions.forEach((name, subs) -> stats.cursors.put(name, new CursorStats()));
        replicators.forEach((name, subs) -> stats.cursors.put(name, new CursorStats()));

        return stats;
    }

    public boolean isActive() {
        if (DestinationName.get(topic).isGlobal()) {
            // No local consumers and no local producers
            return !subscriptions.isEmpty() || hasLocalProducers();
        }
        return USAGE_COUNT_UPDATER.get(this) != 0 || !subscriptions.isEmpty();
    }

    @Override
    public void checkGC(int gcIntervalInSeconds) {
        if (isActive()) {
            lastActive = System.nanoTime();
        } else {
            if (System.nanoTime() - lastActive > TimeUnit.SECONDS.toNanos(gcIntervalInSeconds)) {

                if (DestinationName.get(topic).isGlobal()) {
                    // For global namespace, close repl producers first.
                    // Once all repl producers are closed, we can delete the topic,
                    // provided no remote producers connected to the broker.
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Global topic inactive for {} seconds, closing repl producers.", topic,
                                gcIntervalInSeconds);
                    }

                    stopReplProducers().thenCompose(v -> delete(true))
                            .thenRun(() -> log.info("[{}] Topic deleted successfully due to inactivity", topic))
                            .exceptionally(e -> {
                                if (e.getCause() instanceof TopicBusyException) {
                                    // topic became active again
                                    if (log.isDebugEnabled()) {
                                        log.debug("[{}] Did not delete busy topic: {}", topic,
                                                e.getCause().getMessage());
                                    }
                                    replicators.forEach((region, replicator) -> ((NonPersistentReplicator) replicator)
                                            .startProducer());
                                } else {
                                    log.warn("[{}] Inactive topic deletion failed", topic, e);
                                }
                                return null;
                            });

                }
            }
        }
    }

    @Override
    public CompletableFuture<Void> onPoliciesUpdate(Policies data) {
        producers.forEach(Producer::checkPermissions);
        subscriptions.forEach((subName, sub) -> sub.getConsumers().forEach(Consumer::checkPermissions));
        return checkReplicationAndRetryOnFailure();
    }

    /**
     *
     * @return Backlog quota for topic
     */
    @Override
    public BacklogQuota getBacklogQuota() {
        // No-op
        throw new UnsupportedOperationException("getBacklogQuota method is not supported on non-persistent topic");
    }

    /**
     *
     * @return quota exceeded status for blocking producer creation
     */
    @Override
    public boolean isBacklogQuotaExceeded(String producerName) {
        // No-op
        return false;
    }

    @Override
    public CompletableFuture<Void> unsubscribe(String subName) {
        // No-op
        return CompletableFuture.completedFuture(null);
    }

    public void markBatchMessagePublished() {
        this.hasBatchMessagePublished = true;
    }

    private static final Logger log = LoggerFactory.getLogger(NonPersistentTopic.class);

}
