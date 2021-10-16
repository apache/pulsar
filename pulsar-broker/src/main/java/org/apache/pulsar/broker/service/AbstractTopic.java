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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl.ENTRY_LATENCY_BUCKETS_USEC;
import com.google.common.base.MoreObjects;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Getter;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup;
import org.apache.pulsar.broker.resourcegroup.ResourceGroupPublishLimiter;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.ProducerBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.ProducerFencedException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicTerminatedException;
import org.apache.pulsar.broker.service.schema.BookkeeperSchemaStorage;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.broker.stats.prometheus.metrics.Summary;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTopic implements Topic {

    protected static final long POLICY_UPDATE_FAILURE_RETRY_TIME_SECONDS = 60;

    protected final String topic;

    // Producers currently connected to this topic
    protected final ConcurrentHashMap<String, Producer> producers;

    protected final BrokerService brokerService;

    // Prefix for replication cursors
    protected final String replicatorPrefix;

    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    protected volatile boolean isFenced;

    // Inactive topic policies
    protected InactiveTopicPolicies inactiveTopicPolicies = new InactiveTopicPolicies();

    // Timestamp of when this topic was last seen active
    protected volatile long lastActive;

    // Flag to signal that producer of this topic has published batch-message so, broker should not allow consumer which
    // doesn't support batch-message
    protected volatile boolean hasBatchMessagePublished = false;

    protected StatsBuckets addEntryLatencyStatsUsec = new StatsBuckets(ENTRY_LATENCY_BUCKETS_USEC);

    // Whether messages published must be encrypted or not in this topic
    protected volatile boolean isEncryptionRequired = false;
    protected volatile SchemaCompatibilityStrategy schemaCompatibilityStrategy =
            SchemaCompatibilityStrategy.FULL;
    protected volatile boolean isAllowAutoUpdateSchema = true;
    // schema validation enforced flag
    protected volatile boolean schemaValidationEnforced = false;

    protected volatile int maxUnackedMessagesOnConsumerAppilied = 0;

    protected volatile Integer maxSubscriptionsPerTopic = null;

    protected volatile PublishRateLimiter topicPublishRateLimiter;

    protected volatile ResourceGroupPublishLimiter resourceGroupPublishLimiter;

    protected boolean preciseTopicPublishRateLimitingEnable;

    @Getter
    protected boolean resourceGroupRateLimitingEnabled;

    private LongAdder bytesInCounter = new LongAdder();
    private LongAdder msgInCounter = new LongAdder();

    protected volatile Optional<Long> topicEpoch = Optional.empty();
    private volatile boolean hasExclusiveProducer;
    // pointer to the exclusive producer
    private volatile String exclusiveProducerName;

    private final Queue<Pair<Producer, CompletableFuture<Optional<Long>>>> waitingExclusiveProducers =
            new ConcurrentLinkedQueue<>();

    private static final AtomicLongFieldUpdater<AbstractTopic> USAGE_COUNT_UPDATER =
            AtomicLongFieldUpdater.newUpdater(AbstractTopic.class, "usageCount");
    private volatile long usageCount = 0;

    private volatile int topicMaxMessageSize = 0;
    private volatile long lastTopicMaxMessageSizeCheckTimeStamp = 0;
    private final long topicMaxMessageSizeCheckIntervalMs;

    public AbstractTopic(String topic, BrokerService brokerService) {
        this.topic = topic;
        this.brokerService = brokerService;
        this.producers = new ConcurrentHashMap<>();
        this.isFenced = false;
        this.replicatorPrefix = brokerService.pulsar().getConfiguration().getReplicatorPrefix();
        this.inactiveTopicPolicies.setDeleteWhileInactive(brokerService.pulsar().getConfiguration()
                .isBrokerDeleteInactiveTopicsEnabled());
        this.inactiveTopicPolicies.setMaxInactiveDurationSeconds(brokerService.pulsar().getConfiguration()
                .getBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds());
        this.inactiveTopicPolicies.setInactiveTopicDeleteMode(brokerService.pulsar().getConfiguration()
                .getBrokerDeleteInactiveTopicsMode());
        this.topicMaxMessageSizeCheckIntervalMs = TimeUnit.SECONDS.toMillis(brokerService.pulsar().getConfiguration()
                .getMaxMessageSizeCheckIntervalInSeconds());
        this.lastActive = System.nanoTime();
        this.preciseTopicPublishRateLimitingEnable =
                brokerService.pulsar().getConfiguration().isPreciseTopicPublishRateLimiterEnable();
        updatePublishDispatcher(Optional.empty());
    }

    protected boolean isProducersExceeded() {
        Integer maxProducers = getTopicPolicies().map(TopicPolicies::getMaxProducerPerTopic).orElse(null);

        if (maxProducers == null) {
            Policies policies = brokerService.pulsar().getPulsarResources().getNamespaceResources()
                    .getPoliciesIfCached(TopicName.get(topic).getNamespaceObject())
                    .orElseGet(() -> new Policies());
            maxProducers = policies.max_producers_per_topic;
        }
        maxProducers = maxProducers != null ? maxProducers : brokerService.pulsar()
                .getConfiguration().getMaxProducersPerTopic();
        if (maxProducers > 0 && maxProducers <= producers.size()) {
            return true;
        }
        return false;
    }

    protected boolean isSameAddressProducersExceeded(Producer producer) {
        final int maxSameAddressProducers = brokerService.pulsar().getConfiguration()
                .getMaxSameAddressProducersPerTopic();

        if (maxSameAddressProducers > 0
                && getNumberOfSameAddressProducers(producer.getClientAddress()) >= maxSameAddressProducers) {
            return true;
        }

        return false;
    }

    public int getNumberOfSameAddressProducers(final String clientAddress) {
        int count = 0;
        if (clientAddress != null) {
            for (Producer producer : producers.values()) {
                if (clientAddress.equals(producer.getClientAddress())) {
                    count++;
                }
            }
        }
        return count;
    }

    protected boolean isConsumersExceededOnTopic() {
        Integer maxConsumers = getTopicPolicies().map(TopicPolicies::getMaxConsumerPerTopic).orElse(null);
        if (maxConsumers == null) {

            // Use getDataIfPresent from zk cache to make the call non-blocking and prevent deadlocks
            Policies policies = brokerService.pulsar().getPulsarResources().getNamespaceResources().getPoliciesIfCached(
                            TopicName.get(topic).getNamespaceObject())
                    .orElseGet(() -> new Policies());

            maxConsumers = policies.max_consumers_per_topic;
        }
        final int maxConsumersPerTopic = maxConsumers != null ? maxConsumers
                : brokerService.pulsar().getConfiguration().getMaxConsumersPerTopic();
        if (maxConsumersPerTopic > 0 && maxConsumersPerTopic <= getNumberOfConsumers()) {
            return true;
        }
        return false;
    }

    protected boolean isSameAddressConsumersExceededOnTopic(Consumer consumer) {
        final int maxSameAddressConsumers = brokerService.pulsar().getConfiguration()
                .getMaxSameAddressConsumersPerTopic();

        if (maxSameAddressConsumers > 0
                && getNumberOfSameAddressConsumers(consumer.getClientAddress()) >= maxSameAddressConsumers) {
            return true;
        }

        return false;
    }

    public abstract int getNumberOfConsumers();
    public abstract int getNumberOfSameAddressConsumers(String clientAddress);

    protected int getNumberOfSameAddressConsumers(final String clientAddress,
            final List<? extends Subscription> subscriptions) {
        int count = 0;
        if (clientAddress != null) {
            for (Subscription subscription : subscriptions) {
                count += subscription.getNumberOfSameAddressConsumers(clientAddress);
            }
        }
        return count;
    }

    protected CompletableFuture<Void> addConsumerToSubscription(Subscription subscription, Consumer consumer) {
        if (isConsumersExceededOnTopic()) {
            log.warn("[{}] Attempting to add consumer to topic which reached max consumers limit", topic);
            return FutureUtil.failedFuture(new ConsumerBusyException("Topic reached max consumers limit"));
        }

        if (isSameAddressConsumersExceededOnTopic(consumer)) {
            log.warn("[{}] Attempting to add consumer to topic which reached max same address consumers limit", topic);
            return FutureUtil.failedFuture(new ConsumerBusyException("Topic reached max same address consumers limit"));
        }

        return subscription.addConsumer(consumer);
    }

    @Override
    public void disableCnxAutoRead() {
        producers.values().forEach(producer -> producer.getCnx().disableCnxAutoRead());
    }

    @Override
    public void enableCnxAutoRead() {
        producers.values().forEach(producer -> producer.getCnx().enableCnxAutoRead());
    }

    protected boolean hasLocalProducers() {
        AtomicBoolean foundLocal = new AtomicBoolean(false);
        producers.values().forEach(producer -> {
            if (!producer.isRemote()) {
                foundLocal.set(true);
            }
        });

        return foundLocal.get();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("topic", topic).toString();
    }

    @Override
    public Map<String, Producer> getProducers() {
        return producers;
    }


    public BrokerService getBrokerService() {
        return brokerService;
    }

    @Override
    public String getName() {
        return topic;
    }

    @Override
    public boolean isEncryptionRequired() {
        return isEncryptionRequired;
    }

    @Override
    public boolean getSchemaValidationEnforced() {
        return schemaValidationEnforced;
    }

    public void markBatchMessagePublished() {
        this.hasBatchMessagePublished = true;
    }

    public String getReplicatorPrefix() {
        return replicatorPrefix;
    }

    @Override
    public CompletableFuture<Boolean> hasSchema() {
        String base = TopicName.get(getName()).getPartitionedTopicName();
        String id = TopicName.get(base).getSchemaName();
        return brokerService.pulsar()
                .getSchemaRegistryService()
                .getSchema(id).thenApply(Objects::nonNull);
    }

    @Override
    public CompletableFuture<SchemaVersion> addSchema(SchemaData schema) {
        if (schema == null) {
            return CompletableFuture.completedFuture(SchemaVersion.Empty);
        }

        String base = TopicName.get(getName()).getPartitionedTopicName();
        String id = TopicName.get(base).getSchemaName();
        SchemaRegistryService schemaRegistryService = brokerService.pulsar().getSchemaRegistryService();
        return isAllowAutoUpdateSchema ? schemaRegistryService
                .putSchemaIfAbsent(id, schema, schemaCompatibilityStrategy)
                : schemaRegistryService.trimDeletedSchemaAndGetList(id).thenCompose(schemaAndMetadataList ->
                schemaRegistryService.getSchemaVersionBySchemaData(schemaAndMetadataList, schema)
                        .thenCompose(schemaVersion -> {
                    if (schemaVersion == null) {
                        return FutureUtil
                                .failedFuture(
                                        new IncompatibleSchemaException(
                                                "Schema not found and schema auto updating is disabled."));
                    } else {
                        return CompletableFuture.completedFuture(schemaVersion);
                    }
                }));
    }

    @Override
    public CompletableFuture<SchemaVersion> deleteSchema() {
        String base = TopicName.get(getName()).getPartitionedTopicName();
        String id = TopicName.get(base).getSchemaName();
        SchemaRegistryService schemaRegistryService = brokerService.pulsar().getSchemaRegistryService();
        return BookkeeperSchemaStorage.ignoreUnrecoverableBKException(schemaRegistryService.getSchema(id))
                .thenCompose(schema -> {
                    if (schema != null) {
                        // It's different from `SchemasResource.deleteSchema`
                        // because when we delete a topic, the schema
                        // history is meaningless. But when we delete a schema of a topic, a new schema could be
                        // registered in the future.
                        log.info("Delete schema storage of id: {}", id);
                        return schemaRegistryService.deleteSchemaStorage(id);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Override
    public CompletableFuture<Void> checkSchemaCompatibleForConsumer(SchemaData schema) {
        String base = TopicName.get(getName()).getPartitionedTopicName();
        String id = TopicName.get(base).getSchemaName();
        return brokerService.pulsar()
                .getSchemaRegistryService()
                .checkConsumerCompatibility(id, schema, schemaCompatibilityStrategy);
    }

    @Override
    public CompletableFuture<Optional<Long>> addProducer(Producer producer,
                                                         CompletableFuture<Void> producerQueuedFuture) {
        checkArgument(producer.getTopic() == this);

        return brokerService.checkTopicNsOwnership(getName())
                .thenCompose(__ ->
                        incrementTopicEpochIfNeeded(producer, producerQueuedFuture))
                .thenCompose(producerEpoch -> {
                    lock.writeLock().lock();
                    try {
                        checkTopicFenced();
                        if (isTerminated()) {
                            log.warn("[{}] Attempting to add producer to a terminated topic", topic);
                            throw new TopicTerminatedException("Topic was already terminated");
                        }
                        internalAddProducer(producer);

                        USAGE_COUNT_UPDATER.incrementAndGet(this);
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] [{}] Added producer -- count: {}", topic, producer.getProducerName(),
                                    USAGE_COUNT_UPDATER.get(this));
                        }

                        return CompletableFuture.completedFuture(producerEpoch);
                    } catch (BrokerServiceException e) {
                        return FutureUtil.failedFuture(e);
                    } finally {
                        lock.writeLock().unlock();
                    }
                });
    }

    protected CompletableFuture<Optional<Long>> incrementTopicEpochIfNeeded(Producer producer,
            CompletableFuture<Void> producerQueuedFuture) {
        lock.writeLock().lock();
        try {
            switch (producer.getAccessMode()) {
            case Shared:
                if (hasExclusiveProducer || !waitingExclusiveProducers.isEmpty()) {
                    return FutureUtil.failedFuture(
                            new ProducerBusyException(
                                    "Topic has an existing exclusive producer: " + exclusiveProducerName));
                } else {
                    // Normal producer getting added, we don't need a new epoch
                    return CompletableFuture.completedFuture(topicEpoch);
                }

            case Exclusive:
                if (hasExclusiveProducer || !waitingExclusiveProducers.isEmpty()) {
                    return FutureUtil.failedFuture(
                            new ProducerFencedException(
                                    "Topic has an existing exclusive producer: " + exclusiveProducerName));
                } else if (!producers.isEmpty()) {
                    return FutureUtil.failedFuture(new ProducerFencedException("Topic has existing shared producers"));
                } else if (producer.getTopicEpoch().isPresent()
                        && producer.getTopicEpoch().get() < topicEpoch.orElse(-1L)) {
                    // If a producer reconnects, but all the topic epoch has already moved forward, this producer needs
                    // to be fenced, because a new producer had been present in between.
                    return FutureUtil.failedFuture(new ProducerFencedException(
                            String.format("Topic epoch has already moved. Current epoch: %d, Producer epoch: %d",
                                    topicEpoch.get(), producer.getTopicEpoch().get())));
                } else {
                    // There are currently no existing producers
                    hasExclusiveProducer = true;
                    exclusiveProducerName = producer.getProducerName();

                    CompletableFuture<Long> future;
                    if (producer.getTopicEpoch().isPresent()) {
                        future = setTopicEpoch(producer.getTopicEpoch().get());
                    } else {
                        future = incrementTopicEpoch(topicEpoch);
                    }
                    future.exceptionally(ex -> {
                        hasExclusiveProducer = false;
                        exclusiveProducerName = null;
                        return null;
                    });

                    return future.thenApply(epoch -> {
                        topicEpoch = Optional.of(epoch);
                        return topicEpoch;
                    });
                }

            case WaitForExclusive: {
                if (hasExclusiveProducer || !producers.isEmpty()) {
                    CompletableFuture<Optional<Long>> future = new CompletableFuture<>();
                    log.info("[{}] Queuing producer {} since there's already a producer", topic, producer);
                    waitingExclusiveProducers.add(Pair.of(producer, future));
                    producerQueuedFuture.complete(null);
                    return future;
                } else if (producer.getTopicEpoch().isPresent()
                        && producer.getTopicEpoch().get() < topicEpoch.orElse(-1L)) {
                    // If a producer reconnects, but all the topic epoch has already moved forward, this producer needs
                    // to be fenced, because a new producer had been present in between.
                    return FutureUtil.failedFuture(new ProducerFencedException(
                            String.format("Topic epoch has already moved. Current epoch: %d, Producer epoch: %d",
                                    topicEpoch.get(), producer.getTopicEpoch().get())));
                } else {
                    // There are currently no existing producers
                    hasExclusiveProducer = true;
                    exclusiveProducerName = producer.getProducerName();

                    CompletableFuture<Long> future;
                    if (producer.getTopicEpoch().isPresent()) {
                        future = setTopicEpoch(producer.getTopicEpoch().get());
                    } else {
                        future = incrementTopicEpoch(topicEpoch);
                    }
                    future.exceptionally(ex -> {
                        hasExclusiveProducer = false;
                        exclusiveProducerName = null;
                        return null;
                    });

                    return future.thenApply(epoch -> {
                        topicEpoch = Optional.of(epoch);
                        return topicEpoch;
                    });
                }
            }

            default:
                return FutureUtil.failedFuture(
                        new BrokerServiceException("Invalid producer access mode: " + producer.getAccessMode()));
            }

        } catch (Exception e) {
            log.error("Encountered unexpected error during exclusive producer creation", e);
            return FutureUtil.failedFuture(new BrokerServiceException(e));
        } finally {
            lock.writeLock().unlock();
        }
    }

    protected abstract CompletableFuture<Long> setTopicEpoch(long newEpoch);

    protected abstract CompletableFuture<Long> incrementTopicEpoch(Optional<Long> currentEpoch);

    @Override
    public void recordAddLatency(long latency, TimeUnit unit) {
        addEntryLatencyStatsUsec.addValue(unit.toMicros(latency));

        PUBLISH_LATENCY.observe(latency, unit);
    }

    protected void setSchemaCompatibilityStrategy(Policies policies) {
        if (policies.schema_compatibility_strategy == SchemaCompatibilityStrategy.UNDEFINED) {
            schemaCompatibilityStrategy = brokerService.pulsar()
                    .getConfig().getSchemaCompatibilityStrategy();
            if (schemaCompatibilityStrategy == SchemaCompatibilityStrategy.UNDEFINED) {
                schemaCompatibilityStrategy = SchemaCompatibilityStrategy.fromAutoUpdatePolicy(
                        policies.schema_auto_update_compatibility_strategy);
            }
        } else {
            schemaCompatibilityStrategy = policies.schema_compatibility_strategy;
        }
    }
    private static final Summary PUBLISH_LATENCY = Summary.build("pulsar_broker_publish_latency", "-")
            .quantile(0.0)
            .quantile(0.50)
            .quantile(0.95)
            .quantile(0.99)
            .quantile(0.999)
            .quantile(0.9999)
            .quantile(1.0)
            .register();

    @Override
    public void checkTopicPublishThrottlingRate() {
        this.topicPublishRateLimiter.checkPublishRate();
    }

    @Override
    public void incrementPublishCount(int numOfMessages, long msgSizeInBytes) {
        // increase topic publish rate limiter
        this.topicPublishRateLimiter.incrementPublishCount(numOfMessages, msgSizeInBytes);
        // increase broker publish rate limiter
        getBrokerPublishRateLimiter().incrementPublishCount(numOfMessages, msgSizeInBytes);
        // increase counters
        bytesInCounter.add(msgSizeInBytes);
        msgInCounter.add(numOfMessages);
    }

    @Override
    public void resetTopicPublishCountAndEnableReadIfRequired() {
        // broker rate not exceeded. and completed topic limiter reset.
        if (!getBrokerPublishRateLimiter().isPublishRateExceeded() && topicPublishRateLimiter.resetPublishCount()) {
            enableProducerReadForPublishRateLimiting();
        }
    }

    @Override
    public void resetBrokerPublishCountAndEnableReadIfRequired(boolean doneBrokerReset) {
        // topic rate not exceeded, and completed broker limiter reset.
        if (!topicPublishRateLimiter.isPublishRateExceeded() && doneBrokerReset) {
            enableProducerReadForPublishRateLimiting();
        }
    }

    /**
     * it sets cnx auto-readable if producer's cnx is disabled due to publish-throttling.
     */
    protected void enableProducerReadForPublishRateLimiting() {
        if (producers != null) {
            producers.values().forEach(producer -> {
                producer.getCnx().cancelPublishRateLimiting();
                producer.getCnx().enableCnxAutoRead();
            });
        }
    }

    protected void enableProducerReadForPublishBufferLimiting() {
        if (producers != null) {
            producers.values().forEach(producer -> {
                producer.getCnx().cancelPublishBufferLimiting();
                producer.getCnx().enableCnxAutoRead();
            });
        }
    }

    protected void disableProducerRead() {
        if (producers != null) {
            producers.values().forEach(producer -> producer.getCnx().disableCnxAutoRead());
        }
    }

    protected void checkTopicFenced() throws BrokerServiceException {
        if (isFenced) {
            log.warn("[{}] Attempting to add producer to a fenced topic", topic);
            throw new BrokerServiceException.TopicFencedException("Topic is temporarily unavailable");
        }
    }

    protected void internalAddProducer(Producer producer) throws BrokerServiceException {
        if (isProducersExceeded()) {
            log.warn("[{}] Attempting to add producer to topic which reached max producers limit", topic);
            throw new BrokerServiceException.ProducerBusyException("Topic reached max producers limit");
        }

        if (isSameAddressProducersExceeded(producer)) {
            log.warn("[{}] Attempting to add producer to topic which reached max same address producers limit", topic);
            throw new BrokerServiceException.ProducerBusyException("Topic reached max same address producers limit");
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] {} Got request to create producer ", topic, producer.getProducerName());
        }

        Producer existProducer = producers.putIfAbsent(producer.getProducerName(), producer);
        if (existProducer != null) {
            tryOverwriteOldProducer(existProducer, producer);
        }
    }

    private void tryOverwriteOldProducer(Producer oldProducer, Producer newProducer)
            throws BrokerServiceException {
        boolean canOverwrite = false;
        if (oldProducer.equals(newProducer) && !isUserProvidedProducerName(oldProducer)
                && !isUserProvidedProducerName(newProducer) && newProducer.getEpoch() > oldProducer.getEpoch()) {
            oldProducer.close(false);
            canOverwrite = true;
        }
        if (canOverwrite) {
            if (!producers.replace(newProducer.getProducerName(), oldProducer, newProducer)) {
                // Met concurrent update, throw exception here so that client can try reconnect later.
                throw new BrokerServiceException.NamingException("Producer with name '" + newProducer.getProducerName()
                        + "' replace concurrency error");
            } else {
                handleProducerRemoved(oldProducer);
            }
        } else {
            throw new BrokerServiceException.NamingException(
                    "Producer with name '" + newProducer.getProducerName() + "' is already connected to topic");
        }
    }

    private boolean isUserProvidedProducerName(Producer producer){
        //considered replicator producer as generated name producer
        return producer.isUserProvidedProducerName() && !producer.getProducerName().startsWith(replicatorPrefix);
    }


    @Override
    public void removeProducer(Producer producer) {
        checkArgument(producer.getTopic() == this);

        if (producers.remove(producer.getProducerName(), producer)) {
            handleProducerRemoved(producer);
        }
    }

    protected void handleProducerRemoved(Producer producer) {
        // decrement usage only if this was a valid producer close
        USAGE_COUNT_UPDATER.decrementAndGet(this);
        // this conditional check is an optimization so we don't have acquire the write lock
        // and execute following routine if there are no exclusive producers
        if (hasExclusiveProducer) {
            lock.writeLock().lock();
            try {
                hasExclusiveProducer = false;
                exclusiveProducerName = null;
                Pair<Producer, CompletableFuture<Optional<Long>>> nextWaitingProducer =
                        waitingExclusiveProducers.poll();
                if (nextWaitingProducer != null) {
                    Producer nextProducer = nextWaitingProducer.getKey();
                    CompletableFuture<Optional<Long>> producerFuture = nextWaitingProducer.getValue();
                    hasExclusiveProducer = true;
                    exclusiveProducerName = nextProducer.getProducerName();

                    CompletableFuture<Long> future;
                    if (nextProducer.getTopicEpoch().isPresent()) {
                        future = setTopicEpoch(nextProducer.getTopicEpoch().get());
                    } else {
                        future = incrementTopicEpoch(topicEpoch);
                    }

                    future.thenAccept(epoch -> {
                        topicEpoch = Optional.of(epoch);
                        producerFuture.complete(topicEpoch);
                    }).exceptionally(ex -> {
                        hasExclusiveProducer = false;
                        exclusiveProducerName = null;
                        producerFuture.completeExceptionally(ex);
                        return null;
                    });
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Removed producer -- count: {}", topic, producer.getProducerName(),
                    USAGE_COUNT_UPDATER.get(this));
        }
        lastActive = System.nanoTime();
    }

    public void handleConsumerAdded(String subscriptionName, String consumerName) {
        USAGE_COUNT_UPDATER.incrementAndGet(this);
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] [{}] Added consumer -- count: {}", topic, subscriptionName,
                    consumerName, USAGE_COUNT_UPDATER.get(this));
        }
    }

    public void decrementUsageCount() {
        USAGE_COUNT_UPDATER.decrementAndGet(this);
    }

    public long currentUsageCount() {
        return usageCount;
    }

    @Override
    public boolean isPublishRateExceeded() {
        // either topic or broker publish rate exceeded.
        return this.topicPublishRateLimiter.isPublishRateExceeded()
                || getBrokerPublishRateLimiter().isPublishRateExceeded();
    }

    @Override
    public boolean isResourceGroupPublishRateExceeded(int numMessages, int bytes) {
        return this.resourceGroupRateLimitingEnabled
            && !this.resourceGroupPublishLimiter.tryAcquire(numMessages, bytes);
    }

    @Override
    public boolean isResourceGroupRateLimitingEnabled() {
        return this.resourceGroupRateLimitingEnabled;
    }

    @Override
    public boolean isTopicPublishRateExceeded(int numberMessages, int bytes) {
        // whether topic publish rate exceed if precise rate limit is enable
        return preciseTopicPublishRateLimitingEnable && !this.topicPublishRateLimiter.tryAcquire(numberMessages, bytes);
    }

    @Override
    public boolean isBrokerPublishRateExceeded() {
        // whether broker publish rate exceed
        return  getBrokerPublishRateLimiter().isPublishRateExceeded();
    }

    public PublishRateLimiter getTopicPublishRateLimiter() {
        return topicPublishRateLimiter;
    }

    public PublishRateLimiter getBrokerPublishRateLimiter() {
        return brokerService.getBrokerPublishRateLimiter();
    }

    public void updateMaxPublishRate(Policies policies) {
        updatePublishDispatcher(Optional.of(policies));
    }

    private void updatePublishDispatcher(Optional<Policies> optPolicies) {
        //if topic-level policy exists, try to use topic-level publish rate policy
        Optional<PublishRate> topicPublishRate = getTopicPolicies().map(TopicPolicies::getPublishRate);
        if (topicPublishRate.isPresent()) {
            log.info("Using topic policy publish rate instead of namespace level topic publish rate on topic {}",
                    this.topic);
            updatePublishDispatcher(topicPublishRate.get());
            return;
        }

        Policies policies;
        try {
            if (optPolicies.isPresent()) {
                policies = optPolicies.get();
            } else {
                policies = brokerService.pulsar().getPulsarResources().getNamespaceResources().getPoliciesIfCached(
                                TopicName.get(topic).getNamespaceObject())
                        .orElseGet(() -> new Policies());
            }
        } catch (Exception e) {
            log.warn("[{}] Error getting policies {} and publish throttling will be disabled", topic, e.getMessage());
            policies = new Policies();
        }

        //topic-level policy is not set, try to use namespace-level rate policy
        final String clusterName = brokerService.pulsar().getConfiguration().getClusterName();
        final PublishRate publishRate = policies.publishMaxMessageRate != null
                ? policies.publishMaxMessageRate.get(clusterName)
                : null;

        //both namespace-level and topic-level policy are not set, try to use broker-level policy
        ServiceConfiguration serviceConfiguration = brokerService.pulsar().getConfiguration();
        if (publishRate != null) {
            //publishRate is not null , use namespace-level policy
            updatePublishDispatcher(publishRate);
        } else {
            PublishRate brokerPublishRate = new PublishRate(serviceConfiguration.getMaxPublishRatePerTopicInMessages()
              , serviceConfiguration.getMaxPublishRatePerTopicInBytes());
            updatePublishDispatcher(brokerPublishRate);
        }

        // attach the resource-group level rate limiters, if set
        String rgName = policies != null && policies.resource_group_name != null
          ? policies.resource_group_name
          : null;
        if (rgName != null) {
            final ResourceGroup resourceGroup =
              brokerService.getPulsar().getResourceGroupServiceManager().resourceGroupGet(rgName);
            if (resourceGroup != null) {
                this.resourceGroupRateLimitingEnabled = true;
                this.resourceGroupPublishLimiter = resourceGroup.getResourceGroupPublishLimiter();
                this.resourceGroupPublishLimiter.registerRateLimitFunction(this.getName(),
                  () -> this.enableCnxAutoRead());
                log.info("Using resource group {} rate limiter for topic {}", rgName, topic);
                return;
            }
        } else {
            if (this.resourceGroupRateLimitingEnabled) {
                this.resourceGroupPublishLimiter.unregisterRateLimitFunction(this.getName());
                this.resourceGroupPublishLimiter = null;
                this.resourceGroupRateLimitingEnabled = false;
            }
            /* Namespace detached from resource group. Enable the producer read */
            enableProducerReadForPublishRateLimiting();
        }
    }

    public long getMsgInCounter() {
        return this.msgInCounter.longValue();
    }

    public long getBytesInCounter() {
        return this.bytesInCounter.longValue();
    }

    public long getMsgOutCounter() {
        return getStats(false, false).msgOutCounter;
    }

    public long getBytesOutCounter() {
        return getStats(false, false).bytesOutCounter;
    }

    public boolean isDeleteWhileInactive() {
        return this.inactiveTopicPolicies.isDeleteWhileInactive();
    }

    public boolean deletePartitionedTopicMetadataWhileInactive() {
        return brokerService.pulsar().getConfiguration().isBrokerDeleteInactivePartitionedTopicMetadataEnabled();
    }

    public void setDeleteWhileInactive(boolean deleteWhileInactive) {
        this.inactiveTopicPolicies.setDeleteWhileInactive(deleteWhileInactive);
    }

    protected abstract boolean isTerminated();

    private static final Logger log = LoggerFactory.getLogger(AbstractTopic.class);

    public InactiveTopicPolicies getInactiveTopicPolicies() {
        return inactiveTopicPolicies;
    }

    public void resetInactiveTopicPolicies(InactiveTopicDeleteMode inactiveTopicDeleteMode
            , int maxInactiveDurationSeconds, boolean deleteWhileInactive) {
        inactiveTopicPolicies.setInactiveTopicDeleteMode(inactiveTopicDeleteMode);
        inactiveTopicPolicies.setMaxInactiveDurationSeconds(maxInactiveDurationSeconds);
        inactiveTopicPolicies.setDeleteWhileInactive(deleteWhileInactive);
    }

    /**
     * Get {@link TopicPolicies} for this topic.
     * @return TopicPolicies, if they exist. Otherwise, the value will not be present.
     */
    public Optional<TopicPolicies> getTopicPolicies() {
        return brokerService.getTopicPolicies(TopicName.get(topic));
    }

    public CompletableFuture<Void> deleteTopicPolicies() {
        return brokerService.deleteTopicPolicies(TopicName.get(topic));
    }

    protected int getWaitingProducersCount() {
        return waitingExclusiveProducers.size();
    }

    protected boolean isExceedMaximumMessageSize(int size) {
        if (lastTopicMaxMessageSizeCheckTimeStamp + topicMaxMessageSizeCheckIntervalMs < System.currentTimeMillis()) {
            // refresh topicMaxMessageSize from topic policies
            topicMaxMessageSize = getTopicPolicies().map(TopicPolicies::getMaxMessageSize).orElse(0);
            lastTopicMaxMessageSizeCheckTimeStamp = System.currentTimeMillis();
        }

        if (topicMaxMessageSize == 0) {
            return false;
        }
        return size > topicMaxMessageSize;
    }

    /**
     * update topic publish dispatcher for this topic.
     */
    protected void updatePublishDispatcher(PublishRate publishRate) {
        if (publishRate != null && (publishRate.publishThrottlingRateInByte > 0
                || publishRate.publishThrottlingRateInMsg > 0)) {
            log.info("Enabling publish rate limiting {} ", publishRate);
            if (!preciseTopicPublishRateLimitingEnable) {
                this.brokerService.setupTopicPublishRateLimiterMonitor();
            }

            if (this.topicPublishRateLimiter == null
                    || this.topicPublishRateLimiter == PublishRateLimiter.DISABLED_RATE_LIMITER) {
                // create new rateLimiter if rate-limiter is disabled
                if (preciseTopicPublishRateLimitingEnable) {
                    this.topicPublishRateLimiter = new PrecisPublishLimiter(publishRate,
                            () -> this.enableCnxAutoRead(), brokerService.pulsar().getExecutor());
                } else {
                    this.topicPublishRateLimiter = new PublishRateLimiterImpl(publishRate);
                }
            } else {
                this.topicPublishRateLimiter.update(publishRate);
            }
        } else {
            log.info("Disabling publish throttling for {}", this.topic);
            this.topicPublishRateLimiter = PublishRateLimiter.DISABLED_RATE_LIMITER;
            enableProducerReadForPublishRateLimiting();
        }
    }
}
