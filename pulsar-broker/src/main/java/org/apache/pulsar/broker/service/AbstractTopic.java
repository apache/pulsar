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

import static org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl.ENTRY_LATENCY_BUCKETS_USEC;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;

import com.google.common.base.MoreObjects;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Getter;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerBusyException;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.broker.stats.prometheus.metrics.Summary;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
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

    @Getter
    protected volatile SchemaCompatibilityStrategy schemaCompatibilityStrategy =
            SchemaCompatibilityStrategy.FULL;
    protected volatile boolean isAllowAutoUpdateSchema = true;
    // schema validation enforced flag
    protected volatile boolean schemaValidationEnforced = false;

    protected volatile int maxUnackedMessagesOnConsumer = -1;

    protected volatile Integer maxSubscriptionsPerTopic = null;

    protected volatile PublishRateLimiter topicPublishRateLimiter;

    protected boolean preciseTopicPublishRateLimitingEnable;

    private LongAdder bytesInCounter = new LongAdder();
    private LongAdder msgInCounter = new LongAdder();

    protected CompletableFuture<TransactionBuffer> transactionBuffer;
    protected ReentrantLock transactionBufferLock = new ReentrantLock();

    public AbstractTopic(String topic, BrokerService brokerService) {
        this.topic = topic;
        this.brokerService = brokerService;
        this.producers = new ConcurrentHashMap<>();
        this.isFenced = false;
        this.replicatorPrefix = brokerService.pulsar().getConfiguration().getReplicatorPrefix();
        this.inactiveTopicPolicies.setDeleteWhileInactive(brokerService.pulsar().getConfiguration().isBrokerDeleteInactiveTopicsEnabled());
        this.inactiveTopicPolicies.setMaxInactiveDurationSeconds(brokerService.pulsar().getConfiguration().getBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds());
        this.inactiveTopicPolicies.setInactiveTopicDeleteMode(brokerService.pulsar().getConfiguration().getBrokerDeleteInactiveTopicsMode());
        this.lastActive = System.nanoTime();
        Policies policies;
        try {
            policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                    .getDataIfPresent(AdminResource.path(POLICIES, TopicName.get(topic).getNamespace()));
        } catch (Exception e) {
            log.warn("[{}] Error getting policies {} and publish throttling will be disabled", topic, e.getMessage());
            policies = new Policies();
        }
        this.preciseTopicPublishRateLimitingEnable =
                brokerService.pulsar().getConfiguration().isPreciseTopicPublishRateLimiterEnable();
        updatePublishDispatcher(policies);
    }

    protected boolean isProducersExceeded() {
        Integer maxProducers = null;
        TopicPolicies topicPolicies = getTopicPolicies(TopicName.get(topic));
        if (topicPolicies != null) {
            maxProducers = topicPolicies.getMaxProducerPerTopic();
        }

        if (maxProducers == null) {
            Policies policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                        .getDataIfPresent(AdminResource.path(POLICIES, TopicName.get(topic).getNamespace()));
            if (policies != null) {
                maxProducers = policies.max_producers_per_topic;
            }
        }
        maxProducers = maxProducers != null ? maxProducers : brokerService.pulsar()
                .getConfiguration().getMaxProducersPerTopic();
        if (maxProducers > 0 && maxProducers <= producers.size()) {
            return true;
        }
        return false;
    }

    protected boolean isConsumersExceededOnTopic() {
        Integer maxConsumers = null;
        TopicPolicies topicPolicies = getTopicPolicies(TopicName.get(topic));
        if (topicPolicies != null) {
            maxConsumers = topicPolicies.getMaxConsumerPerTopic();
        }
        if (maxConsumers == null) {
            // Use getDataIfPresent from zk cache to make the call non-blocking and prevent deadlocks
            Policies policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                    .getDataIfPresent(AdminResource.path(POLICIES, TopicName.get(topic).getNamespace()));
            maxConsumers = policies != null ? policies.max_consumers_per_topic : 0;
        }

        final int maxConsumersPerTopic = maxConsumers > 0 ? maxConsumers
                : brokerService.pulsar().getConfiguration().getMaxConsumersPerTopic();
        if (maxConsumersPerTopic > 0 && maxConsumersPerTopic <= getNumberOfConsumers()) {
            return true;
        }
        return false;
    }

    public abstract int getNumberOfConsumers();

    protected void addConsumerToSubscription(Subscription subscription, Consumer consumer)
            throws BrokerServiceException {
        if (isConsumersExceededOnTopic()) {
            log.warn("[{}] Attempting to add consumer to topic which reached max consumers limit", topic);
            throw new ConsumerBusyException("Topic reached max consumers limit");
        }
        subscription.addConsumer(consumer);
    }

    @Override
    public void disableCnxAutoRead() {
        if (producers != null) {
            producers.values().forEach(producer -> producer.getCnx().disableCnxAutoRead());
        }
    }

    @Override
    public void enableCnxAutoRead() {
        if (producers != null) {
            producers.values().forEach(producer -> producer.getCnx().enableCnxAutoRead());
        }
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
        return allowAutoUpdateSchema() ? schemaRegistryService
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
        return schemaRegistryService.getSchema(id)
                .thenCompose(schema -> {
                    if (schema != null) {
                        // It's different from `SchemasResource.deleteSchema` because when we delete a topic, the schema
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
    public void recordAddLatency(long latency, TimeUnit unit) {
        addEntryLatencyStatsUsec.addValue(unit.toMicros(latency));

        PUBLISH_LATENCY.observe(latency, unit);
    }

    protected void setSchemaCompatibilityStrategy(Policies policies) {
        if (SystemTopicClient.isSystemTopic(TopicName.get(this.topic))) {
            schemaCompatibilityStrategy =
                    brokerService.pulsar().getConfig().getSystemTopicSchemaCompatibilityStrategy();
        } else if (policies.schema_compatibility_strategy == SchemaCompatibilityStrategy.UNDEFINED) {
            schemaCompatibilityStrategy = SchemaCompatibilityStrategy.fromAutoUpdatePolicy(
                    policies.schema_auto_update_compatibility_strategy);
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
     * it sets cnx auto-readable if producer's cnx is disabled due to publish-throttling
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
        if (newProducer.isSuccessorTo(oldProducer) && !isUserProvidedProducerName(oldProducer)
                && !isUserProvidedProducerName(newProducer)) {
            oldProducer.close(false);
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

    protected abstract void handleProducerRemoved(Producer producer);

    @Override
    public boolean isPublishRateExceeded() {
        // either topic or broker publish rate exceeded.
        return this.topicPublishRateLimiter.isPublishRateExceeded() ||
            getBrokerPublishRateLimiter().isPublishRateExceeded();
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
        updatePublishDispatcher(policies);
    }

    private void updatePublishDispatcher(Policies policies) {
        //if topic-level policy exists, try to use topic-level publish rate policy
        TopicPolicies topicPolicies = getTopicPolicies(TopicName.get(topic));
        if (topicPolicies != null && topicPolicies.isPublishRateSet()) {
            log.info("Using topic policy publish rate instead of namespace level topic publish rate on topic {}", this.topic);
            updatePublishDispatcher(topicPolicies.getPublishRate());
            return;
        }

        //topic-level policy is not set, try to use namespace-level rate policy
        final String clusterName = brokerService.pulsar().getConfiguration().getClusterName();
        final PublishRate publishRate = policies != null && policies.publishMaxMessageRate != null
                ? policies.publishMaxMessageRate.get(clusterName)
                : null;

        //both namespace-level and topic-level policy are not set, try to use broker-level policy
        ServiceConfiguration serviceConfiguration = brokerService.pulsar().getConfiguration();
        if (publishRate == null) {
            PublishRate brokerPublishRate = new PublishRate(serviceConfiguration.getMaxPublishRatePerTopicInMessages()
                    , serviceConfiguration.getMaxPublishRatePerTopicInBytes());
            updatePublishDispatcher(brokerPublishRate);
            return;
        }
        //publishRate is not null , use namespace-level policy
        updatePublishDispatcher(publishRate);
    }

    public long getMsgInCounter() { return this.msgInCounter.longValue(); }

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
     * @param topicName
     * @return TopicPolicies is exist else return null.
     */
    public TopicPolicies getTopicPolicies(TopicName topicName) {
        try {
            return brokerService.pulsar().getTopicPoliciesService().getTopicPolicies(topicName);
        } catch (BrokerServiceException.TopicPoliciesCacheNotInitException e) {
            log.debug("Topic {} policies cache have not init.", topicName.getPartitionedTopicName());
            return null;
        } catch (NullPointerException e) {
            log.warn("Topic level policies are not enabled. " +
                    "Please refer to systemTopicEnabled and topicLevelPoliciesEnabled on broker.conf");
            return null;
        }
    }

    protected boolean isExceedMaximumMessageSize(int size) {
        Integer maxMessageSize = null;
        TopicPolicies topicPolicies = getTopicPolicies(TopicName.get(topic));
        if (topicPolicies != null && topicPolicies.isMaxMessageSizeSet()) {
            maxMessageSize = topicPolicies.getMaxMessageSize();
        }
        if (maxMessageSize != null) {
            if (maxMessageSize == 0) {
                return false;
            }
            return size > maxMessageSize;
        }
        return false;
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

    private boolean allowAutoUpdateSchema() {
        if (brokerService.isSystemTopic(topic)) {
            return true;
        }
        return isAllowAutoUpdateSchema;
    }
}
