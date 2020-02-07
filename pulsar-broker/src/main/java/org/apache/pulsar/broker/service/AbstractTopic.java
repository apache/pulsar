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
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;

import com.google.common.base.MoreObjects;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import com.google.common.collect.Sets;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.broker.stats.prometheus.metrics.Summary;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTopic implements Topic {

    protected static final long POLICY_UPDATE_FAILURE_RETRY_TIME_SECONDS = 60;

    protected final String topic;

    // Producers currently connected to this topic
    protected final ConcurrentHashMap<String, Set<Producer>> producerGroups;

    protected final BrokerService brokerService;

    // Prefix for replication cursors
    protected final String replicatorPrefix;

    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    protected volatile boolean isFenced;

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

    protected volatile PublishRateLimiter topicPublishRateLimiter;

    private LongAdder bytesInCounter = new LongAdder();
    private LongAdder msgInCounter = new LongAdder();

    public AbstractTopic(String topic, BrokerService brokerService) {
        this.topic = topic;
        this.brokerService = brokerService;
        this.producerGroups = new ConcurrentHashMap<>();
        this.isFenced = false;
        this.replicatorPrefix = brokerService.pulsar().getConfiguration().getReplicatorPrefix();
        this.lastActive = System.nanoTime();
        Policies policies = null;
        try {
            policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                    .getDataIfPresent(AdminResource.path(POLICIES, TopicName.get(topic).getNamespace()));
            if (policies == null) {
                policies = new Policies();
            }
        } catch (Exception e) {
            log.warn("[{}] Error getting policies {} and publish throttling will be disabled", topic, e.getMessage());
        }
        updatePublishDispatcher(policies);
    }

    protected boolean isProducersExceeded() {
        Policies policies;
        try {
            policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                    .get(AdminResource.path(POLICIES, TopicName.get(topic).getNamespace()))
                    .orElseGet(() -> new Policies());
        } catch (Exception e) {
            policies = new Policies();
        }
        final int maxProducers = policies.max_producers_per_topic > 0 ?
                policies.max_producers_per_topic :
                brokerService.pulsar().getConfiguration().getMaxProducersPerTopic();
        return maxProducers > 0 && maxProducers <= getProducers().count();
    }

    @Override
    public void removeProducer(Producer producer) {
        checkArgument(producer.getTopic() == this);

        boolean[] removed = { false };
        producerGroups.computeIfPresent(producer.getProducerName(), (s, producerSet) -> {
            if (producerSet instanceof HashSet) { // a non-exclusive producer group
                if (producerSet.remove(producer)) {
                    removed[0] = true;
                    if (producerSet.size() == 0)
                        return null;
                }
                return producerSet;
            } else { // an exclusive producer "group"
                if (producerSet.contains(producer)) {
                    removed[0] = true;
                    return null;
                } else {
                    return producerSet;
                }
            }
        });
        if (removed[0]) {
            handleProducerRemoved(producer);
        }
    }

    protected boolean hasLocalProducers() {
        return getProducers().anyMatch(producer -> !producer.isRemote());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("topic", topic).toString();
    }

    @Override
    public Stream<Producer> getProducers() {
        return producerGroups.values().stream().flatMap(Collection::stream);
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
        return schemaRegistryService.getSchema(id)
                .thenCompose(schema -> {
                    if (schema != null) {
                        return schemaRegistryService.deleteSchema(id, "");
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

    protected void setSchemaCompatibilityStrategy (Policies policies) {
        if (policies.schema_compatibility_strategy == SchemaCompatibilityStrategy.UNDEFINED) {
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
            enableProducerRead();
        }
    }

    @Override
    public void resetBrokerPublishCountAndEnableReadIfRequired(boolean doneBrokerReset) {
        // topic rate not exceeded, and completed broker limiter reset.
        if (!topicPublishRateLimiter.isPublishRateExceeded() && doneBrokerReset) {
            enableProducerRead();
        }
    }

    /**
     * it sets cnx auto-readable if producer's cnx is disabled due to publish-throttling
     */
    protected void enableProducerRead() {
        if (producerGroups != null) {
            getProducers().forEach(producer -> producer.getCnx().enableCnxAutoRead());
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

        // the following the variables are used to get state out the compute function
        // (we want to get out of producers.compute as quickly as possible so as not to block other concurrent actions)
        BrokerServiceException[] bse = {null};
        Collection<Producer> oldProducers = new LinkedList<>();
        boolean parallelGroupMode = producer.getGroupMode() == PulsarApi.CommandProducer.GroupMode.Parallel;
        producerGroups.compute(producer.getProducerName(), (s, producerSet) -> {
            if (producerSet == null) { // no producer under that name and topic connected yet
                if (parallelGroupMode) {
                    Set<Producer> identityHashSet = Sets.newIdentityHashSet();
                    identityHashSet.add(producer);
                    return identityHashSet;
                } else {
                   return Collections.singleton(producer); // in reality a "set" that can contain only one element is enough here
                }
            } else {
                Producer existingProducer = producerSet.iterator().next();
                boolean existingProducerIsExclusive =
                        existingProducer.getGroupMode() == PulsarApi.CommandProducer.GroupMode.Exclusive;
                if (existingProducerIsExclusive) { // an exclusive producer is already connected under that producerName
                    if (parallelGroupMode) {
                        bse[0] = new BrokerServiceException.NamingException(
                                "Exclusive Producer with name '" + producer.getProducerName() + "' is already connected to topic");
                        return producerSet;
                    } else {
                        if (!isUserProvidedProducerName(existingProducer) && !isUserProvidedProducerName(producer)
                                && producer.getEpoch() > existingProducer.getEpoch()) {
                            oldProducers.add(existingProducer);
                            return Collections.singleton(producer);
                        } else {
                            bse[0] = new BrokerServiceException.NamingException(
                                    "Producer with name '" + producer.getProducerName() + "' is already connected to topic");
                            return producerSet;
                        }
                    }
                } else { // a non-exclusive producer is already connected under that producerName
                    if (parallelGroupMode) {
                        if (!producerSet.add(producer)) {
                            bse[0] = new BrokerServiceException.NamingException(
                                    "Non-exclusive producer with name '" + producer.getProducerName() + "' and address '" +
                                            producer.getCnx().clientAddress() + "' is already connected to topic");
                        }
                        return producerSet;
                    } else {
                        oldProducers.addAll(producerSet);
                        return Collections.singleton(producer);
                    }
                }
            }
        });
        for (Producer oldProducer : oldProducers) {
            oldProducer.close(false);
            handleProducerRemoved(oldProducer);
        }
        if (bse[0] != null)
            throw bse[0];
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
        final String clusterName = brokerService.pulsar().getConfiguration().getClusterName();
        final PublishRate publishRate = policies != null && policies.publishMaxMessageRate != null
                ? policies.publishMaxMessageRate.get(clusterName)
                : null;
        if (publishRate != null
                && (publishRate.publishThrottlingRateInByte > 0 || publishRate.publishThrottlingRateInMsg > 0)) {
            log.info("Enabling publish rate limiting {} on topic {}", publishRate, this.topic);
            // lazy init Publish-rateLimiting monitoring if not initialized yet
            this.brokerService.setupTopicPublishRateLimiterMonitor();
            if (this.topicPublishRateLimiter == null
                    || this.topicPublishRateLimiter == PublishRateLimiter.DISABLED_RATE_LIMITER) {
                // create new rateLimiter if rate-limiter is disabled
                this.topicPublishRateLimiter = new PublishRateLimiterImpl(policies, clusterName);
            } else {
                this.topicPublishRateLimiter.update(policies, clusterName);
            }
        } else {
            log.info("Disabling publish throttling for {}", this.topic);
            this.topicPublishRateLimiter = PublishRateLimiter.DISABLED_RATE_LIMITER;
            enableProducerRead();
        }
    }

    public long getMsgInCounter() { return this.msgInCounter.longValue(); }

    public long getBytesInCounter() {
        return this.bytesInCounter.longValue();
    }

    private static final Logger log = LoggerFactory.getLogger(AbstractTopic.class);
}
