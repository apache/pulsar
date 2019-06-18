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

import com.google.common.base.MoreObjects;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.service.schema.SchemaCompatibilityStrategy;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl.ENTRY_LATENCY_BUCKETS_USEC;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;

public abstract class AbstractTopic implements Topic {
    private static final Logger log = LoggerFactory.getLogger(AbstractTopic.class);

    protected static final long POLICY_UPDATE_FAILURE_RETRY_TIME_SECONDS = 60;

    protected final String topic;

    // Producers currently connected to this topic
    protected final ConcurrentOpenHashSet<Producer> producers;

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
    // schema validation enforced flag
    protected volatile boolean schemaValidationEnforced = false;

    public AbstractTopic(String topic, BrokerService brokerService) {
        this.topic = topic;
        this.brokerService = brokerService;
        this.producers = new ConcurrentOpenHashSet<>(16, 1);
        this.isFenced = false;
        this.replicatorPrefix = brokerService.pulsar().getConfiguration().getReplicatorPrefix();
        this.lastActive = System.nanoTime();
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
        if (maxProducers > 0 && maxProducers <= producers.size()) {
            return true;
        }
        return false;
    }

    protected boolean hasLocalProducers() {
        AtomicBoolean foundLocal = new AtomicBoolean(false);
        producers.forEach(producer -> {
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
    public ConcurrentOpenHashSet<Producer> getProducers() {
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
        return brokerService.pulsar()
                .getSchemaRegistryService()
                .putSchemaIfAbsent(id, schema, schemaCompatibilityStrategy);
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
    public CompletableFuture<Boolean> isSchemaCompatible(SchemaData schema) {
        String base = TopicName.get(getName()).getPartitionedTopicName();
        String id = TopicName.get(base).getSchemaName();
        return brokerService.pulsar()
                .getSchemaRegistryService()
                .isCompatible(id, schema, schemaCompatibilityStrategy);
    }

    @Override
    public void recordAddLatency(long latencyUSec) {
        addEntryLatencyStatsUsec.addValue(latencyUSec);
    }
}
