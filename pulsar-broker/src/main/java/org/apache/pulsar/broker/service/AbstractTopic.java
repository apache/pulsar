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
import static java.util.Objects.requireNonNull;
import static org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl.ENTRY_LATENCY_BUCKETS_USEC;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.ToLongFunction;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
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
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.HierarchyTopicPolicies;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTopic implements Topic, TopicPolicyListener<TopicPolicies> {

    protected static final long POLICY_UPDATE_FAILURE_RETRY_TIME_SECONDS = 60;

    protected final String topic;

    // Producers currently connected to this topic
    protected final ConcurrentHashMap<String, Producer> producers;

    protected final BrokerService brokerService;

    // Prefix for replication cursors
    protected final String replicatorPrefix;

    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    protected volatile boolean isFenced;

    protected final HierarchyTopicPolicies topicPolicies;

    // Timestamp of when this topic was last seen active
    protected volatile long lastActive;

    // Flag to signal that producer of this topic has published batch-message so, broker should not allow consumer which
    // doesn't support batch-message
    protected volatile boolean hasBatchMessagePublished = false;

    protected StatsBuckets addEntryLatencyStatsUsec = new StatsBuckets(ENTRY_LATENCY_BUCKETS_USEC);

    // Whether messages published must be encrypted or not in this topic
    protected volatile boolean isEncryptionRequired = false;

    protected volatile Boolean isAllowAutoUpdateSchema;
    // schema validation enforced flag
    protected volatile boolean schemaValidationEnforced = false;

    protected volatile PublishRateLimiter topicPublishRateLimiter;
    private final Object topicPublishRateLimiterLock = new Object();

    protected volatile ResourceGroupPublishLimiter resourceGroupPublishLimiter;

    protected boolean preciseTopicPublishRateLimitingEnable;

    @Getter
    protected boolean resourceGroupRateLimitingEnabled;

    private LongAdder bytesInCounter = new LongAdder();
    private LongAdder msgInCounter = new LongAdder();
    private final LongAdder filteredEntriesCounter = new LongAdder();

    private static final AtomicLongFieldUpdater<AbstractTopic> RATE_LIMITED_UPDATER =
            AtomicLongFieldUpdater.newUpdater(AbstractTopic.class, "publishRateLimitedTimes");
    protected volatile long publishRateLimitedTimes = 0L;

    private static final AtomicIntegerFieldUpdater<AbstractTopic> USER_CREATED_PRODUCER_COUNTER_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AbstractTopic.class, "userCreatedProducerCount");
    protected volatile int userCreatedProducerCount = 0;

    protected volatile Optional<Long> topicEpoch = Optional.empty();
    private volatile boolean hasExclusiveProducer;
    // pointer to the exclusive producer
    private volatile String exclusiveProducerName;

    private final Queue<Pair<Producer, CompletableFuture<Optional<Long>>>> waitingExclusiveProducers =
            new ConcurrentLinkedQueue<>();

    private static final AtomicLongFieldUpdater<AbstractTopic> USAGE_COUNT_UPDATER =
            AtomicLongFieldUpdater.newUpdater(AbstractTopic.class, "usageCount");
    private volatile long usageCount = 0;

    protected final LongAdder msgOutFromRemovedSubscriptions = new LongAdder();
    protected final LongAdder bytesOutFromRemovedSubscriptions = new LongAdder();

    public AbstractTopic(String topic, BrokerService brokerService) {
        this.topic = topic;
        this.brokerService = brokerService;
        this.producers = new ConcurrentHashMap<>();
        this.isFenced = false;
        ServiceConfiguration config = brokerService.pulsar().getConfiguration();
        this.replicatorPrefix = config.getReplicatorPrefix();

        topicPolicies = new HierarchyTopicPolicies();
        updateTopicPolicyByBrokerConfig();

        this.lastActive = System.nanoTime();
        this.preciseTopicPublishRateLimitingEnable = config.isPreciseTopicPublishRateLimiterEnable();
    }

    public SubscribeRate getSubscribeRate() {
        return this.topicPolicies.getSubscribeRate().get();
    }

    public DispatchRateImpl getSubscriptionDispatchRate() {
        return this.topicPolicies.getSubscriptionDispatchRate().get();
    }

    public SchemaCompatibilityStrategy getSchemaCompatibilityStrategy() {
        return this.topicPolicies.getSchemaCompatibilityStrategy().get();
    }

    public DispatchRateImpl getReplicatorDispatchRate() {
        return this.topicPolicies.getReplicatorDispatchRate().get();
    }

    public DispatchRateImpl getDispatchRate() {
        return this.topicPolicies.getDispatchRate().get();
    }

    private SchemaCompatibilityStrategy formatSchemaCompatibilityStrategy(SchemaCompatibilityStrategy strategy) {
        return strategy == SchemaCompatibilityStrategy.UNDEFINED ? null : strategy;
    }

    protected void updateTopicPolicy(TopicPolicies data) {
        if (!isSystemTopic()) {
            // Only use namespace level setting for system topic.
            topicPolicies.getReplicationClusters().updateTopicValue(data.getReplicationClusters());
            topicPolicies.getSchemaCompatibilityStrategy()
                    .updateTopicValue(formatSchemaCompatibilityStrategy(data.getSchemaCompatibilityStrategy()));
        }
        topicPolicies.getRetentionPolicies().updateTopicValue(data.getRetentionPolicies());
        topicPolicies.getMaxSubscriptionsPerTopic().updateTopicValue(data.getMaxSubscriptionsPerTopic());
        topicPolicies.getMaxUnackedMessagesOnConsumer().updateTopicValue(data.getMaxUnackedMessagesOnConsumer());
        topicPolicies.getMaxUnackedMessagesOnSubscription()
                .updateTopicValue(data.getMaxUnackedMessagesOnSubscription());
        topicPolicies.getMaxProducersPerTopic().updateTopicValue(data.getMaxProducerPerTopic());
        topicPolicies.getMaxConsumerPerTopic().updateTopicValue(data.getMaxConsumerPerTopic());
        topicPolicies.getMaxConsumersPerSubscription().updateTopicValue(data.getMaxConsumersPerSubscription());
        topicPolicies.getInactiveTopicPolicies().updateTopicValue(data.getInactiveTopicPolicies());
        topicPolicies.getDeduplicationEnabled().updateTopicValue(data.getDeduplicationEnabled());
        topicPolicies.getDeduplicationSnapshotIntervalSeconds().updateTopicValue(
                data.getDeduplicationSnapshotIntervalSeconds());
        topicPolicies.getSubscriptionTypesEnabled().updateTopicValue(
                CollectionUtils.isEmpty(data.getSubscriptionTypesEnabled()) ? null :
                        EnumSet.copyOf(data.getSubscriptionTypesEnabled()));
        Arrays.stream(BacklogQuota.BacklogQuotaType.values()).forEach(type ->
                this.topicPolicies.getBackLogQuotaMap().get(type).updateTopicValue(
                        data.getBackLogQuotaMap() == null ? null : data.getBackLogQuotaMap().get(type.toString())));
        topicPolicies.getTopicMaxMessageSize().updateTopicValue(data.getMaxMessageSize());
        topicPolicies.getMessageTTLInSeconds().updateTopicValue(data.getMessageTTLInSeconds());
        topicPolicies.getPublishRate().updateTopicValue(PublishRate.normalize(data.getPublishRate()));
        topicPolicies.getDelayedDeliveryEnabled().updateTopicValue(data.getDelayedDeliveryEnabled());
        topicPolicies.getReplicatorDispatchRate().updateTopicValue(normalize(data.getReplicatorDispatchRate()));
        topicPolicies.getDelayedDeliveryTickTimeMillis().updateTopicValue(data.getDelayedDeliveryTickTimeMillis());
        topicPolicies.getSubscribeRate().updateTopicValue(SubscribeRate.normalize(data.getSubscribeRate()));
        topicPolicies.getSubscriptionDispatchRate().updateTopicValue(normalize(data.getSubscriptionDispatchRate()));
        topicPolicies.getCompactionThreshold().updateTopicValue(data.getCompactionThreshold());
        topicPolicies.getDispatchRate().updateTopicValue(normalize(data.getDispatchRate()));
    }

    protected void updateTopicPolicyByNamespacePolicy(Policies namespacePolicies) {
        if (log.isDebugEnabled()) {
            log.debug("[{}]updateTopicPolicyByNamespacePolicy,data={}", topic, namespacePolicies);
        }
        if (namespacePolicies.deleted) {
            return;
        }
        topicPolicies.getRetentionPolicies().updateNamespaceValue(namespacePolicies.retention_policies);
        topicPolicies.getCompactionThreshold().updateNamespaceValue(namespacePolicies.compaction_threshold);
        topicPolicies.getReplicationClusters().updateNamespaceValue(
                Lists.newArrayList(CollectionUtils.emptyIfNull(namespacePolicies.replication_clusters)));
        topicPolicies.getMaxUnackedMessagesOnConsumer()
                .updateNamespaceValue(namespacePolicies.max_unacked_messages_per_consumer);
        topicPolicies.getMaxUnackedMessagesOnSubscription()
                .updateNamespaceValue(namespacePolicies.max_unacked_messages_per_subscription);
        topicPolicies.getMessageTTLInSeconds().updateNamespaceValue(namespacePolicies.message_ttl_in_seconds);
        topicPolicies.getMaxSubscriptionsPerTopic().updateNamespaceValue(namespacePolicies.max_subscriptions_per_topic);
        topicPolicies.getMaxProducersPerTopic().updateNamespaceValue(namespacePolicies.max_producers_per_topic);
        topicPolicies.getMaxConsumerPerTopic().updateNamespaceValue(namespacePolicies.max_consumers_per_topic);
        topicPolicies.getMaxConsumersPerSubscription()
                .updateNamespaceValue(namespacePolicies.max_consumers_per_subscription);
        topicPolicies.getInactiveTopicPolicies().updateNamespaceValue(namespacePolicies.inactive_topic_policies);
        topicPolicies.getDeduplicationEnabled().updateNamespaceValue(namespacePolicies.deduplicationEnabled);
        topicPolicies.getDeduplicationSnapshotIntervalSeconds().updateNamespaceValue(
                namespacePolicies.deduplicationSnapshotIntervalSeconds);
        updateNamespacePublishRate(namespacePolicies, brokerService.getPulsar().getConfig().getClusterName());
        topicPolicies.getDelayedDeliveryEnabled().updateNamespaceValue(
                Optional.ofNullable(namespacePolicies.delayed_delivery_policies)
                        .map(DelayedDeliveryPolicies::isActive).orElse(null));
        topicPolicies.getDelayedDeliveryTickTimeMillis().updateNamespaceValue(
                Optional.ofNullable(namespacePolicies.delayed_delivery_policies)
                        .map(DelayedDeliveryPolicies::getTickTime).orElse(null));
        topicPolicies.getSubscriptionTypesEnabled().updateNamespaceValue(
                subTypeStringsToEnumSet(namespacePolicies.subscription_types_enabled));
        updateNamespaceReplicatorDispatchRate(namespacePolicies,
            brokerService.getPulsar().getConfig().getClusterName());
        Arrays.stream(BacklogQuota.BacklogQuotaType.values()).forEach(
                type -> this.topicPolicies.getBackLogQuotaMap().get(type)
                        .updateNamespaceValue(MapUtils.getObject(namespacePolicies.backlog_quota_map, type)));
        updateNamespaceSubscribeRate(namespacePolicies, brokerService.getPulsar().getConfig().getClusterName());
        updateNamespaceSubscriptionDispatchRate(namespacePolicies,
            brokerService.getPulsar().getConfig().getClusterName());
        updateSchemaCompatibilityStrategyNamespaceValue(namespacePolicies);
        updateNamespaceDispatchRate(namespacePolicies, brokerService.getPulsar().getConfig().getClusterName());
    }

    private void updateNamespaceDispatchRate(Policies namespacePolicies, String cluster) {
        DispatchRateImpl dispatchRate = namespacePolicies.topicDispatchRate.get(cluster);
        if (dispatchRate == null) {
            dispatchRate = namespacePolicies.clusterDispatchRate.get(cluster);
        }
        topicPolicies.getDispatchRate().updateNamespaceValue(normalize(dispatchRate));
    }

    private void updateNamespaceSubscribeRate(Policies namespacePolicies, String cluster) {
        topicPolicies.getSubscribeRate()
            .updateNamespaceValue(SubscribeRate.normalize(namespacePolicies.clusterSubscribeRate.get(cluster)));
    }

    private void updateNamespaceSubscriptionDispatchRate(Policies namespacePolicies, String cluster) {
        topicPolicies.getSubscriptionDispatchRate()
            .updateNamespaceValue(normalize(namespacePolicies.subscriptionDispatchRate.get(cluster)));
    }

    private void updateNamespaceReplicatorDispatchRate(Policies namespacePolicies, String cluster) {
        topicPolicies.getReplicatorDispatchRate()
            .updateNamespaceValue(normalize(namespacePolicies.replicatorDispatchRate.get(cluster)));
    }

    private DispatchRateImpl normalize(DispatchRateImpl dispatchRate) {
        if (dispatchRate != null
            && (dispatchRate.getDispatchThrottlingRateInMsg() > 0
            || dispatchRate.getDispatchThrottlingRateInByte() > 0)) {
            return dispatchRate;
        } else {
            return null;
        }
    }

    private void updateSchemaCompatibilityStrategyNamespaceValue(Policies namespacePolicies){
        if (isSystemTopic()) {
            return;
        }

        SchemaCompatibilityStrategy strategy = namespacePolicies.schema_compatibility_strategy;
        if (SchemaCompatibilityStrategy.isUndefined(namespacePolicies.schema_compatibility_strategy)) {
            strategy = SchemaCompatibilityStrategy.fromAutoUpdatePolicy(
                    namespacePolicies.schema_auto_update_compatibility_strategy);
        }
        topicPolicies.getSchemaCompatibilityStrategy()
                .updateNamespaceValue(formatSchemaCompatibilityStrategy(strategy));
    }

    private void updateNamespacePublishRate(Policies namespacePolicies, String cluster) {
        topicPolicies.getPublishRate().updateNamespaceValue(
            PublishRate.normalize(
                namespacePolicies.publishMaxMessageRate != null
                    ? namespacePolicies.publishMaxMessageRate.get(cluster)
                    : null));
    }

    private void updateTopicPolicyByBrokerConfig() {
        ServiceConfiguration config = brokerService.pulsar().getConfiguration();
        topicPolicies.getInactiveTopicPolicies().updateBrokerValue(new InactiveTopicPolicies(
                config.getBrokerDeleteInactiveTopicsMode(),
                config.getBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds(),
                config.isBrokerDeleteInactiveTopicsEnabled()));

        updateBrokerSubscriptionTypesEnabled();
        topicPolicies.getMaxSubscriptionsPerTopic().updateBrokerValue(config.getMaxSubscriptionsPerTopic());
        topicPolicies.getMaxProducersPerTopic().updateBrokerValue(config.getMaxProducersPerTopic());
        topicPolicies.getMaxConsumerPerTopic().updateBrokerValue(config.getMaxConsumersPerTopic());
        topicPolicies.getMaxConsumersPerSubscription().updateBrokerValue(config.getMaxConsumersPerSubscription());
        topicPolicies.getDeduplicationEnabled().updateBrokerValue(config.isBrokerDeduplicationEnabled());
        topicPolicies.getRetentionPolicies().updateBrokerValue(new RetentionPolicies(
                config.getDefaultRetentionTimeInMinutes(), config.getDefaultRetentionSizeInMB()));
        topicPolicies.getDeduplicationSnapshotIntervalSeconds().updateBrokerValue(
                config.getBrokerDeduplicationSnapshotIntervalSeconds());
        topicPolicies.getMaxUnackedMessagesOnConsumer()
                .updateBrokerValue(config.getMaxUnackedMessagesPerConsumer());
        topicPolicies.getMaxUnackedMessagesOnSubscription()
                .updateBrokerValue(config.getMaxUnackedMessagesPerSubscription());
        //init backlogQuota
        topicPolicies.getBackLogQuotaMap()
                .get(BacklogQuota.BacklogQuotaType.destination_storage)
                .updateBrokerValue(brokerService.getBacklogQuotaManager().getDefaultQuota());
        topicPolicies.getBackLogQuotaMap()
                .get(BacklogQuota.BacklogQuotaType.message_age)
                .updateBrokerValue(brokerService.getBacklogQuotaManager().getDefaultQuota());

        topicPolicies.getTopicMaxMessageSize().updateBrokerValue(config.getMaxMessageSize());
        topicPolicies.getMessageTTLInSeconds().updateBrokerValue(config.getTtlDurationDefaultInSeconds());
        topicPolicies.getPublishRate().updateBrokerValue(publishRateInBroker(config));
        topicPolicies.getDelayedDeliveryEnabled().updateBrokerValue(config.isDelayedDeliveryEnabled());
        topicPolicies.getDelayedDeliveryTickTimeMillis().updateBrokerValue(config.getDelayedDeliveryTickTimeMillis());
        topicPolicies.getCompactionThreshold().updateBrokerValue(config.getBrokerServiceCompactionThresholdInBytes());
        topicPolicies.getReplicationClusters().updateBrokerValue(Collections.emptyList());
        SchemaCompatibilityStrategy schemaCompatibilityStrategy = config.getSchemaCompatibilityStrategy();
        topicPolicies.getReplicatorDispatchRate().updateBrokerValue(replicatorDispatchRateInBroker(config));
        if (isSystemTopic()) {
            schemaCompatibilityStrategy = config.getSystemTopicSchemaCompatibilityStrategy();
        }
        topicPolicies.getSubscribeRate().updateBrokerValue(subscribeRateInBroker(config));
        topicPolicies.getSubscriptionDispatchRate().updateBrokerValue(subscriptionDispatchRateInBroker(config));
        topicPolicies.getSchemaCompatibilityStrategy()
                .updateBrokerValue(formatSchemaCompatibilityStrategy(schemaCompatibilityStrategy));
        topicPolicies.getDispatchRate().updateBrokerValue(dispatchRateInBroker(config));
    }

    private DispatchRateImpl dispatchRateInBroker(ServiceConfiguration config) {
        return DispatchRateImpl.builder()
                .dispatchThrottlingRateInMsg(config.getDispatchThrottlingRatePerTopicInMsg())
                .dispatchThrottlingRateInByte(config.getDispatchThrottlingRatePerTopicInByte())
                .ratePeriodInSecond(1)
                .build();
    }

    private SubscribeRate subscribeRateInBroker(ServiceConfiguration config) {
        return new SubscribeRate(
            config.getSubscribeThrottlingRatePerConsumer(),
            config.getSubscribeRatePeriodPerConsumerInSecond()
        );
    }

    private DispatchRateImpl subscriptionDispatchRateInBroker(ServiceConfiguration config) {
        return DispatchRateImpl.builder()
            .dispatchThrottlingRateInMsg(config.getDispatchThrottlingRatePerSubscriptionInMsg())
            .dispatchThrottlingRateInByte(config.getDispatchThrottlingRatePerSubscriptionInByte())
            .ratePeriodInSecond(1)
            .build();
    }

    private DispatchRateImpl replicatorDispatchRateInBroker(ServiceConfiguration config) {
        return DispatchRateImpl.builder()
            .dispatchThrottlingRateInMsg(config.getDispatchThrottlingRatePerReplicatorInMsg())
            .dispatchThrottlingRateInByte(config.getDispatchThrottlingRatePerReplicatorInByte())
            .ratePeriodInSecond(1)
            .build();
    }

    private EnumSet<SubType> subTypeStringsToEnumSet(Set<String> getSubscriptionTypesEnabled) {
        EnumSet<SubType> subTypes = EnumSet.noneOf(SubType.class);
        for (String subTypeStr : CollectionUtils.emptyIfNull(getSubscriptionTypesEnabled)) {
            try {
                SubType subType = SubType.valueOf(subTypeStr);
                subTypes.add(subType);
            } catch (Throwable t) {
                //ignore invalid SubType strings.
            }
        }
        if (subTypes.isEmpty()) {
            return null;
        } else {
            return subTypes;
        }
    }

    private PublishRate publishRateInBroker(ServiceConfiguration config) {
        return new PublishRate(config.getMaxPublishRatePerTopicInMessages(), config.getMaxPublishRatePerTopicInBytes());
    }

    protected boolean isProducersExceeded(Producer producer) {
        if (isSystemTopic() || producer.isRemote()) {
            return false;
        }
        Integer maxProducers = topicPolicies.getMaxProducersPerTopic().get();
        return maxProducers != null && maxProducers > 0
                && maxProducers <= USER_CREATED_PRODUCER_COUNTER_UPDATER.get(this);
    }

    protected void registerTopicPolicyListener() {
        if (brokerService.pulsar().getConfig().isSystemTopicEnabled()
                && brokerService.pulsar().getConfig().isTopicLevelPoliciesEnabled()) {
            brokerService.getPulsar().getTopicPoliciesService()
                    .registerListener(TopicName.getPartitionedTopicName(topic), this);
        }
    }

    protected void unregisterTopicPolicyListener() {
        if (brokerService.pulsar().getConfig().isSystemTopicEnabled()
                && brokerService.pulsar().getConfig().isTopicLevelPoliciesEnabled()) {
            brokerService.getPulsar().getTopicPoliciesService()
                    .unregisterListener(TopicName.getPartitionedTopicName(topic), this);
        }
    }

    protected boolean isSameAddressProducersExceeded(Producer producer) {
        if (isSystemTopic() || producer.isRemote()) {
            return false;
        }
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
        if (isSystemTopic()) {
            return false;
        }
        Integer maxConsumersPerTopic = topicPolicies.getMaxConsumerPerTopic().get();
        if (maxConsumersPerTopic != null && maxConsumersPerTopic > 0
                && maxConsumersPerTopic <= getNumberOfConsumers()) {
            return true;
        }
        return false;
    }

    protected boolean isSameAddressConsumersExceededOnTopic(Consumer consumer) {
        if (isSystemTopic()) {
            return false;
        }
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

    protected Consumer getActiveConsumer(Subscription subscription) {
        Dispatcher dispatcher = subscription.getDispatcher();
        if (dispatcher instanceof AbstractDispatcherSingleActiveConsumer) {
            return ((AbstractDispatcherSingleActiveConsumer) dispatcher).getActiveConsumer();
        }
        return null;
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
        if (producers.isEmpty()) {
            return false;
        }
        for (Producer producer : producers.values()) {
            if (!producer.isRemote()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("topic", topic).toString();
    }

    @Override
    public Map<String, Producer> getProducers() {
        return producers;
    }


    @Override
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

        if (allowAutoUpdateSchema()) {
            return schemaRegistryService.putSchemaIfAbsent(id, schema, getSchemaCompatibilityStrategy());
        } else {
            return schemaRegistryService.trimDeletedSchemaAndGetList(id).thenCompose(schemaAndMetadataList ->
                    schemaRegistryService.getSchemaVersionBySchemaData(schemaAndMetadataList, schema)
                            .thenCompose(schemaVersion -> {
                                if (schemaVersion == null) {
                                    return FutureUtil.failedFuture(new IncompatibleSchemaException(
                                            "Schema not found and schema auto updating is disabled."));
                                } else {
                                    return CompletableFuture.completedFuture(schemaVersion);
                                }
                            }));
        }
    }

    private boolean allowAutoUpdateSchema() {
        if (brokerService.isSystemTopic(topic)) {
            return true;
        }
        if (isAllowAutoUpdateSchema == null) {
            return brokerService.pulsar().getConfig().isAllowAutoUpdateSchemaEnabled();
        }
        return isAllowAutoUpdateSchema;
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
                .checkConsumerCompatibility(id, schema, getSchemaCompatibilityStrategy());
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
                        return internalAddProducer(producer).thenApply(ignore -> {
                            USAGE_COUNT_UPDATER.incrementAndGet(this);
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] [{}] Added producer -- count: {}", topic, producer.getProducerName(),
                                        USAGE_COUNT_UPDATER.get(this));
                            }
                            return producerEpoch;
                        });
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

    @Override
    public long increasePublishLimitedTimes() {
        return RATE_LIMITED_UPDATER.incrementAndGet(this);
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

    public void updateDispatchRateLimiter() {
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

    protected CompletableFuture<Void> internalAddProducer(Producer producer) {
        if (isProducersExceeded(producer)) {
            log.warn("[{}] Attempting to add producer to topic which reached max producers limit", topic);
            CompletableFuture<Void> res = new CompletableFuture<>();
            res.completeExceptionally(
                    new ProducerBusyException("Topic reached max producers limit"));
            return res;
        }

        if (isSameAddressProducersExceeded(producer)) {
            log.warn("[{}] Attempting to add producer to topic which reached max same address producers limit", topic);
            CompletableFuture<Void> res = new CompletableFuture<>();
            res.completeExceptionally(
                    new ProducerBusyException("Topic reached max same address producers limit"));
            return res;
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] {} Got request to create producer ", topic, producer.getProducerName());
        }

        Producer existProducer = producers.putIfAbsent(producer.getProducerName(), producer);
        if (existProducer != null) {
            return tryOverwriteOldProducer(existProducer, producer);
        } else if (!producer.isRemote()) {
            USER_CREATED_PRODUCER_COUNTER_UPDATER.incrementAndGet(this);
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> tryOverwriteOldProducer(Producer oldProducer, Producer newProducer) {
        if (newProducer.isSuccessorTo(oldProducer)) {
            oldProducer.close(false);
            if (!producers.replace(newProducer.getProducerName(), oldProducer, newProducer)) {
                // Met concurrent update, throw exception here so that client can try reconnect later.
                CompletableFuture<Void> res = new CompletableFuture<>();
                res.completeExceptionally(new BrokerServiceException.NamingException("Producer with name '"
                        + newProducer.getProducerName() + "' replace concurrency error"));
                return res;
            } else {
                handleProducerRemoved(oldProducer);
                return CompletableFuture.completedFuture(null);
            }
        } else {
            // If a producer with the same name tries to use a new connection, async check the old connection is
            // available. The producers related the connection that not available are automatically cleaned up.
            if (!Objects.equals(oldProducer.getCnx(), newProducer.getCnx())) {
                return oldProducer.getCnx().checkConnectionLiveness().thenCompose(previousIsActive -> {
                    if (previousIsActive) {
                        CompletableFuture<Void> res = new CompletableFuture<>();
                        res.completeExceptionally(new BrokerServiceException.NamingException(
                                "Producer with name '" + newProducer.getProducerName()
                                        + "' is already connected to topic"));
                        return res;
                    } else {
                        // If the connection of the previous producer is not active, the method
                        // "cnx().checkConnectionLiveness()" will trigger the close for it and kick off the previous
                        // producer. So try to add current producer again.
                        // The recursive call will be stopped by these two case(This prevents infinite call):
                        //   1. add current producer success.
                        //   2. once another same name producer registered.
                        return internalAddProducer(newProducer);
                    }
                });
            }
            CompletableFuture<Void> res = new CompletableFuture<>();
            res.completeExceptionally(new BrokerServiceException.NamingException(
                    "Producer with name '" + newProducer.getProducerName() + "' is already connected to topic"));
            return res;
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
            if (!producer.isRemote()) {
                USER_CREATED_PRODUCER_COUNTER_UPDATER.decrementAndGet(this);
            }
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

    /**
     * @deprecated Avoid using the deprecated method
     * #{@link org.apache.pulsar.broker.resources.NamespaceResources#getPoliciesIfCached(NamespaceName)} and we can use
     * #{@link AbstractTopic#updateResourceGroupLimiter(Policies)} to instead of it.
     */
    @Deprecated
    public void updateResourceGroupLimiter(Optional<Policies> optPolicies) {
        Policies policies;
        try {
            policies = optPolicies.orElseGet(() ->
                brokerService.pulsar()
                    .getPulsarResources()
                    .getNamespaceResources()
                    .getPoliciesIfCached(TopicName.get(topic).getNamespaceObject())
                    .orElseGet(Policies::new));
        } catch (Exception e) {
            log.warn("[{}] Error getting policies {} and publish throttling will be disabled", topic, e.getMessage());
            policies = new Policies();
        }
        updateResourceGroupLimiter(policies);
    }

    public void updateResourceGroupLimiter(@Nonnull Policies namespacePolicies) {
        requireNonNull(namespacePolicies);
        // attach the resource-group level rate limiters, if set
        String rgName = namespacePolicies.resource_group_name;
        if (rgName != null) {
            final ResourceGroup resourceGroup =
                    brokerService.getPulsar().getResourceGroupServiceManager().resourceGroupGet(rgName);
            if (resourceGroup != null) {
                this.resourceGroupRateLimitingEnabled = true;
                this.resourceGroupPublishLimiter = resourceGroup.getResourceGroupPublishLimiter();
                this.resourceGroupPublishLimiter.registerRateLimitFunction(this.getName(), this::enableCnxAutoRead);
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
        return msgOutFromRemovedSubscriptions.longValue()
                + sumSubscriptions(AbstractSubscription::getMsgOutCounter);
    }

    public long getBytesOutCounter() {
        return bytesOutFromRemovedSubscriptions.longValue()
                + sumSubscriptions(AbstractSubscription::getBytesOutCounter);
    }

    private long sumSubscriptions(ToLongFunction<AbstractSubscription> toCounter) {
        return getSubscriptions().values().stream()
                .map(AbstractSubscription.class::cast)
                .mapToLong(toCounter)
                .sum();
    }

    public boolean isDeleteWhileInactive() {
        return topicPolicies.getInactiveTopicPolicies().get().isDeleteWhileInactive();
    }

    public boolean deletePartitionedTopicMetadataWhileInactive() {
        return brokerService.pulsar().getConfiguration().isBrokerDeleteInactivePartitionedTopicMetadataEnabled();
    }

    protected abstract boolean isTerminated();

    private static final Logger log = LoggerFactory.getLogger(AbstractTopic.class);

    public InactiveTopicPolicies getInactiveTopicPolicies() {
        return topicPolicies.getInactiveTopicPolicies().get();
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

    protected boolean isExceedMaximumMessageSize(int size, PublishContext publishContext) {
        if (publishContext.isChunked()) {
            //skip topic level max message check if it's chunk message.
            return false;
        }
        int topicMaxMessageSize = topicPolicies.getTopicMaxMessageSize().get();
        if (topicMaxMessageSize <= 0) {
            //invalid setting means this check is disabled.
            return false;
        }
        if (topicMaxMessageSize >= brokerService.pulsar().getConfiguration().getMaxMessageSize()) {
            //broker setting does not contain message header and already handled in client and frameDecoder.
            return false;
        }
        return size > topicMaxMessageSize;
    }

    /**
     * update topic publish dispatcher for this topic.
     */
    public void updatePublishRateLimiter() {
        synchronized (topicPublishRateLimiterLock) {
            PublishRate publishRate = topicPolicies.getPublishRate().get();
            if (publishRate.publishThrottlingRateInByte > 0 || publishRate.publishThrottlingRateInMsg > 0) {
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
                if (topicPublishRateLimiter != null) {
                    topicPublishRateLimiter.close();
                }
                this.topicPublishRateLimiter = PublishRateLimiter.DISABLED_RATE_LIMITER;
                enableProducerReadForPublishRateLimiting();
            }
        }
    }

    // subscriptionTypesEnabled is dynamic and can be updated online.
    public void updateBrokerSubscriptionTypesEnabled() {
        topicPolicies.getSubscriptionTypesEnabled().updateBrokerValue(
            subTypeStringsToEnumSet(brokerService.pulsar().getConfiguration().getSubscriptionTypesEnabled()));
    }

    @Override
    public HierarchyTopicPolicies getHierarchyTopicPolicies() {
        return topicPolicies;
    }

    public void updateBrokerSubscriptionDispatchRate() {
                topicPolicies.getSubscriptionDispatchRate().updateBrokerValue(
                    subscriptionDispatchRateInBroker(brokerService.pulsar().getConfiguration()));
    }

    public void addFilteredEntriesCount(int filtered) {
        this.filteredEntriesCounter.add(filtered);
    }

    public long getFilteredEntriesCount() {
        return this.filteredEntriesCounter.longValue();
    }

    public void updateBrokerReplicatorDispatchRate() {
        topicPolicies.getReplicatorDispatchRate().updateBrokerValue(
            replicatorDispatchRateInBroker(brokerService.pulsar().getConfiguration()));
    }

    public void updateBrokerDispatchRate() {
        topicPolicies.getDispatchRate().updateBrokerValue(
            dispatchRateInBroker(brokerService.pulsar().getConfiguration()));
    }

    public void updateBrokerPublishRate() {
        topicPolicies.getPublishRate().updateBrokerValue(
            publishRateInBroker(brokerService.pulsar().getConfiguration()));
    }

    public void updateBrokerSubscribeRate() {
        topicPolicies.getSubscribeRate().updateBrokerValue(
            subscribeRateInBroker(brokerService.pulsar().getConfiguration()));
    }
}
