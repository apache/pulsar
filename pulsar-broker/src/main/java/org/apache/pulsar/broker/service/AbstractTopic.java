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
package org.apache.pulsar.broker.service;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl.ENTRY_LATENCY_BUCKETS_USEC;
import static org.apache.pulsar.compaction.Compactor.COMPACTION_SUBSCRIPTION;
import com.google.common.base.MoreObjects;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup;
import org.apache.pulsar.broker.resourcegroup.ResourceGroupPublishLimiter;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.ProducerBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.ProducerFencedException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicMigratedException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicTerminatedException;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.broker.service.schema.exceptions.SchemaException;
import org.apache.pulsar.broker.stats.prometheus.metrics.Summary;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.ClusterPolicies.ClusterUrl;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.EntryFilters;
import org.apache.pulsar.common.policies.data.HierarchyTopicPolicies;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.SubscriptionPolicies;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTopic implements Topic, TopicPolicyListener {

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

    protected volatile PublishRateLimiter topicPublishRateLimiter;
    protected volatile ResourceGroupPublishLimiter resourceGroupPublishLimiter;

    protected boolean preciseTopicPublishRateLimitingEnable;

    @Getter
    protected boolean resourceGroupRateLimitingEnabled;

    private LongAdder bytesInCounter = new LongAdder();
    private LongAdder msgInCounter = new LongAdder();
    private LongAdder systemTopicBytesInCounter = new LongAdder();
    private final LongAdder filteredEntriesCounter = new LongAdder();

    private static final AtomicLongFieldUpdater<AbstractTopic> RATE_LIMITED_UPDATER =
            AtomicLongFieldUpdater.newUpdater(AbstractTopic.class, "publishRateLimitedTimes");
    protected volatile long publishRateLimitedTimes = 0L;
    private static final AtomicLongFieldUpdater<AbstractTopic> TOTAL_RATE_LIMITED_UPDATER =
            AtomicLongFieldUpdater.newUpdater(AbstractTopic.class, "totalPublishRateLimitedCounter");
    protected volatile long totalPublishRateLimitedCounter = 0L;

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

    private Map<String/*subscription*/, SubscriptionPolicies> subscriptionPolicies = Collections.emptyMap();

    protected final LongAdder msgOutFromRemovedSubscriptions = new LongAdder();
    protected final LongAdder bytesOutFromRemovedSubscriptions = new LongAdder();
    protected final LongAdder bytesOutFromRemovedSystemSubscriptions = new LongAdder();
    protected volatile Pair<String, List<EntryFilter>> entryFilters;
    protected volatile boolean transferring = false;
    private volatile List<PublishRateLimiter> activeRateLimiters;
    protected final Clock clock;

    protected Set<String> additionalSystemCursorNames = new TreeSet<>();

    public AbstractTopic(String topic, BrokerService brokerService) {
        this.topic = topic;
        this.clock = brokerService.getClock();
        this.brokerService = brokerService;
        this.producers = new ConcurrentHashMap<>();
        this.isFenced = false;
        ServiceConfiguration config = brokerService.pulsar().getConfiguration();
        this.replicatorPrefix = config.getReplicatorPrefix();

        topicPolicies = new HierarchyTopicPolicies();
        updateTopicPolicyByBrokerConfig();

        this.lastActive = System.nanoTime();
        this.preciseTopicPublishRateLimitingEnable = config.isPreciseTopicPublishRateLimiterEnable();
        topicPublishRateLimiter = new PublishRateLimiterImpl(brokerService.getPulsar().getMonotonicSnapshotClock());
        updateActiveRateLimiters();

        additionalSystemCursorNames = brokerService.pulsar().getConfiguration().getAdditionalSystemCursorNames();
    }

    public SubscribeRate getSubscribeRate() {
        return this.topicPolicies.getSubscribeRate().get();
    }

    public DispatchRateImpl getSubscriptionDispatchRate(String subscriptionName) {
        return Optional.ofNullable(subscriptionPolicies.get(subscriptionName))
                .map(SubscriptionPolicies::getDispatchRate)
                .map(DispatchRateImpl::normalize)
                .orElse(this.topicPolicies.getSubscriptionDispatchRate().get());
    }

    public SchemaCompatibilityStrategy getSchemaCompatibilityStrategy() {
        return this.topicPolicies.getSchemaCompatibilityStrategy().get();
    }

    public DispatchRateImpl getDispatchRate() {
        return this.topicPolicies.getDispatchRate().get();
    }

    public EntryFilters getEntryFiltersPolicy() {
        return this.topicPolicies.getEntryFilters().get();
    }

    public List<EntryFilter> getEntryFilters() {
        return this.entryFilters.getRight();
    }

    public DispatchRateImpl getReplicatorDispatchRate() {
        return this.topicPolicies.getReplicatorDispatchRate().get();
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
        topicPolicies.getDispatcherPauseOnAckStatePersistentEnabled()
                .updateTopicValue(data.getDispatcherPauseOnAckStatePersistentEnabled());
        topicPolicies.getMaxSubscriptionsPerTopic()
                .updateTopicValue(normalizeValue(data.getMaxSubscriptionsPerTopic()));
        topicPolicies.getMaxUnackedMessagesOnConsumer()
                .updateTopicValue(normalizeValue(data.getMaxUnackedMessagesOnConsumer()));
        topicPolicies.getMaxUnackedMessagesOnSubscription()
                .updateTopicValue(normalizeValue(data.getMaxUnackedMessagesOnSubscription()));
        topicPolicies.getMaxProducersPerTopic().updateTopicValue(normalizeValue(data.getMaxProducerPerTopic()));
        topicPolicies.getMaxConsumerPerTopic().updateTopicValue(normalizeValue(data.getMaxConsumerPerTopic()));
        topicPolicies.getMaxConsumersPerSubscription()
                .updateTopicValue(normalizeValue(data.getMaxConsumersPerSubscription()));
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
        topicPolicies.getTopicMaxMessageSize().updateTopicValue(normalizeValue(data.getMaxMessageSize()));
        topicPolicies.getMessageTTLInSeconds().updateTopicValue(normalizeValue(data.getMessageTTLInSeconds()));
        topicPolicies.getPublishRate().updateTopicValue(PublishRate.normalize(data.getPublishRate()));
        topicPolicies.getDelayedDeliveryEnabled().updateTopicValue(data.getDelayedDeliveryEnabled());
        topicPolicies.getReplicatorDispatchRate().updateTopicValue(
            DispatchRateImpl.normalize(data.getReplicatorDispatchRate()));
        topicPolicies.getDelayedDeliveryTickTimeMillis().updateTopicValue(data.getDelayedDeliveryTickTimeMillis());
        topicPolicies.getDelayedDeliveryMaxDelayInMillis().updateTopicValue(data.getDelayedDeliveryMaxDelayInMillis());
        topicPolicies.getSubscribeRate().updateTopicValue(SubscribeRate.normalize(data.getSubscribeRate()));
        topicPolicies.getSubscriptionDispatchRate().updateTopicValue(
            DispatchRateImpl.normalize(data.getSubscriptionDispatchRate()));
        topicPolicies.getCompactionThreshold().updateTopicValue(data.getCompactionThreshold());
        topicPolicies.getDispatchRate().updateTopicValue(DispatchRateImpl.normalize(data.getDispatchRate()));
        topicPolicies.getSchemaValidationEnforced().updateTopicValue(data.getSchemaValidationEnforced());
        topicPolicies.getEntryFilters().updateTopicValue(data.getEntryFilters());
        topicPolicies.getDispatcherPauseOnAckStatePersistentEnabled()
                .updateTopicValue(data.getDispatcherPauseOnAckStatePersistentEnabled());
        this.subscriptionPolicies = data.getSubscriptionPolicies();

        updateEntryFilters();
    }

    protected void updateTopicPolicyByNamespacePolicy(Policies namespacePolicies) {
        if (log.isDebugEnabled()) {
            log.debug("[{}]updateTopicPolicyByNamespacePolicy,data={}", topic, namespacePolicies);
        }
        if (!isSystemTopic()) {
            updateNamespacePublishRate(namespacePolicies, brokerService.getPulsar().getConfig().getClusterName());
            updateNamespaceDispatchRate(namespacePolicies, brokerService.getPulsar().getConfig().getClusterName());
        }
        topicPolicies.getRetentionPolicies().updateNamespaceValue(namespacePolicies.retention_policies);
        topicPolicies.getCompactionThreshold().updateNamespaceValue(namespacePolicies.compaction_threshold);
        topicPolicies.getReplicationClusters().updateNamespaceValue(
                new ArrayList<>(CollectionUtils.emptyIfNull(namespacePolicies.replication_clusters)));
        topicPolicies.getMaxUnackedMessagesOnConsumer()
                .updateNamespaceValue(normalizeValue(namespacePolicies.max_unacked_messages_per_consumer));
        topicPolicies.getMaxUnackedMessagesOnSubscription()
                .updateNamespaceValue(normalizeValue(namespacePolicies.max_unacked_messages_per_subscription));
        topicPolicies.getMessageTTLInSeconds()
                .updateNamespaceValue(normalizeValue(namespacePolicies.message_ttl_in_seconds));
        topicPolicies.getMaxSubscriptionsPerTopic()
                .updateNamespaceValue(normalizeValue(namespacePolicies.max_subscriptions_per_topic));
        topicPolicies.getMaxProducersPerTopic()
                .updateNamespaceValue(normalizeValue(namespacePolicies.max_producers_per_topic));
        topicPolicies.getMaxConsumerPerTopic()
                .updateNamespaceValue(normalizeValue(namespacePolicies.max_consumers_per_topic));
        topicPolicies.getMaxConsumersPerSubscription()
                .updateNamespaceValue(normalizeValue(namespacePolicies.max_consumers_per_subscription));
        topicPolicies.getInactiveTopicPolicies().updateNamespaceValue(namespacePolicies.inactive_topic_policies);
        topicPolicies.getDeduplicationEnabled().updateNamespaceValue(namespacePolicies.deduplicationEnabled);
        topicPolicies.getDeduplicationSnapshotIntervalSeconds().updateNamespaceValue(
                namespacePolicies.deduplicationSnapshotIntervalSeconds);
        topicPolicies.getDelayedDeliveryEnabled().updateNamespaceValue(
                Optional.ofNullable(namespacePolicies.delayed_delivery_policies)
                        .map(DelayedDeliveryPolicies::isActive).orElse(null));
        topicPolicies.getDelayedDeliveryTickTimeMillis().updateNamespaceValue(
                Optional.ofNullable(namespacePolicies.delayed_delivery_policies)
                        .map(DelayedDeliveryPolicies::getTickTime).orElse(null));
        topicPolicies.getDelayedDeliveryMaxDelayInMillis().updateNamespaceValue(
                Optional.ofNullable(namespacePolicies.delayed_delivery_policies)
                        .map(DelayedDeliveryPolicies::getMaxDeliveryDelayInMillis).orElse(null));
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
        topicPolicies.getSchemaValidationEnforced().updateNamespaceValue(namespacePolicies.schema_validation_enforced);
        topicPolicies.getEntryFilters().updateNamespaceValue(namespacePolicies.entryFilters);

        topicPolicies.getDispatcherPauseOnAckStatePersistentEnabled().updateNamespaceValue(
                namespacePolicies.dispatcherPauseOnAckStatePersistentEnabled);

        updateEntryFilters();
    }

    private Integer normalizeValue(Integer policyValue) {
        return policyValue != null && policyValue < 0 ? null : policyValue;
    }

    private void updateNamespaceDispatchRate(Policies namespacePolicies, String cluster) {
        DispatchRateImpl dispatchRate = namespacePolicies.topicDispatchRate.get(cluster);
        if (dispatchRate == null) {
            dispatchRate = namespacePolicies.clusterDispatchRate.get(cluster);
        }
        topicPolicies.getDispatchRate().updateNamespaceValue(DispatchRateImpl.normalize(dispatchRate));
    }

    private void updateNamespaceSubscribeRate(Policies namespacePolicies, String cluster) {
        topicPolicies.getSubscribeRate()
            .updateNamespaceValue(SubscribeRate.normalize(namespacePolicies.clusterSubscribeRate.get(cluster)));
    }

    private void updateNamespaceSubscriptionDispatchRate(Policies namespacePolicies, String cluster) {
        topicPolicies.getSubscriptionDispatchRate()
            .updateNamespaceValue(DispatchRateImpl.normalize(namespacePolicies.subscriptionDispatchRate.get(cluster)));
    }

    private void updateNamespaceReplicatorDispatchRate(Policies namespacePolicies, String cluster) {
        topicPolicies.getReplicatorDispatchRate()
            .updateNamespaceValue(DispatchRateImpl.normalize(namespacePolicies.replicatorDispatchRate.get(cluster)));
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
        topicPolicies.getRetentionPolicies().updateBrokerValue(
                new RetentionPolicies(config.getDefaultRetentionTimeInMinutes(), config.getDefaultRetentionSizeInMB()));
        topicPolicies.getDeduplicationSnapshotIntervalSeconds()
                .updateBrokerValue(config.getBrokerDeduplicationSnapshotIntervalSeconds());
        topicPolicies.getMaxUnackedMessagesOnConsumer().updateBrokerValue(config.getMaxUnackedMessagesPerConsumer());
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
        topicPolicies.getDelayedDeliveryMaxDelayInMillis()
                .updateBrokerValue(config.getDelayedDeliveryMaxDelayInMillis());
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
        topicPolicies.getSchemaValidationEnforced().updateBrokerValue(config.isSchemaValidationEnforced());
        topicPolicies.getEntryFilters().updateBrokerValue(new EntryFilters(String.join(",",
                config.getEntryFilterNames())));
        topicPolicies.getDispatcherPauseOnAckStatePersistentEnabled()
                .updateBrokerValue(config.isDispatcherPauseOnAckStatePersistentEnabled());
        updateEntryFilters();
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

    public boolean isProducersExceeded(String producerName) {
        String replicatorPrefix = brokerService.getPulsar().getConfig().getReplicatorPrefix() + ".";
        boolean isRemote = producerName.startsWith(replicatorPrefix);
        return isProducersExceeded(isRemote);
    }

    protected boolean isProducersExceeded(Producer producer) {
        return isProducersExceeded(producer.isRemote());
    }

    protected boolean isProducersExceeded(boolean isRemote) {
        if (isSystemTopic() || isRemote) {
            return false;
        }
        Integer maxProducers = topicPolicies.getMaxProducersPerTopic().get();
        return maxProducers != null && maxProducers > 0
                && maxProducers <= USER_CREATED_PRODUCER_COUNTER_UPDATER.get(this);
    }

    protected void registerTopicPolicyListener() {
        brokerService.getPulsar().getTopicPoliciesService()
                .registerListener(TopicName.getPartitionedTopicName(topic), this);
    }

    protected void unregisterTopicPolicyListener() {
        brokerService.getPulsar().getTopicPoliciesService()
                .unregisterListener(TopicName.getPartitionedTopicName(topic), this);
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

    public boolean isConsumersExceededOnTopic() {
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
            final Collection<? extends Subscription> subscriptions) {
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
        return topicPolicies.getSchemaValidationEnforced().get();
    }

    public void markBatchMessagePublished() {
        this.hasBatchMessagePublished = true;
    }

    public String getReplicatorPrefix() {
        return replicatorPrefix;
    }

    protected String getSchemaId() {
        String base = TopicName.get(getName()).getPartitionedTopicName();
        return TopicName.get(base).getSchemaName();
    }
    @Override
    public CompletableFuture<Boolean> hasSchema() {
        return brokerService.pulsar().getSchemaRegistryService().getSchema(getSchemaId()).thenApply(Objects::nonNull)
                .exceptionally(e -> {
                    Throwable ex = e.getCause();
                    if (brokerService.pulsar().getConfig().isSchemaLedgerForceRecovery()
                            && (ex instanceof SchemaException && !((SchemaException) ex).isRecoverable())) {
                        return false;
                    }
                    throw ex instanceof CompletionException ? (CompletionException) ex : new CompletionException(ex);
                });
    }

    @Override
    public CompletableFuture<SchemaVersion> addSchema(SchemaData schema) {
        if (schema == null) {
            return CompletableFuture.completedFuture(SchemaVersion.Empty);
        }

        String id = getSchemaId();
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
        return brokerService.deleteSchema(TopicName.get(getName()));
    }

    @Override
    public CompletableFuture<Void> checkSchemaCompatibleForConsumer(SchemaData schema) {
        String id = getSchemaId();
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
                        if (isMigrated()) {
                            log.warn("[{}] Attempting to add producer to a migrated topic", topic);
                            throw new TopicMigratedException("Topic was already migrated");
                        } else if (isTerminated()) {
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
                case ExclusiveWithFencing:
                    if (hasExclusiveProducer || !producers.isEmpty()) {
                        // clear all waiting producers
                        // otherwise closing any producer will trigger the promotion
                        // of the next pending producer
                        List<Pair<Producer, CompletableFuture<Optional<Long>>>> waitingExclusiveProducersCopy =
                                new ArrayList<>(waitingExclusiveProducers);
                        waitingExclusiveProducers.clear();
                        waitingExclusiveProducersCopy.forEach((Pair<Producer,
                                                               CompletableFuture<Optional<Long>>> handle) -> {
                            log.info("[{}] Failing waiting producer {}", topic, handle.getKey());
                            handle.getValue().completeExceptionally(new ProducerFencedException("Fenced out"));
                            handle.getKey().close(true);
                        });
                        producers.forEach((k, currentProducer) -> {
                            log.info("[{}] Fencing out producer {}", topic, currentProducer);
                            currentProducer.close(true);
                        });
                    }
                    if (producer.getTopicEpoch().isPresent()
                            && producer.getTopicEpoch().get() < topicEpoch.orElse(-1L)) {
                        // If a producer reconnects, but all the topic epoch has already moved forward,
                        // this producer needs to be fenced, because a new producer had been present in between.
                        hasExclusiveProducer = false;
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
            log.error("[{}] Encountered unexpected error during exclusive producer creation", topic, e);
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
        TOTAL_RATE_LIMITED_UPDATER.incrementAndGet(this);
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
    public void incrementPublishCount(Producer producer, int numOfMessages, long msgSizeInBytes) {
        handlePublishThrottling(producer, numOfMessages, msgSizeInBytes);

        // increase counters
        bytesInCounter.add(msgSizeInBytes);
        msgInCounter.add(numOfMessages);

        if (isSystemTopic()) {
            systemTopicBytesInCounter.add(msgSizeInBytes);
        }

        if (producer.isRemote()) {
            var remoteClusterName = producer.getRemoteCluster();
            var replicator = getReplicators().get(remoteClusterName);
            if (replicator != null) {
                replicator.getStats().incrementPublishCount(numOfMessages, msgSizeInBytes);
            }
        }
    }

    private void handlePublishThrottling(Producer producer, int numOfMessages, long msgSizeInBytes) {
        // consume tokens from rate limiters and possibly throttle the connection that published the message
        // if it's publishing too fast. Each connection will be throttled lazily when they publish messages.
        for (PublishRateLimiter rateLimiter : activeRateLimiters) {
            rateLimiter.handlePublishThrottling(producer, numOfMessages, msgSizeInBytes);
        }
    }

    private void updateActiveRateLimiters() {
        List<PublishRateLimiter> updatedRateLimiters = new ArrayList<>();
        updatedRateLimiters.add(this.topicPublishRateLimiter);
        updatedRateLimiters.add(getBrokerPublishRateLimiter());
        if (isResourceGroupRateLimitingEnabled()) {
            updatedRateLimiters.add(resourceGroupPublishLimiter);
        }
        activeRateLimiters = updatedRateLimiters.stream().filter(Objects::nonNull).toList();
    }

    public void updateDispatchRateLimiter() {
    }


    protected void checkTopicFenced() throws BrokerServiceException {
        if (isFenced) {
            log.warn("[{}] Attempting to add producer to a fenced topic", topic);
            throw new BrokerServiceException.TopicFencedException("Topic is temporarily unavailable");
        }
    }

    protected CompletableFuture<Void> internalAddProducer(Producer producer) {
        if (isSameAddressProducersExceeded(producer)) {
            log.warn("[{}] Attempting to add producer to topic which reached max same address producers limit", topic);
            return CompletableFuture.failedFuture(new BrokerServiceException.ProducerBusyException(
                    "Topic '" + topic + "' reached max same address producers limit"));
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
                return CompletableFuture.failedFuture(new BrokerServiceException.NamingException("Producer with name '"
                        + newProducer.getProducerName() + "' replace concurrency error"));
            } else {
                handleProducerRemoved(oldProducer);
                return CompletableFuture.completedFuture(null);
            }
        } else {
            // If a producer with the same name tries to use a new connection, async check the old connection is
            // available. The producers related the connection that not available are automatically cleaned up.
            if (!Objects.equals(oldProducer.getCnx(), newProducer.getCnx())) {
                return oldProducer.getCnx().checkConnectionLiveness().thenCompose(previousIsActive -> {
                    if (previousIsActive.isEmpty() || previousIsActive.get()) {
                        return CompletableFuture.failedFuture(new BrokerServiceException.NamingException(
                                "Producer with name '" + newProducer.getProducerName()
                                        + "' is already connected to topic '" + topic + "'"));
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
            return CompletableFuture.failedFuture(new BrokerServiceException.NamingException(
                    "Producer with name '" + newProducer.getProducerName() + "' is already connected to topic '"
                            + topic + "'"));
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
                log.info("Using resource group {} rate limiter for topic {}", rgName, topic);
            }
        } else {
            if (this.resourceGroupRateLimitingEnabled) {
                this.resourceGroupPublishLimiter = null;
                this.resourceGroupRateLimitingEnabled = false;
            }
        }
        updateActiveRateLimiters();
    }

    public void updateEntryFilters() {
        if (isSystemTopic()) {
            entryFilters = Pair.of(null, Collections.emptyList());
            return;
        }
        final EntryFilters entryFiltersPolicy = getEntryFiltersPolicy();
        if (entryFiltersPolicy == null || StringUtils.isBlank(entryFiltersPolicy.getEntryFilterNames())) {
            entryFilters = Pair.of(null, Collections.emptyList());
            return;
        }
        final String entryFilterNames = entryFiltersPolicy.getEntryFilterNames();
        if (entryFilters != null && entryFilterNames.equals(entryFilters.getLeft())) {
            return;
        }
        try {
            final List<EntryFilter> filters =
                    brokerService.getEntryFilterProvider().loadEntryFiltersForPolicy(entryFiltersPolicy);
            entryFilters = Pair.of(entryFilterNames, filters);
        } catch (Throwable e) {
            log.error("Failed to load entry filters on topic {}: {}", topic, e.getMessage());
            throw new RuntimeException(e);
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

    public long getSystemTopicBytesInCounter() {
        return systemTopicBytesInCounter.longValue();
    }

    public long getBytesOutCounter() {
        return bytesOutFromRemovedSubscriptions.longValue()
                + sumSubscriptions(AbstractSubscription::getBytesOutCounter);
    }

    public long getTotalPublishRateLimitCounter() {
        return TOTAL_RATE_LIMITED_UPDATER.get(this);
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

    protected abstract boolean isMigrated();

    public boolean isTransferring() {
        return transferring;
    }

    private static final Logger log = LoggerFactory.getLogger(AbstractTopic.class);

    public InactiveTopicPolicies getInactiveTopicPolicies() {
        return topicPolicies.getInactiveTopicPolicies().get();
    }

    public CompletableFuture<Void> deleteTopicPolicies() {
        return brokerService.pulsar().getTopicPoliciesService().deleteTopicPoliciesAsync(TopicName.get(topic));
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
        PublishRate publishRate = topicPolicies.getPublishRate().get();
        if (publishRate.publishThrottlingRateInByte > 0 || publishRate.publishThrottlingRateInMsg > 0) {
            log.info("Enabling publish rate limiting {} on topic {}", publishRate, getName());
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Disabling publish throttling for {}", this.topic);
            }
        }
        this.topicPublishRateLimiter.update(publishRate);
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

    public void updateBrokerReplicatorDispatchRate() {
        topicPolicies.getReplicatorDispatchRate().updateBrokerValue(
            replicatorDispatchRateInBroker(brokerService.pulsar().getConfiguration()));
    }

    public void updateBrokerDispatchRate() {
        topicPolicies.getDispatchRate().updateBrokerValue(
            dispatchRateInBroker(brokerService.pulsar().getConfiguration()));
    }

    public void updateBrokerDispatchPauseOnAckStatePersistentEnabled() {
        topicPolicies.getDispatcherPauseOnAckStatePersistentEnabled().updateBrokerValue(
                brokerService.pulsar().getConfiguration().isDispatcherPauseOnAckStatePersistentEnabled());
    }

    public void addFilteredEntriesCount(int filtered) {
        this.filteredEntriesCounter.add(filtered);
    }

    public long getFilteredEntriesCount() {
        return this.filteredEntriesCounter.longValue();
    }

    public void updateBrokerPublishRate() {
        topicPolicies.getPublishRate().updateBrokerValue(
            publishRateInBroker(brokerService.pulsar().getConfiguration()));
    }

    public void updateBrokerSubscribeRate() {
        topicPolicies.getSubscribeRate().updateBrokerValue(
            subscribeRateInBroker(brokerService.pulsar().getConfiguration()));
    }

    public Optional<ClusterUrl> getMigratedClusterUrl() {
        return getMigratedClusterUrl(brokerService.getPulsar(), topic);
    }

    public static CompletableFuture<Boolean> isClusterMigrationEnabled(PulsarService pulsar,
            String topic) {
        return getMigratedClusterUrlAsync(pulsar, topic).thenApply(url -> url.isPresent());
    }

    public static CompletableFuture<Optional<ClusterUrl>> getMigratedClusterUrlAsync(PulsarService pulsar,
            String topic) {
        CompletableFuture<Optional<ClusterUrl>> result = new CompletableFuture<>();
        pulsar.getPulsarResources().getClusterResources().getClusterPoliciesResources()
                .getClusterPoliciesAsync(pulsar.getConfig().getClusterName())
                .thenCombine(isNamespaceMigrationEnabledAsync(pulsar, topic),
                        ((clusterData, isNamespaceMigrationEnabled) -> {
                            Optional<ClusterUrl> url = (clusterData.isPresent() && (clusterData.get().isMigrated()
                                    || isNamespaceMigrationEnabled))
                                            ? Optional.ofNullable(clusterData.get().getMigratedClusterUrl())
                                            : Optional.empty();
                            return url;
                        }))
                .thenAccept(res -> {
                    // cluster policies future is completed by metadata-store thread and continuing further
                    // processing in the same metadata store can cause deadlock while creating topic as
                    // create topic path may have blocking call on metadata-store. so, complete future on a
                    // separate thread to avoid deadlock.
                    pulsar.getExecutor().execute(() -> result.complete(res));
                }).exceptionally(ex -> {
                    pulsar.getExecutor().execute(() -> result.completeExceptionally(ex.getCause()));
                    return null;
                });
        return result;
    }

    private static CompletableFuture<Boolean> isNamespaceMigrationEnabledAsync(PulsarService pulsar, String topic) {
        return pulsar.getPulsarResources().getLocalPolicies()
                .getLocalPoliciesAsync(TopicName.get(topic).getNamespaceObject())
                .thenApply(policies -> policies.isPresent() && policies.get().migrated);
    }

    public static Optional<ClusterUrl> getMigratedClusterUrl(PulsarService pulsar, String topic) {
        try {
            return getMigratedClusterUrlAsync(pulsar, topic)
                    .get(pulsar.getPulsarResources().getClusterResources().getOperationTimeoutSec(), TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("[{}] Failed to get migration cluster URL", topic, e);
        }
        return Optional.empty();
    }

    public boolean isSystemCursor(String sub) {
        return COMPACTION_SUBSCRIPTION.equals(sub)
                || (additionalSystemCursorNames != null && additionalSystemCursorNames.contains(sub));
    }
}
