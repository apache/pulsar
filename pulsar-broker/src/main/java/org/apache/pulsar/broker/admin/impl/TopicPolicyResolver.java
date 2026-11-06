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
package org.apache.pulsar.broker.admin.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.policies.data.AppliedTopicPolicies;
import org.apache.pulsar.common.policies.data.AppliedValue;
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.EntryFilters;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PolicySource;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.impl.AutoSubscriptionCreationOverrideImpl;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;

/**
 * Resolves the topic-level policies in effect for a topic through the cascade topic -> namespace -> broker.
 *
 * <p>The rules mirror how a loaded topic builds its {@code HierarchyTopicPolicies}
 * ({@code AbstractTopic#updateTopicPolicyByNamespacePolicy} and {@code AbstractTopic#updateTopicPolicyByBrokerConfig}),
 * and the rules that produce the {@code ManagedLedgerConfig} for persistence and offload. Unlike them, the resolver
 * needs no loaded topic: it works from the topic's {@link TopicPolicies}, the namespace {@link Policies} and the
 * broker configuration. A policy that no level sets is returned with {@link PolicySource#BROKER} and a {@code null}
 * value.
 */
public class TopicPolicyResolver {

    private final ServiceConfiguration config;
    private final BacklogQuota defaultBacklogQuota;

    public TopicPolicyResolver(ServiceConfiguration config, BacklogQuota defaultBacklogQuota) {
        this.config = config;
        this.defaultBacklogQuota = defaultBacklogQuota;
    }

    @SuppressWarnings("deprecation")
    public AppliedTopicPolicies resolve(TopicPolicies topic, Policies ns) {
        String cluster = config.getClusterName();
        AppliedTopicPolicies applied = new AppliedTopicPolicies();

        applied.setRetentionPolicies(resolve(topic.getRetentionPolicies(), ns.retention_policies,
                () -> new RetentionPolicies(config.getDefaultRetentionTimeInMinutes(),
                        config.getDefaultRetentionSizeInMB())));
        applied.setPersistence(resolve(topic.getPersistence(), ns.persistence,
                () -> new PersistencePolicies(config.getManagedLedgerDefaultEnsembleSize(),
                        config.getManagedLedgerDefaultWriteQuorum(), config.getManagedLedgerDefaultAckQuorum(),
                        config.getManagedLedgerDefaultMarkDeleteRateLimit())));
        applied.setOffloadPolicies(resolveOffload(topic, ns));
        applied.setDeduplicationEnabled(resolve(topic.getDeduplicationEnabled(), ns.deduplicationEnabled,
                config::isBrokerDeduplicationEnabled));
        applied.setDeduplicationSnapshotIntervalSeconds(resolve(topic.getDeduplicationSnapshotIntervalSeconds(),
                ns.deduplicationSnapshotIntervalSeconds, config::getBrokerDeduplicationSnapshotIntervalSeconds));
        applied.setMessageTTLInSeconds(resolve(topic.getMessageTTLInSeconds(),
                normalize(ns.message_ttl_in_seconds), config::getTtlDurationDefaultInSeconds));
        applied.setSubscriptionExpirationTimeInMinutes(resolve(topic.getSubscriptionExpirationTimeInMinutes(),
                normalize(ns.subscription_expiration_time_minutes), config::getSubscriptionExpirationTimeMinutes));
        applied.setMaxProducerPerTopic(resolve(topic.getMaxProducerPerTopic(),
                normalize(ns.max_producers_per_topic), config::getMaxProducersPerTopic));
        applied.setMaxConsumerPerTopic(resolve(topic.getMaxConsumerPerTopic(),
                normalize(ns.max_consumers_per_topic), config::getMaxConsumersPerTopic));
        applied.setMaxConsumersPerSubscription(resolve(topic.getMaxConsumersPerSubscription(),
                normalize(ns.max_consumers_per_subscription), config::getMaxConsumersPerSubscription));
        applied.setMaxSubscriptionsPerTopic(resolve(topic.getMaxSubscriptionsPerTopic(),
                normalize(ns.max_subscriptions_per_topic), config::getMaxSubscriptionsPerTopic));
        applied.setMaxUnackedMessagesOnConsumer(resolve(topic.getMaxUnackedMessagesOnConsumer(),
                normalize(ns.max_unacked_messages_per_consumer), config::getMaxUnackedMessagesPerConsumer));
        applied.setMaxUnackedMessagesOnSubscription(resolve(topic.getMaxUnackedMessagesOnSubscription(),
                normalize(ns.max_unacked_messages_per_subscription), config::getMaxUnackedMessagesPerSubscription));
        applied.setMaxMessageSize(resolve(topic.getMaxMessageSize(), null, config::getMaxMessageSize));

        DelayedDeliveryPolicies delayed = ns.delayed_delivery_policies;
        applied.setDelayedDeliveryEnabled(resolve(topic.getDelayedDeliveryEnabled(),
                delayed == null ? null : delayed.isActive(), config::isDelayedDeliveryEnabled));
        applied.setDelayedDeliveryTickTimeMillis(resolve(topic.getDelayedDeliveryTickTimeMillis(),
                delayed == null ? null : delayed.getTickTime(), config::getDelayedDeliveryTickTimeMillis));
        applied.setDelayedDeliveryMaxDelayInMillis(resolve(topic.getDelayedDeliveryMaxDelayInMillis(),
                delayed == null ? null : delayed.getMaxDeliveryDelayInMillis(),
                config::getDelayedDeliveryMaxDelayInMillis));
        applied.setDispatcherPauseOnAckStatePersistentEnabled(resolve(
                topic.getDispatcherPauseOnAckStatePersistentEnabled(), ns.dispatcherPauseOnAckStatePersistentEnabled,
                config::isDispatcherPauseOnAckStatePersistentEnabled));
        applied.setInactiveTopicPolicies(resolve(topic.getInactiveTopicPolicies(), ns.inactive_topic_policies,
                () -> new InactiveTopicPolicies(config.getBrokerDeleteInactiveTopicsMode(),
                        config.getBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds(),
                        config.isBrokerDeleteInactiveTopicsEnabled())));
        applied.setCompactionThreshold(resolve(topic.getCompactionThreshold(), ns.compaction_threshold,
                config::getBrokerServiceCompactionThresholdInBytes));

        DispatchRateImpl nsDispatchRate = ns.topicDispatchRate.get(cluster);
        if (nsDispatchRate == null) {
            nsDispatchRate = ns.clusterDispatchRate.get(cluster);
        }
        applied.setDispatchRate(resolve(topic.getDispatchRate(), DispatchRateImpl.normalize(nsDispatchRate),
                () -> dispatchRate(config.getDispatchThrottlingRatePerTopicInMsg(),
                        config.getDispatchThrottlingRatePerTopicInByte())));
        applied.setSubscriptionDispatchRate(resolve(topic.getSubscriptionDispatchRate(),
                DispatchRateImpl.normalize(ns.subscriptionDispatchRate.get(cluster)),
                () -> dispatchRate(config.getDispatchThrottlingRatePerSubscriptionInMsg(),
                        config.getDispatchThrottlingRatePerSubscriptionInByte())));
        applied.setReplicatorDispatchRate(resolve(topic.getReplicatorDispatchRate(),
                DispatchRateImpl.normalize(ns.replicatorDispatchRate.get(cluster)),
                () -> dispatchRate(config.getDispatchThrottlingRatePerReplicatorInMsg(),
                        config.getDispatchThrottlingRatePerReplicatorInByte())));
        applied.setSubscribeRate(resolve(topic.getSubscribeRate(),
                SubscribeRate.normalize(ns.clusterSubscribeRate.get(cluster)),
                () -> new SubscribeRate(config.getSubscribeThrottlingRatePerConsumer(),
                        config.getSubscribeRatePeriodPerConsumerInSecond())));
        applied.setPublishRate(resolve(topic.getPublishRate(), PublishRate.normalize(
                ns.publishMaxMessageRate == null ? null : ns.publishMaxMessageRate.get(cluster)),
                () -> new PublishRate(config.getMaxPublishRatePerTopicInMessages(),
                        config.getMaxPublishRatePerTopicInBytes())));

        applied.setSchemaCompatibilityStrategy(resolve(undefinedToNull(topic.getSchemaCompatibilityStrategy()),
                undefinedToNull(namespaceSchemaCompatibilityStrategy(ns)),
                () -> undefinedToNull(config.getSchemaCompatibilityStrategy())));
        applied.setSchemaValidationEnforced(resolveSchemaValidationEnforced(topic, ns));
        applied.setEntryFilters(resolveEntryFilters(topic, ns));
        applied.setAutoSubscriptionCreationOverride(resolve(topic.getAutoSubscriptionCreationOverride(),
                toImpl(ns.autoSubscriptionCreationOverride),
                () -> AutoSubscriptionCreationOverrideImpl.builder()
                        .allowAutoSubscriptionCreation(config.isAllowAutoSubscriptionCreation()).build()));
        applied.setSubscriptionTypesEnabled(resolve(emptyToNull(topic.getSubscriptionTypesEnabled()),
                subTypes(ns.subscription_types_enabled), () -> subTypes(config.getSubscriptionTypesEnabled())));
        applied.setReplicationClusters(resolve(emptyToNull(topic.getReplicationClusters()),
                emptyToNull(ns.replication_clusters == null ? null : new ArrayList<>(ns.replication_clusters)),
                Collections::emptyList));

        Map<String, AppliedValue<BacklogQuotaImpl>> quotas = new HashMap<>();
        Map<String, BacklogQuotaImpl> topicQuotas = topic.getBackLogQuotaMap();
        for (BacklogQuotaType type : BacklogQuotaType.values()) {
            BacklogQuota nsQuota = ns.backlog_quota_map == null ? null : ns.backlog_quota_map.get(type);
            quotas.put(type.name(), resolve(topicQuotas == null ? null : topicQuotas.get(type.name()),
                    (BacklogQuotaImpl) nsQuota, () -> (BacklogQuotaImpl) defaultBacklogQuota));
        }
        applied.setBackLogQuotaMap(quotas);

        // Policies that exist only at topic level are returned as stored, and are absent when unset.
        if (topic.getShadowTopics() != null) {
            applied.setShadowTopics(new AppliedValue<>(topic.getShadowTopics(), PolicySource.TOPIC));
        }
        applied.setSubscriptionPolicies(topic.getSubscriptionPolicies());
        return applied;
    }

    private static <T> AppliedValue<T> resolve(T topicValue, T namespaceValue, Supplier<T> brokerValue) {
        if (topicValue != null) {
            return new AppliedValue<>(topicValue, PolicySource.TOPIC);
        }
        if (namespaceValue != null) {
            return new AppliedValue<>(namespaceValue, PolicySource.NAMESPACE);
        }
        return new AppliedValue<>(brokerValue.get(), PolicySource.BROKER);
    }

    private AppliedValue<OffloadPoliciesImpl> resolveOffload(TopicPolicies topic, Policies ns) {
        OffloadPoliciesImpl nsPolicies = OffloadPoliciesImpl.oldPoliciesCompatible(
                (OffloadPoliciesImpl) ns.offload_policies, ns);
        OffloadPoliciesImpl merged = OffloadPoliciesImpl.mergeConfiguration(topic.getOffloadPolicies(), nsPolicies,
                config.getProperties());
        PolicySource source = topic.getOffloadPolicies() != null ? PolicySource.TOPIC
                : nsPolicies != null ? PolicySource.NAMESPACE : PolicySource.BROKER;
        return new AppliedValue<>(merged, source);
    }

    // The schema validation is enabled when either the broker or the most specific level enables it.
    private AppliedValue<Boolean> resolveSchemaValidationEnforced(TopicPolicies topic, Policies ns) {
        boolean broker = config.isSchemaValidationEnforced();
        if (broker) {
            return new AppliedValue<>(true, PolicySource.BROKER);
        }
        if (topic.getSchemaValidationEnforced() != null) {
            return new AppliedValue<>(topic.getSchemaValidationEnforced(), PolicySource.TOPIC);
        }
        return new AppliedValue<>(ns.schema_validation_enforced,
                ns.schema_validation_enforced ? PolicySource.NAMESPACE : PolicySource.BROKER);
    }

    private AppliedValue<EntryFilters> resolveEntryFilters(TopicPolicies topic, Policies ns) {
        if (!config.isAllowOverrideEntryFilters()) {
            return new AppliedValue<>(brokerEntryFilters(), PolicySource.BROKER);
        }
        return resolve(topic.getEntryFilters(), ns.entryFilters, this::brokerEntryFilters);
    }

    private EntryFilters brokerEntryFilters() {
        return new EntryFilters(String.join(",", config.getEntryFilterNames()));
    }

    @SuppressWarnings("deprecation")
    private static SchemaCompatibilityStrategy namespaceSchemaCompatibilityStrategy(Policies ns) {
        if (SchemaCompatibilityStrategy.isUndefined(ns.schema_compatibility_strategy)) {
            return SchemaCompatibilityStrategy.fromAutoUpdatePolicy(ns.schema_auto_update_compatibility_strategy);
        }
        return ns.schema_compatibility_strategy;
    }

    private static SchemaCompatibilityStrategy undefinedToNull(SchemaCompatibilityStrategy strategy) {
        return strategy == SchemaCompatibilityStrategy.UNDEFINED ? null : strategy;
    }

    private static AutoSubscriptionCreationOverrideImpl toImpl(AutoSubscriptionCreationOverride override) {
        return (AutoSubscriptionCreationOverrideImpl) override;
    }

    // A negative namespace value means "not set", as it does for a loaded topic.
    private static Integer normalize(Integer value) {
        return value != null && value < 0 ? null : value;
    }

    private static <T> List<T> emptyToNull(List<T> value) {
        return CollectionUtils.isEmpty(value) ? null : value;
    }

    private static DispatchRateImpl dispatchRate(int msg, long bytes) {
        return DispatchRateImpl.builder()
                .dispatchThrottlingRateInMsg(msg)
                .dispatchThrottlingRateInByte(bytes)
                .ratePeriodInSecond(1)
                .build();
    }

    private static List<SubType> subTypes(Set<String> names) {
        EnumSet<SubType> subTypes = EnumSet.noneOf(SubType.class);
        for (String name : CollectionUtils.emptyIfNull(names)) {
            try {
                subTypes.add(SubType.valueOf(name));
            } catch (IllegalArgumentException e) {
                // ignore invalid subscription types, as a loaded topic does
            }
        }
        return subTypes.isEmpty() ? null : new ArrayList<>(subTypes);
    }
}
