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
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
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
import org.apache.pulsar.common.policies.data.SubscriptionPolicies;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.impl.AutoSubscriptionCreationOverrideImpl;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;

/**
 * Resolves the topic-level policies in effect for a topic through the cascade
 * local topic -> global topic -> namespace -> broker.
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

    /**
     * Resolves the policies in effect: the local topic policies win over the global topic policies, which win over the
     * namespace policies, which win over the broker configuration.
     *
     * @param local the local topic policies, never {@code null}
     * @param global the global topic policies, never {@code null}
     * @param ns the namespace policies
     */
    @SuppressWarnings("deprecation")
    public AppliedTopicPolicies resolve(TopicPolicies local, TopicPolicies global, Policies ns) {
        String cluster = config.getClusterName();
        AppliedTopicPolicies applied = new AppliedTopicPolicies();
        Levels l = new Levels(local, global);

        applied.setRetentionPolicies(l.get(TopicPolicies::getRetentionPolicies, ns.retention_policies,
                () -> new RetentionPolicies(config.getDefaultRetentionTimeInMinutes(),
                        config.getDefaultRetentionSizeInMB())));
        applied.setPersistence(withBrokerMarkDeleteRate(l.get(TopicPolicies::getPersistence, ns.persistence,
                () -> new PersistencePolicies(config.getManagedLedgerDefaultEnsembleSize(),
                        config.getManagedLedgerDefaultWriteQuorum(), config.getManagedLedgerDefaultAckQuorum(),
                        config.getManagedLedgerDefaultMarkDeleteRateLimit()))));
        applied.setOffloadPolicies(resolveOffload(l, ns));
        applied.setDeduplicationEnabled(l.get(TopicPolicies::getDeduplicationEnabled, ns.deduplicationEnabled,
                config::isBrokerDeduplicationEnabled));
        applied.setDeduplicationSnapshotIntervalSeconds(l.get(TopicPolicies::getDeduplicationSnapshotIntervalSeconds,
                ns.deduplicationSnapshotIntervalSeconds, config::getBrokerDeduplicationSnapshotIntervalSeconds));
        applied.setMessageTTLInSeconds(l.get(TopicPolicies::getMessageTTLInSeconds, TopicPolicyResolver::normalize,
                normalize(ns.message_ttl_in_seconds), config::getTtlDurationDefaultInSeconds));
        applied.setSubscriptionExpirationTimeInMinutes(l.get(TopicPolicies::getSubscriptionExpirationTimeInMinutes,
                TopicPolicyResolver::normalize, normalize(ns.subscription_expiration_time_minutes),
                config::getSubscriptionExpirationTimeMinutes));
        applied.setMaxProducerPerTopic(l.get(TopicPolicies::getMaxProducerPerTopic, TopicPolicyResolver::normalize,
                normalize(ns.max_producers_per_topic), config::getMaxProducersPerTopic));
        applied.setMaxConsumerPerTopic(l.get(TopicPolicies::getMaxConsumerPerTopic, TopicPolicyResolver::normalize,
                normalize(ns.max_consumers_per_topic), config::getMaxConsumersPerTopic));
        applied.setMaxConsumersPerSubscription(l.get(TopicPolicies::getMaxConsumersPerSubscription,
                TopicPolicyResolver::normalize, normalize(ns.max_consumers_per_subscription),
                config::getMaxConsumersPerSubscription));
        applied.setMaxSubscriptionsPerTopic(l.get(TopicPolicies::getMaxSubscriptionsPerTopic,
                TopicPolicyResolver::normalize, normalize(ns.max_subscriptions_per_topic),
                config::getMaxSubscriptionsPerTopic));
        applied.setMaxUnackedMessagesOnConsumer(l.get(TopicPolicies::getMaxUnackedMessagesOnConsumer,
                TopicPolicyResolver::normalize, normalize(ns.max_unacked_messages_per_consumer),
                config::getMaxUnackedMessagesPerConsumer));
        applied.setMaxUnackedMessagesOnSubscription(l.get(TopicPolicies::getMaxUnackedMessagesOnSubscription,
                TopicPolicyResolver::normalize, normalize(ns.max_unacked_messages_per_subscription),
                config::getMaxUnackedMessagesPerSubscription));
        applied.setMaxMessageSize(l.get(TopicPolicies::getMaxMessageSize, TopicPolicyResolver::normalize, null,
                config::getMaxMessageSize));

        DelayedDeliveryPolicies delayed = ns.delayed_delivery_policies;
        applied.setDelayedDeliveryEnabled(l.get(TopicPolicies::getDelayedDeliveryEnabled,
                delayed == null ? null : delayed.isActive(), config::isDelayedDeliveryEnabled));
        applied.setDelayedDeliveryTickTimeMillis(l.get(TopicPolicies::getDelayedDeliveryTickTimeMillis,
                delayed == null ? null : delayed.getTickTime(), config::getDelayedDeliveryTickTimeMillis));
        applied.setDelayedDeliveryMaxDelayInMillis(l.get(TopicPolicies::getDelayedDeliveryMaxDelayInMillis,
                delayed == null ? null : delayed.getMaxDeliveryDelayInMillis(),
                config::getDelayedDeliveryMaxDelayInMillis));
        applied.setDispatcherPauseOnAckStatePersistentEnabled(l.get(
                TopicPolicies::getDispatcherPauseOnAckStatePersistentEnabled,
                ns.dispatcherPauseOnAckStatePersistentEnabled,
                config::isDispatcherPauseOnAckStatePersistentEnabled));
        applied.setInactiveTopicPolicies(l.get(TopicPolicies::getInactiveTopicPolicies, ns.inactive_topic_policies,
                () -> new InactiveTopicPolicies(config.getBrokerDeleteInactiveTopicsMode(),
                        config.getBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds(),
                        config.isBrokerDeleteInactiveTopicsEnabled())));
        applied.setCompactionThreshold(l.get(TopicPolicies::getCompactionThreshold, ns.compaction_threshold,
                config::getBrokerServiceCompactionThresholdInBytes));

        DispatchRateImpl nsDispatchRate = ns.topicDispatchRate.get(cluster);
        if (nsDispatchRate == null) {
            nsDispatchRate = ns.clusterDispatchRate.get(cluster);
        }
        applied.setDispatchRate(l.get(TopicPolicies::getDispatchRate, DispatchRateImpl::normalize,
                DispatchRateImpl.normalize(nsDispatchRate),
                () -> dispatchRate(config.getDispatchThrottlingRatePerTopicInMsg(),
                        config.getDispatchThrottlingRatePerTopicInByte())));
        applied.setSubscriptionDispatchRate(l.get(TopicPolicies::getSubscriptionDispatchRate,
                DispatchRateImpl::normalize, DispatchRateImpl.normalize(ns.subscriptionDispatchRate.get(cluster)),
                () -> dispatchRate(config.getDispatchThrottlingRatePerSubscriptionInMsg(),
                        config.getDispatchThrottlingRatePerSubscriptionInByte())));
        applied.setReplicatorDispatchRate(l.get(TopicPolicies::getReplicatorDispatchRate,
                DispatchRateImpl::normalize, DispatchRateImpl.normalize(ns.replicatorDispatchRate.get(cluster)),
                () -> dispatchRate(config.getDispatchThrottlingRatePerReplicatorInMsg(),
                        config.getDispatchThrottlingRatePerReplicatorInByte())));
        applied.setSubscribeRate(l.get(TopicPolicies::getSubscribeRate, SubscribeRate::normalize,
                SubscribeRate.normalize(ns.clusterSubscribeRate.get(cluster)),
                () -> new SubscribeRate(config.getSubscribeThrottlingRatePerConsumer(),
                        config.getSubscribeRatePeriodPerConsumerInSecond())));
        applied.setPublishRate(l.get(TopicPolicies::getPublishRate, PublishRate::normalize, PublishRate.normalize(
                ns.publishMaxMessageRate == null ? null : ns.publishMaxMessageRate.get(cluster)),
                () -> new PublishRate(config.getMaxPublishRatePerTopicInMessages(),
                        config.getMaxPublishRatePerTopicInBytes())));

        applied.setSchemaCompatibilityStrategy(l.get(
                t -> undefinedToNull(t.getSchemaCompatibilityStrategy()),
                undefinedToNull(namespaceSchemaCompatibilityStrategy(ns)),
                () -> undefinedToNull(config.getSchemaCompatibilityStrategy())));
        applied.setSchemaValidationEnforced(resolveSchemaValidationEnforced(l, ns));
        applied.setEntryFilters(resolveEntryFilters(l, ns));
        // The broker reads the auto subscription creation override from the local topic policies only.
        applied.setAutoSubscriptionCreationOverride(new Levels(local, new TopicPolicies()).get(
                TopicPolicies::getAutoSubscriptionCreationOverride, toImpl(ns.autoSubscriptionCreationOverride),
                () -> AutoSubscriptionCreationOverrideImpl.builder()
                        .allowAutoSubscriptionCreation(config.isAllowAutoSubscriptionCreation()).build()));
        applied.setSubscriptionTypesEnabled(l.get(t -> emptyToNull(t.getSubscriptionTypesEnabled()),
                subTypes(ns.subscription_types_enabled), () -> subTypes(config.getSubscriptionTypesEnabled())));
        applied.setReplicationClusters(l.get(t -> emptyToNull(t.getReplicationClusters()),
                emptyToNull(ns.replication_clusters == null ? null : new ArrayList<>(ns.replication_clusters)),
                Collections::emptyList));

        Map<String, AppliedValue<BacklogQuotaImpl>> quotas = new HashMap<>();
        for (BacklogQuotaType type : BacklogQuotaType.values()) {
            BacklogQuota nsQuota = ns.backlog_quota_map == null ? null : ns.backlog_quota_map.get(type);
            quotas.put(type.name(), l.get(t -> t.getBackLogQuotaMap() == null ? null
                            : t.getBackLogQuotaMap().get(type.name()),
                    (BacklogQuotaImpl) nsQuota, () -> (BacklogQuotaImpl) defaultBacklogQuota));
        }
        applied.setBackLogQuotaMap(quotas);

        // Policies that exist only at topic level are returned as stored, and are absent when unset.
        AppliedValue<List<String>> shadowTopics = l.getIfSet(TopicPolicies::getShadowTopics);
        if (shadowTopics != null) {
            applied.setShadowTopics(shadowTopics);
        }
        Map<String, AppliedValue<SubscriptionPolicies>> subscriptionPolicies = new HashMap<>();
        if (global.getSubscriptionPolicies() != null) {
            global.getSubscriptionPolicies().forEach((name, policies) ->
                    subscriptionPolicies.put(name, new AppliedValue<>(policies, PolicySource.TOPIC_GLOBAL)));
        }
        if (local.getSubscriptionPolicies() != null) {
            local.getSubscriptionPolicies().forEach((name, policies) ->
                    subscriptionPolicies.put(name, new AppliedValue<>(policies, PolicySource.TOPIC)));
        }
        if (!subscriptionPolicies.isEmpty()) {
            applied.setSubscriptionPolicies(subscriptionPolicies);
        }
        return applied;
    }

    /** The two topic-level scopes: a loaded topic prefers the local value to the global one. */
    private static final class Levels {
        private final TopicPolicies local;
        private final TopicPolicies global;

        Levels(TopicPolicies local, TopicPolicies global) {
            this.local = local;
            this.global = global;
        }

        <T> AppliedValue<T> get(Function<TopicPolicies, T> getter, T namespaceValue, Supplier<T> brokerValue) {
            return get(getter, UnaryOperator.identity(), namespaceValue, brokerValue);
        }

        // The normalizer turns a topic-level value that the broker treats as "not set" into null.
        <T> AppliedValue<T> get(Function<TopicPolicies, T> getter, UnaryOperator<T> normalizer, T namespaceValue,
                                Supplier<T> brokerValue) {
            AppliedValue<T> topic = getIfSet(getter, normalizer);
            if (topic != null) {
                return topic;
            }
            if (namespaceValue != null) {
                return new AppliedValue<>(namespaceValue, PolicySource.NAMESPACE);
            }
            return new AppliedValue<>(brokerValue.get(), PolicySource.BROKER);
        }

        <T> AppliedValue<T> getIfSet(Function<TopicPolicies, T> getter) {
            return getIfSet(getter, UnaryOperator.identity());
        }

        <T> AppliedValue<T> getIfSet(Function<TopicPolicies, T> getter, UnaryOperator<T> normalizer) {
            T localValue = normalizer.apply(getter.apply(local));
            if (localValue != null) {
                return new AppliedValue<>(localValue, PolicySource.TOPIC);
            }
            T globalValue = normalizer.apply(getter.apply(global));
            if (globalValue != null) {
                return new AppliedValue<>(globalValue, PolicySource.TOPIC_GLOBAL);
            }
            return null;
        }
    }

    // The ledger replaces a negative mark delete rate with the broker default.
    private AppliedValue<PersistencePolicies> withBrokerMarkDeleteRate(AppliedValue<PersistencePolicies> applied) {
        PersistencePolicies policies = applied.getValue();
        if (policies.getManagedLedgerMaxMarkDeleteRate() >= 0) {
            return applied;
        }
        PersistencePolicies copy = new PersistencePolicies(policies.getBookkeeperEnsemble(),
                policies.getBookkeeperWriteQuorum(), policies.getBookkeeperAckQuorum(),
                config.getManagedLedgerDefaultMarkDeleteRateLimit(), policies.getManagedLedgerStorageClassName());
        return new AppliedValue<>(copy, applied.getSource());
    }

    private AppliedValue<OffloadPoliciesImpl> resolveOffload(Levels l, Policies ns) {
        AppliedValue<OffloadPoliciesImpl> topic = l.getIfSet(t -> (OffloadPoliciesImpl) t.getOffloadPolicies());
        OffloadPoliciesImpl nsPolicies = OffloadPoliciesImpl.oldPoliciesCompatible(
                (OffloadPoliciesImpl) ns.offload_policies, ns);
        OffloadPoliciesImpl merged = OffloadPoliciesImpl.mergeConfiguration(
                topic == null ? null : topic.getValue(), nsPolicies, config.getProperties());
        PolicySource source = topic != null ? topic.getSource()
                : nsPolicies != null ? PolicySource.NAMESPACE : PolicySource.BROKER;
        return new AppliedValue<>(merged, source);
    }

    // The schema validation is enabled when either the broker or the most specific level enables it.
    private AppliedValue<Boolean> resolveSchemaValidationEnforced(Levels l, Policies ns) {
        if (config.isSchemaValidationEnforced()) {
            return new AppliedValue<>(true, PolicySource.BROKER);
        }
        AppliedValue<Boolean> topic = l.getIfSet(TopicPolicies::getSchemaValidationEnforced);
        if (topic != null) {
            return topic;
        }
        return new AppliedValue<>(ns.schema_validation_enforced,
                ns.schema_validation_enforced ? PolicySource.NAMESPACE : PolicySource.BROKER);
    }

    private AppliedValue<EntryFilters> resolveEntryFilters(Levels l, Policies ns) {
        if (!config.isAllowOverrideEntryFilters()) {
            return new AppliedValue<>(brokerEntryFilters(), PolicySource.BROKER);
        }
        return l.get(TopicPolicies::getEntryFilters, ns.entryFilters, this::brokerEntryFilters);
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
