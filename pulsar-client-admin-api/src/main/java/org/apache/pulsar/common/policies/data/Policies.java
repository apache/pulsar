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
package org.apache.pulsar.common.policies.data;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.ToString;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;

/**
 * Definition of Pulsar policies.
 */
@ToString
public class Policies {

    @SuppressWarnings("checkstyle:MemberName")
    public final AuthPolicies auth_policies = AuthPolicies.builder().build();
    @SuppressWarnings("checkstyle:MemberName")
    public Set<String> replication_clusters = new HashSet<>();
    public BundlesData bundles;
    @SuppressWarnings("checkstyle:MemberName")
    public Map<BacklogQuota.BacklogQuotaType, BacklogQuota> backlog_quota_map = new HashMap<>();
    @Deprecated
    public Map<String, DispatchRateImpl> clusterDispatchRate = new HashMap<>();
    public Map<String, DispatchRateImpl> topicDispatchRate = new HashMap<>();
    public Map<String, DispatchRateImpl> subscriptionDispatchRate = new HashMap<>();
    public Map<String, DispatchRateImpl> replicatorDispatchRate = new HashMap<>();
    public Map<String, SubscribeRate> clusterSubscribeRate = new HashMap<>();
    public PersistencePolicies persistence = null;

    // If set, it will override the broker settings for enabling deduplication
    public Boolean deduplicationEnabled = null;
    // If set, it will override the broker settings for allowing auto topic creation
    public AutoTopicCreationOverride autoTopicCreationOverride = null;
    // If set, it will override the broker settings for allowing auto subscription creation
    public AutoSubscriptionCreationOverride autoSubscriptionCreationOverride = null;
    public Map<String, PublishRate> publishMaxMessageRate = new HashMap<>();

    @SuppressWarnings("checkstyle:MemberName")
    public Map<String, Integer> latency_stats_sample_rate = new HashMap<>();
    @SuppressWarnings("checkstyle:MemberName")
    public Integer message_ttl_in_seconds = null;
    @SuppressWarnings("checkstyle:MemberName")
    public Integer subscription_expiration_time_minutes = null;
    @SuppressWarnings("checkstyle:MemberName")
    public RetentionPolicies retention_policies = null;
    public boolean deleted = false;
    public static final String FIRST_BOUNDARY = "0x00000000";
    public static final String LAST_BOUNDARY = "0xffffffff";

    @SuppressWarnings("checkstyle:MemberName")
    public boolean encryption_required = false;
    @SuppressWarnings("checkstyle:MemberName")
    public DelayedDeliveryPolicies delayed_delivery_policies = null;
    @SuppressWarnings("checkstyle:MemberName")
    public InactiveTopicPolicies inactive_topic_policies = null;
    @SuppressWarnings("checkstyle:MemberName")
    public SubscriptionAuthMode subscription_auth_mode = SubscriptionAuthMode.None;

    @SuppressWarnings("checkstyle:MemberName")
    public Integer max_producers_per_topic = null;
    @SuppressWarnings("checkstyle:MemberName")
    public Integer max_consumers_per_topic = null;
    @SuppressWarnings("checkstyle:MemberName")
    public Integer max_consumers_per_subscription = null;
    @SuppressWarnings("checkstyle:MemberName")
    public Integer max_unacked_messages_per_consumer = null;
    @SuppressWarnings("checkstyle:MemberName")
    public Integer max_unacked_messages_per_subscription = null;
    @SuppressWarnings("checkstyle:MemberName")
    public Integer max_subscriptions_per_topic = null;

    @SuppressWarnings("checkstyle:MemberName")
    public Long compaction_threshold = null;
    @SuppressWarnings("checkstyle:MemberName")
    public long offload_threshold = -1;
    @SuppressWarnings("checkstyle:MemberName")
    public Long offload_deletion_lag_ms = null;
    @SuppressWarnings("checkstyle:MemberName")
    public Integer max_topics_per_namespace = null;

    @SuppressWarnings("checkstyle:MemberName")
    @Deprecated
    public SchemaAutoUpdateCompatibilityStrategy schema_auto_update_compatibility_strategy = null;

    @SuppressWarnings("checkstyle:MemberName")
    public SchemaCompatibilityStrategy schema_compatibility_strategy = SchemaCompatibilityStrategy.UNDEFINED;

    @SuppressWarnings("checkstyle:MemberName")
    public Boolean is_allow_auto_update_schema = null;

    @SuppressWarnings("checkstyle:MemberName")
    public boolean schema_validation_enforced = false;

    @SuppressWarnings("checkstyle:MemberName")
    public OffloadPolicies offload_policies = null;

    public Integer deduplicationSnapshotIntervalSeconds = null;

    @SuppressWarnings("checkstyle:MemberName")
    public Set<String> subscription_types_enabled = new HashSet<>();

    public Map<String, String> properties = new HashMap<>();

    @SuppressWarnings("checkstyle:MemberName")
    public String resource_group_name = null;

    public enum BundleType {
        LARGEST, HOT;
    }

    @Override
    public int hashCode() {
        return Objects.hash(auth_policies, replication_clusters,
                backlog_quota_map, publishMaxMessageRate, clusterDispatchRate,
                topicDispatchRate, subscriptionDispatchRate, replicatorDispatchRate,
                clusterSubscribeRate, deduplicationEnabled, autoTopicCreationOverride,
                autoSubscriptionCreationOverride, persistence,
                bundles, latency_stats_sample_rate,
                message_ttl_in_seconds, subscription_expiration_time_minutes, retention_policies,
                encryption_required, delayed_delivery_policies, inactive_topic_policies,
                subscription_auth_mode,
                max_producers_per_topic,
                max_consumers_per_topic, max_consumers_per_subscription,
                max_unacked_messages_per_consumer, max_unacked_messages_per_subscription,
                compaction_threshold, offload_threshold,
                offload_deletion_lag_ms,
                schema_auto_update_compatibility_strategy,
                schema_validation_enforced,
                schema_compatibility_strategy,
                is_allow_auto_update_schema,
                offload_policies,
                subscription_types_enabled,
                properties,
                resource_group_name);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Policies) {
            Policies other = (Policies) obj;
            return Objects.equals(auth_policies, other.auth_policies)
                    && Objects.equals(replication_clusters, other.replication_clusters)
                    && Objects.equals(backlog_quota_map, other.backlog_quota_map)
                    && Objects.equals(clusterDispatchRate, other.clusterDispatchRate)
                    && Objects.equals(topicDispatchRate, other.topicDispatchRate)
                    && Objects.equals(subscriptionDispatchRate, other.subscriptionDispatchRate)
                    && Objects.equals(replicatorDispatchRate, other.replicatorDispatchRate)
                    && Objects.equals(clusterSubscribeRate, other.clusterSubscribeRate)
                    && Objects.equals(publishMaxMessageRate, other.publishMaxMessageRate)
                    && Objects.equals(deduplicationEnabled, other.deduplicationEnabled)
                    && Objects.equals(autoTopicCreationOverride, other.autoTopicCreationOverride)
                    && Objects.equals(autoSubscriptionCreationOverride, other.autoSubscriptionCreationOverride)
                    && Objects.equals(persistence, other.persistence) && Objects.equals(bundles, other.bundles)
                    && Objects.equals(latency_stats_sample_rate, other.latency_stats_sample_rate)
                    && Objects.equals(message_ttl_in_seconds,
                            other.message_ttl_in_seconds)
                    && Objects.equals(subscription_expiration_time_minutes, other.subscription_expiration_time_minutes)
                    && Objects.equals(retention_policies, other.retention_policies)
                    && Objects.equals(encryption_required, other.encryption_required)
                    && Objects.equals(delayed_delivery_policies, other.delayed_delivery_policies)
                    && Objects.equals(inactive_topic_policies, other.inactive_topic_policies)
                    && Objects.equals(subscription_auth_mode, other.subscription_auth_mode)
                    && Objects.equals(max_producers_per_topic, other.max_producers_per_topic)
                    && Objects.equals(max_consumers_per_topic, other.max_consumers_per_topic)
                    && Objects.equals(max_unacked_messages_per_consumer, other.max_unacked_messages_per_consumer)
                    && Objects.equals(max_unacked_messages_per_subscription,
                        other.max_unacked_messages_per_subscription)
                    && Objects.equals(max_consumers_per_subscription, other.max_consumers_per_subscription)
                    && Objects.equals(compaction_threshold, other.compaction_threshold)
                    && offload_threshold == other.offload_threshold
                    && Objects.equals(offload_deletion_lag_ms, other.offload_deletion_lag_ms)
                    && schema_auto_update_compatibility_strategy == other.schema_auto_update_compatibility_strategy
                    && schema_validation_enforced == other.schema_validation_enforced
                    && schema_compatibility_strategy == other.schema_compatibility_strategy
                    && is_allow_auto_update_schema == other.is_allow_auto_update_schema
                    && Objects.equals(offload_policies, other.offload_policies)
                    && Objects.equals(subscription_types_enabled, other.subscription_types_enabled)
                    && Objects.equals(properties, other.properties)
                    && Objects.equals(resource_group_name, other.resource_group_name);
        }

        return false;
    }


}
