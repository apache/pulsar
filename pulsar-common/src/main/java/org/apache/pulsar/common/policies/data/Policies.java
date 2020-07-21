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

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Definition of Pulsar policies.
 */
public class Policies {

    @SuppressWarnings("checkstyle:MemberName")
    public final AuthPolicies auth_policies = new AuthPolicies();
    @SuppressWarnings("checkstyle:MemberName")
    public Set<String> replication_clusters = Sets.newHashSet();
    public BundlesData bundles;
    @SuppressWarnings("checkstyle:MemberName")
    public Map<BacklogQuota.BacklogQuotaType, BacklogQuota> backlog_quota_map = Maps.newHashMap();
    @Deprecated
    public Map<String, DispatchRate> clusterDispatchRate = Maps.newHashMap();
    public Map<String, DispatchRate> topicDispatchRate = Maps.newHashMap();
    public Map<String, DispatchRate> subscriptionDispatchRate = Maps.newHashMap();
    public Map<String, DispatchRate> replicatorDispatchRate = Maps.newHashMap();
    public Map<String, SubscribeRate> clusterSubscribeRate = Maps.newHashMap();
    public PersistencePolicies persistence = null;

    // If set, it will override the broker settings for enabling deduplication
    public Boolean deduplicationEnabled = null;
    // If set, it will override the broker settings for allowing auto topic creation
    public AutoTopicCreationOverride autoTopicCreationOverride = null;
    // If set, it will override the broker settings for allowing auto subscription creation
    public AutoSubscriptionCreationOverride autoSubscriptionCreationOverride = null;
    public Map<String, PublishRate> publishMaxMessageRate = Maps.newHashMap();

    @SuppressWarnings("checkstyle:MemberName")
    public Map<String, Integer> latency_stats_sample_rate = Maps.newHashMap();
    @SuppressWarnings("checkstyle:MemberName")
    public int message_ttl_in_seconds = 0;
    @SuppressWarnings("checkstyle:MemberName")
    public int subscription_expiration_time_minutes = 0;
    @SuppressWarnings("checkstyle:MemberName")
    public RetentionPolicies retention_policies = null;
    public boolean deleted = false;
    public String antiAffinityGroup;

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
    public int max_producers_per_topic = 0;
    @SuppressWarnings("checkstyle:MemberName")
    public int max_consumers_per_topic = 0;
    @SuppressWarnings("checkstyle:MemberName")
    public int max_consumers_per_subscription = 0;
    @SuppressWarnings("checkstyle:MemberName")
    public int max_unacked_messages_per_consumer = -1;
    @SuppressWarnings("checkstyle:MemberName")
    public int max_unacked_messages_per_subscription = -1;

    @SuppressWarnings("checkstyle:MemberName")
    public long compaction_threshold = 0;
    @SuppressWarnings("checkstyle:MemberName")
    public long offload_threshold = -1;
    @SuppressWarnings("checkstyle:MemberName")
    public Long offload_deletion_lag_ms = null;

    @SuppressWarnings("checkstyle:MemberName")
    @Deprecated
    public SchemaAutoUpdateCompatibilityStrategy schema_auto_update_compatibility_strategy =
        SchemaAutoUpdateCompatibilityStrategy.Full;

    @SuppressWarnings("checkstyle:MemberName")
    public SchemaCompatibilityStrategy schema_compatibility_strategy = SchemaCompatibilityStrategy.UNDEFINED;

    @SuppressWarnings("checkstyle:MemberName")
    public boolean is_allow_auto_update_schema = true;

    @SuppressWarnings("checkstyle:MemberName")
    public boolean schema_validation_enforced = false;

    @SuppressWarnings("checkstyle:MemberName")
    public OffloadPolicies offload_policies = null;

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
                antiAffinityGroup, max_producers_per_topic,
                max_consumers_per_topic, max_consumers_per_subscription,
                max_unacked_messages_per_consumer, max_unacked_messages_per_subscription,
                compaction_threshold, offload_threshold,
                offload_deletion_lag_ms,
                schema_auto_update_compatibility_strategy,
                schema_validation_enforced,
                schema_compatibility_strategy,
                is_allow_auto_update_schema,
                offload_policies);
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
                    && Objects.equals(antiAffinityGroup, other.antiAffinityGroup)
                    && max_producers_per_topic == other.max_producers_per_topic
                    && max_consumers_per_topic == other.max_consumers_per_topic
                    && max_consumers_per_subscription == other.max_consumers_per_subscription
                    && max_unacked_messages_per_consumer == other.max_unacked_messages_per_consumer
                    && max_unacked_messages_per_subscription == other.max_unacked_messages_per_subscription
                    && compaction_threshold == other.compaction_threshold
                    && offload_threshold == other.offload_threshold
                    && Objects.equals(offload_deletion_lag_ms, other.offload_deletion_lag_ms)
                    && schema_auto_update_compatibility_strategy == other.schema_auto_update_compatibility_strategy
                    && schema_validation_enforced == other.schema_validation_enforced
                    && schema_compatibility_strategy == other.schema_compatibility_strategy
                    && is_allow_auto_update_schema == other.is_allow_auto_update_schema
                    && Objects.equals(offload_policies, other.offload_policies);
        }

        return false;
    }

    public static BundlesData defaultBundle() {
        BundlesData bundle = new BundlesData(1);
        List<String> boundaries = Lists.newArrayList();
        boundaries.add(FIRST_BOUNDARY);
        boundaries.add(LAST_BOUNDARY);
        bundle.setBoundaries(boundaries);
        return bundle;
    }

    public static void setStorageQuota(Policies polices, BacklogQuota quota) {
        if (polices == null) {
            return;
        }
        polices.backlog_quota_map.put(BacklogQuota.BacklogQuotaType.destination_storage, quota);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("auth_policies", auth_policies)
                .add("replication_clusters", replication_clusters)
                .add("bundles", bundles)
                .add("backlog_quota_map", backlog_quota_map)
                .add("persistence", persistence)
                .add("deduplicationEnabled", deduplicationEnabled)
                .add("autoTopicCreationOverride", autoTopicCreationOverride)
                .add("autoSubscriptionCreationOverride", autoSubscriptionCreationOverride)
                .add("clusterDispatchRate", clusterDispatchRate)
                .add("topicDispatchRate", topicDispatchRate)
                .add("subscriptionDispatchRate", subscriptionDispatchRate)
                .add("replicatorDispatchRate", replicatorDispatchRate)
                .add("clusterSubscribeRate", clusterSubscribeRate)
                .add("publishMaxMessageRate", publishMaxMessageRate)
                .add("latency_stats_sample_rate", latency_stats_sample_rate)
                .add("antiAffinityGroup", antiAffinityGroup)
                .add("message_ttl_in_seconds", message_ttl_in_seconds)
                .add("subscription_expiration_time_minutes", subscription_expiration_time_minutes)
                .add("retention_policies", retention_policies)
                .add("deleted", deleted)
                .add("encryption_required", encryption_required)
                .add("delayed_delivery_policies", delayed_delivery_policies)
                .add("inactive_topic_policies", inactive_topic_policies)
                .add("subscription_auth_mode", subscription_auth_mode)
                .add("max_producers_per_topic", max_producers_per_topic)
                .add("max_consumers_per_topic", max_consumers_per_topic)
                .add("max_consumers_per_subscription", max_consumers_per_topic)
                .add("max_unacked_messages_per_consumer", max_unacked_messages_per_consumer)
                .add("max_unacked_messages_per_subscription", max_unacked_messages_per_subscription)
                .add("compaction_threshold", compaction_threshold)
                .add("offload_threshold", offload_threshold)
                .add("offload_deletion_lag_ms", offload_deletion_lag_ms)
                .add("schema_auto_update_compatibility_strategy", schema_auto_update_compatibility_strategy)
                .add("schema_validation_enforced", schema_validation_enforced)
                .add("schema_compatibility_Strategy", schema_compatibility_strategy)
                .add("is_allow_auto_update_Schema", is_allow_auto_update_schema)
                .add("offload_policies", offload_policies).toString();
    }
}
