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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class Policies {

    public final AuthPolicies auth_policies = new AuthPolicies();
    public Set<String> replication_clusters = Sets.newHashSet();
    public BundlesData bundles;
    public Map<BacklogQuota.BacklogQuotaType, BacklogQuota> backlog_quota_map = Maps.newHashMap();
    public Map<String, DispatchRate> topicDispatchRate = Maps.newHashMap();
    public Map<String, DispatchRate> subscriptionDispatchRate = Maps.newHashMap();
    public Map<String, DispatchRate> replicatorDispatchRate = Maps.newHashMap();
    public Map<String, SubscribeRate> clusterSubscribeRate = Maps.newHashMap();
    public PersistencePolicies persistence = null;

    // If set, it will override the broker settings for enabling deduplication
    public Boolean deduplicationEnabled = null;

    public Map<String, Integer> latency_stats_sample_rate = Maps.newHashMap();
    public int message_ttl_in_seconds = 0;
    public RetentionPolicies retention_policies = null;
    public boolean deleted = false;
    public String antiAffinityGroup;

    public static final String FIRST_BOUNDARY = "0x00000000";
    public static final String LAST_BOUNDARY = "0xffffffff";

    public boolean encryption_required = false;
    public SubscriptionAuthMode subscription_auth_mode = SubscriptionAuthMode.None;

    public int max_producers_per_topic = 0;
    public int max_consumers_per_topic = 0;
    public int max_consumers_per_subscription = 0;

    public long compaction_threshold = 0;
    public long offload_threshold = -1;
    public Long offload_deletion_lag_ms = null;

    public SchemaAutoUpdateCompatibilityStrategy schema_auto_update_compatibility_strategy =
        SchemaAutoUpdateCompatibilityStrategy.Full;

    public boolean schema_validation_enforced = false;

    @Override
    public int hashCode() {
        return Objects.hash(auth_policies, replication_clusters,
                backlog_quota_map,
                topicDispatchRate, subscriptionDispatchRate, replicatorDispatchRate,
                clusterSubscribeRate, deduplicationEnabled, persistence,
                bundles, latency_stats_sample_rate,
                message_ttl_in_seconds, retention_policies,
                encryption_required, subscription_auth_mode,
                antiAffinityGroup, max_producers_per_topic,
                max_consumers_per_topic, max_consumers_per_subscription,
                compaction_threshold, offload_threshold,
                offload_deletion_lag_ms,
                schema_auto_update_compatibility_strategy,
                schema_validation_enforced);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Policies) {
            Policies other = (Policies) obj;
            return Objects.equals(auth_policies, other.auth_policies)
                    && Objects.equals(replication_clusters, other.replication_clusters)
                    && Objects.equals(backlog_quota_map, other.backlog_quota_map)
                    && Objects.equals(topicDispatchRate, other.topicDispatchRate)
                    && Objects.equals(subscriptionDispatchRate, other.subscriptionDispatchRate)
                    && Objects.equals(replicatorDispatchRate, other.replicatorDispatchRate)
                    && Objects.equals(clusterSubscribeRate, other.clusterSubscribeRate)
                    && Objects.equals(deduplicationEnabled, other.deduplicationEnabled)
                    && Objects.equals(persistence, other.persistence) && Objects.equals(bundles, other.bundles)
                    && Objects.equals(latency_stats_sample_rate, other.latency_stats_sample_rate)
                    && Objects.equals(message_ttl_in_seconds,
                            other.message_ttl_in_seconds)
                    && Objects.equals(retention_policies, other.retention_policies)
                    && Objects.equals(encryption_required, other.encryption_required)
                    && Objects.equals(subscription_auth_mode, other.subscription_auth_mode)
                    && Objects.equals(antiAffinityGroup, other.antiAffinityGroup)
                    && max_producers_per_topic == other.max_producers_per_topic
                    && max_consumers_per_topic == other.max_consumers_per_topic
                    && max_consumers_per_subscription == other.max_consumers_per_subscription
                    && compaction_threshold == other.compaction_threshold
                    && offload_threshold == other.offload_threshold
                    && offload_deletion_lag_ms == other.offload_deletion_lag_ms
                    && schema_auto_update_compatibility_strategy == other.schema_auto_update_compatibility_strategy
                    && schema_validation_enforced == other.schema_validation_enforced;
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
                .add("replication_clusters", replication_clusters).add("bundles", bundles)
                .add("backlog_quota_map", backlog_quota_map).add("persistence", persistence)
                .add("deduplicationEnabled", deduplicationEnabled)
                .add("topicDispatchRate", topicDispatchRate)
                .add("subscriptionDispatchRate", subscriptionDispatchRate)
                .add("replicatorDispatchRate", replicatorDispatchRate)
                .add("clusterSubscribeRate", clusterSubscribeRate)
                .add("latency_stats_sample_rate", latency_stats_sample_rate)
                .add("antiAffinityGroup", antiAffinityGroup)
                .add("message_ttl_in_seconds", message_ttl_in_seconds).add("retention_policies", retention_policies)
                .add("deleted", deleted)
                .add("encryption_required", encryption_required)
                .add("subscription_auth_mode", subscription_auth_mode)
                .add("max_producers_per_topic", max_producers_per_topic)
                .add("max_consumers_per_topic", max_consumers_per_topic)
                .add("max_consumers_per_subscription", max_consumers_per_topic)
                .add("compaction_threshold", compaction_threshold)
                .add("offload_threshold", offload_threshold)
                .add("offload_deletion_lag_ms", offload_deletion_lag_ms)
                .add("schema_auto_update_compatibility_strategy", schema_auto_update_compatibility_strategy)
                .add("schema_validation_enforced", schema_validation_enforced).toString();
    }
}
