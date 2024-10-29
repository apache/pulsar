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
package org.apache.pulsar.common.policies.data;

import com.google.common.collect.ImmutableMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;

/**
 * Topic policy hierarchy value container.
 */
@Getter
public class HierarchyTopicPolicies {
    final PolicyHierarchyValue<List<String>> replicationClusters;
    final PolicyHierarchyValue<RetentionPolicies> retentionPolicies;
    final PolicyHierarchyValue<Boolean> deduplicationEnabled;
    final PolicyHierarchyValue<Integer> deduplicationSnapshotIntervalSeconds;
    final PolicyHierarchyValue<InactiveTopicPolicies> inactiveTopicPolicies;
    final PolicyHierarchyValue<EnumSet<SubType>> subscriptionTypesEnabled;
    final PolicyHierarchyValue<Integer> maxSubscriptionsPerTopic;
    final PolicyHierarchyValue<Integer> maxUnackedMessagesOnConsumer;
    final PolicyHierarchyValue<Integer> maxUnackedMessagesOnSubscription;
    final PolicyHierarchyValue<Integer> maxProducersPerTopic;
    final Map<BacklogQuotaType, PolicyHierarchyValue<BacklogQuota>> backLogQuotaMap;
    final PolicyHierarchyValue<Integer> topicMaxMessageSize;
    final PolicyHierarchyValue<Integer> messageTTLInSeconds;
    final PolicyHierarchyValue<Long> compactionThreshold;
    final PolicyHierarchyValue<Integer> maxConsumerPerTopic;
    final PolicyHierarchyValue<PublishRate> publishRate;
    final PolicyHierarchyValue<Boolean> delayedDeliveryEnabled;
    final PolicyHierarchyValue<Long> delayedDeliveryTickTimeMillis;
    final PolicyHierarchyValue<DispatchRateImpl> replicatorDispatchRate;
    final PolicyHierarchyValue<Integer> maxConsumersPerSubscription;
    final PolicyHierarchyValue<SubscribeRate> subscribeRate;
    final PolicyHierarchyValue<DispatchRateImpl> subscriptionDispatchRate;
    final PolicyHierarchyValue<SchemaCompatibilityStrategy> schemaCompatibilityStrategy;
    final PolicyHierarchyValue<DispatchRateImpl> dispatchRate;

    final PolicyHierarchyValue<Boolean> schemaValidationEnforced;
    final PolicyHierarchyValue<EntryFilters> entryFilters;

    final PolicyHierarchyValue<String> resourceGroupName;

    public HierarchyTopicPolicies() {
        replicationClusters = new PolicyHierarchyValue<>();
        retentionPolicies = new PolicyHierarchyValue<>();
        deduplicationEnabled = new PolicyHierarchyValue<>();
        deduplicationSnapshotIntervalSeconds = new PolicyHierarchyValue<>();
        inactiveTopicPolicies = new PolicyHierarchyValue<>();
        subscriptionTypesEnabled = new PolicyHierarchyValue<>();
        maxSubscriptionsPerTopic = new PolicyHierarchyValue<>();
        maxUnackedMessagesOnConsumer = new PolicyHierarchyValue<>();
        maxUnackedMessagesOnSubscription = new PolicyHierarchyValue<>();
        maxProducersPerTopic = new PolicyHierarchyValue<>();
        maxConsumerPerTopic = new PolicyHierarchyValue<>();
        maxConsumersPerSubscription = new PolicyHierarchyValue<>();
        backLogQuotaMap = new ImmutableMap.Builder<BacklogQuotaType, PolicyHierarchyValue<BacklogQuota>>()
                .put(BacklogQuotaType.destination_storage, new PolicyHierarchyValue<>())
                .put(BacklogQuotaType.message_age, new PolicyHierarchyValue<>())
                .build();
        topicMaxMessageSize = new PolicyHierarchyValue<>();
        messageTTLInSeconds = new PolicyHierarchyValue<>();
        publishRate = new PolicyHierarchyValue<>();
        delayedDeliveryEnabled = new PolicyHierarchyValue<>();
        delayedDeliveryTickTimeMillis = new PolicyHierarchyValue<>();
        replicatorDispatchRate = new PolicyHierarchyValue<>();
        compactionThreshold = new PolicyHierarchyValue<>();
        subscribeRate = new PolicyHierarchyValue<>();
        subscriptionDispatchRate = new PolicyHierarchyValue<>();
        schemaCompatibilityStrategy = new PolicyHierarchyValue<>();
        dispatchRate = new PolicyHierarchyValue<>();
        schemaValidationEnforced = new PolicyHierarchyValue<>();
        entryFilters = new PolicyHierarchyValue<>();
        resourceGroupName = new PolicyHierarchyValue<>();
    }
}
