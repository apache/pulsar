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

import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.policies.data.impl.AutoSubscriptionCreationOverrideImpl;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;

/**
 * The topic-level policies in effect for a topic, resolved through the cascade topic -> namespace -> broker.
 *
 * <p>There is one field for each policy of {@link TopicPolicies}, with the same name. Unlike {@link TopicPolicies},
 * where {@code null} means "not set on the topic", each field holds the value in effect and the
 * {@link PolicySource level} it came from. Policies that exist only at topic level are returned as stored.
 */
@Data
@NoArgsConstructor
public class AppliedTopicPolicies {

    private AppliedValue<List<String>> replicationClusters;
    private AppliedValue<List<String>> shadowTopics;
    private AppliedValue<PersistencePolicies> persistence;
    private AppliedValue<RetentionPolicies> retentionPolicies;
    private AppliedValue<Boolean> deduplicationEnabled;
    private AppliedValue<Integer> messageTTLInSeconds;
    private AppliedValue<Integer> subscriptionExpirationTimeInMinutes;
    private AppliedValue<Integer> maxProducerPerTopic;
    private AppliedValue<Integer> maxConsumerPerTopic;
    private AppliedValue<Integer> maxConsumersPerSubscription;
    private AppliedValue<Integer> maxUnackedMessagesOnConsumer;
    private AppliedValue<Integer> maxUnackedMessagesOnSubscription;
    private AppliedValue<Long> delayedDeliveryTickTimeMillis;
    private AppliedValue<Boolean> delayedDeliveryEnabled;
    private AppliedValue<Long> delayedDeliveryMaxDelayInMillis;
    private AppliedValue<Boolean> dispatcherPauseOnAckStatePersistentEnabled;
    private AppliedValue<OffloadPoliciesImpl> offloadPolicies;
    private AppliedValue<InactiveTopicPolicies> inactiveTopicPolicies;
    private AppliedValue<DispatchRateImpl> dispatchRate;
    private AppliedValue<DispatchRateImpl> subscriptionDispatchRate;
    private AppliedValue<Long> compactionThreshold;
    private AppliedValue<PublishRate> publishRate;
    private AppliedValue<SubscribeRate> subscribeRate;
    private AppliedValue<Integer> deduplicationSnapshotIntervalSeconds;
    private AppliedValue<Integer> maxMessageSize;
    private AppliedValue<Integer> maxSubscriptionsPerTopic;
    private AppliedValue<DispatchRateImpl> replicatorDispatchRate;
    private AppliedValue<SchemaCompatibilityStrategy> schemaCompatibilityStrategy;
    private AppliedValue<EntryFilters> entryFilters;
    private AppliedValue<AutoSubscriptionCreationOverrideImpl> autoSubscriptionCreationOverride;
    private AppliedValue<Boolean> schemaValidationEnforced;
    private AppliedValue<List<SubType>> subscriptionTypesEnabled;
    private Map<String, AppliedValue<BacklogQuotaImpl>> backLogQuotaMap;
    private Map<String, SubscriptionPolicies> subscriptionPolicies;
}
