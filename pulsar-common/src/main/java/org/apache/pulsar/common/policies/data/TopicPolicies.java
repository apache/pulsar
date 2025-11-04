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

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.policies.data.impl.AutoSubscriptionCreationOverrideImpl;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;


/**
 * Topic policies.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopicPolicies implements Cloneable {

    @Builder.Default
    private Map<String, BacklogQuotaImpl> backLogQuotaMap = new HashMap<>();
    @Builder.Default
    private List<SubType> subscriptionTypesEnabled = new ArrayList<>();
    private List<String> replicationClusters;
    private List<String> shadowTopics;
    private Boolean isGlobal = false;
    private PersistencePolicies persistence;
    private RetentionPolicies retentionPolicies;
    private Boolean deduplicationEnabled;
    private Integer messageTTLInSeconds;
    private Integer maxProducerPerTopic;
    private Integer maxConsumerPerTopic;
    private Integer maxConsumersPerSubscription;
    private Integer maxUnackedMessagesOnConsumer;
    private Integer maxUnackedMessagesOnSubscription;
    private Long delayedDeliveryTickTimeMillis;
    private Boolean delayedDeliveryEnabled;
    private Long delayedDeliveryMaxDelayInMillis;
    private Boolean dispatcherPauseOnAckStatePersistentEnabled;
    private OffloadPoliciesImpl offloadPolicies;
    private InactiveTopicPolicies inactiveTopicPolicies;
    private DispatchRateImpl dispatchRate;
    private DispatchRateImpl subscriptionDispatchRate;
    private Long compactionThreshold;
    private PublishRate publishRate;
    private SubscribeRate subscribeRate;
    private Integer deduplicationSnapshotIntervalSeconds;
    private Integer maxMessageSize;
    private Integer maxSubscriptionsPerTopic;
    private DispatchRateImpl replicatorDispatchRate;
    private SchemaCompatibilityStrategy schemaCompatibilityStrategy;
    private EntryFilters entryFilters;
    // If set, it will override the namespace settings for allowing auto subscription creation
    private AutoSubscriptionCreationOverrideImpl autoSubscriptionCreationOverride;

    /**
     * Subscription level policies for specific subscription.
     */
    @Builder.Default
    private Map<String/*subscription*/, SubscriptionPolicies> subscriptionPolicies = new HashMap<>();

    private Boolean schemaValidationEnforced;

    @SneakyThrows
    @Override
    public TopicPolicies clone() {
        TopicPolicies cloned = TopicPolicies.class.cast(super.clone());

        if (this.backLogQuotaMap != null) {
            cloned.backLogQuotaMap = new HashMap<>();
            for (Map.Entry<String, BacklogQuotaImpl> entry : this.backLogQuotaMap.entrySet()) {
                cloned.backLogQuotaMap.put(entry.getKey(),
                        entry.getValue() != null ? entry.getValue().clone() : null);
            }
        } else {
            cloned.backLogQuotaMap = new HashMap<>();
        }

        cloned.subscriptionTypesEnabled = this.subscriptionTypesEnabled != null
                ? new ArrayList<>(this.subscriptionTypesEnabled) : new ArrayList<>();
        cloned.replicationClusters = this.replicationClusters != null
                ? new ArrayList<>(this.replicationClusters) : null;
        cloned.shadowTopics = this.shadowTopics != null ? new ArrayList<>(this.shadowTopics) : null;

        cloned.persistence = this.persistence != null ? this.persistence.clone() : null;
        cloned.retentionPolicies = this.retentionPolicies != null ? this.retentionPolicies.clone() : null;

        cloned.offloadPolicies =
                this.offloadPolicies != null ? OffloadPoliciesImpl.create(this.offloadPolicies.toProperties()) : null;

        cloned.inactiveTopicPolicies =
                this.inactiveTopicPolicies != null ? this.inactiveTopicPolicies.clone() : null;

        cloned.dispatchRate = this.dispatchRate != null ? this.dispatchRate.clone() : null;
        cloned.subscriptionDispatchRate =
                this.subscriptionDispatchRate != null ? this.subscriptionDispatchRate.clone() : null;

        cloned.publishRate = this.publishRate != null ? this.publishRate.clone() : null;
        cloned.subscribeRate = this.subscribeRate != null ? this.subscribeRate.clone() : null;

        cloned.entryFilters =
                this.entryFilters != null ? new EntryFilters(this.entryFilters.getEntryFilterNames()) : null;
        cloned.autoSubscriptionCreationOverride = this.autoSubscriptionCreationOverride != null
                ? AutoSubscriptionCreationOverrideImpl.builder()
                .allowAutoSubscriptionCreation(
                        this.autoSubscriptionCreationOverride.isAllowAutoSubscriptionCreation())
                .build() : null;
        cloned.replicatorDispatchRate =
                this.replicatorDispatchRate != null ? this.replicatorDispatchRate.clone() : null;


        if (this.subscriptionPolicies != null) {
            cloned.subscriptionPolicies = new HashMap<>();
            for (Map.Entry<String, SubscriptionPolicies> entry : this.subscriptionPolicies.entrySet()) {
                cloned.subscriptionPolicies.put(entry.getKey(),
                        entry.getValue() != null ? entry.getValue().clone() : null);
            }
        } else {
            cloned.subscriptionPolicies = new HashMap<>();
        }

        // Primitive types (Boolean, Integer, Long, String) and enums (SchemaCompatibilityStrategy)
        // are fine with the shallow copy from super.clone().
        // isGlobal, deduplicationEnabled, messageTTLInSeconds, maxProducerPerTopic, etc.

        return cloned;
    }

    public boolean isGlobalPolicies() {
        return isGlobal != null && isGlobal;
    }

    public boolean isReplicatorDispatchRateSet() {
        return replicatorDispatchRate != null;
    }

    public boolean isMaxSubscriptionsPerTopicSet() {
        return maxSubscriptionsPerTopic != null;
    }

    public boolean isMaxMessageSizeSet() {
        return maxMessageSize != null;
    }

    public boolean isDeduplicationSnapshotIntervalSecondsSet(){
        return deduplicationSnapshotIntervalSeconds != null;
    }

    public boolean isInactiveTopicPoliciesSet() {
        return inactiveTopicPolicies != null;
    }

    public boolean isOffloadPoliciesSet() {
        return offloadPolicies != null;
    }

    public boolean isMaxUnackedMessagesOnConsumerSet() {
        return maxUnackedMessagesOnConsumer != null;
    }

    public boolean isDelayedDeliveryTickTimeMillisSet(){
        return delayedDeliveryTickTimeMillis != null;
    }

    public boolean isDelayedDeliveryEnabledSet(){
        return delayedDeliveryEnabled != null;
    }

    public boolean isDelayedDeliveryMaxDelayInMillisSet(){
        return delayedDeliveryMaxDelayInMillis != null;
    }

    public boolean isMaxUnackedMessagesOnSubscriptionSet() {
        return maxUnackedMessagesOnSubscription != null;
    }

    public boolean isBacklogQuotaSet() {
        return !backLogQuotaMap.isEmpty();
    }

    public boolean isPersistentPolicySet() {
        return persistence != null;
    }

    public boolean isRetentionSet() {
        return retentionPolicies != null;
    }

    public boolean isDeduplicationSet() {
        return deduplicationEnabled != null;
    }

    public boolean isMessageTTLSet() {
        return messageTTLInSeconds != null;
    }

    public boolean isMaxProducerPerTopicSet() {
        return maxProducerPerTopic != null;
    }

    public boolean isMaxConsumerPerTopicSet() {
        return maxConsumerPerTopic != null;
    }

    public boolean isMaxConsumersPerSubscriptionSet() {
        return maxConsumersPerSubscription != null;
    }

    public boolean isDispatchRateSet() {
        return dispatchRate != null;
    }

    public boolean isSubscriptionDispatchRateSet() {
        return subscriptionDispatchRate != null;
    }

    public boolean isCompactionThresholdSet() {
        return compactionThreshold != null;
    }

    public boolean isPublishRateSet() {
        return publishRate != null;
    }

    public boolean isSubscribeRateSet() {
        return subscribeRate != null;
    }

    public boolean isSchemaValidationEnforced() {
        return schemaValidationEnforced != null;
    }

    public Set<String> getReplicationClustersSet() {
        return replicationClusters != null ? Sets.newTreeSet(this.replicationClusters) : null;
    }

    public Map<String, SubscriptionPolicies> getSubscriptionPolicies() {
        if (subscriptionPolicies == null) {
            subscriptionPolicies = new ConcurrentHashMap<>();
        }
        return subscriptionPolicies;
    }
}
