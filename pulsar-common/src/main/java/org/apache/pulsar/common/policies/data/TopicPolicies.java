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

import com.google.common.collect.Maps;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


/**
 * Topic policies.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopicPolicies {

    private Map<String, BacklogQuota> backLogQuotaMap = Maps.newHashMap();
    private PersistencePolicies persistence = null;
    private RetentionPolicies retentionPolicies = null;
    private Boolean deduplicationEnabled = null;
    private Integer messageTTLInSeconds = null;
    private Integer maxProducerPerTopic = null;
    private Integer maxConsumerPerTopic = null;
    private Integer maxConsumersPerSubscription = null;
    private Integer maxUnackedMessagesOnConsumer = null;
    private Integer maxUnackedMessagesOnSubscription = null;
    private Long delayedDeliveryTickTimeMillis = null;
    private Boolean delayedDeliveryEnabled = null;
    private OffloadPolicies offloadPolicies;
    private InactiveTopicPolicies inactiveTopicPolicies = null;
    private DispatchRate dispatchRate = null;
    private Long compactionThreshold = null;
    private PublishRate publishRate = null;

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

    public boolean isCompactionThresholdSet() {
        return compactionThreshold != null;
    }

    public boolean isPublishRateSet() {
        return publishRate != null;
    }
}
