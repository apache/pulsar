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
package org.apache.pulsar.client.admin;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscribeRate;

/**
 * Admin interface for topic policies management.
 */
public interface TopicPolicies {

    /**
     * Get backlog quota map for a topic.
     * @param topic Topic name
     * @throws PulsarAdminException.NotFoundException Topic does not exist
     * @throws PulsarAdminException Unexpected error
     */
    Map<BacklogQuota.BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(String topic)
            throws PulsarAdminException;

    /**
     * Get applied backlog quota map for a topic.
     * @param topic
     * @param applied
     * @return
     * @throws PulsarAdminException
     */
    Map<BacklogQuota.BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(String topic, boolean applied)
            throws PulsarAdminException;

    /**
     * Set a backlog quota for a topic.
     * @param topic
     *            Topic name
     * @param backlogQuota
     *            the new BacklogQuota
     * @param backlogQuotaType
     *
     * @throws PulsarAdminException.NotFoundException
     *             Topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setBacklogQuota(String topic, BacklogQuota backlogQuota,
                         BacklogQuota.BacklogQuotaType backlogQuotaType) throws PulsarAdminException;

    default void setBacklogQuota(String topic, BacklogQuota backlogQuota) throws PulsarAdminException {
        setBacklogQuota(topic, backlogQuota, BacklogQuota.BacklogQuotaType.destination_storage);
    }

    /**
     * Remove a backlog quota policy from a topic.
     * The namespace backlog policy falls back to the default.
     *
     * @param topic
     *            Topic name
     * @param backlogQuotaType
     *
     * @throws PulsarAdminException.NotFoundException
     *             Topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void removeBacklogQuota(String topic, BacklogQuota.BacklogQuotaType backlogQuotaType) throws PulsarAdminException;

    default void removeBacklogQuota(String topic)
            throws PulsarAdminException {
        removeBacklogQuota(topic, BacklogQuota.BacklogQuotaType.destination_storage);
    }

    /**
     * Get the delayed delivery policy applied for a specified topic.
     * @param topic
     * @param applied
     * @return
     * @throws PulsarAdminException
     */
    DelayedDeliveryPolicies getDelayedDeliveryPolicy(String topic
            , boolean applied) throws PulsarAdminException;

    /**
     * Get the delayed delivery policy applied for a specified topic asynchronously.
     * @param topic
     * @param applied
     * @return
     */
    CompletableFuture<DelayedDeliveryPolicies> getDelayedDeliveryPolicyAsync(String topic
            , boolean applied);
    /**
     * Get the delayed delivery policy for a specified topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    DelayedDeliveryPolicies getDelayedDeliveryPolicy(String topic) throws PulsarAdminException;

    /**
     * Get the delayed delivery policy for a specified topic asynchronously.
     * @param topic
     * @return
     */
    CompletableFuture<DelayedDeliveryPolicies> getDelayedDeliveryPolicyAsync(String topic);

    /**
     * Set the delayed delivery policy for a specified topic.
     * @param topic
     * @param delayedDeliveryPolicies
     * @throws PulsarAdminException
     */
    void setDelayedDeliveryPolicy(String topic
            , DelayedDeliveryPolicies delayedDeliveryPolicies) throws PulsarAdminException;

    /**
     * Set the delayed delivery policy for a specified topic asynchronously.
     * @param topic
     * @param delayedDeliveryPolicies
     * @return
     */
    CompletableFuture<Void> setDelayedDeliveryPolicyAsync(String topic
            , DelayedDeliveryPolicies delayedDeliveryPolicies);

    /**
     * Remove the delayed delivery policy for a specified topic asynchronously.
     * @param topic
     * @return
     */
    CompletableFuture<Void> removeDelayedDeliveryPolicyAsync(String topic);

    /**
     * Remove the delayed delivery policy for a specified topic.
     * @param topic
     * @throws PulsarAdminException
     */
    void removeDelayedDeliveryPolicy(String topic) throws PulsarAdminException;

    /**
     * Set message TTL for a topic.
     *
     * @param topic
     *          Topic name
     * @param messageTTLInSecond
     *          Message TTL in second.
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws PulsarAdminException.NotFoundException
     *             Topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setMessageTTL(String topic, int messageTTLInSecond) throws PulsarAdminException;

    /**
     * Get message TTL for a topic.
     *
     * @param topic
     * @return Message TTL in second.
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws PulsarAdminException.NotFoundException
     *             Topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    Integer getMessageTTL(String topic) throws PulsarAdminException;

    /**
     * Get message TTL applied for a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    Integer getMessageTTL(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Remove message TTL for a topic.
     *
     * @param topic
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws PulsarAdminException.NotFoundException
     *             Topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void removeMessageTTL(String topic) throws PulsarAdminException;

    /**
     * Set the retention configuration on a topic.
     * <p/>
     * Set the retention configuration on a topic. This operation requires Pulsar super-user access.
     * <p/>
     * Request parameter example:
     * <p/>
     *
     * <pre>
     * <code>
     * {
     *     "retentionTimeInMinutes" : 60,            // how long to retain messages
     *     "retentionSizeInMB" : 1024,              // retention backlog limit
     * }
     * </code>
     * </pre>
     *
     * @param topic
     *            Topic name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws PulsarAdminException.NotFoundException
     *             Topic does not exist
     * @throws ConflictException
     *             Concurrent modification
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setRetention(String topic, RetentionPolicies retention) throws PulsarAdminException;

    /**
     * Set the retention configuration for all the topics on a topic asynchronously.
     * <p/>
     * Set the retention configuration on a topic. This operation requires Pulsar super-user access.
     * <p/>
     * Request parameter example:
     * <p/>
     *
     * <pre>
     * <code>
     * {
     *     "retentionTimeInMinutes" : 60,            // how long to retain messages
     *     "retentionSizeInMB" : 1024,              // retention backlog limit
     * }
     * </code>
     * </pre>
     *
     * @param topic
     *            Topic name
     */
    CompletableFuture<Void> setRetentionAsync(String topic, RetentionPolicies retention);

    /**
     * Get the retention configuration for a topic.
     * <p/>
     * Get the retention configuration for a topic.
     * <p/>
     * Response example:
     * <p/>
     *
     * <pre>
     * <code>
     * {
     *     "retentionTimeInMinutes" : 60,            // how long to retain messages
     *     "retentionSizeInMB" : 1024,              // retention backlog limit
     * }
     * </code>
     * </pre>
     *
     * @param topic
     *            Topic name
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws PulsarAdminException.NotFoundException
     *             Topic does not exist
     * @throws ConflictException
     *             Concurrent modification
     * @throws PulsarAdminException
     *             Unexpected error
     */
    RetentionPolicies getRetention(String topic) throws PulsarAdminException;

    /**
     * Get the retention configuration for a topic asynchronously.
     * <p/>
     * Get the retention configuration for a topic.
     * <p/>
     *
     * @param topic
     *            Topic name
     */
    CompletableFuture<RetentionPolicies> getRetentionAsync(String topic);

    /**
     * Get the applied retention configuration for a topic.
     * @param topic
     * @param applied
     * @return
     * @throws PulsarAdminException
     */
    RetentionPolicies getRetention(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get the applied retention configuration for a topic asynchronously.
     * @param topic
     * @param applied
     * @return
     */
    CompletableFuture<RetentionPolicies> getRetentionAsync(String topic, boolean applied);

    /**
     * Remove the retention configuration for all the topics on a topic.
     * <p/>
     * Remove the retention configuration on a topic. This operation requires Pulsar super-user access.
     * <p/>
     * Request parameter example:
     * <p/>
     *
     * @param topic
     *            Topic name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws PulsarAdminException.NotFoundException
     *             Topic does not exist
     * @throws ConflictException
     *             Concurrent modification
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void removeRetention(String topic) throws PulsarAdminException;

    /**
     * Remove the retention configuration for all the topics on a topic asynchronously.
     * <p/>
     * Remove the retention configuration on a topic. This operation requires Pulsar super-user access.
     * <p/>
     * Request parameter example:
     * <p/>
     *
     * <pre>
     * <code>
     * {
     *     "retentionTimeInMinutes" : 60,            // how long to retain messages
     *     "retentionSizeInMB" : 1024,              // retention backlog limit
     * }
     * </code>
     * </pre>
     *
     * @param topic
     *            Topic name
     */
    CompletableFuture<Void> removeRetentionAsync(String topic);

    /**
     * Get max unacked messages on a consumer of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    Integer getMaxUnackedMessagesOnConsumer(String topic) throws PulsarAdminException;

    /**
     * get max unacked messages on consumer of a topic asynchronously.
     * @param topic
     * @return
     */
    CompletableFuture<Integer> getMaxUnackedMessagesOnConsumerAsync(String topic);

    /**
     * get applied max unacked messages on consumer of a topic.
     * @param topic
     * @param applied
     * @return
     * @throws PulsarAdminException
     */
    Integer getMaxUnackedMessagesOnConsumer(String topic, boolean applied) throws PulsarAdminException;

    /**
     * get applied max unacked messages on consumer of a topic asynchronously.
     * @param topic
     * @param applied
     * @return
     */
    CompletableFuture<Integer> getMaxUnackedMessagesOnConsumerAsync(String topic, boolean applied);

    /**
     * set max unacked messages on consumer of a topic.
     * @param topic
     * @param maxNum
     * @throws PulsarAdminException
     */
    void setMaxUnackedMessagesOnConsumer(String topic, int maxNum) throws PulsarAdminException;

    /**
     * set max unacked messages on consumer of a topic asynchronously.
     * @param topic
     * @param maxNum
     * @return
     */
    CompletableFuture<Void> setMaxUnackedMessagesOnConsumerAsync(String topic, int maxNum);

    /**
     * remove max unacked messages on consumer of a topic.
     * @param topic
     * @throws PulsarAdminException
     */
    void removeMaxUnackedMessagesOnConsumer(String topic) throws PulsarAdminException;

    /**
     * remove max unacked messages on consumer of a topic asynchronously.
     * @param topic
     * @return
     */
    CompletableFuture<Void> removeMaxUnackedMessagesOnConsumerAsync(String topic);

    /**
     * Get inactive topic policies applied for a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    InactiveTopicPolicies getInactiveTopicPolicies(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get inactive topic policies applied for a topic asynchronously.
     * @param topic
     * @param applied
     * @return
     */
    CompletableFuture<InactiveTopicPolicies> getInactiveTopicPoliciesAsync(String topic, boolean applied);
    /**
     * get inactive topic policies of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    InactiveTopicPolicies getInactiveTopicPolicies(String topic) throws PulsarAdminException;

    /**
     * get inactive topic policies of a topic asynchronously.
     * @param topic
     * @return
     */
    CompletableFuture<InactiveTopicPolicies> getInactiveTopicPoliciesAsync(String topic);

    /**
     * set inactive topic policies of a topic.
     * @param topic
     * @param inactiveTopicPolicies
     * @throws PulsarAdminException
     */
    void setInactiveTopicPolicies(String topic
            , InactiveTopicPolicies inactiveTopicPolicies) throws PulsarAdminException;

    /**
     * set inactive topic policies of a topic asynchronously.
     * @param topic
     * @param inactiveTopicPolicies
     * @return
     */
    CompletableFuture<Void> setInactiveTopicPoliciesAsync(String topic, InactiveTopicPolicies inactiveTopicPolicies);

    /**
     * remove inactive topic policies of a topic.
     * @param topic
     * @throws PulsarAdminException
     */
    void removeInactiveTopicPolicies(String topic) throws PulsarAdminException;

    /**
     * remove inactive topic policies of a topic asynchronously.
     * @param topic
     * @return
     */
    CompletableFuture<Void> removeInactiveTopicPoliciesAsync(String topic);

    /**
     * get offload policies of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    OffloadPolicies getOffloadPolicies(String topic) throws PulsarAdminException;

    /**
     * get offload policies of a topic asynchronously.
     * @param topic
     * @return
     */
    CompletableFuture<OffloadPolicies> getOffloadPoliciesAsync(String topic);

    /**
     * get applied offload policies of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    OffloadPolicies getOffloadPolicies(String topic, boolean applied) throws PulsarAdminException;

    /**
     * get applied offload policies of a topic asynchronously.
     * @param topic
     * @return
     */
    CompletableFuture<OffloadPolicies> getOffloadPoliciesAsync(String topic, boolean applied);

    /**
     * set offload policies of a topic.
     * @param topic
     * @param offloadPolicies
     * @throws PulsarAdminException
     */
    void setOffloadPolicies(String topic, OffloadPolicies offloadPolicies) throws PulsarAdminException;

    /**
     * set offload policies of a topic asynchronously.
     * @param topic
     * @param offloadPolicies
     * @return
     */
    CompletableFuture<Void> setOffloadPoliciesAsync(String topic, OffloadPolicies offloadPolicies);

    /**
     * remove offload policies of a topic.
     * @param topic
     * @throws PulsarAdminException
     */
    void removeOffloadPolicies(String topic) throws PulsarAdminException;

    /**
     * remove offload policies of a topic asynchronously.
     * @param topic
     * @return
     */
    CompletableFuture<Void> removeOffloadPoliciesAsync(String topic);

    /**
     * get max unacked messages on subscription of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    Integer getMaxUnackedMessagesOnSubscription(String topic) throws PulsarAdminException;

    /**
     * get max unacked messages on subscription of a topic asynchronously.
     * @param topic
     * @return
     */
    CompletableFuture<Integer> getMaxUnackedMessagesOnSubscriptionAsync(String topic);

    /**
     * get max unacked messages on subscription of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    Integer getMaxUnackedMessagesOnSubscription(String topic, boolean applied) throws PulsarAdminException;

    /**
     * get max unacked messages on subscription of a topic asynchronously.
     * @param topic
     * @return
     */
    CompletableFuture<Integer> getMaxUnackedMessagesOnSubscriptionAsync(String topic, boolean applied);

    /**
     * set max unacked messages on subscription of a topic.
     * @param topic
     * @param maxNum
     * @throws PulsarAdminException
     */
    void setMaxUnackedMessagesOnSubscription(String topic, int maxNum) throws PulsarAdminException;

    /**
     * set max unacked messages on subscription of a topic asynchronously.
     * @param topic
     * @param maxNum
     * @return
     */
    CompletableFuture<Void> setMaxUnackedMessagesOnSubscriptionAsync(String topic, int maxNum);

    /**
     * remove max unacked messages on subscription of a topic.
     * @param topic
     * @throws PulsarAdminException
     */
    void removeMaxUnackedMessagesOnSubscription(String topic) throws PulsarAdminException;

    /**
     * remove max unacked messages on subscription of a topic asynchronously.
     * @param topic
     * @return
     */
    CompletableFuture<Void> removeMaxUnackedMessagesOnSubscriptionAsync(String topic);

    /**
     * Set the configuration of persistence policies for specified topic.
     *
     * @param topic Topic name
     * @param persistencePolicies Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    void setPersistence(String topic, PersistencePolicies persistencePolicies) throws PulsarAdminException;

    /**
     * Set the configuration of persistence policies for specified topic asynchronously.
     *
     * @param topic Topic name
     * @param persistencePolicies Configuration of bookkeeper persistence policies
     */
    CompletableFuture<Void> setPersistenceAsync(String topic, PersistencePolicies persistencePolicies);

    /**
     * Get the configuration of persistence policies for specified topic.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    PersistencePolicies getPersistence(String topic) throws PulsarAdminException;

    /**
     * Get the configuration of persistence policies for specified topic asynchronously.
     *
     * @param topic Topic name
     */
    CompletableFuture<PersistencePolicies> getPersistenceAsync(String topic);

    /**
     * Get the applied configuration of persistence policies for specified topic.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    PersistencePolicies getPersistence(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get the applied configuration of persistence policies for specified topic asynchronously.
     *
     * @param topic Topic name
     */
    CompletableFuture<PersistencePolicies> getPersistenceAsync(String topic, boolean applied);

    /**
     * Remove the configuration of persistence policies for specified topic.
     *
     * @param topic Topic name
     * @throws PulsarAdminException Unexpected error
     */
    void removePersistence(String topic) throws PulsarAdminException;

    /**
     * Remove the configuration of persistence policies for specified topic asynchronously.
     *
     * @param topic Topic name
     */
    CompletableFuture<Void> removePersistenceAsync(String topic);

    /**
     * get deduplication enabled of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    Boolean getDeduplicationStatus(String topic) throws PulsarAdminException;

    /**
     * get deduplication enabled of a topic asynchronously.
     * @param topic
     * @return
     */
    CompletableFuture<Boolean> getDeduplicationStatusAsync(String topic);
    /**
     * get applied deduplication enabled of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    Boolean getDeduplicationStatus(String topic, boolean applied) throws PulsarAdminException;

    /**
     * get applied deduplication enabled of a topic asynchronously.
     * @param topic
     * @return
     */
    CompletableFuture<Boolean> getDeduplicationStatusAsync(String topic, boolean applied);

    /**
     * set deduplication enabled of a topic.
     * @param topic
     * @param enabled
     * @throws PulsarAdminException
     */
    void setDeduplicationStatus(String topic, boolean enabled) throws PulsarAdminException;

    /**
     * set deduplication enabled of a topic asynchronously.
     * @param topic
     * @param enabled
     * @return
     */
    CompletableFuture<Void> setDeduplicationStatusAsync(String topic, boolean enabled);

    /**
     * remove deduplication enabled of a topic.
     * @param topic
     * @throws PulsarAdminException
     */
    void removeDeduplicationStatus(String topic) throws PulsarAdminException;

    /**
     * remove deduplication enabled of a topic asynchronously.
     * @param topic
     * @return
     */
    CompletableFuture<Void> removeDeduplicationStatusAsync(String topic);

    /**
     * Set message-dispatch-rate (topic can dispatch this many messages per second).
     *
     * @param topic
     * @param dispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setDispatchRate(String topic, DispatchRate dispatchRate) throws PulsarAdminException;

    /**
     * Set message-dispatch-rate asynchronously.
     * <p/>
     * topic can dispatch this many messages per second
     *
     * @param topic
     * @param dispatchRate
     *            number of messages per second
     */
    CompletableFuture<Void> setDispatchRateAsync(String topic, DispatchRate dispatchRate);

    /**
     * Get message-dispatch-rate (topic can dispatch this many messages per second).
     *
     * @param topic
     * @returns messageRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    DispatchRate getDispatchRate(String topic) throws PulsarAdminException;

    /**
     * Get message-dispatch-rate asynchronously.
     * <p/>
     * Topic can dispatch this many messages per second.
     *
     * @param topic
     * @returns messageRate
     *            number of messages per second
     */
    CompletableFuture<DispatchRate> getDispatchRateAsync(String topic);

    /**
     * Get applied message-dispatch-rate (topic can dispatch this many messages per second).
     *
     * @param topic
     * @returns messageRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    DispatchRate getDispatchRate(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get applied message-dispatch-rate asynchronously.
     * <p/>
     * Topic can dispatch this many messages per second.
     *
     * @param topic
     * @returns messageRate
     *            number of messages per second
     */
    CompletableFuture<DispatchRate> getDispatchRateAsync(String topic, boolean applied);

    /**
     * Remove message-dispatch-rate.
     * <p/>
     * Remove topic message dispatch rate
     *
     * @param topic
     * @throws PulsarAdminException
     *              unexpected error
     */
    void removeDispatchRate(String topic) throws PulsarAdminException;

    /**
     * Remove message-dispatch-rate asynchronously.
     * <p/>
     * Remove topic message dispatch rate
     *
     * @param topic
     * @throws PulsarAdminException
     *              unexpected error
     */
    CompletableFuture<Void> removeDispatchRateAsync(String topic) throws PulsarAdminException;

    /**
     * Set subscription-message-dispatch-rate for the topic.
     * <p/>
     * Subscriptions under this namespace can dispatch this many messages per second
     *
     * @param topic
     * @param dispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setSubscriptionDispatchRate(String topic, DispatchRate dispatchRate) throws PulsarAdminException;

    /**
     * Set subscription-message-dispatch-rate for the topic asynchronously.
     * <p/>
     * Subscriptions under this namespace can dispatch this many messages per second.
     *
     * @param topic
     * @param dispatchRate
     *            number of messages per second
     */
    CompletableFuture<Void> setSubscriptionDispatchRateAsync(String topic, DispatchRate dispatchRate);

    /**
     * Get applied subscription-message-dispatch-rate.
     * <p/>
     * Subscriptions under this namespace can dispatch this many messages per second.
     *
     * @param namespace
     * @returns DispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    DispatchRate getSubscriptionDispatchRate(String namespace, boolean applied) throws PulsarAdminException;

    /**
     * Get applied subscription-message-dispatch-rate asynchronously.
     * <p/>
     * Subscriptions under this namespace can dispatch this many messages per second.
     *
     * @param namespace
     * @returns DispatchRate
     *            number of messages per second
     */
    CompletableFuture<DispatchRate> getSubscriptionDispatchRateAsync(String namespace, boolean applied);

    /**
     * Get subscription-message-dispatch-rate for the topic.
     * <p/>
     * Subscriptions under this namespace can dispatch this many messages per second.
     *
     * @param topic
     * @returns DispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    DispatchRate getSubscriptionDispatchRate(String topic) throws PulsarAdminException;

    /**
     * Get subscription-message-dispatch-rate asynchronously.
     * <p/>
     * Subscriptions under this namespace can dispatch this many messages per second.
     *
     * @param topic
     * @returns DispatchRate
     *            number of messages per second
     */
    CompletableFuture<DispatchRate> getSubscriptionDispatchRateAsync(String topic);

    /**
     * Remove subscription-message-dispatch-rate for a topic.
     * @param topic
     *            Topic name
     * @throws PulsarAdminException
     *            Unexpected error
     */
    void removeSubscriptionDispatchRate(String topic) throws PulsarAdminException;

    /**
     * Remove subscription-message-dispatch-rate for a topic asynchronously.
     * @param topic
     *            Topic name
     */
    CompletableFuture<Void> removeSubscriptionDispatchRateAsync(String topic);

    /**
     * Set replicatorDispatchRate for the topic.
     * <p/>
     * Replicator dispatch rate under this topic can dispatch this many messages per second
     *
     * @param topic
     * @param dispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setReplicatorDispatchRate(String topic, DispatchRate dispatchRate) throws PulsarAdminException;

    /**
     * Set replicatorDispatchRate for the topic asynchronously.
     * <p/>
     * Replicator dispatch rate under this topic can dispatch this many messages per second.
     *
     * @param topic
     * @param dispatchRate
     *            number of messages per second
     */
    CompletableFuture<Void> setReplicatorDispatchRateAsync(String topic, DispatchRate dispatchRate);

    /**
     * Get replicatorDispatchRate for the topic.
     * <p/>
     * Replicator dispatch rate under this topic can dispatch this many messages per second.
     *
     * @param topic
     * @returns DispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    DispatchRate getReplicatorDispatchRate(String topic) throws PulsarAdminException;

    /**
     * Get replicatorDispatchRate asynchronously.
     * <p/>
     * Replicator dispatch rate under this topic can dispatch this many messages per second.
     *
     * @param topic
     * @returns DispatchRate
     *            number of messages per second
     */
    CompletableFuture<DispatchRate> getReplicatorDispatchRateAsync(String topic);

    /**
     * Get applied replicatorDispatchRate for the topic.
     * @param topic
     * @param applied
     * @return
     * @throws PulsarAdminException
     */
    DispatchRate getReplicatorDispatchRate(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get applied replicatorDispatchRate asynchronously.
     * @param topic
     * @param applied
     * @return
     */
    CompletableFuture<DispatchRate> getReplicatorDispatchRateAsync(String topic, boolean applied);

    /**
     * Remove replicatorDispatchRate for a topic.
     * @param topic
     *            Topic name
     * @throws PulsarAdminException
     *            Unexpected error
     */
    void removeReplicatorDispatchRate(String topic) throws PulsarAdminException;

    /**
     * Remove replicatorDispatchRate for a topic asynchronously.
     * @param topic
     *            Topic name
     */
    CompletableFuture<Void> removeReplicatorDispatchRateAsync(String topic);

    /**
     * Get the compactionThreshold for a topic. The maximum number of bytes
     * can have before compaction is triggered. 0 disables.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>10000000</code>
     * </pre>
     *
     * @param topic
     *            Topic name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws PulsarAdminException.NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    Long getCompactionThreshold(String topic) throws PulsarAdminException;

    /**
     * Get the compactionThreshold for a topic asynchronously. The maximum number of bytes
     * can have before compaction is triggered. 0 disables.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>10000000</code>
     * </pre>
     *
     * @param topic
     *            Topic name
     */
    CompletableFuture<Long> getCompactionThresholdAsync(String topic);

    /**
     * Get the compactionThreshold for a topic. The maximum number of bytes
     * can have before compaction is triggered. 0 disables.
     * @param topic Topic name
     * @throws NotAuthorizedException Don't have admin permission
     * @throws PulsarAdminException.NotFoundException Namespace does not exist
     * @throws PulsarAdminException Unexpected error
     */
    Long getCompactionThreshold(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get the compactionThreshold for a topic asynchronously. The maximum number of bytes
     * can have before compaction is triggered. 0 disables.
     * @param topic Topic name
     */
    CompletableFuture<Long> getCompactionThresholdAsync(String topic, boolean applied);

    /**
     * Set the compactionThreshold for a topic. The maximum number of bytes
     * can have before compaction is triggered. 0 disables.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>10000000</code>
     * </pre>
     *
     * @param topic
     *            Topic name
     * @param compactionThreshold
     *            maximum number of backlog bytes before compaction is triggered
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws PulsarAdminException.NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setCompactionThreshold(String topic, long compactionThreshold) throws PulsarAdminException;

    /**
     * Set the compactionThreshold for a topic asynchronously. The maximum number of bytes
     * can have before compaction is triggered. 0 disables.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>10000000</code>
     * </pre>
     *
     * @param topic
     *            Topic name
     * @param compactionThreshold
     *            maximum number of backlog bytes before compaction is triggered
     */
    CompletableFuture<Void> setCompactionThresholdAsync(String topic, long compactionThreshold);

    /**
     * Remove the compactionThreshold for a topic.
     * @param topic
     *            Topic name
     * @throws PulsarAdminException
     *            Unexpected error
     */
    void removeCompactionThreshold(String topic) throws PulsarAdminException;

    /**
     * Remove the compactionThreshold for a topic asynchronously.
     * @param topic
     *            Topic name
     */
    CompletableFuture<Void> removeCompactionThresholdAsync(String topic);

    /**
     * Set message-publish-rate (topics can publish this many messages per second).
     *
     * @param topic
     * @param publishMsgRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setPublishRate(String topic, PublishRate publishMsgRate) throws PulsarAdminException;

    /**
     * Set message-publish-rate (topics can publish this many messages per second) asynchronously.
     *
     * @param topic
     * @param publishMsgRate
     *            number of messages per second
     */
    CompletableFuture<Void> setPublishRateAsync(String topic, PublishRate publishMsgRate);

    /**
     * Get message-publish-rate (topics can publish this many messages per second).
     *
     * @param topic
     * @return number of messages per second
     * @throws PulsarAdminException Unexpected error
     */
    PublishRate getPublishRate(String topic) throws PulsarAdminException;

    /**
     * Get message-publish-rate (topics can publish this many messages per second) asynchronously.
     *
     * @param topic
     * @return number of messages per second
     */
    CompletableFuture<PublishRate> getPublishRateAsync(String topic);

    /**
     * Remove message-publish-rate.
     * <p/>
     * Remove topic message publish rate
     *
     * @param topic
     * @throws PulsarAdminException
     *              unexpected error
     */
    void removePublishRate(String topic) throws PulsarAdminException;

    /**
     * Remove message-publish-rate asynchronously.
     * <p/>
     * Remove topic message publish rate
     *
     * @param topic
     * @throws PulsarAdminException
     *              unexpected error
     */
    CompletableFuture<Void> removePublishRateAsync(String topic) throws PulsarAdminException;

    /**
     * Get the maxConsumersPerSubscription for a topic.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>0</code>
     * </pre>
     *
     * @param topic
     *            Topic name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws PulsarAdminException.NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    Integer getMaxConsumersPerSubscription(String topic) throws PulsarAdminException;

    /**
     * Get the maxConsumersPerSubscription for a topic asynchronously.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>0</code>
     * </pre>
     *
     * @param topic
     *            Topic name
     */
    CompletableFuture<Integer> getMaxConsumersPerSubscriptionAsync(String topic);

    /**
     * Set maxConsumersPerSubscription for a topic.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>10</code>
     * </pre>
     *
     * @param topic
     *            Topic name
     * @param maxConsumersPerSubscription
     *            maxConsumersPerSubscription value for a namespace
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws PulsarAdminException.NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setMaxConsumersPerSubscription(String topic, int maxConsumersPerSubscription) throws PulsarAdminException;

    /**
     * Set maxConsumersPerSubscription for a topic asynchronously.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>10</code>
     * </pre>
     *
     * @param topic
     *            Topic name
     * @param maxConsumersPerSubscription
     *            maxConsumersPerSubscription value for a namespace
     */
    CompletableFuture<Void> setMaxConsumersPerSubscriptionAsync(String topic, int maxConsumersPerSubscription);

    /**
     * Remove the maxConsumersPerSubscription for a topic.
     * @param topic
     *            Topic name
     * @throws PulsarAdminException
     *            Unexpected error
     */
    void removeMaxConsumersPerSubscription(String topic) throws PulsarAdminException;

    /**
     * Remove the maxConsumersPerSubscription for a topic asynchronously.
     * @param topic
     *            Topic name
     */
    CompletableFuture<Void> removeMaxConsumersPerSubscriptionAsync(String topic);

    /**
     * Get the max number of producer for specified topic.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    Integer getMaxProducers(String topic) throws PulsarAdminException;

    /**
     * Get the max number of producer for specified topic asynchronously.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    CompletableFuture<Integer> getMaxProducersAsync(String topic);

    /**
     * Get the max number of producer applied for specified topic.
     * @param topic
     * @param applied
     * @return
     * @throws PulsarAdminException
     */
    Integer getMaxProducers(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get the max number of producer applied for specified topic asynchronously.
     * @param topic
     * @param applied
     * @return
     */
    CompletableFuture<Integer> getMaxProducersAsync(String topic, boolean applied);


    /**
     * Set the max number of producer for specified topic.
     *
     * @param topic Topic name
     * @param maxProducers Max number of producer
     * @throws PulsarAdminException Unexpected error
     */
    void setMaxProducers(String topic, int maxProducers) throws PulsarAdminException;

    /**
     * Set the max number of producer for specified topic asynchronously.
     *
     * @param topic Topic name
     * @param maxProducers Max number of producer
     * @throws PulsarAdminException Unexpected error
     */
    CompletableFuture<Void> setMaxProducersAsync(String topic, int maxProducers);

    /**
     * Remove the max number of producer for specified topic.
     *
     * @param topic Topic name
     * @throws PulsarAdminException Unexpected error
     */
    void removeMaxProducers(String topic) throws PulsarAdminException;

    /**
     * Remove the max number of producer for specified topic asynchronously.
     *
     * @param topic Topic name
     */
    CompletableFuture<Void> removeMaxProducersAsync(String topic);

    /**
     * Get the max number of subscriptions for specified topic.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    Integer getMaxSubscriptionsPerTopic(String topic) throws PulsarAdminException;

    /**
     * Get the max number of subscriptions for specified topic asynchronously.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    CompletableFuture<Integer> getMaxSubscriptionsPerTopicAsync(String topic);


    /**
     * Set the max number of subscriptions for specified topic.
     *
     * @param topic Topic name
     * @param maxSubscriptionsPerTopic Max number of subscriptions
     * @throws PulsarAdminException Unexpected error
     */
    void setMaxSubscriptionsPerTopic(String topic, int maxSubscriptionsPerTopic) throws PulsarAdminException;

    /**
     * Set the max number of subscriptions for specified topic asynchronously.
     *
     * @param topic Topic name
     * @param maxSubscriptionsPerTopic Max number of subscriptions
     * @throws PulsarAdminException Unexpected error
     */
    CompletableFuture<Void> setMaxSubscriptionsPerTopicAsync(String topic, int maxSubscriptionsPerTopic);

    /**
     * Remove the max number of subscriptions for specified topic.
     *
     * @param topic Topic name
     * @throws PulsarAdminException Unexpected error
     */
    void removeMaxSubscriptionsPerTopic(String topic) throws PulsarAdminException;

    /**
     * Remove the max number of subscriptions for specified topic asynchronously.
     *
     * @param topic Topic name
     */
    CompletableFuture<Void> removeMaxSubscriptionsPerTopicAsync(String topic);

    /**
     * Get the max message size for specified topic.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    Integer getMaxMessageSize(String topic) throws PulsarAdminException;

    /**
     * Get the max message size for specified topic asynchronously.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    CompletableFuture<Integer> getMaxMessageSizeAsync(String topic);


    /**
     * Set the max message size for specified topic.
     *
     * @param topic Topic name
     * @param maxMessageSize Max message size of producer
     * @throws PulsarAdminException Unexpected error
     */
    void setMaxMessageSize(String topic, int maxMessageSize) throws PulsarAdminException;

    /**
     * Set the max message size for specified topic asynchronously.0 disables.
     *
     * @param topic Topic name
     * @param maxMessageSize Max message size of topic
     * @throws PulsarAdminException Unexpected error
     */
    CompletableFuture<Void> setMaxMessageSizeAsync(String topic, int maxMessageSize);

    /**
     * Remove the max message size for specified topic.
     *
     * @param topic Topic name
     * @throws PulsarAdminException Unexpected error
     */
    void removeMaxMessageSize(String topic) throws PulsarAdminException;

    /**
     * Remove the max message size for specified topic asynchronously.
     *
     * @param topic Topic name
     */
    CompletableFuture<Void> removeMaxMessageSizeAsync(String topic);

    /**
     * Get the max number of consumer for specified topic.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    Integer getMaxConsumers(String topic) throws PulsarAdminException;

    /**
     * Get the max number of consumer for specified topic asynchronously.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    CompletableFuture<Integer> getMaxConsumersAsync(String topic);

    /**
     * Get the max number of consumer applied for specified topic.
     * @param topic
     * @param applied
     * @return
     * @throws PulsarAdminException
     */
    Integer getMaxConsumers(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get the max number of consumer applied for specified topic asynchronously.
     * @param topic
     * @param applied
     * @return
     */
    CompletableFuture<Integer> getMaxConsumersAsync(String topic, boolean applied);

    /**
     * Set the max number of consumer for specified topic.
     *
     * @param topic Topic name
     * @param maxConsumers Max number of consumer
     * @throws PulsarAdminException Unexpected error
     */
    void setMaxConsumers(String topic, int maxConsumers) throws PulsarAdminException;

    /**
     * Set the max number of consumer for specified topic asynchronously.
     *
     * @param topic Topic name
     * @param maxConsumers Max number of consumer
     * @throws PulsarAdminException Unexpected error
     */
    CompletableFuture<Void> setMaxConsumersAsync(String topic, int maxConsumers);

    /**
     * Remove the max number of consumer for specified topic.
     *
     * @param topic Topic name
     * @throws PulsarAdminException Unexpected error
     */
    void removeMaxConsumers(String topic) throws PulsarAdminException;

    /**
     * Remove the max number of consumer for specified topic asynchronously.
     *
     * @param topic Topic name
     */
    CompletableFuture<Void> removeMaxConsumersAsync(String topic);

    /**
     * Get the deduplication snapshot interval for specified topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    Integer getDeduplicationSnapshotInterval(String topic) throws PulsarAdminException;

    /**
     * Get the deduplication snapshot interval for specified topic asynchronously.
     * @param topic
     * @return
     */
    CompletableFuture<Integer> getDeduplicationSnapshotIntervalAsync(String topic);

    /**
     * Set the deduplication snapshot interval for specified topic.
     * @param topic
     * @param interval
     * @throws PulsarAdminException
     */
    void setDeduplicationSnapshotInterval(String topic, int interval) throws PulsarAdminException;

    /**
     * Set the deduplication snapshot interval for specified topic asynchronously.
     * @param topic
     * @param interval
     * @return
     */
    CompletableFuture<Void> setDeduplicationSnapshotIntervalAsync(String topic, int interval);

    /**
     * Remove the deduplication snapshot interval for specified topic.
     * @param topic
     * @throws PulsarAdminException
     */
    void removeDeduplicationSnapshotInterval(String topic) throws PulsarAdminException;

    /**
     * Remove the deduplication snapshot interval for specified topic asynchronously.
     * @param topic
     * @return
     */
    CompletableFuture<Void> removeDeduplicationSnapshotIntervalAsync(String topic);

    /**
     * Set is enable sub types.
     *
     * @param topic
     * @param subscriptionTypesEnabled
     *            is enable subTypes
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setSubscriptionTypesEnabled(String topic,
                                     Set<SubscriptionType> subscriptionTypesEnabled) throws PulsarAdminException;

    /**
     * Set is enable sub types asynchronously.
     *
     * @param topic
     * @param subscriptionTypesEnabled
     *            is enable subTypes
     */
    CompletableFuture<Void> setSubscriptionTypesEnabledAsync(String topic,
                                                             Set<SubscriptionType> subscriptionTypesEnabled);

    /**
     * Get is enable sub types.
     *
     * @param topic
     *            is topic for get is enable sub types
     * @return set of enable sub types {@link Set <SubscriptionType>}
     * @throws PulsarAdminException
     *             Unexpected error
     */
    Set<SubscriptionType> getSubscriptionTypesEnabled(String topic) throws PulsarAdminException;

    /**
     * Get is enable sub types asynchronously.
     *
     * @param topic
     *            is topic for get is enable sub types
     */
    CompletableFuture<Set<SubscriptionType>> getSubscriptionTypesEnabledAsync(String topic);

    /**
     * Set topic-subscribe-rate (topic will limit by subscribeRate).
     *
     * @param topic
     * @param subscribeRate
     *            consumer subscribe limit by this subscribeRate
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setSubscribeRate(String topic, SubscribeRate subscribeRate) throws PulsarAdminException;

    /**
     * Set topic-subscribe-rate (topics will limit by subscribeRate) asynchronously.
     *
     * @param topic
     * @param subscribeRate
     *            consumer subscribe limit by this subscribeRate
     */
    CompletableFuture<Void> setSubscribeRateAsync(String topic, SubscribeRate subscribeRate);

    /**
     * Get topic-subscribe-rate (topics allow subscribe times per consumer in a period).
     *
     * @param topic
     * @returns subscribeRate
     * @throws PulsarAdminException
     *             Unexpected error
     */
    SubscribeRate getSubscribeRate(String topic) throws PulsarAdminException;

    /**
     * Get topic-subscribe-rate asynchronously.
     * <p/>
     * Topic allow subscribe times per consumer in a period.
     *
     * @param topic
     * @returns subscribeRate
     */
    CompletableFuture<SubscribeRate> getSubscribeRateAsync(String topic);

    /**
     * Get applied topic-subscribe-rate (topics allow subscribe times per consumer in a period).
     *
     * @param topic
     * @returns subscribeRate
     * @throws PulsarAdminException
     *             Unexpected error
     */
    SubscribeRate getSubscribeRate(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get applied topic-subscribe-rate asynchronously.
     *
     * @param topic
     * @returns subscribeRate
     */
    CompletableFuture<SubscribeRate> getSubscribeRateAsync(String topic, boolean applied);

    /**
     * Remove topic-subscribe-rate.
     * <p/>
     * Remove topic subscribe rate
     *
     * @param topic
     * @throws PulsarAdminException
     *              unexpected error
     */
    void removeSubscribeRate(String topic) throws PulsarAdminException;

    /**
     * Remove topic-subscribe-rate asynchronously.
     * <p/>
     * Remove topic subscribe rate
     *
     * @param topic
     * @throws PulsarAdminException
     *              unexpected error
     */
    CompletableFuture<Void> removeSubscribeRateAsync(String topic) throws PulsarAdminException;


}
