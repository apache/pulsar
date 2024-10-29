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
package org.apache.pulsar.client.admin.internal;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.TopicPolicies;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.EntryFilters;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscribeRate;

public class TopicPoliciesImpl extends BaseResource implements TopicPolicies {
    private final WebTarget adminTopics;
    private final WebTarget adminV2Topics;
    private final boolean isGlobal;

    protected TopicPoliciesImpl(WebTarget web, Authentication auth, long readTimeoutMs, boolean isGlobal) {
        super(auth, readTimeoutMs);
        this.adminTopics = web.path("/admin");
        this.adminV2Topics = web.path("/admin/v2");
        this.isGlobal = isGlobal;
    }

    public WebTarget addGlobalIfNeeded(WebTarget path) {
        return isGlobal ? path.queryParam("isGlobal", true) : path;
    }

    @Override
    public Map<BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(String topic) throws PulsarAdminException {
        return getBacklogQuotaMap(topic, false);
    }

    @Override
    public Map<BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(String topic, boolean applied)
            throws PulsarAdminException {
        try {
            TopicName tn = validateTopic(topic);
            WebTarget path = topicPath(tn, "backlogQuotaMap");
            path = path.queryParam("applied", applied);
            return request(path).get(new GenericType<Map<BacklogQuotaType, BacklogQuota>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setBacklogQuota(String topic, BacklogQuota backlogQuota,
                                BacklogQuotaType backlogQuotaType) throws PulsarAdminException {
        try {
            TopicName tn = validateTopic(topic);
            WebTarget path = topicPath(tn, "backlogQuota");
            request(path.queryParam("backlogQuotaType", backlogQuotaType.toString()))
                    .post(Entity.entity(backlogQuota, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void removeBacklogQuota(String topic, BacklogQuotaType backlogQuotaType) throws PulsarAdminException {
        try {
            TopicName tn = validateTopic(topic);
            WebTarget path = topicPath(tn, "backlogQuota");
            request(path.queryParam("backlogQuotaType", backlogQuotaType.toString()))
                    .delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public Integer getMaxUnackedMessagesOnConsumer(String topic) throws PulsarAdminException {
        return getMaxUnackedMessagesOnConsumer(topic, false);
    }

    @Override
    public CompletableFuture<Integer> getMaxUnackedMessagesOnConsumerAsync(String topic) {
        return getMaxUnackedMessagesOnConsumerAsync(topic, false);
    }

    @Override
    public Integer getMaxUnackedMessagesOnConsumer(String topic, boolean applied) throws PulsarAdminException {
        return sync(() -> getMaxUnackedMessagesOnConsumerAsync(topic, applied));
    }

    @Override
    public CompletableFuture<Integer> getMaxUnackedMessagesOnConsumerAsync(String topic, boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "maxUnackedMessagesOnConsumer");
        path = path.queryParam("applied", applied);
        return asyncGetRequest(path, new FutureCallback<Integer>(){});
    }

    @Override
    public CompletableFuture<Void> setMaxUnackedMessagesOnConsumerAsync(String topic, int maxNum) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "maxUnackedMessagesOnConsumer");

        return asyncPostRequest(path, Entity.entity(maxNum, MediaType.APPLICATION_JSON));
    }

    @Override
    public void setMaxUnackedMessagesOnConsumer(String topic, int maxNum) throws PulsarAdminException {
        sync(() -> setMaxUnackedMessagesOnConsumerAsync(topic, maxNum));
    }

    @Override
    public CompletableFuture<Void> removeMaxUnackedMessagesOnConsumerAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "maxUnackedMessagesOnConsumer");
        return asyncDeleteRequest(path);
    }

    @Override
    public void removeMaxUnackedMessagesOnConsumer(String topic) throws PulsarAdminException {
        sync(() -> removeMaxUnackedMessagesOnConsumerAsync(topic));
    }

    @Override
    public InactiveTopicPolicies getInactiveTopicPolicies(String topic, boolean applied) throws PulsarAdminException {
        return sync(() -> getInactiveTopicPoliciesAsync(topic, applied));
    }

    @Override
    public CompletableFuture<InactiveTopicPolicies> getInactiveTopicPoliciesAsync(String topic, boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "inactiveTopicPolicies");
        path = path.queryParam("applied", applied);
        return asyncGetRequest(path, new FutureCallback<InactiveTopicPolicies>(){});
    }

    @Override
    public InactiveTopicPolicies getInactiveTopicPolicies(String topic) throws PulsarAdminException {
        return getInactiveTopicPolicies(topic, false);
    }

    @Override
    public CompletableFuture<InactiveTopicPolicies> getInactiveTopicPoliciesAsync(String topic) {
        return getInactiveTopicPoliciesAsync(topic, false);
    }

    @Override
    public CompletableFuture<Void> setInactiveTopicPoliciesAsync(String topic
            , InactiveTopicPolicies inactiveTopicPolicies) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "inactiveTopicPolicies");
        return asyncPostRequest(path, Entity.entity(inactiveTopicPolicies, MediaType.APPLICATION_JSON));
    }

    @Override
    public void setInactiveTopicPolicies(String topic
            , InactiveTopicPolicies inactiveTopicPolicies) throws PulsarAdminException {
        sync(() -> setInactiveTopicPoliciesAsync(topic, inactiveTopicPolicies));
    }

    @Override
    public CompletableFuture<Void> removeInactiveTopicPoliciesAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "inactiveTopicPolicies");
        return asyncDeleteRequest(path);
    }

    @Override
    public void removeInactiveTopicPolicies(String topic) throws PulsarAdminException {
        sync(() -> removeInactiveTopicPoliciesAsync(topic));
    }

    @Override
    public DelayedDeliveryPolicies getDelayedDeliveryPolicy(String topic
            , boolean applied) throws PulsarAdminException {
        return sync(() -> getDelayedDeliveryPolicyAsync(topic, applied));
    }

    @Override
    public CompletableFuture<DelayedDeliveryPolicies> getDelayedDeliveryPolicyAsync(String topic
            , boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "delayedDelivery");
        path = path.queryParam("applied", applied);

        return asyncGetRequest(path, new FutureCallback<DelayedDeliveryPolicies>(){});
    }

    @Override
    public DelayedDeliveryPolicies getDelayedDeliveryPolicy(String topic) throws PulsarAdminException {
        return getDelayedDeliveryPolicy(topic, false);
    }

    @Override
    public CompletableFuture<DelayedDeliveryPolicies> getDelayedDeliveryPolicyAsync(String topic) {
        return getDelayedDeliveryPolicyAsync(topic, false);
    }

    @Override
    public CompletableFuture<Void> removeDelayedDeliveryPolicyAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "delayedDelivery");
        return asyncDeleteRequest(path);
    }

    @Override
    public void removeDelayedDeliveryPolicy(String topic) throws PulsarAdminException {
        sync(() -> removeDelayedDeliveryPolicyAsync(topic));
    }

    @Override
    public CompletableFuture<Void> setDelayedDeliveryPolicyAsync(String topic
            , DelayedDeliveryPolicies delayedDeliveryPolicies) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "delayedDelivery");
        return asyncPostRequest(path, Entity.entity(delayedDeliveryPolicies, MediaType.APPLICATION_JSON));
    }

    @Override
    public void setDelayedDeliveryPolicy(String topic
            , DelayedDeliveryPolicies delayedDeliveryPolicies) throws PulsarAdminException {
        sync(() -> setDelayedDeliveryPolicyAsync(topic, delayedDeliveryPolicies));
    }

    @Override
    public Boolean getDeduplicationStatus(String topic) throws PulsarAdminException {
        return getDeduplicationStatus(topic, false);
    }

    @Override
    public CompletableFuture<Boolean> getDeduplicationStatusAsync(String topic) {
        return getDeduplicationStatusAsync(topic, false);
    }

    @Override
    public Boolean getDeduplicationStatus(String topic, boolean applied) throws PulsarAdminException {
        return sync(() -> getDeduplicationStatusAsync(topic, applied));
    }

    @Override
    public CompletableFuture<Boolean> getDeduplicationStatusAsync(String topic, boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "deduplicationEnabled");
        path = path.queryParam("applied", applied);
        return asyncGetRequest(path, new FutureCallback<Boolean>(){});
    }

    private CompletableFuture<Void> enableDeduplicationAsync(String topic, boolean enabled) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "deduplicationEnabled");
        return asyncPostRequest(path, Entity.entity(enabled, MediaType.APPLICATION_JSON));
    }

    @Override
    public void setDeduplicationStatus(String topic, boolean enabled) throws PulsarAdminException {
        sync(() -> enableDeduplicationAsync(topic, enabled));
    }

    @Override
    public CompletableFuture<Void> setDeduplicationStatusAsync(String topic, boolean enabled) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "deduplicationEnabled");
        return asyncPostRequest(path, Entity.entity(enabled, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeDeduplicationStatus(String topic) throws PulsarAdminException {
        sync(() -> removeDeduplicationStatusAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removeDeduplicationStatusAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "deduplicationEnabled");
        return asyncDeleteRequest(path);
    }

    @Override
    public OffloadPolicies getOffloadPolicies(String topic) throws PulsarAdminException {
        return getOffloadPolicies(topic, false);
    }

    @Override
    public CompletableFuture<OffloadPolicies> getOffloadPoliciesAsync(String topic) {
        return getOffloadPoliciesAsync(topic, false);
    }

    @Override
    public OffloadPolicies getOffloadPolicies(String topic, boolean applied) throws PulsarAdminException {
        return sync(() -> getOffloadPoliciesAsync(topic, applied));
    }

    @Override
    public CompletableFuture<OffloadPolicies> getOffloadPoliciesAsync(String topic, boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "offloadPolicies");
        path = path.queryParam("applied", applied);
        return asyncGetRequest(path, new FutureCallback<OffloadPoliciesImpl>(){})
                .thenApply(offloadPolicies -> offloadPolicies);
    }

    @Override
    public void setOffloadPolicies(String topic, OffloadPolicies offloadPolicies) throws PulsarAdminException {
        sync(() -> setOffloadPoliciesAsync(topic, offloadPolicies));
    }

    @Override
    public CompletableFuture<Void> setOffloadPoliciesAsync(String topic, OffloadPolicies offloadPolicies) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "offloadPolicies");
        return asyncPostRequest(path, Entity.entity((OffloadPoliciesImpl) offloadPolicies, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeOffloadPolicies(String topic) throws PulsarAdminException {
        sync(() -> removeOffloadPoliciesAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removeOffloadPoliciesAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "offloadPolicies");
        return asyncDeleteRequest(path);
    }

    @Override
    public Integer getMaxUnackedMessagesOnSubscription(String topic) throws PulsarAdminException {
        return getMaxUnackedMessagesOnSubscription(topic, false);
    }

    @Override
    public CompletableFuture<Integer> getMaxUnackedMessagesOnSubscriptionAsync(String topic) {
        return getMaxUnackedMessagesOnSubscriptionAsync(topic, false);
    }

    @Override
    public Integer getMaxUnackedMessagesOnSubscription(String topic, boolean applied) throws PulsarAdminException {
        return sync(() -> getMaxUnackedMessagesOnSubscriptionAsync(topic, applied));
    }

    @Override
    public CompletableFuture<Integer> getMaxUnackedMessagesOnSubscriptionAsync(String topic, boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "maxUnackedMessagesOnSubscription");
        path = path.queryParam("applied", applied);
        return asyncGetRequest(path, new FutureCallback<Integer>(){});
    }

    @Override
    public void setMaxUnackedMessagesOnSubscription(String topic, int maxNum) throws PulsarAdminException {
        sync(() -> setMaxUnackedMessagesOnSubscriptionAsync(topic, maxNum));
    }

    @Override
    public CompletableFuture<Void> setMaxUnackedMessagesOnSubscriptionAsync(String topic, int maxNum) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "maxUnackedMessagesOnSubscription");
        return asyncPostRequest(path, Entity.entity(maxNum, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeMaxUnackedMessagesOnSubscription(String topic) throws PulsarAdminException {
        sync(() -> removeMaxUnackedMessagesOnSubscriptionAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removeMaxUnackedMessagesOnSubscriptionAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "maxUnackedMessagesOnSubscription");
        return asyncDeleteRequest(path);
    }

    @Override
    public void setMessageTTL(String topic, int messageTTLInSecond) throws PulsarAdminException {
        try {
            TopicName topicName = validateTopic(topic);
            WebTarget path = topicPath(topicName, "messageTTL");
            request(path.queryParam("messageTTL", messageTTLInSecond)).
                    post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public Integer getMessageTTL(String topic) throws PulsarAdminException {
        return getMessageTTL(topic, false);
    }

    @Override
    public Integer getMessageTTL(String topic, boolean applied) throws PulsarAdminException {
        try {
            TopicName topicName = validateTopic(topic);
            WebTarget path = topicPath(topicName, "messageTTL");
            path = path.queryParam("applied", applied);
            return request(path).get(new GenericType<Integer>() {});
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void removeMessageTTL(String topic) throws PulsarAdminException {
        try {
            TopicName topicName = validateTopic(topic);
            WebTarget path = topicPath(topicName, "messageTTL");
            request(path.queryParam("messageTTL", 0)).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setRetention(String topic, RetentionPolicies retention) throws PulsarAdminException {
        sync(() -> setRetentionAsync(topic, retention));
    }

    @Override
    public CompletableFuture<Void> setRetentionAsync(String topic, RetentionPolicies retention) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "retention");
        return asyncPostRequest(path, Entity.entity(retention, MediaType.APPLICATION_JSON));
    }

    @Override
    public RetentionPolicies getRetention(String topic) throws PulsarAdminException {
        return getRetention(topic, false);
    }

    @Override
    public CompletableFuture<RetentionPolicies> getRetentionAsync(String topic) {
        return getRetentionAsync(topic, false);
    }

    @Override
    public RetentionPolicies getRetention(String topic, boolean applied) throws PulsarAdminException {
        return sync(() -> getRetentionAsync(topic, applied));
    }

    @Override
    public CompletableFuture<RetentionPolicies> getRetentionAsync(String topic, boolean applied) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "retention");
        path = path.queryParam("applied", applied);
        return asyncGetRequest(path, new FutureCallback<RetentionPolicies>(){});
    }

    @Override
    public void removeRetention(String topic) throws PulsarAdminException {
        sync(() -> removeRetentionAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removeRetentionAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "retention");
        return asyncDeleteRequest(path);
    }

    @Override
    public void setPersistence(String topic, PersistencePolicies persistencePolicies) throws PulsarAdminException {
        sync(() -> setPersistenceAsync(topic, persistencePolicies));
    }

    @Override
    public CompletableFuture<Void> setPersistenceAsync(String topic, PersistencePolicies persistencePolicies) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "persistence");
        return asyncPostRequest(path, Entity.entity(persistencePolicies, MediaType.APPLICATION_JSON));
    }

    @Override
    public PersistencePolicies getPersistence(String topic) throws PulsarAdminException {
        return getPersistence(topic, false);
    }

    @Override
    public CompletableFuture<PersistencePolicies> getPersistenceAsync(String topic) {
        return getPersistenceAsync(topic, false);
    }

    @Override
    public PersistencePolicies getPersistence(String topic, boolean applied) throws PulsarAdminException {
        return sync(() -> getPersistenceAsync(topic, applied));
    }

    @Override
    public CompletableFuture<PersistencePolicies> getPersistenceAsync(String topic, boolean applied) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "persistence");
        path = path.queryParam("applied", applied);
        return asyncGetRequest(path, new FutureCallback<PersistencePolicies>(){});
    }

    @Override
    public void removePersistence(String topic) throws PulsarAdminException {
        sync(() -> removePersistenceAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removePersistenceAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "persistence");
        return asyncDeleteRequest(path);
    }

    @Override
    public DispatchRate getDispatchRate(String topic, boolean applied) throws PulsarAdminException {
        return sync(() -> getDispatchRateAsync(topic, applied));
    }

    @Override
    public CompletableFuture<DispatchRate> getDispatchRateAsync(String topic, boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "dispatchRate");
        path = path.queryParam("applied", applied);
        return asyncGetRequest(path, new FutureCallback<DispatchRate>(){});
    }

    @Override
    public DispatchRate getDispatchRate(String topic) throws PulsarAdminException {
        return getDispatchRate(topic, false);
    }

    @Override
    public CompletableFuture<DispatchRate> getDispatchRateAsync(String topic) {
        return getDispatchRateAsync(topic, false);
    }

    @Override
    public void setDispatchRate(String topic, DispatchRate dispatchRate) throws PulsarAdminException {
        sync(() -> setDispatchRateAsync(topic, dispatchRate));
    }

    @Override
    public CompletableFuture<Void> setDispatchRateAsync(String topic, DispatchRate dispatchRate) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "dispatchRate");
        return asyncPostRequest(path, Entity.entity(dispatchRate, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeDispatchRate(String topic) throws PulsarAdminException {
        sync(() -> removeDispatchRateAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removeDispatchRateAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "dispatchRate");
        return asyncDeleteRequest(path);
    }

    @Override
    public DispatchRate getSubscriptionDispatchRate(String topic, boolean applied) throws PulsarAdminException {
        return sync(() -> getSubscriptionDispatchRateAsync(topic, applied));
    }

    @Override
    public CompletableFuture<DispatchRate> getSubscriptionDispatchRateAsync(String topic, boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "subscriptionDispatchRate");
        path = path.queryParam("applied", applied);
        return asyncGetRequest(path, new FutureCallback<DispatchRate>(){});
    }

    @Override
    public DispatchRate getSubscriptionDispatchRate(String topic) throws PulsarAdminException {
        return getSubscriptionDispatchRate(topic, false);
    }

    @Override
    public CompletableFuture<DispatchRate> getSubscriptionDispatchRateAsync(String topic) {
        return getSubscriptionDispatchRateAsync(topic, false);
    }

    @Override
    public void setSubscriptionDispatchRate(String topic, DispatchRate dispatchRate) throws PulsarAdminException {
        sync(() -> setSubscriptionDispatchRateAsync(topic, dispatchRate));
    }

    @Override
    public CompletableFuture<Void> setSubscriptionDispatchRateAsync(String topic, DispatchRate dispatchRate) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "subscriptionDispatchRate");
        return asyncPostRequest(path, Entity.entity(dispatchRate, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeSubscriptionDispatchRate(String topic) throws PulsarAdminException {
        sync(() -> removeSubscriptionDispatchRateAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removeSubscriptionDispatchRateAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "subscriptionDispatchRate");
        return asyncDeleteRequest(path);
    }

    @Override
    public void setSubscriptionDispatchRate(String topic, String subscriptionName, DispatchRate dispatchRate)
            throws PulsarAdminException {
        sync(() -> setSubscriptionDispatchRateAsync(topic, subscriptionName, dispatchRate));
    }

    @Override
    public CompletableFuture<Void> setSubscriptionDispatchRateAsync(String topic, String subscriptionName,
                                                                    DispatchRate dispatchRate) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, subscriptionName, "dispatchRate");
        return asyncPostRequest(path, Entity.entity(dispatchRate, MediaType.APPLICATION_JSON));
    }

    @Override
    public DispatchRate getSubscriptionDispatchRate(String topic, String subscriptionName, boolean applied)
            throws PulsarAdminException {
        return sync(() -> getSubscriptionDispatchRateAsync(topic, subscriptionName, applied));
    }

    @Override
    public CompletableFuture<DispatchRate> getSubscriptionDispatchRateAsync(String topic, String subscriptionName,
                                                                            boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, subscriptionName, "dispatchRate");
        path = path.queryParam("applied", applied);
        return asyncGetRequest(path, new FutureCallback<DispatchRate>(){});
    }

    @Override
    public DispatchRate getSubscriptionDispatchRate(String topic, String subscriptionName) throws PulsarAdminException {
        return sync(() -> getSubscriptionDispatchRateAsync(topic, subscriptionName));
    }

    @Override
    public CompletableFuture<DispatchRate> getSubscriptionDispatchRateAsync(String topic, String subscriptionName) {
        return getSubscriptionDispatchRateAsync(topic, subscriptionName, false);
    }

    @Override
    public void removeSubscriptionDispatchRate(String topic, String subscriptionName) throws PulsarAdminException {
        sync(() -> removeSubscriptionDispatchRateAsync(topic, subscriptionName));
    }

    @Override
    public CompletableFuture<Void> removeSubscriptionDispatchRateAsync(String topic, String subscriptionName) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, subscriptionName, "dispatchRate");
        return asyncDeleteRequest(path);
    }

    @Override
    public Long getCompactionThreshold(String topic) throws PulsarAdminException {
        return getCompactionThreshold(topic, false);
    }

    @Override
    public CompletableFuture<Long> getCompactionThresholdAsync(String topic) {
        return getCompactionThresholdAsync(topic, false);
    }

    @Override
    public Long getCompactionThreshold(String topic, boolean applied) throws PulsarAdminException {
        return sync(() -> getCompactionThresholdAsync(topic, applied));
    }

    @Override
    public CompletableFuture<Long> getCompactionThresholdAsync(String topic, boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "compactionThreshold");
        path = path.queryParam("applied", applied);
        return asyncGetRequest(path, new FutureCallback<Long>(){});
    }

    @Override
    public void setCompactionThreshold(String topic, long compactionThreshold) throws PulsarAdminException {
        sync(() -> setCompactionThresholdAsync(topic, compactionThreshold));
    }

    @Override
    public CompletableFuture<Void> setCompactionThresholdAsync(String topic, long compactionThreshold) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "compactionThreshold");
        return asyncPostRequest(path, Entity.entity(compactionThreshold, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeCompactionThreshold(String topic) throws PulsarAdminException {
        sync(() -> removeCompactionThresholdAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removeCompactionThresholdAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "compactionThreshold");
        return asyncDeleteRequest(path);
    }

    @Override
    public PublishRate getPublishRate(String topic) throws PulsarAdminException {
        return sync(() -> getPublishRateAsync(topic));
    }

    @Override
    public CompletableFuture<PublishRate> getPublishRateAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "publishRate");
        return asyncGetRequest(path, new FutureCallback<PublishRate>(){});
    }

    @Override
    public void setPublishRate(String topic, PublishRate publishRate) throws PulsarAdminException {
        sync(() -> setPublishRateAsync(topic, publishRate));
    }

    @Override
    public CompletableFuture<Void> setPublishRateAsync(String topic, PublishRate publishRate) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "publishRate");
        return asyncPostRequest(path, Entity.entity(publishRate, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removePublishRate(String topic) throws PulsarAdminException {
        sync(() -> removePublishRateAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removePublishRateAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "publishRate");
        return asyncDeleteRequest(path);
    }

    @Override
    public Integer getMaxConsumersPerSubscription(String topic) throws PulsarAdminException {
        return sync(() -> getMaxConsumersPerSubscriptionAsync(topic));
    }

    @Override
    public CompletableFuture<Integer> getMaxConsumersPerSubscriptionAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "maxConsumersPerSubscription");
        return asyncGetRequest(path, new FutureCallback<Integer>(){});
    }

    @Override
    public void setMaxConsumersPerSubscription(String topic, int maxConsumersPerSubscription)
            throws PulsarAdminException {
        sync(() -> setMaxConsumersPerSubscriptionAsync(topic, maxConsumersPerSubscription));
    }

    @Override
    public CompletableFuture<Void> setMaxConsumersPerSubscriptionAsync(String topic, int maxConsumersPerSubscription) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "maxConsumersPerSubscription");
        return asyncPostRequest(path, Entity.entity(maxConsumersPerSubscription, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeMaxConsumersPerSubscription(String topic) throws PulsarAdminException {
        sync(() -> removeMaxConsumersPerSubscriptionAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removeMaxConsumersPerSubscriptionAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "maxConsumersPerSubscription");
        return asyncDeleteRequest(path);
    }

    @Override
    public Integer getMaxProducers(String topic) throws PulsarAdminException {
        return getMaxProducers(topic, false);
    }

    @Override
    public CompletableFuture<Integer> getMaxProducersAsync(String topic) {
        return getMaxProducersAsync(topic, false);
    }

    @Override
    public Integer getMaxProducers(String topic, boolean applied) throws PulsarAdminException {
        return sync(() -> getMaxProducersAsync(topic, applied));
    }

    @Override
    public CompletableFuture<Integer> getMaxProducersAsync(String topic, boolean applied) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxProducers");
        path = path.queryParam("applied", applied);
        return asyncGetRequest(path, new FutureCallback<Integer>(){});
    }

    @Override
    public void setMaxProducers(String topic, int maxProducers) throws PulsarAdminException {
        sync(() -> setMaxProducersAsync(topic, maxProducers));
    }

    @Override
    public CompletableFuture<Void> setMaxProducersAsync(String topic, int maxProducers) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxProducers");
        return asyncPostRequest(path, Entity.entity(maxProducers, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeMaxProducers(String topic) throws PulsarAdminException {
        sync(() -> removeMaxProducersAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removeMaxProducersAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxProducers");
        return asyncDeleteRequest(path);
    }

    @Override
    public Integer getMaxSubscriptionsPerTopic(String topic) throws PulsarAdminException {
        return sync(() -> getMaxSubscriptionsPerTopicAsync(topic));
    }

    @Override
    public CompletableFuture<Integer> getMaxSubscriptionsPerTopicAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxSubscriptionsPerTopic");
        return asyncGetRequest(path, new FutureCallback<Integer>(){});
    }

    @Override
    public void setMaxSubscriptionsPerTopic(String topic, int maxSubscriptionsPerTopic) throws PulsarAdminException {
        sync(() -> setMaxSubscriptionsPerTopicAsync(topic, maxSubscriptionsPerTopic));
    }

    @Override
    public CompletableFuture<Void> setMaxSubscriptionsPerTopicAsync(String topic, int maxSubscriptionsPerTopic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxSubscriptionsPerTopic");
        return asyncPostRequest(path, Entity.entity(maxSubscriptionsPerTopic, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeMaxSubscriptionsPerTopic(String topic) throws PulsarAdminException {
        sync(() -> removeMaxSubscriptionsPerTopicAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removeMaxSubscriptionsPerTopicAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxSubscriptionsPerTopic");
        return asyncDeleteRequest(path);
    }

    @Override
    public Integer getMaxMessageSize(String topic) throws PulsarAdminException {
        return sync(() -> getMaxMessageSizeAsync(topic));
    }

    @Override
    public CompletableFuture<Integer> getMaxMessageSizeAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxMessageSize");
        return asyncGetRequest(path, new FutureCallback<Integer>(){});
    }

    @Override
    public void setMaxMessageSize(String topic, int maxMessageSize) throws PulsarAdminException {
        sync(() -> setMaxMessageSizeAsync(topic, maxMessageSize));
    }

    @Override
    public CompletableFuture<Void> setMaxMessageSizeAsync(String topic, int maxMessageSize) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxMessageSize");
        return asyncPostRequest(path, Entity.entity(maxMessageSize, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeMaxMessageSize(String topic) throws PulsarAdminException {
        sync(() -> removeMaxMessageSizeAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removeMaxMessageSizeAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxMessageSize");
        return asyncDeleteRequest(path);
    }

    @Override
    public Integer getMaxConsumers(String topic) throws PulsarAdminException {
        return getMaxConsumers(topic, false);
    }

    @Override
    public CompletableFuture<Integer> getMaxConsumersAsync(String topic) {
        return getMaxConsumersAsync(topic, false);
    }

    @Override
    public Integer getMaxConsumers(String topic, boolean applied) throws PulsarAdminException {
        return sync(() -> getMaxConsumersAsync(topic, applied));
    }

    @Override
    public CompletableFuture<Integer> getMaxConsumersAsync(String topic, boolean applied) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxConsumers");
        path = path.queryParam("applied", applied);
        return asyncGetRequest(path, new FutureCallback<Integer>(){});
    }

    @Override
    public void setMaxConsumers(String topic, int maxConsumers) throws PulsarAdminException {
        sync(() -> setMaxConsumersAsync(topic, maxConsumers));
    }

    @Override
    public CompletableFuture<Void> setMaxConsumersAsync(String topic, int maxConsumers) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxConsumers");
        return asyncPostRequest(path, Entity.entity(maxConsumers, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeMaxConsumers(String topic) throws PulsarAdminException {
        sync(() -> removeMaxConsumersAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removeMaxConsumersAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxConsumers");
        return asyncDeleteRequest(path);
    }


    @Override
    public Integer getDeduplicationSnapshotInterval(String topic) throws PulsarAdminException {
        return sync(() -> getDeduplicationSnapshotIntervalAsync(topic));
    }

    @Override
    public CompletableFuture<Integer> getDeduplicationSnapshotIntervalAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "deduplicationSnapshotInterval");
        return asyncGetRequest(path, new FutureCallback<Integer>(){});
    }

    @Override
    public void setDeduplicationSnapshotInterval(String topic, int interval) throws PulsarAdminException {
        sync(() -> setDeduplicationSnapshotIntervalAsync(topic, interval));
    }

    @Override
    public CompletableFuture<Void> setDeduplicationSnapshotIntervalAsync(String topic, int interval) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "deduplicationSnapshotInterval");
        return asyncPostRequest(path, Entity.entity(interval, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeDeduplicationSnapshotInterval(String topic) throws PulsarAdminException {
        sync(() -> removeDeduplicationSnapshotIntervalAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removeDeduplicationSnapshotIntervalAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "deduplicationSnapshotInterval");
        return asyncDeleteRequest(path);
    }

    @Override
    public void setSubscriptionTypesEnabled(
            String topic, Set<SubscriptionType>
            subscriptionTypesEnabled) throws PulsarAdminException {
        sync(() -> setSubscriptionTypesEnabledAsync(topic, subscriptionTypesEnabled));
    }

    @Override
    public CompletableFuture<Void> setSubscriptionTypesEnabledAsync(String topic,
                                                                    Set<SubscriptionType> subscriptionTypesEnabled) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "subscriptionTypesEnabled");
        return asyncPostRequest(path, Entity.entity(subscriptionTypesEnabled, MediaType.APPLICATION_JSON));
    }

    @Override
    public Set<SubscriptionType> getSubscriptionTypesEnabled(String topic) throws PulsarAdminException {
        return sync(() -> getSubscriptionTypesEnabledAsync(topic));
    }

    @Override
    public CompletableFuture<Set<SubscriptionType>> getSubscriptionTypesEnabledAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "subscriptionTypesEnabled");
        return asyncGetRequest(path, new FutureCallback<Set<SubscriptionType>>(){});
    }

    @Override
    public void removeSubscriptionTypesEnabled(String topic) throws PulsarAdminException {
        sync(() -> removeSubscriptionTypesEnabledAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removeSubscriptionTypesEnabledAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "subscriptionTypesEnabled");
        return asyncDeleteRequest(path);
    }

    @Override
    public SubscribeRate getSubscribeRate(String topic) throws PulsarAdminException {
        return getSubscribeRate(topic, false);
    }

    @Override
    public CompletableFuture<SubscribeRate> getSubscribeRateAsync(String topic) {
        return getSubscribeRateAsync(topic, false);
    }

    @Override
    public SubscribeRate getSubscribeRate(String topic, boolean applied) throws PulsarAdminException {
        return sync(() -> getSubscribeRateAsync(topic, applied));
    }

    @Override
    public CompletableFuture<SubscribeRate> getSubscribeRateAsync(String topic, boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "subscribeRate");
        path = path.queryParam("applied", applied);
        return asyncGetRequest(path, new FutureCallback<SubscribeRate>(){});
    }

    @Override
    public void setSubscribeRate(String topic, SubscribeRate subscribeRate) throws PulsarAdminException {
        sync(() -> setSubscribeRateAsync(topic, subscribeRate));
    }

    @Override
    public CompletableFuture<Void> setSubscribeRateAsync(String topic, SubscribeRate subscribeRate) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "subscribeRate");
        return asyncPostRequest(path, Entity.entity(subscribeRate, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeSubscribeRate(String topic) throws PulsarAdminException {
        sync(() -> removeSubscribeRateAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removeSubscribeRateAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "subscribeRate");
        return asyncDeleteRequest(path);
    }

    @Override
    public DispatchRate getReplicatorDispatchRate(String topic) throws PulsarAdminException {
        return getReplicatorDispatchRate(topic, false);
    }

    @Override
    public CompletableFuture<DispatchRate> getReplicatorDispatchRateAsync(String topic) {
        return getReplicatorDispatchRateAsync(topic, false);
    }

    @Override
    public DispatchRate getReplicatorDispatchRate(String topic, boolean applied) throws PulsarAdminException {
        return sync(() -> getReplicatorDispatchRateAsync(topic, applied));
    }

    @Override
    public CompletableFuture<DispatchRate> getReplicatorDispatchRateAsync(String topic, boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "replicatorDispatchRate");
        path = path.queryParam("applied", applied);
        return asyncGetRequest(path, new FutureCallback<DispatchRate>(){});
    }

    @Override
    public void setReplicatorDispatchRate(String topic, DispatchRate dispatchRate) throws PulsarAdminException {
        sync(() -> setReplicatorDispatchRateAsync(topic, dispatchRate));
    }

    @Override
    public CompletableFuture<Void> setReplicatorDispatchRateAsync(String topic, DispatchRate dispatchRate) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "replicatorDispatchRate");
        return asyncPostRequest(path, Entity.entity(dispatchRate, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeReplicatorDispatchRate(String topic) throws PulsarAdminException {
        sync(() -> removeReplicatorDispatchRateAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removeReplicatorDispatchRateAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "replicatorDispatchRate");
        return asyncDeleteRequest(path);
    }

    @Override
    public SchemaCompatibilityStrategy getSchemaCompatibilityStrategy(String topic, boolean applied)
            throws PulsarAdminException {
        return sync(() -> getSchemaCompatibilityStrategyAsync(topic, applied));
    }

    @Override
    public CompletableFuture<SchemaCompatibilityStrategy> getSchemaCompatibilityStrategyAsync(String topic,
                                                                                              boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "schemaCompatibilityStrategy");
        path = path.queryParam("applied", applied);
        return asyncGetRequest(path, new FutureCallback<SchemaCompatibilityStrategy>(){});
    }

    @Override
    public void setSchemaCompatibilityStrategy(String topic, SchemaCompatibilityStrategy strategy)
            throws PulsarAdminException {
        sync(() -> setSchemaCompatibilityStrategyAsync(topic, strategy));
    }

    @Override
    public CompletableFuture<Void> setSchemaCompatibilityStrategyAsync(String topic,
                                                                       SchemaCompatibilityStrategy strategy) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "schemaCompatibilityStrategy");
        return asyncPutRequest(path, Entity.entity(strategy, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeSchemaCompatibilityStrategy(String topic)
            throws PulsarAdminException {
        sync(()->removeSchemaCompatibilityStrategyAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removeSchemaCompatibilityStrategyAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "schemaCompatibilityStrategy");
        return asyncDeleteRequest(path);
    }

    @Override
    public EntryFilters getEntryFiltersPerTopic(String topic, boolean applied) throws PulsarAdminException {
        return sync(() -> getEntryFiltersPerTopicAsync(topic, applied));
    }

    @Override
    public CompletableFuture<EntryFilters> getEntryFiltersPerTopicAsync(String topic, boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "entryFilters");
        path = path.queryParam("applied", applied);
        return asyncGetRequest(path, new FutureCallback<EntryFilters>(){});
    }

    @Override
    public void setEntryFiltersPerTopic(String topic, EntryFilters entryFilters)
            throws PulsarAdminException {
        sync(() -> setEntryFiltersPerTopicAsync(topic, entryFilters));
    }

    @Override
    public CompletableFuture<Void> setEntryFiltersPerTopicAsync(String topic, EntryFilters entryFilters) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "entryFilters");
        return asyncPostRequest(path, Entity.entity(entryFilters, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeEntryFiltersPerTopic(String topic) throws PulsarAdminException {
        sync(() -> removeEntryFiltersPerTopicAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removeEntryFiltersPerTopicAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "entryFilters");
        return asyncDeleteRequest(path);
    }

    @Override
    public void setAutoSubscriptionCreation(
            String topic, AutoSubscriptionCreationOverride autoSubscriptionCreationOverride)
            throws PulsarAdminException {
        sync(() -> setAutoSubscriptionCreationAsync(topic, autoSubscriptionCreationOverride));
    }

    @Override
    public CompletableFuture<Void> setAutoSubscriptionCreationAsync(
            String topic, AutoSubscriptionCreationOverride autoSubscriptionCreationOverride) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "autoSubscriptionCreation");
        return asyncPostRequest(path, Entity.entity(autoSubscriptionCreationOverride, MediaType.APPLICATION_JSON));
    }

    @Override
    public AutoSubscriptionCreationOverride getAutoSubscriptionCreation(String topic,
                                                                        boolean applied) throws PulsarAdminException {
        return sync(() -> getAutoSubscriptionCreationAsync(topic, applied));
    }

    @Override
    public CompletableFuture<AutoSubscriptionCreationOverride> getAutoSubscriptionCreationAsync(String topic,
                                                                                                boolean applied) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "autoSubscriptionCreation");
        path = path.queryParam("applied", applied);
        return asyncGetRequest(path, new FutureCallback<AutoSubscriptionCreationOverride>() {});
    }

    @Override
    public void removeAutoSubscriptionCreation(String topic) throws PulsarAdminException {
        sync(() -> removeAutoSubscriptionCreationAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removeAutoSubscriptionCreationAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "autoSubscriptionCreation");
        return asyncDeleteRequest(path);
    }

    @Override
    public String getResourceGroup(String topic, boolean applied) throws PulsarAdminException {
        return sync(() -> getResourceGroupAsync(topic, applied));
    }

    @Override
    public CompletableFuture<String> getResourceGroupAsync(String topic, boolean applied) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "resourceGroup");
        path = path.queryParam("applied", applied);
        final CompletableFuture<String> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<String>() {
                    @Override
                    public void completed(String rgName) {
                        future.complete(rgName);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setResourceGroup(String topic, String resourceGroupName) throws PulsarAdminException {
        sync(() -> setResourceGroupAsync(topic, resourceGroupName));
    }

    @Override
    public CompletableFuture<Void> setResourceGroupAsync(String topic, String resourceGroupName) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "resourceGroup");
        return asyncPostRequest(path, Entity.entity(resourceGroupName, MediaType.APPLICATION_JSON_TYPE));
    }

    @Override
    public void removeResourceGroup(String topic) throws PulsarAdminException {
        sync(() -> removeResourceGroupAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removeResourceGroupAsync(String topic) {
        return setResourceGroupAsync(topic, null);
    }

    /*
     * returns topic name with encoded Local Name
     */
    private TopicName validateTopic(String topic) {
        // Parsing will throw exception if name is not valid
        return TopicName.get(topic);
    }

    private WebTarget topicPath(TopicName topic, String... parts) {
        final WebTarget base = topic.isV2() ? adminV2Topics : adminTopics;
        WebTarget topicPath = base.path(topic.getRestPath());
        topicPath = WebTargets.addParts(topicPath, parts);
        topicPath = addGlobalIfNeeded(topicPath);
        return topicPath;
    }
}
