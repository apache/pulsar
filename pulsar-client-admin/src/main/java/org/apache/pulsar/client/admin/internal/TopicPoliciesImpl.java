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
package org.apache.pulsar.client.admin.internal;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
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
    public void setBacklogQuota(String topic, BacklogQuota backlogQuota) throws PulsarAdminException {
        TopicPolicies.super.setBacklogQuota(topic, backlogQuota);
    }


    @Override
    public void removeBacklogQuota(String topic) throws PulsarAdminException {
        TopicPolicies.super.removeBacklogQuota(topic);
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
        try {
            return getMaxUnackedMessagesOnConsumerAsync(topic, applied).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Integer> getMaxUnackedMessagesOnConsumerAsync(String topic, boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "maxUnackedMessagesOnConsumer");
        path = path.queryParam("applied", applied);
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path, new InvocationCallback<Integer>() {
            @Override
            public void completed(Integer maxNum) {
                future.complete(maxNum);
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(getApiException(throwable.getCause()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> setMaxUnackedMessagesOnConsumerAsync(String topic, int maxNum) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "maxUnackedMessagesOnConsumer");

        return asyncPostRequest(path, Entity.entity(maxNum, MediaType.APPLICATION_JSON));
    }

    @Override
    public void setMaxUnackedMessagesOnConsumer(String topic, int maxNum) throws PulsarAdminException {
        try {
            setMaxUnackedMessagesOnConsumerAsync(topic, maxNum)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> removeMaxUnackedMessagesOnConsumerAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "maxUnackedMessagesOnConsumer");
        return asyncDeleteRequest(path);
    }

    @Override
    public void removeMaxUnackedMessagesOnConsumer(String topic) throws PulsarAdminException {
        try {
            removeMaxUnackedMessagesOnConsumerAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public InactiveTopicPolicies getInactiveTopicPolicies(String topic, boolean applied) throws PulsarAdminException {
        try {
            return getInactiveTopicPoliciesAsync(topic, applied).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<InactiveTopicPolicies> getInactiveTopicPoliciesAsync(String topic, boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "inactiveTopicPolicies");
        path = path.queryParam("applied", applied);
        final CompletableFuture<InactiveTopicPolicies> future = new CompletableFuture<>();
        asyncGetRequest(path, new InvocationCallback<InactiveTopicPolicies>() {
            @Override
            public void completed(InactiveTopicPolicies inactiveTopicPolicies) {
                future.complete(inactiveTopicPolicies);
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(getApiException(throwable.getCause()));
            }
        });
        return future;
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
        try {
            setInactiveTopicPoliciesAsync(topic, inactiveTopicPolicies)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> removeInactiveTopicPoliciesAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "inactiveTopicPolicies");
        return asyncDeleteRequest(path);
    }

    @Override
    public void removeInactiveTopicPolicies(String topic) throws PulsarAdminException {
        try {
            removeInactiveTopicPoliciesAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public DelayedDeliveryPolicies getDelayedDeliveryPolicy(String topic
            , boolean applied) throws PulsarAdminException {
        try {
            return getDelayedDeliveryPolicyAsync(topic, applied).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<DelayedDeliveryPolicies> getDelayedDeliveryPolicyAsync(String topic
            , boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "delayedDelivery");
        path = path.queryParam("applied", applied);

        final CompletableFuture<DelayedDeliveryPolicies> future = new CompletableFuture<>();
        asyncGetRequest(path, new InvocationCallback<DelayedDeliveryPolicies>() {
            @Override
            public void completed(DelayedDeliveryPolicies delayedDeliveryPolicies) {
                future.complete(delayedDeliveryPolicies);
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(getApiException(throwable.getCause()));
            }
        });
        return future;
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
        try {
            removeDelayedDeliveryPolicyAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
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
        try {
            setDelayedDeliveryPolicyAsync(topic, delayedDeliveryPolicies)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
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
        try {
            return getDeduplicationStatusAsync(topic, applied).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> getDeduplicationStatusAsync(String topic, boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "deduplicationEnabled");
        path = path.queryParam("applied", applied);
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        asyncGetRequest(path, new InvocationCallback<Boolean>() {
            @Override
            public void completed(Boolean enabled) {
                future.complete(enabled);
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(getApiException(throwable.getCause()));
            }
        });
        return future;
    }

    private CompletableFuture<Void> enableDeduplicationAsync(String topic, boolean enabled) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "deduplicationEnabled");
        return asyncPostRequest(path, Entity.entity(enabled, MediaType.APPLICATION_JSON));
    }

    @Override
    public void setDeduplicationStatus(String topic, boolean enabled) throws PulsarAdminException {
        try {
            enableDeduplicationAsync(topic, enabled).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> setDeduplicationStatusAsync(String topic, boolean enabled) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "deduplicationEnabled");
        return asyncPostRequest(path, Entity.entity(enabled, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeDeduplicationStatus(String topic) throws PulsarAdminException {
        try {
            removeDeduplicationStatusAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
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
        try {
            return getOffloadPoliciesAsync(topic, applied).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<OffloadPolicies> getOffloadPoliciesAsync(String topic, boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "offloadPolicies");
        path = path.queryParam("applied", applied);
        final CompletableFuture<OffloadPolicies> future = new CompletableFuture<>();
        asyncGetRequest(path, new InvocationCallback<OffloadPoliciesImpl>() {
            @Override
            public void completed(OffloadPoliciesImpl offloadPolicies) {
                future.complete(offloadPolicies);
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(getApiException(throwable.getCause()));
            }
        });
        return future;
    }

    @Override
    public void setOffloadPolicies(String topic, OffloadPolicies offloadPolicies) throws PulsarAdminException {
        try {
            setOffloadPoliciesAsync(topic, offloadPolicies).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> setOffloadPoliciesAsync(String topic, OffloadPolicies offloadPolicies) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "offloadPolicies");
        return asyncPostRequest(path, Entity.entity((OffloadPoliciesImpl) offloadPolicies, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeOffloadPolicies(String topic) throws PulsarAdminException {
        try {
            removeOffloadPoliciesAsync(topic).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
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
        try {
            return getMaxUnackedMessagesOnSubscriptionAsync(topic, applied).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Integer> getMaxUnackedMessagesOnSubscriptionAsync(String topic, boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "maxUnackedMessagesOnSubscription");
        path = path.queryParam("applied", applied);
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path, new InvocationCallback<Integer>() {
            @Override
            public void completed(Integer maxNum) {
                future.complete(maxNum);
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(getApiException(throwable.getCause()));
            }
        });
        return future;
    }

    @Override
    public void setMaxUnackedMessagesOnSubscription(String topic, int maxNum) throws PulsarAdminException {
        try {
            setMaxUnackedMessagesOnSubscriptionAsync(topic, maxNum).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> setMaxUnackedMessagesOnSubscriptionAsync(String topic, int maxNum) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "maxUnackedMessagesOnSubscription");
        return asyncPostRequest(path, Entity.entity(maxNum, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeMaxUnackedMessagesOnSubscription(String topic) throws PulsarAdminException {
        try {
            removeMaxUnackedMessagesOnSubscriptionAsync(topic).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
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
        try {
            setRetentionAsync(topic, retention).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
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
        try {
            return getRetentionAsync(topic, applied).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<RetentionPolicies> getRetentionAsync(String topic, boolean applied) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "retention");
        path = path.queryParam("applied", applied);
        final CompletableFuture<RetentionPolicies> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<RetentionPolicies>() {
                    @Override
                    public void completed(RetentionPolicies retentionPolicies) {
                        future.complete(retentionPolicies);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void removeRetention(String topic) throws PulsarAdminException {
        try {
            removeRetentionAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> removeRetentionAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "retention");
        return asyncDeleteRequest(path);
    }

    @Override
    public void setPersistence(String topic, PersistencePolicies persistencePolicies) throws PulsarAdminException {
        try {
            setPersistenceAsync(topic, persistencePolicies).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
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
        try {
            return getPersistenceAsync(topic, applied).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<PersistencePolicies> getPersistenceAsync(String topic, boolean applied) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "persistence");
        path = path.queryParam("applied", applied);
        final CompletableFuture<PersistencePolicies> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<PersistencePolicies>() {
                    @Override
                    public void completed(PersistencePolicies persistencePolicies) {
                        future.complete(persistencePolicies);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void removePersistence(String topic) throws PulsarAdminException {
        try {
            removePersistenceAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> removePersistenceAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "persistence");
        return asyncDeleteRequest(path);
    }

    @Override
    public DispatchRate getDispatchRate(String topic, boolean applied) throws PulsarAdminException {
        try {
            return getDispatchRateAsync(topic, applied).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<DispatchRate> getDispatchRateAsync(String topic, boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "dispatchRate");
        path = path.queryParam("applied", applied);
        final CompletableFuture<DispatchRate> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<DispatchRate>() {
                    @Override
                    public void completed(DispatchRate dispatchRate) {
                        future.complete(dispatchRate);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
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
        try {
            setDispatchRateAsync(topic, dispatchRate).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> setDispatchRateAsync(String topic, DispatchRate dispatchRate) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "dispatchRate");
        return asyncPostRequest(path, Entity.entity(dispatchRate, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeDispatchRate(String topic) throws PulsarAdminException {
        try {
            removeDispatchRateAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> removeDispatchRateAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "dispatchRate");
        return asyncDeleteRequest(path);
    }

    @Override
    public DispatchRate getSubscriptionDispatchRate(String topic, boolean applied) throws PulsarAdminException {
        try {
            return getSubscriptionDispatchRateAsync(topic, applied).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<DispatchRate> getSubscriptionDispatchRateAsync(String topic, boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "subscriptionDispatchRate");
        path = path.queryParam("applied", applied);
        final CompletableFuture<DispatchRate> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<DispatchRate>() {
                    @Override
                    public void completed(DispatchRate dispatchRate) {
                        future.complete(dispatchRate);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
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
        try {
            setSubscriptionDispatchRateAsync(topic, dispatchRate).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> setSubscriptionDispatchRateAsync(String topic, DispatchRate dispatchRate) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "subscriptionDispatchRate");
        return asyncPostRequest(path, Entity.entity(dispatchRate, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeSubscriptionDispatchRate(String topic) throws PulsarAdminException {
        try {
            removeSubscriptionDispatchRateAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> removeSubscriptionDispatchRateAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "subscriptionDispatchRate");
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
        try {
            return getCompactionThresholdAsync(topic, applied).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Long> getCompactionThresholdAsync(String topic, boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "compactionThreshold");
        path = path.queryParam("applied", applied);
        final CompletableFuture<Long> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Long>() {
                    @Override
                    public void completed(Long compactionThreshold) {
                        future.complete(compactionThreshold);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setCompactionThreshold(String topic, long compactionThreshold) throws PulsarAdminException {
        try {
            setCompactionThresholdAsync(topic, compactionThreshold).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> setCompactionThresholdAsync(String topic, long compactionThreshold) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "compactionThreshold");
        return asyncPostRequest(path, Entity.entity(compactionThreshold, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeCompactionThreshold(String topic) throws PulsarAdminException {
        try {
            removeCompactionThresholdAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> removeCompactionThresholdAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "compactionThreshold");
        return asyncDeleteRequest(path);
    }

    @Override
    public PublishRate getPublishRate(String topic) throws PulsarAdminException {
        try {
            return getPublishRateAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<PublishRate> getPublishRateAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "publishRate");
        final CompletableFuture<PublishRate> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<PublishRate>() {
                    @Override
                    public void completed(PublishRate publishRate) {
                        future.complete(publishRate);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setPublishRate(String topic, PublishRate publishRate) throws PulsarAdminException {
        try {
            setPublishRateAsync(topic, publishRate).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> setPublishRateAsync(String topic, PublishRate publishRate) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "publishRate");
        return asyncPostRequest(path, Entity.entity(publishRate, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removePublishRate(String topic) throws PulsarAdminException {
        try {
            removePublishRateAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> removePublishRateAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "publishRate");
        return asyncDeleteRequest(path);
    }

    @Override
    public Integer getMaxConsumersPerSubscription(String topic) throws PulsarAdminException {
        try {
            return getMaxConsumersPerSubscriptionAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Integer> getMaxConsumersPerSubscriptionAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "maxConsumersPerSubscription");
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Integer>() {
                    @Override
                    public void completed(Integer maxConsumersPerSubscription) {
                        future.complete(maxConsumersPerSubscription);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setMaxConsumersPerSubscription(String topic, int maxConsumersPerSubscription)
            throws PulsarAdminException {
        try {
            setMaxConsumersPerSubscriptionAsync(topic, maxConsumersPerSubscription)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> setMaxConsumersPerSubscriptionAsync(String topic, int maxConsumersPerSubscription) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "maxConsumersPerSubscription");
        return asyncPostRequest(path, Entity.entity(maxConsumersPerSubscription, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeMaxConsumersPerSubscription(String topic) throws PulsarAdminException {
        try {
            removeMaxConsumersPerSubscriptionAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
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
        try {
            return getMaxProducersAsync(topic, applied).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Integer> getMaxProducersAsync(String topic, boolean applied) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxProducers");
        path = path.queryParam("applied", applied);
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Integer>() {
                    @Override
                    public void completed(Integer maxProducers) {
                        future.complete(maxProducers);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setMaxProducers(String topic, int maxProducers) throws PulsarAdminException {
        try {
            setMaxProducersAsync(topic, maxProducers).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> setMaxProducersAsync(String topic, int maxProducers) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxProducers");
        return asyncPostRequest(path, Entity.entity(maxProducers, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeMaxProducers(String topic) throws PulsarAdminException {
        try {
            removeMaxProducersAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> removeMaxProducersAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxProducers");
        return asyncDeleteRequest(path);
    }

    @Override
    public Integer getMaxSubscriptionsPerTopic(String topic) throws PulsarAdminException {
        try {
            return getMaxSubscriptionsPerTopicAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Integer> getMaxSubscriptionsPerTopicAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxSubscriptionsPerTopic");
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Integer>() {
                    @Override
                    public void completed(Integer maxSubscriptionsPerTopic) {
                        future.complete(maxSubscriptionsPerTopic);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setMaxSubscriptionsPerTopic(String topic, int maxSubscriptionsPerTopic) throws PulsarAdminException {
        try {
            setMaxSubscriptionsPerTopicAsync(topic, maxSubscriptionsPerTopic)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> setMaxSubscriptionsPerTopicAsync(String topic, int maxSubscriptionsPerTopic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxSubscriptionsPerTopic");
        return asyncPostRequest(path, Entity.entity(maxSubscriptionsPerTopic, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeMaxSubscriptionsPerTopic(String topic) throws PulsarAdminException {
        try {
            removeMaxSubscriptionsPerTopicAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> removeMaxSubscriptionsPerTopicAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxSubscriptionsPerTopic");
        return asyncDeleteRequest(path);
    }

    @Override
    public Integer getMaxMessageSize(String topic) throws PulsarAdminException {
        try {
            return getMaxMessageSizeAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Integer> getMaxMessageSizeAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxMessageSize");
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Integer>() {
                    @Override
                    public void completed(Integer maxMessageSize) {
                        future.complete(maxMessageSize);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setMaxMessageSize(String topic, int maxMessageSize) throws PulsarAdminException {
        try {
            setMaxMessageSizeAsync(topic, maxMessageSize).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> setMaxMessageSizeAsync(String topic, int maxMessageSize) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxMessageSize");
        return asyncPostRequest(path, Entity.entity(maxMessageSize, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeMaxMessageSize(String topic) throws PulsarAdminException {
        try {
            removeMaxMessageSizeAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
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
        try {
            return getMaxConsumersAsync(topic, applied).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Integer> getMaxConsumersAsync(String topic, boolean applied) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxConsumers");
        path = path.queryParam("applied", applied);
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Integer>() {
                    @Override
                    public void completed(Integer maxProducers) {
                        future.complete(maxProducers);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setMaxConsumers(String topic, int maxConsumers) throws PulsarAdminException {
        try {
            setMaxConsumersAsync(topic, maxConsumers).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> setMaxConsumersAsync(String topic, int maxConsumers) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxConsumers");
        return asyncPostRequest(path, Entity.entity(maxConsumers, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeMaxConsumers(String topic) throws PulsarAdminException {
        try {
            removeMaxConsumersAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> removeMaxConsumersAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxConsumers");
        return asyncDeleteRequest(path);
    }


    @Override
    public Integer getDeduplicationSnapshotInterval(String topic) throws PulsarAdminException {
        try {
            return getDeduplicationSnapshotIntervalAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Integer> getDeduplicationSnapshotIntervalAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "deduplicationSnapshotInterval");
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Integer>() {
                    @Override
                    public void completed(Integer interval) {
                        future.complete(interval);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setDeduplicationSnapshotInterval(String topic, int interval) throws PulsarAdminException {
        try {
            setDeduplicationSnapshotIntervalAsync(topic, interval).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> setDeduplicationSnapshotIntervalAsync(String topic, int interval) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "deduplicationSnapshotInterval");
        return asyncPostRequest(path, Entity.entity(interval, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeDeduplicationSnapshotInterval(String topic) throws PulsarAdminException {
        try {
            removeDeduplicationSnapshotIntervalAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
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
        try {
            setSubscriptionTypesEnabledAsync(topic, subscriptionTypesEnabled)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
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
        try {
            return getSubscriptionTypesEnabledAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Set<SubscriptionType>> getSubscriptionTypesEnabledAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "subscriptionTypesEnabled");
        final CompletableFuture<Set<SubscriptionType>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Set<SubscriptionType>>() {
                    @Override
                    public void completed(Set<SubscriptionType> subscriptionTypesEnabled) {
                        future.complete(subscriptionTypesEnabled);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
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
        try {
            return getSubscribeRateAsync(topic, applied).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<SubscribeRate> getSubscribeRateAsync(String topic, boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "subscribeRate");
        path = path.queryParam("applied", applied);
        final CompletableFuture<SubscribeRate> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<SubscribeRate>() {
                    @Override
                    public void completed(SubscribeRate subscribeRate) {
                        future.complete(subscribeRate);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setSubscribeRate(String topic, SubscribeRate subscribeRate) throws PulsarAdminException {
        try {
            setSubscribeRateAsync(topic, subscribeRate).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> setSubscribeRateAsync(String topic, SubscribeRate subscribeRate) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "subscribeRate");
        return asyncPostRequest(path, Entity.entity(subscribeRate, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeSubscribeRate(String topic) throws PulsarAdminException {
        try {
            removeSubscribeRateAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
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
        try {
            return getReplicatorDispatchRateAsync(topic, applied).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<DispatchRate> getReplicatorDispatchRateAsync(String topic, boolean applied) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "replicatorDispatchRate");
        path = path.queryParam("applied", applied);
        final CompletableFuture<DispatchRate> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<DispatchRate>() {
                    @Override
                    public void completed(DispatchRate dispatchRate) {
                        future.complete(dispatchRate);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setReplicatorDispatchRate(String topic, DispatchRate dispatchRate) throws PulsarAdminException {
        try {
            setReplicatorDispatchRateAsync(topic, dispatchRate).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> setReplicatorDispatchRateAsync(String topic, DispatchRate dispatchRate) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "replicatorDispatchRate");
        return asyncPostRequest(path, Entity.entity(dispatchRate, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeReplicatorDispatchRate(String topic) throws PulsarAdminException {
        try {
            removeReplicatorDispatchRateAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> removeReplicatorDispatchRateAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "replicatorDispatchRate");
        return asyncDeleteRequest(path);
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
