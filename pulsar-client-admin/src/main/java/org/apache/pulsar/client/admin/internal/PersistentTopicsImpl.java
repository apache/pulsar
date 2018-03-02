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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.pulsar.client.admin.PersistentTopics;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyValue;
import org.apache.pulsar.common.api.proto.PulsarApi.SingleMessageMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicStats;
import org.apache.pulsar.common.util.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class PersistentTopicsImpl extends BaseResource implements PersistentTopics {
    private final WebTarget persistentTopics;
    private final String BATCH_HEADER = "X-Pulsar-num-batch-message";
    public PersistentTopicsImpl(WebTarget web, Authentication auth) {
        super(auth);
        this.persistentTopics = web.path("/persistent");
    }


    @Override
    public List<String> getList(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            return request(persistentTopics.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName())).get(
                    new GenericType<List<String>>() {
                    });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public List<String> getPartitionedTopicList(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            return request(persistentTopics.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path("partitioned")).get(
                    new GenericType<List<String>>() {
                    });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public Map<String, Set<AuthAction>> getPermissions(String topic) throws PulsarAdminException {
        try {
            TopicName ds = TopicName.get(topic);
            return request(persistentTopics.path(ds.getNamespace()).path(ds.getLocalName()).path("permissions")).get(
                    new GenericType<Map<String, Set<AuthAction>>>() {
                    });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void grantPermission(String topic, String role, Set<AuthAction> actions) throws PulsarAdminException {
        try {
            TopicName ds = TopicName.get(topic);
            request(persistentTopics.path(ds.getNamespace()).path(ds.getLocalName()).path("permissions").path(role))
                    .post(Entity.entity(actions, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void revokePermissions(String topic, String role) throws PulsarAdminException {
        try {
            TopicName ds = TopicName.get(topic);
            request(persistentTopics.path(ds.getNamespace()).path(ds.getLocalName()).path("permissions").path(role))
                    .delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createPartitionedTopic(String topic, int numPartitions) throws PulsarAdminException {
        try {
            createPartitionedTopicAsync(topic, numPartitions).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Void> createPartitionedTopicAsync(String topic, int numPartitions) {
        checkArgument(numPartitions > 1, "Number of partitions should be more than 1");
        TopicName ds = validateTopic(topic);
        return asyncPutRequest(
                persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("partitions"),
                Entity.entity(numPartitions, MediaType.APPLICATION_JSON));
    }

    @Override
    public void updatePartitionedTopic(String topic, int numPartitions) throws PulsarAdminException {
        try {
            updatePartitionedTopicAsync(topic, numPartitions).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Void> updatePartitionedTopicAsync(String topic, int numPartitions) {
        checkArgument(numPartitions > 1, "Number of partitions must be more than 1");
        TopicName ds = validateTopic(topic);
        return asyncPostRequest(
                persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("partitions"),
                Entity.entity(numPartitions, MediaType.APPLICATION_JSON));
    }

    @Override
    public PartitionedTopicMetadata getPartitionedTopicMetadata(String topic) throws PulsarAdminException {
        try {
            return getPartitionedTopicMetadataAsync(topic).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadataAsync(String topic) {
        TopicName ds = validateTopic(topic);
        final CompletableFuture<PartitionedTopicMetadata> future = new CompletableFuture<>();
        asyncGetRequest(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("partitions"),
                new InvocationCallback<PartitionedTopicMetadata>() {

                    @Override
                    public void completed(PartitionedTopicMetadata response) {
                        future.complete(response);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void deletePartitionedTopic(String topic) throws PulsarAdminException {
        try {
            deletePartitionedTopicAsync(topic).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Void> deletePartitionedTopicAsync(String topic) {
        TopicName ds = validateTopic(topic);
        return asyncDeleteRequest(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName())
                .path("partitions"));
    }

    @Override
    public void delete(String topic) throws PulsarAdminException {
        try {
            deleteAsync(topic).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Void> deleteAsync(String topic) {
        TopicName ds = validateTopic(topic);
        return asyncDeleteRequest(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()));
    }

    @Override
    public void unload(String topic) throws PulsarAdminException {
        try {
            unloadAsync(topic).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Void> unloadAsync(String topic) {
        TopicName ds = validateTopic(topic);
        return asyncPutRequest(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("unload"),
                Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public List<String> getSubscriptions(String topic) throws PulsarAdminException {
        try {
            return getSubscriptionsAsync(topic).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<List<String>> getSubscriptionsAsync(String topic) {
        TopicName ds = validateTopic(topic);
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("subscriptions"),
                new InvocationCallback<List<String>>() {

                    @Override
                    public void completed(List<String> response) {
                        future.complete(response);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public PersistentTopicStats getStats(String topic) throws PulsarAdminException {
        try {
            return getStatsAsync(topic).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<PersistentTopicStats> getStatsAsync(String topic) {
        TopicName ds = validateTopic(topic);
        final CompletableFuture<PersistentTopicStats> future = new CompletableFuture<>();
        asyncGetRequest(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("stats"),
                new InvocationCallback<PersistentTopicStats>() {

                    @Override
                    public void completed(PersistentTopicStats response) {
                        future.complete(response);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public PersistentTopicInternalStats getInternalStats(String topic) throws PulsarAdminException {
        try {
            return getInternalStatsAsync(topic).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<PersistentTopicInternalStats> getInternalStatsAsync(String topic) {
        TopicName ds = validateTopic(topic);
        final CompletableFuture<PersistentTopicInternalStats> future = new CompletableFuture<>();
        asyncGetRequest(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("internalStats"),
                new InvocationCallback<PersistentTopicInternalStats>() {

                    @Override
                    public void completed(PersistentTopicInternalStats response) {
                        future.complete(response);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public JsonObject getInternalInfo(String topic) throws PulsarAdminException {
        try {
            return getInternalInfoAsync(topic).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<JsonObject> getInternalInfoAsync(String topic) {
        TopicName ds = validateTopic(topic);
        final CompletableFuture<JsonObject> future = new CompletableFuture<>();
        asyncGetRequest(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("internal-info"),
                new InvocationCallback<String>() {
                    @Override
                    public void completed(String response) {
                        JsonObject json = new Gson().fromJson(response, JsonObject.class);
                        future.complete(json);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public PartitionedTopicStats getPartitionedStats(String topic, boolean perPartition)
            throws PulsarAdminException {
        try {
            return getPartitionedStatsAsync(topic, perPartition).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<PartitionedTopicStats> getPartitionedStatsAsync(String topic,
            boolean perPartition) {
        TopicName ds = validateTopic(topic);
        final CompletableFuture<PartitionedTopicStats> future = new CompletableFuture<>();
        asyncGetRequest(
                persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("partitioned-stats"),
                new InvocationCallback<PartitionedTopicStats>() {

                    @Override
                    public void completed(PartitionedTopicStats response) {
                        if (!perPartition) {
                            response.partitions.clear();
                        }
                        future.complete(response);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void deleteSubscription(String topic, String subName) throws PulsarAdminException {
        try {
            deleteSubscriptionAsync(topic, subName).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Void> deleteSubscriptionAsync(String topic, String subName) {
        TopicName ds = validateTopic(topic);
        String encodedSubName = Codec.encode(subName);
        return asyncDeleteRequest(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName())
                .path("subscription").path(encodedSubName));
    }

    @Override
    public void skipAllMessages(String topic, String subName) throws PulsarAdminException {
        try {
            skipAllMessagesAsync(topic, subName).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Void> skipAllMessagesAsync(String topic, String subName) {
        TopicName ds = validateTopic(topic);
        String encodedSubName = Codec.encode(subName);
        return asyncPostRequest(
                persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("subscription")
                        .path(encodedSubName).path("skip_all"), Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void skipMessages(String topic, String subName, long numMessages) throws PulsarAdminException {
        try {
            skipMessagesAsync(topic, subName, numMessages).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Void> skipMessagesAsync(String topic, String subName, long numMessages) {
        TopicName ds = validateTopic(topic);
        String encodedSubName = Codec.encode(subName);
        return asyncPostRequest(
                persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("subscription")
                        .path(encodedSubName).path("skip").path(String.valueOf(numMessages)),
                Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void expireMessages(String topic, String subName, long expireTimeInSeconds) throws PulsarAdminException {
        try {
            expireMessagesAsync(topic, subName, expireTimeInSeconds).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Void> expireMessagesAsync(String topic, String subName, long expireTimeInSeconds) {
        TopicName ds = validateTopic(topic);
        String encodedSubName = Codec.encode(subName);
        return asyncPostRequest(
                persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("subscription")
                        .path(encodedSubName).path("expireMessages").path(String.valueOf(expireTimeInSeconds)),
                Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void expireMessagesForAllSubscriptions(String topic, long expireTimeInSeconds) throws PulsarAdminException {
        try {
            expireMessagesForAllSubscriptionsAsync(topic, expireTimeInSeconds).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Void> expireMessagesForAllSubscriptionsAsync(String topic, long expireTimeInSeconds) {
        TopicName ds = validateTopic(topic);
        return asyncPostRequest(
                persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("all_subscription")
                        .path("expireMessages").path(String.valueOf(expireTimeInSeconds)),
                Entity.entity("", MediaType.APPLICATION_JSON));
    }

    private CompletableFuture<List<Message<byte[]>>> peekNthMessage(String topic, String subName, int messagePosition) {
        TopicName ds = validateTopic(topic);
        String encodedSubName = Codec.encode(subName);
        final CompletableFuture<List<Message<byte[]>>> future = new CompletableFuture<>();
        asyncGetRequest(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("subscription")
                .path(encodedSubName).path("position").path(String.valueOf(messagePosition)),
                new InvocationCallback<Response>() {

                    @Override
                    public void completed(Response response) {
                        try {
                            future.complete(getMessageFromHttpResponse(response));
                        } catch (Exception e) {
                            future.completeExceptionally(getApiException(e));
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public List<Message<byte[]>> peekMessages(String topic, String subName, int numMessages)
            throws PulsarAdminException {
        try {
            return peekMessagesAsync(topic, subName, numMessages).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<List<Message<byte[]>>> peekMessagesAsync(String topic, String subName, int numMessages) {
        checkArgument(numMessages > 0);
        CompletableFuture<List<Message<byte[]>>> future = new CompletableFuture<List<Message<byte[]>>>();
        peekMessagesAsync(topic, subName, numMessages, Lists.newArrayList(), future, 1);
        return future;
    }


    private void peekMessagesAsync(String topic, String subName, int numMessages,
            List<Message<byte[]>> messages, CompletableFuture<List<Message<byte[]>>> future, int nthMessage) {
        if (numMessages <= 0) {
            future.complete(messages);
            return;
        }

        // if peeking first message succeeds, we know that the topic and subscription exists
        peekNthMessage(topic, subName, nthMessage).handle((r, ex) -> {
            if (ex != null) {
                // if we get a not found exception, it means that the position for the message we are trying to get
                // does not exist. At this point, we can return the already found messages.
                if (ex instanceof NotFoundException) {
                    log.warn("Exception '{}' occured while trying to peek Messages.", ex.getMessage());
                    future.complete(messages);
                } else {
                    future.completeExceptionally(ex);
                }
                return null;
            }
            for (int i = 0; i < Math.min(r.size(), numMessages); i++) {
                messages.add(r.get(i));
            }
            peekMessagesAsync(topic, subName, numMessages - r.size(), messages, future, nthMessage + 1);
            return null;
        });
    }

    @Override
    public void createSubscription(String topic, String subscriptionName, MessageId messageId)
            throws PulsarAdminException {
        try {
            TopicName ds = validateTopic(topic);
            String encodedSubName = Codec.encode(subscriptionName);
            request(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("subscription")
                    .path(encodedSubName)).put(Entity.entity(messageId, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public CompletableFuture<Void> createSubscriptionAsync(String topic, String subscriptionName,
            MessageId messageId) {
        TopicName ds = validateTopic(topic);
        String encodedSubName = Codec.encode(subscriptionName);
        return asyncPutRequest(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName())
                .path("subscription").path(encodedSubName), Entity.entity(messageId, MediaType.APPLICATION_JSON));
    }

    @Override
    public void resetCursor(String topic, String subName, long timestamp) throws PulsarAdminException {
        try {
            TopicName ds = validateTopic(topic);
            String encodedSubName = Codec.encode(subName);
            request(
                    persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("subscription")
                            .path(encodedSubName).path("resetcursor").path(String.valueOf(timestamp))).post(
                    Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public CompletableFuture<Void> resetCursorAsync(String topic, String subName, long timestamp) {
        TopicName ds = validateTopic(topic);
        String encodedSubName = Codec.encode(subName);
        return asyncPostRequest(
                persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("subscription")
                        .path(encodedSubName).path("resetcursor").path(String.valueOf(timestamp)),
                Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void resetCursor(String topic, String subName, MessageId messageId) throws PulsarAdminException {
        try {
            TopicName ds = validateTopic(topic);
            String encodedSubName = Codec.encode(subName);
            request(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("subscription")
                    .path(encodedSubName).path("resetcursor")).post(Entity.entity(messageId, MediaType.APPLICATION_JSON),
                            ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public CompletableFuture<Void> resetCursorAsync(String topic, String subName, MessageId messageId) {
        TopicName ds = validateTopic(topic);
        String encodedSubName = Codec.encode(subName);
        return asyncPostRequest(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName())
                .path("subscription").path(encodedSubName).path("resetcursor"),
                Entity.entity(messageId, MediaType.APPLICATION_JSON));
    }

    @Override
    public CompletableFuture<MessageId> terminateTopicAsync(String topic) {
        TopicName ds = validateTopic(topic);

        final CompletableFuture<MessageId> future = new CompletableFuture<>();
        try {
            WebTarget target = persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName())
                    .path("terminate");

            request(target).async().post(Entity.entity("", MediaType.APPLICATION_JSON),
                    new InvocationCallback<MessageIdImpl>() {

                        @Override
                        public void completed(MessageIdImpl messageId) {
                            future.complete(messageId);
                        }

                        @Override
                        public void failed(Throwable throwable) {
                            log.warn("[{}] Failed to perform http post request: {}", target.getUri(),
                                    throwable.getMessage());
                            future.completeExceptionally(getApiException(throwable.getCause()));
                        }
                    });
        } catch (PulsarAdminException cae) {
            future.completeExceptionally(cae);
        }

        return future;
    }

    /*
     * returns topic name with encoded Local Name
     */
    private TopicName validateTopic(String topic) {
        // Parsing will throw exception if name is not valid
        return TopicName.get(topic);
    }

    private List<Message<byte[]>> getMessageFromHttpResponse(Response response) throws Exception {

        if (response.getStatus() != Status.OK.getStatusCode()) {
            if (response.getStatus() >= 500) {
                throw new ServerErrorException(response);
            } else if (response.getStatus() >= 400) {
                throw new ClientErrorException(response);
            } else {
                throw new WebApplicationException(response);
            }
        }

        String msgId = response.getHeaderString("X-Pulsar-Message-ID");
        InputStream stream = null;
        try {
            stream = (InputStream) response.getEntity();
            byte[] data = new byte[stream.available()];
            stream.read(data);

            Map<String, String> properties = Maps.newTreeMap();
            MultivaluedMap<String, Object> headers = response.getHeaders();
            Object tmp = headers.getFirst("X-Pulsar-publish-time");
            if (tmp != null) {
                properties.put("publish-time", (String) tmp);
            }
            tmp =  headers.getFirst(BATCH_HEADER);
            if (response.getHeaderString(BATCH_HEADER) != null) {
                properties.put(BATCH_HEADER, (String)tmp);
                return getIndividualMsgsFromBatch(msgId, data, properties);
            }
            for (Entry<String, List<Object>> entry : headers.entrySet()) {
                String header = entry.getKey();
                if (header.contains("X-Pulsar-PROPERTY-")) {
                    String keyName = header.substring("X-Pulsar-PROPERTY-".length(), header.length());
                    properties.put(keyName, (String) entry.getValue().get(0));
                }
            }

            return Lists.newArrayList(new MessageImpl<byte[]>(msgId, properties, data, Schema.IDENTITY));
        } finally {
            if (stream != null) {
                stream.close();
            }
        }
    }

    private List<Message<byte[]>> getIndividualMsgsFromBatch(String msgId, byte[] data, Map<String, String> properties) {
        List<Message<byte[]>> ret = new ArrayList<>();
        int batchSize = Integer.parseInt(properties.get(BATCH_HEADER));
        for (int i = 0; i < batchSize; i++) {
            String batchMsgId = msgId + ":" + i;
            PulsarApi.SingleMessageMetadata.Builder singleMessageMetadataBuilder = PulsarApi.SingleMessageMetadata
                    .newBuilder();
            ByteBuf buf = Unpooled.wrappedBuffer(data);
            try {
                ByteBuf singleMessagePayload = Commands.deSerializeSingleMessageInBatch(buf, singleMessageMetadataBuilder, i,
                        batchSize);
                SingleMessageMetadata singleMessageMetadata = singleMessageMetadataBuilder.build();
                if (singleMessageMetadata.getPropertiesCount() > 0) {
                    for (KeyValue entry : singleMessageMetadata.getPropertiesList()) {
                        properties.put(entry.getKey(), entry.getValue());
                    }
                }
                ret.add(new MessageImpl<>(batchMsgId, properties, singleMessagePayload, Schema.IDENTITY));
            } catch (Exception ex) {
                log.error("Exception occured while trying to get BatchMsgId: {}", batchMsgId, ex);
            }
            buf.release();
            singleMessageMetadataBuilder.recycle();
        }
        return ret;
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentTopicsImpl.class);
}
