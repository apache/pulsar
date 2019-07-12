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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.OffloadProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyValue;
import org.apache.pulsar.common.api.proto.PulsarApi.SingleMessageMetadata;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.PartitionedTopicInternalStats;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicsImpl extends BaseResource implements Topics {
    private final WebTarget adminTopics;
    private final WebTarget adminV2Topics;
    private final String BATCH_HEADER = "X-Pulsar-num-batch-message";
    public TopicsImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        adminTopics = web.path("/admin");
        adminV2Topics = web.path("/admin/v2");
    }

    @Override
    public List<String> getList(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget persistentPath = namespacePath("persistent", ns);
            WebTarget nonPersistentPath = namespacePath("non-persistent", ns);

            List<String> persistentTopics = request(persistentPath).get(new GenericType<List<String>>() {
            });

            List<String> nonPersistentTopics = request(nonPersistentPath).get(new GenericType<List<String>>() {
            });
            return new ArrayList<>(
                    Stream.concat(persistentTopics.stream(), nonPersistentTopics.stream()).collect(Collectors.toSet()));
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public List<String> getPartitionedTopicList(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);

            WebTarget persistentPath = namespacePath("persistent", ns, "partitioned");
            WebTarget nonPersistentPath = namespacePath("non-persistent", ns, "partitioned");
            List<String> persistentTopics = request(persistentPath).get(new GenericType<List<String>>() {
            });

            List<String> nonPersistentTopics = request(nonPersistentPath).get(new GenericType<List<String>>() {
            });
            return new ArrayList<>(
                    Stream.concat(persistentTopics.stream(), nonPersistentTopics.stream()).collect(Collectors.toSet()));
        } catch (Exception e) {
            throw getApiException(e);
        }
    }


    @Override
    public List<String> getListInBundle(String namespace, String bundleRange) throws PulsarAdminException {
        try {
            return getListInBundleAsync(namespace, bundleRange).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        }
    }

    @Override
    public CompletableFuture<List<String>> getListInBundleAsync(String namespace, String bundleRange) {
        NamespaceName ns = NamespaceName.get(namespace);
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        WebTarget path = namespacePath("non-persistent", ns, bundleRange);

        asyncGetRequest(path,
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
    public Map<String, Set<AuthAction>> getPermissions(String topic) throws PulsarAdminException {
        try {
            TopicName tn = TopicName.get(topic);
            WebTarget path = topicPath(tn, "permissions");
            return request(path).get(new GenericType<Map<String, Set<AuthAction>>>() {});
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void grantPermission(String topic, String role, Set<AuthAction> actions) throws PulsarAdminException {
        try {
            TopicName tn = TopicName.get(topic);
            WebTarget path = topicPath(tn, "permissions", role);
            request(path).post(Entity.entity(actions, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void revokePermissions(String topic, String role) throws PulsarAdminException {
        try {
            TopicName tn = TopicName.get(topic);
            WebTarget path = topicPath(tn, "permissions", role);
            request(path).delete(ErrorData.class);
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
            throw new PulsarAdminException(e);
        }
    }

    @Override
    public void createNonPartitionedTopic(String topic) throws PulsarAdminException {
    	try {
    		createNonPartitionedTopicAsync(topic).get();
    	} catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        }
    }
    
    @Override
    public CompletableFuture<Void> createNonPartitionedTopicAsync(String topic){
    	TopicName tn = validateTopic(topic);
    	WebTarget path = topicPath(tn);
    	return asyncPutRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }
    
    @Override
    public CompletableFuture<Void> createPartitionedTopicAsync(String topic, int numPartitions) {
        checkArgument(numPartitions > 1, "Number of partitions should be more than 1");
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "partitions");
        return asyncPutRequest(path, Entity.entity(numPartitions, MediaType.APPLICATION_JSON));
    }

    @Override
    public void updatePartitionedTopic(String topic, int numPartitions) throws PulsarAdminException {
        try {
            updatePartitionedTopicAsync(topic, numPartitions).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        }
    }

    @Override
    public CompletableFuture<Void> updatePartitionedTopicAsync(String topic, int numPartitions) {
        checkArgument(numPartitions > 1, "Number of partitions must be more than 1");
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "partitions");
        return asyncPostRequest(path, Entity.entity(numPartitions, MediaType.APPLICATION_JSON));
    }

    @Override
    public PartitionedTopicMetadata getPartitionedTopicMetadata(String topic) throws PulsarAdminException {
        try {
            return getPartitionedTopicMetadataAsync(topic).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        }
    }

    @Override
    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadataAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "partitions");
        final CompletableFuture<PartitionedTopicMetadata> future = new CompletableFuture<>();
        asyncGetRequest(path,
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
        deletePartitionedTopic(topic, false);
    }

    @Override
    public void deletePartitionedTopic(String topic, boolean force) throws PulsarAdminException {
        try {
            deletePartitionedTopicAsync(topic, force).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        }
    }

    @Override
    public CompletableFuture<Void> deletePartitionedTopicAsync(String topic, boolean force) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "partitions");
        path = path.queryParam("force", force);
        return asyncDeleteRequest(path);
    }

    @Override
    public void delete(String topic) throws PulsarAdminException {
        delete(topic, false);
    }

    @Override
    public void delete(String topic, boolean force) throws PulsarAdminException {
        try {
            deleteAsync(topic, force).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        }
    }

    @Override
    public CompletableFuture<Void> deleteAsync(String topic, boolean force) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn);
        path = path.queryParam("force", Boolean.toString(force));
        return asyncDeleteRequest(path);
    }

    @Override
    public void unload(String topic) throws PulsarAdminException {
        try {
            unloadAsync(topic).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        }
    }

    @Override
    public CompletableFuture<Void> unloadAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "unload");
        return asyncPutRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public List<String> getSubscriptions(String topic) throws PulsarAdminException {
        try {
            return getSubscriptionsAsync(topic).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        }
    }

    @Override
    public CompletableFuture<List<String>> getSubscriptionsAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "subscriptions");
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
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
    public TopicStats getStats(String topic) throws PulsarAdminException {
        try {
            return getStatsAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<TopicStats> getStatsAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "stats");
        final CompletableFuture<TopicStats> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<TopicStats>() {

                    @Override
                    public void completed(TopicStats response) {
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
            return getInternalStatsAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<PersistentTopicInternalStats> getInternalStatsAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "internalStats");
        final CompletableFuture<PersistentTopicInternalStats> future = new CompletableFuture<>();
        asyncGetRequest(path,
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
            return getInternalInfoAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<JsonObject> getInternalInfoAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "internal-info");
        final CompletableFuture<JsonObject> future = new CompletableFuture<>();
        asyncGetRequest(path,
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
            return getPartitionedStatsAsync(topic, perPartition).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<PartitionedTopicStats> getPartitionedStatsAsync(String topic,
            boolean perPartition) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "partitioned-stats");
        path = path.queryParam("perPartition", perPartition);
        final CompletableFuture<PartitionedTopicStats> future = new CompletableFuture<>();
        asyncGetRequest(path,
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
    public PartitionedTopicInternalStats getPartitionedInternalStats(String topic)
            throws PulsarAdminException {
        try {
            return getPartitionedInternalStatsAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<PartitionedTopicInternalStats> getPartitionedInternalStatsAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "partitioned-internalStats");
        final CompletableFuture<PartitionedTopicInternalStats> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<PartitionedTopicInternalStats>() {

                    @Override
                    public void completed(PartitionedTopicInternalStats response) {
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
            deleteSubscriptionAsync(topic, subName).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> deleteSubscriptionAsync(String topic, String subName) {
        TopicName tn = validateTopic(topic);
        String encodedSubName = Codec.encode(subName);
        WebTarget path = topicPath(tn, "subscription", encodedSubName);
        return asyncDeleteRequest(path);
    }

    @Override
    public void skipAllMessages(String topic, String subName) throws PulsarAdminException {
        try {
            skipAllMessagesAsync(topic, subName).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        }
    }

    @Override
    public CompletableFuture<Void> skipAllMessagesAsync(String topic, String subName) {
        TopicName tn = validateTopic(topic);
        String encodedSubName = Codec.encode(subName);
        WebTarget path = topicPath(tn, "subscription", encodedSubName, "skip_all");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void skipMessages(String topic, String subName, long numMessages) throws PulsarAdminException {
        try {
            skipMessagesAsync(topic, subName, numMessages).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> skipMessagesAsync(String topic, String subName, long numMessages) {
        TopicName tn = validateTopic(topic);
        String encodedSubName = Codec.encode(subName);
        WebTarget path = topicPath(tn, "subscription", encodedSubName, "skip", String.valueOf(numMessages));
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void expireMessages(String topic, String subName, long expireTimeInSeconds) throws PulsarAdminException {
        try {
            expireMessagesAsync(topic, subName, expireTimeInSeconds).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> expireMessagesAsync(String topic, String subName, long expireTimeInSeconds) {
        TopicName tn = validateTopic(topic);
        String encodedSubName = Codec.encode(subName);
        WebTarget path = topicPath(tn, "subscription", encodedSubName,
                "expireMessages", String.valueOf(expireTimeInSeconds));
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void expireMessagesForAllSubscriptions(String topic, long expireTimeInSeconds) throws PulsarAdminException {
        try {
            expireMessagesForAllSubscriptionsAsync(topic, expireTimeInSeconds).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> expireMessagesForAllSubscriptionsAsync(String topic, long expireTimeInSeconds) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "all_subscription",
                "expireMessages", String.valueOf(expireTimeInSeconds));
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    private CompletableFuture<List<Message<byte[]>>> peekNthMessage(String topic, String subName, int messagePosition) {
        TopicName tn = validateTopic(topic);
        String encodedSubName = Codec.encode(subName);
        WebTarget path = topicPath(tn, "subscription", encodedSubName,
                "position", String.valueOf(messagePosition));
        final CompletableFuture<List<Message<byte[]>>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {

                    @Override
                    public void completed(Response response) {
                        try {
                            future.complete(getMessageFromHttpResponse(tn.toString(), response));
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
            return peekMessagesAsync(topic, subName, numMessages).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
            TopicName tn = validateTopic(topic);
            String encodedSubName = Codec.encode(subscriptionName);
            WebTarget path = topicPath(tn, "subscription", encodedSubName);
            request(path).put(Entity.entity(messageId, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public CompletableFuture<Void> createSubscriptionAsync(String topic, String subscriptionName,
            MessageId messageId) {
        TopicName tn = validateTopic(topic);
        String encodedSubName = Codec.encode(subscriptionName);
        WebTarget path = topicPath(tn, "subscription", encodedSubName);
        return asyncPutRequest(path, Entity.entity(messageId, MediaType.APPLICATION_JSON));
    }

    @Override
    public void resetCursor(String topic, String subName, long timestamp) throws PulsarAdminException {
        try {
            TopicName tn = validateTopic(topic);
            String encodedSubName = Codec.encode(subName);
            WebTarget path = topicPath(tn, "subscription", encodedSubName,
                    "resetcursor", String.valueOf(timestamp));
            request(path).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public CompletableFuture<Void> resetCursorAsync(String topic, String subName, long timestamp) {
        TopicName tn = validateTopic(topic);
        String encodedSubName = Codec.encode(subName);
        WebTarget path = topicPath(tn, "subscription", encodedSubName,
                "resetcursor", String.valueOf(timestamp));
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void resetCursor(String topic, String subName, MessageId messageId) throws PulsarAdminException {
        try {
            TopicName tn = validateTopic(topic);
            String encodedSubName = Codec.encode(subName);
            WebTarget path = topicPath(tn, "subscription", encodedSubName, "resetcursor");
            request(path).post(Entity.entity(messageId, MediaType.APPLICATION_JSON),
                            ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public CompletableFuture<Void> resetCursorAsync(String topic, String subName, MessageId messageId) {
        TopicName tn = validateTopic(topic);
        String encodedSubName = Codec.encode(subName);
        final WebTarget path = topicPath(tn, "subscription", encodedSubName, "resetcursor");
        return asyncPostRequest(path, Entity.entity(messageId, MediaType.APPLICATION_JSON));
    }

    @Override
    public CompletableFuture<MessageId> terminateTopicAsync(String topic) {
        TopicName tn = validateTopic(topic);

        final CompletableFuture<MessageId> future = new CompletableFuture<>();
        try {
            final WebTarget path = topicPath(tn, "terminate");

            request(path).async().post(Entity.entity("", MediaType.APPLICATION_JSON),
                    new InvocationCallback<MessageIdImpl>() {

                        @Override
                        public void completed(MessageIdImpl messageId) {
                            future.complete(messageId);
                        }

                        @Override
                        public void failed(Throwable throwable) {
                            log.warn("[{}] Failed to perform http post request: {}", path.getUri(),
                                    throwable.getMessage());
                            future.completeExceptionally(getApiException(throwable.getCause()));
                        }
                    });
        } catch (PulsarAdminException cae) {
            future.completeExceptionally(cae);
        }

        return future;
    }

    @Override
    public void triggerCompaction(String topic)
            throws PulsarAdminException {
        try {
            TopicName tn = validateTopic(topic);
            request(topicPath(tn, "compaction"))
                .put(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public LongRunningProcessStatus compactionStatus(String topic)
            throws PulsarAdminException {
        try {
            TopicName tn = validateTopic(topic);
            return request(topicPath(tn, "compaction"))
                .get(LongRunningProcessStatus.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void triggerOffload(String topic, MessageId messageId) throws PulsarAdminException {
        try {
            TopicName tn = validateTopic(topic);
            WebTarget path = topicPath(tn, "offload");
            request(path).put(Entity.entity(messageId, MediaType.APPLICATION_JSON), MessageIdImpl.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public OffloadProcessStatus offloadStatus(String topic)
            throws PulsarAdminException {
        try {
            TopicName tn = validateTopic(topic);
            return request(topicPath(tn, "offload"))
                .get(OffloadProcessStatus.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    private WebTarget namespacePath(String domain, NamespaceName namespace, String... parts) {
        final WebTarget base = namespace.isV2() ? adminV2Topics : adminTopics;
        WebTarget namespacePath = base.path(domain).path(namespace.toString());
        namespacePath = WebTargets.addParts(namespacePath, parts);
        return namespacePath;
    }

    private WebTarget topicPath(TopicName topic, String... parts) {
        final WebTarget base = topic.isV2() ? adminV2Topics : adminTopics;
        WebTarget topicPath = base.path(topic.getRestPath());
        topicPath = WebTargets.addParts(topicPath, parts);
        return topicPath;
    }

    /*
     * returns topic name with encoded Local Name
     */
    private TopicName validateTopic(String topic) {
        // Parsing will throw exception if name is not valid
        return TopicName.get(topic);
    }

    private List<Message<byte[]>> getMessageFromHttpResponse(String topic, Response response) throws Exception {

        if (response.getStatus() != Status.OK.getStatusCode()) {
            throw getApiException(response);
        }

        String msgId = response.getHeaderString("X-Pulsar-Message-ID");
        try (InputStream stream = (InputStream) response.getEntity()) {
            byte[] data = new byte[stream.available()];
            stream.read(data);

            Map<String, String> properties = Maps.newTreeMap();
            MultivaluedMap<String, Object> headers = response.getHeaders();
            Object tmp = headers.getFirst("X-Pulsar-publish-time");
            if (tmp != null) {
                properties.put("publish-time", (String) tmp);
            }
            tmp = headers.getFirst(BATCH_HEADER);
            if (response.getHeaderString(BATCH_HEADER) != null) {
                properties.put(BATCH_HEADER, (String) tmp);
                return getIndividualMsgsFromBatch(topic, msgId, data, properties);
            }
            for (Entry<String, List<Object>> entry : headers.entrySet()) {
                String header = entry.getKey();
                if (header.contains("X-Pulsar-PROPERTY-")) {
                    String keyName = header.substring("X-Pulsar-PROPERTY-".length(), header.length());
                    properties.put(keyName, (String) entry.getValue().get(0));
                }
            }

            return Collections.singletonList(new MessageImpl<byte[]>(topic, msgId, properties,
                    Unpooled.wrappedBuffer(data), Schema.BYTES));
        }
    }

    private List<Message<byte[]>> getIndividualMsgsFromBatch(String topic, String msgId, byte[] data,
                                                             Map<String, String> properties) {
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
                ret.add(new MessageImpl<>(topic, batchMsgId, properties, singleMessagePayload, Schema.BYTES));
            } catch (Exception ex) {
                log.error("Exception occured while trying to get BatchMsgId: {}", batchMsgId, ex);
            }
            buf.release();
            singleMessageMetadataBuilder.recycle();
        }
        return ret;
    }

    @Override
    public MessageId getLastMessageId(String topic) throws PulsarAdminException {
        try {
            return getLastMessageIdAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    public CompletableFuture<MessageId> getLastMessageIdAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "lastMessageId");
        final CompletableFuture<MessageId> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<MessageIdImpl>() {

                    @Override
                    public void completed(MessageIdImpl response) {
                        future.complete(response);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    private static final Logger log = LoggerFactory.getLogger(TopicsImpl.class);
}
