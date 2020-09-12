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
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyValue;
import org.apache.pulsar.common.api.proto.PulsarApi.SingleMessageMetadata;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.PartitionedTopicInternalStats;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicsImpl extends BaseResource implements Topics {
    private final WebTarget adminTopics;
    private final WebTarget adminV2Topics;
    // CHECKSTYLE.OFF: MemberName
    private final String BATCH_HEADER = "X-Pulsar-num-batch-message";
    private final String MESSAGE_ID = "X-Pulsar-Message-ID";
    private final String PUBLISH_TIME = "X-Pulsar-publish-time";
    // CHECKSTYLE.ON: MemberName

    public TopicsImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        adminTopics = web.path("/admin");
        adminV2Topics = web.path("/admin/v2");
    }

    @Override
    public List<String> getList(String namespace) throws PulsarAdminException {
        try {
            return getListAsync(namespace).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<List<String>> getListAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget persistentPath = namespacePath("persistent", ns);
        WebTarget nonPersistentPath = namespacePath("non-persistent", ns);
        final CompletableFuture<List<String>> persistentList = new CompletableFuture<>();
        final CompletableFuture<List<String>> nonPersistentList = new CompletableFuture<>();
        asyncGetRequest(persistentPath,
                new InvocationCallback<List<String>>() {
                    @Override
                    public void completed(List<String> topics) {
                        persistentList.complete(topics);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        persistentList.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        asyncGetRequest(nonPersistentPath,
                new InvocationCallback<List<String>>() {
                    @Override
                    public void completed(List<String> a) {
                        nonPersistentList.complete(a);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        nonPersistentList.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });

        return persistentList.thenCombine(nonPersistentList,
                (l1, l2) -> new ArrayList<>(Stream.concat(l1.stream(), l2.stream()).collect(Collectors.toSet())));
    }

    @Override
    public List<String> getPartitionedTopicList(String namespace) throws PulsarAdminException {
        try {
            return getPartitionedTopicListAsync(namespace).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<List<String>> getPartitionedTopicListAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget persistentPath = namespacePath("persistent", ns, "partitioned");
        WebTarget nonPersistentPath = namespacePath("non-persistent", ns, "partitioned");
        final CompletableFuture<List<String>> persistentList = new CompletableFuture<>();
        final CompletableFuture<List<String>> nonPersistentList = new CompletableFuture<>();
        asyncGetRequest(persistentPath,
                new InvocationCallback<List<String>>() {
                    @Override
                    public void completed(List<String> topics) {
                        persistentList.complete(topics);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        persistentList.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        asyncGetRequest(nonPersistentPath,
                new InvocationCallback<List<String>>() {
                    @Override
                    public void completed(List<String> topics) {
                        nonPersistentList.complete(topics);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        nonPersistentList.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });

        return persistentList.thenCombine(nonPersistentList,
                (l1, l2) -> new ArrayList<>(Stream.concat(l1.stream(), l2.stream()).collect(Collectors.toSet())));
    }


    @Override
    public List<String> getListInBundle(String namespace, String bundleRange) throws PulsarAdminException {
        try {
            return getListInBundleAsync(namespace, bundleRange).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
            return getPermissionsAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Map<String, Set<AuthAction>>> getPermissionsAsync(String topic) {
        TopicName tn = TopicName.get(topic);
        WebTarget path = topicPath(tn, "permissions");
        final CompletableFuture<Map<String, Set<AuthAction>>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Map<String, Set<AuthAction>>>() {
                    @Override
                    public void completed(Map<String, Set<AuthAction>> permissions) {
                        future.complete(permissions);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void grantPermission(String topic, String role, Set<AuthAction> actions) throws PulsarAdminException {
        try {
            grantPermissionAsync(topic, role, actions).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> grantPermissionAsync(String topic, String role, Set<AuthAction> actions) {
        TopicName tn = TopicName.get(topic);
        WebTarget path = topicPath(tn, "permissions", role);
        return asyncPostRequest(path, Entity.entity(actions, MediaType.APPLICATION_JSON));
    }

    @Override
    public void revokePermissions(String topic, String role) throws PulsarAdminException {
        try {
            revokePermissionsAsync(topic, role).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> revokePermissionsAsync(String topic, String role) {
        TopicName tn = TopicName.get(topic);
        WebTarget path = topicPath(tn, "permissions", role);
        return asyncDeleteRequest(path);
    }

    @Override
    public void createPartitionedTopic(String topic, int numPartitions) throws PulsarAdminException {
        try {
            createPartitionedTopicAsync(topic, numPartitions).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void createNonPartitionedTopic(String topic) throws PulsarAdminException {
        try {
            createNonPartitionedTopicAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void createMissedPartitions(String topic) throws PulsarAdminException {
        try {
            createMissedPartitionsAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> createNonPartitionedTopicAsync(String topic){
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn);
        return asyncPutRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public CompletableFuture<Void> createPartitionedTopicAsync(String topic, int numPartitions) {
        checkArgument(numPartitions > 0, "Number of partitions should be more than 0");
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "partitions");
        return asyncPutRequest(path, Entity.entity(numPartitions, MediaType.APPLICATION_JSON));
    }

    @Override
    public CompletableFuture<Void> createMissedPartitionsAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "createMissedPartitions");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void updatePartitionedTopic(String topic, int numPartitions)
            throws PulsarAdminException {
        try {
            updatePartitionedTopicAsync(topic, numPartitions).get(this.readTimeoutMs,
                    TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> updatePartitionedTopicAsync(String topic, int numPartitions) {
        return updatePartitionedTopicAsync(topic, numPartitions, false);
    }

    @Override
    public void updatePartitionedTopic(String topic, int numPartitions, boolean updateLocalTopicOnly)
            throws PulsarAdminException {
        try {
            updatePartitionedTopicAsync(topic, numPartitions, updateLocalTopicOnly)
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
    public CompletableFuture<Void> updatePartitionedTopicAsync(String topic, int numPartitions,
            boolean updateLocalTopicOnly) {
        checkArgument(numPartitions > 0, "Number of partitions must be more than 0");
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "partitions");
        path = path.queryParam("updateLocalTopicOnly", Boolean.toString(updateLocalTopicOnly));
        return asyncPostRequest(path, Entity.entity(numPartitions, MediaType.APPLICATION_JSON));
    }

    @Override
    public PartitionedTopicMetadata getPartitionedTopicMetadata(String topic) throws PulsarAdminException {
        try {
            return getPartitionedTopicMetadataAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> deletePartitionedTopicAsync(String topic) {
        return deletePartitionedTopicAsync(topic, false);
    }

    @Override
    public void deletePartitionedTopic(String topic, boolean force) throws PulsarAdminException {
        try {
            deletePartitionedTopicAsync(topic, force).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> deleteAsync(String topic) {
        return deleteAsync(topic, false);
    }

    @Override
    public void delete(String topic, boolean force) throws PulsarAdminException {
        try {
            deleteAsync(topic, force).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> deleteAsync(String topic, boolean force) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn);
        path = path.queryParam("force", Boolean.toString(force));
        return asyncDeleteRequest(path);
    }

    @Override
    public void unload(String topic) throws PulsarAdminException {
        try {
            unloadAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> unloadAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "unload");
        return asyncPutRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public MessageId terminateTopic(String topic) throws PulsarAdminException {
        try {
            return terminateTopicAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public List<String> getSubscriptions(String topic) throws PulsarAdminException {
        try {
            return getSubscriptionsAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public TopicStats getStats(String topic, boolean getPreciseBacklog) throws PulsarAdminException {
        try {
            return getStatsAsync(topic, getPreciseBacklog).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<TopicStats> getStatsAsync(String topic, boolean getPreciseBacklog) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "stats").queryParam("getPreciseBacklog", getPreciseBacklog);
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
    public PartitionedTopicStats getPartitionedStats(String topic, boolean perPartition, boolean getPreciseBacklog)
            throws PulsarAdminException {
        try {
            return getPartitionedStatsAsync(topic, perPartition, getPreciseBacklog)
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
    public CompletableFuture<PartitionedTopicStats> getPartitionedStatsAsync(String topic,
            boolean perPartition, boolean getPreciseBacklog) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "partitioned-stats");
        path = path.queryParam("perPartition", perPartition).queryParam("getPreciseBacklog", getPreciseBacklog);
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
    public void deleteSubscription(String topic, String subName, boolean force) throws PulsarAdminException {
        try {
            deleteSubscriptionAsync(topic, subName, force).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
        return deleteSubscriptionAsync(topic, subName, false);
    }

    @Override
    public CompletableFuture<Void> deleteSubscriptionAsync(String topic, String subName, boolean force) {
        TopicName tn = validateTopic(topic);
        String encodedSubName = Codec.encode(subName);
        WebTarget path = topicPath(tn, "subscription", encodedSubName);
        path = path.queryParam("force", force);
        return asyncDeleteRequest(path);
    }

    @Override
    public void skipAllMessages(String topic, String subName) throws PulsarAdminException {
        try {
            skipAllMessagesAsync(topic, subName).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
            expireMessagesForAllSubscriptionsAsync(topic, expireTimeInSeconds)
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
                            future.complete(getMessagesFromHttpResponse(tn.toString(), response));
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
                    log.warn("Exception '{}' occurred while trying to peek Messages.", ex.getMessage());
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
    public CompletableFuture<Message<byte[]>> getMessageByIdAsync(String topic, long ledgerId, long entryId) {
        CompletableFuture<Message<byte[]>> future = new CompletableFuture<>();
        getRemoteMessageById(topic, ledgerId, entryId).handle((r, ex) -> {
            if (ex != null) {
                if (ex instanceof NotFoundException) {
                    log.warn("Exception '{}' occurred while trying to get message.", ex.getMessage());
                    future.complete(r);
                } else {
                    future.completeExceptionally(ex);
                }
                return null;
            }
            future.complete(r);
            return null;
        });
        return future;
    }

    private CompletableFuture<Message<byte[]>> getRemoteMessageById(String topic, long ledgerId, long entryId) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "ledger", Long.toString(ledgerId), "entry", Long.toString(entryId));
        final CompletableFuture<Message<byte[]>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        try {
                            future.complete(getMessagesFromHttpResponse(topicName.toString(), response).get(0));
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
    public Message<byte[]> getMessageById(String topic, long ledgerId, long entryId)
            throws PulsarAdminException {
        try {
            return getMessageByIdAsync(topic, ledgerId, entryId).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void triggerCompaction(String topic) throws PulsarAdminException {
        try {
            triggerCompactionAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> triggerCompactionAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "compaction");
        return asyncPutRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public LongRunningProcessStatus compactionStatus(String topic)
            throws PulsarAdminException {
        try {
            return compactionStatusAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<LongRunningProcessStatus> compactionStatusAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "compaction");
        final CompletableFuture<LongRunningProcessStatus> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<LongRunningProcessStatus>() {
                    @Override
                    public void completed(LongRunningProcessStatus longRunningProcessStatus) {
                        future.complete(longRunningProcessStatus);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void triggerOffload(String topic, MessageId messageId) throws PulsarAdminException {
        try {
            triggerOffloadAsync(topic, messageId).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> triggerOffloadAsync(String topic, MessageId messageId) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "offload");
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            request(path).async().put(Entity.entity(messageId, MediaType.APPLICATION_JSON)
                    , new InvocationCallback<MessageIdImpl>() {
                        @Override
                        public void completed(MessageIdImpl response) {
                            future.complete(null);
                        }

                        @Override
                        public void failed(Throwable throwable) {
                            future.completeExceptionally(getApiException(throwable.getCause()));
                        }
                    });
        } catch (PulsarAdminException cae) {
            future.completeExceptionally(cae);
        }
        return future;
    }

    @Override
    public OffloadProcessStatus offloadStatus(String topic)
            throws PulsarAdminException {
        try {
            return offloadStatusAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<OffloadProcessStatus> offloadStatusAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "offload");
        final CompletableFuture<OffloadProcessStatus> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<OffloadProcessStatus>() {
                    @Override
                    public void completed(OffloadProcessStatus offloadProcessStatus) {
                        future.complete(offloadProcessStatus);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
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

    private List<Message<byte[]>> getMessagesFromHttpResponse(String topic, Response response) throws Exception {

        if (response.getStatus() != Status.OK.getStatusCode()) {
            throw getApiException(response);
        }

        String msgId = response.getHeaderString(MESSAGE_ID);
        PulsarApi.MessageMetadata.Builder messageMetadata = PulsarApi.MessageMetadata.newBuilder();
        try (InputStream stream = (InputStream) response.getEntity()) {
            byte[] data = new byte[stream.available()];
            stream.read(data);

            Map<String, String> properties = Maps.newTreeMap();
            MultivaluedMap<String, Object> headers = response.getHeaders();
            Object tmp = headers.getFirst(PUBLISH_TIME);
            if (tmp != null) {
                properties.put("publish-time", (String) tmp);
            }

            tmp = headers.getFirst("X-Pulsar-null-value");
            if (tmp != null) {
                messageMetadata.setNullValue(Boolean.parseBoolean(tmp.toString()));
            }

            tmp = headers.getFirst(BATCH_HEADER);
            if (response.getHeaderString(BATCH_HEADER) != null) {
                properties.put(BATCH_HEADER, (String) tmp);
                return getIndividualMsgsFromBatch(topic, msgId, data, properties, messageMetadata);
            }
            for (Entry<String, List<Object>> entry : headers.entrySet()) {
                String header = entry.getKey();
                if (header.contains("X-Pulsar-PROPERTY-")) {
                    String keyName = header.substring("X-Pulsar-PROPERTY-".length());
                    properties.put(keyName, (String) entry.getValue().get(0));
                }
            }

            return Collections.singletonList(new MessageImpl<byte[]>(topic, msgId, properties,
                    Unpooled.wrappedBuffer(data), Schema.BYTES, messageMetadata));
        }
    }

    private List<Message<byte[]>> getIndividualMsgsFromBatch(String topic, String msgId, byte[] data,
                                 Map<String, String> properties, PulsarApi.MessageMetadata.Builder msgMetadataBuilder) {
        List<Message<byte[]>> ret = new ArrayList<>();
        int batchSize = Integer.parseInt(properties.get(BATCH_HEADER));
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        for (int i = 0; i < batchSize; i++) {
            String batchMsgId = msgId + ":" + i;
            PulsarApi.SingleMessageMetadata.Builder singleMessageMetadataBuilder = PulsarApi.SingleMessageMetadata
                    .newBuilder();
            try {
                ByteBuf singleMessagePayload =
                        Commands.deSerializeSingleMessageInBatch(buf, singleMessageMetadataBuilder, i, batchSize);
                SingleMessageMetadata singleMessageMetadata = singleMessageMetadataBuilder.build();
                if (singleMessageMetadata.getPropertiesCount() > 0) {
                    for (KeyValue entry : singleMessageMetadata.getPropertiesList()) {
                        properties.put(entry.getKey(), entry.getValue());
                    }
                }
                ret.add(new MessageImpl<>(topic, batchMsgId, properties, singleMessagePayload,
                        Schema.BYTES, msgMetadataBuilder));
            } catch (Exception ex) {
                log.error("Exception occurred while trying to get BatchMsgId: {}", batchMsgId, ex);
            }
            singleMessageMetadataBuilder.recycle();
        }
        buf.release();
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

    @Override
    public CompletableFuture<MessageId> getLastMessageIdAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "lastMessageId");
        final CompletableFuture<MessageId> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<BatchMessageIdImpl>() {

                    @Override
                    public void completed(BatchMessageIdImpl response) {
                        if (response.getBatchIndex() == -1) {
                            future.complete(new MessageIdImpl(response.getLedgerId(),
                                    response.getEntryId(), response.getPartitionIndex()));
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
    public Map<BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(String topic) throws PulsarAdminException {
        try {
            TopicName tn = validateTopic(topic);
            WebTarget path = topicPath(tn, "backlogQuotaMap");
            return request(path).get(new GenericType<Map<BacklogQuotaType, BacklogQuota>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setBacklogQuota(String topic, BacklogQuota backlogQuota) throws PulsarAdminException {
        try {
            TopicName tn = validateTopic(topic);
            WebTarget path = topicPath(tn, "backlogQuota");
            request(path).post(Entity.entity(backlogQuota, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void removeBacklogQuota(String topic) throws PulsarAdminException {
        try {
            TopicName tn = validateTopic(topic);
            WebTarget path = topicPath(tn, "backlogQuota");
            request(path.queryParam("backlogQuotaType", BacklogQuotaType.destination_storage.toString()))
                    .delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public Integer getMaxUnackedMessagesOnConsumer(String topic) throws PulsarAdminException {
        try {
            return getMaxUnackedMessagesOnConsumerAsync(topic).
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
    public CompletableFuture<Integer> getMaxUnackedMessagesOnConsumerAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "maxUnackedMessagesOnConsumer");
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
    public InactiveTopicPolicies getInactiveTopicPolicies(String topic) throws PulsarAdminException {
        try {
            return getInactiveTopicPoliciesAsync(topic).
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
    public CompletableFuture<InactiveTopicPolicies> getInactiveTopicPoliciesAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "inactiveTopicPolicies");
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
    public DelayedDeliveryPolicies getDelayedDeliveryPolicy(String topic) throws PulsarAdminException {
        try {
            return getDelayedDeliveryPolicyAsync(topic).
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
    public CompletableFuture<DelayedDeliveryPolicies> getDelayedDeliveryPolicyAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "delayedDelivery");
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
    public Boolean getDeduplicationEnabled(String topic) throws PulsarAdminException {
        try {
            return getDeduplicationEnabledAsync(topic).
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
    public CompletableFuture<Boolean> getDeduplicationEnabledAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "deduplicationEnabled");
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

    @Override
    public void enableDeduplication(String topic, boolean enabled) throws PulsarAdminException {
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
    public CompletableFuture<Void> enableDeduplicationAsync(String topic, boolean enabled) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "deduplicationEnabled");
        return asyncPostRequest(path, Entity.entity(enabled, MediaType.APPLICATION_JSON));
    }

    @Override
    public void disableDeduplication(String topic) throws PulsarAdminException {
        try {
            disableDeduplicationAsync(topic).
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
    public CompletableFuture<Void> disableDeduplicationAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "deduplicationEnabled");
        return asyncDeleteRequest(path);
    }

    @Override
    public OffloadPolicies getOffloadPolicies(String topic) throws PulsarAdminException {
        try {
            return getOffloadPoliciesAsync(topic).
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
    public CompletableFuture<OffloadPolicies> getOffloadPoliciesAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "offloadPolicies");
        final CompletableFuture<OffloadPolicies> future = new CompletableFuture<>();
        asyncGetRequest(path, new InvocationCallback<OffloadPolicies>() {
            @Override
            public void completed(OffloadPolicies offloadPolicies) {
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
        return asyncPostRequest(path, Entity.entity(offloadPolicies, MediaType.APPLICATION_JSON));
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
        try {
            return getMaxUnackedMessagesOnSubscriptionAsync(topic).
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
    public CompletableFuture<Integer> getMaxUnackedMessagesOnSubscriptionAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "maxUnackedMessagesOnSubscription");
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
    public int getMessageTTL(String topic) throws PulsarAdminException {
        try {
            TopicName topicName = validateTopic(topic);
            WebTarget path = topicPath(topicName, "messageTTL");
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
        try {
            return getRetentionAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<RetentionPolicies> getRetentionAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "retention");
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
        try {
            return getPersistenceAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<PersistencePolicies> getPersistenceAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "persistence");
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
    public DispatchRate getDispatchRate(String topic) throws PulsarAdminException {
        try {
            return getDispatchRateAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<DispatchRate> getDispatchRateAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "dispatchRate");
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
    public Long getCompactionThreshold(String topic) throws PulsarAdminException {
        try {
            return getCompactionThresholdAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Long> getCompactionThresholdAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "compactionThreshold");
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
        try {
            return getMaxProducersAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Integer> getMaxProducersAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxProducers");
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
    public Integer getMaxConsumers(String topic) throws PulsarAdminException {
        try {
            return getMaxConsumersAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Integer> getMaxConsumersAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "maxConsumers");
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

    private static final Logger log = LoggerFactory.getLogger(TopicsImpl.class);
}
