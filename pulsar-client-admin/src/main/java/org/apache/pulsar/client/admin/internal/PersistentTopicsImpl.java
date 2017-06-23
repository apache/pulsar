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
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.naming.DestinationName;
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

public class PersistentTopicsImpl extends BaseResource implements PersistentTopics {
    private final WebTarget persistentTopics;

    public PersistentTopicsImpl(WebTarget web, Authentication auth) {
        super(auth);
        this.persistentTopics = web.path("/persistent");
    }


    @Override
    public List<String> getList(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
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
            NamespaceName ns = new NamespaceName(namespace);
            return request(persistentTopics.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path("partitioned")).get(
                    new GenericType<List<String>>() {
                    });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public Map<String, Set<AuthAction>> getPermissions(String destination) throws PulsarAdminException {
        try {
            DestinationName ds = DestinationName.get(destination);
            return request(persistentTopics.path(ds.getNamespace()).path(ds.getLocalName()).path("permissions")).get(
                    new GenericType<Map<String, Set<AuthAction>>>() {
                    });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void grantPermission(String destination, String role, Set<AuthAction> actions) throws PulsarAdminException {
        try {
            DestinationName ds = DestinationName.get(destination);
            request(persistentTopics.path(ds.getNamespace()).path(ds.getLocalName()).path("permissions").path(role))
                    .post(Entity.entity(actions, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void revokePermissions(String destination, String role) throws PulsarAdminException {
        try {
            DestinationName ds = DestinationName.get(destination);
            request(persistentTopics.path(ds.getNamespace()).path(ds.getLocalName()).path("permissions").path(role))
                    .delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createPartitionedTopic(String destination, int numPartitions) throws PulsarAdminException {
        try {
            createPartitionedTopicAsync(destination, numPartitions).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Void> createPartitionedTopicAsync(String destination, int numPartitions) {
        checkArgument(numPartitions > 1, "Number of partitions should be more than 1");
        DestinationName ds = validateTopic(destination);
        return asyncPutRequest(
                persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("partitions"),
                Entity.entity(numPartitions, MediaType.APPLICATION_JSON));
    }

    @Override
    public void updatePartitionedTopic(String destination, int numPartitions) throws PulsarAdminException {
        try {
            updatePartitionedTopicAsync(destination, numPartitions).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Void> updatePartitionedTopicAsync(String destination, int numPartitions) {
        checkArgument(numPartitions > 1, "Number of partitions must be more than 1");
        DestinationName ds = validateTopic(destination);
        return asyncPostRequest(
                persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("partitions"),
                Entity.entity(numPartitions, MediaType.APPLICATION_JSON));
    }
    
    @Override
    public PartitionedTopicMetadata getPartitionedTopicMetadata(String destination) throws PulsarAdminException {
        try {
            return getPartitionedTopicMetadataAsync(destination).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadataAsync(String destination) {
        DestinationName ds = validateTopic(destination);
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
    public void deletePartitionedTopic(String destination) throws PulsarAdminException {
        try {
            deletePartitionedTopicAsync(destination).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Void> deletePartitionedTopicAsync(String destination) {
        DestinationName ds = validateTopic(destination);
        return asyncDeleteRequest(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName())
                .path("partitions"));
    }

    @Override
    public void delete(String destination) throws PulsarAdminException {
        try {
            deleteAsync(destination).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Void> deleteAsync(String destination) {
        DestinationName ds = validateTopic(destination);
        return asyncDeleteRequest(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()));
    }

    @Override
    public List<String> getSubscriptions(String destination) throws PulsarAdminException {
        try {
            return getSubscriptionsAsync(destination).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<List<String>> getSubscriptionsAsync(String destination) {
        DestinationName ds = validateTopic(destination);
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
    public PersistentTopicStats getStats(String destination) throws PulsarAdminException {
        try {
            return getStatsAsync(destination).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<PersistentTopicStats> getStatsAsync(String destination) {
        DestinationName ds = validateTopic(destination);
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
    public PersistentTopicInternalStats getInternalStats(String destination) throws PulsarAdminException {
        try {
            return getInternalStatsAsync(destination).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<PersistentTopicInternalStats> getInternalStatsAsync(String destination) {
        DestinationName ds = validateTopic(destination);
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
    public JsonObject getInternalInfo(String destination) throws PulsarAdminException {
        try {
            return getInternalInfoAsync(destination).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<JsonObject> getInternalInfoAsync(String destination) {
        DestinationName ds = validateTopic(destination);
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
    public PartitionedTopicStats getPartitionedStats(String destination, boolean perPartition)
            throws PulsarAdminException {
        try {
            return getPartitionedStatsAsync(destination, perPartition).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<PartitionedTopicStats> getPartitionedStatsAsync(String destination,
            boolean perPartition) {
        DestinationName ds = validateTopic(destination);
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
    public void deleteSubscription(String destination, String subName) throws PulsarAdminException {
        try {
            deleteSubscriptionAsync(destination, subName).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Void> deleteSubscriptionAsync(String destination, String subName) {
        DestinationName ds = validateTopic(destination);
        String encodedSubName = Codec.encode(subName);
        return asyncDeleteRequest(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName())
                .path("subscription").path(encodedSubName));
    }

    @Override
    public void skipAllMessages(String destination, String subName) throws PulsarAdminException {
        try {
            skipAllMessagesAsync(destination, subName).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Void> skipAllMessagesAsync(String destination, String subName) {
        DestinationName ds = validateTopic(destination);
        String encodedSubName = Codec.encode(subName);
        return asyncPostRequest(
                persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("subscription")
                        .path(encodedSubName).path("skip_all"), Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void skipMessages(String destination, String subName, long numMessages) throws PulsarAdminException {
        try {
            skipMessagesAsync(destination, subName, numMessages).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Void> skipMessagesAsync(String destination, String subName, long numMessages) {
        DestinationName ds = validateTopic(destination);
        String encodedSubName = Codec.encode(subName);
        return asyncPostRequest(
                persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("subscription")
                        .path(encodedSubName).path("skip").path(String.valueOf(numMessages)),
                Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void expireMessages(String destination, String subName, long expireTimeInSeconds) throws PulsarAdminException {
        try {
            expireMessagesAsync(destination, subName, expireTimeInSeconds).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Void> expireMessagesAsync(String destination, String subName, long expireTimeInSeconds) {
        DestinationName ds = validateTopic(destination);
        String encodedSubName = Codec.encode(subName);
        return asyncPostRequest(
                persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("subscription")
                        .path(encodedSubName).path("expireMessages").path(String.valueOf(expireTimeInSeconds)),
                Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void expireMessagesForAllSubscriptions(String destination, long expireTimeInSeconds) throws PulsarAdminException {
        try {
            expireMessagesForAllSubscriptionsAsync(destination, expireTimeInSeconds).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Void> expireMessagesForAllSubscriptionsAsync(String destination, long expireTimeInSeconds) {
        DestinationName ds = validateTopic(destination);
        return asyncPostRequest(
                persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("all_subscription")
                        .path("expireMessages").path(String.valueOf(expireTimeInSeconds)),
                Entity.entity("", MediaType.APPLICATION_JSON));
    }

    private CompletableFuture<Message> peekNthMessage(String destination, String subName, int messagePosition) {
        DestinationName ds = validateTopic(destination);
        String encodedSubName = Codec.encode(subName);
        final CompletableFuture<Message> future = new CompletableFuture<Message>();
        asyncGetRequest(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("subscription")
                .path(encodedSubName).path("position").path(String.valueOf(messagePosition)),
                new InvocationCallback<Response>() {

                    @Override
                    public void completed(Response response) {
                        try {
                            Message msg = getMessageFromHttpResponse(response);
                            future.complete(msg);
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
    public List<Message> peekMessages(String destination, String subName, int numMessages) throws PulsarAdminException {

        try {
            return peekMessagesAsync(destination, subName, numMessages).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<List<Message>> peekMessagesAsync(String destination, String subName, int numMessages) {
        checkArgument(numMessages > 0);
        List<Message> messages = Lists.newArrayList();
        CompletableFuture<List<Message>> futures = new CompletableFuture<List<Message>>();

        // if peeking first message succeeds, we know that the topic and subscription exists
        peekNthMessage(destination, subName, 1).handle((r, ex) -> {
            if (ex != null) {
                futures.completeExceptionally(ex);
            } else {
                messages.add(r);
                List<CompletableFuture<Message>> futureMessages = Lists.newArrayList();
                for (int i = 2; i <= numMessages; i++) {
                    futureMessages.add(peekNthMessage(destination, subName, i));
                }

                try {
                    for (CompletableFuture<Message> futureMessage : futureMessages) {
                        messages.add(futureMessage.get());
                    }
                } catch (Exception e) {
                    // if we get a not found exception, it means that the position for the message we are trying to get
                    // does not exist. At this point, we can return the already found messages.
                if (!(e.getCause() instanceof NotFoundException)) {
                    futures.completeExceptionally(e.getCause());
                    return null;
                }
            }

            futures.complete(messages);
        }
        return null;
    }   );

        return futures;
    }

    @Override
    public void resetCursor(String destination, String subName, long timestamp) throws PulsarAdminException {
        try {
            DestinationName ds = validateTopic(destination);
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
    public CompletableFuture<Void> resetCursorAsync(String destination, String subName, long timestamp) {
        DestinationName ds = validateTopic(destination);
        String encodedSubName = Codec.encode(subName);
        return asyncPostRequest(
                persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("subscription")
                        .path(encodedSubName).path("resetcursor").path(String.valueOf(timestamp)),
                Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public CompletableFuture<MessageId> terminateTopicAsync(String destination) {
        DestinationName ds = validateTopic(destination);

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
     * returns destination name with encoded Local Name
     */
    private DestinationName validateTopic(String destination) {
        // Parsing will throw exception if name is not valid
        return DestinationName.get(destination);
    }

    private Message getMessageFromHttpResponse(Response response) throws Exception {

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
            Object publishTime = headers.getFirst("X-Pulsar-publish-time");
            if (publishTime != null) {
                properties.put("publish-time", (String) publishTime);
            }
            for (Entry<String, List<Object>> entry : headers.entrySet()) {
                String header = entry.getKey();
                if (header.contains("X-Pulsar-PROPERTY-")) {
                    String keyName = header.substring(header.indexOf("X-Pulsar-PROPERTY-") + 1, header.length());
                    properties.put(keyName, (String) entry.getValue().get(0));
                }
            }

            return new MessageImpl(msgId, properties, data);
        } finally {
            if (stream != null) {
                stream.close();
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentTopicsImpl.class);
}
