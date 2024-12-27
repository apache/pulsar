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

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
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
import org.apache.pulsar.client.admin.GetStatsOptions;
import org.apache.pulsar.client.admin.ListTopicsOptions;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.OffloadProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TransactionIsolationLevel;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.ResetCursorData;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.apache.pulsar.common.api.proto.EncryptionKeys;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.NonPersistentPartitionedTopicStats;
import org.apache.pulsar.common.policies.data.NonPersistentTopicStats;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.PartitionedTopicInternalStats;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.stats.AnalyzeSubscriptionBacklogResult;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.DateFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicsImpl extends BaseResource implements Topics {
    private final WebTarget adminTopics;
    private final WebTarget adminV2Topics;
    // CHECKSTYLE.OFF: MemberName
    private static final String BATCH_HEADER = "X-Pulsar-num-batch-message";
    private static final String BATCH_SIZE_HEADER = "X-Pulsar-batch-size";
    private static final String MESSAGE_ID = "X-Pulsar-Message-ID";
    private static final String PUBLISH_TIME = "X-Pulsar-publish-time";
    private static final String EVENT_TIME = "X-Pulsar-event-time";
    private static final String DELIVER_AT_TIME = "X-Pulsar-deliver-at-time";
    private static final String BROKER_ENTRY_TIMESTAMP = "X-Pulsar-Broker-Entry-METADATA-timestamp";
    private static final String BROKER_ENTRY_INDEX = "X-Pulsar-Broker-Entry-METADATA-index";
    private static final String PRODUCER_NAME = "X-Pulsar-producer-name";
    private static final String SEQUENCE_ID = "X-Pulsar-sequence-id";
    private static final String REPLICATED_FROM = "X-Pulsar-replicated-from";
    private static final String PARTITION_KEY = "X-Pulsar-partition-key";
    private static final String COMPRESSION = "X-Pulsar-compression";
    private static final String UNCOMPRESSED_SIZE = "X-Pulsar-uncompressed-size";
    private static final String ENCRYPTION_ALGO = "X-Pulsar-encryption-algo";
    private static final String MARKER_TYPE = "X-Pulsar-marker-type";
    private static final String TXNID_LEAST_BITS = "X-Pulsar-txnid-least-bits";
    private static final String TXNID_MOST_BITS = "X-Pulsar-txnid-most-bits";
    private static final String HIGHEST_SEQUENCE_ID = "X-Pulsar-highest-sequence-id";
    private static final String UUID = "X-Pulsar-uuid";
    private static final String NUM_CHUNKS_FROM_MSG = "X-Pulsar-num-chunks-from-msg";
    private static final String TOTAL_CHUNK_MSG_SIZE = "X-Pulsar-total-chunk-msg-size";
    private static final String CHUNK_ID = "X-Pulsar-chunk-id";
    private static final String PARTITION_KEY_B64_ENCODED = "X-Pulsar-partition-key-b64-encoded";
    private static final String NULL_PARTITION_KEY = "X-Pulsar-null-partition-key";
    private static final String REPLICATED_TO = "X-Pulsar-replicated-to";
    private static final String ORDERING_KEY = "X-Pulsar-Base64-ordering-key";
    private static final String SCHEMA_VERSION = "X-Pulsar-Base64-schema-version-b64encoded";
    private static final String ENCRYPTION_PARAM = "X-Pulsar-Base64-encryption-param";
    private static final String ENCRYPTION_KEYS = "X-Pulsar-Base64-encryption-keys";
    public static final String TXN_ABORTED = "X-Pulsar-txn-aborted";
    public static final String TXN_UNCOMMITTED = "X-Pulsar-txn-uncommitted";
    // CHECKSTYLE.ON: MemberName

    public static final String PROPERTY_SHADOW_SOURCE_KEY = "PULSAR.SHADOW_SOURCE";

    public TopicsImpl(WebTarget web, Authentication auth, long requestTimeoutMs) {
        super(auth, requestTimeoutMs);
        adminTopics = web.path("/admin");
        adminV2Topics = web.path("/admin/v2");
    }

    @Override
    public List<String> getList(String namespace) throws PulsarAdminException {
        return getList(namespace, null);
    }

    @Override
    public List<String> getList(String namespace, TopicDomain topicDomain) throws PulsarAdminException {
        return getList(namespace, topicDomain, ListTopicsOptions.EMPTY);
    }

    @Override
    public List<String> getList(String namespace, TopicDomain topicDomain, Map<QueryParam, Object> params)
            throws PulsarAdminException {
        ListTopicsOptions options = ListTopicsOptions
                .builder()
                .bundle((String) params.get(QueryParam.Bundle))
                .build();
        return getList(namespace, topicDomain, options);
    }

    @Override
    public List<String> getList(String namespace, TopicDomain topicDomain, ListTopicsOptions options)
            throws PulsarAdminException {
        return sync(() -> getListAsync(namespace, topicDomain, options));
    }

    @Override
    public CompletableFuture<List<String>> getListAsync(String namespace) {
        return getListAsync(namespace, null);
    }

    @Override
    public CompletableFuture<List<String>> getListAsync(String namespace, TopicDomain topicDomain) {
        return getListAsync(namespace, topicDomain, ListTopicsOptions.EMPTY);
    }

    @Override
    public CompletableFuture<List<String>> getListAsync(String namespace, TopicDomain topicDomain,
                                                        Map<QueryParam, Object> params) {
        ListTopicsOptions options = ListTopicsOptions
                .builder()
                .bundle((String) params.get(QueryParam.Bundle.value))
                .build();
        return getListAsync(namespace, topicDomain, options);
    }

    @Override
    public CompletableFuture<List<String>> getListAsync(String namespace, TopicDomain topicDomain,
                                                        ListTopicsOptions options) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget persistentPath = namespacePath("persistent", ns);
        WebTarget nonPersistentPath = namespacePath("non-persistent", ns);
        persistentPath = persistentPath
                .queryParam("bundle", options.getBundle())
                .queryParam("includeSystemTopic", options.isIncludeSystemTopic());
        nonPersistentPath = nonPersistentPath
                .queryParam("bundle", options.getBundle())
                .queryParam("includeSystemTopic", options.isIncludeSystemTopic());
        final CompletableFuture<List<String>> persistentList;
        final CompletableFuture<List<String>> nonPersistentList;
        if (topicDomain == null || TopicDomain.persistent.equals(topicDomain)) {
            persistentList = asyncGetRequest(persistentPath, new FutureCallback<List<String>>() {});
        } else {
            persistentList = CompletableFuture.completedFuture(Collections.emptyList());
        }

        if (topicDomain == null || TopicDomain.non_persistent.equals(topicDomain)) {
            nonPersistentList = asyncGetRequest(nonPersistentPath, new FutureCallback<List<String>>() {});
        } else {
            nonPersistentList = CompletableFuture.completedFuture(Collections.emptyList());
        }

        return persistentList.thenCombine(nonPersistentList,
                (l1, l2) -> new ArrayList<>(Stream.concat(l1.stream(), l2.stream()).collect(Collectors.toSet())));
    }

    @Override
    public List<String> getPartitionedTopicList(String namespace) throws PulsarAdminException {
        return getPartitionedTopicList(namespace, ListTopicsOptions.EMPTY);
    }

    @Override
    public List<String> getPartitionedTopicList(String namespace, ListTopicsOptions options)
            throws PulsarAdminException {
        return sync(() -> getPartitionedTopicListAsync(namespace, options));
    }

    @Override
    public CompletableFuture<List<String>> getPartitionedTopicListAsync(String namespace) {
        return getPartitionedTopicListAsync(namespace, ListTopicsOptions.EMPTY);
    }

    @Override
    public CompletableFuture<List<String>> getPartitionedTopicListAsync(String namespace, ListTopicsOptions options) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget persistentPath = namespacePath("persistent", ns, "partitioned");
        WebTarget nonPersistentPath = namespacePath("non-persistent", ns, "partitioned");
        persistentPath = persistentPath.queryParam("includeSystemTopic", options.isIncludeSystemTopic());
        nonPersistentPath = nonPersistentPath.queryParam("includeSystemTopic", options.isIncludeSystemTopic());
        final CompletableFuture<List<String>> persistentList =
                asyncGetRequest(persistentPath, new FutureCallback<List<String>>() {});
        final CompletableFuture<List<String>> nonPersistentList =
                asyncGetRequest(nonPersistentPath, new FutureCallback<List<String>>() {});

        return persistentList.thenCombine(nonPersistentList,
                (l1, l2) -> new ArrayList<>(Stream.concat(l1.stream(), l2.stream()).collect(Collectors.toSet())));
    }


    @Override
    public List<String> getListInBundle(String namespace, String bundleRange) throws PulsarAdminException {
        return sync(() -> getListInBundleAsync(namespace, bundleRange));
    }

    @Override
    public CompletableFuture<List<String>> getListInBundleAsync(String namespace, String bundleRange) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath("non-persistent", ns, bundleRange);
        return asyncGetRequest(path, new FutureCallback<List<String>>(){});
    }


    @Override
    public Map<String, Set<AuthAction>> getPermissions(String topic) throws PulsarAdminException {
        return sync(() -> getPermissionsAsync(topic));
    }

    @Override
    public CompletableFuture<Map<String, Set<AuthAction>>> getPermissionsAsync(String topic) {
        TopicName tn = TopicName.get(topic);
        WebTarget path = topicPath(tn, "permissions");
        return asyncGetRequest(path, new FutureCallback<Map<String, Set<AuthAction>>>(){});
    }

    @Override
    public void grantPermission(String topic, String role, Set<AuthAction> actions) throws PulsarAdminException {
        sync(() -> grantPermissionAsync(topic, role, actions));
    }

    @Override
    public CompletableFuture<Void> grantPermissionAsync(String topic, String role, Set<AuthAction> actions) {
        TopicName tn = TopicName.get(topic);
        WebTarget path = topicPath(tn, "permissions", role);
        return asyncPostRequest(path, Entity.entity(actions, MediaType.APPLICATION_JSON));
    }

    @Override
    public void revokePermissions(String topic, String role) throws PulsarAdminException {
        sync(() -> revokePermissionsAsync(topic, role));
    }

    @Override
    public CompletableFuture<Void> revokePermissionsAsync(String topic, String role) {
        TopicName tn = TopicName.get(topic);
        WebTarget path = topicPath(tn, "permissions", role);
        return asyncDeleteRequest(path);
    }

    @Override
    public void createPartitionedTopic(String topic, int numPartitions, Map<String, String> metadata)
            throws PulsarAdminException {
        sync(() -> createPartitionedTopicAsync(topic, numPartitions, metadata));
    }

    @Override
    public void createNonPartitionedTopic(String topic, Map<String, String> metadata) throws PulsarAdminException {
        sync(() -> createNonPartitionedTopicAsync(topic, metadata));
    }

    @Override
    public void createMissedPartitions(String topic) throws PulsarAdminException {
        sync(() -> createMissedPartitionsAsync(topic));
    }

    @Override
    public CompletableFuture<Void> createNonPartitionedTopicAsync(String topic, Map<String, String> properties){
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn);
        properties = properties == null ? new HashMap<>() : properties;
        return asyncPutRequest(path, Entity.entity(properties, MediaType.APPLICATION_JSON));
    }

    @Override
    public CompletableFuture<Void> createPartitionedTopicAsync(String topic, int numPartitions,
                                                               Map<String, String> properties) {
        return createPartitionedTopicAsync(topic, numPartitions, false, properties);
    }

    public CompletableFuture<Void> createPartitionedTopicAsync(
            String topic, int numPartitions, boolean createLocalTopicOnly, Map<String, String> properties) {
        checkArgument(numPartitions > 0, "Number of partitions should be more than 0");
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "partitions")
                .queryParam("createLocalTopicOnly", Boolean.toString(createLocalTopicOnly));
        Entity entity;
        if (properties != null && !properties.isEmpty()) {
            PartitionedTopicMetadata metadata = new PartitionedTopicMetadata(numPartitions, properties);
            entity = Entity.entity(metadata, MediaType.valueOf(PartitionedTopicMetadata.MEDIA_TYPE));
        } else {
            entity = Entity.entity(numPartitions, MediaType.APPLICATION_JSON);
        }
        return asyncPutRequest(path, entity);
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
        sync(() -> updatePartitionedTopicAsync(topic, numPartitions));
    }

    @Override
    public CompletableFuture<Void> updatePartitionedTopicAsync(String topic, int numPartitions) {
        return updatePartitionedTopicAsync(topic, numPartitions, false, false);
    }

    @Override
    public void updatePartitionedTopic(String topic, int numPartitions, boolean updateLocalTopicOnly)
            throws PulsarAdminException {
        updatePartitionedTopic(topic, numPartitions, updateLocalTopicOnly, false);
    }

    @Override
    public void updatePartitionedTopic(String topic, int numPartitions, boolean updateLocalTopicOnly, boolean force)
            throws PulsarAdminException {
        sync(() -> updatePartitionedTopicAsync(topic, numPartitions, updateLocalTopicOnly, force));
    }

    @Override
    public CompletableFuture<Void> updatePartitionedTopicAsync(String topic, int numPartitions,
            boolean updateLocalTopicOnly) {
        return updatePartitionedTopicAsync(topic, numPartitions, updateLocalTopicOnly, false);
    }

    @Override
    public CompletableFuture<Void> updatePartitionedTopicAsync(String topic, int numPartitions,
            boolean updateLocalTopicOnly, boolean force) {
        checkArgument(numPartitions > 0, "Number of partitions must be more than 0");
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "partitions");
        path = path.queryParam("updateLocalTopicOnly", Boolean.toString(updateLocalTopicOnly)).queryParam("force",
                force);
        return asyncPostRequest(path, Entity.entity(numPartitions, MediaType.APPLICATION_JSON));
    }

    @Override
    public PartitionedTopicMetadata getPartitionedTopicMetadata(String topic) throws PulsarAdminException {
        return sync(() -> getPartitionedTopicMetadataAsync(topic));
    }

    @Override
    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadataAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "partitions");
        return asyncGetRequest(path, new FutureCallback<PartitionedTopicMetadata>(){});
    }

    @Override
    public Map<String, String> getProperties(String topic) throws PulsarAdminException {
        return sync(() -> getPropertiesAsync(topic));
    }

    @Override
    public CompletableFuture<Map<String, String>> getPropertiesAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "properties");
        return asyncGetRequest(path, new FutureCallback<Map<String, String>>(){});
    }

    @Override
    public void updateProperties(String topic, Map<String, String> properties) throws PulsarAdminException {
        sync(() -> updatePropertiesAsync(topic, properties));
    }

    @Override
    public CompletableFuture<Void> updatePropertiesAsync(String topic, Map<String, String> properties) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "properties");
        if (properties == null) {
            properties = new HashMap<>();
        }
        return asyncPutRequest(path, Entity.entity(properties, MediaType.APPLICATION_JSON));
    }

    @Override
    public void deletePartitionedTopic(String topic) throws PulsarAdminException {
        deletePartitionedTopic(topic, false);
    }

    @Override
    public void removeProperties(String topic, String key) throws PulsarAdminException {
        sync(() -> removePropertiesAsync(topic, key));
    }

    @Override
    public CompletableFuture<Void> removePropertiesAsync(String topic, String key) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "properties")
                .queryParam("key", key);
        return asyncDeleteRequest(path);
    }

    @Override
    public CompletableFuture<Void> deletePartitionedTopicAsync(String topic) {
        return deletePartitionedTopicAsync(topic, false);
    }

    @Override
    public void deletePartitionedTopic(String topic, boolean force, boolean deleteSchema) throws PulsarAdminException {
        sync(() -> deletePartitionedTopicAsync(topic, force, true));
    }

    @Override
    public CompletableFuture<Void> deletePartitionedTopicAsync(String topic, boolean force, boolean deleteSchema) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "partitions") //
                .queryParam("force", Boolean.toString(force)) //
                .queryParam("deleteSchema", "true");
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
    public void delete(String topic, boolean force, boolean deleteSchema) throws PulsarAdminException {
        sync(() -> deleteAsync(topic, force, true));
    }

    @Override
    public CompletableFuture<Void> deleteAsync(String topic, boolean force, boolean deleteSchema) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn) //
                .queryParam("force", Boolean.toString(force)) //
                .queryParam("deleteSchema", "true");
        return asyncDeleteRequest(path);
    }

    @Override
    public void unload(String topic) throws PulsarAdminException {
        sync(() -> unloadAsync(topic));
    }

    @Override
    public CompletableFuture<Void> unloadAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "unload");
        return asyncPutRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public MessageId terminateTopic(String topic) throws PulsarAdminException {
        return sync(() -> terminateTopicAsync(topic));
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
    public Map<Integer, MessageId> terminatePartitionedTopic(String topic) throws PulsarAdminException {
        return sync(() -> terminatePartitionedTopicAsync(topic));
    }

    @Override
    public CompletableFuture<Map<Integer, MessageId>> terminatePartitionedTopicAsync(String topic) {
        TopicName tn = validateTopic(topic);

        final CompletableFuture<Map<Integer, MessageId>> future = new CompletableFuture<>();
        try {
            final WebTarget path = topicPath(tn, "terminate", "partitions");

            request(path).async().post(Entity.entity("", MediaType.APPLICATION_JSON),
                    new InvocationCallback<Map<Integer, MessageIdImpl>>() {

                        @Override
                        public void completed(Map<Integer, MessageIdImpl> messageId) {
                            Map<Integer, MessageId> messageIdImpl = new HashMap<>();
                            for (Map.Entry<Integer, MessageIdImpl> entry: messageId.entrySet()) {
                                messageIdImpl.put(entry.getKey(), entry.getValue());
                            }
                            future.complete(messageIdImpl);
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
        return sync(() -> getSubscriptionsAsync(topic));
    }

    @Override
    public CompletableFuture<List<String>> getSubscriptionsAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "subscriptions");
        return asyncGetRequest(path, new FutureCallback<List<String>>(){});
    }

    @Override
    public TopicStats getStats(String topic, GetStatsOptions getStatsOptions) throws PulsarAdminException {
        boolean getPreciseBacklog = getStatsOptions.isGetPreciseBacklog();
        boolean subscriptionBacklogSize = getStatsOptions.isSubscriptionBacklogSize();
        boolean getEarliestTimeInBacklog = getStatsOptions.isGetEarliestTimeInBacklog();
        return sync(() -> getStatsAsync(topic, getPreciseBacklog, subscriptionBacklogSize, getEarliestTimeInBacklog));
    }

    @Override
    public CompletableFuture<TopicStats> getStatsAsync(String topic, boolean getPreciseBacklog,
                                                       boolean subscriptionBacklogSize,
                                                       boolean getEarliestTimeInBacklog) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "stats")
                .queryParam("getPreciseBacklog", getPreciseBacklog)
                .queryParam("subscriptionBacklogSize", subscriptionBacklogSize)
                .queryParam("getEarliestTimeInBacklog", getEarliestTimeInBacklog);
        final CompletableFuture<TopicStats> future = new CompletableFuture<>();

        InvocationCallback<TopicStats> persistentCB = new InvocationCallback<TopicStats>() {
            @Override
            public void completed(TopicStats response) {
                future.complete(response);
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(getApiException(throwable.getCause()));
            }
        };

        InvocationCallback<NonPersistentTopicStats> nonpersistentCB =
                new InvocationCallback<NonPersistentTopicStats>() {
            @Override
            public void completed(NonPersistentTopicStats response) {
                future.complete(response);
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(getApiException(throwable.getCause()));
            }
        };

        if (topic.startsWith(TopicDomain.non_persistent.value())) {
            asyncGetRequest(path, nonpersistentCB);
        } else {
            asyncGetRequest(path, persistentCB);
        }

        return future;
    }

    @Override
    public PersistentTopicInternalStats getInternalStats(String topic) throws PulsarAdminException {
        return getInternalStats(topic, false);
    }

    @Override
    public PersistentTopicInternalStats getInternalStats(String topic, boolean metadata) throws PulsarAdminException {
        return sync(() -> getInternalStatsAsync(topic, metadata));
    }

    @Override
    public CompletableFuture<PersistentTopicInternalStats> getInternalStatsAsync(String topic) {
        return getInternalStatsAsync(topic, false);
    }

    @Override
    public CompletableFuture<PersistentTopicInternalStats> getInternalStatsAsync(String topic, boolean metadata) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "internalStats");
        path = path.queryParam("metadata", metadata);
        return asyncGetRequest(path, new FutureCallback<PersistentTopicInternalStats>(){});
    }

    @Override
    public String getInternalInfo(String topic) throws PulsarAdminException {
        return sync(() -> getInternalInfoAsync(topic));
    }

    @Override
    public CompletableFuture<String> getInternalInfoAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "internal-info");
        return asyncGetRequest(path, new FutureCallback<String>(){});
    }

    @Override
    public PartitionedTopicStats getPartitionedStats(String topic, boolean perPartition, boolean getPreciseBacklog,
                                                     boolean subscriptionBacklogSize, boolean getEarliestTimeInBacklog)
            throws PulsarAdminException {
        return sync(() -> getPartitionedStatsAsync(topic, perPartition, getPreciseBacklog,
                subscriptionBacklogSize, getEarliestTimeInBacklog));
    }

    @Override
    public CompletableFuture<PartitionedTopicStats> getPartitionedStatsAsync(String topic,
            boolean perPartition, boolean getPreciseBacklog, boolean subscriptionBacklogSize,
                                                                             boolean getEarliestTimeInBacklog) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "partitioned-stats");
        path = path.queryParam("perPartition", perPartition)
                .queryParam("getPreciseBacklog", getPreciseBacklog)
                .queryParam("subscriptionBacklogSize", subscriptionBacklogSize)
                .queryParam("getEarliestTimeInBacklog", getEarliestTimeInBacklog);
        final CompletableFuture<PartitionedTopicStats> future = new CompletableFuture<>();

        InvocationCallback<NonPersistentPartitionedTopicStats> nonpersistentCB =
                new InvocationCallback<NonPersistentPartitionedTopicStats>() {

            @Override
            public void completed(NonPersistentPartitionedTopicStats response) {
                if (!perPartition) {
                    response.getPartitions().clear();
                }
                future.complete(response);
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(getApiException(throwable.getCause()));
            }
        };

        InvocationCallback<PartitionedTopicStats> persistentCB = new InvocationCallback<PartitionedTopicStats>() {

            @Override
            public void completed(PartitionedTopicStats response) {
                if (!perPartition) {
                    response.getPartitions().clear();
                }
                future.complete(response);
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(getApiException(throwable.getCause()));
            }
        };

        if (topic.startsWith(TopicDomain.non_persistent.value())) {
            asyncGetRequest(path, nonpersistentCB);
        } else {
            asyncGetRequest(path, persistentCB);
        }
        return future;
    }

    @Override
    public PartitionedTopicInternalStats getPartitionedInternalStats(String topic)
            throws PulsarAdminException {
        return sync(() -> getPartitionedInternalStatsAsync(topic));
    }

    @Override
    public CompletableFuture<PartitionedTopicInternalStats> getPartitionedInternalStatsAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "partitioned-internalStats");
        return asyncGetRequest(path, new FutureCallback<PartitionedTopicInternalStats>(){});
    }

    @Override
    public void deleteSubscription(String topic, String subName) throws PulsarAdminException {
        sync(() -> deleteSubscriptionAsync(topic, subName));
    }

    @Override
    public void deleteSubscription(String topic, String subName, boolean force) throws PulsarAdminException {
        sync(() -> deleteSubscriptionAsync(topic, subName, force));
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
        sync(() -> skipAllMessagesAsync(topic, subName));
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
        sync(() -> skipMessagesAsync(topic, subName, numMessages));
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
        sync(() -> expireMessagesAsync(topic, subName, expireTimeInSeconds));
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
    public void expireMessages(String topic, String subscriptionName, MessageId messageId, boolean isExcluded)
            throws PulsarAdminException {
        sync(() -> expireMessagesAsync(topic, subscriptionName, messageId, isExcluded));
    }

    @Override
    public CompletableFuture<Void> expireMessagesAsync(String topic, String subscriptionName,
                                                       MessageId messageId, boolean isExcluded) {
        TopicName tn = validateTopic(topic);
        String encodedSubName = Codec.encode(subscriptionName);
        ResetCursorData resetCursorData = new ResetCursorData(messageId);
        resetCursorData.setExcluded(isExcluded);
        WebTarget path = topicPath(tn, "subscription", encodedSubName, "expireMessages");
        return asyncPostRequest(path, Entity.entity(resetCursorData, MediaType.APPLICATION_JSON));
    }

    @Override
    public void expireMessagesForAllSubscriptions(String topic, long expireTimeInSeconds) throws PulsarAdminException {
        sync(() -> expireMessagesForAllSubscriptionsAsync(topic, expireTimeInSeconds));
    }

    @Override
    public CompletableFuture<Void> expireMessagesForAllSubscriptionsAsync(String topic, long expireTimeInSeconds) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "all_subscription",
                "expireMessages", String.valueOf(expireTimeInSeconds));
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    private CompletableFuture<List<Message<byte[]>>> peekNthMessage(
            String topic, String subName, int messagePosition, boolean showServerMarker,
            TransactionIsolationLevel transactionIsolationLevel) {
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
                            future.complete(getMessagesFromHttpResponse(tn.toString(), response,
                                    showServerMarker, transactionIsolationLevel));
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
    public List<Message<byte[]>> peekMessages(String topic, String subName, int numMessages,
                                              boolean showServerMarker,
                                              TransactionIsolationLevel transactionIsolationLevel)
            throws PulsarAdminException {
        return sync(() -> peekMessagesAsync(topic, subName, numMessages, showServerMarker, transactionIsolationLevel));
    }

    @Override
    public CompletableFuture<List<Message<byte[]>>> peekMessagesAsync(
            String topic, String subName, int numMessages,
            boolean showServerMarker, TransactionIsolationLevel transactionIsolationLevel) {
        checkArgument(numMessages > 0);
        CompletableFuture<List<Message<byte[]>>> future = new CompletableFuture<List<Message<byte[]>>>();
        peekMessagesAsync(topic, subName, numMessages, new ArrayList<>(),
                future, 1, showServerMarker, transactionIsolationLevel);
        return future;
    }

    private void peekMessagesAsync(String topic, String subName, int numMessages,
            List<Message<byte[]>> messages, CompletableFuture<List<Message<byte[]>>> future, int nthMessage,
            boolean showServerMarker, TransactionIsolationLevel transactionIsolationLevel) {
        if (numMessages <= 0) {
            future.complete(messages);
            return;
        }

        // if peeking first message succeeds, we know that the topic and subscription exists
        peekNthMessage(topic, subName, nthMessage, showServerMarker, transactionIsolationLevel)
                .handle((r, ex) -> {
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
            peekMessagesAsync(topic, subName, numMessages - r.size(), messages, future,
                    nthMessage + 1, showServerMarker, transactionIsolationLevel);
            return null;
        });
    }

    @Override
    public Message<byte[]> examineMessage(String topic, String initialPosition, long messagePosition)
            throws PulsarAdminException {
        return sync(() -> examineMessageAsync(topic, initialPosition, messagePosition));
    }

    @Override
    public CompletableFuture<Message<byte[]>> examineMessageAsync(String topic, String initialPosition,
                                                                  long messagePosition) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "examinemessage")
                .queryParam("initialPosition", initialPosition)
                .queryParam("messagePosition", messagePosition);
        final CompletableFuture<Message<byte[]>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        try {
                            List<Message<byte[]>> messages = getMessagesFromHttpResponse(tn.toString(), response);
                            if (messages.size() > 0) {
                                future.complete(messages.get(0));
                            } else {
                                future.complete(null);
                            }
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
    public void truncate(String topic) throws PulsarAdminException {
        sync(() -> truncateAsync(topic));
    }

    @Override
    public CompletableFuture<Void> truncateAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "truncate"); //
        return asyncDeleteRequest(path);
    }

    @Override
    public CompletableFuture<List<Message<byte[]>>> getMessagesByIdAsync(String topic, long ledgerId, long entryId) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "ledger", Long.toString(ledgerId), "entry", Long.toString(entryId));
        final CompletableFuture<List<Message<byte[]>>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        try {
                            future.complete(getMessagesFromHttpResponse(topicName.toString(), response));
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
    public List<Message<byte[]>> getMessagesById(String topic, long ledgerId, long entryId)
            throws PulsarAdminException {
        return sync(() -> getMessagesByIdAsync(topic, ledgerId, entryId));
    }

    @Deprecated
    @Override
    public CompletableFuture<Message<byte[]>> getMessageByIdAsync(String topic, long ledgerId, long entryId) {
        return getMessagesByIdAsync(topic, ledgerId, entryId).thenApply(n -> n.get(0));
    }

    @Deprecated
    @Override
    public Message<byte[]> getMessageById(String topic, long ledgerId, long entryId)
            throws PulsarAdminException {
        return sync(() -> getMessageByIdAsync(topic, ledgerId, entryId));
    }

    @Override
    public CompletableFuture<MessageId> getMessageIdByTimestampAsync(String topic, long timestamp) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "messageid", Long.toString(timestamp));
        return asyncGetRequest(path, new FutureCallback<MessageIdImpl>(){})
                .thenApply(messageIdImpl -> messageIdImpl);
    }


    @Override
    public MessageId getMessageIdByTimestamp(String topic, long timestamp)
            throws PulsarAdminException {
        return sync(() -> getMessageIdByTimestampAsync(topic, timestamp));
    }


    @Override
    public void createSubscription(String topic, String subscriptionName, MessageId messageId, boolean replicated,
                                   Map<String, String> properties)
            throws PulsarAdminException {
        sync(() -> createSubscriptionAsync(topic, subscriptionName, messageId, replicated, properties));
    }

    @Override
    public CompletableFuture<Void> createSubscriptionAsync(String topic, String subscriptionName,
            MessageId messageId, boolean replicated, Map<String, String> properties) {
        TopicName tn = validateTopic(topic);
        String encodedSubName = Codec.encode(subscriptionName);
        WebTarget path = topicPath(tn, "subscription", encodedSubName);
        path = path.queryParam("replicated", replicated);
        Object payload = messageId;
        if (properties != null && !properties.isEmpty()) {
            ResetCursorData resetCursorData = messageId != null
                    ? new ResetCursorData(messageId) : new ResetCursorData(MessageId.latest);
            resetCursorData.setProperties(properties);
            payload = resetCursorData;
        }
        return asyncPutRequest(path, Entity.entity(payload, MediaType.APPLICATION_JSON));
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
        sync(() -> resetCursorAsync(topic, subName, messageId));
    }

    @Override
    public void updateSubscriptionProperties(String topic, String subName, Map<String, String> subscriptionProperties)
            throws PulsarAdminException {
        sync(() -> updateSubscriptionPropertiesAsync(topic, subName, subscriptionProperties));
    }

    @Override
    public Map<String, String> getSubscriptionProperties(String topic, String subName)
            throws PulsarAdminException {
        return sync(() -> getSubscriptionPropertiesAsync(topic, subName));
    }

    @Override
    public CompletableFuture<Void> updateSubscriptionPropertiesAsync(String topic, String subName,
                                                                     Map<String, String> subscriptionProperties) {
        TopicName tn = validateTopic(topic);
        String encodedSubName = Codec.encode(subName);
        WebTarget path = topicPath(tn, "subscription", encodedSubName,
                "properties");
        if (subscriptionProperties == null) {
            subscriptionProperties = new HashMap<>();
        }
        return asyncPutRequest(path, Entity.entity(subscriptionProperties, MediaType.APPLICATION_JSON));
    }

    @Override
    public CompletableFuture<Map<String, String>> getSubscriptionPropertiesAsync(String topic, String subName) {
        TopicName tn = validateTopic(topic);
        String encodedSubName = Codec.encode(subName);
        WebTarget path = topicPath(tn, "subscription", encodedSubName,
                "properties");
        return asyncGetRequest(path, new FutureCallback<Map<String, String>>(){});
    }

    @Override
    public void resetCursor(String topic, String subName, MessageId messageId
            , boolean isExcluded) throws PulsarAdminException {
        sync(() -> resetCursorAsync(topic, subName, messageId, isExcluded));
    }

    @Override
    public CompletableFuture<Void> resetCursorAsync(String topic, String subName, MessageId messageId) {
        return resetCursorAsync(topic, subName, messageId, false);
    }

    @Override
    public CompletableFuture<Void> resetCursorAsync(String topic, String subName
            , MessageId messageId, boolean isExcluded) {
        TopicName tn = validateTopic(topic);
        String encodedSubName = Codec.encode(subName);
        final WebTarget path = topicPath(tn, "subscription", encodedSubName, "resetcursor");
        ResetCursorData resetCursorData = new ResetCursorData(messageId);
        resetCursorData.setExcluded(isExcluded);
        return asyncPostRequest(path, Entity.entity(resetCursorData, MediaType.APPLICATION_JSON));
    }

    @Override
    public void triggerCompaction(String topic) throws PulsarAdminException {
        sync(() -> triggerCompactionAsync(topic));
    }

    @Override
    public CompletableFuture<Void> triggerCompactionAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "compaction");
        return asyncPutRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void trimTopic(String topic) throws PulsarAdminException {
        sync(() -> trimTopicAsync(topic));
    }

    @Override
    public CompletableFuture<Void> trimTopicAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "trim");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public LongRunningProcessStatus compactionStatus(String topic)
            throws PulsarAdminException {
        return sync(() -> compactionStatusAsync(topic));
    }

    @Override
    public CompletableFuture<LongRunningProcessStatus> compactionStatusAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "compaction");
        return asyncGetRequest(path, new FutureCallback<LongRunningProcessStatus>(){});
    }

    @Override
    public void triggerOffload(String topic, MessageId messageId) throws PulsarAdminException {
        sync(() -> triggerOffloadAsync(topic, messageId));
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
        return sync(() -> offloadStatusAsync(topic));
    }

    @Override
    public CompletableFuture<OffloadProcessStatus> offloadStatusAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "offload");
        return asyncGetRequest(path, new FutureCallback<OffloadProcessStatus>(){});
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
        return getMessagesFromHttpResponse(topic, response, true,
                TransactionIsolationLevel.READ_UNCOMMITTED);
    }

    private List<Message<byte[]>> getMessagesFromHttpResponse(
            String topic, Response response, boolean showServerMarker,
            TransactionIsolationLevel transactionIsolationLevel) throws Exception {

        if (response.getStatus() != Status.OK.getStatusCode()) {
            throw getApiException(response);
        }

        String msgId = response.getHeaderString(MESSAGE_ID);

        // build broker entry metadata if exist
        String brokerEntryTimestamp = response.getHeaderString(BROKER_ENTRY_TIMESTAMP);
        String brokerEntryIndex = response.getHeaderString(BROKER_ENTRY_INDEX);
        BrokerEntryMetadata brokerEntryMetadata;
        if (brokerEntryTimestamp == null && brokerEntryIndex == null) {
            brokerEntryMetadata = null;
        } else {
            brokerEntryMetadata = new BrokerEntryMetadata();
            if (brokerEntryTimestamp != null) {
                brokerEntryMetadata.setBrokerTimestamp(DateFormatter.parse(brokerEntryTimestamp));
            }

            if (brokerEntryIndex != null) {
                brokerEntryMetadata.setIndex(Long.parseLong(brokerEntryIndex));
            }
        }

        MessageMetadata messageMetadata = new MessageMetadata();
        try (InputStream stream = (InputStream) response.getEntity()) {
            byte[] data = new byte[stream.available()];
            stream.read(data);

            Map<String, String> properties = new TreeMap<>();
            MultivaluedMap<String, Object> headers = response.getHeaders();
            Object tmp = headers.getFirst(MARKER_TYPE);
            if (tmp != null) {
                if (!showServerMarker) {
                    return new ArrayList<>();
                } else {
                    messageMetadata.setMarkerType(Integer.parseInt(tmp.toString()));
                }
            }

            tmp = headers.getFirst(TXN_ABORTED);
            if (tmp != null && Boolean.parseBoolean(tmp.toString())) {
                properties.put(TXN_ABORTED, tmp.toString());
                if (transactionIsolationLevel == TransactionIsolationLevel.READ_COMMITTED) {
                    return new ArrayList<>();
                }
            }

            tmp = headers.getFirst(TXN_UNCOMMITTED);
            if (tmp != null && Boolean.parseBoolean(tmp.toString())) {
                properties.put(TXN_UNCOMMITTED, tmp.toString());
                if (transactionIsolationLevel == TransactionIsolationLevel.READ_COMMITTED) {
                    return new ArrayList<>();
                }
            }

            tmp = headers.getFirst(PUBLISH_TIME);
            if (tmp != null) {
                messageMetadata.setPublishTime(DateFormatter.parse(tmp.toString()));
            }

            tmp = headers.getFirst(EVENT_TIME);
            if (tmp != null) {
                messageMetadata.setEventTime(DateFormatter.parse(tmp.toString()));
            }

            tmp = headers.getFirst(DELIVER_AT_TIME);
            if (tmp != null) {
                messageMetadata.setDeliverAtTime(DateFormatter.parse(tmp.toString()));
            }

            tmp = headers.getFirst("X-Pulsar-null-value");
            if (tmp != null) {
                messageMetadata.setNullValue(Boolean.parseBoolean(tmp.toString()));
            }

            tmp = headers.getFirst(PRODUCER_NAME);
            if (tmp != null) {
                messageMetadata.setProducerName(tmp.toString());
            }
            tmp = headers.getFirst(SEQUENCE_ID);
            if (tmp != null) {
                messageMetadata.setSequenceId(Long.parseLong(tmp.toString()));
            }
            tmp = headers.getFirst(REPLICATED_FROM);
            if (tmp != null) {
                messageMetadata.setReplicatedFrom(tmp.toString());
            }
            tmp = headers.getFirst(PARTITION_KEY);
            if (tmp != null) {
                messageMetadata.setPartitionKey(tmp.toString());
            }
            tmp = headers.getFirst(COMPRESSION);
            if (tmp != null) {
                messageMetadata.setCompression(CompressionType.valueOf(tmp.toString()));
            }
            tmp = headers.getFirst(UNCOMPRESSED_SIZE);
            if (tmp != null) {
                messageMetadata.setUncompressedSize(Integer.parseInt(tmp.toString()));
            }
            tmp = headers.getFirst(ENCRYPTION_ALGO);
            if (tmp != null) {
                messageMetadata.setEncryptionAlgo(tmp.toString());
            }
            tmp = headers.getFirst(PARTITION_KEY_B64_ENCODED);
            if (tmp != null) {
                messageMetadata.setPartitionKeyB64Encoded(Boolean.parseBoolean(tmp.toString()));
            }
            tmp = headers.getFirst(TXNID_LEAST_BITS);
            if (tmp != null) {
                messageMetadata.setTxnidLeastBits(Long.parseLong(tmp.toString()));
            }
            tmp = headers.getFirst(TXNID_MOST_BITS);
            if (tmp != null) {
                messageMetadata.setTxnidMostBits(Long.parseLong(tmp.toString()));
            }
            tmp = headers.getFirst(HIGHEST_SEQUENCE_ID);
            if (tmp != null) {
                messageMetadata.setHighestSequenceId(Long.parseLong(tmp.toString()));
            }
            tmp = headers.getFirst(UUID);
            if (tmp != null) {
                messageMetadata.setUuid(tmp.toString());
            }
            tmp = headers.getFirst(NUM_CHUNKS_FROM_MSG);
            if (tmp != null) {
                messageMetadata.setNumChunksFromMsg(Integer.parseInt(tmp.toString()));
            }
            tmp = headers.getFirst(TOTAL_CHUNK_MSG_SIZE);
            if (tmp != null) {
                messageMetadata.setTotalChunkMsgSize(Integer.parseInt(tmp.toString()));
            }
            tmp = headers.getFirst(CHUNK_ID);
            if (tmp != null) {
                messageMetadata.setChunkId(Integer.parseInt(tmp.toString()));
            }
            tmp = headers.getFirst(NULL_PARTITION_KEY);
            if (tmp != null) {
                messageMetadata.setNullPartitionKey(Boolean.parseBoolean(tmp.toString()));
            }
            tmp = headers.getFirst(ENCRYPTION_PARAM);
            if (tmp != null) {
                messageMetadata.setEncryptionParam(Base64.getDecoder().decode(tmp.toString()));
            }
            tmp = headers.getFirst(ORDERING_KEY);
            if (tmp != null) {
                messageMetadata.setOrderingKey(Base64.getDecoder().decode(tmp.toString()));
            }
            tmp = headers.getFirst(SCHEMA_VERSION);
            if (tmp != null) {
                messageMetadata.setSchemaVersion(Base64.getDecoder().decode(tmp.toString()));
            }
            tmp = headers.getFirst(ENCRYPTION_PARAM);
            if (tmp != null) {
                messageMetadata.setEncryptionParam(Base64.getDecoder().decode(tmp.toString()));
            }
            List<Object> tmpList = headers.get(REPLICATED_TO);
            if (tmpList != null) {
                for (Object o : tmpList) {
                    messageMetadata.addReplicateTo(o.toString());
                }
            }
            tmpList = headers.get(ENCRYPTION_KEYS);
            if (tmpList != null) {
                for (Object o : tmpList) {
                    EncryptionKeys encryptionKey = messageMetadata.addEncryptionKey();
                    encryptionKey.parseFrom(Base64.getDecoder().decode(o.toString()));
                }
            }

            tmp = headers.getFirst(BATCH_SIZE_HEADER);
            if (tmp != null) {
                properties.put(BATCH_SIZE_HEADER, (String) tmp);
            }

            for (Entry<String, List<Object>> entry : headers.entrySet()) {
                String header = entry.getKey();
                if (header.contains("X-Pulsar-PROPERTY-")) {
                    String keyName = header.substring("X-Pulsar-PROPERTY-".length());
                    properties.put(keyName, (String) entry.getValue().get(0));
                }
            }

            tmp = headers.getFirst(BATCH_HEADER);
            if (tmp != null) {
                properties.put(BATCH_HEADER, (String) tmp);
            }

            boolean isEncrypted = false;
            tmp = headers.getFirst("X-Pulsar-Is-Encrypted");
            if (tmp != null) {
                isEncrypted = Boolean.parseBoolean(tmp.toString());
            }

            if (!isEncrypted && response.getHeaderString(BATCH_HEADER) != null) {
                return getIndividualMsgsFromBatch(topic, msgId, data, properties, messageMetadata, brokerEntryMetadata);
            }

            MessageImpl message = new MessageImpl(topic, msgId, properties,
                    Unpooled.wrappedBuffer(data), Schema.BYTES, messageMetadata);
            if (brokerEntryMetadata != null) {
                message.setBrokerEntryMetadata(brokerEntryMetadata);
            }
            return Collections.singletonList(message);
        }
    }

    private List<Message<byte[]>> getIndividualMsgsFromBatch(String topic, String msgId, byte[] data,
                                 Map<String, String> properties, MessageMetadata msgMetadataBuilder,
                                                             BrokerEntryMetadata brokerEntryMetadata) {
        List<Message<byte[]>> ret = new ArrayList<>();
        int batchSize = Integer.parseInt(properties.get(BATCH_HEADER));
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        for (int i = 0; i < batchSize; i++) {
            String batchMsgId = msgId + ":" + i;
            SingleMessageMetadata singleMessageMetadata = new SingleMessageMetadata();
            try {
                ByteBuf singleMessagePayload =
                        Commands.deSerializeSingleMessageInBatch(buf, singleMessageMetadata, i, batchSize);
                if (singleMessageMetadata.getPropertiesCount() > 0) {
                    for (KeyValue entry : singleMessageMetadata.getPropertiesList()) {
                        properties.put(entry.getKey(), entry.getValue());
                    }
                }
                MessageImpl message = new MessageImpl<>(topic, batchMsgId, properties, singleMessagePayload,
                        Schema.BYTES, msgMetadataBuilder);
                if (brokerEntryMetadata != null) {
                    message.setBrokerEntryMetadata(brokerEntryMetadata);
                }
                ret.add(message);
            } catch (Exception ex) {
                log.error("Exception occurred while trying to get BatchMsgId: {}", batchMsgId, ex);
            }
        }
        buf.release();
        return ret;
    }

    @Override
    public MessageId getLastMessageId(String topic) throws PulsarAdminException {
        return sync(() -> getLastMessageIdAsync(topic));
    }

    @Override
    public CompletableFuture<MessageId> getLastMessageIdAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "lastMessageId");
        return asyncGetRequest(path, new FutureCallback<BatchMessageIdImpl>() {})
                .thenApply(response -> {
                    if (response.getBatchIndex() == -1) {
                        return new MessageIdImpl(response.getLedgerId(),
                                response.getEntryId(), response.getPartitionIndex());
                    }
                    return response;
                });
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
    public AnalyzeSubscriptionBacklogResult analyzeSubscriptionBacklog(String topic,
                                                                       String subscriptionName,
                                                                       Optional<MessageId> startPosition)
            throws PulsarAdminException {
        return sync(() -> analyzeSubscriptionBacklogAsync(topic, subscriptionName, startPosition));
    }

    @Override
    public CompletableFuture<AnalyzeSubscriptionBacklogResult> analyzeSubscriptionBacklogAsync(String topic,
                                                                                String subscriptionName,
                                                                                Optional<MessageId> startPosition) {
        TopicName topicName = validateTopic(topic);
        String encodedSubName = Codec.encode(subscriptionName);
        WebTarget path = topicPath(topicName, "subscription", encodedSubName, "analyzeBacklog");

        final CompletableFuture<AnalyzeSubscriptionBacklogResult> future = new CompletableFuture<>();
        Entity entity = null;
        if (startPosition.isPresent()) {
            ResetCursorData resetCursorData = new ResetCursorData(startPosition.get());
            entity = Entity.entity(resetCursorData, MediaType.APPLICATION_JSON);
        } else {
            entity = null;
        }

        asyncPostRequestWithResponse(path, entity, new InvocationCallback<AnalyzeSubscriptionBacklogResult>() {
            @Override
            public void completed(AnalyzeSubscriptionBacklogResult res) {
                future.complete(res);
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(getApiException(throwable.getCause()));
            }
        });
        return future;
    }

    @Override
    public Long getBacklogSizeByMessageId(String topic, MessageId messageId)
            throws PulsarAdminException {
        return sync(() -> getBacklogSizeByMessageIdAsync(topic, messageId));
    }

    @Override
    public CompletableFuture<Long> getBacklogSizeByMessageIdAsync(String topic, MessageId messageId) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "backlogSize");

        final CompletableFuture<Long> future = new CompletableFuture<>();
        try {
            request(path).async().put(Entity.entity(messageId, MediaType.APPLICATION_JSON),
                    new InvocationCallback<Long>() {

                @Override
                public void completed(Long backlogSize) {
                    future.complete(backlogSize);
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
    public Boolean getDeduplicationEnabled(String topic) throws PulsarAdminException {
        return sync(() -> getDeduplicationEnabledAsync(topic));
    }

    @Override
    public CompletableFuture<Boolean> getDeduplicationEnabledAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "deduplicationEnabled");
        return asyncGetRequest(path, new FutureCallback<Boolean>(){});
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

    @Override
    public void enableDeduplication(String topic, boolean enabled) throws PulsarAdminException {
        sync(() -> enableDeduplicationAsync(topic, enabled));
    }

    @Override
    public CompletableFuture<Void> enableDeduplicationAsync(String topic, boolean enabled) {
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
    public void disableDeduplication(String topic) throws PulsarAdminException {
        sync(() -> disableDeduplicationAsync(topic));
    }

    @Override
    public CompletableFuture<Void> disableDeduplicationAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "deduplicationEnabled");
        return asyncDeleteRequest(path);
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
    public void setReplicatedSubscriptionStatus(String topic, String subName, Boolean enabled)
            throws PulsarAdminException {
        sync(() -> setReplicatedSubscriptionStatusAsync(topic, subName, enabled));
    }

    @Override
    public CompletableFuture<Void> setReplicatedSubscriptionStatusAsync(String topic, String subName, Boolean enabled) {
        TopicName topicName = validateTopic(topic);
        String encodedSubName = Codec.encode(subName);
        WebTarget path = topicPath(topicName, "subscription", encodedSubName, "replicatedSubscriptionStatus");
        return asyncPostRequest(path, Entity.entity(enabled, MediaType.APPLICATION_JSON));
    }

    public Map<String, Boolean> getReplicatedSubscriptionStatus(String topic,
                                                                String subName) throws PulsarAdminException {
        return sync(() -> getReplicatedSubscriptionStatusAsync(topic, subName));
    }

    public CompletableFuture<Map<String, Boolean>> getReplicatedSubscriptionStatusAsync(String topic, String subName) {
        TopicName topicName = validateTopic(topic);
        String encodedSubName = Codec.encode(subName);
        WebTarget path = topicPath(topicName, "subscription", encodedSubName, "replicatedSubscriptionStatus");
        return asyncGetRequest(path, new FutureCallback<Map<String, Boolean>>(){});
    }

    @Override
    public boolean getSchemaValidationEnforced(String topic, boolean applied) throws PulsarAdminException {
        return sync(() -> getSchemaValidationEnforcedAsync(topic, applied));
    }

    @Override
    public void setSchemaValidationEnforced(String topic, boolean enable) throws PulsarAdminException {
        sync(() -> setSchemaValidationEnforcedAsync(topic, enable));
    }

    @Override
    public CompletableFuture<Boolean> getSchemaValidationEnforcedAsync(String topic, boolean applied) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "schemaValidationEnforced");
        path = path.queryParam("applied", applied);
        return asyncGetRequest(path, new FutureCallback<Boolean>(){});
    }

    @Override
    public CompletableFuture<Void> setSchemaValidationEnforcedAsync(String topic, boolean schemaValidationEnforced) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "schemaValidationEnforced");
        return asyncPostRequest(path, Entity.entity(schemaValidationEnforced, MediaType.APPLICATION_JSON));
    }

    @Override
    public Set<String> getReplicationClusters(String topic, boolean applied) throws PulsarAdminException {
        return sync(() -> getReplicationClustersAsync(topic, applied));
    }

    @Override
    public CompletableFuture<Set<String>> getReplicationClustersAsync(String topic, boolean applied) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "replication");
        path = path.queryParam("applied", applied);
        return asyncGetRequest(path, new FutureCallback<Set<String>>(){});
    }

    @Override
    public void setReplicationClusters(String topic, List<String> clusterIds) throws PulsarAdminException {
        sync(() -> setReplicationClustersAsync(topic, clusterIds));
    }

    @Override
    public CompletableFuture<Void> setReplicationClustersAsync(String topic, List<String> clusterIds) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "replication");
        return asyncPostRequest(path, Entity.entity(clusterIds, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeReplicationClusters(String topic) throws PulsarAdminException {
        sync(() -> removeReplicationClustersAsync(topic));
    }

    @Override
    public CompletableFuture<Void> removeReplicationClustersAsync(String topic) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn, "replication");
        return asyncDeleteRequest(path);
    }

    @Override
    public void setShadowTopics(String sourceTopic, List<String> shadowTopics) throws PulsarAdminException {
        sync(() -> setShadowTopicsAsync(sourceTopic, shadowTopics));
    }

    @Override
    public void removeShadowTopics(String sourceTopic) throws PulsarAdminException {
        sync(() -> removeShadowTopicsAsync(sourceTopic));
    }

    @Override
    public List<String> getShadowTopics(String sourceTopic) throws PulsarAdminException {
        return sync(() -> getShadowTopicsAsync(sourceTopic));
    }

    @Override
    public CompletableFuture<Void> setShadowTopicsAsync(String sourceTopic, List<String> shadowTopics) {
        TopicName tn = validateTopic(sourceTopic);
        WebTarget path = topicPath(tn, "shadowTopics");
        return asyncPutRequest(path, Entity.entity(shadowTopics, MediaType.APPLICATION_JSON));
    }

    @Override
    public CompletableFuture<Void> removeShadowTopicsAsync(String sourceTopic) {
        TopicName tn = validateTopic(sourceTopic);
        WebTarget path = topicPath(tn, "shadowTopics");
        return asyncDeleteRequest(path);
    }

    @Override
    public CompletableFuture<List<String>> getShadowTopicsAsync(String sourceTopic) {
        TopicName tn = validateTopic(sourceTopic);
        WebTarget path = topicPath(tn, "shadowTopics");
        return asyncGetRequest(path, new FutureCallback<List<String>>() {});
    }

    @Override
    public String getShadowSource(String shadowTopic) throws PulsarAdminException {
        return sync(() -> getShadowSourceAsync(shadowTopic));
    }

    @Override
    public CompletableFuture<String> getShadowSourceAsync(String shadowTopic) {
        return getPropertiesAsync(shadowTopic).thenApply(
                properties -> properties != null ? properties.get(PROPERTY_SHADOW_SOURCE_KEY) : null);
    }

    @Override
    public void createShadowTopic(String shadowTopic, String sourceTopic, Map<String, String> properties)
            throws PulsarAdminException {
        sync(() -> createShadowTopicAsync(shadowTopic, sourceTopic, properties));
    }

    @Override
    public CompletableFuture<Void> createShadowTopicAsync(String shadowTopic, String sourceTopic,
                                                          Map<String, String> properties) {
        checkArgument(TopicName.get(shadowTopic).isPersistent(), "Shadow topic must be persistent");
        checkArgument(TopicName.get(sourceTopic).isPersistent(), "Source topic must be persistent");
        return getPartitionedTopicMetadataAsync(sourceTopic).thenCompose(sourceTopicMeta -> {
            HashMap<String, String> shadowProperties = new HashMap<>();
            if (properties != null) {
                shadowProperties.putAll(properties);
            }
            shadowProperties.put(PROPERTY_SHADOW_SOURCE_KEY, sourceTopic);
            if (sourceTopicMeta.partitions == PartitionedTopicMetadata.NON_PARTITIONED) {
                return createNonPartitionedTopicAsync(shadowTopic, shadowProperties);
            } else {
                return createPartitionedTopicAsync(shadowTopic, sourceTopicMeta.partitions, shadowProperties);
            }
        });
    }

    private static final Logger log = LoggerFactory.getLogger(TopicsImpl.class);
}
