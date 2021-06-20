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
package org.apache.pulsar.broker.admin.impl;

import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.apache.pulsar.broker.resources.PulsarResources.DEFAULT_OPERATION_TIMEOUT_SEC;
import static org.apache.pulsar.common.util.Codec.decode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.zafarkhaja.semver.Version;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ManagedLedgerInfoCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetadataNotFoundException;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerOfflineBacklog;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.admin.ZkAdminPaths;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.BrokerServiceException.AlreadyRunningException;
import org.apache.pulsar.broker.service.BrokerServiceException.NotAllowedException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionInvalidCursorPosition;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicBusyException;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.OffloadProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.PartitionedManagedLedgerInfo;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.AuthPolicies;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.PartitionedTopicInternalStats;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;
import org.apache.pulsar.common.policies.data.stats.PartitionedTopicStatsImpl;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class PersistentTopicsBase extends AdminResource {
    private static final Logger log = LoggerFactory.getLogger(PersistentTopicsBase.class);

    private static final int OFFLINE_TOPIC_STAT_TTL_MINS = 10;
    private static final String DEPRECATED_CLIENT_VERSION_PREFIX = "Pulsar-CPP-v";
    private static final Version LEAST_SUPPORTED_CLIENT_VERSION_PREFIX = Version.forIntegers(1, 21);

    protected List<String> internalGetList() {
        validateNamespaceOperation(namespaceName, NamespaceOperation.GET_TOPICS);

        // Validate that namespace exists, throws 404 if it doesn't exist
        try {
            if (!namespaceResources().exists(path(POLICIES, namespaceName.toString()))) {
                throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
            }
        } catch (RestException re) {
            throw re;
        } catch (Exception e) {
            log.error("[{}] Failed to get topic list {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }

        List<String> topics = Lists.newArrayList();

        try {
            String path = String.format("/managed-ledgers/%s/%s", namespaceName.toString(), domain());
            for (String topic : getLocalPolicies().getChildren(path)) {
                if (domain().equals(TopicDomain.persistent.toString())) {
                    topics.add(TopicName.get(domain(), namespaceName, decode(topic)).toString());
                }
            }
        } catch (org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException e) {
            // NoNode means there are no topics in this domain for this namespace
        } catch (Exception e) {
            log.error("[{}] Failed to get topics list for namespace {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }

        topics.sort(null);
        return topics;
    }

    protected List<String> internalGetPartitionedTopicList() {
        validateAdminAccessForTenant(namespaceName.getTenant());
        validateNamespaceOperation(namespaceName, NamespaceOperation.GET_TOPICS);
        // Validate that namespace exists, throws 404 if it doesn't exist
        try {
            if (!namespaceResources().exists(path(POLICIES, namespaceName.toString()))) {
                log.warn("[{}] Failed to get partitioned topic list {}: Namespace does not exist", clientAppId(),
                        namespaceName);
                throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
            }
        } catch (RestException e) {
            throw e;
        } catch (Exception e) {
            log.error("[{}] Failed to get partitioned topic list for namespace {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
        return getPartitionedTopicList(TopicDomain.getEnum(domain()));
    }

    protected Map<String, Set<AuthAction>> internalGetPermissionsOnTopic() {
        // This operation should be reading from zookeeper and it should be allowed without having admin privileges
        validateAdminAccessForTenant(namespaceName.getTenant());

        String topicUri = topicName.toString();

        try {
            Policies policies = namespaceResources().get(path(POLICIES, namespaceName.toString()))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Namespace does not exist"));

            Map<String, Set<AuthAction>> permissions = Maps.newHashMap();
            AuthPolicies auth = policies.auth_policies;

            // First add namespace level permissions
            auth.getNamespaceAuthentication().forEach(permissions::put);

            // Then add topic level permissions
            if (auth.getTopicAuthentication().containsKey(topicUri)) {
                for (Map.Entry<String, Set<AuthAction>> entry :
                        auth.getTopicAuthentication().get(topicUri).entrySet()) {
                    String role = entry.getKey();
                    Set<AuthAction> topicPermissions = entry.getValue();

                    if (!permissions.containsKey(role)) {
                        permissions.put(role, topicPermissions);
                    } else {
                        // Do the union between namespace and topic level
                        Set<AuthAction> union = Sets.union(permissions.get(role), topicPermissions);
                        permissions.put(role, union);
                    }
                }
            }

            return permissions;
        } catch (Exception e) {
            log.error("[{}] Failed to get permissions for topic {}", clientAppId(), topicUri, e);
            throw new RestException(e);
        }
    }

    protected void validateAdminAndClientPermission() {
        try {
            validateAdminAccessForTenant(topicName.getTenant());
        } catch (Exception ve) {
            try {
                checkAuthorization(pulsar(), topicName, clientAppId(), clientAuthData());
            } catch (RestException re) {
                throw re;
            } catch (Exception e) {
                // unknown error marked as internal server error
                log.warn("Unexpected error while authorizing request. topic={}, role={}. Error: {}",
                        topicName, clientAppId(), e.getMessage(), e);
                throw new RestException(e);
            }
        }
    }

    public void validateAdminOperationOnTopic(boolean authoritative) {
        validateAdminAccessForTenant(topicName.getTenant());
        validateTopicOwnership(topicName, authoritative);
    }

    private void grantPermissions(String topicUri, String role, Set<AuthAction> actions) {
        try {
            namespaceResources().set(path(POLICIES, namespaceName.toString()), (policies) -> {
                if (!policies.auth_policies.getTopicAuthentication().containsKey(topicUri)) {
                    policies.auth_policies.getTopicAuthentication().put(topicUri, new HashMap<>());
                }

                policies.auth_policies.getTopicAuthentication().get(topicUri).put(role, actions);
                return policies;
            });
            log.info("[{}] Successfully granted access for role {}: {} - topic {}", clientAppId(), role, actions,
                    topicUri);
        } catch (org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException e) {
            log.warn("[{}] Failed to grant permissions on topic {}: Namespace does not exist", clientAppId(), topicUri);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to grant permissions for topic {}", clientAppId(), topicUri, e);
            throw new RestException(e);
        }
    }

    protected void internalGrantPermissionsOnTopic(String role, Set<AuthAction> actions) {
        // This operation should be reading from zookeeper and it should be allowed without having admin privileges
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        PartitionedTopicMetadata meta = getPartitionedTopicMetadata(topicName, true, false);
        int numPartitions = meta.partitions;
        if (numPartitions > 0) {
            for (int i = 0; i < numPartitions; i++) {
                TopicName topicNamePartition = topicName.getPartition(i);
                grantPermissions(topicNamePartition.toString(), role, actions);
            }
        }
        grantPermissions(topicName.toString(), role, actions);
    }

    protected void internalDeleteTopicForcefully(boolean authoritative, boolean deleteSchema) {
        validateTopicOwnership(topicName, authoritative);
        validateNamespaceOperation(topicName.getNamespaceObject(), NamespaceOperation.DELETE_TOPIC);

        try {
            pulsar().getBrokerService().deleteTopic(topicName.toString(), true, deleteSchema).get();
        } catch (Exception e) {
            if (e.getCause() instanceof MetadataNotFoundException) {
                log.info("[{}] Topic was already not existing {}", clientAppId(), topicName, e);
            } else {
                log.error("[{}] Failed to delete topic forcefully {}", clientAppId(), topicName, e);
                throw new RestException(e);
            }
        }
    }

    private void revokePermissions(String topicUri, String role) {
        Policies policies;
        try {
            policies = namespaceResources().get(path(POLICIES, namespaceName.toString()))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Namespace does not exist"));
        } catch (Exception e) {
            log.error("[{}] Failed to revoke permissions for topic {}", clientAppId(), topicUri, e);
            throw new RestException(e);
        }
        if (!policies.auth_policies.getTopicAuthentication().containsKey(topicUri)
                || !policies.auth_policies.getTopicAuthentication().get(topicUri).containsKey(role)) {
            log.warn("[{}] Failed to revoke permission from role {} on topic: Not set at topic level {}", clientAppId(),
                    role, topicUri);
            throw new RestException(Status.PRECONDITION_FAILED, "Permissions are not set at the topic level");
        }
        try {
            // Write the new policies to zookeeper
            String namespacePath = path(POLICIES, namespaceName.toString());
            namespaceResources().set(namespacePath, (p) -> {
                p.auth_policies.getTopicAuthentication().get(topicUri).remove(role);
                return p;
            });
            log.info("[{}] Successfully revoke access for role {} - topic {}", clientAppId(), role, topicUri);
        } catch (Exception e) {
            log.error("[{}] Failed to revoke permissions for topic {}", clientAppId(), topicUri, e);
            throw new RestException(e);
        }

    }

    protected void internalRevokePermissionsOnTopic(String role) {
        // This operation should be reading from zookeeper and it should be allowed without having admin privileges
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        PartitionedTopicMetadata meta = getPartitionedTopicMetadata(topicName, true, false);
        int numPartitions = meta.partitions;
        if (numPartitions > 0) {
            for (int i = 0; i < numPartitions; i++) {
                TopicName topicNamePartition = topicName.getPartition(i);
                revokePermissions(topicNamePartition.toString(), role);
            }
        }
        revokePermissions(topicName.toString(), role);
    }

    protected void internalCreateNonPartitionedTopic(boolean authoritative) {
        validateNonPartitionTopicName(topicName.getLocalName());
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        validateTopicOwnership(topicName, authoritative);
        validateNamespaceOperation(topicName.getNamespaceObject(), NamespaceOperation.CREATE_TOPIC);

        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(topicName, authoritative, false);
        if (partitionMetadata.partitions > 0) {
            log.warn("[{}] Partitioned topic with the same name already exists {}", clientAppId(), topicName);
            throw new RestException(Status.CONFLICT, "This topic already exists");
        }

        try {
            Optional<Topic> existedTopic = pulsar().getBrokerService().getTopicIfExists(topicName.toString()).get();
            if (existedTopic.isPresent()) {
                log.error("[{}] Topic {} already exists", clientAppId(), topicName);
                throw new RestException(Status.CONFLICT, "This topic already exists");
            }

            Topic createdTopic = getOrCreateTopic(topicName);
            log.info("[{}] Successfully created non-partitioned topic {}", clientAppId(), createdTopic);
        } catch (Exception e) {
            if (e instanceof RestException) {
                throw (RestException) e;
            } else {
                log.error("[{}] Failed to create non-partitioned topic {}", clientAppId(), topicName, e);
                throw new RestException(e);
            }
        }
    }

    /**
     * It updates number of partitions of an existing partitioned topic. It requires partitioned-topic to
     * already exist and number of new partitions must be greater than existing number of partitions. Decrementing
     * number of partitions requires deletion of topic which is not supported.
     *
     * Already created partitioned producers and consumers can't see newly created partitions and it requires to
     * recreate them at application so, newly created producers and consumers can connect to newly added partitions as
     * well. Therefore, it can violate partition ordering at producers until all producers are restarted at application.
     *
     * @param numPartitions
     */
    protected void internalUpdatePartitionedTopic(int numPartitions,
                                                  boolean updateLocalTopicOnly, boolean authoritative) {
        validateTopicOwnership(topicName, authoritative);
        validateTopicPolicyOperation(topicName, PolicyName.PARTITION, PolicyOperation.WRITE);

        // Only do the validation if it's the first hop.
        if (!updateLocalTopicOnly) {
            validatePartitionTopicUpdate(topicName.getLocalName(), numPartitions);
        }
        final int maxPartitions = pulsar().getConfig().getMaxNumPartitionsPerPartitionedTopic();
        if (maxPartitions > 0 && numPartitions > maxPartitions) {
            throw new RestException(Status.NOT_ACCEPTABLE,
                    "Number of partitions should be less than or equal to " + maxPartitions);
        }

        if (topicName.isGlobal() && isNamespaceReplicated(topicName.getNamespaceObject())) {
            Set<String> clusters = getNamespaceReplicatedClusters(topicName.getNamespaceObject());
            if (!clusters.contains(pulsar().getConfig().getClusterName())) {
                log.error("[{}] local cluster is not part of replicated cluster for namespace {}", clientAppId(),
                        topicName);
                throw new RestException(Status.FORBIDDEN, "Local cluster is not part of replicate cluster list");
            }
            try {
                tryCreatePartitionsAsync(numPartitions).get(DEFAULT_OPERATION_TIMEOUT_SEC, TimeUnit.SECONDS);
                createSubscriptions(topicName, numPartitions).get(DEFAULT_OPERATION_TIMEOUT_SEC, TimeUnit.SECONDS);
            } catch (Exception e) {
                if (e.getCause() instanceof RestException) {
                    throw (RestException) e.getCause();
                }
                log.error("[{}] Failed to update partitioned topic {}", clientAppId(), topicName, e);
                throw new RestException(e);
            }
            // if this cluster is the first hop which needs to coordinate with other clusters then update partitions in
            // other clusters and then update number of partitions.
            if (!updateLocalTopicOnly) {
                CompletableFuture<Void> updatePartition = new CompletableFuture<>();
                final String path = ZkAdminPaths.partitionedTopicPath(topicName);
                updatePartitionInOtherCluster(numPartitions, clusters).thenRun(() -> {
                    try {
                        namespaceResources().getPartitionedTopicResources().setAsync(path, (p) -> {
                            return new PartitionedTopicMetadata(numPartitions);
                        }).thenAccept(r -> updatePartition.complete(null)).exceptionally(ex -> {
                            updatePartition.completeExceptionally(ex.getCause());
                            return null;
                        });
                    } catch (Exception e) {
                        updatePartition.completeExceptionally(e);
                    }
                }).exceptionally(ex -> {
                    updatePartition.completeExceptionally(ex);
                    return null;
                });
                try {
                    updatePartition.get(DEFAULT_OPERATION_TIMEOUT_SEC, TimeUnit.SECONDS);
                } catch (Exception e) {
                    log.error("{} Failed to update number of partitions in zk for topic {} and partitions {}",
                            clientAppId(), topicName, numPartitions, e);
                    if (e.getCause() instanceof RestException) {
                        throw (RestException) e.getCause();
                    }
                    throw new RestException(e);
                }
            }
            return;
        }

        if (numPartitions <= 0) {
            throw new RestException(Status.NOT_ACCEPTABLE, "Number of partitions should be more than 0");
        }
        try {
            tryCreatePartitionsAsync(numPartitions).get(DEFAULT_OPERATION_TIMEOUT_SEC, TimeUnit.SECONDS);
            updatePartitionedTopic(topicName, numPartitions).get(DEFAULT_OPERATION_TIMEOUT_SEC, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (e.getCause() instanceof RestException) {
                throw (RestException) e.getCause();
            }
            log.error("[{}] Failed to update partitioned topic {}", clientAppId(), topicName, e);
            throw new RestException(e);
        }
    }

    protected void internalCreateMissedPartitions(AsyncResponse asyncResponse) {
        getPartitionedTopicMetadataAsync(topicName, false, false).thenAccept(metadata -> {
            if (metadata != null) {
                tryCreatePartitionsAsync(metadata.partitions).thenAccept(v -> {
                    asyncResponse.resume(Response.noContent().build());
                }).exceptionally(e -> {
                    log.error("[{}] Failed to create partitions for topic {}", clientAppId(), topicName);
                    resumeAsyncResponseExceptionally(asyncResponse, e);
                    return null;
                });
            }
        }).exceptionally(e -> {
            log.error("[{}] Failed to create partitions for topic {}",
                    clientAppId(), topicName);
            resumeAsyncResponseExceptionally(asyncResponse, e);
            return null;
        });
    }

    protected void internalSetDelayedDeliveryPolicies(AsyncResponse asyncResponse,
                                                      DelayedDeliveryPolicies deliveryPolicies) {
        TopicPolicies topicPolicies = null;
        try {
            topicPolicies = pulsar().getTopicPoliciesService().getTopicPolicies(topicName);
        } catch (BrokerServiceException.TopicPoliciesCacheNotInitException e) {
            log.error("Topic {} policies have not been initialized yet.", topicName);
            asyncResponse.resume(new RestException(e));
            return;
        }
        if (topicPolicies == null) {
            topicPolicies = new TopicPolicies();
        }
        topicPolicies.setDelayedDeliveryEnabled(deliveryPolicies == null ? null : deliveryPolicies.isActive());
        topicPolicies.setDelayedDeliveryTickTimeMillis(
                deliveryPolicies == null ? null : deliveryPolicies.getTickTime());
        pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed set delayed delivery policy for topic", ex);
                        asyncResponse.resume(new RestException(ex));
                    } else {
                        asyncResponse.resume(Response.noContent().build());
                    }
                });
    }

    private CompletableFuture<Void> updatePartitionInOtherCluster(int numPartitions, Set<String> clusters) {
        List<CompletableFuture<Void>> results = new ArrayList<>(clusters.size() - 1);
        clusters.forEach(cluster -> {
            if (cluster.equals(pulsar().getConfig().getClusterName())) {
                return;
            }
            results.add(pulsar().getBrokerService().getClusterPulsarAdmin(cluster).topics()
                    .updatePartitionedTopicAsync(topicName.toString(),
                            numPartitions, true));
        });
        return FutureUtil.waitForAll(results);
    }

    protected PartitionedTopicMetadata internalGetPartitionedMetadata(boolean authoritative,
                                                                      boolean checkAllowAutoCreation) {
        PartitionedTopicMetadata metadata = getPartitionedTopicMetadata(topicName,
                authoritative, checkAllowAutoCreation);
        if (metadata.partitions == 0 && !checkAllowAutoCreation) {
            // The topic may be a non-partitioned topic, so check if it exists here.
            // However, when checkAllowAutoCreation is true, the client will create the topic if it doesn't exist.
            // In this case, `partitions == 0` means the automatically created topic is a non-partitioned topic so we
            // shouldn't check if the topic exists.
            try {
                if (!pulsar().getNamespaceService().checkTopicExists(topicName).get()) {
                    throw new RestException(Status.NOT_FOUND,
                            new PulsarClientException.NotFoundException("Topic not exist"));
                }
            } catch (InterruptedException | ExecutionException e) {
                log.error("Failed to check if topic '{}' exists", topicName, e);
                throw new RestException(Status.INTERNAL_SERVER_ERROR, "Failed to get topic metadata");
            }
        }
        if (metadata.partitions > 1) {
            validateClientVersion();
        }
        return metadata;
    }

    protected void internalDeletePartitionedTopic(AsyncResponse asyncResponse, boolean authoritative,
                                                  boolean force, boolean deleteSchema) {
        try {
            validateNamespaceOperation(topicName.getNamespaceObject(), NamespaceOperation.DELETE_TOPIC);
            validateTopicOwnership(topicName, authoritative);
        } catch (WebApplicationException wae) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed to delete partitioned topic {}, redirecting to other brokers.",
                        clientAppId(), topicName, wae);
            }
            resumeAsyncResponseExceptionally(asyncResponse, wae);
            return;
        } catch (Exception e) {
            log.error("[{}] Failed to delete partitioned topic {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
            return;
        }
        final CompletableFuture<Void> future = new CompletableFuture<>();
        pulsar().getBrokerService().fetchPartitionedTopicMetadataAsync(topicName).thenAccept(partitionMeta -> {
            final int numPartitions = partitionMeta.partitions;
            if (numPartitions > 0) {
                final AtomicInteger count = new AtomicInteger(numPartitions);
                if (deleteSchema) {
                    count.incrementAndGet();
                    pulsar().getBrokerService().deleteSchemaStorage(topicName.getPartition(0).toString())
                            .whenComplete((r, ex) -> {
                                if (ex != null) {
                                    log.warn("Failed to delete schema storage of topic: {}", topicName);
                                }
                                if (count.decrementAndGet() == 0) {
                                    future.complete(null);
                                }
                            });
                }
                for (int i = 0; i < numPartitions; i++) {
                    TopicName topicNamePartition = topicName.getPartition(i);
                    try {
                        pulsar().getAdminClient().topics()
                                .deleteAsync(topicNamePartition.toString(), force)
                                .whenComplete((r, ex) -> {
                                    if (ex != null) {
                                        if (ex instanceof NotFoundException) {
                                            // if the sub-topic is not found, the client might not have called create
                                            // producer or it might have been deleted earlier,
                                            //so we ignore the 404 error.
                                            // For all other exception,
                                            //we fail the delete partition method even if a single
                                            // partition is failed to be deleted
                                            if (log.isDebugEnabled()) {
                                                log.debug("[{}] Partition not found: {}", clientAppId(),
                                                        topicNamePartition);
                                            }
                                        } else {
                                            log.error("[{}] Failed to delete partition {}", clientAppId(),
                                                    topicNamePartition, ex);
                                            future.completeExceptionally(ex);
                                            return;
                                        }
                                    } else {
                                        log.info("[{}] Deleted partition {}", clientAppId(), topicNamePartition);
                                    }
                                    if (count.decrementAndGet() == 0) {
                                        future.complete(null);
                                    }
                                });
                    } catch (Exception e) {
                        log.error("[{}] Failed to delete partition {}", clientAppId(), topicNamePartition, e);
                        future.completeExceptionally(e);
                    }
                }
            } else {
                future.complete(null);
            }
        }).exceptionally(ex -> {
            future.completeExceptionally(ex);
            return null;
        });

        future.whenComplete((r, ex) -> {
            if (ex != null) {
                if (ex instanceof PreconditionFailedException) {
                    asyncResponse.resume(
                            new RestException(Status.PRECONDITION_FAILED, "Topic has active producers/subscriptions"));
                    return;
                } else if (ex instanceof PulsarAdminException) {
                    asyncResponse.resume(new RestException((PulsarAdminException) ex));
                    return;
                } else if (ex instanceof WebApplicationException) {
                    asyncResponse.resume(ex);
                    return;
                } else {
                    asyncResponse.resume(new RestException(ex));
                    return;
                }
            }
            // Only tries to delete the znode for partitioned topic when all its partitions are successfully deleted
            String path = path(PARTITIONED_TOPIC_PATH_ZNODE, namespaceName.toString(), domain(),
                    topicName.getEncodedLocalName());
            try {
                namespaceResources().getPartitionedTopicResources().deleteAsync(path).thenAccept(r2 -> {
                    log.info("[{}] Deleted partitioned topic {}", clientAppId(), topicName);
                    asyncResponse.resume(Response.noContent().build());
                }).exceptionally(ex1 -> {
                    log.error("[{}] Failed to delete partitioned topic {}", clientAppId(), topicName, ex1.getCause());
                    if (ex1.getCause()
                            instanceof org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException) {
                        asyncResponse.resume(new RestException(
                                new RestException(Status.NOT_FOUND, "Partitioned topic does not exist")));
                    } else if (ex1
                            .getCause()
                            instanceof org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException) {
                        asyncResponse.resume(
                                new RestException(new RestException(Status.CONFLICT, "Concurrent modification")));
                    } else {
                        asyncResponse.resume(new RestException((ex1.getCause())));
                    }
                    return null;
                });
            } catch (Exception e1) {
                log.error("[{}] Failed to delete partitioned topic {}", clientAppId(), topicName, e1);
                asyncResponse.resume(new RestException(e1));
            }
        });
    }

    protected void internalUnloadTopic(AsyncResponse asyncResponse, boolean authoritative) {
        log.info("[{}] Unloading topic {}", clientAppId(), topicName);
        try {
            if (topicName.isGlobal()) {
                validateGlobalNamespaceOwnership(namespaceName);
            }
        } catch (Exception e) {
            log.error("[{}] Failed to unload topic {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
            return;
        }
        // If the topic name is a partition name, no need to get partition topic metadata again
        if (topicName.isPartitioned()) {
            internalUnloadNonPartitionedTopic(asyncResponse, authoritative);
        } else {
            getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                    .thenAccept(meta -> {
                        if (meta.partitions > 0) {
                            final List<CompletableFuture<Void>> futures = Lists.newArrayList();

                            for (int i = 0; i < meta.partitions; i++) {
                                TopicName topicNamePartition = topicName.getPartition(i);
                                try {
                                    futures.add(pulsar().getAdminClient().topics().unloadAsync(
                                            topicNamePartition.toString()));
                                } catch (Exception e) {
                                    log.error("[{}] Failed to unload topic {}", clientAppId(), topicNamePartition, e);
                                    asyncResponse.resume(new RestException(e));
                                    return;
                                }
                            }

                            FutureUtil.waitForAll(futures).handle((result, exception) -> {
                                if (exception != null) {
                                    Throwable th = exception.getCause();
                                    if (th instanceof NotFoundException) {
                                        asyncResponse.resume(new RestException(Status.NOT_FOUND, th.getMessage()));
                                    } else if (th instanceof WebApplicationException) {
                                        asyncResponse.resume(th);
                                    } else {
                                        log.error("[{}] Failed to unload topic {}", clientAppId(), topicName,
                                                exception);
                                        asyncResponse.resume(new RestException(exception));
                                    }
                                } else {
                                    asyncResponse.resume(Response.noContent().build());
                                }
                                return null;
                            });
                        } else {
                            internalUnloadNonPartitionedTopic(asyncResponse, authoritative);
                        }
                    }).exceptionally(t -> {
                log.error("[{}] Failed to unload topic {}", clientAppId(), topicName, t);
                if (t instanceof WebApplicationException) {
                    asyncResponse.resume(t);
                } else {
                    asyncResponse.resume(new RestException(t));
                }
                return null;
            });
        }
    }

    protected CompletableFuture<DelayedDeliveryPolicies> internalGetDelayedDeliveryPolicies(boolean applied) {
        TopicPolicies policies = getTopicPolicies(topicName).orElseGet(TopicPolicies::new);
        DelayedDeliveryPolicies delayedDeliveryPolicies = null;
        if (policies.isDelayedDeliveryEnabledSet() && policies.isDelayedDeliveryTickTimeMillisSet()) {
            delayedDeliveryPolicies = DelayedDeliveryPolicies.builder()
                    .tickTime(policies.getDelayedDeliveryTickTimeMillis())
                    .active(policies.getDelayedDeliveryEnabled())
                    .build();
        }
        if (delayedDeliveryPolicies == null && applied) {
            delayedDeliveryPolicies = getNamespacePolicies(namespaceName).delayed_delivery_policies;
            if (delayedDeliveryPolicies == null) {
                delayedDeliveryPolicies = DelayedDeliveryPolicies.builder()
                        .tickTime(pulsar().getConfiguration().getDelayedDeliveryTickTimeMillis())
                        .active(pulsar().getConfiguration().isDelayedDeliveryEnabled())
                        .build();
            }
        }
        return CompletableFuture.completedFuture(delayedDeliveryPolicies);
    }

    protected CompletableFuture<OffloadPoliciesImpl> internalGetOffloadPolicies(boolean applied) {
        OffloadPoliciesImpl offloadPolicies =
                getTopicPolicies(topicName).map(TopicPolicies::getOffloadPolicies).orElse(null);
        if (applied) {
            OffloadPoliciesImpl namespacePolicy =
                    (OffloadPoliciesImpl) getNamespacePolicies(namespaceName).offload_policies;
            offloadPolicies = OffloadPoliciesImpl.mergeConfiguration(offloadPolicies
                    , namespacePolicy, pulsar().getConfiguration().getProperties());
        }
        return CompletableFuture.completedFuture(offloadPolicies);
    }

    protected CompletableFuture<Void> internalSetOffloadPolicies(OffloadPoliciesImpl offloadPolicies) {
        TopicPolicies topicPolicies = null;
        try {
            topicPolicies = pulsar().getTopicPoliciesService().getTopicPolicies(topicName);
        } catch (BrokerServiceException.TopicPoliciesCacheNotInitException e) {
            log.error("Topic {} policies have not been initialized yet.", topicName);
            throw new RestException(Status.PRECONDITION_FAILED, "Policies have not been initialized yet");
        }
        if (topicPolicies == null) {
            topicPolicies = new TopicPolicies();
        }
        topicPolicies.setOffloadPolicies(offloadPolicies);
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies)
                .thenCompose((res) -> {
                    //The policy update is asynchronous. Cache at this step may not be updated yet.
                    //So we need to set the loader by the incoming offloadPolicies instead of topic policies cache.
                    PartitionedTopicMetadata metadata = fetchPartitionedTopicMetadata(pulsar(), topicName);
                    if (metadata.partitions > 0) {
                        List<CompletableFuture<Void>> futures = new ArrayList<>(metadata.partitions);
                        for (int i = 0; i < metadata.partitions; i++) {
                            futures.add(internalUpdateOffloadPolicies(offloadPolicies, topicName.getPartition(i)));
                        }
                        return FutureUtil.waitForAll(futures);
                    } else {
                        return internalUpdateOffloadPolicies(offloadPolicies, topicName);
                    }
                })
                .whenComplete((result, e) -> {
                    if (e != null) {
                        completableFuture.completeExceptionally(e);
                    } else {
                        completableFuture.complete(null);
                    }
                });
        return completableFuture;
    }

    protected CompletableFuture<InactiveTopicPolicies> internalGetInactiveTopicPolicies(boolean applied) {
        InactiveTopicPolicies inactiveTopicPolicies = getTopicPolicies(topicName)
                .map(TopicPolicies::getInactiveTopicPolicies)
                .orElseGet(() -> {
                    if (applied) {
                        InactiveTopicPolicies policies = getNamespacePolicies(namespaceName).inactive_topic_policies;
                        return policies == null ? new InactiveTopicPolicies(
                                config().getBrokerDeleteInactiveTopicsMode(),
                                config().getBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds(),
                                config().isBrokerDeleteInactiveTopicsEnabled()) : policies;
                    }
                    return null;
                });
        return CompletableFuture.completedFuture(inactiveTopicPolicies);
    }

    protected CompletableFuture<Void> internalSetInactiveTopicPolicies(InactiveTopicPolicies inactiveTopicPolicies) {
        TopicPolicies topicPolicies = null;
        try {
            topicPolicies = pulsar().getTopicPoliciesService().getTopicPolicies(topicName);
        } catch (BrokerServiceException.TopicPoliciesCacheNotInitException e) {
            log.error("Topic {} policies have not been initialized yet.", topicName);
            return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                    "Policies have not been initialized yet"));
        }
        if (topicPolicies == null) {
            topicPolicies = new TopicPolicies();
        }
        topicPolicies.setInactiveTopicPolicies(inactiveTopicPolicies);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
    }

    private CompletableFuture<Void> internalUpdateOffloadPolicies(OffloadPoliciesImpl offloadPolicies,
                                                                  TopicName topicName) {
        return pulsar().getBrokerService().getTopicIfExists(topicName.toString())
                .thenAccept(optionalTopic -> {
                    try {
                        if (!optionalTopic.isPresent() || !topicName.isPersistent()) {
                            return;
                        }
                        PersistentTopic persistentTopic = (PersistentTopic) optionalTopic.get();
                        ManagedLedgerConfig managedLedgerConfig = persistentTopic.getManagedLedger().getConfig();
                        if (offloadPolicies == null) {
                            LedgerOffloader namespaceOffloader =
                                    pulsar().getLedgerOffloaderMap().get(topicName.getNamespaceObject());
                            LedgerOffloader topicOffloader = managedLedgerConfig.getLedgerOffloader();
                            if (topicOffloader != null && topicOffloader != namespaceOffloader) {
                                topicOffloader.close();
                            }
                            managedLedgerConfig.setLedgerOffloader(namespaceOffloader);
                        } else {
                            managedLedgerConfig.setLedgerOffloader(
                                    pulsar().createManagedLedgerOffloader(offloadPolicies));
                        }
                        persistentTopic.getManagedLedger().setConfig(managedLedgerConfig);
                    } catch (PulsarServerException e) {
                        throw new RestException(e);
                    }
                });
    }

    protected CompletableFuture<Integer> internalGetMaxUnackedMessagesOnSubscription(boolean applied) {
        Integer maxNum = getTopicPolicies(topicName)
                .map(TopicPolicies::getMaxUnackedMessagesOnSubscription)
                .orElseGet(() -> {
                    if (applied) {
                        Integer maxUnackedNum = getNamespacePolicies(namespaceName)
                                .max_unacked_messages_per_subscription;
                        return maxUnackedNum == null ? config().getMaxUnackedMessagesPerSubscription() : maxUnackedNum;
                    }
                    return null;
                });
        return CompletableFuture.completedFuture(maxNum);
    }

    protected CompletableFuture<Void> internalSetMaxUnackedMessagesOnSubscription(Integer maxUnackedNum) {
        if (maxUnackedNum != null && maxUnackedNum < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxUnackedNum must be 0 or more");
        }

        TopicPolicies topicPolicies = null;
        try {
            topicPolicies = pulsar().getTopicPoliciesService().getTopicPolicies(topicName);
        } catch (BrokerServiceException.TopicPoliciesCacheNotInitException e) {
            log.error("Topic {} policies have not been initialized yet.", topicName);
            throw new RestException(Status.PRECONDITION_FAILED, "Policies have not been initialized yet");
        }
        if (topicPolicies == null) {
            topicPolicies = new TopicPolicies();
        }
        topicPolicies.setMaxUnackedMessagesOnSubscription(maxUnackedNum);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
    }

    protected CompletableFuture<Integer> internalGetMaxUnackedMessagesOnConsumer(boolean applied) {
        Integer maxNum = getTopicPolicies(topicName)
                .map(TopicPolicies::getMaxUnackedMessagesOnConsumer)
                .orElseGet(() -> {
                    if (applied) {
                        Integer maxUnacked = getNamespacePolicies(namespaceName).max_unacked_messages_per_consumer;
                        return maxUnacked == null ? config().getMaxUnackedMessagesPerConsumer() : maxUnacked;
                    }
                    return null;
                });
        return CompletableFuture.completedFuture(maxNum);
    }

    protected CompletableFuture<Void> internalSetMaxUnackedMessagesOnConsumer(Integer maxUnackedNum) {
        if (maxUnackedNum != null && maxUnackedNum < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxUnackedNum must be 0 or more");
        }

        TopicPolicies topicPolicies = null;
        try {
            topicPolicies = pulsar().getTopicPoliciesService().getTopicPolicies(topicName);
        } catch (BrokerServiceException.TopicPoliciesCacheNotInitException e) {
            log.error("Topic {} policies have not been initialized yet.", topicName);
            return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                    "Policies have not been initialized yet"));
        }
        if (topicPolicies == null) {
            topicPolicies = new TopicPolicies();
        }
        topicPolicies.setMaxUnackedMessagesOnConsumer(maxUnackedNum);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
    }

    protected CompletableFuture<Void> internalSetDeduplicationSnapshotInterval(Integer interval) {
        if (interval != null && interval < 0) {
            throw new RestException(Status.PRECONDITION_FAILED, "interval must be 0 or more");
        }
        TopicPolicies policies = getTopicPolicies(topicName).orElseGet(TopicPolicies::new);
        policies.setDeduplicationSnapshotIntervalSeconds(interval);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, policies);

    }

    private void internalUnloadNonPartitionedTopic(AsyncResponse asyncResponse, boolean authoritative) {
        try {
            validateTopicOperation(topicName, TopicOperation.UNLOAD);
        } catch (Exception e) {
            log.error("[{}] Failed to unload topic {}", clientAppId(), topicName, e.getMessage());
            resumeAsyncResponseExceptionally(asyncResponse, e);
            return;
        }

        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenCompose(topic -> topic.close(false))
                .thenRun(() -> {
                    log.info("[{}] Successfully unloaded topic {}", clientAppId(), topicName);
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    log.error("[{}] Failed to unload topic {}, {}", clientAppId(), topicName, ex.getMessage());
                    asyncResponse.resume(ex.getCause());
                    return null;
                });
    }

    protected void internalDeleteTopic(boolean authoritative, boolean force, boolean deleteSchema) {
        if (force) {
            internalDeleteTopicForcefully(authoritative, deleteSchema);
        } else {
            internalDeleteTopic(authoritative, deleteSchema);
        }
    }

    protected void internalDeleteTopic(boolean authoritative, boolean deleteSchema) {
        validateNamespaceOperation(topicName.getNamespaceObject(), NamespaceOperation.DELETE_TOPIC);
        validateTopicOwnership(topicName, authoritative);

        try {
            pulsar().getBrokerService().deleteTopic(topicName.toString(), false, deleteSchema).get();
            log.info("[{}] Successfully removed topic {}", clientAppId(), topicName);
        } catch (Exception e) {
            Throwable t = e.getCause();
            log.error("[{}] Failed to delete topic {}", clientAppId(), topicName, t);
            if (t instanceof TopicBusyException) {
                throw new RestException(Status.PRECONDITION_FAILED, "Topic has active producers/subscriptions");
            } else if (t instanceof MetadataNotFoundException) {
                throw new RestException(Status.NOT_FOUND, "Topic not found");
            } else {
                throw new RestException(t);
            }
        }
    }

    protected void internalGetSubscriptions(AsyncResponse asyncResponse, boolean authoritative) {
        if (topicName.isGlobal()) {
            try {
                validateGlobalNamespaceOwnership(namespaceName);
            } catch (Exception e) {
                log.error("[{}] Failed to get subscriptions for topic {}", clientAppId(), topicName, e);
                resumeAsyncResponseExceptionally(asyncResponse, e);
                return;
            }
        }

        validateTopicOwnership(topicName, authoritative);

        // If the topic name is a partition name, no need to get partition topic metadata again
        if (topicName.isPartitioned()) {
            internalGetSubscriptionsForNonPartitionedTopic(asyncResponse, authoritative);
        } else {
            getPartitionedTopicMetadataAsync(topicName, authoritative,
                    false).thenAccept(partitionMetadata -> {
                if (partitionMetadata.partitions > 0) {
                    try {
                        // get the subscriptions only from the 1st partition
                        // since all the other partitions will have the same
                        // subscriptions
                        pulsar().getAdminClient().topics().getSubscriptionsAsync(topicName.getPartition(0).toString())
                                .whenComplete((r, ex) -> {
                                    if (ex != null) {
                                        log.warn("[{}] Failed to get list of subscriptions for {}: {}", clientAppId(),
                                                topicName, ex.getMessage());

                                        if (ex instanceof PulsarAdminException) {
                                            PulsarAdminException pae = (PulsarAdminException) ex;
                                            if (pae.getStatusCode() == Status.NOT_FOUND.getStatusCode()) {
                                                asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                                        "Internal topics have not been generated yet"));
                                                return;
                                            } else {
                                                asyncResponse.resume(new RestException(pae));
                                                return;
                                            }
                                        } else {
                                            asyncResponse.resume(new RestException(ex));
                                            return;
                                        }
                                    }
                                    final List<String> subscriptions = Lists.newArrayList();
                                    subscriptions.addAll(r);
                                    asyncResponse.resume(subscriptions);
                                });
                    } catch (Exception e) {
                        log.error("[{}] Failed to get list of subscriptions for {}", clientAppId(), topicName, e);
                        asyncResponse.resume(e);
                    }
                } else {
                    internalGetSubscriptionsForNonPartitionedTopic(asyncResponse, authoritative);
                }
            }).exceptionally(ex -> {
                log.error("[{}] Failed to get subscriptions for topic {}", clientAppId(), topicName, ex);
                resumeAsyncResponseExceptionally(asyncResponse, ex);
                return null;
            });
        }
    }

    private void internalGetSubscriptionsForNonPartitionedTopic(AsyncResponse asyncResponse, boolean authoritative) {
        try {
            validateTopicOwnership(topicName, authoritative);
            validateTopicOperation(topicName, TopicOperation.GET_SUBSCRIPTIONS);

            Topic topic = getTopicReference(topicName);
            final List<String> subscriptions = Lists.newArrayList();
            topic.getSubscriptions().forEach((subName, sub) -> subscriptions.add(subName));
            asyncResponse.resume(subscriptions);
        } catch (WebApplicationException wae) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed to get subscriptions for non-partitioned topic {},"
                                + " redirecting to other brokers.",
                        clientAppId(), topicName, wae);
            }
            resumeAsyncResponseExceptionally(asyncResponse, wae);
            return;
        } catch (Exception e) {
            log.error("[{}] Failed to get list of subscriptions for {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    protected TopicStats internalGetStats(boolean authoritative, boolean getPreciseBacklog,
                                          boolean subscriptionBacklogSize) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        validateTopicOwnership(topicName, authoritative);
        validateTopicOperation(topicName, TopicOperation.GET_STATS);

        Topic topic = getTopicReference(topicName);
        return topic.getStats(getPreciseBacklog, subscriptionBacklogSize);
    }

    protected PersistentTopicInternalStats internalGetInternalStats(boolean authoritative, boolean metadata) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        validateTopicOwnership(topicName, authoritative);
        validateTopicOperation(topicName, TopicOperation.GET_STATS);

        Topic topic = getTopicReference(topicName);
        try {
            if (metadata) {
                validateTopicOperation(topicName, TopicOperation.GET_METADATA);
            }
            return topic.getInternalStats(metadata).get();
        } catch (Exception e) {
            log.error("[{}] Failed to get internal stats for {}", clientAppId(), topicName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR,
                    (e instanceof ExecutionException) ? e.getCause().getMessage() : e.getMessage());
        }
    }

    protected void internalGetManagedLedgerInfo(AsyncResponse asyncResponse, boolean authoritative) {
        if (topicName.isGlobal()) {
            try {
                validateGlobalNamespaceOwnership(namespaceName);
            } catch (Exception e) {
                log.error("[{}] Failed to get managed info for {}", clientAppId(), topicName, e);
                resumeAsyncResponseExceptionally(asyncResponse, e);
                return;
            }
        }

        // If the topic name is a partition name, no need to get partition topic metadata again
        if (topicName.isPartitioned()) {
            internalGetManagedLedgerInfoForNonPartitionedTopic(asyncResponse);
        } else {
            getPartitionedTopicMetadataAsync(topicName,
                    authoritative, false).thenAccept(partitionMetadata -> {
                if (partitionMetadata.partitions > 0) {
                    final List<CompletableFuture<String>> futures = Lists.newArrayList();

                    PartitionedManagedLedgerInfo partitionedManagedLedgerInfo = new PartitionedManagedLedgerInfo();

                    for (int i = 0; i < partitionMetadata.partitions; i++) {
                        TopicName topicNamePartition = topicName.getPartition(i);
                        try {
                            futures.add(pulsar().getAdminClient().topics()
                                    .getInternalInfoAsync(
                                            topicNamePartition.toString()).whenComplete((response, throwable) -> {
                                        if (throwable != null) {
                                            log.error("[{}] Failed to get managed info for {}",
                                                    clientAppId(), topicNamePartition, throwable);
                                            asyncResponse.resume(new RestException(throwable));
                                        }
                                        try {
                                            partitionedManagedLedgerInfo.partitions.put(topicNamePartition.toString(),
                                                    jsonMapper().readValue(response,
                                                            ManagedLedgerInfo.class));
                                        } catch (JsonProcessingException ex) {
                                            log.error("[{}] Failed to parse ManagedLedgerInfo for {} from [{}]",
                                                    clientAppId(),
                                                    topicNamePartition, response, ex);
                                        }
                                    }));
                        } catch (Exception e) {
                            log.error("[{}] Failed to get managed info for {}", clientAppId(), topicNamePartition, e);
                            throw new RestException(e);
                        }
                    }

                    FutureUtil.waitForAll(futures).handle((result, exception) -> {
                        if (exception != null) {
                            Throwable t = exception.getCause();
                            if (t instanceof NotFoundException) {
                                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Topic not found"));
                            } else {
                                log.error("[{}] Failed to get managed info for {}", clientAppId(), topicName, t);
                                asyncResponse.resume(new RestException(t));
                            }
                        }
                        asyncResponse.resume((StreamingOutput) output -> {
                            jsonMapper().writer().writeValue(output, partitionedManagedLedgerInfo);
                        });
                        return null;
                    });
                } else {
                    internalGetManagedLedgerInfoForNonPartitionedTopic(asyncResponse);
                }
            }).exceptionally(ex -> {
                log.error("[{}] Failed to get managed info for {}", clientAppId(), topicName, ex);
                resumeAsyncResponseExceptionally(asyncResponse, ex);
                return null;
            });
        }
    }

    protected void internalGetManagedLedgerInfoForNonPartitionedTopic(AsyncResponse asyncResponse) {
        String managedLedger;
        try {
            validateTopicOperation(topicName, TopicOperation.GET_STATS);
            managedLedger = topicName.getPersistenceNamingEncoding();
        } catch (Exception e) {
            log.error("[{}] Failed to get managed info for {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
            return;
        }
        pulsar().getManagedLedgerFactory().asyncGetManagedLedgerInfo(managedLedger, new ManagedLedgerInfoCallback() {
            @Override
            public void getInfoComplete(ManagedLedgerInfo info, Object ctx) {
                asyncResponse.resume((StreamingOutput) output -> {
                    jsonMapper().writer().writeValue(output, info);
                });
            }

            @Override
            public void getInfoFailed(ManagedLedgerException exception, Object ctx) {
                asyncResponse.resume(exception);
            }
        }, null);
    }

    protected void internalGetPartitionedStats(AsyncResponse asyncResponse, boolean authoritative,
            boolean perPartition, boolean getPreciseBacklog, boolean subscriptionBacklogSize) {
        if (topicName.isGlobal()) {
            try {
                validateGlobalNamespaceOwnership(namespaceName);
            } catch (Exception e) {
                log.error("[{}] Failed to get partitioned stats for {}", clientAppId(), topicName, e);
                resumeAsyncResponseExceptionally(asyncResponse, e);
                return;
            }
        }
        getPartitionedTopicMetadataAsync(topicName,
                authoritative, false).thenAccept(partitionMetadata -> {
            if (partitionMetadata.partitions == 0) {
                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Partitioned Topic not found"));
                return;
            }
            PartitionedTopicStatsImpl stats = new PartitionedTopicStatsImpl(partitionMetadata);
            List<CompletableFuture<TopicStats>> topicStatsFutureList = Lists.newArrayList();
            for (int i = 0; i < partitionMetadata.partitions; i++) {
                try {
                    topicStatsFutureList
                            .add(pulsar().getAdminClient().topics().getStatsAsync(
                                    (topicName.getPartition(i).toString()), getPreciseBacklog,
                                    subscriptionBacklogSize));
                } catch (PulsarServerException e) {
                    asyncResponse.resume(new RestException(e));
                    return;
                }
            }

            FutureUtil.waitForAll(topicStatsFutureList).handle((result, exception) -> {
                CompletableFuture<TopicStats> statFuture = null;
                for (int i = 0; i < topicStatsFutureList.size(); i++) {
                    statFuture = topicStatsFutureList.get(i);
                    if (statFuture.isDone() && !statFuture.isCompletedExceptionally()) {
                        try {
                            stats.add(statFuture.get());
                            if (perPartition) {
                                stats.partitions.put(topicName.getPartition(i).toString(), statFuture.get());
                            }
                        } catch (Exception e) {
                            asyncResponse.resume(new RestException(e));
                            return null;
                        }
                    }
                }
                if (perPartition && stats.partitions.isEmpty()) {
                    String path = ZkAdminPaths.partitionedTopicPath(topicName);
                    try {
                        boolean zkPathExists = namespaceResources().getPartitionedTopicResources().exists(path);
                        if (zkPathExists) {
                            stats.partitions.put(topicName.toString(), new TopicStatsImpl());
                        } else {
                            asyncResponse.resume(
                                    new RestException(Status.NOT_FOUND,
                                            "Internal topics have not been generated yet"));
                            return null;
                        }
                    } catch (Exception e) {
                        asyncResponse.resume(new RestException(e));
                        return null;
                    }
                }
                asyncResponse.resume(stats);
                return null;
            });
        }).exceptionally(ex -> {
            log.error("[{}] Failed to get partitioned stats for {}", clientAppId(), topicName, ex);
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    protected void internalGetPartitionedStatsInternal(AsyncResponse asyncResponse, boolean authoritative) {
        if (topicName.isGlobal()) {
            try {
                validateGlobalNamespaceOwnership(namespaceName);
            } catch (Exception e) {
                log.error("[{}] Failed to get partitioned internal stats for {}", clientAppId(), topicName, e);
                resumeAsyncResponseExceptionally(asyncResponse, e);
                return;
            }
        }
        getPartitionedTopicMetadataAsync(topicName,
                authoritative, false).thenAccept(partitionMetadata -> {
            if (partitionMetadata.partitions == 0) {
                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Partitioned Topic not found"));
                return;
            }

            PartitionedTopicInternalStats stats = new PartitionedTopicInternalStats(partitionMetadata);

            List<CompletableFuture<PersistentTopicInternalStats>> topicStatsFutureList = Lists.newArrayList();
            for (int i = 0; i < partitionMetadata.partitions; i++) {
                try {
                    topicStatsFutureList.add(pulsar().getAdminClient().topics()
                            .getInternalStatsAsync((topicName.getPartition(i).toString()), false));
                } catch (PulsarServerException e) {
                    asyncResponse.resume(new RestException(e));
                    return;
                }
            }

            FutureUtil.waitForAll(topicStatsFutureList).handle((result, exception) -> {
                CompletableFuture<PersistentTopicInternalStats> statFuture = null;
                for (int i = 0; i < topicStatsFutureList.size(); i++) {
                    statFuture = topicStatsFutureList.get(i);
                    if (statFuture.isDone() && !statFuture.isCompletedExceptionally()) {
                        try {
                            stats.partitions.put(topicName.getPartition(i).toString(), statFuture.get());
                        } catch (Exception e) {
                            asyncResponse.resume(new RestException(e));
                            return null;
                        }
                    }
                }
                asyncResponse.resume(!stats.partitions.isEmpty() ? stats
                        : new RestException(Status.NOT_FOUND, "Internal topics have not been generated yet"));
                return null;
            });
        }).exceptionally(ex -> {
            log.error("[{}] Failed to get partitioned internal stats for {}", clientAppId(), topicName, ex);
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    protected void internalDeleteSubscription(AsyncResponse asyncResponse,
                                              String subName, boolean authoritative, boolean force) {
        if (force) {
            internalDeleteSubscriptionForcefully(asyncResponse, subName, authoritative);
        } else {
            internalDeleteSubscription(asyncResponse, subName, authoritative);
        }
    }

    protected void internalDeleteSubscription(AsyncResponse asyncResponse, String subName, boolean authoritative) {
        if (topicName.isGlobal()) {
            try {
                validateGlobalNamespaceOwnership(namespaceName);
            } catch (Exception e) {
                log.error("[{}] Failed to delete subscription {} from topic {}", clientAppId(), subName, topicName, e);
                resumeAsyncResponseExceptionally(asyncResponse, e);
                return;
            }
        }
        validateTopicOwnership(topicName, authoritative);
        // If the topic name is a partition name, no need to get partition topic metadata again
        if (topicName.isPartitioned()) {
            internalDeleteSubscriptionForNonPartitionedTopic(asyncResponse, subName, authoritative);
        } else {
            getPartitionedTopicMetadataAsync(topicName,
                    authoritative, false).thenAccept(partitionMetadata -> {
                if (partitionMetadata.partitions > 0) {
                    final List<CompletableFuture<Void>> futures = Lists.newArrayList();

                    for (int i = 0; i < partitionMetadata.partitions; i++) {
                        TopicName topicNamePartition = topicName.getPartition(i);
                        try {
                            futures.add(pulsar().getAdminClient().topics()
                                    .deleteSubscriptionAsync(topicNamePartition.toString(), subName, false));
                        } catch (Exception e) {
                            log.error("[{}] Failed to delete subscription {} {}",
                                    clientAppId(), topicNamePartition, subName,
                                    e);
                            asyncResponse.resume(new RestException(e));
                            return;
                        }
                    }

                    FutureUtil.waitForAll(futures).handle((result, exception) -> {
                        if (exception != null) {
                            Throwable t = exception.getCause();
                            if (t instanceof NotFoundException) {
                                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Subscription not found"));
                                return null;
                            } else if (t instanceof PreconditionFailedException) {
                                asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                                        "Subscription has active connected consumers"));
                                return null;
                            } else {
                                log.error("[{}] Failed to delete subscription {} {}",
                                        clientAppId(), topicName, subName, t);
                                asyncResponse.resume(new RestException(t));
                                return null;
                            }
                        }

                        asyncResponse.resume(Response.noContent().build());
                        return null;
                    });
                } else {
                    internalDeleteSubscriptionForNonPartitionedTopic(asyncResponse, subName, authoritative);
                }
            }).exceptionally(ex -> {
                log.error("[{}] Failed to delete subscription {} from topic {}",
                        clientAppId(), subName, topicName, ex);
                resumeAsyncResponseExceptionally(asyncResponse, ex);
                return null;
            });
        }
    }

    private void internalDeleteSubscriptionForNonPartitionedTopic(AsyncResponse asyncResponse,
                                                                  String subName, boolean authoritative) {
        try {
            validateTopicOwnership(topicName, authoritative);
            validateTopicOperation(topicName, TopicOperation.UNSUBSCRIBE);

            Topic topic = getTopicReference(topicName);
            Subscription sub = topic.getSubscription(subName);
            if (sub == null) {
                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Subscription not found"));
                return;
            }
            sub.delete().get();
            log.info("[{}][{}] Deleted subscription {}", clientAppId(), topicName, subName);
            asyncResponse.resume(Response.noContent().build());
        } catch (Exception e) {
            if (e.getCause() instanceof SubscriptionBusyException) {
                log.error("[{}] Failed to delete subscription {} from topic {}", clientAppId(), subName, topicName, e);
                asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                    "Subscription has active connected consumers"));
            } else if (e instanceof WebApplicationException) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Failed to delete subscription from topic {}, redirecting to other brokers.",
                            clientAppId(), topicName, e);
                }
                asyncResponse.resume(e);
            } else {
                log.error("[{}] Failed to delete subscription {} {}", clientAppId(), topicName, subName, e);
                asyncResponse.resume(new RestException(e));
            }
        }
    }

    protected void internalDeleteSubscriptionForcefully(AsyncResponse asyncResponse,
                                                        String subName, boolean authoritative) {
        if (topicName.isGlobal()) {
            try {
                validateGlobalNamespaceOwnership(namespaceName);
            } catch (Exception e) {
                log.error("[{}] Failed to delete subscription forcefully {} from topic {}",
                        clientAppId(), subName, topicName, e);
                resumeAsyncResponseExceptionally(asyncResponse, e);
                return;
            }
        }
        // If the topic name is a partition name, no need to get partition topic metadata again
        if (topicName.isPartitioned()) {
            internalDeleteSubscriptionForNonPartitionedTopicForcefully(asyncResponse, subName, authoritative);
        } else {
            getPartitionedTopicMetadataAsync(topicName,
                    authoritative, false).thenAccept(partitionMetadata -> {
                if (partitionMetadata.partitions > 0) {
                    final List<CompletableFuture<Void>> futures = Lists.newArrayList();

                    for (int i = 0; i < partitionMetadata.partitions; i++) {
                        TopicName topicNamePartition = topicName.getPartition(i);
                        try {
                            futures.add(pulsar().getAdminClient().topics()
                                    .deleteSubscriptionAsync(topicNamePartition.toString(), subName, true));
                        } catch (Exception e) {
                            log.error("[{}] Failed to delete subscription forcefully {} {}",
                                    clientAppId(), topicNamePartition, subName,
                                    e);
                            asyncResponse.resume(new RestException(e));
                            return;
                        }
                    }

                    FutureUtil.waitForAll(futures).handle((result, exception) -> {
                        if (exception != null) {
                            Throwable t = exception.getCause();
                            if (t instanceof NotFoundException) {
                                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Subscription not found"));
                                return null;
                            } else {
                                log.error("[{}] Failed to delete subscription forcefully {} {}",
                                        clientAppId(), topicName, subName, t);
                                asyncResponse.resume(new RestException(t));
                                return null;
                            }
                        }

                        asyncResponse.resume(Response.noContent().build());
                        return null;
                    });
                } else {
                    internalDeleteSubscriptionForNonPartitionedTopicForcefully(asyncResponse, subName, authoritative);
                }
            }).exceptionally(ex -> {
                log.error("[{}] Failed to delete subscription forcefully {} from topic {}",
                        clientAppId(), subName, topicName, ex);
                resumeAsyncResponseExceptionally(asyncResponse, ex);
                return null;
            });
        }
    }

    private void internalDeleteSubscriptionForNonPartitionedTopicForcefully(AsyncResponse asyncResponse,
                                                                            String subName, boolean authoritative) {
        try {
            validateTopicOwnership(topicName, authoritative);
            validateTopicOperation(topicName, TopicOperation.UNSUBSCRIBE);

            Topic topic = getTopicReference(topicName);
            Subscription sub = topic.getSubscription(subName);
            if (sub == null) {
                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Subscription not found"));
                return;
            }
            sub.deleteForcefully().get();
            log.info("[{}][{}] Deleted subscription forcefully {}", clientAppId(), topicName, subName);
            asyncResponse.resume(Response.noContent().build());
        } catch (Exception e) {
            if (e instanceof WebApplicationException) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Failed to delete subscription forcefully from topic {},"
                                    + " redirecting to other brokers.",
                            clientAppId(), topicName, e);
                }
                asyncResponse.resume(e);
            } else {
                log.error("[{}] Failed to delete subscription forcefully {} {}",
                        clientAppId(), topicName, subName, e);
                asyncResponse.resume(new RestException(e));
            }
        }
    }

    protected void internalSkipAllMessages(AsyncResponse asyncResponse, String subName, boolean authoritative) {
        if (topicName.isGlobal()) {
            try {
                validateGlobalNamespaceOwnership(namespaceName);
            } catch (Exception e) {
                log.error("[{}] Failed to skip all messages for subscription {} on topic {}",
                        clientAppId(), subName, topicName, e);
                resumeAsyncResponseExceptionally(asyncResponse, e);
                return;
            }
        }

        validateTopicOwnership(topicName, authoritative);
        validateTopicOperation(topicName, TopicOperation.SKIP, subName);

        // If the topic name is a partition name, no need to get partition topic metadata again
        if (topicName.isPartitioned()) {
            internalSkipAllMessagesForNonPartitionedTopic(asyncResponse, subName, authoritative);
        } else {
            getPartitionedTopicMetadataAsync(topicName,
                    authoritative, false).thenAccept(partitionMetadata -> {
                if (partitionMetadata.partitions > 0) {
                    final List<CompletableFuture<Void>> futures = Lists.newArrayList();

                    for (int i = 0; i < partitionMetadata.partitions; i++) {
                        TopicName topicNamePartition = topicName.getPartition(i);
                        try {
                            futures.add(pulsar()
                                    .getAdminClient()
                                    .topics()
                                    .skipAllMessagesAsync(topicNamePartition.toString(),
                                            subName));
                        } catch (Exception e) {
                            log.error("[{}] Failed to skip all messages {} {}",
                                    clientAppId(), topicNamePartition, subName, e);
                            asyncResponse.resume(new RestException(e));
                            return;
                        }
                    }

                    FutureUtil.waitForAll(futures).handle((result, exception) -> {
                        if (exception != null) {
                            Throwable t = exception.getCause();
                            if (t instanceof NotFoundException) {
                                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Subscription not found"));
                                return null;
                            } else {
                                log.error("[{}] Failed to skip all messages {} {}",
                                        clientAppId(), topicName, subName, t);
                                asyncResponse.resume(new RestException(t));
                                return null;
                            }
                        }

                        asyncResponse.resume(Response.noContent().build());
                        return null;
                    });
                } else {
                    internalSkipAllMessagesForNonPartitionedTopic(asyncResponse, subName, authoritative);
                }
            }).exceptionally(ex -> {
                log.error("[{}] Failed to skip all messages for subscription {} on topic {}",
                        clientAppId(), subName, topicName, ex);
                resumeAsyncResponseExceptionally(asyncResponse, ex);
                return null;
            });
        }
    }

    private void internalSkipAllMessagesForNonPartitionedTopic(AsyncResponse asyncResponse,
                                                               String subName, boolean authoritative) {
        try {
            validateTopicOwnership(topicName, authoritative);
            validateTopicOperation(topicName, TopicOperation.SKIP, subName);

            PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
            BiConsumer<Void, Throwable> biConsumer = (v, ex) -> {
                if (ex != null) {
                    asyncResponse.resume(new RestException(ex));
                    log.error("[{}] Failed to skip all messages {} {}", clientAppId(), topicName, subName, ex);
                } else {
                    asyncResponse.resume(Response.noContent().build());
                    log.info("[{}] Cleared backlog on {} {}", clientAppId(), topicName, subName);
                }
            };
            if (subName.startsWith(topic.getReplicatorPrefix())) {
                String remoteCluster = PersistentReplicator.getRemoteCluster(subName);
                PersistentReplicator repl = (PersistentReplicator) topic.getPersistentReplicator(remoteCluster);
                if (repl == null) {
                    asyncResponse.resume(new RestException(Status.NOT_FOUND, "Subscription not found"));
                    return;
                }
                repl.clearBacklog().whenComplete(biConsumer);
            } else {
                PersistentSubscription sub = topic.getSubscription(subName);
                if (sub == null) {
                    asyncResponse.resume(new RestException(Status.NOT_FOUND, "Subscription not found"));
                    return;
                }
                sub.clearBacklog().whenComplete(biConsumer);
            }
        } catch (WebApplicationException wae) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed to skip all messages for subscription on topic {},"
                                + " redirecting to other brokers.",
                        clientAppId(), topicName, wae);
            }
            resumeAsyncResponseExceptionally(asyncResponse, wae);
        } catch (Exception e) {
            log.error("[{}] Failed to skip all messages for subscription {} on topic {}",
                    clientAppId(), subName, topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    protected void internalSkipMessages(String subName, int numMessages, boolean authoritative) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(topicName,
                authoritative, false);
        if (partitionMetadata.partitions > 0) {
            throw new RestException(Status.METHOD_NOT_ALLOWED, "Skip messages on a partitioned topic is not allowed");
        }

        validateTopicOwnership(topicName, authoritative);
        validateTopicOperation(topicName, TopicOperation.SKIP);

        PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
        try {
            if (subName.startsWith(topic.getReplicatorPrefix())) {
                String remoteCluster = PersistentReplicator.getRemoteCluster(subName);
                PersistentReplicator repl = (PersistentReplicator) topic.getPersistentReplicator(remoteCluster);
                checkNotNull(repl);
                repl.skipMessages(numMessages).get();
            } else {
                PersistentSubscription sub = topic.getSubscription(subName);
                checkNotNull(sub);
                sub.skipMessages(numMessages).get();
            }
            log.info("[{}] Skipped {} messages on {} {}", clientAppId(), numMessages, topicName, subName);
        } catch (NullPointerException npe) {
            throw new RestException(Status.NOT_FOUND, "Subscription not found");
        } catch (Exception exception) {
            log.error("[{}] Failed to skip {} messages {} {}", clientAppId(), numMessages, topicName, subName,
                    exception);
            throw new RestException(exception);
        }
    }

    protected void internalExpireMessagesForAllSubscriptions(AsyncResponse asyncResponse, int expireTimeInSeconds,
            boolean authoritative) {
        if (topicName.isGlobal()) {
            try {
                validateGlobalNamespaceOwnership(namespaceName);
            } catch (Exception e) {
                log.error("[{}] Failed to expire messages for all subscription on topic {}",
                        clientAppId(), topicName, e);
                resumeAsyncResponseExceptionally(asyncResponse, e);
                return;
            }
        }
        // If the topic name is a partition name, no need to get partition topic metadata again
        if (topicName.isPartitioned()) {
            internalExpireMessagesForAllSubscriptionsForNonPartitionedTopic(asyncResponse,
                    expireTimeInSeconds, authoritative);
        } else {
            getPartitionedTopicMetadataAsync(topicName,
                    authoritative, false).thenAccept(partitionMetadata -> {
                if (partitionMetadata.partitions > 0) {
                    final List<CompletableFuture<Void>> futures = Lists.newArrayList();

                    // expire messages for each partition topic
                    for (int i = 0; i < partitionMetadata.partitions; i++) {
                        TopicName topicNamePartition = topicName.getPartition(i);
                        try {
                            futures.add(pulsar()
                                    .getAdminClient()
                                    .topics()
                                    .expireMessagesForAllSubscriptionsAsync(
                                            topicNamePartition.toString(), expireTimeInSeconds));
                        } catch (Exception e) {
                            log.error("[{}] Failed to expire messages up to {} on {}",
                                    clientAppId(), expireTimeInSeconds,
                                    topicNamePartition, e);
                            asyncResponse.resume(new RestException(e));
                            return;
                        }
                    }

                    FutureUtil.waitForAll(futures).handle((result, exception) -> {
                        if (exception != null) {
                            Throwable t = exception.getCause();
                            log.error("[{}] Failed to expire messages up to {} on {}",
                                    clientAppId(), expireTimeInSeconds,
                                    topicName, t);
                            asyncResponse.resume(new RestException(t));
                            return null;
                        }

                        asyncResponse.resume(Response.noContent().build());
                        return null;
                    });
                } else {
                    internalExpireMessagesForAllSubscriptionsForNonPartitionedTopic(asyncResponse,
                            expireTimeInSeconds, authoritative);
                }
            }).exceptionally(ex -> {
                log.error("[{}] Failed to expire messages for all subscription on topic {}",
                        clientAppId(), topicName, ex);
                resumeAsyncResponseExceptionally(asyncResponse, ex);
                return null;
            });
        }
    }

    private void internalExpireMessagesForAllSubscriptionsForNonPartitionedTopic(AsyncResponse asyncResponse,
                                                                                 int expireTimeInSeconds,
                                                                                 boolean authoritative) {
        // validate ownership and redirect if current broker is not owner
        PersistentTopic topic;
        try {
            validateTopicOwnership(topicName, authoritative);
            validateTopicOperation(topicName, TopicOperation.EXPIRE_MESSAGES);
            topic = (PersistentTopic) getTopicReference(topicName);
        } catch (WebApplicationException wae) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed to expire messages for all subscription on topic {},"
                                + " redirecting to other brokers.",
                        clientAppId(), topicName, wae);
            }
            resumeAsyncResponseExceptionally(asyncResponse, wae);
            return;
        } catch (Exception e) {
            log.error("[{}] Failed to expire messages for all subscription on topic {}",
                    clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
            return;
        }
        final AtomicReference<Throwable> exception = new AtomicReference<>();

        topic.getReplicators().forEach((subName, replicator) -> {
            try {
                internalExpireMessagesByTimestampForSinglePartition(subName, expireTimeInSeconds, authoritative);
            } catch (Throwable t) {
                exception.set(t);
            }
        });

        topic.getSubscriptions().forEach((subName, subscriber) -> {
            try {
                internalExpireMessagesByTimestampForSinglePartition(subName, expireTimeInSeconds, authoritative);
            } catch (Throwable t) {
                exception.set(t);
            }
        });

        if (exception.get() != null) {
            if (exception.get() instanceof WebApplicationException) {
                WebApplicationException wae = (WebApplicationException) exception.get();
                asyncResponse.resume(wae);
                return;
            } else {
                asyncResponse.resume(new RestException(exception.get()));
                return;
            }
        }

        asyncResponse.resume(Response.noContent().build());
    }

    protected void internalResetCursor(AsyncResponse asyncResponse, String subName, long timestamp,
            boolean authoritative) {
        if (topicName.isGlobal()) {
            try {
                validateGlobalNamespaceOwnership(namespaceName);
            } catch (Exception e) {
                log.warn("[{}][{}] Failed to reset cursor on subscription {} to time {}: {}",
                        clientAppId(), topicName,
                        subName, timestamp, e.getMessage());
                resumeAsyncResponseExceptionally(asyncResponse, e);
                return;
            }
        }

        validateTopicOwnership(topicName, authoritative);
        validateTopicOperation(topicName, TopicOperation.RESET_CURSOR, subName);

        // If the topic name is a partition name, no need to get partition topic metadata again
        if (topicName.isPartitioned()) {
            internalResetCursorForNonPartitionedTopic(asyncResponse, subName, timestamp, authoritative);
        } else {
            getPartitionedTopicMetadataAsync(topicName,
                    authoritative, false).thenAccept(partitionMetadata -> {
                final int numPartitions = partitionMetadata.partitions;
                if (numPartitions > 0) {
                    final CompletableFuture<Void> future = new CompletableFuture<>();
                    final AtomicInteger count = new AtomicInteger(numPartitions);
                    final AtomicInteger failureCount = new AtomicInteger(0);
                    final AtomicReference<Throwable> partitionException = new AtomicReference<>();

                    for (int i = 0; i < numPartitions; i++) {
                        TopicName topicNamePartition = topicName.getPartition(i);
                        try {
                            pulsar().getAdminClient().topics()
                                    .resetCursorAsync(topicNamePartition.toString(),
                                            subName, timestamp).handle((r, ex) -> {
                                if (ex != null) {
                                    if (ex instanceof PreconditionFailedException) {
                                        // throw the last exception if all partitions get this error
                                        // any other exception on partition is reported back to user
                                        failureCount.incrementAndGet();
                                        partitionException.set(ex);
                                    } else {
                                        log.warn("[{}] [{}] Failed to reset cursor on subscription {} to time {}",
                                                clientAppId(), topicNamePartition, subName, timestamp, ex);
                                        future.completeExceptionally(ex);
                                        return null;
                                    }
                                }

                                if (count.decrementAndGet() == 0) {
                                    future.complete(null);
                                }

                                return null;
                            });
                        } catch (Exception e) {
                            log.warn("[{}] [{}] Failed to reset cursor on subscription {} to time {}", clientAppId(),
                                    topicNamePartition, subName, timestamp, e);
                            future.completeExceptionally(e);
                        }
                    }

                    future.whenComplete((r, ex) -> {
                        if (ex != null) {
                            if (ex instanceof PulsarAdminException) {
                                asyncResponse.resume(new RestException((PulsarAdminException) ex));
                                return;
                            } else {
                                asyncResponse.resume(new RestException(ex));
                                return;
                            }
                        }

                        // report an error to user if unable to reset for all partitions
                        if (failureCount.get() == numPartitions) {
                            log.warn("[{}] [{}] Failed to reset cursor on subscription {} to time {}",
                                    clientAppId(), topicName,
                                    subName, timestamp, partitionException.get());
                            asyncResponse.resume(
                                    new RestException(Status.PRECONDITION_FAILED,
                                            partitionException.get().getMessage()));
                            return;
                        } else if (failureCount.get() > 0) {
                            log.warn("[{}] [{}] Partial errors for reset cursor on subscription {} to time {}",
                                    clientAppId(), topicName, subName, timestamp, partitionException.get());
                        }

                        asyncResponse.resume(Response.noContent().build());
                    });
                } else {
                    internalResetCursorForNonPartitionedTopic(asyncResponse, subName, timestamp, authoritative);
                }
            }).exceptionally(ex -> {
                log.error("[{}] Failed to expire messages for all subscription on topic {}",
                        clientAppId(), topicName, ex);
                resumeAsyncResponseExceptionally(asyncResponse, ex);
                return null;
            });
        }
    }

    private void internalResetCursorForNonPartitionedTopic(AsyncResponse asyncResponse, String subName, long timestamp,
                                       boolean authoritative) {
        try {
            validateTopicOwnership(topicName, authoritative);
            validateTopicOperation(topicName, TopicOperation.RESET_CURSOR);

            log.info("[{}] [{}] Received reset cursor on subscription {} to time {}",
                    clientAppId(), topicName, subName, timestamp);

            PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
            if (topic == null) {
                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Topic not found"));
                return;
            }
            PersistentSubscription sub = topic.getSubscription(subName);
            if (sub == null) {
                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Subscription not found"));
                return;
            }
            sub.resetCursor(timestamp).thenRun(() -> {
                log.info("[{}][{}] Reset cursor on subscription {} to time {}", clientAppId(), topicName, subName,
                        timestamp);
                asyncResponse.resume(Response.noContent().build());
            }).exceptionally(ex -> {
                Throwable t = (ex instanceof CompletionException ? ex.getCause() : ex);
                log.warn("[{}][{}] Failed to reset cursor on subscription {} to time {}", clientAppId(), topicName,
                        subName, timestamp, t);
                if (t instanceof SubscriptionInvalidCursorPosition) {
                    asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                            "Unable to find position for timestamp specified: " + t.getMessage()));
                } else if (t instanceof SubscriptionBusyException) {
                    asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                            "Failed for Subscription Busy: " + t.getMessage()));
                } else {
                    resumeAsyncResponseExceptionally(asyncResponse, t);
                }
                return null;
            });
        } catch (Exception e) {
            log.warn("[{}][{}] Failed to reset cursor on subscription {} to time {}",
                    clientAppId(), topicName, subName, timestamp, e);
            if (e instanceof NotAllowedException) {
                asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED, e.getMessage()));
            } else {
                resumeAsyncResponseExceptionally(asyncResponse, e);
            }
        }
    }

    protected void internalCreateSubscription(AsyncResponse asyncResponse, String subscriptionName,
            MessageIdImpl messageId, boolean authoritative, boolean replicated) {
        if (topicName.isGlobal()) {
            try {
                validateGlobalNamespaceOwnership(namespaceName);
            } catch (Exception e) {
                log.error("[{}] Failed to create subscription {} on topic {}",
                        clientAppId(), subscriptionName, topicName, e);
                resumeAsyncResponseExceptionally(asyncResponse, e);
                return;
            }
        }
        final MessageIdImpl targetMessageId = messageId == null ? (MessageIdImpl) MessageId.latest : messageId;
        log.info("[{}][{}] Creating subscription {} at message id {}", clientAppId(), topicName, subscriptionName,
                targetMessageId);
        // If the topic name is a partition name, no need to get partition topic metadata again
        if (topicName.isPartitioned()) {
            internalCreateSubscriptionForNonPartitionedTopic(asyncResponse,
                    subscriptionName, targetMessageId, authoritative, replicated);
        } else {
            boolean allowAutoTopicCreation = pulsar().getConfiguration().isAllowAutoTopicCreation();
            getPartitionedTopicMetadataAsync(topicName,
                    authoritative, allowAutoTopicCreation).thenAccept(partitionMetadata -> {
                final int numPartitions = partitionMetadata.partitions;
                if (numPartitions > 0) {
                    final CompletableFuture<Void> future = new CompletableFuture<>();
                    final AtomicInteger count = new AtomicInteger(numPartitions);
                    final AtomicInteger failureCount = new AtomicInteger(0);
                    final AtomicReference<Throwable> partitionException = new AtomicReference<>();

                    // Create the subscription on each partition
                    for (int i = 0; i < numPartitions; i++) {
                        TopicName topicNamePartition = topicName.getPartition(i);
                        try {
                            pulsar().getAdminClient().topics()
                                    .createSubscriptionAsync(topicNamePartition.toString(),
                                            subscriptionName, targetMessageId)
                                    .handle((r, ex) -> {
                                        if (ex != null) {
                                            // fail the operation on unknown exception or
                                            // if all the partitioned failed due to
                                            // subscription-already-exist
                                            if (failureCount.incrementAndGet() == numPartitions
                                                    || !(ex instanceof PulsarAdminException.ConflictException)) {
                                                partitionException.set(ex);
                                            }
                                        }

                                        if (count.decrementAndGet() == 0) {
                                            future.complete(null);
                                        }

                                        return null;
                                    });
                        } catch (Exception e) {
                            log.warn("[{}] [{}] Failed to create subscription {} at message id {}", clientAppId(),
                                    topicNamePartition, subscriptionName, targetMessageId, e);
                            future.completeExceptionally(e);
                        }
                    }

                    future.whenComplete((r, ex) -> {
                        if (ex != null) {
                            if (ex instanceof PulsarAdminException) {
                                asyncResponse.resume(new RestException((PulsarAdminException) ex));
                                return;
                            } else {
                                asyncResponse.resume(new RestException(ex));
                                return;
                            }
                        }

                        if (partitionException.get() != null) {
                            log.warn("[{}] [{}] Failed to create subscription {} at message id {}",
                                    clientAppId(), topicName,
                                    subscriptionName, targetMessageId, partitionException.get());
                            if (partitionException.get() instanceof PulsarAdminException) {
                                asyncResponse.resume(
                                        new RestException((PulsarAdminException) partitionException.get()));
                                return;
                            } else {
                                asyncResponse.resume(new RestException(partitionException.get()));
                                return;
                            }
                        }

                        asyncResponse.resume(Response.noContent().build());
                    });
                } else {
                    internalCreateSubscriptionForNonPartitionedTopic(asyncResponse,
                            subscriptionName, targetMessageId, authoritative, replicated);
                }
            }).exceptionally(ex -> {
                log.error("[{}] Failed to create subscription {} on topic {}",
                        clientAppId(), subscriptionName, topicName, ex);
                resumeAsyncResponseExceptionally(asyncResponse, ex);
                return null;
            });
        }
    }

    private void internalCreateSubscriptionForNonPartitionedTopic(
            AsyncResponse asyncResponse, String subscriptionName,
            MessageIdImpl targetMessageId, boolean authoritative, boolean replicated) {

        boolean isAllowAutoTopicCreation = pulsar().getConfiguration().isAllowAutoTopicCreation();

        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> {
                    validateTopicOperation(topicName, TopicOperation.SUBSCRIBE);
                    return pulsar().getBrokerService().getTopic(topicName.toString(), isAllowAutoTopicCreation);
                }).thenApply(optTopic -> {
            if (optTopic.isPresent()) {
                return optTopic.get();
            } else {
                throw new RestException(Status.PRECONDITION_FAILED,
                        "Topic does not exist and cannot be auto-created");
            }
        }).thenCompose(topic -> {
            if (topic.getSubscriptions().containsKey(subscriptionName)) {
                throw new RestException(Status.CONFLICT, "Subscription already exists for topic");
            }

            return topic.createSubscription(subscriptionName, InitialPosition.Latest, replicated);
        }).thenCompose(subscription -> {
            // Mark the cursor as "inactive" as it was created without a real consumer connected
            ((PersistentSubscription) subscription).deactivateCursor();
            return subscription.resetCursor(
                    PositionImpl.get(targetMessageId.getLedgerId(), targetMessageId.getEntryId()));
        }).thenRun(() -> {
            log.info("[{}][{}] Successfully created subscription {} at message id {}", clientAppId(),
                    topicName, subscriptionName, targetMessageId);
            asyncResponse.resume(Response.noContent().build());
        }).exceptionally(ex -> {
            Throwable t = (ex instanceof CompletionException ? ex.getCause() : ex);
            if (!(t instanceof WebApplicationException)) {
                log.warn("[{}][{}] Failed to create subscription {} at message id {}", clientAppId(), topicName,
                        subscriptionName, targetMessageId, t);
            }

            if (t instanceof SubscriptionInvalidCursorPosition) {
                asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                        "Unable to find position for position specified: " + t.getMessage()));
            } else if (t instanceof SubscriptionBusyException) {
                asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                        "Failed for Subscription Busy: " + t.getMessage()));
            } else {
                resumeAsyncResponseExceptionally(asyncResponse, t);
            }
            return null;
        });
    }

    protected void internalResetCursorOnPosition(AsyncResponse asyncResponse, String subName, boolean authoritative,
            MessageIdImpl messageId, boolean isExcluded, int batchIndex) {
        if (topicName.isGlobal()) {
            try {
                validateGlobalNamespaceOwnership(namespaceName);
            } catch (Exception e) {
                log.warn("[{}][{}] Failed to reset cursor on subscription {} to position {}: {}", clientAppId(),
                        topicName, subName, messageId, e.getMessage());
                resumeAsyncResponseExceptionally(asyncResponse, e);
                return;
            }
        }

        log.info("[{}][{}] received reset cursor on subscription {} to position {}", clientAppId(), topicName,
                subName, messageId);

        // If the topic name is a partition name, no need to get partition topic metadata again
        if (!topicName.isPartitioned() && getPartitionedTopicMetadata(topicName, authoritative, false).partitions > 0) {
            log.warn("[{}] Not supported operation on partitioned-topic {} {}", clientAppId(), topicName,
                    subName);
            asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED,
                    "Reset-cursor at position is not allowed for partitioned-topic"));
            return;
        } else {
            validateTopicOwnership(topicName, authoritative);
            validateTopicOperation(topicName, TopicOperation.RESET_CURSOR);

            PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
            if (topic == null) {
                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Topic not found"));
                return;
            }
            try {
                PersistentSubscription sub = topic.getSubscription(subName);
                if (sub == null) {
                    asyncResponse.resume(new RestException(Status.NOT_FOUND, "Subscription not found"));
                    return;
                }
                CompletableFuture<Integer> batchSizeFuture = new CompletableFuture<>();
                getEntryBatchSize(batchSizeFuture, topic, messageId, batchIndex);
                batchSizeFuture.thenAccept(bi -> {
                    PositionImpl seekPosition = calculatePositionAckSet(isExcluded, bi, batchIndex, messageId);
                    sub.resetCursor(seekPosition).thenRun(() -> {
                        log.info("[{}][{}] successfully reset cursor on subscription {} to position {}", clientAppId(),
                                topicName, subName, messageId);
                        asyncResponse.resume(Response.noContent().build());
                    }).exceptionally(ex -> {
                        Throwable t = (ex instanceof CompletionException ? ex.getCause() : ex);
                        log.warn("[{}][{}] Failed to reset cursor on subscription {} to position {}", clientAppId(),
                                topicName, subName, messageId, t);
                        if (t instanceof SubscriptionInvalidCursorPosition) {
                            asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                                    "Unable to find position for position specified: " + t.getMessage()));
                        } else if (t instanceof SubscriptionBusyException) {
                            asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                                    "Failed for Subscription Busy: " + t.getMessage()));
                        } else {
                            resumeAsyncResponseExceptionally(asyncResponse, t);
                        }
                        return null;
                    });
                }).exceptionally(e -> {
                    asyncResponse.resume(e);
                    return null;
                });
            } catch (Exception e) {
                log.warn("[{}][{}] Failed to reset cursor on subscription {} to position {}", clientAppId(), topicName,
                        subName, messageId, e);
                resumeAsyncResponseExceptionally(asyncResponse, e);
            }
        }
    }

    private void getEntryBatchSize(CompletableFuture<Integer> batchSizeFuture, PersistentTopic topic,
                                   MessageIdImpl messageId, int batchIndex) {
        if (batchIndex >= 0) {
            try {
                ManagedLedgerImpl ledger = (ManagedLedgerImpl) topic.getManagedLedger();
                ledger.asyncReadEntry(new PositionImpl(messageId.getLedgerId(),
                        messageId.getEntryId()), new AsyncCallbacks.ReadEntryCallback() {
                    @Override
                    public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                        // Since we can't read the message from the storage layer,
                        // it might be an already delete message ID or an invalid message ID
                        // We should fall back to non batch index seek.
                        batchSizeFuture.complete(0);
                    }

                    @Override
                    public void readEntryComplete(Entry entry, Object ctx) {
                        try {
                            try {
                                if (entry == null) {
                                    batchSizeFuture.complete(0);
                                } else {
                                    MessageMetadata metadata =
                                            Commands.parseMessageMetadata(entry.getDataBuffer());
                                    batchSizeFuture.complete(metadata.getNumMessagesInBatch());
                                }
                            } catch (Exception e) {
                                batchSizeFuture.completeExceptionally(new RestException(e));
                            }
                        } finally {
                            if (entry != null) {
                                entry.release();
                            }
                        }
                    }
                }, null);
            } catch (NullPointerException npe) {
                batchSizeFuture.completeExceptionally(new RestException(Status.NOT_FOUND, "Message not found"));
            } catch (Exception exception) {
                log.error("[{}] Failed to get message with ledgerId {} entryId {} from {}",
                        clientAppId(), messageId.getLedgerId(), messageId.getEntryId(), topicName, exception);
                batchSizeFuture.completeExceptionally(new RestException(exception));
            }
        } else {
            batchSizeFuture.complete(0);
        }
    }

    private PositionImpl calculatePositionAckSet(boolean isExcluded, int batchSize,
                                                 int batchIndex, MessageIdImpl messageId) {
        PositionImpl seekPosition;
        if (batchSize > 0) {
            long[] ackSet;
            BitSetRecyclable bitSet = BitSetRecyclable.create();
            bitSet.set(0, batchSize);
            if (isExcluded) {
                bitSet.clear(0, Math.max(batchIndex + 1, 0));
                if (bitSet.length() > 0) {
                    ackSet = bitSet.toLongArray();
                    seekPosition = PositionImpl.get(messageId.getLedgerId(),
                            messageId.getEntryId(), ackSet);
                } else {
                    seekPosition = PositionImpl.get(messageId.getLedgerId(), messageId.getEntryId());
                    seekPosition = seekPosition.getNext();
                }
            } else {
                if (batchIndex - 1 >= 0) {
                    bitSet.clear(0, batchIndex);
                    ackSet = bitSet.toLongArray();
                    seekPosition = PositionImpl.get(messageId.getLedgerId(),
                            messageId.getEntryId(), ackSet);
                } else {
                    seekPosition = PositionImpl.get(messageId.getLedgerId(), messageId.getEntryId());
                }
            }
            bitSet.recycle();
        } else {
            seekPosition = PositionImpl.get(messageId.getLedgerId(), messageId.getEntryId());
            seekPosition = isExcluded ? seekPosition.getNext() : seekPosition;
        }
        return seekPosition;
    }

    protected void internalGetMessageById(AsyncResponse asyncResponse, long ledgerId, long entryId,
                                              boolean authoritative) {
        try {
            // will redirect if the topic not owned by current broker
            validateTopicOwnership(topicName, authoritative);
            validateTopicOperation(topicName, TopicOperation.PEEK_MESSAGES);

            if (topicName.isGlobal()) {
                validateGlobalNamespaceOwnership(namespaceName);
            }
            PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
            ManagedLedgerImpl ledger = (ManagedLedgerImpl) topic.getManagedLedger();
            ledger.asyncReadEntry(new PositionImpl(ledgerId, entryId), new AsyncCallbacks.ReadEntryCallback() {
                @Override
                public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                    asyncResponse.resume(new RestException(exception));
                }

                @Override
                public void readEntryComplete(Entry entry, Object ctx) {
                    try {
                        asyncResponse.resume(generateResponseWithEntry(entry));
                    } catch (IOException exception) {
                        asyncResponse.resume(new RestException(exception));
                    } finally {
                        if (entry != null) {
                            entry.release();
                        }
                    }
                }
            }, null);
        } catch (NullPointerException npe) {
            asyncResponse.resume(new RestException(Status.NOT_FOUND, "Message not found"));
        } catch (Exception exception) {
            log.error("[{}] Failed to get message with ledgerId {} entryId {} from {}",
                    clientAppId(), ledgerId, entryId, topicName, exception);
            asyncResponse.resume(new RestException(exception));
        }
    }

    protected Response internalPeekNthMessage(String subName, int messagePosition, boolean authoritative) {
        // If the topic name is a partition name, no need to get partition topic metadata again
        if (!topicName.isPartitioned() && getPartitionedTopicMetadata(topicName,
                authoritative, false).partitions > 0) {
            throw new RestException(Status.METHOD_NOT_ALLOWED, "Peek messages on a partitioned topic is not allowed");
        }

        validateTopicOwnership(topicName, authoritative);
        validateTopicOperation(topicName, TopicOperation.PEEK_MESSAGES);

        if (!(getTopicReference(topicName) instanceof PersistentTopic)) {
            log.error("[{}] Not supported operation of non-persistent topic {} {}", clientAppId(), topicName,
                    subName);
            throw new RestException(Status.METHOD_NOT_ALLOWED,
                    "Peek messages on a non-persistent topic is not allowed");
        }

        PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
        PersistentReplicator repl = null;
        PersistentSubscription sub = null;
        Entry entry = null;
        if (subName.startsWith(topic.getReplicatorPrefix())) {
            repl = getReplicatorReference(subName, topic);
        } else {
            sub = (PersistentSubscription) getSubscriptionReference(subName, topic);
        }
        try {
            if (subName.startsWith(topic.getReplicatorPrefix())) {
                entry = repl.peekNthMessage(messagePosition).get();
            } else {
                entry = sub.peekNthMessage(messagePosition).get();
            }
            return generateResponseWithEntry(entry);
        } catch (NullPointerException npe) {
            throw new RestException(Status.NOT_FOUND, "Message not found");
        } catch (Exception exception) {
            log.error("[{}] Failed to peek message at position {} from {} {}", clientAppId(), messagePosition,
                    topicName, subName, exception);
            throw new RestException(exception);
        } finally {
            if (entry != null) {
                entry.release();
            }
        }
    }

    protected Response internalExamineMessage(String initialPosition, long messagePosition, boolean authoritative) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }

        if (!topicName.isPartitioned() && getPartitionedTopicMetadata(topicName,
                authoritative, false).partitions > 0) {
            throw new RestException(Status.METHOD_NOT_ALLOWED,
                    "Examine messages on a partitioned topic is not allowed, "
                            + "please try examine message on specific topic partition");
        }
        validateTopicOwnership(topicName, authoritative);
        if (!(getTopicReference(topicName) instanceof PersistentTopic)) {
            log.error("[{}] Not supported operation of non-persistent topic {} ", clientAppId(), topicName);
            throw new RestException(Status.METHOD_NOT_ALLOWED,
                    "Examine messages on a non-persistent topic is not allowed");
        }

        if (messagePosition < 1) {
            messagePosition = 1;
        }

        if (null == initialPosition) {
            initialPosition = "latest";
        }

        try {
            PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
            long totalMessage = topic.getNumberOfEntries();
            PositionImpl startPosition = topic.getFirstPosition();
            long messageToSkip =
                    initialPosition.equals("earliest") ? messagePosition : totalMessage - messagePosition + 1;
            CompletableFuture<Entry> future = new CompletableFuture<>();
            PositionImpl readPosition = topic.getPositionAfterN(startPosition, messageToSkip);
            topic.asyncReadEntry(readPosition, new AsyncCallbacks.ReadEntryCallback() {
                @Override
                public void readEntryComplete(Entry entry, Object ctx) {
                    future.complete(entry);
                }

                @Override
                public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                    future.completeExceptionally(exception);
                }
            }, null);
            return generateResponseWithEntry(future.get());
        } catch (Exception exception) {
            exception.printStackTrace();
            log.error("[{}] Failed to examine message at position {} from {} due to {}", clientAppId(), messagePosition,
                    topicName , exception);
            throw new RestException(exception);
        }
    }

    private void verifyReadOperation(boolean authoritative) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(topicName, authoritative, false);
        if (partitionMetadata.partitions > 0) {
            throw new RestException(Status.METHOD_NOT_ALLOWED, "Peek messages on a partitioned topic is not allowed");
        }
    }

    private Response generateResponseWithEntry(Entry entry) throws IOException {
        checkNotNull(entry);
        PositionImpl pos = (PositionImpl) entry.getPosition();
        ByteBuf metadataAndPayload = entry.getDataBuffer();

        MessageMetadata metadata = Commands.parseMessageMetadata(metadataAndPayload);

        ResponseBuilder responseBuilder = Response.ok();
        responseBuilder.header("X-Pulsar-Message-ID", pos.toString());
        for (KeyValue keyValue : metadata.getPropertiesList()) {
            responseBuilder.header("X-Pulsar-PROPERTY-" + keyValue.getKey(), keyValue.getValue());
        }
        if (metadata.hasPublishTime()) {
            responseBuilder.header("X-Pulsar-publish-time", DateFormatter.format(metadata.getPublishTime()));
        }
        if (metadata.hasEventTime()) {
            responseBuilder.header("X-Pulsar-event-time", DateFormatter.format(metadata.getEventTime()));
        }
        if (metadata.hasNumMessagesInBatch()) {
            responseBuilder.header("X-Pulsar-num-batch-message", metadata.getNumMessagesInBatch());
        }
        if (metadata.hasNullValue()) {
            responseBuilder.header("X-Pulsar-null-value", metadata.isNullValue());
        }
        if (metadata.hasNumChunksFromMsg()) {
            responseBuilder.header("X-Pulsar-PROPERTY-TOTAL-CHUNKS", Integer.toString(metadata.getNumChunksFromMsg()));
            responseBuilder.header("X-Pulsar-PROPERTY-CHUNK-ID", Integer.toString(metadata.getChunkId()));
        }

        // Decode if needed
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(metadata.getCompression());
        ByteBuf uncompressedPayload = codec.decode(metadataAndPayload, metadata.getUncompressedSize());

        // Copy into a heap buffer for output stream compatibility
        ByteBuf data = PulsarByteBufAllocator.DEFAULT.heapBuffer(uncompressedPayload.readableBytes(),
                uncompressedPayload.readableBytes());
        data.writeBytes(uncompressedPayload);
        uncompressedPayload.release();

        StreamingOutput stream = output -> {
            output.write(data.array(), data.arrayOffset(), data.readableBytes());
            data.release();
        };

        return responseBuilder.entity(stream).build();
    }

    protected PersistentOfflineTopicStats internalGetBacklog(boolean authoritative) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        // Validate that namespace exists, throw 404 if it doesn't exist
        // note that we do not want to load the topic and hence skip authorization check
        try {
            namespaceResources().get(path(POLICIES, namespaceName.toString()));
        } catch (org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException e) {
            log.warn("[{}] Failed to get topic backlog {}: Namespace does not exist", clientAppId(), namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to get topic backlog {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }

        PersistentOfflineTopicStats offlineTopicStats = null;
        try {

            offlineTopicStats = pulsar().getBrokerService().getOfflineTopicStat(topicName);
            if (offlineTopicStats != null) {
                // offline topic stat has a cost - so use cached value until TTL
                long elapsedMs = System.currentTimeMillis() - offlineTopicStats.statGeneratedAt.getTime();
                if (TimeUnit.MINUTES.convert(elapsedMs, TimeUnit.MILLISECONDS) < OFFLINE_TOPIC_STAT_TTL_MINS) {
                    return offlineTopicStats;
                }
            }
            final ManagedLedgerConfig config = pulsar().getBrokerService().getManagedLedgerConfig(topicName)
                    .get();
            ManagedLedgerOfflineBacklog offlineTopicBacklog = new ManagedLedgerOfflineBacklog(config.getDigestType(),
                    config.getPassword(), pulsar().getAdvertisedAddress(), false);
            offlineTopicStats = offlineTopicBacklog.estimateUnloadedTopicBacklog(
                    (ManagedLedgerFactoryImpl) pulsar().getManagedLedgerFactory(), topicName);
            pulsar().getBrokerService().cacheOfflineTopicStats(topicName, offlineTopicStats);
        } catch (Exception exception) {
            throw new RestException(exception);
        }
        return offlineTopicStats;
    }

    protected Map<BacklogQuota.BacklogQuotaType, BacklogQuota> internalGetBacklogQuota(boolean applied) {
        Map<BacklogQuota.BacklogQuotaType, BacklogQuota> quotaMap = getTopicPolicies(topicName)
                .map(TopicPolicies::getBackLogQuotaMap)
                .map(map -> {
                    HashMap<BacklogQuota.BacklogQuotaType, BacklogQuota> hashMap = Maps.newHashMap();
                    map.forEach((key, value) -> hashMap.put(BacklogQuota.BacklogQuotaType.valueOf(key), value));
                    return hashMap;
                }).orElse(Maps.newHashMap());
        if (applied && quotaMap.isEmpty()) {
            quotaMap = getNamespacePolicies(namespaceName).backlog_quota_map;
            if (quotaMap.isEmpty()) {
                String namespace = namespaceName.toString();
                quotaMap.put(
                        BacklogQuota.BacklogQuotaType.destination_storage,
                        namespaceBacklogQuota(namespace, AdminResource.path(POLICIES, namespace))
                );

            }
        }
        return quotaMap;
    }

    protected void internalSetBacklogQuota(AsyncResponse asyncResponse,
                                           BacklogQuota.BacklogQuotaType backlogQuotaType,
                                           BacklogQuotaImpl backlogQuota) {
        validateTopicPolicyOperation(topicName, PolicyName.BACKLOG, PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();
        if (backlogQuotaType == null) {
            backlogQuotaType = BacklogQuota.BacklogQuotaType.destination_storage;
        }
        TopicPolicies topicPolicies;
        try {
            topicPolicies = pulsar().getTopicPoliciesService().getTopicPolicies(topicName);
        } catch (BrokerServiceException.TopicPoliciesCacheNotInitException e) {
            log.error("Topic {} policies have not been initialized yet.", topicName);
            asyncResponse.resume(new RestException(e));
            return;
        }
        if (topicPolicies == null){
            topicPolicies = new TopicPolicies();
        }

        RetentionPolicies retentionPolicies = getRetentionPolicies(topicName, topicPolicies);
        if (!checkBacklogQuota(backlogQuota, retentionPolicies)) {
            log.warn(
                    "[{}] Failed to update backlog configuration for topic {}: conflicts with retention quota",
                    clientAppId(), topicName);
            asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                    "Backlog Quota exceeds configured retention quota for topic. "
                            + "Please increase retention quota and retry"));
            return;
        }

        if (backlogQuota != null) {
            topicPolicies.getBackLogQuotaMap().put(backlogQuotaType.name(), backlogQuota);
        } else {
            topicPolicies.getBackLogQuotaMap().remove(backlogQuotaType.name());
        }
        Map<String, BacklogQuotaImpl> backLogQuotaMap = topicPolicies.getBackLogQuotaMap();
        pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies)
                .whenComplete((r, ex) -> {
                    if (ex != null) {
                        log.error("Failed updated backlog quota map", ex);
                        asyncResponse.resume(new RestException(ex));
                    } else {
                        try {
                            log.info("[{}] Successfully updated backlog quota map: namespace={}, topic={}, map={}",
                                    clientAppId(),
                                    namespaceName,
                                    topicName.getLocalName(),
                                    jsonMapper().writeValueAsString(backLogQuotaMap));
                        } catch (JsonProcessingException ignore) { }
                        asyncResponse.resume(Response.noContent().build());
                    }
                });
    }

    protected CompletableFuture<Boolean> internalGetDeduplication(boolean applied) {
        Boolean deduplicationEnabled = getTopicPolicies(topicName)
                .map(TopicPolicies::getDeduplicationEnabled)
                .orElseGet(() -> {
                    if (applied) {
                        Boolean enabled = getNamespacePolicies(namespaceName).deduplicationEnabled;
                        return enabled == null ? config().isBrokerDeduplicationEnabled() : enabled;
                    }
                    return null;
                });
        return CompletableFuture.completedFuture(deduplicationEnabled);
    }

    protected CompletableFuture<Void> internalSetDeduplication(Boolean enabled) {
        TopicPolicies topicPolicies = null;
        try {
            topicPolicies = pulsar().getTopicPoliciesService().getTopicPolicies(topicName);
        } catch (BrokerServiceException.TopicPoliciesCacheNotInitException e) {
            log.error("Topic {} policies have not been initialized yet.", topicName);
            throw new RestException(Status.PRECONDITION_FAILED, "Policies have not been initialized yet");
        }
        if (topicPolicies == null) {
            topicPolicies = new TopicPolicies();
        }
        topicPolicies.setDeduplicationEnabled(enabled);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
    }

    protected void internalSetMessageTTL(AsyncResponse asyncResponse, Integer ttlInSecond) {
        //Validate message ttl value.
        if (ttlInSecond != null && ttlInSecond < 0) {
            throw new RestException(Status.PRECONDITION_FAILED, "Invalid value for message TTL");
        }
        TopicPolicies topicPolicies;
        //Update existing topic policy or create a new one if not exist.
        try {
            topicPolicies = pulsar().getTopicPoliciesService().getTopicPolicies(topicName);
        } catch (BrokerServiceException.TopicPoliciesCacheNotInitException e) {
            log.error("Topic {} policies have not been initialized yet.", topicName);
            asyncResponse.resume(new RestException(e));
            return;
        }
        if (topicPolicies == null){
            topicPolicies = new TopicPolicies();
        }
        topicPolicies.setMessageTTLInSeconds(ttlInSecond);
        pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed set message ttl for topic", ex);
                        asyncResponse.resume(new RestException(ex));
                    } else {
                        log.info("[{}] Successfully set topic message ttl: namespace={}, topic={}, ttl={}",
                                clientAppId(),
                                namespaceName,
                                topicName.getLocalName(),
                                ttlInSecond);
                        asyncResponse.resume(Response.noContent().build());
                    }
                });
    }

    private RetentionPolicies getRetentionPolicies(TopicName topicName, TopicPolicies topicPolicies) {
        RetentionPolicies retentionPolicies = topicPolicies.getRetentionPolicies();
        if (retentionPolicies == null){
            try {
                retentionPolicies = getNamespacePoliciesAsync(topicName.getNamespaceObject())
                        .thenApply(policies -> policies.retention_policies)
                        .get(1L, TimeUnit.SECONDS);
            } catch (Exception e) {
               throw new RestException(e);
            }
        }
        return retentionPolicies;
    }

    protected void internalRemoveBacklogQuota(AsyncResponse asyncResponse,
            BacklogQuota.BacklogQuotaType backlogQuotaType) {
        internalSetBacklogQuota(asyncResponse, backlogQuotaType, null);
    }

    protected void internalGetRetention(AsyncResponse asyncResponse, boolean applied){
        RetentionPolicies retentionPolicies = getTopicPolicies(topicName)
                .map(TopicPolicies::getRetentionPolicies).orElseGet(() -> {
                    if (applied) {
                        RetentionPolicies policies = getNamespacePolicies(namespaceName).retention_policies;
                        return policies == null ? new RetentionPolicies(
                                config().getDefaultRetentionTimeInMinutes(), config().getDefaultRetentionSizeInMB())
                                : policies;
                    }
                    return null;
                });
        asyncResponse.resume(retentionPolicies == null ? Response.noContent().build() : retentionPolicies);
    }

    protected CompletableFuture<Void> internalSetRetention(RetentionPolicies retention) {
        if (retention == null) {
            return CompletableFuture.completedFuture(null);
        }
        TopicPolicies topicPolicies = getTopicPolicies(topicName)
                .orElseGet(TopicPolicies::new);
        BacklogQuota backlogQuota =
                    topicPolicies.getBackLogQuotaMap().get(BacklogQuota.BacklogQuotaType.destination_storage.name());
        if (backlogQuota == null) {
            Policies policies = getNamespacePolicies(topicName.getNamespaceObject());
            backlogQuota = policies.backlog_quota_map.get(BacklogQuota.BacklogQuotaType.destination_storage);
        }
        if (!checkBacklogQuota(backlogQuota, retention)) {
            log.warn(
                    "[{}] Failed to update retention quota configuration for topic {}: conflicts with retention quota",
                    clientAppId(), topicName);
            throw new RestException(Status.PRECONDITION_FAILED,
                    "Retention Quota must exceed configured backlog quota for topic. "
                            + "Please increase retention quota and retry");
        }
        topicPolicies.setRetentionPolicies(retention);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
    }

    protected CompletableFuture<Void> internalRemoveRetention() {
        Optional<TopicPolicies> topicPolicies = getTopicPolicies(topicName);
        if (!topicPolicies.isPresent()) {
            return CompletableFuture.completedFuture(null);
        }
        topicPolicies.get().setRetentionPolicies(null);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies.get());
    }

    protected CompletableFuture<PersistencePolicies> internalGetPersistence(boolean applied) {
        PersistencePolicies persistencePolicies = getTopicPolicies(topicName)
                .map(TopicPolicies::getPersistence)
                .orElseGet(() -> {
                    if (applied) {
                        PersistencePolicies namespacePolicy = getNamespacePolicies(namespaceName)
                                .persistence;
                        return namespacePolicy == null
                                ? new PersistencePolicies(
                                        pulsar().getConfiguration().getManagedLedgerDefaultEnsembleSize(),
                                        pulsar().getConfiguration().getManagedLedgerDefaultWriteQuorum(),
                                        pulsar().getConfiguration().getManagedLedgerDefaultAckQuorum(),
                                        pulsar().getConfiguration().getManagedLedgerDefaultMarkDeleteRateLimit())
                                : namespacePolicy;
                    }
                    return null;
                });
        return CompletableFuture.completedFuture(persistencePolicies);
    }

    protected CompletableFuture<Void> internalSetPersistence(PersistencePolicies persistencePolicies) {
        validatePersistencePolicies(persistencePolicies);

        TopicPolicies topicPolicies = getTopicPolicies(topicName).orElseGet(TopicPolicies::new);
        topicPolicies.setPersistence(persistencePolicies);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
    }

    protected CompletableFuture<Void> internalRemovePersistence() {
        Optional<TopicPolicies> topicPolicies = getTopicPolicies(topicName);
        if (!topicPolicies.isPresent()) {
            return CompletableFuture.completedFuture(null);
        }
        topicPolicies.get().setPersistence(null);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies.get());
    }

    protected CompletableFuture<Void> internalSetMaxMessageSize(Integer maxMessageSize) {
        if (maxMessageSize != null && (maxMessageSize < 0 || maxMessageSize > config().getMaxMessageSize())) {
            throw new RestException(Status.PRECONDITION_FAILED
                    , "topic-level maxMessageSize must be greater than or equal to 0 "
                    + "and must be smaller than that in the broker-level");
        }

        TopicPolicies topicPolicies = getTopicPolicies(topicName).orElseGet(TopicPolicies::new);
        topicPolicies.setMaxMessageSize(maxMessageSize);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
    }

    protected Optional<Integer> internalGetMaxMessageSize() {
        return getTopicPolicies(topicName).map(TopicPolicies::getMaxMessageSize);
    }

    protected CompletableFuture<Integer> internalGetMaxProducers(boolean applied) {
        Integer maxNum = getTopicPolicies(topicName)
                .map(TopicPolicies::getMaxProducerPerTopic)
                .orElseGet(() -> {
                    if (applied) {
                        Integer maxProducer = getNamespacePolicies(namespaceName).max_producers_per_topic;
                        return maxProducer == null ? config().getMaxProducersPerTopic() : maxProducer;
                    }
                    return null;
                });
        return CompletableFuture.completedFuture(maxNum);
    }

    protected CompletableFuture<Void> internalSetMaxProducers(Integer maxProducers) {
        if (maxProducers != null && maxProducers < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxProducers must be 0 or more");
        }
        TopicPolicies topicPolicies = getTopicPolicies(topicName).orElseGet(TopicPolicies::new);
        topicPolicies.setMaxProducerPerTopic(maxProducers);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
    }

    protected Optional<Integer> internalGetMaxSubscriptionsPerTopic() {
        return getTopicPolicies(topicName).map(TopicPolicies::getMaxSubscriptionsPerTopic);
    }

    protected CompletableFuture<Void> internalSetMaxSubscriptionsPerTopic(Integer maxSubscriptionsPerTopic) {
        if (maxSubscriptionsPerTopic != null && maxSubscriptionsPerTopic < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxSubscriptionsPerTopic must be 0 or more");
        }

        TopicPolicies topicPolicies = getTopicPolicies(topicName).orElseGet(TopicPolicies::new);
        topicPolicies.setMaxSubscriptionsPerTopic(maxSubscriptionsPerTopic);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
    }

    protected CompletableFuture<DispatchRateImpl> internalGetReplicatorDispatchRate(boolean applied) {
        DispatchRateImpl dispatchRate = getTopicPolicies(topicName)
                .map(TopicPolicies::getReplicatorDispatchRate)
                .orElseGet(() -> {
                    if (applied) {
                        DispatchRateImpl namespacePolicy = getNamespacePolicies(namespaceName)
                                .replicatorDispatchRate.get(pulsar().getConfiguration().getClusterName());
                        return namespacePolicy == null ? replicatorDispatchRate() : namespacePolicy;
                    }
                    return null;
                });
        return CompletableFuture.completedFuture(dispatchRate);
    }

    protected CompletableFuture<Void> internalSetReplicatorDispatchRate(DispatchRateImpl dispatchRate) {
        TopicPolicies topicPolicies = getTopicPolicies(topicName).orElseGet(TopicPolicies::new);
        topicPolicies.setReplicatorDispatchRate(dispatchRate);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
    }

    protected void preValidation() {
        checkTopicLevelPolicyEnable();
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        validateTopicOwnership(topicName, false);
    }

    protected CompletableFuture<Void> internalRemoveMaxProducers() {
        Optional<TopicPolicies> topicPolicies = getTopicPolicies(topicName);
        if (!topicPolicies.isPresent()) {
            return CompletableFuture.completedFuture(null);
        }
        topicPolicies.get().setMaxProducerPerTopic(null);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies.get());
    }

    protected CompletableFuture<Integer> internalGetMaxConsumers(boolean applied) {
        Integer maxNum = getTopicPolicies(topicName)
                .map(TopicPolicies::getMaxConsumerPerTopic)
                .orElseGet(() -> {
                    if (applied) {
                        Integer maxConsumer = getNamespacePolicies(namespaceName).max_consumers_per_topic;
                        return maxConsumer == null ? config().getMaxConsumersPerTopic() : maxConsumer;
                    }
                    return null;
                });
        return CompletableFuture.completedFuture(maxNum);
    }

    protected CompletableFuture<Void> internalSetMaxConsumers(Integer maxConsumers) {
        if (maxConsumers != null && maxConsumers < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxConsumers must be 0 or more");
        }

        TopicPolicies topicPolicies = getTopicPolicies(topicName).orElseGet(TopicPolicies::new);
        topicPolicies.setMaxConsumerPerTopic(maxConsumers);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
    }

    protected CompletableFuture<Void> internalRemoveMaxConsumers() {
        Optional<TopicPolicies> topicPolicies = getTopicPolicies(topicName);
        if (!topicPolicies.isPresent()) {
            return CompletableFuture.completedFuture(null);
        }
        topicPolicies.get().setMaxConsumerPerTopic(null);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies.get());
    }

    protected MessageId internalTerminate(boolean authoritative) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(topicName, authoritative, false);
        if (partitionMetadata.partitions > 0) {
            throw new RestException(Status.METHOD_NOT_ALLOWED, "Termination of a partitioned topic is not allowed");
        }
        validateTopicOwnership(topicName, authoritative);
        validateTopicOperation(topicName, TopicOperation.TERMINATE);
        Topic topic = getTopicReference(topicName);
        try {
            return ((PersistentTopic) topic).terminate().get();
        } catch (Exception exception) {
            log.error("[{}] Failed to terminated topic {}", clientAppId(), topicName, exception);
            throw new RestException(exception);
        }
    }

    protected void internalTerminatePartitionedTopic(AsyncResponse asyncResponse, boolean authoritative) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        validateTopicOwnership(topicName, authoritative);
        validateTopicOperation(topicName, TopicOperation.TERMINATE);

      List<MessageId> messageIds = new ArrayList<>();

      PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(topicName, authoritative, false);
        if (partitionMetadata.partitions > 0) {
          final List<CompletableFuture<MessageId>> futures = Lists.newArrayList();

          for (int i = 0; i < partitionMetadata.partitions; i++) {
            TopicName topicNamePartition = topicName.getPartition(i);
            try {
              futures.add(pulsar().getAdminClient().topics()
                  .terminateTopicAsync(topicNamePartition.toString()).whenComplete((messageId, throwable) -> {
                          if (throwable != null) {
                              log.error("[{}] Failed to terminate topic {}", clientAppId(), topicNamePartition,
                                      throwable);
                              asyncResponse.resume(new RestException(throwable));
                          }
                          messageIds.add(messageId);
                      }));
            } catch (Exception e) {
              log.error("[{}] Failed to terminate topic {}", clientAppId(), topicNamePartition, e);
              throw new RestException(e);
            }
          }
            FutureUtil.waitForAll(futures).handle((result, exception) -> {
            if (exception != null) {
              Throwable t = exception.getCause();
              if (t instanceof NotFoundException) {
                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Topic not found"));
              } else {
                  log.error("[{}] Failed to terminate topic {}", clientAppId(), topicName, t);
                  asyncResponse.resume(new RestException(t));
              }
            }
          asyncResponse.resume(messageIds);
            return null;
          });
        }
    }

    protected void internalExpireMessagesByTimestamp(AsyncResponse asyncResponse, String subName,
                                                     int expireTimeInSeconds, boolean authoritative) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        // If the topic name is a partition name, no need to get partition topic metadata again
        if (topicName.isPartitioned()) {
            try {
                internalExpireMessagesByTimestampForSinglePartition(subName, expireTimeInSeconds, authoritative);
            } catch (WebApplicationException wae) {
                asyncResponse.resume(wae);
                return;
            } catch (Exception e) {
                asyncResponse.resume(new RestException(e));
                return;
            }
            asyncResponse.resume(Response.noContent().build());
        } else {
            PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(topicName, authoritative, false);
            if (partitionMetadata.partitions > 0) {
                final List<CompletableFuture<Void>> futures = Lists.newArrayList();

                // expire messages for each partition topic
                for (int i = 0; i < partitionMetadata.partitions; i++) {
                    TopicName topicNamePartition = topicName.getPartition(i);
                    try {
                        futures.add(pulsar()
                                .getAdminClient()
                                .topics()
                                .expireMessagesAsync(topicNamePartition.toString(),
                                        subName, expireTimeInSeconds));
                    } catch (Exception e) {
                        log.error("[{}] Failed to expire messages up to {} on {}", clientAppId(), expireTimeInSeconds,
                            topicNamePartition, e);
                        asyncResponse.resume(new RestException(e));
                        return;
                    }
                }

                FutureUtil.waitForAll(futures).handle((result, exception) -> {
                    if (exception != null) {
                        Throwable t = exception.getCause();
                        if (t instanceof NotFoundException) {
                            asyncResponse.resume(new RestException(Status.NOT_FOUND, "Subscription not found"));
                            return null;
                        } else {
                            log.error("[{}] Failed to expire messages up to {} on {}",
                                    clientAppId(), expireTimeInSeconds,
                                    topicName, t);
                            asyncResponse.resume(new RestException(t));
                            return null;
                        }
                    }

                    asyncResponse.resume(Response.noContent().build());
                    return null;
                });
            } else {
                try {
                    internalExpireMessagesByTimestampForSinglePartition(subName, expireTimeInSeconds, authoritative);
                } catch (WebApplicationException wae) {
                    asyncResponse.resume(wae);
                    return;
                } catch (Exception e) {
                    asyncResponse.resume(new RestException(e));
                    return;
                }
                asyncResponse.resume(Response.noContent().build());
            }
        }
    }

    private void internalExpireMessagesByTimestampForSinglePartition(String subName, int expireTimeInSeconds,
            boolean authoritative) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        // If the topic name is a partition name, no need to get partition topic metadata again
        if (!topicName.isPartitioned() && getPartitionedTopicMetadata(topicName, authoritative, false).partitions > 0) {
            String msg = "This method should not be called for partitioned topic";
            log.error("[{}] {} {} {}", clientAppId(), msg, topicName, subName);
            throw new IllegalStateException(msg);
        }

        validateTopicOwnership(topicName, authoritative);
        validateTopicOperation(topicName, TopicOperation.EXPIRE_MESSAGES);

        if (!(getTopicReference(topicName) instanceof PersistentTopic)) {
            log.error("[{}] Not supported operation of non-persistent topic {} {}", clientAppId(), topicName, subName);
            throw new RestException(Status.METHOD_NOT_ALLOWED,
                    "Expire messages on a non-persistent topic is not allowed");
        }

        PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
        try {
            boolean issued;
            if (subName.startsWith(topic.getReplicatorPrefix())) {
                String remoteCluster = PersistentReplicator.getRemoteCluster(subName);
                PersistentReplicator repl = (PersistentReplicator) topic.getPersistentReplicator(remoteCluster);
                checkNotNull(repl);
                issued = repl.expireMessages(expireTimeInSeconds);
            } else {
                PersistentSubscription sub = topic.getSubscription(subName);
                checkNotNull(sub);
                issued = sub.expireMessages(expireTimeInSeconds);
            }
            if (issued) {
                log.info("[{}] Message expire started up to {} on {} {}", clientAppId(), expireTimeInSeconds, topicName,
                        subName);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Expire message by timestamp not issued on topic {} for subscription {} due to ongoing "
                            + "message expiration not finished or subscription almost catch up. If it's performed on "
                            + "a partitioned topic operation might succeeded on other partitions, please check "
                            + "stats of individual partition.", topicName, subName);
                }
                throw new RestException(Status.CONFLICT, "Expire message by timestamp not issued on topic "
                + topicName + " for subscription " + subName + " due to ongoing message expiration not finished or "
                + " subscription almost catch up. If it's performed on a partitioned topic operation might succeeded "
                + "on other partitions, please check stats of individual partition.");
            }
        } catch (NullPointerException npe) {
            throw new RestException(Status.NOT_FOUND, "Subscription not found");
        } catch (Exception exception) {
            log.error("[{}] Failed to expire messages up to {} on {} with subscription {} {}", clientAppId(),
                    expireTimeInSeconds, topicName, subName, exception);
            throw new RestException(exception);
        }
    }

    protected void internalExpireMessagesByPosition(AsyncResponse asyncResponse, String subName, boolean authoritative,
                                                 MessageIdImpl messageId, boolean isExcluded, int batchIndex) {
        if (topicName.isGlobal()) {
            try {
                validateGlobalNamespaceOwnership(namespaceName);
            } catch (Exception e) {
                log.warn("[{}][{}] Failed to expire messages on subscription {} to position {}: {}", clientAppId(),
                        topicName, subName, messageId, e.getMessage());
                resumeAsyncResponseExceptionally(asyncResponse, e);
                return;
            }
        }

        validateTopicOwnership(topicName, authoritative);
        validateTopicOperation(topicName, TopicOperation.EXPIRE_MESSAGES);

        log.info("[{}][{}] received expire messages on subscription {} to position {}", clientAppId(), topicName,
                subName, messageId);

        // If the topic name is a partition name, no need to get partition topic metadata again
        if (!topicName.isPartitioned() && getPartitionedTopicMetadata(topicName, authoritative, false).partitions > 0) {
            log.warn("[{}] Not supported operation expire message up to {} on partitioned-topic {} {}",
                    clientAppId(), messageId, topicName, subName);
            asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED,
                    "Expire message at position is not supported for partitioned-topic"));
            return;
        } else if (messageId.getPartitionIndex() != topicName.getPartitionIndex()) {
            log.warn("[{}] Invalid parameter for expire message by position, partition index of passed in message"
                            + " position {} doesn't match partition index of topic requested {}.",
                    clientAppId(), messageId, topicName);
            asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                    "Invalid parameter for expire message by position, partition index of message position "
                            + "passed in doesn't match partition index for the topic."));
        } else {
            PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
            if (topic == null) {
                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Topic not found"));
                return;
            }
            try {
                PersistentSubscription sub = topic.getSubscription(subName);
                if (sub == null) {
                    asyncResponse.resume(new RestException(Status.NOT_FOUND, "Subscription not found"));
                    return;
                }
                CompletableFuture<Integer> batchSizeFuture = new CompletableFuture<>();
                getEntryBatchSize(batchSizeFuture, topic, messageId, batchIndex);
                batchSizeFuture.thenAccept(bi -> {
                    PositionImpl position = calculatePositionAckSet(isExcluded, bi, batchIndex, messageId);
                    boolean issued;
                    try {
                        if (subName.startsWith(topic.getReplicatorPrefix())) {
                            String remoteCluster = PersistentReplicator.getRemoteCluster(subName);
                            PersistentReplicator repl = (PersistentReplicator)
                                    topic.getPersistentReplicator(remoteCluster);
                            checkNotNull(repl);
                            issued = repl.expireMessages(position);
                        } else {
                            checkNotNull(sub);
                            issued = sub.expireMessages(position);
                        }
                        if (issued) {
                            log.info("[{}] Message expire started up to {} on {} {}", clientAppId(), position,
                                    topicName, subName);
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("Expire message by position not issued on topic {} for subscription {} "
                                        + "due to ongoing message expiration not finished or subscription "
                                        + "almost catch up.", topicName, subName);
                            }
                            throw new RestException(Status.CONFLICT, "Expire message by position not issued on topic "
                                    + topicName + " for subscription " + subName + " due to ongoing message expiration"
                                    + " not finished or invalid message position provided.");
                        }
                    } catch (NullPointerException npe) {
                        throw new RestException(Status.NOT_FOUND, "Subscription not found");
                    } catch (Exception exception) {
                        log.error("[{}] Failed to expire messages up to {} on {} with subscription {} {}",
                                clientAppId(), position, topicName, subName, exception);
                        throw new RestException(exception);
                    }
                }).exceptionally(e -> {
                    log.error("[{}] Failed to expire messages up to {} on {} with subscription {} {}", clientAppId(),
                            messageId, topicName, subName, e);
                    asyncResponse.resume(e);
                    return null;
                });
            } catch (Exception e) {
                log.warn("[{}][{}] Failed to expire messages up to {} on subscription {} to position {}",
                        clientAppId(), topicName, messageId, subName, messageId, e);
                resumeAsyncResponseExceptionally(asyncResponse, e);
            }
        }
        asyncResponse.resume(Response.noContent().build());
    }

    protected void internalTriggerCompaction(AsyncResponse asyncResponse, boolean authoritative) {
        log.info("[{}] Trigger compaction on topic {}", clientAppId(), topicName);
        try {
            if (topicName.isGlobal()) {
                validateGlobalNamespaceOwnership(namespaceName);
            }
        } catch (Exception e) {
            log.error("[{}] Failed to trigger compaction on topic {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
            return;
        }

        // If the topic name is a partition name, no need to get partition topic metadata again
        if (topicName.isPartitioned()) {
            try {
                internalTriggerCompactionNonPartitionedTopic(authoritative);
            } catch (Exception e) {
                log.error("[{}] Failed to trigger compaction on topic {}", clientAppId(), topicName, e);
                resumeAsyncResponseExceptionally(asyncResponse, e);
                return;
            }
            asyncResponse.resume(Response.noContent().build());
        } else {
            getPartitionedTopicMetadataAsync(topicName, authoritative, false).thenAccept(partitionMetadata -> {
                final int numPartitions = partitionMetadata.partitions;
                if (numPartitions > 0) {
                    final List<CompletableFuture<Void>> futures = Lists.newArrayList();

                    for (int i = 0; i < numPartitions; i++) {
                        TopicName topicNamePartition = topicName.getPartition(i);
                        try {
                            futures.add(pulsar()
                                    .getAdminClient()
                                    .topics()
                                    .triggerCompactionAsync(topicNamePartition.toString()));
                        } catch (Exception e) {
                            log.error("[{}] Failed to trigger compaction on topic {}",
                                    clientAppId(), topicNamePartition, e);
                            asyncResponse.resume(new RestException(e));
                            return;
                        }
                    }

                    FutureUtil.waitForAll(futures).handle((result, exception) -> {
                        if (exception != null) {
                            Throwable th = exception.getCause();
                            if (th instanceof NotFoundException) {
                                asyncResponse.resume(new RestException(Status.NOT_FOUND, th.getMessage()));
                                return null;
                            } else if (th instanceof WebApplicationException) {
                                asyncResponse.resume(th);
                                return null;
                            } else {
                                log.error("[{}] Failed to trigger compaction on topic {}",
                                        clientAppId(), topicName, exception);
                                asyncResponse.resume(new RestException(exception));
                                return null;
                            }
                        }
                        asyncResponse.resume(Response.noContent().build());
                        return null;
                    });
                } else {
                    try {
                        internalTriggerCompactionNonPartitionedTopic(authoritative);
                    } catch (Exception e) {
                        log.error("[{}] Failed to trigger compaction on topic {}", clientAppId(), topicName, e);
                        resumeAsyncResponseExceptionally(asyncResponse, e);
                        return;
                    }
                    asyncResponse.resume(Response.noContent().build());
                }
            }).exceptionally(ex -> {
                log.error("[{}] Failed to trigger compaction on topic {}", clientAppId(), topicName, ex);
                resumeAsyncResponseExceptionally(asyncResponse, ex);
                return null;
            });
        }
    }

    protected void internalTriggerCompactionNonPartitionedTopic(boolean authoritative) {
        validateTopicOwnership(topicName, authoritative);
        validateTopicOperation(topicName, TopicOperation.COMPACT);

        PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
        try {
            topic.triggerCompaction();
        } catch (AlreadyRunningException e) {
            throw new RestException(Status.CONFLICT, e.getMessage());
        } catch (Exception e) {
            log.error("[{}] Failed to trigger compaction on topic {}", clientAppId(), topicName, e);
            throw new RestException(e);
        }
    }

    protected LongRunningProcessStatus internalCompactionStatus(boolean authoritative) {
        validateTopicOwnership(topicName, authoritative);
        validateTopicOperation(topicName, TopicOperation.COMPACT);

        PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
        return topic.compactionStatus();
    }

    protected void internalTriggerOffload(boolean authoritative, MessageIdImpl messageId) {
        validateTopicOwnership(topicName, authoritative);
        validateTopicOperation(topicName, TopicOperation.OFFLOAD);

        PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
        try {
            topic.triggerOffload(messageId);
        } catch (AlreadyRunningException e) {
            throw new RestException(Status.CONFLICT, e.getMessage());
        } catch (Exception e) {
            log.warn("Unexpected error triggering offload", e);
            throw new RestException(e);
        }
    }

    protected OffloadProcessStatus internalOffloadStatus(boolean authoritative) {
        validateTopicOwnership(topicName, authoritative);
        validateTopicOperation(topicName, TopicOperation.OFFLOAD);

        PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
        return topic.offloadStatus();
    }

    public static CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(
            PulsarService pulsar, String clientAppId, String originalPrincipal,
            AuthenticationDataSource authenticationData, TopicName topicName) {
        CompletableFuture<PartitionedTopicMetadata> metadataFuture = new CompletableFuture<>();
        try {
            // (1) authorize client
            try {
                checkAuthorization(pulsar, topicName, clientAppId, authenticationData);
            } catch (RestException e) {
                try {
                    validateAdminAccessForTenant(pulsar,
                            clientAppId, originalPrincipal, topicName.getTenant(), authenticationData);
                } catch (RestException authException) {
                    log.warn("Failed to authorize {} on cluster {}", clientAppId, topicName.toString());
                    throw new PulsarClientException(String.format("Authorization failed %s on topic %s with error %s",
                            clientAppId, topicName.toString(), authException.getMessage()));
                }
            } catch (Exception ex) {
                // throw without wrapping to PulsarClientException that considers: unknown error marked as internal
                // server error
                log.warn("Failed to authorize {} on cluster {} with unexpected exception {}", clientAppId,
                        topicName.toString(), ex.getMessage(), ex);
                throw ex;
            }

            // validates global-namespace contains local/peer cluster: if peer/local cluster present then lookup can
            // serve/redirect request else fail partitioned-metadata-request so, client fails while creating
            // producer/consumer
            checkLocalOrGetPeerReplicationCluster(pulsar, topicName.getNamespaceObject())
                    .thenCompose(res -> pulsar.getBrokerService()
                            .fetchPartitionedTopicMetadataCheckAllowAutoCreationAsync(topicName))
                    .thenAccept(metadata -> {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Total number of partitions for topic {} is {}", clientAppId, topicName,
                                    metadata.partitions);
                        }
                        metadataFuture.complete(metadata);
                    }).exceptionally(ex -> {
                        metadataFuture.completeExceptionally(ex.getCause());
                        return null;
                    });
        } catch (Exception ex) {
            metadataFuture.completeExceptionally(ex);
        }
        return metadataFuture;
    }

    /**
     * Get partitioned topic metadata without checking the permission.
     */
    public static CompletableFuture<PartitionedTopicMetadata> unsafeGetPartitionedTopicMetadataAsync(
        PulsarService pulsar, TopicName topicName) {
        CompletableFuture<PartitionedTopicMetadata> metadataFuture = new CompletableFuture();

        // validates global-namespace contains local/peer cluster: if peer/local cluster present then lookup can
        // serve/redirect request else fail partitioned-metadata-request so, client fails while creating
        // producer/consumer
        checkLocalOrGetPeerReplicationCluster(pulsar, topicName.getNamespaceObject())
            .thenCompose(res -> pulsar.getBrokerService()
                .fetchPartitionedTopicMetadataCheckAllowAutoCreationAsync(topicName))
            .thenAccept(metadata -> {
                if (log.isDebugEnabled()) {
                    log.debug("Total number of partitions for topic {} is {}", topicName,
                        metadata.partitions);
                }
                metadataFuture.complete(metadata);
            }).exceptionally(ex -> {
            metadataFuture.completeExceptionally(ex.getCause());
            return null;
        });

        return metadataFuture;
    }

    /**
     * Get the Topic object reference from the Pulsar broker.
     */
    private Topic getTopicReference(TopicName topicName) {
        try {
            return pulsar().getBrokerService().getTopicIfExists(topicName.toString())
                    .get(pulsar().getConfiguration().getZooKeeperOperationTimeoutSeconds(), TimeUnit.SECONDS)
                    .orElseThrow(() -> topicNotFoundReason(topicName));
        } catch (RestException e) {
            throw e;
        } catch (Exception e) {
            throw new RestException(e);
        }
    }

    private CompletableFuture<Topic> getTopicReferenceAsync(TopicName topicName) {
        return pulsar().getBrokerService().getTopicIfExists(topicName.toString())
                .thenCompose(optTopic -> {
                    if (optTopic.isPresent()) {
                        return CompletableFuture.completedFuture(optTopic.get());
                    } else {
                        return topicNotFoundReasonAsync(topicName);
                    }
                });
    }

    private RestException topicNotFoundReason(TopicName topicName) {
        if (!topicName.isPartitioned()) {
            return new RestException(Status.NOT_FOUND, "Topic not found");
        }

        PartitionedTopicMetadata partitionedTopicMetadata = getPartitionedTopicMetadata(
                TopicName.get(topicName.getPartitionedTopicName()), false, false);
        if (partitionedTopicMetadata == null || partitionedTopicMetadata.partitions == 0) {
            final String topicErrorType = partitionedTopicMetadata
                    == null ? "has no metadata" : "has zero partitions";
            return new RestException(Status.NOT_FOUND, String.format(
                    "Partitioned Topic not found: %s %s", topicName.toString(), topicErrorType));
        } else if (!internalGetList().contains(topicName.toString())) {
            return new RestException(Status.NOT_FOUND, "Topic partitions were not yet created");
        }
        return new RestException(Status.NOT_FOUND, "Partitioned Topic not found");
    }

    private CompletableFuture<Topic> topicNotFoundReasonAsync(TopicName topicName) {
        if (!topicName.isPartitioned()) {
            return FutureUtil.failedFuture(new RestException(Status.NOT_FOUND, "Topic not found"));
        }

        return getPartitionedTopicMetadataAsync(
                TopicName.get(topicName.getPartitionedTopicName()), false, false)
                .thenApply(partitionedTopicMetadata -> {
                    if (partitionedTopicMetadata == null || partitionedTopicMetadata.partitions == 0) {
                        final String topicErrorType = partitionedTopicMetadata
                                == null ? "has no metadata" : "has zero partitions";
                        throw new RestException(Status.NOT_FOUND, String.format(
                                "Partitioned Topic not found: %s %s", topicName.toString(), topicErrorType));
                    } else if (!internalGetList().contains(topicName.toString())) {
                        throw new RestException(Status.NOT_FOUND, "Topic partitions were not yet created");
                    }
                    throw new RestException(Status.NOT_FOUND, "Partitioned Topic not found");
                });
    }

    private Topic getOrCreateTopic(TopicName topicName) {
        return pulsar().getBrokerService().getTopic(
                topicName.toString(), true).thenApply(Optional::get).join();
    }

    /**
     * Get the Subscription object reference from the Topic reference.
     */
    private Subscription getSubscriptionReference(String subName, PersistentTopic topic) {
        try {
            Subscription sub = topic.getSubscription(subName);
            if (sub == null) {
                sub = topic.createSubscription(subName,
                        InitialPosition.Earliest, false).get();
            }

            return checkNotNull(sub);
        } catch (Exception e) {
            throw new RestException(Status.NOT_FOUND, "Subscription not found");
        }
    }

    /**
     * Get the Replicator object reference from the Topic reference.
     */
    private PersistentReplicator getReplicatorReference(String replName, PersistentTopic topic) {
        try {
            String remoteCluster = PersistentReplicator.getRemoteCluster(replName);
            PersistentReplicator repl = (PersistentReplicator) topic.getPersistentReplicator(remoteCluster);
            return checkNotNull(repl);
        } catch (Exception e) {
            throw new RestException(Status.NOT_FOUND, "Replicator not found");
        }
    }

    private CompletableFuture<Void> updatePartitionedTopic(TopicName topicName, int numPartitions) {
        final String path = ZkAdminPaths.partitionedTopicPath(topicName);

        CompletableFuture<Void> updatePartition = new CompletableFuture<>();
        createSubscriptions(topicName, numPartitions).thenAccept(res -> {
            try {
                namespaceResources().getPartitionedTopicResources().set(path,
                        p -> new PartitionedTopicMetadata(numPartitions));
                updatePartition.complete(null);
            } catch (Exception e) {
                updatePartition.completeExceptionally(e);
            }
        }).exceptionally(ex -> {
            updatePartition.completeExceptionally(ex);
            return null;
        });

        return updatePartition;
    }

    /**
     * It creates subscriptions for new partitions of existing partitioned-topics.
     *
     * @param topicName     : topic-name: persistent://prop/cluster/ns/topic
     * @param numPartitions : number partitions for the topics
     */
    private CompletableFuture<Void> createSubscriptions(TopicName topicName, int numPartitions) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        pulsar().getBrokerService().fetchPartitionedTopicMetadataAsync(topicName).thenAccept(partitionMetadata -> {
            if (partitionMetadata.partitions < 1) {
                result.completeExceptionally(new RestException(Status.CONFLICT, "Topic is not partitioned topic"));
                return;
            }

            if (partitionMetadata.partitions >= numPartitions) {
                result.completeExceptionally(new RestException(Status.CONFLICT,
                        "number of partitions must be more than existing " + partitionMetadata.partitions));
                return;
            }

            PulsarAdmin admin;
            try {
                admin = pulsar().getAdminClient();
            } catch (PulsarServerException e1) {
                result.completeExceptionally(e1);
                return;
            }

            admin.topics().getStatsAsync(topicName.getPartition(0).toString()).thenAccept(stats -> {
                List<CompletableFuture<Void>> subscriptionFutures = new ArrayList<>();

                stats.getSubscriptions().entrySet().forEach(e -> {
                    String subscription = e.getKey();
                    SubscriptionStats ss = e.getValue();
                    if (!ss.isDurable()) {
                        // We must not re-create non-durable subscriptions on the new partitions
                        return;
                    }

                    for (int i = partitionMetadata.partitions; i < numPartitions; i++) {
                        final String topicNamePartition = topicName.getPartition(i).toString();

                        subscriptionFutures.add(admin.topics().createSubscriptionAsync(topicNamePartition,
                                subscription, MessageId.latest));
                    }
                });

                FutureUtil.waitForAll(subscriptionFutures).thenRun(() -> {
                    log.info("[{}] Successfully created new partitions {}", clientAppId(), topicName);
                    result.complete(null);
                }).exceptionally(ex -> {
                    log.warn("[{}] Failed to create subscriptions on new partitions for {}",
                            clientAppId(), topicName, ex);
                    result.completeExceptionally(ex);
                    return null;
                });
            }).exceptionally(ex -> {
                if (ex.getCause() instanceof PulsarAdminException.NotFoundException) {
                    // The first partition doesn't exist, so there are currently to subscriptions to recreate
                    result.complete(null);
                } else {
                    log.warn("[{}] Failed to get list of subscriptions of {}",
                            clientAppId(), topicName.getPartition(0), ex);
                    result.completeExceptionally(ex);
                }
                return null;
            });
        }).exceptionally(ex -> {
            log.warn("[{}] Failed to get partition metadata for {}",
                    clientAppId(), topicName.toString());
            result.completeExceptionally(ex);
            return null;
        });
        return result;
    }

    // as described at : (PR: #836) CPP-client old client lib should not be allowed to connect on partitioned-topic.
    // So, all requests from old-cpp-client (< v1.21) must be rejected.
    // Pulsar client-java lib always passes user-agent as X-Java-$version.
    // However, cpp-client older than v1.20 (PR #765) never used to pass it.
    // So, request without user-agent and Pulsar-CPP-vX (X < 1.21) must be rejected
    private void validateClientVersion() {
        if (!pulsar().getConfiguration().isClientLibraryVersionCheckEnabled()) {
            return;
        }
        final String userAgent = httpRequest.getHeader("User-Agent");
        if (StringUtils.isBlank(userAgent)) {
            throw new RestException(Status.METHOD_NOT_ALLOWED,
                    "Client lib is not compatible to"
                            + " access partitioned metadata: version in user-agent is not present");
        }
        // Version < 1.20 for cpp-client is not allowed
        if (userAgent.contains(DEPRECATED_CLIENT_VERSION_PREFIX)) {
            try {
                // Version < 1.20 for cpp-client is not allowed
                String[] tokens = userAgent.split(DEPRECATED_CLIENT_VERSION_PREFIX);
                String[] splits = tokens.length > 1 ? tokens[1].split("-")[0].trim().split("\\.") : null;
                if (splits != null && splits.length > 1) {
                    if (LEAST_SUPPORTED_CLIENT_VERSION_PREFIX.getMajorVersion() > Integer.parseInt(splits[0])
                            || LEAST_SUPPORTED_CLIENT_VERSION_PREFIX.getMinorVersion() > Integer.parseInt(splits[1])) {
                        throw new RestException(Status.METHOD_NOT_ALLOWED,
                                "Client lib is not compatible to access partitioned metadata: version " + userAgent
                                        + " is not supported");
                    }
                }
            } catch (RestException re) {
                throw re;
            } catch (Exception e) {
                log.warn("[{}] Failed to parse version {} ", clientAppId(), userAgent);
            }
        }
        return;
    }

    /**
     * Validate update of number of partition for partitioned topic.
     * If there's already non partition topic with same name and contains partition suffix "-partition-"
     * followed by numeric value X then the new number of partition of that partitioned topic can not be greater
     * than that X else that non partition topic will essentially be overwritten and cause unexpected consequence.
     *
     * @param topicName
     */
    private void validatePartitionTopicUpdate(String topicName, int numberOfPartition) {
        List<String> existingTopicList = internalGetList();
        TopicName partitionTopicName = TopicName.get(domain(), namespaceName, topicName);
        PartitionedTopicMetadata metadata = getPartitionedTopicMetadata(partitionTopicName, false, false);
        int oldPartition = metadata.partitions;
        String prefix = topicName + TopicName.PARTITIONED_TOPIC_SUFFIX;
        for (String exsitingTopicName : existingTopicList) {
            if (exsitingTopicName.contains(prefix)) {
                try {
                    long suffix = Long.parseLong(exsitingTopicName.substring(
                            exsitingTopicName.indexOf(TopicName.PARTITIONED_TOPIC_SUFFIX)
                                    + TopicName.PARTITIONED_TOPIC_SUFFIX.length()));
                    // Skip partition of partitioned topic by making sure
                    // the numeric suffix greater than old partition number.
                    if (suffix >= oldPartition && suffix <= (long) numberOfPartition) {
                        log.warn(
                                "[{}] Already have non partition topic {} which contains partition suffix"
                                        + " '-partition-' and end with numeric value smaller than the new number"
                                        + " of partition. Update of partitioned topic {} could cause conflict.",
                                clientAppId(),
                                exsitingTopicName, topicName);
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "Already have non partition topic" + exsitingTopicName
                                        + " which contains partition suffix '-partition-' "
                                        + "and end with numeric value and end with numeric value smaller than the new "
                                        + "number of partition. Update of partitioned topic "
                                        + topicName + " could cause conflict.");
                    }
                } catch (NumberFormatException e) {
                    // Do nothing, if value after partition suffix is not pure numeric value,
                    // as it can't conflict with internal created partitioned topic's name.
                }
            }
        }
    }

    /**
     * Validate non partition topic name,
     * Validation will fail and throw RestException if
     * 1) Topic name contains partition suffix "-partition-" and the remaining part follow the partition
     * suffix is numeric value larger than the number of partition if there's already a partition topic with same
     * name(the part before suffix "-partition-").
     * 2)Topic name contains partition suffix "-partition-" and the remaining part follow the partition
     * suffix is numeric value but there isn't a partitioned topic with same name.
     *
     * @param topicName
     */
    private void validateNonPartitionTopicName(String topicName) {
        if (topicName.contains(TopicName.PARTITIONED_TOPIC_SUFFIX)) {
            try {
                // First check if what's after suffix "-partition-" is number or not, if not number then can create.
                int partitionIndex = topicName.indexOf(TopicName.PARTITIONED_TOPIC_SUFFIX);
                long suffix = Long.parseLong(topicName.substring(partitionIndex
                        + TopicName.PARTITIONED_TOPIC_SUFFIX.length()));
                TopicName partitionTopicName = TopicName.get(domain(),
                        namespaceName, topicName.substring(0, partitionIndex));
                PartitionedTopicMetadata metadata = getPartitionedTopicMetadata(partitionTopicName, false, false);

                // Partition topic index is 0 to (number of partition - 1)
                if (metadata.partitions > 0 && suffix >= (long) metadata.partitions) {
                    log.warn("[{}] Can't create topic {} with \"-partition-\" followed by"
                                    + " a number smaller then number of partition of partitioned topic {}.",
                            clientAppId(), topicName, partitionTopicName.getLocalName());
                    throw new RestException(Status.PRECONDITION_FAILED,
                            "Can't create topic " + topicName + " with \"-partition-\" followed by"
                                    + " a number smaller then number of partition of partitioned topic "
                                    + partitionTopicName.getLocalName());
                } else if (metadata.partitions == 0) {
                    log.warn("[{}] Can't create topic {} with \"-partition-\" followed by"
                                    + " numeric value if there isn't a partitioned topic {} created.",
                            clientAppId(), topicName, partitionTopicName.getLocalName());
                    throw new RestException(Status.PRECONDITION_FAILED,
                            "Can't create topic " + topicName + " with \"-partition-\" followed by"
                                    + " numeric value if there isn't a partitioned topic "
                                    + partitionTopicName.getLocalName() + " created.");
                }
                // If there is a  partitioned topic with the same name and numeric suffix is smaller than the
                // number of partition for that partitioned topic, validation will pass.
            } catch (NumberFormatException e) {
                // Do nothing, if value after partition suffix is not pure numeric value,
                // as it can't conflict if user want to create partitioned topic with same
                // topic name prefix in the future.
            }
        }
    }

    protected void internalGetLastMessageId(AsyncResponse asyncResponse, boolean authoritative) {
        Topic topic;
        try {
            validateTopicOwnership(topicName, authoritative);
            validateTopicOperation(topicName, TopicOperation.PEEK_MESSAGES);
            topic = getTopicReference(topicName);
        } catch (WebApplicationException wae) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed to get last messageId {}, redirecting to other brokers.",
                        clientAppId(), topicName, wae);
            }
            resumeAsyncResponseExceptionally(asyncResponse, wae);
            return;
        } catch (Exception e) {
            log.error("[{}] Failed to get last messageId {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
            return;
        }

        if (!(topic instanceof PersistentTopic)) {
            log.error("[{}] Not supported operation of non-persistent topic {}", clientAppId(), topicName);
            asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED,
                    "GetLastMessageId on a non-persistent topic is not allowed"));
            return;
        }

        topic.getLastMessageId().whenComplete((v, e) -> {
            if (e != null) {
                asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage()));
            } else {
                asyncResponse.resume(v);
            }
        });

    }

    protected CompletableFuture<DispatchRateImpl> internalGetDispatchRate(boolean applied) {
        DispatchRateImpl dispatchRate = getTopicPolicies(topicName)
                .map(TopicPolicies::getDispatchRate)
                .orElseGet(() -> {
                    if (applied) {
                        DispatchRateImpl namespacePolicy = getNamespacePolicies(namespaceName)
                                .topicDispatchRate.get(pulsar().getConfiguration().getClusterName());
                        return namespacePolicy == null ? dispatchRate() : namespacePolicy;
                    }
                    return null;
                });
        return CompletableFuture.completedFuture(dispatchRate);
    }

    protected CompletableFuture<Void> internalSetDispatchRate(DispatchRateImpl dispatchRate) {
        if (dispatchRate == null) {
            return CompletableFuture.completedFuture(null);
        }
        TopicPolicies topicPolicies = getTopicPolicies(topicName)
            .orElseGet(TopicPolicies::new);
        topicPolicies.setDispatchRate(dispatchRate);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
    }

    protected CompletableFuture<Void> internalRemoveDispatchRate() {
        Optional<TopicPolicies> topicPolicies = getTopicPolicies(topicName);
        if (!topicPolicies.isPresent()) {
            return CompletableFuture.completedFuture(null);
        }
        topicPolicies.get().setDispatchRate(null);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies.get());

    }

    protected CompletableFuture<DispatchRate> internalGetSubscriptionDispatchRate(boolean applied) {
        DispatchRate dispatchRate = getTopicPolicies(topicName)
                .map(TopicPolicies::getSubscriptionDispatchRate)
                .orElseGet(() -> {
                    if (applied) {
                        DispatchRateImpl namespacePolicy = getNamespacePolicies(namespaceName)
                                .subscriptionDispatchRate.get(pulsar().getConfiguration().getClusterName());
                        return namespacePolicy == null ? subscriptionDispatchRate() : namespacePolicy;
                    }
                    return null;
                });
        return CompletableFuture.completedFuture(dispatchRate);
    }

    protected CompletableFuture<Void> internalSetSubscriptionDispatchRate(DispatchRateImpl dispatchRate) {
        if (dispatchRate == null) {
            return CompletableFuture.completedFuture(null);
        }
        TopicPolicies topicPolicies = getTopicPolicies(topicName)
            .orElseGet(TopicPolicies::new);
        topicPolicies.setSubscriptionDispatchRate(dispatchRate);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
    }

    protected CompletableFuture<Void> internalRemoveSubscriptionDispatchRate() {
        Optional<TopicPolicies> topicPolicies = getTopicPolicies(topicName);
        if (!topicPolicies.isPresent()) {
            return CompletableFuture.completedFuture(null);
        }
        topicPolicies.get().setSubscriptionDispatchRate(null);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies.get());
    }


    protected Optional<Integer> internalGetMaxConsumersPerSubscription() {
        return getTopicPolicies(topicName).map(TopicPolicies::getMaxConsumersPerSubscription);
    }

    protected CompletableFuture<Void> internalSetMaxConsumersPerSubscription(Integer maxConsumersPerSubscription) {
        if (maxConsumersPerSubscription != null && maxConsumersPerSubscription < 0) {
            throw new RestException(Status.PRECONDITION_FAILED, "Invalid value for maxConsumersPerSubscription");
        }

        TopicPolicies topicPolicies = getTopicPolicies(topicName)
                .orElseGet(TopicPolicies::new);
        topicPolicies.setMaxConsumersPerSubscription(maxConsumersPerSubscription);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
    }

    protected CompletableFuture<Void> internalRemoveMaxConsumersPerSubscription() {
        Optional<TopicPolicies> topicPolicies = getTopicPolicies(topicName);
        if (!topicPolicies.isPresent()) {
            return CompletableFuture.completedFuture(null);
        }
        topicPolicies.get().setMaxConsumersPerSubscription(null);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies.get());
    }

    protected CompletableFuture<Long> internalGetCompactionThreshold(boolean applied) {
        Long threshold = getTopicPolicies(topicName)
                .map(TopicPolicies::getCompactionThreshold)
                .orElseGet(() -> {
                    if (applied) {
                        Long namespacePolicy = getNamespacePolicies(namespaceName).compaction_threshold;
                        return namespacePolicy == null
                                ? pulsar().getConfiguration().getBrokerServiceCompactionThresholdInBytes()
                                : namespacePolicy;
                    }
                    return null;
                });
        return CompletableFuture.completedFuture(threshold);
    }

    protected CompletableFuture<Void> internalSetCompactionThreshold(Long compactionThreshold) {
        if (compactionThreshold != null && compactionThreshold < 0) {
            throw new RestException(Status.PRECONDITION_FAILED, "Invalid value for compactionThreshold");
        }

        TopicPolicies topicPolicies = getTopicPolicies(topicName)
            .orElseGet(TopicPolicies::new);
        topicPolicies.setCompactionThreshold(compactionThreshold);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
    }

    protected CompletableFuture<Void> internalRemoveCompactionThreshold() {
        Optional<TopicPolicies> topicPolicies = getTopicPolicies(topicName);
        if (!topicPolicies.isPresent()) {
          return CompletableFuture.completedFuture(null);
        }
        topicPolicies.get().setCompactionThreshold(null);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies.get());
    }

    protected Optional<PublishRate> internalGetPublishRate() {
        return getTopicPolicies(topicName).map(TopicPolicies::getPublishRate);

    }

    protected CompletableFuture<Void> internalSetPublishRate(PublishRate publishRate) {
        if (publishRate == null) {
            return CompletableFuture.completedFuture(null);
        }
        TopicPolicies topicPolicies = getTopicPolicies(topicName)
            .orElseGet(TopicPolicies::new);
        topicPolicies.setPublishRate(publishRate);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
    }

    protected Optional<List<SubType>> internalGetSubscriptionTypesEnabled() {
        return getTopicPolicies(topicName).map(TopicPolicies::getSubscriptionTypesEnabled);
    }

    protected CompletableFuture<Void> internalSetSubscriptionTypesEnabled(
            Set<SubscriptionType> subscriptionTypesEnabled) {
        List<SubType> subTypes = Lists.newArrayList();
        subscriptionTypesEnabled.forEach(subscriptionType -> subTypes.add(SubType.valueOf(subscriptionType.name())));
        TopicPolicies topicPolicies = getTopicPolicies(topicName)
                .orElseGet(TopicPolicies::new);
        topicPolicies.setSubscriptionTypesEnabled(subTypes);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
    }

    protected CompletableFuture<Void> internalRemovePublishRate() {
        Optional<TopicPolicies> topicPolicies = getTopicPolicies(topicName);
        if (!topicPolicies.isPresent()) {
            return CompletableFuture.completedFuture(null);
        }
        topicPolicies.get().setPublishRate(null);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies.get());
    }

    protected CompletableFuture<SubscribeRate> internalGetSubscribeRate(boolean applied) {
        SubscribeRate subscribeRate = getTopicPolicies(topicName)
                .map(TopicPolicies::getSubscribeRate)
                .orElseGet(() -> {
                    if (applied) {
                        SubscribeRate namespacePolicy = getNamespacePolicies(namespaceName)
                                .clusterSubscribeRate.get(pulsar().getConfiguration().getClusterName());
                        return namespacePolicy == null ? subscribeRate() : namespacePolicy;
                    }
                    return null;
                });
        return CompletableFuture.completedFuture(subscribeRate);
    }

    protected CompletableFuture<Void> internalSetSubscribeRate(SubscribeRate subscribeRate) {
        if (subscribeRate == null) {
            return CompletableFuture.completedFuture(null);
        }
        TopicPolicies topicPolicies = getTopicPolicies(topicName)
                .orElseGet(TopicPolicies::new);
        topicPolicies.setSubscribeRate(subscribeRate);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
    }

    protected CompletableFuture<Void> internalRemoveSubscribeRate() {
        Optional<TopicPolicies> topicPolicies = getTopicPolicies(topicName);
        if (!topicPolicies.isPresent()) {
            return CompletableFuture.completedFuture(null);
        }
        topicPolicies.get().setSubscribeRate(null);
        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies.get());
    }

    protected void internalHandleResult(AsyncResponse asyncResponse,
                                        Object res,
                                        Throwable ex,
                                        String errorMsg) {
        if (ex instanceof RestException) {
            log.error(errorMsg, ex);
            asyncResponse.resume(ex);
        } else if (ex != null) {
            log.error(errorMsg, ex);
            asyncResponse.resume(new RestException(ex));
        } else {
            if (res == null) {
                asyncResponse.resume(Response.noContent().build());
            } else {
                asyncResponse.resume(res);
            }
        }
    }

    protected void internalTruncateNonPartitionedTopic(AsyncResponse asyncResponse, boolean authoritative) {
        Topic topic;
        try {
            validateAdminAccessForTenant(topicName.getTenant());
            validateTopicOwnership(topicName, authoritative);
            topic = getTopicReference(topicName);
        } catch (Exception e) {
            log.error("[{}] Failed to truncate topic {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
            return;
        }
        CompletableFuture<Void> future = topic.truncate();
        future.thenAccept(a -> {
            asyncResponse.resume(new RestException(Response.Status.NO_CONTENT.getStatusCode(),
                    Response.Status.NO_CONTENT.getReasonPhrase()));
        }).exceptionally(e -> {
            asyncResponse.resume(e);
            return null;
        });
    }

    protected void internalTruncateTopic(AsyncResponse asyncResponse, boolean authoritative) {

        // If the topic name is a partition name, no need to get partition topic metadata again
        if (topicName.isPartitioned()) {
            internalTruncateNonPartitionedTopic(asyncResponse, authoritative);
        } else {
            getPartitionedTopicMetadataAsync(topicName, authoritative, false).whenComplete((meta, t) -> {
                if (meta.partitions > 0) {
                    final List<CompletableFuture<Void>> futures = Lists.newArrayList();
                    for (int i = 0; i < meta.partitions; i++) {
                        TopicName topicNamePartition = topicName.getPartition(i);
                        try {
                            futures.add(pulsar().getAdminClient().topics()
                                .truncateAsync(topicNamePartition.toString()));
                        } catch (Exception e) {
                            log.error("[{}] Failed to truncate topic {}", clientAppId(), topicNamePartition, e);
                            asyncResponse.resume(new RestException(e));
                            return;
                        }
                    }
                    FutureUtil.waitForAll(futures).handle((result, exception) -> {
                        if (exception != null) {
                            Throwable th = exception.getCause();
                            if (th instanceof NotFoundException) {
                                asyncResponse.resume(new RestException(Status.NOT_FOUND, th.getMessage()));
                            } else if (th instanceof WebApplicationException) {
                                asyncResponse.resume(th);
                            } else {
                                log.error("[{}] Failed to truncate topic {}", clientAppId(), topicName, exception);
                                asyncResponse.resume(new RestException(exception));
                            }
                        } else {
                            asyncResponse.resume(Response.noContent().build());
                        }
                        return null;
                    });
                } else {
                    internalTruncateNonPartitionedTopic(asyncResponse, authoritative);
                }
            }).exceptionally(t -> {
                log.error("[{}] Failed to truncate topic {}", clientAppId(), topicName, t);
                if (t instanceof WebApplicationException) {
                    asyncResponse.resume(t);
                } else {
                    asyncResponse.resume(new RestException(t));
                }
                return null;
            });
        }
    }
}
