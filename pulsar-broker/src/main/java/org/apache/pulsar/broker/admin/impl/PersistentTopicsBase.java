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

import static org.apache.pulsar.broker.PulsarService.isTransactionInternalName;
import static org.apache.pulsar.broker.resources.PulsarResources.DEFAULT_OPERATION_TIMEOUT_SEC;
import static org.apache.pulsar.common.events.EventsTopicNames.checkTopicIsTransactionCoordinatorAssign;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.zafarkhaja.semver.Version;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerOfflineBacklog;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authorization.AuthorizationService;
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
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.EncryptionKeys;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.NamespaceBundle;
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
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
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
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class PersistentTopicsBase extends AdminResource {
    private static final Logger log = LoggerFactory.getLogger(PersistentTopicsBase.class);

    private static final int OFFLINE_TOPIC_STAT_TTL_MINS = 10;
    private static final String DEPRECATED_CLIENT_VERSION_PREFIX = "Pulsar-CPP-v";
    private static final Version LEAST_SUPPORTED_CLIENT_VERSION_PREFIX = Version.forIntegers(1, 21);

    protected List<String> internalGetList(Optional<String> bundle) {
        validateNamespaceOperation(namespaceName, NamespaceOperation.GET_TOPICS);

        // Validate that namespace exists, throws 404 if it doesn't exist
        try {
            if (!namespaceResources().namespaceExists(namespaceName)) {
                throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
            }
        } catch (RestException re) {
            throw re;
        } catch (Exception e) {
            log.error("[{}] Failed to get topic list {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }

        try {
            List<String> topics = topicResources().listPersistentTopicsAsync(namespaceName).join();
            return topics.stream().filter(topic -> {
                if (isTransactionInternalName(TopicName.get(topic))) {
                    return false;
                }
                if (bundle.isPresent()) {
                    NamespaceBundle b = pulsar().getNamespaceService().getNamespaceBundleFactory()
                            .getBundle(TopicName.get(topic));
                    return b != null && bundle.get().equals(b.getBundleRange());
                }
                return true;
            }).collect(Collectors.toList());
        } catch (Exception e) {
            log.error("[{}] Failed to get topics list for namespace {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    protected List<String> internalGetPartitionedTopicList() {
        validateNamespaceOperation(namespaceName, NamespaceOperation.GET_TOPICS);
        // Validate that namespace exists, throws 404 if it doesn't exist
        try {
            if (!namespaceResources().namespaceExists(namespaceName)) {
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
            Policies policies = namespaceResources().getPolicies(namespaceName)
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

    protected void validateCreateTopic(TopicName topicName) {
        if (isTransactionInternalName(topicName)) {
            log.warn("Forbidden to create transaction internal topic: {}", topicName);
            throw new RestException(Status.BAD_REQUEST, "Cannot create topic in system topic format!");
        }
    }

    public void validateAdminOperationOnTopic(boolean authoritative) {
        validateAdminAccessForTenant(topicName.getTenant());
        validateTopicOwnership(topicName, authoritative);
    }

    private void grantPermissions(TopicName topicUri, String role, Set<AuthAction> actions) {
        try {
            AuthorizationService authService = pulsar().getBrokerService().getAuthorizationService();
            if (null != authService) {
                authService.grantPermissionAsync(topicUri, actions, role, null/*additional auth-data json*/).get();
            } else {
                throw new RestException(Status.NOT_IMPLEMENTED, "Authorization is not enabled");
            }
            log.info("[{}] Successfully granted access for role {}: {} - topic {}", clientAppId(), role, actions,
                    topicUri);
        } catch (InterruptedException e) {
            log.error("[{}] Failed to get permissions for topic {}", clientAppId(), topicUri, e);
            throw new RestException(e);
        } catch (ExecutionException e) {
            // The IllegalArgumentException and the IllegalStateException were historically thrown by the
            // grantPermissionAsync method, so we catch them here to ensure backwards compatibility.
            if (e.getCause() instanceof MetadataStoreException.NotFoundException
                    || e.getCause() instanceof IllegalArgumentException) {
                log.warn("[{}] Failed to set permissions for topic {}: Namespace does not exist", clientAppId(),
                        topicUri, e);
                throw new RestException(Status.NOT_FOUND, "Topic's namespace does not exist");
            } else if (e.getCause() instanceof MetadataStoreException.BadVersionException
                    || e.getCause() instanceof IllegalStateException) {
                log.warn("[{}] Failed to set permissions for topic {}: {}",
                        clientAppId(), topicUri, e.getCause().getMessage(), e);
                throw new RestException(Status.CONFLICT, "Concurrent modification");
            } else {
                log.error("[{}] Failed to get permissions for topic {}", clientAppId(), topicUri, e);
                throw new RestException(e);
            }
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
                grantPermissions(topicNamePartition, role, actions);
            }
        }
        grantPermissions(topicName, role, actions);
    }

    protected void internalDeleteTopicForcefully(boolean authoritative, boolean deleteSchema) {
        validateTopicOwnership(topicName, authoritative);
        validateNamespaceOperation(topicName.getNamespaceObject(), NamespaceOperation.DELETE_TOPIC);

        try {
            pulsar().getBrokerService().deleteTopic(topicName.toString(), true, deleteSchema).get();
        } catch (Exception e) {
            if (isManagedLedgerNotFoundException(e)) {
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
            policies = namespaceResources().getPolicies(namespaceName)
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
            // Write the new policies to metadata store
            namespaceResources().setPolicies(namespaceName, p -> {
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

    protected void internalCreateNonPartitionedTopic(boolean authoritative, Map<String, String> properties) {
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

            Topic createdTopic = getOrCreateTopic(topicName, properties);
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
                                                  boolean updateLocalTopicOnly, boolean authoritative,
                                                  boolean force) {
        if (numPartitions <= 0) {
            throw new RestException(Status.NOT_ACCEPTABLE, "Number of partitions should be more than 0");
        }

        validateTopicOwnership(topicName, authoritative);
        validateTopicPolicyOperation(topicName, PolicyName.PARTITION, PolicyOperation.WRITE);
        // Only do the validation if it's the first hop.
        if (!updateLocalTopicOnly && !force) {
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
                updatePartitionInOtherCluster(numPartitions, clusters).thenRun(() -> {
                    try {
                        namespaceResources().getPartitionedTopicResources()
                                .updatePartitionedTopicAsync(topicName, p ->
                            new PartitionedTopicMetadata(numPartitions)
                        ).thenAccept(r -> updatePartition.complete(null)).exceptionally(ex -> {
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

        try {
            tryCreatePartitionsAsync(numPartitions).get(DEFAULT_OPERATION_TIMEOUT_SEC, TimeUnit.SECONDS);
            updatePartitionedTopic(topicName, numPartitions, force).get(DEFAULT_OPERATION_TIMEOUT_SEC,
                    TimeUnit.SECONDS);
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
        }).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (!isRedirectException(ex)) {
                log.error("[{}] Failed to create partitions for topic {}",
                        clientAppId(), topicName);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    protected CompletableFuture<Void> internalSetDelayedDeliveryPolicies(DelayedDeliveryPolicies deliveryPolicies,
                                                                         boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setIsGlobal(isGlobal);
                topicPolicies.setDelayedDeliveryEnabled(deliveryPolicies == null ? null : deliveryPolicies.isActive());
                topicPolicies.setDelayedDeliveryTickTimeMillis(
                        deliveryPolicies == null ? null : deliveryPolicies.getTickTime());
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    private CompletableFuture<Void> updatePartitionInOtherCluster(int numPartitions, Set<String> clusters) {
        List<CompletableFuture<Void>> results = new ArrayList<>(clusters.size() - 1);
        clusters.forEach(cluster -> {
            if (cluster.equals(pulsar().getConfig().getClusterName())) {
                return;
            }
            CompletableFuture<Void> updatePartitionTopicFuture =
                pulsar().getPulsarResources().getClusterResources().getClusterAsync(cluster)
                    .thenApply(clusterDataOp ->
                            pulsar().getBrokerService().getClusterPulsarAdmin(cluster, clusterDataOp))
                    .thenCompose(pulsarAdmin ->
                            pulsarAdmin.topics().updatePartitionedTopicAsync(
                                    topicName.toString(), numPartitions, true, false));
            results.add(updatePartitionTopicFuture);
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
        validateNamespaceOperationAsync(topicName.getNamespaceObject(), NamespaceOperation.DELETE_TOPIC)
                .thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative))
                .thenCompose(__ -> pulsar().getBrokerService()
                        .fetchPartitionedTopicMetadataAsync(topicName)
                        .thenCompose(partitionedMeta -> {
                            final int numPartitions = partitionedMeta.partitions;
                            if (numPartitions < 1){
                                return CompletableFuture.completedFuture(null);
                            }
                            if (deleteSchema) {
                                return pulsar().getBrokerService()
                                        .deleteSchemaStorage(topicName.getPartition(0).toString())
                                        .thenCompose(unused ->
                                                internalRemovePartitionsAuthenticationPoliciesAsync(numPartitions))
                                        .thenCompose(unused2 ->
                                                internalRemovePartitionsTopicAsync(numPartitions, force));
                            }
                            return internalRemovePartitionsAuthenticationPoliciesAsync(numPartitions)
                                    .thenCompose(unused -> internalRemovePartitionsTopicAsync(numPartitions, force));
                        })
                // Only tries to delete the znode for partitioned topic when all its partitions are successfully deleted
                ).thenCompose(__ -> namespaceResources()
                                .getPartitionedTopicResources().deletePartitionedTopicAsync(topicName))
                .thenAccept(__ -> {
                    log.info("[{}] Deleted partitioned topic {}", clientAppId(), topicName);
                    asyncResponse.resume(Response.noContent().build());
                }).exceptionally(ex -> {
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    if (realCause instanceof PreconditionFailedException) {
                        asyncResponse.resume(
                                new RestException(Status.PRECONDITION_FAILED,
                                        "Topic has active producers/subscriptions"));
                    } else if (realCause instanceof WebApplicationException){
                        asyncResponse.resume(realCause);
                    } else if (realCause instanceof MetadataStoreException.NotFoundException) {
                        log.warn("Namespace policies of {} not found", topicName.getNamespaceObject());
                        asyncResponse.resume(new RestException(
                                new RestException(Status.NOT_FOUND, "Partitioned topic does not exist")));
                    } else if (realCause instanceof PulsarAdminException) {
                        asyncResponse.resume(new RestException((PulsarAdminException) realCause));
                    } else if (realCause instanceof MetadataStoreException.BadVersionException) {
                        asyncResponse.resume(new RestException(
                                new RestException(Status.CONFLICT, "Concurrent modification")));
                    } else {
                        // If the exception is not redirect exception we need to log it.
                        if (!isRedirectException(ex)) {
                            log.error("[{}] Fail to Delete partitioned topic {}", clientAppId(), topicName, realCause);
                        }
                        asyncResponse.resume(new RestException(realCause));
                    }
                    return null;
                });
    }

    private CompletableFuture<Void> internalRemovePartitionsTopicAsync(int numPartitions, boolean force) {
        return FutureUtil.waitForAll(IntStream.range(0, numPartitions)
                .mapToObj(i -> {
                    TopicName topicNamePartition = topicName.getPartition(i);
                    try {
                        CompletableFuture<Void> future = new CompletableFuture<>();
                        pulsar().getAdminClient().topics()
                                .deleteAsync(topicNamePartition.toString(), force)
                                .whenComplete((r, ex) -> {
                                    if (ex != null) {
                                        Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                                        if (realCause instanceof NotFoundException){
                                            // if the sub-topic is not found, the client might not have called
                                            // create producer or it might have been deleted earlier,
                                            // so we ignore the 404 error.
                                            // For all other exception,
                                            // we fail the delete partition method even if a single
                                            // partition is failed to be deleted
                                            if (log.isDebugEnabled()) {
                                                log.debug("[{}] Partition not found: {}", clientAppId(),
                                                        topicNamePartition);
                                            }
                                            future.complete(null);
                                        } else {
                                            log.error("[{}] Failed to delete partition {}", clientAppId(),
                                                    topicNamePartition, realCause);
                                            future.completeExceptionally(realCause);
                                        }
                                    } else {
                                        future.complete(null);
                                    }
                                });
                        return future;
                    } catch (PulsarServerException ex) {
                        log.error("[{}] Failed to get admin client while delete partition {}",
                                clientAppId(), topicNamePartition, ex);
                        return FutureUtil.failedFuture(ex);
                    }
                }).collect(Collectors.toList()));
    }

    private CompletableFuture<Void> internalRemovePartitionsAuthenticationPoliciesAsync(int numPartitions) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        pulsar().getPulsarResources().getNamespaceResources()
                .setPoliciesAsync(topicName.getNamespaceObject(), p -> {
                    IntStream.range(0, numPartitions)
                            .forEach(i -> p.auth_policies.getTopicAuthentication()
                                    .remove(topicName.getPartition(i).toString()));
                    p.auth_policies.getTopicAuthentication().remove(topicName.toString());
                    return p;
                })
                .whenComplete((r, ex) -> {
                    if (ex != null){
                        Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                        if (realCause instanceof MetadataStoreException.NotFoundException) {
                            log.warn("Namespace policies of {} not found", topicName.getNamespaceObject());
                            future.complete(null);
                        } else {
                            log.error("Failed to delete authentication policies for partitioned topic {}",
                                    topicName, ex);
                            future.completeExceptionally(realCause);
                        }
                    } else {
                        log.info("Successfully delete authentication policies for partitioned topic {}", topicName);
                        future.complete(null);
                    }
                });
        return future;
    }

    protected void internalUnloadTopic(AsyncResponse asyncResponse, boolean authoritative) {
        log.info("[{}] Unloading topic {}", clientAppId(), topicName);
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }
       future.thenAccept(__ -> {
           // If the topic name is a partition name, no need to get partition topic metadata again
           if (topicName.isPartitioned()) {
               if (checkTopicIsTransactionCoordinatorAssign(topicName)) {
                   internalUnloadTransactionCoordinatorAsync(asyncResponse, authoritative);
               } else {
                   internalUnloadNonPartitionedTopicAsync(asyncResponse, authoritative);
               }
           } else {
               getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                       .thenAccept(meta -> {
                           if (meta.partitions > 0) {
                               final List<CompletableFuture<Void>> futures =
                                       Lists.newArrayListWithCapacity(meta.partitions);
                               for (int i = 0; i < meta.partitions; i++) {
                                   TopicName topicNamePartition = topicName.getPartition(i);
                                   try {
                                       futures.add(pulsar().getAdminClient().topics().unloadAsync(
                                               topicNamePartition.toString()));
                                   } catch (Exception e) {
                                       log.error("[{}] Failed to unload topic {}", clientAppId(),
                                               topicNamePartition, e);
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
                               internalUnloadNonPartitionedTopicAsync(asyncResponse, authoritative);
                           }
                       }).exceptionally(ex -> {
                           // If the exception is not redirect exception we need to log it.
                           if (!isRedirectException(ex)) {
                               log.error("[{}] Failed to get partitioned metadata while unloading topic {}",
                                       clientAppId(), topicName, ex);
                           }
                           resumeAsyncResponseExceptionally(asyncResponse, ex);
                           return null;
                       });
           }
       }).exceptionally(ex -> {
           // If the exception is not redirect exception we need to log it.
           if (!isRedirectException(ex)) {
               log.error("[{}] Failed to validate the global namespace ownership while unloading topic {}",
                       clientAppId(), topicName, ex);
           }
           resumeAsyncResponseExceptionally(asyncResponse, ex);
           return null;
       });
    }

    protected CompletableFuture<DelayedDeliveryPolicies> internalGetDelayedDeliveryPolicies(boolean applied,
                                                                                            boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> {
                TopicPolicies policies = op.orElseGet(TopicPolicies::new);
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
                return delayedDeliveryPolicies;
            });
    }

    protected CompletableFuture<OffloadPoliciesImpl> internalGetOffloadPolicies(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> {
                OffloadPoliciesImpl offloadPolicies = op.map(TopicPolicies::getOffloadPolicies).orElse(null);
                if (applied) {
                    OffloadPoliciesImpl namespacePolicy =
                            (OffloadPoliciesImpl) getNamespacePolicies(namespaceName).offload_policies;
                    offloadPolicies = OffloadPoliciesImpl.mergeConfiguration(offloadPolicies
                            , namespacePolicy, pulsar().getConfiguration().getProperties());
                }
                return offloadPolicies;
            });
    }

    protected CompletableFuture<Void> internalSetOffloadPolicies
            (OffloadPoliciesImpl offloadPolicies, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setOffloadPolicies(offloadPolicies);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            }).thenCompose(__ -> {
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
            });
    }

    protected CompletableFuture<InactiveTopicPolicies> internalGetInactiveTopicPolicies
            (boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getInactiveTopicPolicies)
                .orElseGet(() -> {
                    if (applied) {
                        InactiveTopicPolicies policies = getNamespacePolicies(namespaceName).inactive_topic_policies;
                        return policies == null ? new InactiveTopicPolicies(
                                config().getBrokerDeleteInactiveTopicsMode(),
                                config().getBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds(),
                                config().isBrokerDeleteInactiveTopicsEnabled()) : policies;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetInactiveTopicPolicies
            (InactiveTopicPolicies inactiveTopicPolicies, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setIsGlobal(isGlobal);
                topicPolicies.setInactiveTopicPolicies(inactiveTopicPolicies);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
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

    protected CompletableFuture<Integer> internalGetMaxUnackedMessagesOnSubscription(boolean applied,
                                                                                     boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getMaxUnackedMessagesOnSubscription)
                .orElseGet(() -> {
                    if (applied) {
                        Integer maxUnackedNum = getNamespacePolicies(namespaceName)
                                .max_unacked_messages_per_subscription;
                        return maxUnackedNum == null ? config().getMaxUnackedMessagesPerSubscription() : maxUnackedNum;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetMaxUnackedMessagesOnSubscription(Integer maxUnackedNum,
                                                                                  boolean isGlobal) {
        if (maxUnackedNum != null && maxUnackedNum < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxUnackedNum must be 0 or more");
        }

        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setMaxUnackedMessagesOnSubscription(maxUnackedNum);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Integer> internalGetMaxUnackedMessagesOnConsumer(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getMaxUnackedMessagesOnConsumer)
                .orElseGet(() -> {
                    if (applied) {
                        Integer maxUnacked = getNamespacePolicies(namespaceName).max_unacked_messages_per_consumer;
                        return maxUnacked == null ? config().getMaxUnackedMessagesPerConsumer() : maxUnacked;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetMaxUnackedMessagesOnConsumer(Integer maxUnackedNum,
                                                                              boolean isGlobal) {
        if (maxUnackedNum != null && maxUnackedNum < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxUnackedNum must be 0 or more");
        }

        return getTopicPoliciesAsyncWithRetry(topicName)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setMaxUnackedMessagesOnConsumer(maxUnackedNum);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Void> internalSetDeduplicationSnapshotInterval(Integer interval, boolean isGlobal) {
        if (interval != null && interval < 0) {
            throw new RestException(Status.PRECONDITION_FAILED, "interval must be 0 or more");
        }
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies policies = op.orElseGet(TopicPolicies::new);
                policies.setDeduplicationSnapshotIntervalSeconds(interval);
                policies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, policies);
            });
    }

    private void internalUnloadNonPartitionedTopicAsync(AsyncResponse asyncResponse, boolean authoritative) {
        validateTopicOperationAsync(topicName, TopicOperation.UNLOAD)
                .thenCompose(unused -> validateTopicOwnershipAsync(topicName, authoritative)
                        .thenCompose(__ -> getTopicReferenceAsync(topicName))
                        .thenCompose(topic -> topic.close(false))
                        .thenRun(() -> {
                            log.info("[{}] Successfully unloaded topic {}", clientAppId(), topicName);
                            asyncResponse.resume(Response.noContent().build());
                        }))
                .exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to unload topic {}, {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    private void internalUnloadTransactionCoordinatorAsync(AsyncResponse asyncResponse, boolean authoritative) {
        validateTopicOperationAsync(topicName, TopicOperation.UNLOAD)
                .thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative)
                        .thenCompose(v -> pulsar()
                                .getTransactionMetadataStoreService()
                                .removeTransactionMetadataStore(
                                        TransactionCoordinatorID.get(topicName.getPartitionIndex())))
                        .thenRun(() -> {
                            log.info("[{}] Successfully unloaded tc {}", clientAppId(),
                                    topicName.getPartitionIndex());
                            asyncResponse.resume(Response.noContent().build());
                        }))
                .exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to unload tc {},{}", clientAppId(),
                                topicName.getPartitionIndex(), ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
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
            } else if (isManagedLedgerNotFoundException(e)) {
                throw new RestException(Status.NOT_FOUND, "Topic not found");
            } else {
                throw new RestException(t);
            }
        }
    }

    protected void internalGetSubscriptions(AsyncResponse asyncResponse, boolean authoritative) {
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }
        future.thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative))
                .thenAccept(unused -> {
                    // If the topic name is a partition name, no need to get partition topic metadata again
                    if (topicName.isPartitioned()) {
                        internalGetSubscriptionsForNonPartitionedTopic(asyncResponse, authoritative);
                    } else {
                        getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                                .thenAccept(partitionMetadata -> {
                            if (partitionMetadata.partitions > 0) {
                                try {
                                    final Set<String> subscriptions =
                                            Collections.newSetFromMap(
                                                    new ConcurrentHashMap<>(partitionMetadata.partitions));
                                    final List<CompletableFuture<Object>> subscriptionFutures = Lists.newArrayList();
                                    if (topicName.getDomain() == TopicDomain.persistent) {
                                        final Map<Integer, CompletableFuture<Boolean>> existsFutures =
                                                new ConcurrentHashMap<>(partitionMetadata.partitions);
                                        for (int i = 0; i < partitionMetadata.partitions; i++) {
                                            existsFutures.put(i,
                                                    topicResources().persistentTopicExists(topicName.getPartition(i)));
                                        }
                                        FutureUtil.waitForAll(Lists.newArrayList(existsFutures.values()))
                                                .thenApply(__ ->
                                                existsFutures.entrySet().stream().filter(e -> e.getValue().join())
                                                        .map(item -> topicName.getPartition(item.getKey()).toString())
                                                        .collect(Collectors.toList())
                                        ).thenAccept(topics -> {
                                            if (log.isDebugEnabled()) {
                                                log.debug("activeTopics : {}", topics);
                                            }
                                            topics.forEach(topic -> {
                                                try {
                                                    CompletableFuture<List<String>> subscriptionsAsync = pulsar()
                                                            .getAdminClient()
                                                            .topics().getSubscriptionsAsync(topic);
                                                    subscriptionFutures.add(subscriptionsAsync
                                                            .thenApply(subscriptions::addAll));
                                                } catch (PulsarServerException e) {
                                                    throw new RestException(e);
                                                }
                                            });
                                        }).thenAccept(__ -> resumeAsyncResponse(asyncResponse,
                                                        subscriptions, subscriptionFutures));
                                    } else {
                                        for (int i = 0; i < partitionMetadata.partitions; i++) {
                                            CompletableFuture<List<String>> subscriptionsAsync = pulsar()
                                                    .getAdminClient().topics()
                                                    .getSubscriptionsAsync(topicName.getPartition(i).toString());
                                            subscriptionFutures.add(subscriptionsAsync
                                                    .thenApply(subscriptions::addAll));
                                        }
                                        resumeAsyncResponse(asyncResponse, subscriptions, subscriptionFutures);
                                    }
                                } catch (Exception e) {
                                    log.error("[{}] Failed to get list of subscriptions for {}",
                                            clientAppId(), topicName, e);
                                    asyncResponse.resume(e);
                                }
                            } else {
                                internalGetSubscriptionsForNonPartitionedTopic(asyncResponse, authoritative);
                            }
                        }).exceptionally(ex -> {
                            // If the exception is not redirect exception we need to log it.
                            if (!isRedirectException(ex)) {
                                log.error("[{}] Failed to get partitioned topic metadata while get"
                                        + " subscriptions for topic {}", clientAppId(), topicName, ex);
                            }
                            resumeAsyncResponseExceptionally(asyncResponse, ex);
                            return null;
                        });
                    }
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to validate the global namespace/topic ownership while get subscriptions"
                                + " for topic {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    private void resumeAsyncResponse(AsyncResponse asyncResponse, Set<String> subscriptions,
                                     List<CompletableFuture<Object>> subscriptionFutures) {
        FutureUtil.waitForAll(subscriptionFutures).whenComplete((r, ex) -> {
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
            } else {
                asyncResponse.resume(new ArrayList<>(subscriptions));
            }
        });
    }

    private void internalGetSubscriptionsForNonPartitionedTopic(AsyncResponse asyncResponse, boolean authoritative) {
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.GET_SUBSCRIPTIONS))
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenAccept(topic -> asyncResponse.resume(Lists.newArrayList(topic.getSubscriptions().keys())))
                .exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to get list of subscriptions for {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    protected TopicStats internalGetStats(boolean authoritative, boolean getPreciseBacklog,
                                          boolean subscriptionBacklogSize, boolean getEarliestTimeInBacklog) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        validateTopicOwnership(topicName, authoritative);
        validateTopicOperation(topicName, TopicOperation.GET_STATS);

        Topic topic = getTopicReference(topicName);
        try {
            return topic.asyncGetStats(getPreciseBacklog, subscriptionBacklogSize, getEarliestTimeInBacklog).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("[{}] Failed to get stats for {}", clientAppId(), topicName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR,
                    (e instanceof ExecutionException) ? e.getCause().getMessage() : e.getMessage());
        }
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
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }
        future.thenAccept(__ -> {
            // If the topic name is a partition name, no need to get partition topic metadata again
            if (topicName.isPartitioned()) {
                internalGetManagedLedgerInfoForNonPartitionedTopic(asyncResponse);
            } else {
                getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                        .thenAccept(partitionMetadata -> {
                    if (partitionMetadata.partitions > 0) {
                        final List<CompletableFuture<String>> futures =
                                Lists.newArrayListWithCapacity(partitionMetadata.partitions);
                        PartitionedManagedLedgerInfo partitionedManagedLedgerInfo = new PartitionedManagedLedgerInfo();
                        for (int i = 0; i < partitionMetadata.partitions; i++) {
                            TopicName topicNamePartition = topicName.getPartition(i);
                            try {
                                futures.add(pulsar().getAdminClient().topics()
                                        .getInternalInfoAsync(topicNamePartition.toString())
                                        .whenComplete((response, throwable) -> {
                                            if (throwable != null) {
                                                log.error("[{}] Failed to get managed info for {}",
                                                        clientAppId(), topicNamePartition, throwable);
                                                asyncResponse.resume(new RestException(throwable));
                                            }
                                            try {
                                                partitionedManagedLedgerInfo.partitions
                                                        .put(topicNamePartition.toString(), jsonMapper()
                                                                .readValue(response, ManagedLedgerInfo.class));
                                            } catch (JsonProcessingException ex) {
                                                log.error("[{}] Failed to parse ManagedLedgerInfo for {} from [{}]",
                                                        clientAppId(), topicNamePartition, response, ex);
                                            }
                                        })
                                );
                            } catch (PulsarServerException e) {
                                log.error("[{}] Failed to get admin client while get managed info for {}" ,
                                        clientAppId(), topicNamePartition, e);
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
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to get partitioned metadata while get managed info for {}",
                                clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
            }
        }).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (!isRedirectException(ex)) {
                log.error("[{}] Failed to validate the global namespace ownership while get managed info for {}",
                        clientAppId(), topicName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    protected void internalGetManagedLedgerInfoForNonPartitionedTopic(AsyncResponse asyncResponse) {
        validateTopicOperationAsync(topicName, TopicOperation.GET_STATS)
                .thenAccept(__ -> {
                    String managedLedger = topicName.getPersistenceNamingEncoding();
                    pulsar().getManagedLedgerFactory()
                            .asyncGetManagedLedgerInfo(managedLedger, new ManagedLedgerInfoCallback() {
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
                }).exceptionally(ex -> {
                    log.error("[{}] Failed to get managed info for {}", clientAppId(), topicName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });

    }

    protected void internalGetPartitionedStats(AsyncResponse asyncResponse, boolean authoritative, boolean perPartition,
                                               boolean getPreciseBacklog, boolean subscriptionBacklogSize,
                                               boolean getEarliestTimeInBacklog) {
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }
        future.thenCompose(__ -> getPartitionedTopicMetadataAsync(topicName,
                authoritative, false)).thenAccept(partitionMetadata -> {
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
                                    (topicName.getPartition(i).toString()), getPreciseBacklog, subscriptionBacklogSize,
                                    getEarliestTimeInBacklog));
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
                                stats.getPartitions().put(topicName.getPartition(i).toString(),
                                        (TopicStatsImpl) statFuture.get());
                            }
                        } catch (Exception e) {
                            asyncResponse.resume(new RestException(e));
                            return null;
                        }
                    }
                }
                if (perPartition && stats.partitions.isEmpty()) {
                    try {
                        boolean pathExists = namespaceResources().getPartitionedTopicResources()
                                .partitionedTopicExists(topicName);
                        if (pathExists) {
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
            // If the exception is not redirect exception we need to log it.
            if (!isRedirectException(ex)) {
                log.error("[{}] Failed to get partitioned internal stats for {}", clientAppId(), topicName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    protected void internalGetPartitionedStatsInternal(AsyncResponse asyncResponse, boolean authoritative) {
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }
        future.thenCompose(__ -> getPartitionedTopicMetadataAsync(topicName, authoritative, false))
                .thenAccept(partitionMetadata -> {
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
            // If the exception is not redirect exception we need to log it.
            if (!isRedirectException(ex)) {
                log.error("[{}] Failed to get partitioned internal stats for {}", clientAppId(), topicName, ex);
            }
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
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }

        future.thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative)).thenAccept(__ -> {
            if (topicName.isPartitioned()) {
                internalDeleteSubscriptionForNonPartitionedTopic(asyncResponse, subName, authoritative);
            } else {
                getPartitionedTopicMetadataAsync(topicName,
                        authoritative, false).thenAcceptAsync(partitionMetadata -> {
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
                }, pulsar().getExecutor()).exceptionally(ex -> {
                    log.error("[{}] Failed to delete subscription {} from topic {}",
                            clientAppId(), subName, topicName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
            }
        }).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (!isRedirectException(ex)) {
                log.error("[{}] Failed to delete subscription {} from topic {}",
                        clientAppId(), subName, topicName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    private void internalDeleteSubscriptionForNonPartitionedTopic(AsyncResponse asyncResponse,
                                                                  String subName, boolean authoritative) {
        validateTopicOwnershipAsync(topicName, authoritative)
            .thenRun(() -> validateTopicOperation(topicName, TopicOperation.UNSUBSCRIBE))
            .thenCompose(__ -> {
                Topic topic = getTopicReference(topicName);
                Subscription sub = topic.getSubscription(subName);
                if (sub == null) {
                    throw new RestException(Status.NOT_FOUND, "Subscription not found");
                }
                return sub.delete();
            }).thenRun(() -> {
                log.info("[{}][{}] Deleted subscription {}", clientAppId(), topicName, subName);
                asyncResponse.resume(Response.noContent().build());
            }).exceptionally(ex -> {
                Throwable cause = ex.getCause();
                if (cause instanceof SubscriptionBusyException) {
                    log.error("[{}] Failed to delete subscription {} from topic {}", clientAppId(), subName,
                            topicName, cause);
                    asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                            "Subscription has active connected consumers"));
                } else {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to delete subscription {} {}", clientAppId(), topicName, subName, cause);
                    }
                    asyncResponse.resume(new RestException(cause));
                }
                return null;
            });
    }

    protected void internalDeleteSubscriptionForcefully(AsyncResponse asyncResponse,
                                                        String subName, boolean authoritative) {
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }

        future.thenAccept(__ -> {
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
                        internalDeleteSubscriptionForNonPartitionedTopicForcefully(asyncResponse, subName,
                                authoritative);
                    }
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to delete subscription forcefully {} from topic {}",
                                clientAppId(), subName, topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
            }
        }).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (!isRedirectException(ex)) {
                log.error("[{}] Failed to delete subscription {} from topic {}",
                        clientAppId(), subName, topicName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    private void internalDeleteSubscriptionForNonPartitionedTopicForcefully(AsyncResponse asyncResponse,
                                                                            String subName, boolean authoritative) {
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenRun(() -> validateTopicOperation(topicName, TopicOperation.UNSUBSCRIBE))
                .thenCompose(__ -> {
                    Topic topic = getTopicReference(topicName);
                    Subscription sub = topic.getSubscription(subName);
                    if (sub == null) {
                        throw new RestException(Status.NOT_FOUND, "Subscription not found");
                    }
                    return sub.deleteForcefully();
                }).thenRun(() -> {
                    log.info("[{}][{}] Deleted subscription forcefully {}", clientAppId(), topicName, subName);
                    asyncResponse.resume(Response.noContent().build());
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to delete subscription forcefully {} {}",
                                clientAppId(), topicName, subName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    protected void internalSkipAllMessages(AsyncResponse asyncResponse, String subName, boolean authoritative) {
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }

        future.thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative))
            .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.SKIP, subName))
            .thenCompose(__ -> {
                // If the topic name is a partition name, no need to get partition topic metadata again
                if (topicName.isPartitioned()) {
                    return internalSkipAllMessagesForNonPartitionedTopicAsync(asyncResponse, subName, authoritative);
                } else {
                    return getPartitionedTopicMetadataAsync(topicName,
                        authoritative, false).thenCompose(partitionMetadata -> {
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
                                    return CompletableFuture.completedFuture(null);
                                }
                            }

                            return FutureUtil.waitForAll(futures).handle((result, exception) -> {
                                if (exception != null) {
                                    Throwable t = exception.getCause();
                                    if (t instanceof NotFoundException) {
                                        asyncResponse.resume(
                                            new RestException(Status.NOT_FOUND, "Subscription not found"));
                                    } else {
                                        log.error("[{}] Failed to skip all messages {} {}",
                                            clientAppId(), topicName, subName, t);
                                        asyncResponse.resume(new RestException(t));
                                    }
                                    return null;
                                }
                                asyncResponse.resume(Response.noContent().build());
                                return null;
                            });
                        } else {
                            return internalSkipAllMessagesForNonPartitionedTopicAsync(asyncResponse, subName,
                                authoritative);
                        }
                    });
                }
            }).exceptionally(ex -> {
                // If the exception is not redirect exception we need to log it.
                if (!isRedirectException(ex)) {
                    log.error("[{}] Failed to skip all messages for subscription {} on topic {}",
                            clientAppId(), subName, topicName, ex);
                }
                resumeAsyncResponseExceptionally(asyncResponse, ex);
                return null;
            });
    }

    private CompletableFuture<Void> internalSkipAllMessagesForNonPartitionedTopicAsync(AsyncResponse asyncResponse,
                                                                                       String subName,
                                                                                       boolean authoritative) {
        return validateTopicOwnershipAsync(topicName, authoritative)
            .thenCompose(__ ->
                validateTopicOperationAsync(topicName, TopicOperation.SKIP, subName))
            .thenCompose(__ ->
                getTopicReferenceAsync(topicName).thenCompose(t -> {
                    PersistentTopic topic = (PersistentTopic) t;
                    BiConsumer<Void, Throwable> biConsumer = (v, ex) -> {
                        if (ex != null) {
                            asyncResponse.resume(new RestException(ex));
                            log.error("[{}] Failed to skip all messages {} {}",
                                clientAppId(), topicName, subName, ex);
                        } else {
                            asyncResponse.resume(Response.noContent().build());
                            log.info("[{}] Cleared backlog on {} {}", clientAppId(), topicName, subName);
                        }
                    };
                    if (subName.startsWith(topic.getReplicatorPrefix())) {
                        String remoteCluster = PersistentReplicator.getRemoteCluster(subName);
                        PersistentReplicator repl =
                            (PersistentReplicator) topic.getPersistentReplicator(remoteCluster);
                        if (repl == null) {
                            asyncResponse.resume(new RestException(Status.NOT_FOUND, "Subscription not found"));
                            return CompletableFuture.completedFuture(null);
                        }
                        return repl.clearBacklog().whenComplete(biConsumer);
                    } else {
                        PersistentSubscription sub = topic.getSubscription(subName);
                        if (sub == null) {
                            asyncResponse.resume(new RestException(Status.NOT_FOUND, "Subscription not found"));
                            return CompletableFuture.completedFuture(null);
                        }
                        return sub.clearBacklog().whenComplete(biConsumer);
                    }
                })
                .exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to skip all messages for subscription {} on topic {}",
                                clientAppId(), subName, topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                }));
    }

    protected void internalSkipMessages(AsyncResponse asyncResponse, String subName, int numMessages,
                                        boolean authoritative) {
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }
        future.thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.SKIP))
                .thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative))
                .thenCompose(__ -> getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                     .thenCompose(partitionMetadata -> {
                         if (partitionMetadata.partitions > 0) {
                             String msg = "Skip messages on a partitioned topic is not allowed";
                             log.warn("[{}] {} {} {}", clientAppId(), msg, topicName, subName);
                             throw new  RestException(Status.METHOD_NOT_ALLOWED, msg);
                         }
                         return getTopicReferenceAsync(topicName).thenCompose(t -> {
                             PersistentTopic topic = (PersistentTopic) t;
                             if (topic == null) {
                                 throw new RestException(new RestException(Status.NOT_FOUND, "Topic not found"));
                             }
                             if (subName.startsWith(topic.getReplicatorPrefix())) {
                                 String remoteCluster = PersistentReplicator.getRemoteCluster(subName);
                                 PersistentReplicator repl =
                                         (PersistentReplicator) topic.getPersistentReplicator(remoteCluster);
                                 if (repl == null) {
                                     return FutureUtil.failedFuture(
                                             new RestException(Status.NOT_FOUND, "Replicator not found"));
                                 }
                                 return repl.skipMessages(numMessages).thenAccept(unused -> {
                                     log.info("[{}] Skipped {} messages on {} {}", clientAppId(), numMessages,
                                             topicName, subName);
                                     asyncResponse.resume(Response.noContent().build());
                                     }
                                 );
                             } else {
                                 PersistentSubscription sub = topic.getSubscription(subName);
                                 if (sub == null) {
                                     return FutureUtil.failedFuture(
                                             new RestException(Status.NOT_FOUND, "Subscription not found"));
                                 }
                                 return sub.skipMessages(numMessages).thenAccept(unused -> {
                                     log.info("[{}] Skipped {} messages on {} {}", clientAppId(), numMessages,
                                             topicName, subName);
                                     asyncResponse.resume(Response.noContent().build());
                                     }
                                 );
                             }
                         });
                     })
                ).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to skip {} messages {} {}", clientAppId(), numMessages, topicName,
                                subName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    protected void internalExpireMessagesForAllSubscriptions(AsyncResponse asyncResponse, int expireTimeInSeconds,
            boolean authoritative) {
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }
        future.thenCompose(__ ->
            getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                .thenAccept(partitionMetadata -> {
                    if (topicName.isPartitioned()) {
                        internalExpireMessagesForAllSubscriptionsForNonPartitionedTopic(asyncResponse,
                                partitionMetadata, expireTimeInSeconds, authoritative);
                    } else {
                        if (partitionMetadata.partitions > 0) {
                            final List<CompletableFuture<Void>> futures =
                                    Lists.newArrayListWithCapacity(partitionMetadata.partitions);

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
                                    partitionMetadata, expireTimeInSeconds, authoritative);
                        }
                    }
                }
             )
        ).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (!isRedirectException(ex)) {
                log.error("[{}] Failed to expire messages for all subscription on topic {}", clientAppId(), topicName,
                        ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });

    }

    private void internalExpireMessagesForAllSubscriptionsForNonPartitionedTopic(AsyncResponse asyncResponse,
                                                                                 PartitionedTopicMetadata
                                                                                 partitionMetadata,
                                                                                 int expireTimeInSeconds,
                                                                                 boolean authoritative) {
        // validate ownership and redirect if current broker is not owner
        validateTopicOperationAsync(topicName, TopicOperation.EXPIRE_MESSAGES)
                .thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative))
                .thenCompose(__ -> getTopicReferenceAsync(topicName).thenAccept(t -> {
                     if (t == null) {
                         resumeAsyncResponseExceptionally(asyncResponse, new RestException(Status.NOT_FOUND,
                                 "Topic not found"));
                         return;
                     }
                    if (!(t instanceof PersistentTopic)) {
                        resumeAsyncResponseExceptionally(asyncResponse, new RestException(Status.METHOD_NOT_ALLOWED,
                                "Expire messages for all subscriptions on a non-persistent topic is not allowed"));
                        return;
                    }
                    PersistentTopic topic = (PersistentTopic) t;
                    final List<CompletableFuture<Void>> futures =
                            Lists.newArrayListWithCapacity((int) topic.getReplicators().size());
                    List<String> subNames =
                            Lists.newArrayListWithCapacity((int) topic.getReplicators().size()
                                    + (int) topic.getSubscriptions().size());
                    subNames.addAll(topic.getReplicators().keys());
                    subNames.addAll(topic.getSubscriptions().keys());
                    for (int i = 0; i < subNames.size(); i++) {
                        try {
                            futures.add(internalExpireMessagesByTimestampForSinglePartitionAsync(partitionMetadata,
                                    subNames.get(i), expireTimeInSeconds, authoritative));
                        } catch (Exception e) {
                            log.error("[{}] Failed to expire messages for all subscription up to {} on {}",
                                    clientAppId(), expireTimeInSeconds, topicName, e);
                            asyncResponse.resume(new RestException(e));
                            return;
                        }
                    }

                    FutureUtil.waitForAll(futures).handle((result, exception) -> {
                        if (exception != null) {
                            Throwable throwable = FutureUtil.unwrapCompletionException(exception);
                            log.error("[{}] Failed to expire messages for all subscription up to {} on {}",
                                    clientAppId(), expireTimeInSeconds, topicName, throwable);
                            asyncResponse.resume(new RestException(throwable));
                            return null;
                        }
                        asyncResponse.resume(Response.noContent().build());
                        return null;
                    });
                        })
                ).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (!isRedirectException(ex)) {
                log.error("[{}] Failed to expire messages for all subscription up to {} on {}", clientAppId(),
                        expireTimeInSeconds, topicName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
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
                // If the exception is not redirect exception we need to log it.
                if (!isRedirectException(ex)) {
                    log.error("[{}] Failed to expire messages for all subscription on topic {}",
                            clientAppId(), topicName, ex);
                }
                resumeAsyncResponseExceptionally(asyncResponse, ex);
                return null;
            });
        }
    }

    private void internalResetCursorForNonPartitionedTopic(AsyncResponse asyncResponse, String subName, long timestamp,
                                       boolean authoritative) {
        try {
            validateTopicOwnership(topicName, authoritative);
            validateTopicOperation(topicName, TopicOperation.RESET_CURSOR, subName);

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
        CompletableFuture<Void> ret;
        if (topicName.isGlobal()) {
            ret = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            ret = CompletableFuture.completedFuture(null);
        }
        ret.thenAccept(__ -> {
            final MessageIdImpl targetMessageId = messageId == null ? (MessageIdImpl) MessageId.latest : messageId;
            log.info("[{}][{}] Creating subscription {} at message id {}", clientAppId(), topicName, subscriptionName,
                    targetMessageId);
            // If the topic name is a partition name, no need to get partition topic metadata again
            if (topicName.isPartitioned()) {
                internalCreateSubscriptionForNonPartitionedTopic(asyncResponse,
                        subscriptionName, targetMessageId, authoritative, replicated);
            } else {
                boolean allowAutoTopicCreation = pulsar().getBrokerService().isAllowAutoTopicCreation(topicName);
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
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to create subscription {} on topic {}",
                                clientAppId(), subscriptionName, topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
            }
        }).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (!isRedirectException(ex)) {
                log.error("[{}] Failed to create subscription {} on topic {}",
                        clientAppId(), subscriptionName, topicName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    private void internalCreateSubscriptionForNonPartitionedTopic(
            AsyncResponse asyncResponse, String subscriptionName,
            MessageIdImpl targetMessageId, boolean authoritative, boolean replicated) {

        boolean isAllowAutoTopicCreation = pulsar().getBrokerService().isAllowAutoTopicCreation(topicName);

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
        CompletableFuture<Void> ret;
        if (topicName.isGlobal()) {
            ret = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            ret = CompletableFuture.completedFuture(null);
        }
        ret.thenAccept(__ -> {
            log.info("[{}][{}] received reset cursor on subscription {} to position {}", clientAppId(), topicName,
                    subName, messageId);
            // If the topic name is a partition name, no need to get partition topic metadata again
            if (!topicName.isPartitioned()
                    && getPartitionedTopicMetadata(topicName, authoritative, false).partitions > 0) {
                log.warn("[{}] Not supported operation on partitioned-topic {} {}", clientAppId(), topicName,
                        subName);
                asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED,
                        "Reset-cursor at position is not allowed for partitioned-topic"));
                return;
            } else {
                validateTopicOwnershipAsync(topicName, authoritative)
                        .thenCompose(ignore ->
                                validateTopicOperationAsync(topicName, TopicOperation.RESET_CURSOR, subName))
                        .thenCompose(ignore -> getTopicReferenceAsync(topicName))
                        .thenAccept(topic -> {
                                if (topic == null) {
                                    asyncResponse.resume(new RestException(Status.NOT_FOUND, "Topic not found"));
                                    return;
                                }
                                PersistentSubscription sub = ((PersistentTopic) topic).getSubscription(subName);
                                if (sub == null) {
                                    asyncResponse.resume(new RestException(Status.NOT_FOUND, "Subscription not found"));
                                    return;
                                }
                                CompletableFuture<Integer> batchSizeFuture = new CompletableFuture<>();
                                getEntryBatchSize(batchSizeFuture, (PersistentTopic) topic, messageId, batchIndex);
                                batchSizeFuture.thenAccept(bi -> {
                                    PositionImpl seekPosition = calculatePositionAckSet(isExcluded, bi, batchIndex,
                                            messageId);
                                    sub.resetCursor(seekPosition).thenRun(() -> {
                                        log.info("[{}][{}] successfully reset cursor on subscription {}"
                                                        + " to position {}", clientAppId(),
                                                topicName, subName, messageId);
                                        asyncResponse.resume(Response.noContent().build());
                                    }).exceptionally(ex -> {
                                        Throwable t = (ex instanceof CompletionException ? ex.getCause() : ex);
                                        log.warn("[{}][{}] Failed to reset cursor on subscription {}"
                                                        + " to position {}", clientAppId(),
                                                        topicName, subName, messageId, t);
                                        if (t instanceof SubscriptionInvalidCursorPosition) {
                                            asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                                                    "Unable to find position for position specified: "
                                                            + t.getMessage()));
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
                        }).exceptionally(ex -> {
                            // If the exception is not redirect exception we need to log it.
                            if (!isRedirectException(ex)) {
                                log.warn("[{}][{}] Failed to reset cursor on subscription {} to position {}",
                                        clientAppId(), topicName, subName, messageId, ex.getCause());
                            }
                            resumeAsyncResponseExceptionally(asyncResponse, ex.getCause());
                            return null;
                        });
                }
        }).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (!isRedirectException(ex)) {
                log.warn("[{}][{}] Failed to reset cursor on subscription {} to position {}",
                        clientAppId(), topicName, subName, messageId, ex.getCause());
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex.getCause());
            return null;
        });
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
        // will redirect if the topic not owned by current broker
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.PEEK_MESSAGES))
                .thenCompose(__ -> {
                    CompletableFuture<Void> ret;
                    if (topicName.isGlobal()) {
                        ret = validateGlobalNamespaceOwnershipAsync(namespaceName);
                    } else {
                        ret = CompletableFuture.completedFuture(null);
                    }
                    return ret;
                })
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenAccept(topic -> {
                    ManagedLedgerImpl ledger =
                            (ManagedLedgerImpl) ((PersistentTopic) topic).getManagedLedger();
                    ledger.asyncReadEntry(new PositionImpl(ledgerId, entryId),
                            new AsyncCallbacks.ReadEntryCallback() {
                                @Override
                                public void readEntryFailed(ManagedLedgerException exception,
                                                            Object ctx) {
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
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to get message with ledgerId {} entryId {} from {}",
                                clientAppId(), ledgerId, entryId, topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    protected CompletableFuture<MessageId> internalGetMessageIdByTimestamp(long timestamp, boolean authoritative) {
        try {
            if (topicName.isGlobal()) {
                validateGlobalNamespaceOwnership(namespaceName);
            }

            if (!topicName.isPartitioned() && getPartitionedTopicMetadata(topicName,
                    authoritative, false).partitions > 0) {
                throw new RestException(Status.METHOD_NOT_ALLOWED,
                        "Get message ID by timestamp on a partitioned topic is not allowed, "
                                + "please try do it on specific topic partition");
            }

            validateTopicOwnership(topicName, authoritative);
            validateTopicOperation(topicName, TopicOperation.PEEK_MESSAGES);

            Topic topic = getTopicReference(topicName);
            if (!(topic instanceof PersistentTopic)) {
                log.error("[{}] Not supported operation of non-persistent topic {} ", clientAppId(), topicName);
                throw new RestException(Status.METHOD_NOT_ALLOWED,
                        "Get message ID by timestamp on a non-persistent topic is not allowed");
            }

            ManagedLedger ledger = ((PersistentTopic) topic).getManagedLedger();
            return ledger.asyncFindPosition(entry -> {
                try {
                    long entryTimestamp = Commands.getEntryTimestamp(entry.getDataBuffer());
                    return MessageImpl.isEntryPublishedEarlierThan(entryTimestamp, timestamp);
                } catch (Exception e) {
                    log.error("[{}] Error deserializing message for message position find", topicName, e);
                } finally {
                    entry.release();
                }
                return false;
            }).thenApply(position -> {
                if (position == null) {
                    return null;
                } else {
                    return new MessageIdImpl(position.getLedgerId(), position.getEntryId(),
                            topicName.getPartitionIndex());
                }
            });
        } catch (WebApplicationException exception) {
            return FutureUtil.failedFuture(exception);
        } catch (Exception exception) {
            return FutureUtil.failedFuture(new RestException(exception));
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
                    topicName, exception);
            throw new RestException(exception);
        }
    }

    private Response generateResponseWithEntry(Entry entry) throws IOException {
        checkNotNull(entry);
        PositionImpl pos = (PositionImpl) entry.getPosition();
        ByteBuf metadataAndPayload = entry.getDataBuffer();

        long totalSize = metadataAndPayload.readableBytes();
        BrokerEntryMetadata brokerEntryMetadata = Commands.peekBrokerEntryMetadataIfExist(metadataAndPayload);
        MessageMetadata metadata = Commands.parseMessageMetadata(metadataAndPayload);

        ResponseBuilder responseBuilder = Response.ok();
        responseBuilder.header("X-Pulsar-Message-ID", pos.toString());
        for (KeyValue keyValue : metadata.getPropertiesList()) {
            responseBuilder.header("X-Pulsar-PROPERTY-" + keyValue.getKey(), keyValue.getValue());
        }
        if (brokerEntryMetadata != null) {
            if (brokerEntryMetadata.hasBrokerTimestamp()) {
                responseBuilder.header("X-Pulsar-Broker-Entry-METADATA-timestamp",
                        DateFormatter.format(brokerEntryMetadata.getBrokerTimestamp()));
            }
            if (brokerEntryMetadata.hasIndex()) {
                responseBuilder.header("X-Pulsar-Broker-Entry-METADATA-index", brokerEntryMetadata.getIndex());
            }
        }
        if (metadata.hasPublishTime()) {
            responseBuilder.header("X-Pulsar-publish-time", DateFormatter.format(metadata.getPublishTime()));
        }
        if (metadata.hasEventTime()) {
            responseBuilder.header("X-Pulsar-event-time", DateFormatter.format(metadata.getEventTime()));
        }
        if (metadata.hasDeliverAtTime()) {
            responseBuilder.header("X-Pulsar-deliver-at-time", DateFormatter.format(metadata.getDeliverAtTime()));
        }
        if (metadata.hasNumMessagesInBatch()) {
            responseBuilder.header("X-Pulsar-num-batch-message", metadata.getNumMessagesInBatch());
            responseBuilder.header("X-Pulsar-batch-size", totalSize
                    - metadata.getSerializedSize());
        }
        if (metadata.hasNullValue()) {
            responseBuilder.header("X-Pulsar-null-value", metadata.isNullValue());
        }
        if (metadata.hasNumChunksFromMsg()) {
            responseBuilder.header("X-Pulsar-PROPERTY-TOTAL-CHUNKS", Integer.toString(metadata.getNumChunksFromMsg()));
            responseBuilder.header("X-Pulsar-PROPERTY-CHUNK-ID", Integer.toString(metadata.getChunkId()));
        }
        responseBuilder.header("X-Pulsar-Is-Encrypted", metadata.getEncryptionKeysCount() > 0);

        if (metadata.hasProducerName()) {
            responseBuilder.header("X-Pulsar-producer-name", metadata.getProducerName());
        }
        if (metadata.hasSequenceId()) {
            responseBuilder.header("X-Pulsar-sequence-id", metadata.getSequenceId());
        }
        if (metadata.hasReplicatedFrom()) {
            responseBuilder.header("X-Pulsar-replicated-from", metadata.getReplicatedFrom());
        }
        for (String replicatedTo : metadata.getReplicateTosList()) {
            responseBuilder.header("X-Pulsar-replicated-to", replicatedTo);
        }
        if (metadata.hasPartitionKey()) {
            responseBuilder.header("X-Pulsar-partition-key", metadata.getPartitionKey());
        }
        if (metadata.hasCompression()) {
            responseBuilder.header("X-Pulsar-compression", metadata.getCompression());
        }
        if (metadata.hasUncompressedSize()) {
            responseBuilder.header("X-Pulsar-uncompressed-size", metadata.getUncompressedSize());
        }
        if (metadata.hasEncryptionAlgo()) {
            responseBuilder.header("X-Pulsar-encryption-algo", metadata.getEncryptionAlgo());
        }
        for (EncryptionKeys encryptionKeys : metadata.getEncryptionKeysList()) {
            responseBuilder.header("X-Pulsar-Base64-encryption-keys",
                    Base64.getEncoder().encodeToString(encryptionKeys.toByteArray()));
        }
        if (metadata.hasEncryptionParam()) {
            responseBuilder.header("X-Pulsar-Base64-encryption-param",
                    Base64.getEncoder().encodeToString(metadata.getEncryptionParam()));
        }
        if (metadata.hasSchemaVersion()) {
            responseBuilder.header("X-Pulsar-Base64-schema-version",
                    Base64.getEncoder().encodeToString(metadata.getSchemaVersion()));
        }
        if (metadata.hasPartitionKeyB64Encoded()) {
            responseBuilder.header("X-Pulsar-partition-key-b64-encoded", metadata.isPartitionKeyB64Encoded());
        }
        if (metadata.hasOrderingKey()) {
            responseBuilder.header("X-Pulsar-Base64-ordering-key",
                    Base64.getEncoder().encodeToString(metadata.getOrderingKey()));
        }
        if (metadata.hasMarkerType()) {
            responseBuilder.header("X-Pulsar-marker-type", metadata.getMarkerType());
        }
        if (metadata.hasTxnidLeastBits()) {
            responseBuilder.header("X-Pulsar-txnid-least-bits", metadata.getTxnidLeastBits());
        }
        if (metadata.hasTxnidMostBits()) {
            responseBuilder.header("X-Pulsar-txnid-most-bits", metadata.getTxnidMostBits());
        }
        if (metadata.hasHighestSequenceId()) {
            responseBuilder.header("X-Pulsar-highest-sequence-id", metadata.getHighestSequenceId());
        }
        if (metadata.hasUuid()) {
            responseBuilder.header("X-Pulsar-uuid", metadata.getUuid());
        }
        if (metadata.hasNumChunksFromMsg()) {
            responseBuilder.header("X-Pulsar-num-chunks-from-msg", metadata.getNumChunksFromMsg());
        }
        if (metadata.hasTotalChunkMsgSize()) {
            responseBuilder.header("X-Pulsar-total-chunk-msg-size", metadata.getTotalChunkMsgSize());
        }
        if (metadata.hasChunkId()) {
            responseBuilder.header("X-Pulsar-chunk-id", metadata.getChunkId());
        }
        if (metadata.hasNullPartitionKey()) {
            responseBuilder.header("X-Pulsar-null-partition-key", metadata.isNullPartitionKey());
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
            namespaceResources().getPolicies(namespaceName);
        } catch (MetadataStoreException.NotFoundException e) {
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

    protected CompletableFuture<Map<BacklogQuota.BacklogQuotaType, BacklogQuota>> internalGetBacklogQuota(
            boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> {
                Map<BacklogQuota.BacklogQuotaType, BacklogQuota> quotaMap = op
                        .map(TopicPolicies::getBackLogQuotaMap)
                        .map(map -> {
                            HashMap<BacklogQuota.BacklogQuotaType, BacklogQuota> hashMap = Maps.newHashMap();
                            map.forEach((key, value) -> hashMap.put(BacklogQuota.BacklogQuotaType.valueOf(key), value));
                            return hashMap;
                        }).orElse(Maps.newHashMap());
                if (applied && quotaMap.isEmpty()) {
                    quotaMap = getNamespacePolicies(namespaceName).backlog_quota_map;
                    if (quotaMap.isEmpty()) {
                        for (BacklogQuota.BacklogQuotaType backlogQuotaType : BacklogQuota.BacklogQuotaType.values()) {
                            quotaMap.put(
                                    backlogQuotaType,
                                    namespaceBacklogQuota(namespaceName, backlogQuotaType)
                            );
                        }
                    }
                }
                return quotaMap;
        });
    }

    protected void internalGetBacklogSizeByMessageId(AsyncResponse asyncResponse,
                                                     MessageIdImpl messageId, boolean authoritative) {
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }
        future.thenAccept(__ -> {
            getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                    .thenAccept(partitionMetadata -> {
                        if (!topicName.isPartitioned() && partitionMetadata.partitions > 0) {
                            log.warn("[{}] Not supported calculate backlog size operation on partitioned-topic {}",
                                    clientAppId(), topicName);
                            asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED,
                                    "calculate backlog size is not allowed for partitioned-topic"));
                        } else {
                            validateTopicOwnershipAsync(topicName, authoritative)
                                    .thenCompose(unused -> validateTopicOperationAsync(topicName,
                                            TopicOperation.GET_BACKLOG_SIZE))
                                    .thenCompose(unused -> getTopicReferenceAsync(topicName))
                                    .thenAccept(t -> {
                                        PersistentTopic topic = (PersistentTopic) t;
                                        PositionImpl pos = new PositionImpl(messageId.getLedgerId(),
                                                messageId.getEntryId());
                                        if (topic == null) {
                                            asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                                    "Topic not found"));
                                            return;
                                        }
                                        ManagedLedgerImpl managedLedger =
                                                (ManagedLedgerImpl) topic.getManagedLedger();
                                        if (messageId.getLedgerId() == -1) {
                                            asyncResponse.resume(managedLedger.getTotalSize());
                                        } else {
                                            asyncResponse.resume(managedLedger.getEstimatedBacklogSize(pos));
                                        }
                                    }).exceptionally(ex -> {
                                        // If the exception is not redirect exception we need to log it.
                                        if (!isRedirectException(ex)) {
                                            log.error("[{}] Failed to get backlog size for topic {}", clientAppId(),
                                                    topicName, ex);
                                        }
                                        resumeAsyncResponseExceptionally(asyncResponse, ex);
                                        return null;
                                    });
                        }
                    }).exceptionally(ex -> {
                        // If the exception is not redirect exception we need to log it.
                        if (!isRedirectException(ex)) {
                            log.error("[{}] Failed to get backlog size for topic {}", clientAppId(), topicName, ex);
                        }
                        resumeAsyncResponseExceptionally(asyncResponse, ex);
                        return null;
            });
        }).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (!isRedirectException(ex)) {
                log.error("[{}] Failed to validate global namespace ownership to get backlog size for topic "
                        + "{}", clientAppId(), topicName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    protected CompletableFuture<Void> internalSetBacklogQuota(BacklogQuota.BacklogQuotaType backlogQuotaType,
                                                              BacklogQuotaImpl backlogQuota, boolean isGlobal) {
        BacklogQuota.BacklogQuotaType finalBacklogQuotaType = backlogQuotaType == null
                ? BacklogQuota.BacklogQuotaType.destination_storage : backlogQuotaType;

        return validateTopicPolicyOperationAsync(topicName, PolicyName.BACKLOG, PolicyOperation.WRITE)
                .thenAccept(__ -> validatePoliciesReadOnlyAccess())
                .thenCompose(__ -> getTopicPoliciesAsyncWithRetry(topicName, isGlobal))
                .thenCompose(op -> {
                    TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                    return getRetentionPoliciesAsync(topicName, topicPolicies)
                            .thenCompose(retentionPolicies -> {
                                if (!checkBacklogQuota(backlogQuota, retentionPolicies)) {
                                    log.warn(
                                            "[{}] Failed to update backlog configuration for topic {}: conflicts with"
                                                    + " retention quota",
                                            clientAppId(), topicName);
                                    return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                                            "Backlog Quota exceeds configured retention quota for topic. "
                                                    + "Please increase retention quota and retry"));
                                }
                                if (backlogQuota != null) {
                                    topicPolicies.getBackLogQuotaMap().put(finalBacklogQuotaType.name(), backlogQuota);
                                } else {
                                    topicPolicies.getBackLogQuotaMap().remove(finalBacklogQuotaType.name());
                                }
                                Map<String, BacklogQuotaImpl> backLogQuotaMap = topicPolicies.getBackLogQuotaMap();
                                topicPolicies.setIsGlobal(isGlobal);
                                return pulsar().getTopicPoliciesService()
                                        .updateTopicPoliciesAsync(topicName, topicPolicies)
                                        .thenRun(() -> {
                                            try {
                                                log.info(
                                                        "[{}] Successfully updated backlog quota map: namespace={}, "
                                                                + "topic={}, map={}",
                                                        clientAppId(),
                                                        namespaceName,
                                                        topicName.getLocalName(),
                                                        jsonMapper().writeValueAsString(backLogQuotaMap));
                                            } catch (JsonProcessingException ignore) {
                                            }
                                        });
                            });
                });
    }

    protected CompletableFuture<Void> internalSetReplicationClusters(List<String> clusterIds) {

        return validateTopicPolicyOperationAsync(topicName, PolicyName.REPLICATION, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenAccept(__ -> {
                    Set<String> replicationClusters = Sets.newHashSet(clusterIds);
                    if (replicationClusters.contains("global")) {
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "Cannot specify global in the list of replication clusters");
                    }
                    Set<String> clusters = clusters();
                    for (String clusterId : replicationClusters) {
                        if (!clusters.contains(clusterId)) {
                            throw new RestException(Status.FORBIDDEN, "Invalid cluster id: " + clusterId);
                        }
                        validatePeerClusterConflict(clusterId, replicationClusters);
                        validateClusterForTenant(namespaceName.getTenant(), clusterId);
                    }
                }).thenCompose(__ ->
                    getTopicPoliciesAsyncWithRetry(topicName).thenCompose(op -> {
                            TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                            topicPolicies.setReplicationClusters(clusterIds);
                            return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies)
                                    .thenRun(() -> {
                                        log.info("[{}] Successfully set replication clusters for namespace={}, "
                                                        + "topic={}, clusters={}",
                                                clientAppId(),
                                                namespaceName,
                                                topicName.getLocalName(),
                                                topicPolicies.getReplicationClusters());
                                    });
                        }
                ));
    }

    protected CompletableFuture<Void> internalRemoveReplicationClusters() {
        return validateTopicPolicyOperationAsync(topicName, PolicyName.REPLICATION, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> getTopicPoliciesAsyncWithRetry(topicName).thenCompose(op -> {
                            TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                            topicPolicies.setReplicationClusters(null);
                            return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies)
                                    .thenRun(() -> {
                                        log.info("[{}] Successfully set replication clusters for namespace={}, "
                                                        + "topic={}, clusters={}",
                                                clientAppId(),
                                                namespaceName,
                                                topicName.getLocalName(),
                                                topicPolicies.getReplicationClusters());
                                    });
                        })
                );
    }

    protected CompletableFuture<Boolean> internalGetDeduplication(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getDeduplicationEnabled)
                .orElseGet(() -> {
                    if (applied) {
                        Boolean enabled = getNamespacePolicies(namespaceName).deduplicationEnabled;
                        return enabled == null ? config().isBrokerDeduplicationEnabled() : enabled;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetDeduplication(Boolean enabled, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setDeduplicationEnabled(enabled);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Void> internalSetMessageTTL(Integer ttlInSecond, boolean isGlobal) {
        //Validate message ttl value.
        if (ttlInSecond != null && ttlInSecond < 0) {
            return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                    "Invalid value for message TTL"));
        }

        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setMessageTTLInSeconds(ttlInSecond);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies)
                        .thenRun(() ->
                                log.info("[{}] Successfully set topic message ttl: namespace={}, topic={}, ttl={}",
                                        clientAppId(), namespaceName, topicName.getLocalName(), ttlInSecond));
            });
    }

    private CompletableFuture<RetentionPolicies> getRetentionPoliciesAsync(TopicName topicName,
                                                                           TopicPolicies topicPolicies) {
        RetentionPolicies retentionPolicies = topicPolicies.getRetentionPolicies();
        if (retentionPolicies != null) {
            return CompletableFuture.completedFuture(retentionPolicies);
        }
        return getNamespacePoliciesAsync(topicName.getNamespaceObject())
                .thenApply(policies -> policies.retention_policies);
    }

    protected CompletableFuture<RetentionPolicies> internalGetRetention(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getRetentionPolicies).orElseGet(() -> {
                if (applied) {
                    RetentionPolicies policies = getNamespacePolicies(namespaceName).retention_policies;
                    return policies == null ? new RetentionPolicies(
                            config().getDefaultRetentionTimeInMinutes(), config().getDefaultRetentionSizeInMB())
                            : policies;
                }
                return null;
            }));
    }

    protected CompletableFuture<Void> internalSetRetention(RetentionPolicies retention, boolean isGlobal) {
        if (retention == null) {
            return CompletableFuture.completedFuture(null);
        }
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                for (BacklogQuota.BacklogQuotaType backlogQuotaType : BacklogQuota.BacklogQuotaType.values()) {
                    BacklogQuota backlogQuota = topicPolicies.getBackLogQuotaMap().get(backlogQuotaType.name());
                    if (backlogQuota == null) {
                        Policies policies = getNamespacePolicies(topicName.getNamespaceObject());
                        backlogQuota = policies.backlog_quota_map.get(backlogQuotaType);
                    }
                    if (!checkBacklogQuota(backlogQuota, retention)) {
                        log.warn(
                                "[{}] Failed to update retention quota configuration for topic {}: "
                                        + "conflicts with retention quota",
                                clientAppId(), topicName);
                        return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                                "Retention Quota must exceed configured backlog quota for topic. "
                                        + "Please increase retention quota and retry"));
                    }
                }
                topicPolicies.setRetentionPolicies(retention);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Void> internalRemoveRetention(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                .thenCompose(op -> {
                    if (!op.isPresent()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    op.get().setRetentionPolicies(null);
                    return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
                });
    }

    protected CompletableFuture<PersistencePolicies> internalGetPersistence(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getPersistence)
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
                }));
    }

    protected CompletableFuture<Void> internalSetPersistence(PersistencePolicies persistencePolicies,
                                                             boolean isGlobal) {
        validatePersistencePolicies(persistencePolicies);
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setPersistence(persistencePolicies);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Void> internalRemovePersistence(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                if (!op.isPresent()) {
                    return CompletableFuture.completedFuture(null);
                }
                op.get().setPersistence(null);
                op.get().setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
            });
    }

    protected CompletableFuture<Void> internalSetMaxMessageSize(Integer maxMessageSize, boolean isGlobal) {
        if (maxMessageSize != null && (maxMessageSize < 0 || maxMessageSize > config().getMaxMessageSize())) {
            throw new RestException(Status.PRECONDITION_FAILED
                    , "topic-level maxMessageSize must be greater than or equal to 0 "
                    + "and must be smaller than that in the broker-level");
        }

        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setMaxMessageSize(maxMessageSize);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Optional<Integer>> internalGetMaxMessageSize(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                .thenApply(op -> op.map(TopicPolicies::getMaxMessageSize));
    }

    protected CompletableFuture<Integer> internalGetMaxProducers(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getMaxProducerPerTopic)
                .orElseGet(() -> {
                    if (applied) {
                        Integer maxProducer = getNamespacePolicies(namespaceName).max_producers_per_topic;
                        return maxProducer == null ? config().getMaxProducersPerTopic() : maxProducer;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetMaxProducers(Integer maxProducers, boolean isGlobal) {
        if (maxProducers != null && maxProducers < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxProducers must be 0 or more");
        }
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setMaxProducerPerTopic(maxProducers);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });

    }

    protected CompletableFuture<Optional<Integer>> internalGetMaxSubscriptionsPerTopic(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                .thenApply(op -> op.map(TopicPolicies::getMaxSubscriptionsPerTopic));
    }

    protected CompletableFuture<Void> internalSetMaxSubscriptionsPerTopic(Integer maxSubscriptionsPerTopic,
                                                                          boolean isGlobal) {
        if (maxSubscriptionsPerTopic != null && maxSubscriptionsPerTopic < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxSubscriptionsPerTopic must be 0 or more");
        }

        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setMaxSubscriptionsPerTopic(maxSubscriptionsPerTopic);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<DispatchRateImpl> internalGetReplicatorDispatchRate(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getReplicatorDispatchRate)
                .orElseGet(() -> {
                    if (applied) {
                        DispatchRateImpl namespacePolicy = getNamespacePolicies(namespaceName)
                                .replicatorDispatchRate.get(pulsar().getConfiguration().getClusterName());
                        return namespacePolicy == null ? replicatorDispatchRate() : namespacePolicy;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetReplicatorDispatchRate(DispatchRateImpl dispatchRate,
                                                                        boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setReplicatorDispatchRate(dispatchRate);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Void> preValidation(boolean authoritative) {
        if (!config().isTopicLevelPoliciesEnabled()) {
            return FutureUtil.failedFuture(new RestException(Status.METHOD_NOT_ALLOWED,
                    "Topic level policies is disabled, to enable the topic level policy and retry."));
        }
        if (topicName.isPartitioned()) {
            return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                    "Not allowed to set/get topic policy for a partition"));
        }
        CompletableFuture<Void> ret;
        if (topicName.isGlobal()) {
            ret = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            ret = CompletableFuture.completedFuture(null);
        }
        return ret
                .thenCompose(__ -> checkTopicExistsAsync(topicName))
                .thenCompose(exist -> {
                    if (!exist) {
                        throw new RestException(Status.NOT_FOUND, "Topic not found");
                    } else {
                        return getPartitionedTopicMetadataAsync(topicName, false, false)
                            .thenCompose(metadata -> {
                                if (metadata.partitions > 0) {
                                    return validateTopicOwnershipAsync(TopicName.get(topicName.toString()
                                    + TopicName.PARTITIONED_TOPIC_SUFFIX + 0), authoritative);
                                } else {
                                    return validateTopicOwnershipAsync(topicName, authoritative);
                                }
                        });
                    }
        });
    }

    protected CompletableFuture<Void> internalRemoveMaxProducers(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                if (!op.isPresent()) {
                    return CompletableFuture.completedFuture(null);
                }
                op.get().setMaxProducerPerTopic(null);
                op.get().setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
            });
    }

    protected CompletableFuture<Integer> internalGetMaxConsumers(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getMaxConsumerPerTopic)
                .orElseGet(() -> {
                    if (applied) {
                        Integer maxConsumer = getNamespacePolicies(namespaceName).max_consumers_per_topic;
                        return maxConsumer == null ? config().getMaxConsumersPerTopic() : maxConsumer;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetMaxConsumers(Integer maxConsumers, boolean isGlobal) {
        if (maxConsumers != null && maxConsumers < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxConsumers must be 0 or more");
        }
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setMaxConsumerPerTopic(maxConsumers);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Void> internalRemoveMaxConsumers(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                if (!op.isPresent()) {
                    return CompletableFuture.completedFuture(null);
                }
                op.get().setMaxConsumerPerTopic(null);
                op.get().setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
            });

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
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }

        future.thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.TERMINATE)
                .thenCompose(unused -> getPartitionedTopicMetadataAsync(topicName, authoritative, false))
                .thenAccept(partitionMetadata -> {
                    if (partitionMetadata.partitions == 0) {
                        String msg = "Termination of a non-partitioned topic is not allowed using partitioned-terminate"
                                + ", please use terminate commands";
                        log.error("[{}] [{}] {}", clientAppId(), topicName, msg);
                        asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED, msg));
                        return;
                    }
                    if (partitionMetadata.partitions > 0) {
                        Map<Integer, MessageId> messageIds = new ConcurrentHashMap<>(partitionMetadata.partitions);
                        final List<CompletableFuture<MessageId>> futures =
                                Lists.newArrayListWithCapacity(partitionMetadata.partitions);

                        for (int i = 0; i < partitionMetadata.partitions; i++) {
                            TopicName topicNamePartition = topicName.getPartition(i);
                            try {
                                int finalI = i;
                                futures.add(pulsar().getAdminClient().topics()
                                        .terminateTopicAsync(topicNamePartition.toString())
                                        .whenComplete((messageId, throwable) -> {
                                            if (throwable != null) {
                                                log.error("[{}] Failed to terminate topic {}", clientAppId(),
                                                        topicNamePartition, throwable);
                                                asyncResponse.resume(new RestException(throwable));
                                            }
                                            messageIds.put(finalI, messageId);
                                        }));
                            } catch (Exception e) {
                                log.error("[{}] Failed to terminate topic {}", clientAppId(), topicNamePartition,
                                        e);
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
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to terminate topic {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                })
        ).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (!isRedirectException(ex)) {
                log.error("[{}] Failed to terminate topic {}", clientAppId(), topicName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    protected void internalExpireMessagesByTimestamp(AsyncResponse asyncResponse, String subName,
                                                     int expireTimeInSeconds, boolean authoritative) {
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }
        future.thenCompose(__ ->
            // If the topic name is a partition name, no need to get partition topic metadata again
            getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                    .thenCompose(partitionMetadata -> {
                        if (topicName.isPartitioned()) {
                            return internalExpireMessagesByTimestampForSinglePartitionAsync(partitionMetadata, subName,
                                    expireTimeInSeconds, authoritative)
                                    .thenAccept(unused -> asyncResponse.resume(Response.noContent().build()));
                        } else {
                            if (partitionMetadata.partitions > 0) {
                                return CompletableFuture.completedFuture(null).thenAccept(unused -> {
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
                                            log.error("[{}] Failed to expire messages up to {} on {}", clientAppId(),
                                                    expireTimeInSeconds, topicNamePartition, e);
                                            asyncResponse.resume(new RestException(e));
                                            return;
                                        }
                                    }

                                    FutureUtil.waitForAll(futures).handle((result, exception) -> {
                                        if (exception != null) {
                                            Throwable t = exception.getCause();
                                            if (t instanceof NotFoundException) {
                                                asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                                        "Subscription not found"));
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
                                });
                            } else {
                                return internalExpireMessagesByTimestampForSinglePartitionAsync(partitionMetadata,
                                        subName, expireTimeInSeconds, authoritative)
                                        .thenAccept(unused -> asyncResponse.resume(Response.noContent().build()));
                            }
                        }
                    })
        ).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (!isRedirectException(ex)) {
                log.error("[{}] Failed to expire messages up to {} on {}", clientAppId(),
                        expireTimeInSeconds, topicName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    private CompletableFuture<Void> internalExpireMessagesByTimestampForSinglePartitionAsync(
            PartitionedTopicMetadata partitionMetadata, String subName, int expireTimeInSeconds,
            boolean authoritative) {
        if (!topicName.isPartitioned() && partitionMetadata.partitions > 0) {
            String msg = "This method should not be called for partitioned topic";
            return FutureUtil.failedFuture(new IllegalStateException(msg));
        } else {
            final CompletableFuture<Void> resultFuture = new CompletableFuture<>();
            validateTopicOperationAsync(topicName, TopicOperation.EXPIRE_MESSAGES)
                    .thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative))
                    .thenCompose(__ -> getTopicReferenceAsync(topicName).thenAccept(t -> {
                         if (t == null) {
                             resultFuture.completeExceptionally(new RestException(Status.NOT_FOUND, "Topic not found"));
                             return;
                         }
                        if (!(t instanceof PersistentTopic)) {
                            resultFuture.completeExceptionally(new RestException(Status.METHOD_NOT_ALLOWED,
                                    "Expire messages on a non-persistent topic is not allowed"));
                            return;
                        }
                        PersistentTopic topic = (PersistentTopic) t;

                        boolean issued;
                        if (subName.startsWith(topic.getReplicatorPrefix())) {
                            String remoteCluster = PersistentReplicator.getRemoteCluster(subName);
                            PersistentReplicator repl = (PersistentReplicator) topic
                                    .getPersistentReplicator(remoteCluster);
                            if (repl == null) {
                                resultFuture.completeExceptionally(
                                        new RestException(Status.NOT_FOUND, "Replicator not found"));
                                return;
                            }
                            issued = repl.expireMessages(expireTimeInSeconds);
                        } else {
                            PersistentSubscription sub = topic.getSubscription(subName);
                            if (sub == null) {
                                resultFuture.completeExceptionally(
                                        new RestException(Status.NOT_FOUND, "Subscription not found"));
                                return;
                            }
                            issued = sub.expireMessages(expireTimeInSeconds);
                        }
                        if (issued) {
                            log.info("[{}] Message expire started up to {} on {} {}", clientAppId(),
                                    expireTimeInSeconds, topicName, subName);
                            resultFuture.complete(__);
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("Expire message by timestamp not issued on topic {} for subscription {} "
                                        + "due to ongoing message expiration not finished or subscription almost"
                                        + " catch up. If it's performed on a partitioned topic operation might "
                                        + "succeeded on other partitions, please check stats of individual "
                                        + "partition.", topicName, subName);
                            }
                            resultFuture.completeExceptionally(new RestException(Status.CONFLICT, "Expire message "
                                    + "by timestamp not issued on topic " + topicName + " for subscription "
                                    + subName + " due to ongoing message expiration not finished or subscription "
                                    + "almost catch  up. If it's performed on a partitioned topic operation might"
                                    + " succeeded on other partitions, please check stats of individual partition."
                            ));
                            return;
                        }
                            })
                    ).exceptionally(e -> {
                resultFuture.completeExceptionally(FutureUtil.unwrapCompletionException(e));
                return null;
            });
            return resultFuture;
        }
    }

    protected void internalExpireMessagesByPosition(AsyncResponse asyncResponse, String subName, boolean authoritative,
                                                 MessageIdImpl messageId, boolean isExcluded, int batchIndex) {
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }

        future.thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.EXPIRE_MESSAGES))
                .thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative))
                .thenCompose(__ -> {
                    log.info("[{}][{}] received expire messages on subscription {} to position {}", clientAppId(),
                            topicName, subName, messageId);
                    return getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                            .thenAccept(partitionMetadata -> {
                                if (!topicName.isPartitioned() && partitionMetadata.partitions > 0) {
                                    String msg = "Expire message at position is not supported for partitioned-topic";
                                    log.warn("[{}] {} {}({}) {}", clientAppId(), msg, topicName, messageId, subName);
                                    asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED, msg));
                                    return;
                                } else if (messageId.getPartitionIndex() != topicName.getPartitionIndex()) {
                                    String msg = "Invalid parameter for expire message by position, partition index of "
                                            + "passed in message position doesn't match partition index for the topic";
                                    log.warn("[{}] {} {}({}).", clientAppId(), msg, topicName, messageId);
                                    asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED, msg));
                                    return;
                                } else {
                                    internalExpireMessagesNonPartitionedTopicByPosition(asyncResponse, subName,
                                            messageId, isExcluded, batchIndex);
                                }
                            });
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to expire messages up to {} on subscription {} to position {}",
                                clientAppId(), topicName, subName, messageId, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    private CompletableFuture<Void> internalExpireMessagesNonPartitionedTopicByPosition(AsyncResponse asyncResponse,
                                                                                        String subName,
                                                                                        MessageIdImpl messageId,
                                                                                        boolean isExcluded,
                                                                                        int batchIndex) {
        return getTopicReferenceAsync(topicName).thenAccept(t -> {
            PersistentTopic topic = (PersistentTopic) t;
            if (topic == null) {
                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Topic not found"));
                return;
            }
            try {
                PersistentSubscription sub = topic.getSubscription(subName);
                if (sub == null) {
                    asyncResponse.resume(new RestException(Status.NOT_FOUND,
                            "Subscription not found"));
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
                            if (repl == null) {
                                asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                        "Replicator not found"));
                                return;
                            }
                            issued = repl.expireMessages(position);
                        } else {
                            issued = sub.expireMessages(position);
                        }
                        if (issued) {
                            log.info("[{}] Message expire started up to {} on {} {}", clientAppId(), position,
                                    topicName, subName);
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("Expire message by position not issued on topic {} for subscription {} "
                                        + "due to ongoing message expiration not finished or subscription almost "
                                        + "catch up.", topicName, subName);
                            }
                            throw new RestException(Status.CONFLICT, "Expire message by position not issued on topic "
                                    + topicName + " for subscription " + subName + " due to ongoing"
                                    + " message expiration not finished or invalid message position provided.");
                        }
                    } catch (Exception exception) {
                        log.error("[{}] Failed to expire messages up to {} on {} with subscription {} {}",
                                clientAppId(), position, topicName, subName, exception);
                        throw new RestException(exception);
                    }
                    asyncResponse.resume(Response.noContent().build());
                }).exceptionally(e -> {
                    log.error("[{}] Failed to expire messages up to {} on {} with subscription {} {}",
                            clientAppId(), messageId, topicName, subName, e);
                    asyncResponse.resume(e);
                    return null;
                });
            } catch (Exception e) {
                log.warn("[{}][{}] Failed to expire messages up to {} on subscription {} to position {}",
                        clientAppId(), topicName, messageId, subName, messageId, e);
                resumeAsyncResponseExceptionally(asyncResponse, e);
            }
        }).exceptionally(ex -> {
            Throwable cause = ex.getCause();
            log.error("[{}] Failed to expire messages up to {} on subscription {} to position {}", clientAppId(),
                    topicName, subName, messageId, cause);
            resumeAsyncResponseExceptionally(asyncResponse, cause);
            return null;
        });
    }

    protected void internalTriggerCompaction(AsyncResponse asyncResponse, boolean authoritative) {
        log.info("[{}] Trigger compaction on topic {}", clientAppId(), topicName);
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }
        future.thenAccept(__ -> {
            // If the topic name is a partition name, no need to get partition topic metadata again
            if (topicName.isPartitioned()) {
                internalTriggerCompactionNonPartitionedTopic(asyncResponse, authoritative);
            } else {
                getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                        .thenAccept(partitionMetadata -> {
                    final int numPartitions = partitionMetadata.partitions;
                    if (numPartitions > 0) {
                        final List<CompletableFuture<Void>> futures = Lists.newArrayListWithCapacity(numPartitions);

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
                        internalTriggerCompactionNonPartitionedTopic(asyncResponse, authoritative);
                    }
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to trigger compaction on topic {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
            }
        }).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (!isRedirectException(ex)) {
                log.error("[{}] Failed to validate global namespace ownership to trigger compaction on topic {}",
                        clientAppId(), topicName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    protected void internalTriggerCompactionNonPartitionedTopic(AsyncResponse asyncResponse, boolean authoritative) {
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.COMPACT))
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenAccept(topic -> {
                    try {
                        ((PersistentTopic) topic).triggerCompaction();
                        asyncResponse.resume(Response.noContent().build());
                    } catch (AlreadyRunningException e) {
                        resumeAsyncResponseExceptionally(asyncResponse,
                                new RestException(Status.CONFLICT, e.getMessage()));
                        return;
                    } catch (Exception e) {
                        log.error("[{}] Failed to trigger compaction on topic {}", clientAppId(),
                                topicName, e);
                        resumeAsyncResponseExceptionally(asyncResponse, new RestException(e));
                        return;
                    }
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to trigger compaction for {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                }
        );
    }

    protected LongRunningProcessStatus internalCompactionStatus(boolean authoritative) {
        validateTopicOwnership(topicName, authoritative);
        validateTopicOperation(topicName, TopicOperation.COMPACT);

        PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
        return topic.compactionStatus();
    }

    protected void internalTriggerOffload(AsyncResponse asyncResponse,
                                          boolean authoritative, MessageIdImpl messageId) {
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.OFFLOAD))
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenAccept(topic -> {
                    try {
                        ((PersistentTopic) topic).triggerOffload(messageId);
                        asyncResponse.resume(Response.noContent().build());
                    } catch (AlreadyRunningException e) {
                        resumeAsyncResponseExceptionally(asyncResponse,
                                new RestException(Status.CONFLICT, e.getMessage()));
                        return;
                    } catch (Exception e) {
                        log.warn("Unexpected error triggering offload", e);
                        resumeAsyncResponseExceptionally(asyncResponse, new RestException(e));
                        return;
                    }
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to trigger offload for {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    protected void internalOffloadStatus(AsyncResponse asyncResponse, boolean authoritative) {
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.OFFLOAD))
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenAccept(topic -> {
                    OffloadProcessStatus offloadProcessStatus = ((PersistentTopic) topic).offloadStatus();
                    asyncResponse.resume(offloadProcessStatus);
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to offload status on topic {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
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
                    log.warn("Failed to authorize {} on topic {}", clientAppId, topicName);
                    throw new PulsarClientException(String.format("Authorization failed %s on topic %s with error %s",
                            clientAppId, topicName, authException.getMessage()));
                }
            } catch (Exception ex) {
                // throw without wrapping to PulsarClientException that considers: unknown error marked as internal
                // server error
                log.warn("Failed to authorize {} on topic {}", clientAppId, topicName, ex);
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
                    .get(pulsar().getConfiguration().getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS)
                    .orElseThrow(() -> topicNotFoundReason(topicName));
        } catch (RestException e) {
            throw e;
        } catch (Exception e) {
            if (e.getCause() instanceof NotAllowedException) {
                throw new RestException(Status.BAD_REQUEST, e.getCause());
            }
            throw new RestException(e.getCause() == null ? e : e.getCause());
        }
    }

    private CompletableFuture<Topic> getTopicReferenceAsync(TopicName topicName) {
        return pulsar().getBrokerService().getTopicIfExists(topicName.toString())
                .thenCompose(optTopic -> optTopic
                        .map(CompletableFuture::completedFuture)
                        .orElseGet(() -> topicNotFoundReasonAsync(topicName)));
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
        } else if (!internalGetList(Optional.empty()).contains(topicName.toString())) {
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
                    } else if (!internalGetList(Optional.empty()).contains(topicName.toString())) {
                        throw new RestException(Status.NOT_FOUND, "Topic partitions were not yet created");
                    }
                    throw new RestException(Status.NOT_FOUND, "Partitioned Topic not found");
                });
    }

    private Topic getOrCreateTopic(TopicName topicName, Map<String, String> properties) {
        return pulsar().getBrokerService().getTopic(topicName.toString(), true, properties)
                .thenApply(Optional::get).join();
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


    private CompletableFuture<Void> updatePartitionedTopic(TopicName topicName, int numPartitions, boolean force) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        createSubscriptions(topicName, numPartitions).thenCompose(__ -> {
            CompletableFuture<Void> future = namespaceResources().getPartitionedTopicResources()
                    .updatePartitionedTopicAsync(topicName, p -> new PartitionedTopicMetadata(numPartitions));
            future.exceptionally(ex -> {
                // If the update operation fails, clean up the partitions that were created
                getPartitionedTopicMetadataAsync(topicName, false, false).thenAccept(metadata -> {
                    int oldPartition = metadata.partitions;
                    for (int i = oldPartition; i < numPartitions; i++) {
                        topicResources().deletePersistentTopicAsync(topicName.getPartition(i)).exceptionally(ex1 -> {
                            log.warn("[{}] Failed to clean up managedLedger {}", clientAppId(), topicName,
                                    ex1.getCause());
                            return null;
                        });
                    }
                }).exceptionally(e -> {
                    log.warn("[{}] Failed to clean up managedLedger", topicName, e);
                    return null;
                });
                return null;
            });
            return future;
        }).thenAccept(__ -> result.complete(null)).exceptionally(ex -> {
            if (force && ex.getCause() instanceof PulsarAdminException.ConflictException) {
                result.complete(null);
                return null;
            }
            result.completeExceptionally(ex);
            return null;
        });
        return result;
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
        List<String> existingTopicList = internalGetList(Optional.empty());
        TopicName partitionTopicName = TopicName.get(domain(), namespaceName, topicName);
        PartitionedTopicMetadata metadata = getPartitionedTopicMetadata(partitionTopicName, false, false);
        int oldPartition = metadata.partitions;
        String prefix = partitionTopicName.getPartitionedTopicName() + TopicName.PARTITIONED_TOPIC_SUFFIX;
        for (String exsitingTopicName : existingTopicList) {
            if (exsitingTopicName.startsWith(prefix)) {
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
                                "Already have non partition topic " + exsitingTopicName
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
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.PEEK_MESSAGES))
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenAccept(topic -> {
                    if (topic == null) {
                        asyncResponse.resume(new RestException(Status.NOT_FOUND, "Topic not found"));
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
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to get last messageId {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
        });
    }

    protected CompletableFuture<DispatchRateImpl> internalGetDispatchRate(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getDispatchRate)
                .orElseGet(() -> {
                    if (applied) {
                        DispatchRateImpl namespacePolicy = getNamespacePolicies(namespaceName)
                                .topicDispatchRate.get(pulsar().getConfiguration().getClusterName());
                        return namespacePolicy == null ? dispatchRate() : namespacePolicy;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetDispatchRate(DispatchRateImpl dispatchRate, boolean isGlobal) {
        if (dispatchRate == null) {
            return CompletableFuture.completedFuture(null);
        }
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setDispatchRate(dispatchRate);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Void> internalRemoveDispatchRate(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                if (!op.isPresent()) {
                    return CompletableFuture.completedFuture(null);
                }
                TopicPolicies topicPolicies = op.get();
                topicPolicies.setDispatchRate(null);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
            });
    }

    protected CompletableFuture<DispatchRate> internalGetSubscriptionDispatchRate(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getSubscriptionDispatchRate)
                .orElseGet(() -> {
                    if (applied) {
                        DispatchRateImpl namespacePolicy = getNamespacePolicies(namespaceName)
                                .subscriptionDispatchRate.get(pulsar().getConfiguration().getClusterName());
                        return namespacePolicy == null ? subscriptionDispatchRate() : namespacePolicy;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetSubscriptionDispatchRate
            (DispatchRateImpl dispatchRate, boolean isGlobal) {
        if (dispatchRate == null) {
            return CompletableFuture.completedFuture(null);
        }
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setSubscriptionDispatchRate(dispatchRate);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Void> internalRemoveSubscriptionDispatchRate(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                if (!op.isPresent()) {
                    return CompletableFuture.completedFuture(null);
                }
                TopicPolicies topicPolicies = op.get();
                topicPolicies.setSubscriptionDispatchRate(null);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
            });
    }


    protected CompletableFuture<Optional<Integer>> internalGetMaxConsumersPerSubscription(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                .thenApply(op -> op.map(TopicPolicies::getMaxConsumersPerSubscription));
    }

    protected CompletableFuture<Void> internalSetMaxConsumersPerSubscription(
            Integer maxConsumersPerSubscription, boolean isGlobal) {
        if (maxConsumersPerSubscription != null && maxConsumersPerSubscription < 0) {
            throw new RestException(Status.PRECONDITION_FAILED, "Invalid value for maxConsumersPerSubscription");
        }
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setMaxConsumersPerSubscription(maxConsumersPerSubscription);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Void> internalRemoveMaxConsumersPerSubscription(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                if (!op.isPresent()) {
                    return CompletableFuture.completedFuture(null);
                }
                op.get().setMaxConsumersPerSubscription(null);
                op.get().setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
            });
    }

    protected CompletableFuture<Long> internalGetCompactionThreshold(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getCompactionThreshold)
                .orElseGet(() -> {
                    if (applied) {
                        Long namespacePolicy = getNamespacePolicies(namespaceName).compaction_threshold;
                        return namespacePolicy == null
                                ? pulsar().getConfiguration().getBrokerServiceCompactionThresholdInBytes()
                                : namespacePolicy;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetCompactionThreshold(Long compactionThreshold, boolean isGlobal) {
        if (compactionThreshold != null && compactionThreshold < 0) {
            throw new RestException(Status.PRECONDITION_FAILED, "Invalid value for compactionThreshold");
        }

        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setCompactionThreshold(compactionThreshold);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });

    }

    protected CompletableFuture<Void> internalRemoveCompactionThreshold(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                if (!op.isPresent()) {
                    return CompletableFuture.completedFuture(null);
                }
                TopicPolicies topicPolicies = op.get();
                topicPolicies.setCompactionThreshold(null);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
            });
    }

    protected CompletableFuture<Optional<PublishRate>> internalGetPublishRate(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getPublishRate));
    }

    protected CompletableFuture<Void> internalSetPublishRate(PublishRate publishRate, boolean isGlobal) {
        if (publishRate == null) {
            return CompletableFuture.completedFuture(null);
        }
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setPublishRate(publishRate);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Optional<List<SubType>>> internalGetSubscriptionTypesEnabled(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getSubscriptionTypesEnabled));
    }

    protected CompletableFuture<Void> internalSetSubscriptionTypesEnabled(
            Set<SubscriptionType> subscriptionTypesEnabled, boolean isGlobal) {
        List<SubType> subTypes = Lists.newArrayList();
        subscriptionTypesEnabled.forEach(subscriptionType -> subTypes.add(SubType.valueOf(subscriptionType.name())));
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setSubscriptionTypesEnabled(subTypes);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Void> internalRemoveSubscriptionTypesEnabled(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                .thenCompose(op -> {
                    if (!op.isPresent()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    op.get().setSubscriptionTypesEnabled(Lists.newArrayList());
                    op.get().setIsGlobal(isGlobal);
                    return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
                });
    }

    protected CompletableFuture<Void> internalRemovePublishRate(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                if (!op.isPresent()) {
                    return CompletableFuture.completedFuture(null);
                }
                op.get().setPublishRate(null);
                op.get().setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
            });
    }

    protected CompletableFuture<SubscribeRate> internalGetSubscribeRate(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getSubscribeRate)
                .orElseGet(() -> {
                    if (applied) {
                        SubscribeRate namespacePolicy = getNamespacePolicies(namespaceName)
                                .clusterSubscribeRate.get(pulsar().getConfiguration().getClusterName());
                        return namespacePolicy == null ? subscribeRate() : namespacePolicy;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetSubscribeRate(SubscribeRate subscribeRate, boolean isGlobal) {
        if (subscribeRate == null) {
            return CompletableFuture.completedFuture(null);
        }
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setSubscribeRate(subscribeRate);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Void> internalRemoveSubscribeRate(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                if (!op.isPresent()) {
                    return CompletableFuture.completedFuture(null);
                }
                op.get().setSubscribeRate(null);
                op.get().setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
            });
    }

    protected void handleTopicPolicyException(String methodName, Throwable thr, AsyncResponse asyncResponse) {
        Throwable cause = thr.getCause();
        if (!(cause instanceof WebApplicationException) || !(
                ((WebApplicationException) cause).getResponse().getStatus() == 307
                        || ((WebApplicationException) cause).getResponse().getStatus() == 404)) {
            log.error("[{}] Failed to perform {} on topic {}",
                    clientAppId(), methodName, topicName, cause);
        }
        resumeAsyncResponseExceptionally(asyncResponse, cause);
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
            }).exceptionally(ex -> {
                // If the exception is not redirect exception we need to log it.
                if (!isRedirectException(ex)) {
                    log.error("[{}] Failed to truncate topic {}", clientAppId(), topicName, ex);
                }
                resumeAsyncResponseExceptionally(asyncResponse, ex);
                return null;
            });
        }
    }

    protected void internalSetReplicatedSubscriptionStatus(AsyncResponse asyncResponse, String subName,
            boolean authoritative, boolean enabled) {
        log.info("[{}] Attempting to change replicated subscription status to {} - {} {}", clientAppId(), enabled,
                topicName, subName);

        // Reject the request if the topic is not persistent
        if (!topicName.isPersistent()) {
            asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED,
                    "Cannot enable/disable replicated subscriptions on non-persistent topics"));
            return;
        }

        // Reject the request if the topic is not global
        if (!topicName.isGlobal()) {
            asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED,
                    "Cannot enable/disable replicated subscriptions on non-global topics"));
            return;
        }

        // 1.Permission to consume this topic is required
        // 2.Redirect the request to the peer-cluster if the local cluster is not included in the replication clusters
        CompletableFuture<Void> validateFuture =
                validateTopicOperationAsync(topicName, TopicOperation.SET_REPLICATED_SUBSCRIPTION_STATUS, subName)
                        .thenCompose(__ -> validateGlobalNamespaceOwnershipAsync(namespaceName));


        CompletableFuture<Void> resultFuture;
        // If the topic name is a partition name, no need to get partition topic metadata again
        if (topicName.isPartitioned()) {
            resultFuture = validateFuture.thenAccept(
                    __ -> internalSetReplicatedSubscriptionStatusForNonPartitionedTopic(asyncResponse, subName,
                            authoritative, enabled));
        } else {
            resultFuture = validateFuture.
                    thenCompose(__ -> getPartitionedTopicMetadataAsync(topicName, authoritative, false))
                    .thenAccept(partitionMetadata -> {
                        if (partitionMetadata.partitions > 0) {
                            final List<CompletableFuture<Void>> futures = Lists.newArrayList();

                            for (int i = 0; i < partitionMetadata.partitions; i++) {
                                TopicName topicNamePartition = topicName.getPartition(i);
                                try {
                                    futures.add(pulsar().getAdminClient().topics().setReplicatedSubscriptionStatusAsync(
                                            topicNamePartition.toString(), subName, enabled));
                                } catch (Exception e) {
                                    log.warn("[{}] Failed to change replicated subscription status to {} - {} {}",
                                            clientAppId(), enabled, topicNamePartition, subName, e);
                                    resumeAsyncResponseExceptionally(asyncResponse, e);
                                    return;
                                }
                            }

                            FutureUtil.waitForAll(futures).handle((result, exception) -> {
                                if (exception != null) {
                                    Throwable t = exception.getCause();
                                    if (t instanceof NotFoundException) {
                                        asyncResponse
                                                .resume(new RestException(Status.NOT_FOUND,
                                                        "Topic or subscription not found"));
                                        return null;
                                    } else if (t instanceof PreconditionFailedException) {
                                        asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                                                "Cannot enable/disable replicated subscriptions on non-global topics"));
                                        return null;
                                    } else {
                                        log.warn("[{}] Failed to change replicated subscription status to {} - {} {}",
                                                clientAppId(), enabled, topicName, subName, t);
                                        asyncResponse.resume(new RestException(t));
                                        return null;
                                    }
                                }

                                asyncResponse.resume(Response.noContent().build());
                                return null;
                            });
                        } else {
                            internalSetReplicatedSubscriptionStatusForNonPartitionedTopic(asyncResponse, subName,
                                    authoritative, enabled);
                        }
                    });
        }

        resultFuture.exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (!isRedirectException(ex)) {
                log.warn("[{}] Failed to change replicated subscription status to {} - {} {}", clientAppId(), enabled,
                        topicName, subName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    private void internalSetReplicatedSubscriptionStatusForNonPartitionedTopic(
            AsyncResponse asyncResponse, String subName, boolean authoritative, boolean enabled) {
        // Redirect the request to the appropriate broker if this broker is not the owner of the topic
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenAccept(topic -> {
                            if (topic == null) {
                                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Topic not found"));
                                return;
                            }

                            Subscription sub = topic.getSubscription(subName);
                            if (sub == null) {
                                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Subscription not found"));
                                return;
                            }

                            if (topic instanceof PersistentTopic && sub instanceof PersistentSubscription) {
                                if (!((PersistentSubscription) sub).setReplicated(enabled)) {
                                    asyncResponse.resume(
                                            new RestException(Status.INTERNAL_SERVER_ERROR,
                                                    "Failed to update cursor properties"));
                                    return;
                                }

                                ((PersistentTopic) topic).checkReplicatedSubscriptionControllerState();
                                log.info("[{}] Changed replicated subscription status to {} - {} {}", clientAppId(),
                                        enabled, topicName, subName);
                                asyncResponse.resume(Response.noContent().build());
                            } else {
                                asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED,
                                        "Cannot enable/disable replicated subscriptions on non-persistent topics"));
                            }
                        }
                ).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to set replicated subscription status on {} {}", clientAppId(),
                                topicName, subName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    protected void internalGetReplicatedSubscriptionStatus(AsyncResponse asyncResponse,
                                                           String subName,
                                                           boolean authoritative) {
        log.info("[{}] Attempting to get replicated subscription status on {} {}", clientAppId(), topicName, subName);

        // Reject the request if the topic is not persistent
        if (!topicName.isPersistent()) {
            asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED,
                    "Cannot get replicated subscriptions on non-persistent topics"));
            return;
        }

        // Reject the request if the topic is not global
        if (!topicName.isGlobal()) {
            asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED,
                    "Cannot get replicated subscriptions on non-global topics"));
            return;
        }

        // Permission to consume this topic is required
        try {
            validateTopicOperation(topicName, TopicOperation.GET_REPLICATED_SUBSCRIPTION_STATUS, subName);
        } catch (Exception e) {
            resumeAsyncResponseExceptionally(asyncResponse, e);
            return;
        }

        // If the topic name is a partition name, no need to get partition topic metadata again
        if (topicName.isPartitioned()) {
            internalGetReplicatedSubscriptionStatusForNonPartitionedTopic(asyncResponse, subName, authoritative);
        } else {
            getPartitionedTopicMetadataAsync(topicName,
                    authoritative, false).thenAccept(partitionMetadata -> {
                if (partitionMetadata.partitions > 0) {
                    final List<CompletableFuture<Map<String, Boolean>>> futures = Lists.newArrayList();
                    final Map<String, Boolean> status = Maps.newHashMap();

                    for (int i = 0; i < partitionMetadata.partitions; i++) {
                        TopicName partition = topicName.getPartition(i);
                        try {
                            futures.add(pulsar().getAdminClient().topics().getReplicatedSubscriptionStatusAsync(
                                    partition.toString(), subName).whenComplete((response, throwable) -> {
                                if (throwable != null) {
                                    log.error("[{}] Failed to get replicated subscriptions on {} {}",
                                            clientAppId(), partition, subName, throwable);
                                    asyncResponse.resume(new RestException(throwable));
                                }
                                status.putAll(response);
                            }));
                        } catch (Exception e) {
                            log.warn("[{}] Failed to get replicated subscription status on {} {}",
                                    clientAppId(), partition, subName, e);
                            throw new RestException(e);
                        }
                    }

                    FutureUtil.waitForAll(futures).handle((result, exception) -> {
                        if (exception != null) {
                            Throwable t = exception.getCause();
                            if (t instanceof NotFoundException) {
                                asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                        "Topic or subscription not found"));
                            } else if (t instanceof PreconditionFailedException) {
                                asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                                        "Cannot get replicated subscriptions on non-global topics"));
                            } else {
                                log.error("[{}] Failed to get replicated subscription status on {} {}",
                                        clientAppId(), topicName, subName, t);
                                asyncResponse.resume(new RestException(t));
                            }
                        }
                        asyncResponse.resume(status);
                        return null;
                    });
                } else {
                    internalGetReplicatedSubscriptionStatusForNonPartitionedTopic(asyncResponse, subName,
                            authoritative);
                }
            }).exceptionally(ex -> {
                // If the exception is not redirect exception we need to log it.
                if (!isRedirectException(ex)) {
                    log.error("[{}] Failed to get replicated subscription status on {} {}", clientAppId(),
                            topicName, subName, ex);
                }
                resumeAsyncResponseExceptionally(asyncResponse, ex);
                return null;
            });
        }
    }

    private void internalGetReplicatedSubscriptionStatusForNonPartitionedTopic(AsyncResponse asyncResponse,
                                                                               String subName,
                                                                               boolean authoritative) {
        try {
            // Redirect the request to the appropriate broker if this broker is not the owner of the topic
            validateTopicOwnership(topicName, authoritative);

            Topic topic = getTopicReference(topicName);
            if (topic == null) {
                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Topic not found"));
                return;
            }

            Subscription sub = topic.getSubscription(subName);
            if (sub == null) {
                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Subscription not found"));
                return;
            }

            if (topic instanceof PersistentTopic && sub instanceof PersistentSubscription) {
                Map res = Maps.newHashMap();
                res.put(topicName.toString(), sub.isReplicated());
                asyncResponse.resume(res);
            } else {
                asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED,
                        "Cannot get replicated subscriptions on non-persistent topics"));
            }
        } catch (Exception e) {
            log.error("[{}] Failed to get replicated subscription status on {} {}", clientAppId(),
                    topicName, subName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    protected CompletableFuture<SchemaCompatibilityStrategy> internalGetSchemaCompatibilityStrategy(boolean applied) {
        if (applied) {
            return getSchemaCompatibilityStrategyAsync();
        }
        return validateTopicPolicyOperationAsync(topicName,
                PolicyName.SCHEMA_COMPATIBILITY_STRATEGY,
                PolicyOperation.READ)
                .thenCompose(n -> getTopicPoliciesAsyncWithRetry(topicName).thenApply(op -> {
                    if (!op.isPresent()) {
                        return null;
                    }
                    SchemaCompatibilityStrategy strategy = op.get().getSchemaCompatibilityStrategy();
                    return SchemaCompatibilityStrategy.isUndefined(strategy) ? null : strategy;
                }));
    }

    protected CompletableFuture<Void> internalSetSchemaCompatibilityStrategy(SchemaCompatibilityStrategy strategy) {
        return validateTopicPolicyOperationAsync(topicName,
                PolicyName.SCHEMA_COMPATIBILITY_STRATEGY,
                PolicyOperation.WRITE)
                .thenCompose((__) -> getTopicPoliciesAsyncWithRetry(topicName)
                        .thenCompose(op -> {
                            TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                            topicPolicies.setSchemaCompatibilityStrategy(
                                    strategy == SchemaCompatibilityStrategy.UNDEFINED ? null : strategy);
                            return pulsar().getTopicPoliciesService()
                                    .updateTopicPoliciesAsync(topicName, topicPolicies);
                        }));
    }
}
