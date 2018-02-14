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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.apache.pulsar.common.util.Codec.decode;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

import org.apache.bookkeeper.mledger.AsyncCallbacks.ManagedLedgerInfoCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerOfflineBacklog;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
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
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyValue;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.DestinationDomain;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.AuthPolicies;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.zafarkhaja.semver.Version;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

/**
 */
public class PersistentTopicsBase extends AdminResource {
    private static final Logger log = LoggerFactory.getLogger(PersistentTopicsBase.class);

    protected static final int PARTITIONED_TOPIC_WAIT_SYNC_TIME_MS = 1000;
    private static final int OFFLINE_TOPIC_STAT_TTL_MINS = 10;
    private static final String DEPRECATED_CLIENT_VERSION_PREFIX = "Pulsar-CPP-v";
    private static final Version LEAST_SUPPORTED_CLIENT_VERSION_PREFIX = Version.forIntegers(1, 21);

    protected List<String> internalGetList() {
        validateAdminAccessOnProperty(namespaceName.getProperty());

        // Validate that namespace exists, throws 404 if it doesn't exist
        try {
            policiesCache().get(path(POLICIES, namespaceName.toString()));
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to get topic list {}: Namespace does not exist", clientAppId(), namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to get topic list {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }

        List<String> destinations = Lists.newArrayList();

        try {
            String path = String.format("/managed-ledgers/%s/%s", namespaceName.toString(), domain());
            for (String destination : managedLedgerListCache().get(path)) {
                if (domain().equals(DestinationDomain.persistent.toString())) {
                    destinations.add(DestinationName.get(domain(), namespaceName, decode(destination)).toString());
                }
            }
        } catch (KeeperException.NoNodeException e) {
            // NoNode means there are no destination in this domain for this namespace
        } catch (Exception e) {
            log.error("[{}] Failed to get destination list for namespace {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }

        destinations.sort(null);
        return destinations;
    }

    protected List<String> internalGetPartitionedTopicList() {
        validateAdminAccessOnProperty(namespaceName.getProperty());

        // Validate that namespace exists, throws 404 if it doesn't exist
        try {
            policiesCache().get(path(POLICIES, namespaceName.toString()));
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to get partitioned topic list {}: Namespace does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to get partitioned topic list for namespace {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }

        List<String> partitionedTopics = Lists.newArrayList();

        try {
            String partitionedTopicPath = path(PARTITIONED_TOPIC_PATH_ZNODE, namespaceName.toString(), domain());
            List<String> destinations = globalZk().getChildren(partitionedTopicPath, false);
            partitionedTopics = destinations.stream()
                    .map(s -> String.format("persistent://%s/%s", namespaceName.toString(), decode(s)))
                    .collect(Collectors.toList());
        } catch (KeeperException.NoNodeException e) {
            // NoNode means there are no partitioned topics in this domain for this namespace
        } catch (Exception e) {
            log.error("[{}] Failed to get partitioned topic list for namespace {}", clientAppId(),
                    namespaceName.toString(), e);
            throw new RestException(e);
        }

        partitionedTopics.sort(null);
        return partitionedTopics;
    }

    protected Map<String, Set<AuthAction>> internalGetPermissionsOnDestination() {
        // This operation should be reading from zookeeper and it should be allowed without having admin privileges
        validateAdminAccessOnProperty(namespaceName.getProperty());

        String destinationUri = destinationName.toString();

        try {
            Policies policies = policiesCache().get(path(POLICIES, namespaceName.toString()))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Namespace does not exist"));

            Map<String, Set<AuthAction>> permissions = Maps.newTreeMap();
            AuthPolicies auth = policies.auth_policies;

            // First add namespace level permissions
            for (String role : auth.namespace_auth.keySet()) {
                permissions.put(role, auth.namespace_auth.get(role));
            }

            // Then add destination level permissions
            if (auth.destination_auth.containsKey(destinationUri)) {
                for (Map.Entry<String, Set<AuthAction>> entry : auth.destination_auth.get(destinationUri).entrySet()) {
                    String role = entry.getKey();
                    Set<AuthAction> destinationPermissions = entry.getValue();

                    if (!permissions.containsKey(role)) {
                        permissions.put(role, destinationPermissions);
                    } else {
                        // Do the union between namespace and destination level
                        Set<AuthAction> union = Sets.union(permissions.get(role), destinationPermissions);
                        permissions.put(role, union);
                    }
                }
            }

            return permissions;
        } catch (Exception e) {
            log.error("[{}] Failed to get permissions for destination {}", clientAppId(), destinationUri, e);
            throw new RestException(e);
        }
    }

    protected void validateAdminAndClientPermission() {
        try {
            validateAdminAccessOnProperty(destinationName.getProperty());
        } catch (Exception ve) {
            try {
                checkAuthorization(pulsar(), destinationName, clientAppId(), clientAuthData());
            } catch (RestException re) {
                throw re;
            } catch (Exception e) {
                // unknown error marked as internal server error
                log.warn("Unexpected error while authorizing request. destination={}, role={}. Error: {}",
                        destinationName, clientAppId(), e.getMessage(), e);
                throw new RestException(e);
            }
        }
    }

    public void validateAdminOperationOnDestination(boolean authoritative) {
        validateAdminAccessOnProperty(destinationName.getProperty());
        validateDestinationOwnership(destinationName, authoritative);
    }

    protected void internalGrantPermissionsOnDestination(String role, Set<AuthAction> actions) {
        // This operation should be reading from zookeeper and it should be allowed without having admin privileges
        validateAdminAccessOnProperty(namespaceName.getProperty());
        validatePoliciesReadOnlyAccess();

        String destinationUri = destinationName.toString();

        try {
            Stat nodeStat = new Stat();
            byte[] content = globalZk().getData(path(POLICIES, namespaceName.toString()), null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);

            if (!policies.auth_policies.destination_auth.containsKey(destinationUri)) {
                policies.auth_policies.destination_auth.put(destinationUri, new TreeMap<String, Set<AuthAction>>());
            }

            policies.auth_policies.destination_auth.get(destinationUri).put(role, actions);

            // Write the new policies to zookeeper
            globalZk().setData(path(POLICIES, namespaceName.toString()), jsonMapper().writeValueAsBytes(policies),
                    nodeStat.getVersion());

            // invalidate the local cache to force update
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));

            log.info("[{}] Successfully granted access for role {}: {} - destination {}", clientAppId(), role, actions,
                    destinationUri);

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to grant permissions on destination {}: Namespace does not exist", clientAppId(),
                    destinationUri);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to grant permissions for destination {}", clientAppId(), destinationUri, e);
            throw new RestException(e);
        }
    }

    protected void internalRevokePermissionsOnDestination(String role) {
        // This operation should be reading from zookeeper and it should be allowed without having admin privileges
        validateAdminAccessOnProperty(namespaceName.getProperty());
        validatePoliciesReadOnlyAccess();

        String destinationUri = destinationName.toString();
        Stat nodeStat = new Stat();
        Policies policies;

        try {
            byte[] content = globalZk().getData(path(POLICIES, namespaceName.toString()), null, nodeStat);
            policies = jsonMapper().readValue(content, Policies.class);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to revoke permissions on destination {}: Namespace does not exist", clientAppId(),
                    destinationUri);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to revoke permissions for destination {}", clientAppId(), destinationUri, e);
            throw new RestException(e);
        }

        if (!policies.auth_policies.destination_auth.containsKey(destinationUri)
                || !policies.auth_policies.destination_auth.get(destinationUri).containsKey(role)) {
            log.warn("[{}] Failed to revoke permission from role {} on destination: Not set at destination level",
                    clientAppId(), role, destinationUri);
            throw new RestException(Status.PRECONDITION_FAILED, "Permissions are not set at the destination level");
        }

        policies.auth_policies.destination_auth.get(destinationUri).remove(role);

        try {
            // Write the new policies to zookeeper
            String namespacePath = path(POLICIES, namespaceName.toString());
            globalZk().setData(namespacePath, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());

            // invalidate the local cache to force update
            policiesCache().invalidate(namespacePath);
            globalZkCache().invalidate(namespacePath);

            log.info("[{}] Successfully revoke access for role {} - destination {}", clientAppId(), role,
                    destinationUri);
        } catch (Exception e) {
            log.error("[{}] Failed to revoke permissions for destination {}", clientAppId(), destinationUri, e);
            throw new RestException(e);
        }
    }

    protected void internalCreatePartitionedTopic(int numPartitions, boolean authoritative) {
        validateAdminAccessOnProperty(destinationName.getProperty());
        if (numPartitions <= 1) {
            throw new RestException(Status.NOT_ACCEPTABLE, "Number of partitions should be more than 1");
        }
        try {
            String path = path(PARTITIONED_TOPIC_PATH_ZNODE, namespaceName.toString(), domain(),
                    destinationName.getEncodedLocalName());
            byte[] data = jsonMapper().writeValueAsBytes(new PartitionedTopicMetadata(numPartitions));
            zkCreateOptimistic(path, data);
            // we wait for the data to be synced in all quorums and the observers
            Thread.sleep(PARTITIONED_TOPIC_WAIT_SYNC_TIME_MS);
            log.info("[{}] Successfully created partitioned topic {}", clientAppId(), destinationName);
        } catch (KeeperException.NodeExistsException e) {
            log.warn("[{}] Failed to create already existing partitioned topic {}", clientAppId(), destinationName);
            throw new RestException(Status.CONFLICT, "Partitioned topic already exist");
        } catch (Exception e) {
            log.error("[{}] Failed to create partitioned topic {}", clientAppId(), destinationName, e);
            throw new RestException(e);
        }
    }

    /**
     * It updates number of partitions of an existing non-global partitioned topic. It requires partitioned-topic to be
     * already exist and number of new partitions must be greater than existing number of partitions. Decrementing
     * number of partitions requires deletion of topic which is not supported.
     *
     * Already created partitioned producers and consumers can't see newly created partitions and it requires to
     * recreate them at application so, newly created producers and consumers can connect to newly added partitions as
     * well. Therefore, it can violate partition ordering at producers until all producers are restarted at application.
     *
     * @param numPartitions
     */
    protected void internalUpdatePartitionedTopic(int numPartitions) {
        validateAdminAccessOnProperty(destinationName.getProperty());
        if (destinationName.isGlobal()) {
            log.error("[{}] Update partitioned-topic is forbidden on global namespace {}", clientAppId(),
                    destinationName);
            throw new RestException(Status.FORBIDDEN, "Update forbidden on global namespace");
        }
        if (numPartitions <= 1) {
            throw new RestException(Status.NOT_ACCEPTABLE, "Number of partitions should be more than 1");
        }
        try {
            updatePartitionedTopic(destinationName, numPartitions).get();
        } catch (Exception e) {
            if (e.getCause() instanceof RestException) {
                throw (RestException) e.getCause();
            }
            log.error("[{}] Failed to update partitioned topic {}", clientAppId(), destinationName, e.getCause());
            throw new RestException(e.getCause());
        }
    }

    protected PartitionedTopicMetadata internalGetPartitionedMetadata(boolean authoritative) {
        PartitionedTopicMetadata metadata = getPartitionedTopicMetadata(destinationName, authoritative);
        if (metadata.partitions > 1) {
            validateClientVersion();
        }
        return metadata;
    }

    protected void internalDeletePartitionedTopic(boolean authoritative) {
        validateAdminAccessOnProperty(destinationName.getProperty());
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(destinationName, authoritative);
        int numPartitions = partitionMetadata.partitions;
        if (numPartitions > 0) {
            final CompletableFuture<Void> future = new CompletableFuture<>();
            final AtomicInteger count = new AtomicInteger(numPartitions);
            try {
                for (int i = 0; i < numPartitions; i++) {
                    DestinationName dn_partition = destinationName.getPartition(i);
                    pulsar().getAdminClient().persistentTopics().deleteAsync(dn_partition.toString())
                            .whenComplete((r, ex) -> {
                                if (ex != null) {
                                    if (ex instanceof NotFoundException) {
                                        // if the sub-topic is not found, the client might not have called create
                                        // producer or it might have been deleted earlier, so we ignore the 404 error.
                                        // For all other exception, we fail the delete partition method even if a single
                                        // partition is failed to be deleted
                                        if (log.isDebugEnabled()) {
                                            log.debug("[{}] Partition not found: {}", clientAppId(), dn_partition);
                                        }
                                    } else {
                                        future.completeExceptionally(ex);
                                        log.error("[{}] Failed to delete partition {}", clientAppId(), dn_partition,
                                                ex);
                                        return;
                                    }
                                } else {
                                    log.info("[{}] Deleted partition {}", clientAppId(), dn_partition);
                                }
                                if (count.decrementAndGet() == 0) {
                                    future.complete(null);
                                }
                            });
                }
                future.get();
            } catch (Exception e) {
                Throwable t = e.getCause();
                if (t instanceof PreconditionFailedException) {
                    throw new RestException(Status.PRECONDITION_FAILED, "Topic has active producers/subscriptions");
                } else {
                    throw new RestException(t);
                }
            }
        }

        // Only tries to delete the znode for partitioned topic when all its partitions are successfully deleted
        String path = path(PARTITIONED_TOPIC_PATH_ZNODE, namespaceName.toString(), domain(),
                destinationName.getEncodedLocalName());
        try {
            globalZk().delete(path, -1);
            globalZkCache().invalidate(path);
            // we wait for the data to be synced in all quorums and the observers
            Thread.sleep(PARTITIONED_TOPIC_WAIT_SYNC_TIME_MS);
            log.info("[{}] Deleted partitioned topic {}", clientAppId(), destinationName);
        } catch (KeeperException.NoNodeException nne) {
            throw new RestException(Status.NOT_FOUND, "Partitioned topic does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to delete partitioned topic {}", clientAppId(), destinationName, e);
            throw new RestException(e);
        }
    }

    protected void internalUnloadTopic(boolean authoritative) {
        log.info("[{}] Unloading topic {}", clientAppId(), destinationName);
        if (destinationName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        unloadTopic(destinationName, authoritative);
    }

    protected void internalDeleteTopic(boolean authoritative) {
        validateAdminOperationOnDestination(authoritative);
        Topic topic = getTopicReference(destinationName);
        if (destinationName.isGlobal()) {
            // Delete is disallowed on global topic
            log.error("[{}] Delete topic is forbidden on global namespace {}", clientAppId(), destinationName);
            throw new RestException(Status.FORBIDDEN, "Delete forbidden on global namespace");
        }

        try {
            topic.delete().get();
            log.info("[{}] Successfully removed topic {}", clientAppId(), destinationName);
        } catch (Exception e) {
            Throwable t = e.getCause();
            log.error("[{}] Failed to get delete topic {}", clientAppId(), destinationName, t);
            if (t instanceof TopicBusyException) {
                throw new RestException(Status.PRECONDITION_FAILED, "Topic has active producers/subscriptions");
            } else {
                throw new RestException(t);
            }
        }
    }

    protected List<String> internalGetSubscriptions(boolean authoritative) {
        if (destinationName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }

        List<String> subscriptions = Lists.newArrayList();
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(destinationName, authoritative);
        if (partitionMetadata.partitions > 0) {
            try {
                // get the subscriptions only from the 1st partition since all the other partitions will have the same
                // subscriptions
                subscriptions.addAll(pulsar().getAdminClient().persistentTopics()
                        .getSubscriptions(destinationName.getPartition(0).toString()));
            } catch (Exception e) {
                throw new RestException(e);
            }
        } else {
            validateAdminOperationOnDestination(authoritative);
            Topic topic = getTopicReference(destinationName);

            try {
                topic.getSubscriptions().forEach((subName, sub) -> subscriptions.add(subName));
            } catch (Exception e) {
                log.error("[{}] Failed to get list of subscriptions for {}", clientAppId(), destinationName);
                throw new RestException(e);
            }
        }

        return subscriptions;
    }

    protected PersistentTopicStats internalGetStats(boolean authoritative) {
        validateAdminAndClientPermission();
        if (destinationName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        validateDestinationOwnership(destinationName, authoritative);
        Topic topic = getTopicReference(destinationName);
        return topic.getStats();
    }

    protected PersistentTopicInternalStats internalGetInternalStats(boolean authoritative) {
        validateAdminAndClientPermission();
        if (destinationName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        validateDestinationOwnership(destinationName, authoritative);
        Topic topic = getTopicReference(destinationName);
        return topic.getInternalStats();
    }

    protected void internalGetManagedLedgerInfo(AsyncResponse asyncResponse) {
        validateAdminAccessOnProperty(destinationName.getProperty());
        if (destinationName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        String managedLedger = destinationName.getPersistenceNamingEncoding();
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

    protected PartitionedTopicStats internalGetPartitionedStats(boolean authoritative) {
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(destinationName, authoritative);
        if (partitionMetadata.partitions == 0) {
            throw new RestException(Status.NOT_FOUND, "Partitioned Topic not found");
        }
        if (destinationName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicStats stats = new PartitionedTopicStats(partitionMetadata);
        try {
            for (int i = 0; i < partitionMetadata.partitions; i++) {
                PersistentTopicStats partitionStats = pulsar().getAdminClient().persistentTopics()
                        .getStats(destinationName.getPartition(i).toString());
                stats.add(partitionStats);
                stats.partitions.put(destinationName.getPartition(i).toString(), partitionStats);
            }
        } catch (Exception e) {
            throw new RestException(e);
        }
        return stats;
    }

    protected void internalDeleteSubscription(String subName, boolean authoritative) {
        if (destinationName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(destinationName, authoritative);
        if (partitionMetadata.partitions > 0) {
            try {
                for (int i = 0; i < partitionMetadata.partitions; i++) {
                    pulsar().getAdminClient().persistentTopics()
                            .deleteSubscription(destinationName.getPartition(i).toString(), subName);
                }
            } catch (Exception e) {
                if (e instanceof NotFoundException) {
                    throw new RestException(Status.NOT_FOUND, "Subscription not found");
                } else if (e instanceof PreconditionFailedException) {
                    throw new RestException(Status.PRECONDITION_FAILED, "Subscription has active connected consumers");
                } else {
                    log.error("[{}] Failed to delete subscription {} {}", clientAppId(), destinationName, subName, e);
                    throw new RestException(e);
                }
            }
        } else {
            validateAdminOperationOnDestination(authoritative);
            Topic topic = getTopicReference(destinationName);
            try {
                Subscription sub = topic.getSubscription(subName);
                checkNotNull(sub);
                sub.delete().get();
                log.info("[{}][{}] Deleted subscription {}", clientAppId(), destinationName, subName);
            } catch (Exception e) {
                Throwable t = e.getCause();
                if (e instanceof NullPointerException) {
                    throw new RestException(Status.NOT_FOUND, "Subscription not found");
                } else if (t instanceof SubscriptionBusyException) {
                    throw new RestException(Status.PRECONDITION_FAILED, "Subscription has active connected consumers");
                } else {
                    log.error("[{}] Failed to delete subscription {} {}", clientAppId(), destinationName, subName, e);
                    throw new RestException(t);
                }
            }
        }
    }

    protected void internalSkipAllMessages(String subName, boolean authoritative) {
        if (destinationName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(destinationName, authoritative);
        if (partitionMetadata.partitions > 0) {
            try {
                for (int i = 0; i < partitionMetadata.partitions; i++) {
                    pulsar().getAdminClient().persistentTopics()
                            .skipAllMessages(destinationName.getPartition(i).toString(), subName);
                }
            } catch (Exception e) {
                throw new RestException(e);
            }
        } else {
            validateAdminOperationOnDestination(authoritative);
            PersistentTopic topic = (PersistentTopic) getTopicReference(destinationName);
            try {
                if (subName.startsWith(topic.replicatorPrefix)) {
                    String remoteCluster = PersistentReplicator.getRemoteCluster(subName);
                    PersistentReplicator repl = (PersistentReplicator) topic.getPersistentReplicator(remoteCluster);
                    checkNotNull(repl);
                    repl.clearBacklog().get();
                } else {
                    PersistentSubscription sub = topic.getSubscription(subName);
                    checkNotNull(sub);
                    sub.clearBacklog().get();
                }
                log.info("[{}] Cleared backlog on {} {}", clientAppId(), destinationName, subName);
            } catch (NullPointerException npe) {
                throw new RestException(Status.NOT_FOUND, "Subscription not found");
            } catch (Exception exception) {
                log.error("[{}] Failed to skip all messages {} {}", clientAppId(), destinationName, subName, exception);
                throw new RestException(exception);
            }
        }
    }

    protected void internalSkipMessages(String subName, int numMessages, boolean authoritative) {
        if (destinationName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(destinationName, authoritative);
        if (partitionMetadata.partitions > 0) {
            throw new RestException(Status.METHOD_NOT_ALLOWED, "Skip messages on a partitioned topic is not allowed");
        }
        validateAdminOperationOnDestination(authoritative);
        PersistentTopic topic = (PersistentTopic) getTopicReference(destinationName);
        try {
            if (subName.startsWith(topic.replicatorPrefix)) {
                String remoteCluster = PersistentReplicator.getRemoteCluster(subName);
                PersistentReplicator repl = (PersistentReplicator) topic.getPersistentReplicator(remoteCluster);
                checkNotNull(repl);
                repl.skipMessages(numMessages).get();
            } else {
                PersistentSubscription sub = topic.getSubscription(subName);
                checkNotNull(sub);
                sub.skipMessages(numMessages).get();
            }
            log.info("[{}] Skipped {} messages on {} {}", clientAppId(), numMessages, destinationName, subName);
        } catch (NullPointerException npe) {
            throw new RestException(Status.NOT_FOUND, "Subscription not found");
        } catch (Exception exception) {
            log.error("[{}] Failed to skip {} messages {} {}", clientAppId(), numMessages, destinationName, subName,
                    exception);
            throw new RestException(exception);
        }
    }

    protected void internalExpireMessagesForAllSubscriptions(int expireTimeInSeconds, boolean authoritative) {
        if (destinationName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(destinationName, authoritative);
        if (partitionMetadata.partitions > 0) {
            try {
                // expire messages for each partition destination
                for (int i = 0; i < partitionMetadata.partitions; i++) {
                    pulsar().getAdminClient().persistentTopics().expireMessagesForAllSubscriptions(
                            destinationName.getPartition(i).toString(), expireTimeInSeconds);
                }
            } catch (Exception e) {
                log.error("[{}] Failed to expire messages up to {} on {} {}", clientAppId(), expireTimeInSeconds,
                        destinationName, e);
                throw new RestException(e);
            }
        } else {
            // validate ownership and redirect if current broker is not owner
            validateAdminOperationOnDestination(authoritative);
            PersistentTopic topic = (PersistentTopic) getTopicReference(destinationName);
            topic.getReplicators().forEach((subName, replicator) -> {
                internalExpireMessages(subName, expireTimeInSeconds, authoritative);
            });
            topic.getSubscriptions().forEach((subName, subscriber) -> {
                internalExpireMessages(subName, expireTimeInSeconds, authoritative);
            });
        }
    }

    protected void internalResetCursor(String subName, long timestamp, boolean authoritative) {
        if (destinationName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(destinationName, authoritative);

        if (partitionMetadata.partitions > 0) {
            int numParts = partitionMetadata.partitions;
            int numPartException = 0;
            Exception partitionException = null;
            try {
                for (int i = 0; i < numParts; i++) {
                    pulsar().getAdminClient().persistentTopics().resetCursor(destinationName.getPartition(i).toString(),
                            subName, timestamp);
                }
            } catch (PreconditionFailedException pfe) {
                // throw the last exception if all partitions get this error
                // any other exception on partition is reported back to user
                ++numPartException;
                partitionException = pfe;
            } catch (Exception e) {
                log.warn("[{}] [{}] Failed to reset cursor on subscription {} to time {}", clientAppId(),
                        destinationName, subName, timestamp, e);
                throw new RestException(e);
            }
            // report an error to user if unable to reset for all partitions
            if (numPartException == numParts) {
                log.warn("[{}] [{}] Failed to reset cursor on subscription {} to time {}", clientAppId(),
                        destinationName, subName, timestamp, partitionException);
                throw new RestException(Status.PRECONDITION_FAILED, partitionException.getMessage());
            } else if (numPartException > 0) {
                log.warn("[{}][{}] partial errors for reset cursor on subscription {} to time {} - ", clientAppId(),
                        destinationName, subName, timestamp, partitionException);
            }

        } else {
            validateAdminOperationOnDestination(authoritative);
            log.info("[{}][{}] received reset cursor on subscription {} to time {}", clientAppId(), destinationName,
                    subName, timestamp);
            PersistentTopic topic = (PersistentTopic) getTopicReference(destinationName);
            if (topic == null) {
                throw new RestException(Status.NOT_FOUND, "Topic not found");
            }
            try {
                PersistentSubscription sub = topic.getSubscription(subName);
                checkNotNull(sub);
                sub.resetCursor(timestamp).get();
                log.info("[{}][{}] reset cursor on subscription {} to time {}", clientAppId(), destinationName, subName,
                        timestamp);
            } catch (Exception e) {
                Throwable t = e.getCause();
                log.warn("[{}] [{}] Failed to reset cursor on subscription {} to time {}", clientAppId(),
                        destinationName, subName, timestamp, e);
                if (e instanceof NullPointerException) {
                    throw new RestException(Status.NOT_FOUND, "Subscription not found");
                } else if (e instanceof NotAllowedException) {
                    throw new RestException(Status.METHOD_NOT_ALLOWED, e.getMessage());
                } else if (t instanceof SubscriptionInvalidCursorPosition) {
                    throw new RestException(Status.PRECONDITION_FAILED,
                            "Unable to find position for timestamp specified -" + t.getMessage());
                } else {
                    throw new RestException(e);
                }
            }
        }
    }

    protected void internalCreateSubscription(String subscriptionName, MessageIdImpl messageId, boolean authoritative) {
        if (destinationName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        log.info("[{}][{}] Creating subscription {} at message id {}", clientAppId(), destinationName,
                subscriptionName, messageId);

        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(destinationName, authoritative);

        try {
            if (partitionMetadata.partitions > 0) {
                // Create the subscription on each partition
                List<CompletableFuture<Void>> futures = Lists.newArrayList();
                PulsarAdmin admin = pulsar().getAdminClient();

                for (int i = 0; i < partitionMetadata.partitions; i++) {
                    futures.add(admin.persistentTopics().createSubscriptionAsync(
                            destinationName.getPartition(i).toString(),
                            subscriptionName, messageId));
                }

                FutureUtil.waitForAll(futures).join();
            } else {
                validateAdminOperationOnDestination(authoritative);

                PersistentTopic topic = (PersistentTopic) getOrCreateTopic(destinationName);

                if (topic.getSubscriptions().containsKey(subscriptionName)) {
                    throw new RestException(Status.CONFLICT, "Subscription already exists for topic");
                }

                PersistentSubscription subscription = (PersistentSubscription) topic
                        .createSubscription(subscriptionName).get();
                subscription.resetCursor(PositionImpl.get(messageId.getLedgerId(), messageId.getEntryId())).get();
                log.info("[{}][{}] Successfully created subscription {} at message id {}", clientAppId(),
                        destinationName, subscriptionName, messageId);
            }
        } catch (Exception e) {
            Throwable t = e.getCause();
            log.warn("[{}] [{}] Failed to create subscription {} at message id {}", clientAppId(),
                    destinationName, subscriptionName, messageId, e);
            if (t instanceof SubscriptionInvalidCursorPosition) {
                throw new RestException(Status.PRECONDITION_FAILED,
                        "Unable to find position for position specified: " + t.getMessage());
            } else {
                throw new RestException(e);
            }
        }
    }

    protected void internalResetCursorOnPosition(String subName, boolean authoritative, MessageIdImpl messageId) {
        if (destinationName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        log.info("[{}][{}] received reset cursor on subscription {} to position {}", clientAppId(), destinationName,
                subName, messageId);

        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(destinationName, authoritative);

        if (partitionMetadata.partitions > 0) {
            log.warn("[{}] Not supported operation on partitioned-topic {} {}", clientAppId(), destinationName,
                    subName);
            throw new RestException(Status.METHOD_NOT_ALLOWED,
                    "Reset-cursor at position is not allowed for partitioned-topic");
        } else {
            validateAdminOperationOnDestination(authoritative);
            PersistentTopic topic = (PersistentTopic) getTopicReference(destinationName);
            if (topic == null) {
                throw new RestException(Status.NOT_FOUND, "Topic not found");
            }
            try {
                PersistentSubscription sub = topic.getSubscription(subName);
                checkNotNull(sub);
                sub.resetCursor(PositionImpl.get(messageId.getLedgerId(), messageId.getEntryId())).get();
                log.info("[{}][{}] successfully reset cursor on subscription {} to position {}", clientAppId(),
                        destinationName, subName, messageId);
            } catch (Exception e) {
                Throwable t = e.getCause();
                log.warn("[{}] [{}] Failed to reset cursor on subscription {} to position {}", clientAppId(),
                        destinationName, subName, messageId, e);
                if (e instanceof NullPointerException) {
                    throw new RestException(Status.NOT_FOUND, "Subscription not found");
                } else if (t instanceof SubscriptionInvalidCursorPosition) {
                    throw new RestException(Status.PRECONDITION_FAILED,
                            "Unable to find position for position specified: " + t.getMessage());
                } else {
                    throw new RestException(e);
                }
            }
        }
    }

    protected Response internalPeekNthMessage(String subName, int messagePosition, boolean authoritative) {
        if (destinationName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(destinationName, authoritative);
        if (partitionMetadata.partitions > 0) {
            throw new RestException(Status.METHOD_NOT_ALLOWED, "Peek messages on a partitioned topic is not allowed");
        }
        validateAdminOperationOnDestination(authoritative);
        if (!(getTopicReference(destinationName) instanceof PersistentTopic)) {
            log.error("[{}] Not supported operation of non-persistent topic {} {}", clientAppId(), destinationName,
                    subName);
            throw new RestException(Status.METHOD_NOT_ALLOWED,
                    "Skip messages on a non-persistent topic is not allowed");
        }
        PersistentTopic topic = (PersistentTopic) getTopicReference(destinationName);
        PersistentReplicator repl = null;
        PersistentSubscription sub = null;
        Entry entry = null;
        if (subName.startsWith(topic.replicatorPrefix)) {
            repl = getReplicatorReference(subName, topic);
        } else {
            sub = (PersistentSubscription) getSubscriptionReference(subName, topic);
        }
        try {
            if (subName.startsWith(topic.replicatorPrefix)) {
                entry = repl.peekNthMessage(messagePosition).get();
            } else {
                entry = sub.peekNthMessage(messagePosition).get();
            }
            checkNotNull(entry);
            PositionImpl pos = (PositionImpl) entry.getPosition();
            ByteBuf metadataAndPayload = entry.getDataBuffer();

            // moves the readerIndex to the payload
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

            // Decode if needed
            CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(metadata.getCompression());
            ByteBuf uncompressedPayload = codec.decode(metadataAndPayload, metadata.getUncompressedSize());

            // Copy into a heap buffer for output stream compatibility
            ByteBuf data = PooledByteBufAllocator.DEFAULT.heapBuffer(uncompressedPayload.readableBytes(),
                    uncompressedPayload.readableBytes());
            data.writeBytes(uncompressedPayload);
            uncompressedPayload.release();

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream output) throws IOException, WebApplicationException {
                    output.write(data.array(), data.arrayOffset(), data.readableBytes());
                    data.release();
                }
            };

            return responseBuilder.entity(stream).build();
        } catch (NullPointerException npe) {
            throw new RestException(Status.NOT_FOUND, "Message not found");
        } catch (Exception exception) {
            log.error("[{}] Failed to get message at position {} from {} {}", clientAppId(), messagePosition,
                    destinationName, subName, exception);
            throw new RestException(exception);
        } finally {
            if (entry != null) {
                entry.release();
            }
        }
    }

    protected PersistentOfflineTopicStats internalGetBacklog(boolean authoritative) {
        if (destinationName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        // Validate that namespace exists, throw 404 if it doesn't exist
        // note that we do not want to load the topic and hence skip validateAdminOperationOnDestination()
        try {
            policiesCache().get(path(POLICIES, namespaceName.toString()));
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to get topic backlog {}: Namespace does not exist", clientAppId(), namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to get topic backlog {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }

        PersistentOfflineTopicStats offlineTopicStats = null;
        try {

            offlineTopicStats = pulsar().getBrokerService().getOfflineTopicStat(destinationName);
            if (offlineTopicStats != null) {
                // offline topic stat has a cost - so use cached value until TTL
                long elapsedMs = System.currentTimeMillis() - offlineTopicStats.statGeneratedAt.getTime();
                if (TimeUnit.MINUTES.convert(elapsedMs, TimeUnit.MILLISECONDS) < OFFLINE_TOPIC_STAT_TTL_MINS) {
                    return offlineTopicStats;
                }
            }
            final ManagedLedgerConfig config = pulsar().getBrokerService().getManagedLedgerConfig(destinationName)
                    .get();
            ManagedLedgerOfflineBacklog offlineTopicBacklog = new ManagedLedgerOfflineBacklog(config.getDigestType(),
                    config.getPassword(), pulsar().getAdvertisedAddress(), false);
            offlineTopicStats = offlineTopicBacklog.estimateUnloadedTopicBacklog(
                    (ManagedLedgerFactoryImpl) pulsar().getManagedLedgerFactory(), destinationName);
            pulsar().getBrokerService().cacheOfflineTopicStats(destinationName, offlineTopicStats);
        } catch (Exception exception) {
            throw new RestException(exception);
        }
        return offlineTopicStats;
    }

    protected MessageId internalTerminate(boolean authoritative) {
        if (destinationName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(destinationName, authoritative);
        if (partitionMetadata.partitions > 0) {
            throw new RestException(Status.METHOD_NOT_ALLOWED, "Termination of a partitioned topic is not allowed");
        }
        validateAdminOperationOnDestination(authoritative);
        Topic topic = getTopicReference(destinationName);
        try {
            return ((PersistentTopic) topic).terminate().get();
        } catch (Exception exception) {
            log.error("[{}] Failed to terminated topic {}", clientAppId(), destinationName, exception);
            throw new RestException(exception);
        }
    }

    protected void internalExpireMessages(String subName, int expireTimeInSeconds, boolean authoritative) {
        if (destinationName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(destinationName, authoritative);
        if (partitionMetadata.partitions > 0) {
            // expire messages for each partition destination
            try {
                for (int i = 0; i < partitionMetadata.partitions; i++) {
                    pulsar().getAdminClient().persistentTopics()
                            .expireMessages(destinationName.getPartition(i).toString(), subName, expireTimeInSeconds);
                }
            } catch (Exception e) {
                throw new RestException(e);
            }
        } else {
            // validate ownership and redirect if current broker is not owner
            validateAdminOperationOnDestination(authoritative);
            if (!(getTopicReference(destinationName) instanceof PersistentTopic)) {
                log.error("[{}] Not supported operation of non-persistent topic {} {}", clientAppId(), destinationName,
                        subName);
                throw new RestException(Status.METHOD_NOT_ALLOWED,
                        "Expire messages on a non-persistent topic is not allowed");
            }
            PersistentTopic topic = (PersistentTopic) getTopicReference(destinationName);
            try {
                if (subName.startsWith(topic.replicatorPrefix)) {
                    String remoteCluster = PersistentReplicator.getRemoteCluster(subName);
                    PersistentReplicator repl = (PersistentReplicator) topic.getPersistentReplicator(remoteCluster);
                    checkNotNull(repl);
                    repl.expireMessages(expireTimeInSeconds);
                } else {
                    PersistentSubscription sub = topic.getSubscription(subName);
                    checkNotNull(sub);
                    sub.expireMessages(expireTimeInSeconds);
                }
                log.info("[{}] Message expire started up to {} on {} {}", clientAppId(), expireTimeInSeconds,
                        destinationName, subName);
            } catch (NullPointerException npe) {
                throw new RestException(Status.NOT_FOUND, "Subscription not found");
            } catch (Exception exception) {
                log.error("[{}] Failed to expire messages up to {} on {} with subscription {} {}", clientAppId(),
                        expireTimeInSeconds, destinationName, subName, exception);
                throw new RestException(exception);
            }
        }
    }

    public static CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(PulsarService pulsar,
            String clientAppId, AuthenticationDataSource authenticationData, DestinationName dn) {
        CompletableFuture<PartitionedTopicMetadata> metadataFuture = new CompletableFuture<>();
        try {
            // (1) authorize client
            try {
                checkAuthorization(pulsar, dn, clientAppId, authenticationData);
            } catch (RestException e) {
                try {
                    validateAdminAccessOnProperty(pulsar, clientAppId, dn.getProperty());
                } catch (RestException authException) {
                    log.warn("Failed to authorize {} on cluster {}", clientAppId, dn.toString());
                    throw new PulsarClientException(String.format("Authorization failed %s on topic %s with error %s",
                            clientAppId, dn.toString(), authException.getMessage()));
                }
            } catch (Exception ex) {
                // throw without wrapping to PulsarClientException that considers: unknown error marked as internal
                // server error
                log.warn("Failed to authorize {} on cluster {} with unexpected exception {}", clientAppId,
                        dn.toString(), ex.getMessage(), ex);
                throw ex;
            }

            String path = path(PARTITIONED_TOPIC_PATH_ZNODE, dn.getNamespace(),
                    "persistent", dn.getEncodedLocalName());

            // validates global-namespace contains local/peer cluster: if peer/local cluster present then lookup can
            // serve/redirect request else fail partitioned-metadata-request so, client fails while creating
            // producer/consumer
            checkLocalOrGetPeerReplicationCluster(pulsar, dn.getNamespaceObject())
                    .thenCompose(res -> fetchPartitionedTopicMetadataAsync(pulsar, path)).thenAccept(metadata -> {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Total number of partitions for topic {} is {}", clientAppId, dn,
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
     * Get the Topic object reference from the Pulsar broker
     */
    private Topic getTopicReference(DestinationName dn) {
        try {
            Topic topic = pulsar().getBrokerService().getTopicReference(dn.toString());
            checkNotNull(topic);
            return topic;
        } catch (Exception e) {
            throw new RestException(Status.NOT_FOUND, "Topic not found");
        }
    }

    private Topic getOrCreateTopic(DestinationName dn) {
        try {
            return pulsar().getBrokerService().getTopic(dn.toString()).get();
        } catch (InterruptedException | ExecutionException e) {
           throw new RestException(e);
        }
    }

    /**
     * Get the Subscription object reference from the Topic reference
     */
    private Subscription getSubscriptionReference(String subName, PersistentTopic topic) {
        try {
            Subscription sub = topic.getSubscription(subName);
            return checkNotNull(sub);
        } catch (Exception e) {
            throw new RestException(Status.NOT_FOUND, "Subscription not found");
        }
    }

    /**
     * Get the Replicator object reference from the Topic reference
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

    private CompletableFuture<Void> updatePartitionedTopic(DestinationName dn, int numPartitions) {
        String path = path(PARTITIONED_TOPIC_PATH_ZNODE, dn.getProperty(), dn.getCluster(), dn.getNamespacePortion(),
                domain(), dn.getEncodedLocalName());

        CompletableFuture<Void> updatePartition = new CompletableFuture<>();
        createSubscriptions(dn, numPartitions).thenAccept(res -> {
            try {
                byte[] data = jsonMapper().writeValueAsBytes(new PartitionedTopicMetadata(numPartitions));
                globalZk().setData(path, data, -1, (rc, path1, ctx, stat) -> {
                    if (rc == KeeperException.Code.OK.intValue()) {
                        updatePartition.complete(null);
                    } else {
                        updatePartition.completeExceptionally(KeeperException.create(KeeperException.Code.get(rc),
                                "failed to create update partitions"));
                    }
                }, null);
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
     * It creates subscriptions for new partitions of existing partitioned-topics
     *
     * @param dn
     *            : topic-name: persistent://prop/cluster/ns/topic
     * @param numPartitions
     *            : number partitions for the topics
     */
    private CompletableFuture<Void> createSubscriptions(DestinationName dn, int numPartitions) {
        String path = path(PARTITIONED_TOPIC_PATH_ZNODE, dn.getProperty(), dn.getCluster(), dn.getNamespacePortion(),
                domain(), dn.getEncodedLocalName());
        CompletableFuture<Void> result = new CompletableFuture<>();
        fetchPartitionedTopicMetadataAsync(pulsar(), path).thenAccept(partitionMetadata -> {
            if (partitionMetadata.partitions <= 1) {
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

            admin.persistentTopics().getStatsAsync(dn.getPartition(0).toString()).thenAccept(stats -> {
                stats.subscriptions.keySet().forEach(subscription -> {
                    List<CompletableFuture<Void>> subscriptionFutures = new ArrayList<>();
                    for (int i = partitionMetadata.partitions; i < numPartitions; i++) {
                        final String topicName = dn.getPartition(i).toString();

                        subscriptionFutures.add(admin.persistentTopics().createSubscriptionAsync(topicName,
                                subscription, MessageId.latest));
                    }

                    FutureUtil.waitForAll(subscriptionFutures).thenRun(() -> {
                        log.info("[{}] Successfully created new partitions {}", clientAppId(), dn);
                        result.complete(null);
                    }).exceptionally(ex -> {
                        log.warn("[{}] Failed to create subscriptions on new partitions for {}", clientAppId(), dn, ex);
                        result.completeExceptionally(ex);
                        return null;
                    });
                });
            }).exceptionally(ex -> {
                if (ex.getCause() instanceof PulsarAdminException.NotFoundException) {
                    // The first partition doesn't exist, so there are currently to subscriptions to recreate
                    result.complete(null);
                } else {
                    log.warn("[{}] Failed to get list of subscriptions of {}", clientAppId(), dn.getPartition(0), ex);
                    result.completeExceptionally(ex);
                }
                return null;
            });
        }).exceptionally(ex -> {
            log.warn("[{}] Failed to get partition metadata for {}", clientAppId(), dn.toString());
            result.completeExceptionally(ex);
            return null;
        });
        return result;
    }

    protected void unloadTopic(DestinationName destination, boolean authoritative) {
        validateSuperUserAccess();
        validateDestinationOwnership(destination, authoritative);
        try {
            Topic topic = getTopicReference(destination);
            topic.close().get();
            log.info("[{}] Successfully unloaded topic {}", clientAppId(), destination);
        } catch (NullPointerException e) {
            log.error("[{}] topic {} not found", clientAppId(), destination);
            throw new RestException(Status.NOT_FOUND, "Topic does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to unload topic {}, {}", clientAppId(), destination, e.getCause().getMessage(), e);
            throw new RestException(e.getCause());
        }
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
                    "Client lib is not compatible to access partitioned metadata: version in user-agent is not present");
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
}
