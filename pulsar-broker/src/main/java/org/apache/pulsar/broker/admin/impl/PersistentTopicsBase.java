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

import com.github.zafarkhaja.semver.Version;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerOfflineBacklog;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.admin.ZkAdminPaths;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
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
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyValue;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.AuthPolicies;
import org.apache.pulsar.common.policies.data.PartitionedTopicInternalStats;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class PersistentTopicsBase extends AdminResource {
    private static final Logger log = LoggerFactory.getLogger(PersistentTopicsBase.class);

    protected static final int PARTITIONED_TOPIC_WAIT_SYNC_TIME_MS = 1000;
    private static final int OFFLINE_TOPIC_STAT_TTL_MINS = 10;
    private static final String DEPRECATED_CLIENT_VERSION_PREFIX = "Pulsar-CPP-v";
    private static final Version LEAST_SUPPORTED_CLIENT_VERSION_PREFIX = Version.forIntegers(1, 21);

    protected List<String> internalGetList() {
        validateAdminAccessForTenant(namespaceName.getTenant());

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

        List<String> topics = Lists.newArrayList();

        try {
            String path = String.format("/managed-ledgers/%s/%s", namespaceName.toString(), domain());
            for (String topic : managedLedgerListCache().get(path)) {
                if (domain().equals(TopicDomain.persistent.toString())) {
                    topics.add(TopicName.get(domain(), namespaceName, decode(topic)).toString());
                }
            }
        } catch (KeeperException.NoNodeException e) {
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
        return getPartitionedTopicList(TopicDomain.getEnum(domain()));
    }

    protected Map<String, Set<AuthAction>> internalGetPermissionsOnTopic() {
        // This operation should be reading from zookeeper and it should be allowed without having admin privileges
        validateAdminAccessForTenant(namespaceName.getTenant());

        String topicUri = topicName.toString();

        try {
            Policies policies = policiesCache().get(path(POLICIES, namespaceName.toString()))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Namespace does not exist"));

            Map<String, Set<AuthAction>> permissions = Maps.newTreeMap();
            AuthPolicies auth = policies.auth_policies;

            // First add namespace level permissions
            for (String role : auth.namespace_auth.keySet()) {
                permissions.put(role, auth.namespace_auth.get(role));
            }

            // Then add topic level permissions
            if (auth.destination_auth.containsKey(topicUri)) {
                for (Map.Entry<String, Set<AuthAction>> entry : auth.destination_auth.get(topicUri).entrySet()) {
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

    protected void validateAdminAccessForSubscriber(String subscriptionName, boolean authoritative) {
        validateTopicOwnership(topicName, authoritative);
        try {
            validateAdminAccessForTenant(topicName.getTenant());
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] failed to validate admin access for {}", topicName, clientAppId());
            }
            validateAdminAccessForSubscriber(subscriptionName);
        }
    }

    private void validateAdminAccessForSubscriber(String subscriptionName) {
        try {
            if (!pulsar().getBrokerService().getAuthorizationService().canConsume(topicName, clientAppId(),
                    clientAuthData(), subscriptionName)) {
                log.warn("[{}} Subscriber {} is not authorized to access api", topicName, clientAppId());
                throw new RestException(Status.UNAUTHORIZED,
                        String.format("Subscriber %s is not authorized to access this operation", clientAppId()));
            }
        } catch (RestException re) {
            throw re;
        } catch (Exception e) {
            // unknown error marked as internal server error
            log.warn("Unexpected error while authorizing request. topic={}, role={}. Error: {}", topicName,
                    clientAppId(), e.getMessage(), e);
            throw new RestException(e);
        }
    }

    protected void internalGrantPermissionsOnTopic(String role, Set<AuthAction> actions) {
        // This operation should be reading from zookeeper and it should be allowed without having admin privileges
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        String topicUri = topicName.toString();

        try {
            Stat nodeStat = new Stat();
            byte[] content = globalZk().getData(path(POLICIES, namespaceName.toString()), null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);

            if (!policies.auth_policies.destination_auth.containsKey(topicUri)) {
                policies.auth_policies.destination_auth.put(topicUri, new TreeMap<String, Set<AuthAction>>());
            }

            policies.auth_policies.destination_auth.get(topicUri).put(role, actions);

            // Write the new policies to zookeeper
            globalZk().setData(path(POLICIES, namespaceName.toString()), jsonMapper().writeValueAsBytes(policies),
                    nodeStat.getVersion());

            // invalidate the local cache to force update
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));

            log.info("[{}] Successfully granted access for role {}: {} - topic {}", clientAppId(), role, actions,
                    topicUri);

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to grant permissions on topic {}: Namespace does not exist", clientAppId(),
                    topicUri);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to grant permissions on topic {}: concurrent modification", clientAppId(),
                    topicUri);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        }
        catch (Exception e) {
            log.error("[{}] Failed to grant permissions for topic {}", clientAppId(), topicUri, e);
            throw new RestException(e);
        }
    }

    protected void internalDeleteTopicForcefully(boolean authoritative) {
        validateAdminOperationOnTopic(authoritative);
        Topic topic = getTopicReference(topicName);
        try {
            topic.deleteForcefully().get();
        } catch (Exception e) {
            log.error("[{}] Failed to delete topic forcefully {}", clientAppId(), topicName, e);
            throw new RestException(e);
        }
    }

    protected void internalRevokePermissionsOnTopic(String role) {
        // This operation should be reading from zookeeper and it should be allowed without having admin privileges
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        String topicUri = topicName.toString();
        Stat nodeStat = new Stat();
        Policies policies;

        try {
            byte[] content = globalZk().getData(path(POLICIES, namespaceName.toString()), null, nodeStat);
            policies = jsonMapper().readValue(content, Policies.class);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to revoke permissions on topic {}: Namespace does not exist", clientAppId(),
                    topicUri);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to revoke permissions on topic {}: concurrent modification", clientAppId(),
                topicUri);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        }
        catch (Exception e) {
            log.error("[{}] Failed to revoke permissions for topic {}", clientAppId(), topicUri, e);
            throw new RestException(e);
        }

        if (!policies.auth_policies.destination_auth.containsKey(topicUri)
                || !policies.auth_policies.destination_auth.get(topicUri).containsKey(role)) {
            log.warn("[{}] Failed to revoke permission from role {} on topic: Not set at topic level",
                    clientAppId(), role, topicUri);
            throw new RestException(Status.PRECONDITION_FAILED, "Permissions are not set at the topic level");
        }

        policies.auth_policies.destination_auth.get(topicUri).remove(role);

        try {
            // Write the new policies to zookeeper
            String namespacePath = path(POLICIES, namespaceName.toString());
            globalZk().setData(namespacePath, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());

            // invalidate the local cache to force update
            policiesCache().invalidate(namespacePath);
            globalZkCache().invalidate(namespacePath);

            log.info("[{}] Successfully revoke access for role {} - topic {}", clientAppId(), role,
                    topicUri);
        } catch (Exception e) {
            log.error("[{}] Failed to revoke permissions for topic {}", clientAppId(), topicUri, e);
            throw new RestException(e);
        }
    }

    protected void internalCreatePartitionedTopic(int numPartitions) {
        validateAdminAccessForTenant(topicName.getTenant());
        if (numPartitions <= 1) {
            throw new RestException(Status.NOT_ACCEPTABLE, "Number of partitions should be more than 1");
        }
        try {
            String path = ZkAdminPaths.partitionedTopicPath(topicName);
            byte[] data = jsonMapper().writeValueAsBytes(new PartitionedTopicMetadata(numPartitions));
            zkCreateOptimistic(path, data);
            // we wait for the data to be synced in all quorums and the observers
            Thread.sleep(PARTITIONED_TOPIC_WAIT_SYNC_TIME_MS);
            log.info("[{}] Successfully created partitioned topic {}", clientAppId(), topicName);
        } catch (KeeperException.NodeExistsException e) {
            log.warn("[{}] Failed to create already existing partitioned topic {}", clientAppId(), topicName);
            throw new RestException(Status.CONFLICT, "Partitioned topic already exists");
        } catch (KeeperException.BadVersionException e) {
                log.warn("[{}] Failed to create partitioned topic {}: concurrent modification", clientAppId(),
                        topicName);
                throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to create partitioned topic {}", clientAppId(), topicName, e);
            throw new RestException(e);
        }
    }

    protected void internalCreateNonPartitionedTopic(boolean authoritative) {
        validateAdminAccessForTenant(topicName.getTenant());

        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }

        validateTopicOwnership(topicName, authoritative);
    	try {
            Topic createdTopic = getOrCreateTopic(topicName);
            log.info("[{}] Successfully created non-partitioned topic {}", clientAppId(), createdTopic);
    	} catch (Exception e) {
    		log.error("[{}] Failed to create non-partitioned topic {}", clientAppId(), topicName, e);
    		throw new RestException(e);
    	}
    }

    /**
     * It updates number of partitions of an existing non-global partitioned topic. It requires partitioned-topic to
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
        validateAdminAccessForTenant(topicName.getTenant());

        if (topicName.isGlobal() && isNamespaceReplicated(topicName.getNamespaceObject())) {
            log.error("[{}] Update partitioned-topic is forbidden on global namespace {}", clientAppId(),
                    topicName);
            throw new RestException(Status.FORBIDDEN, "Update forbidden on global namespace");
        }
        if (numPartitions <= 1) {
            throw new RestException(Status.NOT_ACCEPTABLE, "Number of partitions should be more than 1");
        }
        try {
            updatePartitionedTopic(topicName, numPartitions).get();
        } catch (Exception e) {
            if (e.getCause() instanceof RestException) {
                throw (RestException) e.getCause();
            }
            log.error("[{}] Failed to update partitioned topic {}", clientAppId(), topicName, e);
            throw new RestException(e);
        }
    }

    protected PartitionedTopicMetadata internalGetPartitionedMetadata(boolean authoritative) {
        PartitionedTopicMetadata metadata = getPartitionedTopicMetadata(topicName, authoritative);
        if (metadata.partitions > 1) {
            validateClientVersion();
        }
        return metadata;
    }

    protected void internalDeletePartitionedTopic(boolean authoritative, boolean force) {
        validateAdminAccessForTenant(topicName.getTenant());
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(topicName, authoritative);
        int numPartitions = partitionMetadata.partitions;
        if (numPartitions > 0) {
            final CompletableFuture<Void> future = new CompletableFuture<>();
            final AtomicInteger count = new AtomicInteger(numPartitions);
            try {
                for (int i = 0; i < numPartitions; i++) {
                    TopicName topicNamePartition = topicName.getPartition(i);
                    pulsar().getAdminClient().topics().deleteAsync(topicNamePartition.toString(), force)
                            .whenComplete((r, ex) -> {
                                if (ex != null) {
                                    if (ex instanceof NotFoundException) {
                                        // if the sub-topic is not found, the client might not have called create
                                        // producer or it might have been deleted earlier, so we ignore the 404 error.
                                        // For all other exception, we fail the delete partition method even if a single
                                        // partition is failed to be deleted
                                        if (log.isDebugEnabled()) {
                                            log.debug("[{}] Partition not found: {}", clientAppId(), topicNamePartition);
                                        }
                                    } else {
                                        future.completeExceptionally(ex);
                                        log.error("[{}] Failed to delete partition {}", clientAppId(), topicNamePartition,
                                                ex);
                                        return;
                                    }
                                } else {
                                    log.info("[{}] Deleted partition {}", clientAppId(), topicNamePartition);
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
                topicName.getEncodedLocalName());
        try {
            globalZk().delete(path, -1);
            globalZkCache().invalidate(path);
            // we wait for the data to be synced in all quorums and the observers
            Thread.sleep(PARTITIONED_TOPIC_WAIT_SYNC_TIME_MS);
            log.info("[{}] Deleted partitioned topic {}", clientAppId(), topicName);
        } catch (KeeperException.NoNodeException nne) {
            throw new RestException(Status.NOT_FOUND, "Partitioned topic does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to delete partitioned topic {}: concurrent modification", clientAppId(),
                    topicName);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to delete partitioned topic {}", clientAppId(), topicName, e);
            throw new RestException(e);
        }
    }

    protected void internalUnloadTopic(boolean authoritative) {
        log.info("[{}] Unloading topic {}", clientAppId(), topicName);
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        unloadTopic(topicName, authoritative);
    }

    protected void internalDeleteTopic(boolean authoritative, boolean force) {
        if (force) {
            internalDeleteTopicForcefully(authoritative);
        } else {
            internalDeleteTopic(authoritative);
        }
    }

    protected void internalDeleteTopic(boolean authoritative) {
        validateAdminOperationOnTopic(authoritative);
        Topic topic = getTopicReference(topicName);

        // v2 topics have a global name so check if the topic is replicated.
        if (topic.isReplicated()) {
            // Delete is disallowed on global topic
            final List<String> clusters = topic.getReplicators().keys();
            log.error("[{}] Delete forbidden topic {} is replicated on clusters {}",
                    clientAppId(), topicName, clusters);
            throw new RestException(Status.FORBIDDEN, "Delete forbidden topic is replicated on clusters " + clusters);
        }

        try {
            topic.delete().get();
            log.info("[{}] Successfully removed topic {}", clientAppId(), topicName);
        } catch (Exception e) {
            Throwable t = e.getCause();
            log.error("[{}] Failed to get delete topic {}", clientAppId(), topicName, t);
            if (t instanceof TopicBusyException) {
                throw new RestException(Status.PRECONDITION_FAILED, "Topic has active producers/subscriptions");
            } else {
                throw new RestException(t);
            }
        }
    }

    protected List<String> internalGetSubscriptions(boolean authoritative) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }

        List<String> subscriptions = Lists.newArrayList();
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(topicName, authoritative);
        if (partitionMetadata.partitions > 0) {
            try {
                // get the subscriptions only from the 1st partition since all the other partitions will have the same
                // subscriptions
                subscriptions.addAll(pulsar().getAdminClient().topics()
                        .getSubscriptions(topicName.getPartition(0).toString()));
            } catch (PulsarAdminException e) {
                if (e.getStatusCode() == Status.NOT_FOUND.getStatusCode()) {
                    throw new RestException(Status.NOT_FOUND, "Internal topics have not been generated yet");
                } else {
                    throw new RestException(e);
                }
            } catch (Exception e) {
                throw new RestException(e);
            }
        } else {
            validateAdminOperationOnTopic(authoritative);
            Topic topic = getTopicReference(topicName);

            try {
                topic.getSubscriptions().forEach((subName, sub) -> subscriptions.add(subName));
            } catch (Exception e) {
                log.error("[{}] Failed to get list of subscriptions for {}", clientAppId(), topicName);
                throw new RestException(e);
            }
        }

        return subscriptions;
    }

    protected TopicStats internalGetStats(boolean authoritative) {
        validateAdminAndClientPermission();
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        validateTopicOwnership(topicName, authoritative);
        Topic topic = getTopicReference(topicName);
        return topic.getStats();
    }

    protected PersistentTopicInternalStats internalGetInternalStats(boolean authoritative) {
        validateAdminAndClientPermission();
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        validateTopicOwnership(topicName, authoritative);
        Topic topic = getTopicReference(topicName);
        return topic.getInternalStats();
    }

    protected void internalGetManagedLedgerInfo(AsyncResponse asyncResponse) {
        validateAdminAccessForTenant(topicName.getTenant());
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        String managedLedger = topicName.getPersistenceNamingEncoding();
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
            boolean perPartition) {
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(topicName, authoritative);
        if (partitionMetadata.partitions == 0) {
            throw new RestException(Status.NOT_FOUND, "Partitioned Topic not found");
        }
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicStats stats = new PartitionedTopicStats(partitionMetadata);

        List<CompletableFuture<TopicStats>> topicStatsFutureList = Lists.newArrayList();
        for (int i = 0; i < partitionMetadata.partitions; i++) {
            try {
                topicStatsFutureList
                        .add(pulsar().getAdminClient().topics().getStatsAsync((topicName.getPartition(i).toString())));
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
                    boolean zkPathExists = zkPathExists(path);
                    if (zkPathExists) {
                        stats.partitions.put(topicName.toString(), new TopicStats());
                    } else {
                        asyncResponse.resume(
                                new RestException(Status.NOT_FOUND, "Internal topics have not been generated yet"));
                        return null;
                    }
                } catch (KeeperException | InterruptedException e) {
                    asyncResponse.resume(new RestException(e));
                    return null;
                }
            }
            asyncResponse.resume(stats);
            return null;
        });
    }

    protected void internalGetPartitionedStatsInternal(AsyncResponse asyncResponse, boolean authoritative) {
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(topicName, authoritative);
        if (partitionMetadata.partitions == 0) {
            throw new RestException(Status.NOT_FOUND, "Partitioned Topic not found");
        }
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicInternalStats stats = new PartitionedTopicInternalStats(partitionMetadata);

        List<CompletableFuture<PersistentTopicInternalStats>> topicStatsFutureList = Lists.newArrayList();
        for (int i = 0; i < partitionMetadata.partitions; i++) {
            try {
                topicStatsFutureList.add(pulsar().getAdminClient().topics()
                        .getInternalStatsAsync((topicName.getPartition(i).toString())));
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
    }

    protected void internalDeleteSubscription(String subName, boolean authoritative) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(topicName, authoritative);
        if (partitionMetadata.partitions > 0) {
            try {
                for (int i = 0; i < partitionMetadata.partitions; i++) {
                    pulsar().getAdminClient().topics()
                            .deleteSubscription(topicName.getPartition(i).toString(), subName);
                }
            } catch (Exception e) {
                if (e instanceof NotFoundException) {
                    throw new RestException(Status.NOT_FOUND, "Subscription not found");
                } else if (e instanceof PreconditionFailedException) {
                    throw new RestException(Status.PRECONDITION_FAILED, "Subscription has active connected consumers");
                } else {
                    log.error("[{}] Failed to delete subscription {} {}", clientAppId(), topicName, subName, e);
                    throw new RestException(e);
                }
            }
        } else {
            validateAdminAccessForSubscriber(subName, authoritative);
            Topic topic = getTopicReference(topicName);
            try {
                Subscription sub = topic.getSubscription(subName);
                checkNotNull(sub);
                sub.delete().get();
                log.info("[{}][{}] Deleted subscription {}", clientAppId(), topicName, subName);
            } catch (Exception e) {
                Throwable t = e.getCause();
                if (e instanceof NullPointerException) {
                    throw new RestException(Status.NOT_FOUND, "Subscription not found");
                } else if (t instanceof SubscriptionBusyException) {
                    throw new RestException(Status.PRECONDITION_FAILED, "Subscription has active connected consumers");
                } else {
                    log.error("[{}] Failed to delete subscription {} {}", clientAppId(), topicName, subName, e);
                    throw new RestException(t);
                }
            }
        }
    }

    protected void internalSkipAllMessages(String subName, boolean authoritative) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(topicName, authoritative);
        if (partitionMetadata.partitions > 0) {
            try {
                for (int i = 0; i < partitionMetadata.partitions; i++) {
                    pulsar().getAdminClient().topics()
                            .skipAllMessages(topicName.getPartition(i).toString(), subName);
                }
            } catch (Exception e) {
                throw new RestException(e);
            }
        } else {
            validateAdminAccessForSubscriber(subName, authoritative);
            PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
            try {
                if (subName.startsWith(topic.getReplicatorPrefix())) {
                    String remoteCluster = PersistentReplicator.getRemoteCluster(subName);
                    PersistentReplicator repl = (PersistentReplicator) topic.getPersistentReplicator(remoteCluster);
                    checkNotNull(repl);
                    repl.clearBacklog().get();
                } else {
                    PersistentSubscription sub = topic.getSubscription(subName);
                    checkNotNull(sub);
                    sub.clearBacklog().get(pulsar().getConfiguration().getZooKeeperOperationTimeoutSeconds(),
                            TimeUnit.SECONDS);
                }
                log.info("[{}] Cleared backlog on {} {}", clientAppId(), topicName, subName);
            } catch (NullPointerException npe) {
                throw new RestException(Status.NOT_FOUND, "Subscription not found");
            } catch (Exception exception) {
                log.error("[{}] Failed to skip all messages {} {}", clientAppId(), topicName, subName, exception);
                throw new RestException(exception);
            }
        }
    }

    protected void internalSkipMessages(String subName, int numMessages, boolean authoritative) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(topicName, authoritative);
        if (partitionMetadata.partitions > 0) {
            throw new RestException(Status.METHOD_NOT_ALLOWED, "Skip messages on a partitioned topic is not allowed");
        }
        validateAdminAccessForSubscriber(subName, authoritative);
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

    protected void internalExpireMessagesForAllSubscriptions(int expireTimeInSeconds, boolean authoritative) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(topicName, authoritative);
        if (partitionMetadata.partitions > 0) {
            try {
                // expire messages for each partition topic
                for (int i = 0; i < partitionMetadata.partitions; i++) {
                    pulsar().getAdminClient().topics().expireMessagesForAllSubscriptions(
                            topicName.getPartition(i).toString(), expireTimeInSeconds);
                }
            } catch (Exception e) {
                log.error("[{}] Failed to expire messages up to {} on {} {}", clientAppId(), expireTimeInSeconds,
                        topicName, e);
                throw new RestException(e);
            }
        } else {
            // validate ownership and redirect if current broker is not owner
            validateAdminOperationOnTopic(authoritative);
            PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
            topic.getReplicators().forEach((subName, replicator) -> {
                internalExpireMessages(subName, expireTimeInSeconds, authoritative);
            });
            topic.getSubscriptions().forEach((subName, subscriber) -> {
                internalExpireMessages(subName, expireTimeInSeconds, authoritative);
            });
        }
    }

    protected void internalResetCursor(String subName, long timestamp, boolean authoritative) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(topicName, authoritative);

        if (partitionMetadata.partitions > 0) {
            int numParts = partitionMetadata.partitions;
            int numPartException = 0;
            Exception partitionException = null;
            try {
                for (int i = 0; i < numParts; i++) {
                    pulsar().getAdminClient().topics().resetCursor(topicName.getPartition(i).toString(),
                            subName, timestamp);
                }
            } catch (PreconditionFailedException pfe) {
                // throw the last exception if all partitions get this error
                // any other exception on partition is reported back to user
                ++numPartException;
                partitionException = pfe;
            } catch (Exception e) {
                log.warn("[{}] [{}] Failed to reset cursor on subscription {} to time {}", clientAppId(),
                        topicName, subName, timestamp, e);
                throw new RestException(e);
            }
            // report an error to user if unable to reset for all partitions
            if (numPartException == numParts) {
                log.warn("[{}] [{}] Failed to reset cursor on subscription {} to time {}", clientAppId(),
                        topicName, subName, timestamp, partitionException);
                throw new RestException(Status.PRECONDITION_FAILED, partitionException.getMessage());
            } else if (numPartException > 0) {
                log.warn("[{}][{}] partial errors for reset cursor on subscription {} to time {} - ", clientAppId(),
                        topicName, subName, timestamp, partitionException);
            }

        } else {
            validateAdminAccessForSubscriber(subName, authoritative);
            log.info("[{}][{}] received reset cursor on subscription {} to time {}", clientAppId(), topicName,
                    subName, timestamp);
            PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
            if (topic == null) {
                throw new RestException(Status.NOT_FOUND, "Topic not found");
            }
            try {
                PersistentSubscription sub = topic.getSubscription(subName);
                checkNotNull(sub);
                sub.resetCursor(timestamp).get();
                log.info("[{}][{}] reset cursor on subscription {} to time {}", clientAppId(), topicName, subName,
                        timestamp);
            } catch (Exception e) {
                Throwable t = e.getCause();
                log.warn("[{}] [{}] Failed to reset cursor on subscription {} to time {}", clientAppId(),
                        topicName, subName, timestamp, e);
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

    protected void internalCreateSubscription(String subscriptionName, MessageIdImpl messageId, boolean authoritative, boolean replicated) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        messageId = messageId == null ? (MessageIdImpl) MessageId.earliest : messageId;
        log.info("[{}][{}] Creating subscription {} at message id {}", clientAppId(), topicName,
                subscriptionName, messageId);

        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(topicName, authoritative);

        try {
            if (partitionMetadata.partitions > 0) {
                // Create the subscription on each partition
                PulsarAdmin admin = pulsar().getAdminClient();

                CountDownLatch latch = new CountDownLatch(partitionMetadata.partitions);
                AtomicReference<Throwable> exception = new AtomicReference<>();
                AtomicInteger failureCount = new AtomicInteger(0);

                for (int i = 0; i < partitionMetadata.partitions; i++) {
                    admin.topics()
                            .createSubscriptionAsync(topicName.getPartition(i).toString(), subscriptionName, messageId)
                            .handle((result, ex) -> {
                                if (ex != null) {
                                    int c = failureCount.incrementAndGet();
                                    // fail the operation on unknown exception or if all the partitioned failed due to
                                    // subscription-already-exist
                                    if (c == partitionMetadata.partitions
                                            || !(ex instanceof PulsarAdminException.ConflictException)) {
                                        exception.set(ex);
                                    }
                                }
                                latch.countDown();
                                return null;
                            });
                }

                latch.await();
                if (exception.get() != null) {
                    throw exception.get();
                }
            } else {
                validateAdminAccessForSubscriber(subscriptionName, authoritative);

                PersistentTopic topic = (PersistentTopic) getOrCreateTopic(topicName);

                if (topic.getSubscriptions().containsKey(subscriptionName)) {
                    throw new RestException(Status.CONFLICT, "Subscription already exists for topic");
                }

                PersistentSubscription subscription = (PersistentSubscription) topic
                        .createSubscription(subscriptionName, InitialPosition.Latest, replicated).get();
                // Mark the cursor as "inactive" as it was created without a real consumer connected
                subscription.deactivateCursor();
                subscription.resetCursor(PositionImpl.get(messageId.getLedgerId(), messageId.getEntryId())).get();
                log.info("[{}][{}] Successfully created subscription {} at message id {}", clientAppId(), topicName,
                        subscriptionName, messageId);
            }
        } catch (Throwable e) {
            Throwable t = e.getCause();
            log.warn("[{}] [{}] Failed to create subscription {} at message id {}", clientAppId(),
                    topicName, subscriptionName, messageId, e);
            if (t instanceof SubscriptionInvalidCursorPosition) {
                throw new RestException(Status.PRECONDITION_FAILED,
                        "Unable to find position for position specified: " + t.getMessage());
            } else {
                throw new RestException(e);
            }
        }
    }

    protected void internalResetCursorOnPosition(String subName, boolean authoritative, MessageIdImpl messageId) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        log.info("[{}][{}] received reset cursor on subscription {} to position {}", clientAppId(), topicName,
                subName, messageId);

        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(topicName, authoritative);

        if (partitionMetadata.partitions > 0) {
            log.warn("[{}] Not supported operation on partitioned-topic {} {}", clientAppId(), topicName,
                    subName);
            throw new RestException(Status.METHOD_NOT_ALLOWED,
                    "Reset-cursor at position is not allowed for partitioned-topic");
        } else {
            validateAdminAccessForSubscriber(subName, authoritative);
            PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
            if (topic == null) {
                throw new RestException(Status.NOT_FOUND, "Topic not found");
            }
            try {
                PersistentSubscription sub = topic.getSubscription(subName);
                checkNotNull(sub);
                sub.resetCursor(PositionImpl.get(messageId.getLedgerId(), messageId.getEntryId())).get();
                log.info("[{}][{}] successfully reset cursor on subscription {} to position {}", clientAppId(),
                        topicName, subName, messageId);
            } catch (Exception e) {
                Throwable t = e.getCause();
                log.warn("[{}] [{}] Failed to reset cursor on subscription {} to position {}", clientAppId(),
                        topicName, subName, messageId, e);
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
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(topicName, authoritative);
        if (partitionMetadata.partitions > 0) {
            throw new RestException(Status.METHOD_NOT_ALLOWED, "Peek messages on a partitioned topic is not allowed");
        }
        validateAdminAccessForSubscriber(subName, authoritative);
        if (!(getTopicReference(topicName) instanceof PersistentTopic)) {
            log.error("[{}] Not supported operation of non-persistent topic {} {}", clientAppId(), topicName,
                    subName);
            throw new RestException(Status.METHOD_NOT_ALLOWED,
                    "Skip messages on a non-persistent topic is not allowed");
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
            ByteBuf data = PulsarByteBufAllocator.DEFAULT.heapBuffer(uncompressedPayload.readableBytes(),
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
                    topicName, subName, exception);
            throw new RestException(exception);
        } finally {
            if (entry != null) {
                entry.release();
            }
        }
    }

    protected PersistentOfflineTopicStats internalGetBacklog(boolean authoritative) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        // Validate that namespace exists, throw 404 if it doesn't exist
        // note that we do not want to load the topic and hence skip validateAdminOperationOnTopic()
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

    protected MessageId internalTerminate(boolean authoritative) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(topicName, authoritative);
        if (partitionMetadata.partitions > 0) {
            throw new RestException(Status.METHOD_NOT_ALLOWED, "Termination of a partitioned topic is not allowed");
        }
        validateAdminOperationOnTopic(authoritative);
        Topic topic = getTopicReference(topicName);
        try {
            return ((PersistentTopic) topic).terminate().get();
        } catch (Exception exception) {
            log.error("[{}] Failed to terminated topic {}", clientAppId(), topicName, exception);
            throw new RestException(exception);
        }
    }

    protected void internalExpireMessages(String subName, int expireTimeInSeconds, boolean authoritative) {
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(topicName, authoritative);
        if (partitionMetadata.partitions > 0) {
            // expire messages for each partition topic
            try {
                for (int i = 0; i < partitionMetadata.partitions; i++) {
                    pulsar().getAdminClient().topics()
                            .expireMessages(topicName.getPartition(i).toString(), subName, expireTimeInSeconds);
                }
            } catch (Exception e) {
                throw new RestException(e);
            }
        } else {
            // validate ownership and redirect if current broker is not owner
            validateAdminAccessForSubscriber(subName, authoritative);
            if (!(getTopicReference(topicName) instanceof PersistentTopic)) {
                log.error("[{}] Not supported operation of non-persistent topic {} {}", clientAppId(), topicName,
                        subName);
                throw new RestException(Status.METHOD_NOT_ALLOWED,
                        "Expire messages on a non-persistent topic is not allowed");
            }
            PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
            try {
                if (subName.startsWith(topic.getReplicatorPrefix())) {
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
                        topicName, subName);
            } catch (NullPointerException npe) {
                throw new RestException(Status.NOT_FOUND, "Subscription not found");
            } catch (Exception exception) {
                log.error("[{}] Failed to expire messages up to {} on {} with subscription {} {}", clientAppId(),
                        expireTimeInSeconds, topicName, subName, exception);
                throw new RestException(exception);
            }
        }
    }

    protected void internalTriggerCompaction(boolean authoritative) {
        validateAdminOperationOnTopic(authoritative);

        PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
        try {
            topic.triggerCompaction();
        } catch (AlreadyRunningException e) {
            throw new RestException(Status.CONFLICT, e.getMessage());
        } catch (Exception e) {
            throw new RestException(e);
        }
    }

    protected LongRunningProcessStatus internalCompactionStatus(boolean authoritative) {
        validateAdminOperationOnTopic(authoritative);
        PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
        return topic.compactionStatus();
    }

    protected void internalTriggerOffload(boolean authoritative, MessageIdImpl messageId) {
        validateAdminOperationOnTopic(authoritative);
        PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
        try {
            topic.triggerOffload(messageId);
        } catch (AlreadyRunningException e) {
            throw new RestException(Status.CONFLICT, e.getMessage());
        } catch (Exception e) {
            throw new RestException(e);
        }
    }

    protected OffloadProcessStatus internalOffloadStatus(boolean authoritative) {
        validateAdminOperationOnTopic(authoritative);
        PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
        return topic.offloadStatus();
    }

    public static CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(PulsarService pulsar,
            String clientAppId, String originalPrincipal, AuthenticationDataSource authenticationData, TopicName topicName) {
        CompletableFuture<PartitionedTopicMetadata> metadataFuture = new CompletableFuture<>();
        try {
            // (1) authorize client
            try {
                checkAuthorization(pulsar, topicName, clientAppId, authenticationData);
            } catch (RestException e) {
                try {
                    validateAdminAccessForTenant(pulsar, clientAppId, originalPrincipal, topicName.getTenant());
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

            String path = path(PARTITIONED_TOPIC_PATH_ZNODE, topicName.getNamespace(), topicName.getDomain().toString(),
                    topicName.getEncodedLocalName());

            // validates global-namespace contains local/peer cluster: if peer/local cluster present then lookup can
            // serve/redirect request else fail partitioned-metadata-request so, client fails while creating
            // producer/consumer
            checkLocalOrGetPeerReplicationCluster(pulsar, topicName.getNamespaceObject())
                    .thenCompose(res -> fetchPartitionedTopicMetadataAsync(pulsar, path)).thenAccept(metadata -> {
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
     * Get the Topic object reference from the Pulsar broker
     */
    private Topic getTopicReference(TopicName topicName) {
        try {
            return pulsar().getBrokerService().getTopicIfExists(topicName.toString())
                    .get(pulsar().getConfiguration().getZooKeeperSessionTimeoutMillis(), TimeUnit.MILLISECONDS)
                    .orElseThrow(() -> topicNotFoundReason(topicName));
        } catch (RestException e) {
            throw e;
        } catch (Exception e) {
            throw new RestException(e);
        }
    }

    private RestException topicNotFoundReason(TopicName topicName) {
        if (!topicName.isPartitioned()) {
            return new RestException(Status.NOT_FOUND, "Topic not found");
        }

        PartitionedTopicMetadata partitionedTopicMetadata = getPartitionedTopicMetadata(
                TopicName.get(topicName.getPartitionedTopicName()), false);
        if (partitionedTopicMetadata == null || partitionedTopicMetadata.partitions == 0) {
            final String topicErrorType = partitionedTopicMetadata == null ?
                    "has no metadata" : "has zero partitions";
            return new RestException(Status.NOT_FOUND, String.format(
                    "Partitioned Topic not found: %s %s", topicName.toString(), topicErrorType));
        } else if (!internalGetList().contains(topicName.toString())) {
            return new RestException(Status.NOT_FOUND, "Topic partitions were not yet created");
        }
        return new RestException(Status.NOT_FOUND, "Partitioned Topic not found");
    }

    private Topic getOrCreateTopic(TopicName topicName) {
        return pulsar().getBrokerService().getTopic(topicName.toString(), true).thenApply(Optional::get).join();
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

    private CompletableFuture<Void> updatePartitionedTopic(TopicName topicName, int numPartitions) {
        final String path = ZkAdminPaths.partitionedTopicPath(topicName);

        CompletableFuture<Void> updatePartition = new CompletableFuture<>();
        createSubscriptions(topicName, numPartitions).thenAccept(res -> {
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
     * @param topicName
     *            : topic-name: persistent://prop/cluster/ns/topic
     * @param numPartitions
     *            : number partitions for the topics
     */
    private CompletableFuture<Void> createSubscriptions(TopicName topicName, int numPartitions) {
        String path = path(PARTITIONED_TOPIC_PATH_ZNODE, topicName.getPersistenceNamingEncoding());
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

            admin.topics().getStatsAsync(topicName.getPartition(0).toString()).thenAccept(stats -> {
                stats.subscriptions.keySet().forEach(subscription -> {
                    List<CompletableFuture<Void>> subscriptionFutures = new ArrayList<>();
                    for (int i = partitionMetadata.partitions; i < numPartitions; i++) {
                        final String topicNamePartition = topicName.getPartition(i).toString();

                        subscriptionFutures.add(admin.topics().createSubscriptionAsync(topicNamePartition,
                                subscription, MessageId.latest));
                    }

                    FutureUtil.waitForAll(subscriptionFutures).thenRun(() -> {
                        log.info("[{}] Successfully created new partitions {}", clientAppId(), topicName);
                        result.complete(null);
                    }).exceptionally(ex -> {
                        log.warn("[{}] Failed to create subscriptions on new partitions for {}", clientAppId(), topicName, ex);
                        result.completeExceptionally(ex);
                        return null;
                    });
                });
            }).exceptionally(ex -> {
                if (ex.getCause() instanceof PulsarAdminException.NotFoundException) {
                    // The first partition doesn't exist, so there are currently to subscriptions to recreate
                    result.complete(null);
                } else {
                    log.warn("[{}] Failed to get list of subscriptions of {}", clientAppId(), topicName.getPartition(0), ex);
                    result.completeExceptionally(ex);
                }
                return null;
            });
        }).exceptionally(ex -> {
            log.warn("[{}] Failed to get partition metadata for {}", clientAppId(), topicName.toString());
            result.completeExceptionally(ex);
            return null;
        });
        return result;
    }

    protected void unloadTopic(TopicName topicName, boolean authoritative) {
        validateSuperUserAccess();
        validateTopicOwnership(topicName, authoritative);
        try {
            Topic topic = getTopicReference(topicName);
            topic.close().get();
            log.info("[{}] Successfully unloaded topic {}", clientAppId(), topicName);
        } catch (NullPointerException e) {
            log.error("[{}] topic {} not found", clientAppId(), topicName);
            throw new RestException(Status.NOT_FOUND, "Topic does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to unload topic {}, {}", clientAppId(), topicName, e.getMessage(), e);
            throw new RestException(e);
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

    protected MessageId internalGetLastMessageId(boolean authoritative) {
        validateAdminOperationOnTopic(authoritative);

        if (!(getTopicReference(topicName) instanceof PersistentTopic)) {
            log.error("[{}] Not supported operation of non-persistent topic {}", clientAppId(), topicName);
            throw new RestException(Status.METHOD_NOT_ALLOWED,
                    "GetLastMessageId on a non-persistent topic is not allowed");
        }
        PersistentTopic topic = (PersistentTopic) getTopicReference(topicName);
        Position position = topic.getLastMessageId();
        int partitionIndex = TopicName.getPartitionIndex(topic.getName());

        MessageId messageId = new MessageIdImpl(((PositionImpl)position).getLedgerId(), ((PositionImpl)position).getEntryId(), partitionIndex);

        return messageId;
    }
}
