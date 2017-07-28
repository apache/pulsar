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
package org.apache.pulsar.broker.admin;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.pulsar.common.util.Codec.decode;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

import org.apache.bookkeeper.mledger.AsyncCallbacks.ManagedLedgerInfoCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerOfflineBacklog;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
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
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.util.FutureUtil;
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
import org.apache.pulsar.common.util.Codec;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

/**
 */
@Path("/persistent")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/persistent", description = "Persistent topic admin apis", tags = "persistent topic")
public class PersistentTopics extends AdminResource {
    private static final Logger log = LoggerFactory.getLogger(PersistentTopics.class);

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSZ").withZone(ZoneId.systemDefault());

    protected static final int PARTITIONED_TOPIC_WAIT_SYNC_TIME_MS = 1000;
    private static final int OFFLINE_TOPIC_STAT_TTL_MINS = 10;

    @GET
    @Path("/{property}/{cluster}/{namespace}")
    @ApiOperation(value = "Get the list of destinations under a namespace.", response = String.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
    public List<String> getList(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace) {
        validateAdminAccessOnProperty(property);

        // Validate that namespace exists, throws 404 if it doesn't exist
        try {
            policiesCache().get(path("policies", property, cluster, namespace));
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to get topic list {}/{}/{}: Namespace does not exist", clientAppId(), property,
                    cluster, namespace);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to get topic list {}/{}/{}", clientAppId(), property, cluster, namespace, e);
            throw new RestException(e);
        }

        List<String> destinations = Lists.newArrayList();

        try {
            String path = String.format("/managed-ledgers/%s/%s/%s/%s", property, cluster, namespace, domain());
            for (String destination : managedLedgerListCache().get(path)) {
                if (domain().equals(DestinationDomain.persistent.toString())) {
                    destinations.add(DestinationName
                            .get(domain(), property, cluster, namespace, decode(destination)).toString());
                }
            }
        } catch (KeeperException.NoNodeException e) {
            // NoNode means there are no destination in this domain for this namespace
        } catch (Exception e) {
            log.error("[{}] Failed to get destination list for namespace {}/{}/{}", clientAppId(), property, cluster,
                    namespace, e);
            throw new RestException(e);
        }

        destinations.sort(null);
        return destinations;
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/partitioned")
    @ApiOperation(value = "Get the list of partitioned topics under a namespace.", response = String.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
    public List<String> getPartitionedTopicList(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace) {
        validateAdminAccessOnProperty(property);

        // Validate that namespace exists, throws 404 if it doesn't exist
        try {
            policiesCache().get(path("policies", property, cluster, namespace));
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to get partitioned topic list {}/{}/{}: Namespace does not exist", clientAppId(), property,
                    cluster, namespace);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to get partitioned topic list for namespace {}/{}/{}", clientAppId(), property, cluster, namespace, e);
            throw new RestException(e);
        }

        List<String> partitionedTopics = Lists.newArrayList();

        try {
            String partitionedTopicPath = path(PARTITIONED_TOPIC_PATH_ZNODE, property, cluster, namespace, domain());
            List<String> destinations = globalZk().getChildren(partitionedTopicPath, false);
            partitionedTopics = destinations.stream().map(s -> String.format("persistent://%s/%s/%s/%s", property, cluster, namespace, decode(s))).collect(
                    Collectors.toList());
        } catch (KeeperException.NoNodeException e) {
            // NoNode means there are no partitioned topics in this domain for this namespace
        } catch (Exception e) {
            log.error("[{}] Failed to get partitioned topic list for namespace {}/{}/{}", clientAppId(), property, cluster,
                    namespace, e);
            throw new RestException(e);
        }

        partitionedTopics.sort(null);
        return partitionedTopics;
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/{destination}/permissions")
    @ApiOperation(value = "Get permissions on a destination.", notes = "Retrieve the effective permissions for a destination. These permissions are defined by the permissions set at the"
            + "namespace level combined (union) with any eventual specific permission set on the destination.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
    public Map<String, Set<AuthAction>> getPermissionsOnDestination(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("destination") @Encoded String destination) {
        // This operation should be reading from zookeeper and it should be allowed without having admin privileges
        destination = decode(destination);
        validateAdminAccessOnProperty(property);

        String destinationUri = DestinationName.get(domain(), property, cluster, namespace, destination).toString();

        try {
            Policies policies = policiesCache().get(path("policies", property, cluster, namespace))
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

    protected void validateAdminAndClientPermission(DestinationName destination) {
        try {
            validateAdminAccessOnProperty(destination.getProperty());
        } catch (Exception ve) {
            try {
                checkAuthorization(pulsar(), destination, clientAppId());
            } catch (RestException re) {
                throw re;
            } catch (Exception e) {
                // unknown error marked as internal server error
                log.warn("Unexpected error while authorizing request. destination={}, role={}. Error: {}", destination,
                        clientAppId(), e.getMessage(), e);
                throw new RestException(e);
            }
        }
    }

    protected void validateAdminOperationOnDestination(DestinationName fqdn, boolean authoritative) {
        validateAdminAccessOnProperty(fqdn.getProperty());
        validateDestinationOwnership(fqdn, authoritative);
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{destination}/permissions/{role}")
    @ApiOperation(value = "Grant a new permission to a role on a single destination.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void grantPermissionsOnDestination(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("destination") @Encoded String destination, @PathParam("role") String role, Set<AuthAction> actions) {
        destination = decode(destination);
        // This operation should be reading from zookeeper and it should be allowed without having admin privileges
        validateAdminAccessOnProperty(property);
        validatePoliciesReadOnlyAccess();

        String destinationUri = DestinationName.get(domain(), property, cluster, namespace, destination).toString();

        try {
            Stat nodeStat = new Stat();
            byte[] content = globalZk().getData(path("policies", property, cluster, namespace), null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);

            if (!policies.auth_policies.destination_auth.containsKey(destinationUri)) {
                policies.auth_policies.destination_auth.put(destinationUri, new TreeMap<String, Set<AuthAction>>());
            }

            policies.auth_policies.destination_auth.get(destinationUri).put(role, actions);

            // Write the new policies to zookeeper
            globalZk().setData(path("policies", property, cluster, namespace), jsonMapper().writeValueAsBytes(policies),
                    nodeStat.getVersion());

            // invalidate the local cache to force update
            policiesCache().invalidate(path("policies", property, cluster, namespace));

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

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/{destination}/permissions/{role}")
    @ApiOperation(value = "Revoke permissions on a destination.", notes = "Revoke permissions to a role on a single destination. If the permission was not set at the destination"
            + "level, but rather at the namespace level, this operation will return an error (HTTP status code 412).")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Permissions are not set at the destination level") })
    public void revokePermissionsOnDestination(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("destination") @Encoded String destination, @PathParam("role") String role) {
        destination = decode(destination);
        // This operation should be reading from zookeeper and it should be allowed without having admin privileges
        validateAdminAccessOnProperty(property);
        validatePoliciesReadOnlyAccess();

        String destinationUri = DestinationName.get(domain(), property, cluster, namespace, destination).toString();
        Stat nodeStat = new Stat();
        Policies policies;

        try {
            byte[] content = globalZk().getData(path("policies", property, cluster, namespace), null, nodeStat);
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
            String namespacePath = path("policies", property, cluster, namespace);
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

    @PUT
    @Path("/{property}/{cluster}/{namespace}/{destination}/partitions")
    @ApiOperation(value = "Create a partitioned topic.", notes = "It needs to be called before creating a producer on a partitioned topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 409, message = "Partitioned topic already exist") })
    public void createPartitionedTopic(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("destination") @Encoded String destination, int numPartitions,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        destination = decode(destination);
        DestinationName dn = DestinationName.get(domain(), property, cluster, namespace, destination);
        validateAdminAccessOnProperty(dn.getProperty());
        if (numPartitions <= 1) {
            throw new RestException(Status.NOT_ACCEPTABLE, "Number of partitions should be more than 1");
        }
        try {
            String path = path(PARTITIONED_TOPIC_PATH_ZNODE, property, cluster, namespace, domain(),
                    dn.getEncodedLocalName());
            byte[] data = jsonMapper().writeValueAsBytes(new PartitionedTopicMetadata(numPartitions));
            zkCreateOptimistic(path, data);
            // we wait for the data to be synced in all quorums and the observers
            Thread.sleep(PARTITIONED_TOPIC_WAIT_SYNC_TIME_MS);
            log.info("[{}] Successfully created partitioned topic {}", clientAppId(), dn);
        } catch (KeeperException.NodeExistsException e) {
            log.warn("[{}] Failed to create already existing partitioned topic {}", clientAppId(), dn);
            throw new RestException(Status.CONFLICT, "Partitioned topic already exist");
        } catch (Exception e) {
            log.error("[{}] Failed to create partitioned topic {}", clientAppId(), dn, e);
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
     * @param property
     * @param cluster
     * @param namespace
     * @param destination
     * @param numPartitions
     */
    @POST
    @Path("/{property}/{cluster}/{namespace}/{destination}/partitions")
    @ApiOperation(value = "Increment partitons of an existing partitioned topic.", notes = "It only increments partitions of existing non-global partitioned-topic")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 409, message = "Partitioned topic does not exist") })
    public void updatePartitionedTopic(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("destination") @Encoded String destination,
            int numPartitions) {
        destination = decode(destination);
        DestinationName dn = DestinationName.get(domain(), property, cluster, namespace, destination);
        validateAdminAccessOnProperty(dn.getProperty());
        if (dn.isGlobal()) {
            log.error("[{}] Update partitioned-topic is forbidden on global namespace {}", clientAppId(), dn);
            throw new RestException(Status.FORBIDDEN, "Update forbidden on global namespace");
        }
        if (numPartitions <= 1) {
            throw new RestException(Status.NOT_ACCEPTABLE, "Number of partitions should be more than 1");
        }
        try {
            updatePartitionedTopic(dn, numPartitions).get();
        } catch (Exception e) {
            if (e.getCause() instanceof RestException) {
                throw (RestException) e.getCause();
            }
            log.error("[{}] Failed to update partitioned topic {}", clientAppId(), dn, e.getCause());
            throw new RestException(e.getCause());
        }
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/{destination}/partitions")
    @ApiOperation(value = "Get partitioned topic metadata.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public PartitionedTopicMetadata getPartitionedMetadata(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("destination") @Encoded String destination,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        destination = decode(destination);
       return getPartitionedTopicMetadata(property, cluster, namespace, destination, authoritative);
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/{destination}/partitions")
    @ApiOperation(value = "Delete a partitioned topic.", notes = "It will also delete all the partitions of the topic if it exists.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Partitioned topic does not exist") })
    public void deletePartitionedTopic(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("destination") @Encoded String destination,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        destination = decode(destination);
        DestinationName dn = DestinationName.get(domain(), property, cluster, namespace, destination);
        validateAdminAccessOnProperty(dn.getProperty());
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(property, cluster, namespace,
                destination, authoritative);
        int numPartitions = partitionMetadata.partitions;
        if (numPartitions > 0) {
            final CompletableFuture<Void> future = new CompletableFuture<>();
            final AtomicInteger count = new AtomicInteger(numPartitions);
            try {
                for (int i = 0; i < numPartitions; i++) {
                    DestinationName dn_partition = dn.getPartition(i);
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
        String path = path(PARTITIONED_TOPIC_PATH_ZNODE, property, cluster, namespace, domain(),
                dn.getEncodedLocalName());
        try {
            globalZk().delete(path, -1);
            globalZkCache().invalidate(path);
            // we wait for the data to be synced in all quorums and the observers
            Thread.sleep(PARTITIONED_TOPIC_WAIT_SYNC_TIME_MS);
            log.info("[{}] Deleted partitioned topic {}", clientAppId(), dn);
        } catch (KeeperException.NoNodeException nne) {
            throw new RestException(Status.NOT_FOUND, "Partitioned topic does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to delete partitioned topic {}", clientAppId(), dn, e);
            throw new RestException(e);
        }
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/{destination}")
    @ApiOperation(value = "Delete a topic.", notes = "The topic cannot be deleted if there's any active subscription or producer connected to the it.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 412, message = "Topic has active producers/subscriptions") })
    public void deleteTopic(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("destination") @Encoded String destination,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        destination = decode(destination);
        DestinationName dn = DestinationName.get(domain(), property, cluster, namespace, destination);
        validateAdminOperationOnDestination(dn, authoritative);
        Topic topic = getTopicReference(dn);
        if (dn.isGlobal()) {
            // Delete is disallowed on global topic
            log.error("[{}] Delete topic is forbidden on global namespace {}", clientAppId(), dn);
            throw new RestException(Status.FORBIDDEN, "Delete forbidden on global namespace");
        }
        try {
            topic.delete().get();
            log.info("[{}] Successfully removed topic {}", clientAppId(), dn);
        } catch (Exception e) {
            Throwable t = e.getCause();
            log.error("[{}] Failed to get delete topic {}", clientAppId(), dn, t);
            if (t instanceof TopicBusyException) {
                throw new RestException(Status.PRECONDITION_FAILED, "Topic has active producers/subscriptions");
            } else {
                throw new RestException(t);
            }
        }
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/{destination}/subscriptions")
    @ApiOperation(value = "Get the list of persistent subscriptions for a given topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public List<String> getSubscriptions(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("destination") @Encoded String destination,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        destination = decode(destination);
        DestinationName dn = DestinationName.get(domain(), property, cluster, namespace, destination);
        List<String> subscriptions = Lists.newArrayList();
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(property, cluster, namespace,
                destination, authoritative);
        if (partitionMetadata.partitions > 0) {
            try {
                // get the subscriptions only from the 1st partition since all the other partitions will have the same
                // subscriptions
                subscriptions.addAll(
                        pulsar().getAdminClient().persistentTopics().getSubscriptions(dn.getPartition(0).toString()));
            } catch (Exception e) {
                throw new RestException(e);
            }
        } else {
            validateAdminOperationOnDestination(dn, authoritative);
            Topic topic = getTopicReference(dn);

            try {
                topic.getSubscriptions().forEach((subName, sub) -> subscriptions.add(subName));
            } catch (Exception e) {
                log.error("[{}] Failed to get list of subscriptions for {}", clientAppId(), dn);
                throw new RestException(e);
            }
        }

        return subscriptions;
    }

    @GET
    @Path("{property}/{cluster}/{namespace}/{destination}/stats")
    @ApiOperation(value = "Get the stats for the topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public PersistentTopicStats getStats(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("destination") @Encoded String destination,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        destination = decode(destination);
        DestinationName dn = DestinationName.get(domain(), property, cluster, namespace, destination);
        validateAdminAndClientPermission(dn);
        validateDestinationOwnership(dn, authoritative);
        Topic topic = getTopicReference(dn);
        return topic.getStats();
    }

    @GET
    @Path("{property}/{cluster}/{namespace}/{destination}/internalStats")
    @ApiOperation(value = "Get the internal stats for the topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public PersistentTopicInternalStats getInternalStats(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("destination") @Encoded String destination,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        destination = decode(destination);
        DestinationName dn = DestinationName.get(domain(), property, cluster, namespace, destination);
        validateAdminAndClientPermission(dn);
        validateDestinationOwnership(dn, authoritative);
        Topic topic = getTopicReference(dn);
        return topic.getInternalStats();
    }

    @GET
    @Path("{property}/{cluster}/{namespace}/{destination}/internal-info")
    @ApiOperation(value = "Get the internal stats for the topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public void getManagedLedgerInfo(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("destination") @Encoded String destination,
            @Suspended AsyncResponse asyncResponse) {
        destination = decode(destination);
        DestinationName dn = DestinationName.get(domain(), property, cluster, namespace, destination);
        validateAdminAccessOnProperty(dn.getProperty());

        String managedLedger = dn.getPersistenceNamingEncoding();
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

    @GET
    @Path("{property}/{cluster}/{namespace}/{destination}/partitioned-stats")
    @ApiOperation(value = "Get the stats for the partitioned topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public PartitionedTopicStats getPartitionedStats(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("destination") @Encoded String destination,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        destination = decode(destination);
        DestinationName dn = DestinationName.get(domain(), property, cluster, namespace, destination);
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(property, cluster, namespace,
                destination, authoritative);
        if (partitionMetadata.partitions == 0) {
            throw new RestException(Status.NOT_FOUND, "Partitioned Topic not found");
        }
        PartitionedTopicStats stats = new PartitionedTopicStats(partitionMetadata);
        try {
            for (int i = 0; i < partitionMetadata.partitions; i++) {
                PersistentTopicStats partitionStats = pulsar().getAdminClient().persistentTopics()
                        .getStats(dn.getPartition(i).toString());
                stats.add(partitionStats);
                stats.partitions.put(dn.getPartition(i).toString(), partitionStats);
            }
        } catch (Exception e) {
            throw new RestException(e);
        }
        return stats;
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/{destination}/subscription/{subName}")
    @ApiOperation(value = "Delete a subscription.", notes = "There should not be any active consumers on the subscription.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 412, message = "Subscription has active consumers") })
    public void deleteSubscription(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("destination") @Encoded String destination,
            @PathParam("subName") String subName,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        destination = decode(destination);
        DestinationName dn = DestinationName.get(domain(), property, cluster, namespace, destination);
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(property, cluster, namespace,
                destination, authoritative);
        if (partitionMetadata.partitions > 0) {
            try {
                for (int i = 0; i < partitionMetadata.partitions; i++) {
                    pulsar().getAdminClient().persistentTopics().deleteSubscription(dn.getPartition(i).toString(),
                            subName);
                }
            } catch (Exception e) {
                throw new RestException(e);
            }
        } else {
            validateAdminOperationOnDestination(dn, authoritative);
            Topic topic = getTopicReference(dn);
            try {
                Subscription sub = topic.getSubscription(subName);
                checkNotNull(sub);
                sub.delete().get();
                log.info("[{}][{}] Deleted subscription {}", clientAppId(), dn, subName);
            } catch (Exception e) {
                Throwable t = e.getCause();
                log.error("[{}] Failed to delete subscription {} {}", clientAppId(), dn, subName, e);
                if (e instanceof NullPointerException) {
                    throw new RestException(Status.NOT_FOUND, "Subscription not found");
                } else if (t instanceof SubscriptionBusyException) {
                    throw new RestException(Status.PRECONDITION_FAILED, "Subscription has active connected consumers");
                } else {
                    throw new RestException(t);
                }
            }
        }

    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{destination}/subscription/{subName}/skip_all")
    @ApiOperation(value = "Skip all messages on a topic subscription.", notes = "Completely clears the backlog on the subscription.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 405, message = "Operation not allowed on non-persistent topic"),
            @ApiResponse(code = 404, message = "Topic or subscription does not exist") })
    public void skipAllMessages(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("destination") @Encoded String destination,
            @PathParam("subName") String subName,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        destination = decode(destination);
        DestinationName dn = DestinationName.get(domain(), property, cluster, namespace, destination);
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(property, cluster, namespace,
                destination, authoritative);
        if (partitionMetadata.partitions > 0) {
            try {
                for (int i = 0; i < partitionMetadata.partitions; i++) {
                    pulsar().getAdminClient().persistentTopics().skipAllMessages(dn.getPartition(i).toString(),
                            subName);
                }
            } catch (Exception e) {
                throw new RestException(e);
            }
        } else {
            validateAdminOperationOnDestination(dn, authoritative);
            PersistentTopic topic = (PersistentTopic) getTopicReference(dn);
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
                log.info("[{}] Cleared backlog on {} {}", clientAppId(), dn, subName);
            } catch (NullPointerException npe) {
                throw new RestException(Status.NOT_FOUND, "Subscription not found");
            } catch (Exception exception) {
                log.error("[{}] Failed to skip all messages {} {}", clientAppId(), dn, subName, exception);
                throw new RestException(exception);
            }
        }

    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{destination}/subscription/{subName}/skip/{numMessages}")
    @ApiOperation(value = "Skip messages on a topic subscription.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic or subscription does not exist") })
    public void skipMessages(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("destination") @Encoded String destination,
            @PathParam("subName") String subName, @PathParam("numMessages") int numMessages,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        destination = decode(destination);
        DestinationName dn = DestinationName.get(domain(), property, cluster, namespace, destination);
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(property, cluster, namespace,
                destination, authoritative);
        if (partitionMetadata.partitions > 0) {
            throw new RestException(Status.METHOD_NOT_ALLOWED, "Skip messages on a partitioned topic is not allowed");
        }
        validateAdminOperationOnDestination(dn, authoritative);
        PersistentTopic topic = (PersistentTopic) getTopicReference(dn);
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
            log.info("[{}] Skipped {} messages on {} {}", clientAppId(), numMessages, dn, subName);
        } catch (NullPointerException npe) {
            throw new RestException(Status.NOT_FOUND, "Subscription not found");
        } catch (Exception exception) {
            log.error("[{}] Failed to skip {} messages {} {}", clientAppId(), numMessages, dn, subName, exception);
            throw new RestException(exception);
        }
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{destination}/subscription/{subName}/expireMessages/{expireTimeInSeconds}")
    @ApiOperation(value = "Expire messages on a topic subscription.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic or subscription does not exist") })
    public void expireTopicMessages(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("destination") @Encoded String destination,
            @PathParam("subName") String subName, @PathParam("expireTimeInSeconds") int expireTimeInSeconds,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        destination = decode(destination);
        expireMessages(property, cluster, namespace, destination, subName, expireTimeInSeconds, authoritative);
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{destination}/all_subscription/expireMessages/{expireTimeInSeconds}")
    @ApiOperation(value = "Expire messages on all subscriptions of topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic or subscription does not exist") })
    public void expireMessagesForAllSubscriptions(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("destination") @Encoded String destinationName, @PathParam("expireTimeInSeconds") int expireTimeInSeconds,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        final String destination = decode(destinationName);
        DestinationName dn = DestinationName.get(domain(), property, cluster, namespace, destination);
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(property, cluster, namespace,
                destination, authoritative);
        if (partitionMetadata.partitions > 0) {
            try {
                // expire messages for each partition destination
                for (int i = 0; i < partitionMetadata.partitions; i++) {
                    pulsar().getAdminClient().persistentTopics()
                            .expireMessagesForAllSubscriptions(dn.getPartition(i).toString(), expireTimeInSeconds);
                }
            } catch (Exception e) {
                log.error("[{}] Failed to expire messages up to {} on {} {}", clientAppId(), expireTimeInSeconds, dn,
                        e);
                throw new RestException(e);
            }
        } else {
            // validate ownership and redirect if current broker is not owner
            validateAdminOperationOnDestination(dn, authoritative);
            PersistentTopic topic = (PersistentTopic) getTopicReference(dn);
            topic.getReplicators().forEach((subName, replicator) -> {
                expireMessages(property, cluster, namespace, destination, subName, expireTimeInSeconds, authoritative);
            });
            topic.getSubscriptions().forEach((subName, subscriber) -> {
                expireMessages(property, cluster, namespace, destination, subName, expireTimeInSeconds, authoritative);
            });
        }
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{destination}/subscription/{subName}/resetcursor/{timestamp}")
    @ApiOperation(value = "Reset subscription to message position closest to absolute timestamp (in ms).", notes = "There should not be any active consumers on the subscription.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Not supported for global and non-persistent topics"),
            @ApiResponse(code = 412, message = "Subscription has active consumers") })
    public void resetCursor(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("destination") @Encoded String destination,
            @PathParam("subName") String subName, @PathParam("timestamp") long timestamp,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        destination = decode(destination);
        DestinationName dn = DestinationName.get(domain(), property, cluster, namespace, destination);
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(property, cluster, namespace,
                destination, authoritative);

        if (partitionMetadata.partitions > 0) {
            int numParts = partitionMetadata.partitions;
            int numPartException = 0;
            Exception partitionException = null;
            try {
                for (int i = 0; i < numParts; i++) {
                    pulsar().getAdminClient().persistentTopics().resetCursor(dn.getPartition(i).toString(), subName,
                            timestamp);
                }
            } catch (PreconditionFailedException pfe) {
                // throw the last exception if all partitions get this error
                // any other exception on partition is reported back to user
                ++numPartException;
                partitionException = pfe;
            } catch (Exception e) {
                log.warn("[{}] [{}] Failed to reset cursor on subscription {} to time {}", clientAppId(), dn, subName,
                        timestamp, e);
                throw new RestException(e);
            }
            // report an error to user if unable to reset for all partitions
            if (numPartException == numParts) {
                log.warn("[{}] [{}] Failed to reset cursor on subscription {} to time {}", clientAppId(), dn, subName,
                        timestamp, partitionException);
                throw new RestException(Status.PRECONDITION_FAILED, partitionException.getMessage());
            } else if (numPartException > 0 && log.isDebugEnabled()) {
                log.debug("[{}][{}] partial errors for reset cursor on subscription {} to time {} - ", clientAppId(),
                        destination, subName, timestamp, partitionException);
            }

        } else {
            validateAdminOperationOnDestination(dn, authoritative);
            log.info("[{}][{}] received reset cursor on subscription {} to time {}", clientAppId(), destination,
                    subName, timestamp);
            PersistentTopic topic = (PersistentTopic) getTopicReference(dn);
            try {
                PersistentSubscription sub = topic.getSubscription(subName);
                checkNotNull(sub);
                sub.resetCursor(timestamp).get();
                log.info("[{}][{}] reset cursor on subscription {} to time {}", clientAppId(), dn, subName, timestamp);
            } catch (Exception e) {
                Throwable t = e.getCause();
                log.warn("[{}] [{}] Failed to reset cursor on subscription {} to time {}", clientAppId(), dn, subName,
                        timestamp, e);
                if (e instanceof NullPointerException) {
                    throw new RestException(Status.NOT_FOUND, "Subscription not found");
                } else if (e instanceof NotAllowedException) {
                    throw new RestException(Status.METHOD_NOT_ALLOWED, e.getMessage());
                } else if (t instanceof SubscriptionBusyException) {
                    throw new RestException(Status.PRECONDITION_FAILED, "Subscription has active connected consumers");
                } else if (t instanceof SubscriptionInvalidCursorPosition) {
                    throw new RestException(Status.PRECONDITION_FAILED,
                            "Unable to find position for timestamp specified -" + t.getMessage());
                } else {
                    throw new RestException(e);
                }
            }
        }
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/{destination}/subscription/{subName}/position/{messagePosition}")
    @ApiOperation(value = "Peek nth message on a topic subscription.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic, subscription or the message position does not exist") })
    public Response peekNthMessage(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("destination") @Encoded String destination,
            @PathParam("subName") String subName, @PathParam("messagePosition") int messagePosition,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        destination = decode(destination);
        DestinationName dn = DestinationName.get(domain(), property, cluster, namespace, destination);
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(property, cluster, namespace,
                destination, authoritative);
        if (partitionMetadata.partitions > 0) {
            throw new RestException(Status.METHOD_NOT_ALLOWED, "Peek messages on a partitioned topic is not allowed");
        }
        validateAdminOperationOnDestination(dn, authoritative);
        if (!(getTopicReference(dn) instanceof PersistentTopic)) {
            log.error("[{}] Not supported operation of non-persistent topic {} {}", clientAppId(), dn, subName);
            throw new RestException(Status.METHOD_NOT_ALLOWED,
                    "Skip messages on a non-persistent topic is not allowed");
        }
        PersistentTopic topic = (PersistentTopic) getTopicReference(dn);
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
                responseBuilder.header("X-Pulsar-publish-time",
                        DATE_FORMAT.format(Instant.ofEpochMilli(metadata.getPublishTime())));
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
            log.error("[{}] Failed to get message at position {} from {} {}", clientAppId(), messagePosition, dn,
                    subName, exception);
            throw new RestException(exception);
        } finally {
            if (entry != null) {
                entry.release();
            }
        }
    }

    @GET
    @Path("{property}/{cluster}/{namespace}/{destination}/backlog")
    @ApiOperation(value = "Get estimated backlog for offline topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public PersistentOfflineTopicStats getBacklog(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("destination") @Encoded String destination,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        destination = decode(destination);
        validateAdminAccessOnProperty(property);
        // Validate that namespace exists, throw 404 if it doesn't exist
        // note that we do not want to load the topic and hence skip validateAdminOperationOnDestination()
        try {
            policiesCache().get(path("policies", property, cluster, namespace));
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to get topic backlog {}/{}/{}: Namespace does not exist", clientAppId(), property,
                    cluster, namespace);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to get topic backlog {}/{}/{}", clientAppId(), property, cluster, namespace, e);
            throw new RestException(e);
        }
        DestinationName dn = DestinationName.get(domain(), property, cluster, namespace, destination);
        PersistentOfflineTopicStats offlineTopicStats = null;
        try {

            offlineTopicStats = pulsar().getBrokerService().getOfflineTopicStat(dn);
            if (offlineTopicStats != null) {
                // offline topic stat has a cost - so use cached value until TTL
                long elapsedMs = System.currentTimeMillis() - offlineTopicStats.statGeneratedAt.getTime();
                if (TimeUnit.MINUTES.convert(elapsedMs, TimeUnit.MILLISECONDS) < OFFLINE_TOPIC_STAT_TTL_MINS) {
                    return offlineTopicStats;
                }
            }
            final ManagedLedgerConfig config = pulsar().getBrokerService().getManagedLedgerConfig(dn).get();
            ManagedLedgerOfflineBacklog offlineTopicBacklog = new ManagedLedgerOfflineBacklog(config.getDigestType(),
                    config.getPassword(), pulsar().getAdvertisedAddress(), false);
            offlineTopicStats = offlineTopicBacklog
                    .estimateUnloadedTopicBacklog((ManagedLedgerFactoryImpl) pulsar().getManagedLedgerFactory(), dn);
            pulsar().getBrokerService().cacheOfflineTopicStats(dn, offlineTopicStats);
        } catch (Exception exception) {
            throw new RestException(exception);
        }
        return offlineTopicStats;
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{destination}/terminate")
    @ApiOperation(value = "Terminate a topic. A topic that is terminated will not accept any more "
            + "messages to be published and will let consumer to drain existing messages in backlog")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 405, message = "Operation not allowed on non-persistent topic"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public MessageId terminate(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("destination") @Encoded String destination,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        destination = decode(destination);
        DestinationName dn = DestinationName.get(domain(), property, cluster, namespace, destination);
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(property, cluster, namespace,
                destination, authoritative);
        if (partitionMetadata.partitions > 0) {
            throw new RestException(Status.METHOD_NOT_ALLOWED, "Termination of a partitioned topic is not allowed");
        }
        validateAdminOperationOnDestination(dn, authoritative);
        Topic topic = getTopicReference(dn);
        try {
            return ((PersistentTopic) topic).terminate().get();
        } catch (Exception exception) {
            log.error("[{}] Failed to terminated topic {}", clientAppId(), dn, exception);
            throw new RestException(exception);
        }
    }

    public void expireMessages(String property, String cluster, String namespace, String destination, String subName,
            int expireTimeInSeconds, boolean authoritative) {
        DestinationName dn = DestinationName.get(domain(), property, cluster, namespace, destination);
        PartitionedTopicMetadata partitionMetadata = getPartitionedTopicMetadata(property, cluster, namespace,
                destination, authoritative);
        if (partitionMetadata.partitions > 0) {
            // expire messages for each partition destination
            try {
                for (int i = 0; i < partitionMetadata.partitions; i++) {
                    pulsar().getAdminClient().persistentTopics().expireMessages(dn.getPartition(i).toString(), subName,
                            expireTimeInSeconds);
                }
            } catch (Exception e) {
                throw new RestException(e);
            }
        } else {
            // validate ownership and redirect if current broker is not owner
            validateAdminOperationOnDestination(dn, authoritative);
            if (!(getTopicReference(dn) instanceof PersistentTopic)) {
                log.error("[{}] Not supported operation of non-persistent topic {} {}", clientAppId(), dn, subName);
                throw new RestException(Status.METHOD_NOT_ALLOWED,
                        "Expire messages on a non-persistent topic is not allowed");
            }
            PersistentTopic topic = (PersistentTopic) getTopicReference(dn);
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
                log.info("[{}] Message expire started up to {} on {} {}", clientAppId(), expireTimeInSeconds, dn,
                        subName);
            } catch (NullPointerException npe) {
                throw new RestException(Status.NOT_FOUND, "Subscription not found");
            } catch (Exception exception) {
                log.error("[{}] Failed to expire messages up to {} on {} with subscription {} {}", clientAppId(),
                        expireTimeInSeconds, dn, subName, exception);
                throw new RestException(exception);
            }
        }
    }

    public static CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(PulsarService pulsar,
            String clientAppId, DestinationName dn) {
        CompletableFuture<PartitionedTopicMetadata> metadataFuture = new CompletableFuture<>();
        try {
            // (1) authorize client
            try {
                checkAuthorization(pulsar, dn, clientAppId);
            } catch (RestException e) {
                try {
                    validateAdminAccessOnProperty(pulsar, clientAppId, dn.getProperty());
                } catch (RestException authException) {
                    log.warn("Failed to authorize {} on cluster {}", clientAppId, dn.toString());
                    throw new PulsarClientException(String.format("Authorization failed %s on cluster %s with error %s",
                            clientAppId, dn.toString(), authException.getMessage()));
                }
            } catch (Exception ex) {
                // throw without wrapping to PulsarClientException that considers: unknown error marked as internal
                // server error
                log.warn("Failed to authorize {} on cluster {} with unexpected exception {}", clientAppId,
                        dn.toString(), ex.getMessage(), ex);
                throw ex;
            }
            String path = path(PARTITIONED_TOPIC_PATH_ZNODE, dn.getProperty(), dn.getCluster(),
                    dn.getNamespacePortion(), "persistent", dn.getEncodedLocalName());
            fetchPartitionedTopicMetadataAsync(pulsar, path).thenAccept(metadata -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Total number of partitions for topic {} is {}", clientAppId, dn,
                            metadata.partitions);
                }
                metadataFuture.complete(metadata);
            }).exceptionally(ex -> {
                metadataFuture.completeExceptionally(ex);
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
     * @param dn : topic-name: persistent://prop/cluster/ns/topic
     * @param numPartitions : number partitions for the topics
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

            // get list of cursors name of partition-1
            final String ledgerName = dn.getPartition(1).getPersistenceNamingEncoding();
            ((ManagedLedgerFactoryImpl) pulsar().getManagedLedgerFactory()).getMetaStore().getCursors(ledgerName,
                    new MetaStoreCallback<List<String>>() {

                        @Override
                        public void operationComplete(List<String> cursors,
                                org.apache.bookkeeper.mledger.impl.MetaStore.Stat stat) {
                            List<CompletableFuture<Void>> topicCreationFuture = Lists.newArrayList();
                            // create subscriptions for all new partition-topics
                            cursors.forEach(cursor -> {
                                String subName = Codec.decode(cursor);
                                for (int i = partitionMetadata.partitions; i < numPartitions; i++) {
                                    final String topicName = dn.getPartition(i).toString();
                                    CompletableFuture<Void> future = new CompletableFuture<>();
                                    pulsar().getBrokerService().getTopic(topicName).handle((topic, ex) -> {
                                        if (ex != null) {
                                            log.warn("[{}] Failed to create topic {}", clientAppId(), topicName);
                                            future.completeExceptionally(ex);
                                            return null;
                                        } else {
                                            topic.createSubscription(subName).handle((sub, e) -> {
                                                if (e != null) {
                                                    log.warn("[{}] Failed to create subsciption {} {}", clientAppId(),
                                                            topicName, subName);
                                                    future.completeExceptionally(e);
                                                    return null;
                                                } else {
                                                    log.info("[{}] Successfully create subsciption {} {}",
                                                            clientAppId(), topicName, subName);
                                                    // close topic
                                                    topic.close();
                                                    future.complete(null);
                                                    return null;
                                                }
                                            });
                                            return null;
                                        }
                                    });
                                    topicCreationFuture.add(future);
                                }
                            });
                            // wait for all subscriptions to be created
                            FutureUtil.waitForAll(topicCreationFuture).handle((res, e) -> {
                                if (e != null) {
                                    result.completeExceptionally(e);
                                } else {
                                    log.info("[{}] Successfully create new partitions {}", clientAppId(),
                                            dn.toString());
                                    result.complete(null);
                                }
                                return null;
                            });
                        }

                        @Override
                        public void operationFailed(MetaStoreException ex) {
                            log.warn("[{}] Failed to get list of cursors of {}", clientAppId(), ledgerName);
                            result.completeExceptionally(ex);
                        }
                    });
        }).exceptionally(ex -> {
            log.warn("[{}] Failed to get partition metadata for {}", clientAppId(), dn.toString());
            result.completeExceptionally(ex);
            return null;
        });
        return result;
    }
}
