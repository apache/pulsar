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
package org.apache.pulsar.broker.admin.v2;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.pulsar.broker.admin.impl.PersistentTopicsBase;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.OffloadProcessStatus;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.TopicStats;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

/**
 */
@Path("/persistent")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/persistent", description = "Persistent topic admin apis", tags = "persistent topic")
public class PersistentTopics extends PersistentTopicsBase {

    @GET
    @Path("/{tenant}/{namespace}")
    @ApiOperation(value = "Get the list of topics under a namespace.", response = String.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
    public List<String> getList(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetList();
    }

    @GET
    @Path("/{tenant}/{namespace}/partitioned")
    @ApiOperation(value = "Get the list of partitioned topics under a namespace.", response = String.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
    public List<String> getPartitionedTopicList(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetPartitionedTopicList();
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/permissions")
    @ApiOperation(value = "Get permissions on a topic.", notes = "Retrieve the effective permissions for a topic. These permissions are defined by the permissions set at the"
            + "namespace level combined (union) with any eventual specific permission set on the topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
    public Map<String, Set<AuthAction>> getPermissionsOnTopic(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        return internalGetPermissionsOnTopic();
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/permissions/{role}")
    @ApiOperation(value = "Grant a new permission to a role on a single topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void grantPermissionsOnTopic(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @PathParam("role") String role, Set<AuthAction> actions) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalGrantPermissionsOnTopic(role, actions);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/permissions/{role}")
    @ApiOperation(value = "Revoke permissions on a topic.", notes = "Revoke permissions to a role on a single topic. If the permission was not set at the topic"
            + "level, but rather at the namespace level, this operation will return an error (HTTP status code 412).")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Permissions are not set at the topic level") })
    public void revokePermissionsOnTopic(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @PathParam("role") String role) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalRevokePermissionsOnTopic(role);
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Create a partitioned topic.", notes = "It needs to be called before creating a producer on a partitioned topic.")
    @ApiResponses(value = {
        @ApiResponse(code = 403, message = "Don't have admin permission"),
        @ApiResponse(code = 409, message = "Partitioned topic already exist"),
        @ApiResponse(code = 412, message = "Partitioned topic name is invalid")
    })
    public void createPartitionedTopic(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, int numPartitions,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validatePartitionedTopicName(tenant, namespace, encodedTopic);
        internalCreatePartitionedTopic(numPartitions, authoritative);
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
     * @param tenant
     * @param cluster
     * @param namespace
     * @param topic
     * @param numPartitions
     */
    @POST
    @Path("/{tenant}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Increment partitons of an existing partitioned topic.", notes = "It only increments partitions of existing non-global partitioned-topic")
    @ApiResponses(value = {
        @ApiResponse(code = 403, message = "Don't have admin permission"),
        @ApiResponse(code = 409, message = "Partitioned topic does not exist"),
        @ApiResponse(code = 412, message = "Partitioned topic name is invalid")
    })
    public void updatePartitionedTopic(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, int numPartitions) {
        validatePartitionedTopicName(tenant, namespace, encodedTopic);
        internalUpdatePartitionedTopic(numPartitions);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Get partitioned topic metadata.")
    @ApiResponses(value = {
        @ApiResponse(code = 403, message = "Don't have admin permission"),
        @ApiResponse(code = 409, message = "Partitioned topic does not exist"),
        @ApiResponse(code = 412, message = "Partitioned topic name is invalid")
    })
    public PartitionedTopicMetadata getPartitionedMetadata(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        return internalGetPartitionedMetadata(authoritative);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Delete a partitioned topic.", notes = "It will also delete all the partitions of the topic if it exists.")
    @ApiResponses(value = {
        @ApiResponse(code = 403, message = "Don't have admin permission"),
        @ApiResponse(code = 404, message = "Partitioned topic does not exist"),
        @ApiResponse(code = 412, message = "Partitioned topic name is invalid")
    })
    public void deletePartitionedTopic(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("force") @DefaultValue("false") boolean force,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validatePartitionedTopicName(tenant, namespace, encodedTopic);
        internalDeletePartitionedTopic(authoritative, force);
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/unload")
    @ApiOperation(value = "Unload a topic")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public void unloadTopic(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalUnloadTopic(authoritative);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}")
    @ApiOperation(value = "Delete a topic.", notes = "The topic cannot be deleted if delete is not forcefully and there's any active "
            + "subscription or producer connected to the it. Force delete ignores connected clients and deletes topic by explicitly closing them.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 412, message = "Topic has active producers/subscriptions") })
    public void deleteTopic(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, @QueryParam("force") @DefaultValue("false") boolean force,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalDeleteTopic(authoritative, force);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/subscriptions")
    @ApiOperation(value = "Get the list of persistent subscriptions for a given topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public List<String> getSubscriptions(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        return internalGetSubscriptions(authoritative);
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/stats")
    @ApiOperation(value = "Get the stats for the topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public TopicStats getStats(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        return internalGetStats(authoritative);
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/internalStats")
    @ApiOperation(value = "Get the internal stats for the topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public PersistentTopicInternalStats getInternalStats(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        return internalGetInternalStats(authoritative);
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/internal-info")
    @ApiOperation(value = "Get the internal stats for the topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public void getManagedLedgerInfo(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, @Suspended AsyncResponse asyncResponse) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalGetManagedLedgerInfo(asyncResponse);
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/partitioned-stats")
    @ApiOperation(value = "Get the stats for the partitioned topic.")
    @ApiResponses(value = {
        @ApiResponse(code = 403, message = "Don't have admin permission"),
        @ApiResponse(code = 404, message = "Topic does not exist"),
        @ApiResponse(code = 412, message = "Partitioned topic name is invalid")
    })
    public PartitionedTopicStats getPartitionedStats(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validatePartitionedTopicName(tenant, namespace, encodedTopic);
        return internalGetPartitionedStats(authoritative);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}")
    @ApiOperation(value = "Delete a subscription.", notes = "There should not be any active consumers on the subscription.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 412, message = "Subscription has active consumers") })
    public void deleteSubscription(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, @PathParam("subName") String subName,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalDeleteSubscription(subName, authoritative);
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/skip_all")
    @ApiOperation(value = "Skip all messages on a topic subscription.", notes = "Completely clears the backlog on the subscription.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 405, message = "Operation not allowed on non-persistent topic"),
            @ApiResponse(code = 404, message = "Topic or subscription does not exist") })
    public void skipAllMessages(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, @PathParam("subName") String subName,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalSkipAllMessages(subName, authoritative);
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/skip/{numMessages}")
    @ApiOperation(value = "Skip messages on a topic subscription.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic or subscription does not exist") })
    public void skipMessages(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, @PathParam("subName") String subName,
            @PathParam("numMessages") int numMessages,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalSkipMessages(subName, numMessages, authoritative);
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/expireMessages/{expireTimeInSeconds}")
    @ApiOperation(value = "Expire messages on a topic subscription.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic or subscription does not exist") })
    public void expireTopicMessages(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, @PathParam("subName") String subName,
            @PathParam("expireTimeInSeconds") int expireTimeInSeconds,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalExpireMessages(subName, expireTimeInSeconds, authoritative);
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/all_subscription/expireMessages/{expireTimeInSeconds}")
    @ApiOperation(value = "Expire messages on all subscriptions of topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic or subscription does not exist") })
    public void expireMessagesForAllSubscriptions(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @PathParam("expireTimeInSeconds") int expireTimeInSeconds,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalExpireMessagesForAllSubscriptions(expireTimeInSeconds, authoritative);
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subscriptionName}")
    @ApiOperation(value = "Reset subscription to message position closest to given position.", notes = "Creates a subscription on the topic at the specified message id")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic/Subscription does not exist"),
            @ApiResponse(code = 405, message = "Not supported for partitioned topics") })
    public void createSubscription(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String topic, @PathParam("subscriptionName") String subscriptionName,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative, MessageIdImpl messageId) {
        validateTopicName(tenant, namespace, topic);
        internalCreateSubscription(subscriptionName, messageId, authoritative);
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/resetcursor/{timestamp}")
    @ApiOperation(value = "Reset subscription to message position closest to absolute timestamp (in ms).", notes = "It fence cursor and disconnects all active consumers before reseting cursor.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic/Subscription does not exist") })
    public void resetCursor(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, @PathParam("subName") String subName,
            @PathParam("timestamp") long timestamp,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalResetCursor(subName, timestamp, authoritative);
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/resetcursor")
    @ApiOperation(value = "Reset subscription to message position closest to given position.", notes = "It fence cursor and disconnects all active consumers before reseting cursor.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic/Subscription does not exist"),
            @ApiResponse(code = 405, message = "Not supported for partitioned topics") })
    public void resetCursorOnPosition(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, @PathParam("subName") String subName,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative, MessageIdImpl messageId) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalResetCursorOnPosition(subName, authoritative, messageId);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/position/{messagePosition}")
    @ApiOperation(value = "Peek nth message on a topic subscription.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic, subscription or the message position does not exist") })
    public Response peekNthMessage(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, @PathParam("subName") String subName,
            @PathParam("messagePosition") int messagePosition,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        return internalPeekNthMessage(subName, messagePosition, authoritative);
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/backlog")
    @ApiOperation(value = "Get estimated backlog for offline topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public PersistentOfflineTopicStats getBacklog(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        return internalGetBacklog(authoritative);
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/terminate")
    @ApiOperation(value = "Terminate a topic. A topic that is terminated will not accept any more "
            + "messages to be published and will let consumer to drain existing messages in backlog")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 405, message = "Operation not allowed on non-persistent topic"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public MessageId terminate(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        return internalTerminate(authoritative);
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/compaction")
    @ApiOperation(value = "Trigger a compaction operation on a topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 405, message = "Operation not allowed on persistent topic"),
                            @ApiResponse(code = 404, message = "Topic does not exist"),
                            @ApiResponse(code = 409, message = "Compaction already running")})
    public void compact(@PathParam("tenant") String tenant,
                        @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
                        @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalTriggerCompaction(authoritative);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/compaction")
    @ApiOperation(value = "Get the status of a compaction operation for a topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 405, message = "Operation not allowed on persistent topic"),
                            @ApiResponse(code = 404, message = "Topic does not exist, or compaction hasn't run") })
    public LongRunningProcessStatus compactionStatus(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        return internalCompactionStatus(authoritative);
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/offload")
    @ApiOperation(value = "Offload a prefix of a topic to long term storage")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 405, message = "Operation not allowed on persistent topic"),
                            @ApiResponse(code = 404, message = "Topic does not exist"),
                            @ApiResponse(code = 409, message = "Offload already running")})
    public void triggerOffload(@PathParam("tenant") String tenant,
                               @PathParam("namespace") String namespace,
                               @PathParam("topic") @Encoded String encodedTopic,
                               @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
                               MessageIdImpl messageId) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalTriggerOffload(authoritative, messageId);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/offload")
    @ApiOperation(value = "Offload a prefix of a topic to long term storage")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 405, message = "Operation not allowed on persistent topic"),
                            @ApiResponse(code = 404, message = "Topic does not exist")})
    public OffloadProcessStatus offloadStatus(@PathParam("tenant") String tenant,
                                              @PathParam("namespace") String namespace,
                                              @PathParam("topic") @Encoded String encodedTopic,
                                              @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        return internalOffloadStatus(authoritative);
    }
}
