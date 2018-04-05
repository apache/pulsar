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
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicStats;

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
    @Path("/{property}/{namespace}")
    @ApiOperation(value = "Get the list of topics under a namespace.", response = String.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
    public List<String> getList(@PathParam("property") String property, @PathParam("namespace") String namespace) {
        validateNamespaceName(property, namespace);
        return internalGetList();
    }

    @GET
    @Path("/{property}/{namespace}/partitioned")
    @ApiOperation(value = "Get the list of partitioned topics under a namespace.", response = String.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
    public List<String> getPartitionedTopicList(@PathParam("property") String property,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, namespace);
        return internalGetPartitionedTopicList();
    }

    @GET
    @Path("/{property}/{namespace}/{topic}/permissions")
    @ApiOperation(value = "Get permissions on a topic.", notes = "Retrieve the effective permissions for a topic. These permissions are defined by the permissions set at the"
            + "namespace level combined (union) with any eventual specific permission set on the topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
    public Map<String, Set<AuthAction>> getPermissionsOnTopic(@PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(property, namespace, encodedTopic);
        return internalGetPermissionsOnTopic();
    }

    @POST
    @Path("/{property}/{namespace}/{topic}/permissions/{role}")
    @ApiOperation(value = "Grant a new permission to a role on a single topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void grantPermissionsOnTopic(@PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @PathParam("role") String role, Set<AuthAction> actions) {
        validateTopicName(property, namespace, encodedTopic);
        internalGrantPermissionsOnTopic(role, actions);
    }

    @DELETE
    @Path("/{property}/{namespace}/{topic}/permissions/{role}")
    @ApiOperation(value = "Revoke permissions on a topic.", notes = "Revoke permissions to a role on a single topic. If the permission was not set at the topic"
            + "level, but rather at the namespace level, this operation will return an error (HTTP status code 412).")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Permissions are not set at the topic level") })
    public void revokePermissionsOnTopic(@PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @PathParam("role") String role) {
        validateTopicName(property, namespace, encodedTopic);
        internalRevokePermissionsOnTopic(role);
    }

    @PUT
    @Path("/{property}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Create a partitioned topic.", notes = "It needs to be called before creating a producer on a partitioned topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 409, message = "Partitioned topic already exist") })
    public void createPartitionedTopic(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, int numPartitions,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
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
     * @param property
     * @param cluster
     * @param namespace
     * @param topic
     * @param numPartitions
     */
    @POST
    @Path("/{property}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Increment partitons of an existing partitioned topic.", notes = "It only increments partitions of existing non-global partitioned-topic")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 409, message = "Partitioned topic does not exist") })
    public void updatePartitionedTopic(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, int numPartitions) {
        validateTopicName(property, namespace, encodedTopic);
        internalUpdatePartitionedTopic(numPartitions);
    }

    @GET
    @Path("/{property}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Get partitioned topic metadata.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public PartitionedTopicMetadata getPartitionedMetadata(@PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        return internalGetPartitionedMetadata(authoritative);
    }

    @DELETE
    @Path("/{property}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Delete a partitioned topic.", notes = "It will also delete all the partitions of the topic if it exists.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Partitioned topic does not exist") })
    public void deletePartitionedTopic(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        internalDeletePartitionedTopic(authoritative);
    }

    @PUT
    @Path("/{property}/{namespace}/{topic}/unload")
    @ApiOperation(value = "Unload a topic")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public void unloadTopic(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        internalUnloadTopic(authoritative);
    }

    @DELETE
    @Path("/{property}/{namespace}/{topic}")
    @ApiOperation(value = "Delete a topic.", notes = "The topic cannot be deleted if there's any active subscription or producer connected to the it.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 412, message = "Topic has active producers/subscriptions") })
    public void deleteTopic(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        internalDeleteTopic(authoritative);
    }

    @GET
    @Path("/{property}/{namespace}/{topic}/subscriptions")
    @ApiOperation(value = "Get the list of persistent subscriptions for a given topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public List<String> getSubscriptions(@PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        return internalGetSubscriptions(authoritative);
    }

    @GET
    @Path("{property}/{namespace}/{topic}/stats")
    @ApiOperation(value = "Get the stats for the topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public PersistentTopicStats getStats(@PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        return internalGetStats(authoritative);
    }

    @GET
    @Path("{property}/{namespace}/{topic}/internalStats")
    @ApiOperation(value = "Get the internal stats for the topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public PersistentTopicInternalStats getInternalStats(@PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        return internalGetInternalStats(authoritative);
    }

    @GET
    @Path("{property}/{namespace}/{topic}/internal-info")
    @ApiOperation(value = "Get the internal stats for the topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public void getManagedLedgerInfo(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, @Suspended AsyncResponse asyncResponse) {
        validateTopicName(property, namespace, encodedTopic);
        internalGetManagedLedgerInfo(asyncResponse);
    }

    @GET
    @Path("{property}/{namespace}/{topic}/partitioned-stats")
    @ApiOperation(value = "Get the stats for the partitioned topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public PartitionedTopicStats getPartitionedStats(@PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        return internalGetPartitionedStats(authoritative);
    }

    @DELETE
    @Path("/{property}/{namespace}/{topic}/subscription/{subName}")
    @ApiOperation(value = "Delete a subscription.", notes = "There should not be any active consumers on the subscription.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 412, message = "Subscription has active consumers") })
    public void deleteSubscription(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, @PathParam("subName") String subName,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        internalDeleteSubscription(subName, authoritative);
    }

    @POST
    @Path("/{property}/{namespace}/{topic}/subscription/{subName}/skip_all")
    @ApiOperation(value = "Skip all messages on a topic subscription.", notes = "Completely clears the backlog on the subscription.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 405, message = "Operation not allowed on non-persistent topic"),
            @ApiResponse(code = 404, message = "Topic or subscription does not exist") })
    public void skipAllMessages(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, @PathParam("subName") String subName,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        internalSkipAllMessages(subName, authoritative);
    }

    @POST
    @Path("/{property}/{namespace}/{topic}/subscription/{subName}/skip/{numMessages}")
    @ApiOperation(value = "Skip messages on a topic subscription.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic or subscription does not exist") })
    public void skipMessages(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, @PathParam("subName") String subName,
            @PathParam("numMessages") int numMessages,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        internalSkipMessages(subName, numMessages, authoritative);
    }

    @POST
    @Path("/{property}/{namespace}/{topic}/subscription/{subName}/expireMessages/{expireTimeInSeconds}")
    @ApiOperation(value = "Expire messages on a topic subscription.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic or subscription does not exist") })
    public void expireTopicMessages(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, @PathParam("subName") String subName,
            @PathParam("expireTimeInSeconds") int expireTimeInSeconds,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        internalExpireMessages(subName, expireTimeInSeconds, authoritative);
    }

    @POST
    @Path("/{property}/{namespace}/{topic}/all_subscription/expireMessages/{expireTimeInSeconds}")
    @ApiOperation(value = "Expire messages on all subscriptions of topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic or subscription does not exist") })
    public void expireMessagesForAllSubscriptions(@PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @PathParam("expireTimeInSeconds") int expireTimeInSeconds,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        internalExpireMessagesForAllSubscriptions(expireTimeInSeconds, authoritative);
    }

    @POST
    @Path("/{property}/{namespace}/{topic}/subscription/{subName}/resetcursor/{timestamp}")
    @ApiOperation(value = "Reset subscription to message position closest to absolute timestamp (in ms).", notes = "It fence cursor and disconnects all active consumers before reseting cursor.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic/Subscription does not exist") })
    public void resetCursor(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, @PathParam("subName") String subName,
            @PathParam("timestamp") long timestamp,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        internalResetCursor(subName, timestamp, authoritative);
    }

    @POST
    @Path("/{property}/{namespace}/{topic}/subscription/{subName}/resetcursor")
    @ApiOperation(value = "Reset subscription to message position closest to given position.", notes = "It fence cursor and disconnects all active consumers before reseting cursor.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic/Subscription does not exist"),
            @ApiResponse(code = 405, message = "Not supported for partitioned topics") })
    public void resetCursorOnPosition(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, @PathParam("subName") String subName,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative, MessageIdImpl messageId) {
        validateTopicName(property, namespace, encodedTopic);
        internalResetCursorOnPosition(subName, authoritative, messageId);
    }

    @GET
    @Path("/{property}/{namespace}/{topic}/subscription/{subName}/position/{messagePosition}")
    @ApiOperation(value = "Peek nth message on a topic subscription.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic, subscription or the message position does not exist") })
    public Response peekNthMessage(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, @PathParam("subName") String subName,
            @PathParam("messagePosition") int messagePosition,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        return internalPeekNthMessage(subName, messagePosition, authoritative);
    }

    @GET
    @Path("{property}/{namespace}/{topic}/backlog")
    @ApiOperation(value = "Get estimated backlog for offline topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public PersistentOfflineTopicStats getBacklog(@PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        return internalGetBacklog(authoritative);
    }

    @POST
    @Path("/{property}/{namespace}/{topic}/terminate")
    @ApiOperation(value = "Terminate a topic. A topic that is terminated will not accept any more "
            + "messages to be published and will let consumer to drain existing messages in backlog")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 405, message = "Operation not allowed on non-persistent topic"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public MessageId terminate(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        return internalTerminate(authoritative);
    }

    @PUT
    @Path("/{property}/{namespace}/{topic}/compaction")
    @ApiOperation(value = "Trigger a compaction operation on a topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 405, message = "Operation not allowed on persistent topic"),
                            @ApiResponse(code = 404, message = "Topic does not exist"),
                            @ApiResponse(code = 409, message = "Compaction already running")})
    public void compact(@PathParam("property") String property,
                        @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
                        @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        internalTriggerCompaction(authoritative);
    }
}
