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
package org.apache.pulsar.broker.admin.v1;

import static org.apache.pulsar.common.util.Codec.decode;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.Optional;
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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pulsar.broker.admin.impl.PersistentTopicsBase;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.ResetCursorData;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
@Path("/persistent")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/persistent", description = "Persistent topic admin apis", tags = "persistent topic", hidden = true)
@SuppressWarnings("deprecation")
public class PersistentTopics extends PersistentTopicsBase {
    private static final Logger log = LoggerFactory.getLogger(PersistentTopics.class);
    @GET
    @Path("/{property}/{cluster}/{namespace}")
    @ApiOperation(hidden = true, value = "Get the list of topics under a namespace.",
            response = String.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "Don't have admin or operate permission on the namespace"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist")})
    public void getList(@Suspended final AsyncResponse asyncResponse, @PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify the bundle name", required = false)
            @QueryParam("bundle") String bundle) {
        validateNamespaceName(property, cluster, namespace);
        internalGetListAsync(Optional.ofNullable(bundle))
            .thenAccept(asyncResponse::resume)
            .exceptionally(ex -> {
                if (!isRedirectException(ex)) {
                    log.error("[{}] Failed to get topic list {}", clientAppId(), namespaceName, ex);
                }
                resumeAsyncResponseExceptionally(asyncResponse, ex);
                return null;
            });
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/partitioned")
    @ApiOperation(hidden = true, value = "Get the list of partitioned topics under a namespace.",
            response = String.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "Don't have admin or operate permission on the namespace"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist")})
    public void getPartitionedTopicList(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("property") String property,
            @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        internalGetPartitionedTopicListAsync()
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to get partitioned topic list {}", clientAppId(), namespaceName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/{topic}/permissions")
    @ApiOperation(hidden = true, value = "Get permissions on a topic.",
            notes = "Retrieve the effective permissions for a topic."
                    + " These permissions are defined by the permissions set at the"
                    + "namespace level combined (union) with any eventual specific permission set on the topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist")})
    public void getPermissionsOnTopic(@Suspended AsyncResponse asyncResponse,
                                                              @PathParam("property") String property,
                                                              @PathParam("cluster") String cluster,
                                                              @PathParam("namespace") String namespace,
                                                              @PathParam("topic") @Encoded String encodedTopic) {
        try {
            validateTopicName(property, cluster, namespace, encodedTopic);
            internalGetPermissionsOnTopic().thenAccept(permissions -> asyncResponse.resume(permissions))
                    .exceptionally(ex -> {
                        log.error("[{}] Failed to get permissions for topic {}", clientAppId(), topicName, ex);
                        resumeAsyncResponseExceptionally(asyncResponse, ex);
                        return null;
                    });
        } catch (Exception e) {
            log.error("[{}] Failed to validate topic name {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{topic}/permissions/{role}")
    @ApiOperation(hidden = true, value = "Grant a new permission to a role on a single topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void grantPermissionsOnTopic(@Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @PathParam("role") String role,
            Set<AuthAction> actions) {

        try {
            validateTopicName(property, cluster, namespace, encodedTopic);
            internalGrantPermissionsOnTopic(asyncResponse, role, actions);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/{topic}/permissions/{role}")
    @ApiOperation(hidden = true, value = "Revoke permissions on a topic.",
            notes = "Revoke permissions to a role on a single topic. If the permission was not set at the topic"
                    + "level, but rather at the namespace level,"
                    + " this operation will return an error (HTTP status code 412).")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Permissions are not set at the topic level")})
    public void revokePermissionsOnTopic(@Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @PathParam("role") String role) {
        try {
            validateTopicName(property, cluster, namespace, encodedTopic);
            internalRevokePermissionsOnTopic(asyncResponse, role);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @PUT
    @Path("/{property}/{cluster}/{namespace}/{topic}/partitions")
    @ApiOperation(hidden = true, value = "Create a partitioned topic.",
            notes = "It needs to be called before creating a producer on a partitioned topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 406, message = "The number of partitions should be "
                    + "more than 0 and less than or equal to maxNumPartitionsPerPartitionedTopic"),
            @ApiResponse(code = 409, message = "Partitioned topic already exist")})
    public void createPartitionedTopic(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property,
            @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            int numPartitions,
            @QueryParam("createLocalTopicOnly") @DefaultValue("false") boolean createLocalTopicOnly) {
        try {
            validateTopicName(property, cluster, namespace, encodedTopic);
            internalCreatePartitionedTopic(asyncResponse, numPartitions, createLocalTopicOnly);
        } catch (Exception e) {
            log.error("[{}] Failed to create partitioned topic {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    @PUT
    @Path("/{tenant}/{cluster}/{namespace}/{topic}")
    @ApiOperation(value = "Create a non-partitioned topic.",
            notes = "This is the only REST endpoint from which non-partitioned topics could be created.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Partitioned topic already exist"),
            @ApiResponse(code = 412,
                    message = "Failed Reason : Name is invalid or Namespace does not have any clusters configured"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")
    })
    public void createNonPartitionedTopic(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the cluster", required = true)
            @PathParam("cluster") String cluster,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, cluster, namespace);
        validateTopicName(tenant, cluster, namespace, encodedTopic);
        validateGlobalNamespaceOwnership();
        validateCreateTopic(topicName);
        internalCreateNonPartitionedTopicAsync(authoritative, null)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to create non-partitioned topic {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
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
     * @param numPartitions
     */
    @POST
    @Path("/{property}/{cluster}/{namespace}/{topic}/partitions")
    @ApiOperation(hidden = true, value = "Increment partitions of an existing partitioned topic.",
            notes = "It only increments partitions of existing non-global partitioned-topic")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 406, message = "The number of partitions should be more than 0"
                    + " and less than or equal to maxNumPartitionsPerPartitionedTopic"
                    + " and number of new partitions must be greater than existing number of partitions"),
            @ApiResponse(code = 409, message = "Partitioned topic does not exist")})
    public void updatePartitionedTopic(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("updateLocalTopicOnly") @DefaultValue("false") boolean updateLocalTopicOnly,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("force") @DefaultValue("false") boolean force,
            int numPartitions) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        internalUpdatePartitionedTopicAsync(numPartitions, updateLocalTopicOnly, authoritative, force)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to update partitioned topic {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/{topic}/partitions")
    @ApiOperation(hidden = true, value = "Get partitioned topic metadata.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist")})
    public void getPartitionedMetadata(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("checkAllowAutoCreation") @DefaultValue("false") boolean checkAllowAutoCreation) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        internalGetPartitionedMetadataAsync(authoritative, checkAllowAutoCreation)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to get partitioned metadata topic {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/{topic}/partitions")
    @ApiOperation(hidden = true, value = "Delete a partitioned topic.",
            notes = "It will also delete all the partitions of the topic if it exists.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or partitioned topic does not exist")})
    public void deletePartitionedTopic(@Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("force") @DefaultValue("false") boolean force,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(property, cluster, namespace, encodedTopic);
            internalDeletePartitionedTopic(asyncResponse, authoritative, force);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @PUT
    @Path("/{property}/{cluster}/{namespace}/{topic}/unload")
    @ApiOperation(hidden = true, value = "Unload a topic")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist") })
    public void unloadTopic(@Suspended final AsyncResponse asyncResponse, @PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        internalUnloadTopic(asyncResponse, authoritative);
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/{topic}")
    @ApiOperation(hidden = true, value = "Delete a topic.",
            notes = "The topic cannot be deleted if delete is not forcefully and there's any active "
                    + "subscription or producer connected to the it. "
                    + "Force delete ignores connected clients and deletes topic by explicitly closing them.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 412, message = "Topic has active producers/subscriptions")})
    public void deleteTopic(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("force") @DefaultValue("false") boolean force,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        internalDeleteTopicAsync(authoritative, force)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    Throwable t = FutureUtil.unwrapCompletionException(ex);
                    if (!force && (t instanceof BrokerServiceException.TopicBusyException)) {
                        ex = new RestException(Response.Status.PRECONDITION_FAILED,
                                t.getMessage());
                    }
                    if (isManagedLedgerNotFoundException(t)) {
                        ex = new RestException(Response.Status.NOT_FOUND,
                                getTopicNotFoundErrorMessage(topicName.toString()));
                    } else if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to delete topic {}", clientAppId(), topicName, t);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/{topic}/subscriptions")
    @ApiOperation(hidden = true, value = "Get the list of persistent subscriptions for a given topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist") })
    public void getSubscriptions(@Suspended final AsyncResponse asyncResponse, @PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(property, cluster, namespace, encodedTopic);
            internalGetSubscriptions(asyncResponse, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("{property}/{cluster}/{namespace}/{topic}/stats")
    @ApiOperation(hidden = true, value = "Get the stats for the topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist")})
    public void getStats(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("getPreciseBacklog") @DefaultValue("false") boolean getPreciseBacklog) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        internalGetStatsAsync(authoritative, getPreciseBacklog, false, false)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to get stats for {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("{property}/{cluster}/{namespace}/{topic}/internalStats")
    @ApiOperation(hidden = true, value = "Get the internal stats for the topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist")})
    public void getInternalStats(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("metadata") @DefaultValue("false") boolean metadata) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        internalGetInternalStatsAsync(authoritative, metadata)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to get internal stats for topic {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("{property}/{cluster}/{namespace}/{topic}/internal-info")
    @ApiOperation(hidden = true, value = "Get the stored topic metadata.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist")})
    public void getManagedLedgerInfo(@PathParam("property") String property, @PathParam("cluster") String cluster,
                                     @PathParam("namespace") String namespace, @PathParam("topic") @Encoded
                                                 String encodedTopic,
                                     @Suspended AsyncResponse asyncResponse,
                                     @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        internalGetManagedLedgerInfo(asyncResponse, authoritative);
    }

    @GET
    @Path("{property}/{cluster}/{namespace}/{topic}/partitioned-stats")
    @ApiOperation(hidden = true, value = "Get the stats for the partitioned topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist")})
    public void getPartitionedStats(@Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("perPartition") @DefaultValue("true") boolean perPartition,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(property, cluster, namespace, encodedTopic);
            internalGetPartitionedStats(asyncResponse, authoritative, perPartition, false, false, false);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }


    @GET
    @Path("{property}/{cluster}/{namespace}/{topic}/partitioned-internalStats")
    @ApiOperation(hidden = true, value = "Get the stats-internal for the partitioned topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist")})
    public void getPartitionedStatsInternal(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(property, cluster, namespace, encodedTopic);
            internalGetPartitionedStatsInternal(asyncResponse, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/{topic}/subscription/{subName}")
    @ApiOperation(hidden = true, value = "Delete a subscription.",
            notes = "The subscription cannot be deleted if delete is not forcefully"
                    + " and there are any active consumers attached to it. "
                    + "Force delete ignores connected consumers and deletes subscription by explicitly closing them.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic or subscription does not exist"),
            @ApiResponse(code = 412, message = "Subscription has active consumers")})
    public void deleteSubscription(@Suspended final AsyncResponse asyncResponse, @PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, @PathParam("subName") String encodedSubName,
            @QueryParam("force") @DefaultValue("false") boolean force,
                                   @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        String subName = decode(encodedSubName);
        internalDeleteSubscriptionAsync(subName, authoritative, force)
                .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    Throwable cause = FutureUtil.unwrapCompletionException(ex);

                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(cause)) {
                        log.error("[{}] Failed to delete subscription {} from topic {}", clientAppId(), subName,
                                topicName, cause);
                    }

                    if (cause instanceof BrokerServiceException.SubscriptionBusyException) {
                        resumeAsyncResponseExceptionally(asyncResponse,
                                new RestException(Response.Status.PRECONDITION_FAILED,
                                        "Subscription has active connected consumers"));
                    } else {
                        resumeAsyncResponseExceptionally(asyncResponse, cause);
                    }

                    return null;
                });
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{topic}/subscription/{subName}/skip_all")
    @ApiOperation(hidden = true, value = "Skip all messages on a topic subscription.",
            notes = "Completely clears the backlog on the subscription.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 405, message = "Operation not allowed on non-persistent topic"),
            @ApiResponse(code = 404, message = "Namespace or topic or subscription does not exist")})
    public void skipAllMessages(@Suspended final AsyncResponse asyncResponse, @PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, @PathParam("subName") String encodedSubName,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(property, cluster, namespace, encodedTopic);
            internalSkipAllMessages(asyncResponse, decode(encodedSubName), authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{topic}/subscription/{subName}/skip/{numMessages}")
    @ApiOperation(hidden = true, value = "Skip messages on a topic subscription.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namesapce or topic or subscription does not exist") })
    public void skipMessages(@Suspended final AsyncResponse asyncResponse, @PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, @PathParam("subName") String encodedSubName,
            @PathParam("numMessages") int numMessages,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(property, cluster, namespace, encodedTopic);
            internalSkipMessages(asyncResponse, decode(encodedSubName), numMessages, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{topic}/subscription/{subName}/expireMessages/{expireTimeInSeconds}")
    @ApiOperation(hidden = true, value = "Expire messages on a topic subscription.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic or subscription does not exist") })
    public void expireTopicMessages(@Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @PathParam("subName") String encodedSubName, @PathParam("expireTimeInSeconds") int expireTimeInSeconds,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(property, cluster, namespace, encodedTopic);
            internalExpireMessagesByTimestamp(asyncResponse, decode(encodedSubName),
                    expireTimeInSeconds, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{topic}/subscription/{subName}/expireMessages")
    @ApiOperation(value = "Expiry messages on a topic subscription.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or"
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic or subscription does not exist"),
            @ApiResponse(code = 405, message = "Expiry messages on a non-persistent topic is not allowed"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void expireTopicMessages(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property, @PathParam("cluster") String cluster,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Subscription to be Expiry messages on")
            @PathParam("subName") String encodedSubName,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(name = "messageId", value = "messageId to reset back to (ledgerId:entryId)")
                    ResetCursorData resetCursorData) {
        try {
            validateTopicName(property, cluster, namespace, encodedTopic);
            internalExpireMessagesByPosition(asyncResponse, decode(encodedSubName), authoritative,
                    new MessageIdImpl(resetCursorData.getLedgerId(),
                            resetCursorData.getEntryId(), resetCursorData.getPartitionIndex())
                    , resetCursorData.isExcluded(), resetCursorData.getBatchIndex());
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{topic}/all_subscription/expireMessages/{expireTimeInSeconds}")
    @ApiOperation(hidden = true, value = "Expire messages on all subscriptions of topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic or subscription does not exist") })
    public void expireMessagesForAllSubscriptions(@Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @PathParam("expireTimeInSeconds") int expireTimeInSeconds,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(property, cluster, namespace, encodedTopic);
            internalExpireMessagesForAllSubscriptions(asyncResponse, expireTimeInSeconds, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{topic}/subscription/{subName}/resetcursor/{timestamp}")
    @ApiOperation(hidden = true,
            value = "Reset subscription to message position closest to absolute timestamp (in ms).",
            notes = "It fence cursor and disconnects all active consumers before resetting cursor.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic or subscription does not exist")})
    public void resetCursor(@Suspended final AsyncResponse asyncResponse, @PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, @PathParam("subName") String encodedSubName,
            @PathParam("timestamp") long timestamp,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        internalResetCursorAsync(decode(encodedSubName), timestamp, authoritative)
            .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                Throwable t = FutureUtil.unwrapCompletionException(ex);
                if (!isRedirectException(t)) {
                    log.error("[{}][{}] Failed to reset cursor on subscription {} to time {}",
                        clientAppId(), topicName, encodedSubName, timestamp, t);
                }
                if (t instanceof BrokerServiceException.SubscriptionInvalidCursorPosition) {
                    t = new RestException(Response.Status.PRECONDITION_FAILED,
                        "Unable to find position for timestamp specified: " + t.getMessage());
                } else if (t instanceof BrokerServiceException.SubscriptionBusyException) {
                    t = new RestException(Response.Status.PRECONDITION_FAILED,
                        "Failed for Subscription Busy: " + t.getMessage());
                }
                resumeAsyncResponseExceptionally(asyncResponse, t);
                return null;
            });
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{topic}/subscription/{subName}/resetcursor")
    @ApiOperation(hidden = true, value = "Reset subscription to message position closest to given position.",
            notes = "It fence cursor and disconnects all active consumers before resetting cursor.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic or subscription does not exist"),
            @ApiResponse(code = 405, message = "Not supported for partitioned topics")})
    public void resetCursorOnPosition(@Suspended final AsyncResponse asyncResponse,
                                      @PathParam("property") String property, @PathParam("cluster") String cluster,
                                      @PathParam("namespace") String namespace, @PathParam("topic") @Encoded
                                                  String encodedTopic,
                                      @PathParam("subName") String encodedSubName,
                                      @QueryParam("authoritative") @DefaultValue("false")
                                                  boolean authoritative, ResetCursorData resetCursorData) {
        try {
            validateTopicName(property, cluster, namespace, encodedTopic);
            internalResetCursorOnPosition(asyncResponse, decode(encodedSubName), authoritative,
                    new MessageIdImpl(resetCursorData.getLedgerId(),
                            resetCursorData.getEntryId(), resetCursorData.getPartitionIndex())
                    , resetCursorData.isExcluded(), resetCursorData.getBatchIndex());
        } catch (Exception e) {
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    @PUT
    @Path("/{property}/{cluster}/{namespace}/{topic}/subscription/{subscriptionName}")
    @ApiOperation(value = "Create a subscription on the topic.",
            notes = "Creates a subscription on the topic at the specified message id")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 400, message = "Create subscription on non persistent topic is not supported"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic or subscription does not exist"),
            @ApiResponse(code = 405, message = "Not supported for partitioned topics")})
    public void createSubscription(@Suspended final AsyncResponse asyncResponse, @PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String topic, @PathParam("subscriptionName") String encodedSubName,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative, MessageIdImpl messageId,
            @QueryParam("replicated") boolean replicated) {
        try {
            validateTopicName(property, cluster, namespace, topic);
            if (!topicName.isPersistent()) {
                throw new RestException(Response.Status.BAD_REQUEST, "Create subscription on non-persistent topic "
                        + "can only be done through client");
            }
            internalCreateSubscription(asyncResponse, decode(encodedSubName), messageId, authoritative, replicated,
                    null);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/{topic}/subscription/{subName}/position/{messagePosition}")
    @ApiOperation(hidden = true, value = "Peek nth message on a topic subscription.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic or subscription or the message position does not"
                    + " exist") })
    public void peekNthMessage(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @PathParam("subName") String encodedSubName, @PathParam("messagePosition") int messagePosition,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        internalPeekNthMessageAsync(decode(encodedSubName), messagePosition, authoritative)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to get peek nth message for topic {} subscription {}", clientAppId(),
                                topicName, decode(encodedSubName), ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/{topic}/ledger/{ledgerId}/entry/{entryId}")
    @ApiOperation(hidden = true, value = "Get message by its messageId.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't java admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic or subscription or the messageId does not exist")
    })
    public void getMessageByID(@Suspended final AsyncResponse asyncResponse, @PathParam("property") String property,
                               @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
                               @PathParam("topic") @Encoded String encodedTopic, @PathParam("ledgerId") Long ledgerId,
                               @PathParam("entryId") Long entryId,
                               @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        internalGetMessageById(ledgerId, entryId, authoritative)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to get message with ledgerId {} entryId {} from {}",
                                clientAppId(), ledgerId, entryId, topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
        });
    }

    @GET
    @Path("{property}/{cluster}/{namespace}/{topic}/backlog")
    @ApiOperation(hidden = true, value = "Get estimated backlog for offline topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist")})
    public void getBacklog(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property,
            @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        internalGetBacklogAsync(authoritative)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    Throwable t = FutureUtil.unwrapCompletionException(ex);
                    if (t instanceof MetadataStoreException.NotFoundException) {
                        log.warn("[{}] Failed to get topic backlog {}: Namespace does not exist", clientAppId(),
                                namespaceName);
                        ex = new RestException(Response.Status.NOT_FOUND, "Namespace does not exist");
                    } else if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to get estimated backlog for topic {}", clientAppId(), encodedTopic, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{topic}/terminate")
    @ApiOperation(hidden = true, value = "Terminate a topic. A topic that is terminated will not accept any more "
            + "messages to be published and will let consumer to drain existing messages in backlog")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 405, message = "Operation not allowed on non-persistent topic"),
            @ApiResponse(code = 406, message = "Need to provide a persistent topic name"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist") })
    public void terminate(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property,
            @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validatePersistentTopicName(property, cluster, namespace, encodedTopic);
        internalTerminateAsync(authoritative)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to terminated topic {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{topic}/terminate/partitions")
    @ApiOperation(hidden = true,
            value = "Terminate all partitioned topic. A topic that is terminated will not accept any more "
                    + "messages to be published and will let consumer to drain existing messages in backlog")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 405, message = "Operation not allowed on non-persistent topic"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist")})
    public void terminatePartitionedTopic(@Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        internalTerminatePartitionedTopic(asyncResponse, authoritative);
    }

    @PUT
    @Path("/{property}/{cluster}/{namespace}/{topic}/compaction")
    @ApiOperation(value = "Trigger a compaction operation on a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 405, message = "Operation not allowed on persistent topic"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 409, message = "Compaction already running")})
    public void compact(@Suspended final AsyncResponse asyncResponse,
                        @PathParam("property") String property, @PathParam("cluster") String cluster,
                        @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
                        @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(property, cluster, namespace, encodedTopic);
            internalTriggerCompaction(asyncResponse, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/{topic}/compaction")
    @ApiOperation(value = "Get the status of a compaction operation for a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 405, message = "Operation not allowed on persistent topic"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist, or compaction hasn't run") })
    public void compactionStatus(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        internalCompactionStatusAsync(authoritative)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to get the status of a compaction operation for the topic {}",
                                clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{cluster}/{namespace}/{topic}/offload")
    @ApiOperation(value = "Offload a prefix of a topic to long term storage")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 405, message = "Operation not allowed on persistent topic"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 409, message = "Offload already running")})
    public void triggerOffload(@Suspended final AsyncResponse asyncResponse,
                               @PathParam("tenant") String tenant,
                               @PathParam("cluster") String cluster,
                               @PathParam("namespace") String namespace,
                               @PathParam("topic") @Encoded String encodedTopic,
                               @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
                               MessageIdImpl messageId) {
        try {
            validateTopicName(tenant, cluster, namespace, encodedTopic);
            internalTriggerOffload(asyncResponse, authoritative, messageId);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("/{tenant}/{cluster}/{namespace}/{topic}/offload")
    @ApiOperation(value = "Offload a prefix of a topic to long term storage")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 405, message = "Operation not allowed on persistent topic"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist")})
    public void offloadStatus(@Suspended final AsyncResponse asyncResponse,
                              @PathParam("tenant") String tenant,
                              @PathParam("cluster") String cluster,
                              @PathParam("namespace") String namespace,
                              @PathParam("topic") @Encoded String encodedTopic,
                              @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, cluster, namespace, encodedTopic);
            internalOffloadStatus(asyncResponse, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("/{tenant}/{cluster}/{namespace}/{topic}/lastMessageId")
    @ApiOperation(value = "Return the last commit message id of topic")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or"
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 405, message = "Operation is not allowed on the persistent topic"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void getLastMessageId(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the cluster", required = true)
            @PathParam("cluster") String cluster,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, cluster, namespace, encodedTopic);
            internalGetLastMessageId(asyncResponse, authoritative);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{cluster}/{namespace}/{topic}/subscription/{subName}/replicatedSubscriptionStatus")
    @ApiOperation(value = "Enable or disable a replicated subscription on a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or "
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic or subscription does not exist"),
            @ApiResponse(code = 405, message = "Operation not allowed on this topic"),
            @ApiResponse(code = 412, message = "Can't find owner for topic"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void setReplicatedSubscriptionStatus(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the cluster", required = true)
            @PathParam("cluster") String cluster,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Name of subscription", required = true)
            @PathParam("subName") String encodedSubName,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Whether to enable replicated subscription", required = true)
            boolean enabled) {
        try {
            validateTopicName(tenant, cluster, namespace, encodedTopic);
            internalSetReplicatedSubscriptionStatus(asyncResponse, decode(encodedSubName), authoritative, enabled);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("/{tenant}/{cluster}/{namespace}/{topic}/subscription/{subName}/replicatedSubscriptionStatus")
    @ApiOperation(value = "Get replicated subscription status on a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 412, message = "Can't find owner for topic"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void getReplicatedSubscriptionStatus(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the cluster", required = true)
            @PathParam("cluster") String cluster,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Name of subscription", required = true)
            @PathParam("subName") String encodedSubName,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, cluster, namespace, encodedTopic);
        internalGetReplicatedSubscriptionStatus(asyncResponse, decode(encodedSubName), authoritative);
    }

}
