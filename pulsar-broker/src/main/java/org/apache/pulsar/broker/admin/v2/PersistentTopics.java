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
package org.apache.pulsar.broker.admin.v2;

import static org.apache.pulsar.common.util.Codec.decode;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.List;
import java.util.Map;
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
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.admin.impl.PersistentTopicsBase;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.OffloadProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.ResetCursorData;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.naming.PartitionedManagedLedgerInfo;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.EntryFilters;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.PartitionedTopicInternalStats;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicStats;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.impl.AutoSubscriptionCreationOverrideImpl;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;
import org.apache.pulsar.common.policies.data.stats.PartitionedTopicStatsImpl;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
@Path("/persistent")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/persistent", description = "Persistent topic admin apis", tags = "persistent topic")
public class PersistentTopics extends PersistentTopicsBase {

    @GET
    @Path("/{tenant}/{namespace}")
    @ApiOperation(value = "Get the list of topics under a namespace.",
            response = String.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin or operate permission on the namespace"),
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void getList(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify the bundle name", required = false)
            @QueryParam("bundle") String bundle,
            @ApiParam(value = "Include system topic")
            @QueryParam("includeSystemTopic") boolean includeSystemTopic) {
        validateNamespaceName(tenant, namespace);
        internalGetListAsync(Optional.ofNullable(bundle))
            .thenAccept(topicList -> asyncResponse.resume(filterSystemTopic(topicList, includeSystemTopic)))
            .exceptionally(ex -> {
                if (isNot307And404Exception(ex)) {
                    log.error("[{}] Failed to get topic list {}", clientAppId(), namespaceName, ex);
                }
                resumeAsyncResponseExceptionally(asyncResponse, ex);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/partitioned")
    @ApiOperation(value = "Get the list of partitioned topics under a namespace.",
            response = String.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin or operate permission on the namespace"),
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void getPartitionedTopicList(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Include system topic")
            @QueryParam("includeSystemTopic") boolean includeSystemTopic) {
        validateNamespaceName(tenant, namespace);
        internalGetPartitionedTopicListAsync()
                .thenAccept(partitionedTopicList -> asyncResponse.resume(
                        filterSystemTopic(partitionedTopicList, includeSystemTopic)))
                .exceptionally(ex -> {
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to get partitioned topic list {}", clientAppId(), namespaceName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/permissions")
    @ApiOperation(value = "Get permissions on a topic.",
            notes = "Retrieve the effective permissions for a topic."
                    + " These permissions are defined by the permissions set at the"
                    + "namespace level combined (union) with any eventual specific permission set on the topic."
                    + "Returns a nested map structure which Swagger does not fully support for display. "
                    + "Structure: Map<String, Set<AuthAction>>. Please refer to this structure for details.",
            response = AuthAction.class, responseContainer = "Map")
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void getPermissionsOnTopic(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
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
    @Path("/{tenant}/{namespace}/{topic}/permissions/{role}")
    @ApiOperation(value = "Grant a new permission to a role on a single topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error") })
    public void grantPermissionsOnTopic(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Client role to which grant permissions", required = true)
            @PathParam("role") String role,
            @ApiParam(value = "Actions to be granted (produce,functions,consume)",
                    allowableValues = "produce,functions,consume")
                    Set<AuthAction> actions) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalGrantPermissionsOnTopic(asyncResponse, role, actions);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/permissions/{role}")
    @ApiOperation(value = "Revoke permissions on a topic.",
            notes = "Revoke permissions to a role on a single topic. If the permission was not set at the topic"
                    + "level, but rather at the namespace level,"
                    + " this operation will return an error (HTTP status code 412).")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 412, message = "Permissions are not set at the topic level"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void revokePermissionsOnTopic(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Client role to which grant permissions", required = true)
            @PathParam("role") String role) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalRevokePermissionsOnTopic(asyncResponse, role);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Create a partitioned topic.",
            notes = "It needs to be called before creating a producer on a partitioned topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or namespace doesn't exist"),
            @ApiResponse(code = 406, message = "The number of partitions should be more than 0 and"
                    + " less than or equal to maxNumPartitionsPerPartitionedTopic"),
            @ApiResponse(code = 409, message = "Partitioned topic already exist"),
            @ApiResponse(code = 412,
                    message = "Failed Reason : Name is invalid or Namespace does not have any clusters configured"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")
    })
    public void createPartitionedTopic(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "The number of partitions for the topic",
                    required = true, type = "int", defaultValue = "0")
                    int numPartitions,
            @QueryParam("createLocalTopicOnly") @DefaultValue("false") boolean createLocalTopicOnly) {
        try {
            validateNamespaceName(tenant, namespace);
            validateGlobalNamespaceOwnership();
            validatePartitionedTopicName(tenant, namespace, encodedTopic);
            validateTopicPolicyOperation(topicName, PolicyName.PARTITION, PolicyOperation.WRITE);
            validateCreateTopic(topicName);
            internalCreatePartitionedTopic(asyncResponse, numPartitions, createLocalTopicOnly);
        } catch (Exception e) {
            log.error("[{}] Failed to create partitioned topic {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}")
    @ApiOperation(value = "Create a non-partitioned topic.",
            notes = "This is the only REST endpoint from which non-partitioned topics could be created.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 404, message = "Tenant or namespace doesn't exist"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
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
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Key value pair properties for the topic metadata")
            Map<String, String> properties) {
        validateNamespaceName(tenant, namespace);
        validateGlobalNamespaceOwnership();
        validateTopicName(tenant, namespace, encodedTopic);
        validateCreateTopic(topicName);
        internalCreateNonPartitionedTopicAsync(authoritative, properties)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    if (isNot307And404Exception(ex) && !isConflictException(ex)) {
                        log.error("[{}] Failed to create non-partitioned topic {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/offloadPolicies")
    @ApiOperation(value = "Get offload policies on a topic.", response = OffloadPoliciesImpl.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(code = 500, message = "Internal server error"), })
    public void getOffloadPolicies(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.OFFLOAD, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetOffloadPolicies(applied, isGlobal))
            .thenApply(asyncResponse::resume)
            .exceptionally(ex -> {
                handleTopicPolicyException("getOffloadPolicies", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/offloadPolicies")
    @ApiOperation(value = "Set offload policies on a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"), })
    public void setOffloadPolicies(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Offload policies for the specified topic") OffloadPoliciesImpl offloadPolicies) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.OFFLOAD, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetOffloadPolicies(offloadPolicies, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("setOffloadPolicies", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/offloadPolicies")
    @ApiOperation(value = "Delete offload policies on a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"), })
    public void removeOffloadPolicies(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.OFFLOAD, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetOffloadPolicies(null, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("removeOffloadPolicies", ex, asyncResponse);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/maxUnackedMessagesOnConsumer")
    @ApiOperation(value = "Get max unacked messages per consumer config on a topic.", response = Integer.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(code = 500, message = "Internal server error"), })
    public void getMaxUnackedMessagesOnConsumer(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.MAX_UNACKED, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetMaxUnackedMessagesOnConsumer(applied, isGlobal))
            .thenApply(asyncResponse::resume).exceptionally(ex -> {
                handleTopicPolicyException("getMaxUnackedMessagesOnConsumer", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/maxUnackedMessagesOnConsumer")
    @ApiOperation(value = "Set max unacked messages per consumer config on a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"), })
    public void setMaxUnackedMessagesOnConsumer(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Max unacked messages on consumer policies for the specified topic")
                    Integer maxUnackedNum) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.MAX_UNACKED, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetMaxUnackedMessagesOnConsumer(maxUnackedNum, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("setMaxUnackedMessagesOnConsumer", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/maxUnackedMessagesOnConsumer")
    @ApiOperation(value = "Delete max unacked messages per consumer config on a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"), })
    public void deleteMaxUnackedMessagesOnConsumer(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.MAX_UNACKED, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetMaxUnackedMessagesOnConsumer(null, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("deleteMaxUnackedMessagesOnConsumer", ex, asyncResponse);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/deduplicationSnapshotInterval")
    @ApiOperation(value = "Get deduplicationSnapshotInterval config on a topic.", response = Integer.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(code = 500, message = "Internal server error"), })
    public void getDeduplicationSnapshotInterval(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.DEDUPLICATION_SNAPSHOT, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> getTopicPoliciesAsyncWithRetry(topicName, isGlobal))
            .thenAccept(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                asyncResponse.resume(topicPolicies.getDeduplicationSnapshotIntervalSeconds());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("getDeduplicationSnapshotInterval", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/deduplicationSnapshotInterval")
    @ApiOperation(value = "Set deduplicationSnapshotInterval config on a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"), })
    public void setDeduplicationSnapshotInterval(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Interval to take deduplication snapshot for the specified topic")
                    Integer interval,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.DEDUPLICATION_SNAPSHOT, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetDeduplicationSnapshotInterval(interval, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("setDeduplicationSnapshotInterval", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/deduplicationSnapshotInterval")
    @ApiOperation(value = "Delete deduplicationSnapshotInterval config on a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"), })
    public void deleteDeduplicationSnapshotInterval(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.DEDUPLICATION_SNAPSHOT, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetDeduplicationSnapshotInterval(null, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("deleteDeduplicationSnapshotInterval", ex, asyncResponse);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/inactiveTopicPolicies")
    @ApiOperation(value = "Get inactive topic policies on a topic.", response = InactiveTopicPolicies.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(code = 500, message = "Internal server error"), })
    public void getInactiveTopicPolicies(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.INACTIVE_TOPIC, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetInactiveTopicPolicies(applied, isGlobal))
            .thenApply(asyncResponse::resume).exceptionally(ex -> {
                handleTopicPolicyException("getInactiveTopicPolicies", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/inactiveTopicPolicies")
    @ApiOperation(value = "Set inactive topic policies on a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"), })
    public void setInactiveTopicPolicies(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "inactive topic policies for the specified topic")
            InactiveTopicPolicies inactiveTopicPolicies) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.INACTIVE_TOPIC, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetInactiveTopicPolicies(inactiveTopicPolicies, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("setInactiveTopicPolicies", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/inactiveTopicPolicies")
    @ApiOperation(value = "Delete inactive topic policies on a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"), })
    public void deleteInactiveTopicPolicies(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.INACTIVE_TOPIC, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetInactiveTopicPolicies(null, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("deleteInactiveTopicPolicies", ex, asyncResponse);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/maxUnackedMessagesOnSubscription")
    @ApiOperation(value = "Get max unacked messages per subscription config on a topic.", response = Integer.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(code = 500, message = "Internal server error"), })
    public void getMaxUnackedMessagesOnSubscription(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.MAX_UNACKED, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetMaxUnackedMessagesOnSubscription(applied, isGlobal))
            .thenApply(asyncResponse::resume)
            .exceptionally(ex -> {
                handleTopicPolicyException("getMaxUnackedMessagesOnSubscription", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/maxUnackedMessagesOnSubscription")
    @ApiOperation(value = "Set max unacked messages per subscription config on a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"), })
    public void setMaxUnackedMessagesOnSubscription(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Max unacked messages on subscription policies for the specified topic")
                    Integer maxUnackedNum) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperation(topicName, PolicyName.MAX_UNACKED, PolicyOperation.WRITE);
        preValidation(authoritative)
            .thenCompose(__ -> internalSetMaxUnackedMessagesOnSubscription(maxUnackedNum, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("setMaxUnackedMessagesOnSubscription", ex, asyncResponse);
                return null;
            });
    }



    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/maxUnackedMessagesOnSubscription")
    @ApiOperation(value = "Delete max unacked messages per subscription config on a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"), })
    public void deleteMaxUnackedMessagesOnSubscription(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperation(topicName, PolicyName.MAX_UNACKED, PolicyOperation.WRITE);
        preValidation(authoritative)
            .thenCompose(__ -> internalSetMaxUnackedMessagesOnSubscription(null, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("deleteMaxUnackedMessagesOnSubscription", ex, asyncResponse);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/delayedDelivery")
    @ApiOperation(value = "Get delayed delivery messages config on a topic.", response = DelayedDeliveryPolicies.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(code = 500, message = "Internal server error"), })
    public void getDelayedDeliveryPolicies(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.DELAYED_DELIVERY, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetDelayedDeliveryPolicies(applied, isGlobal))
            .thenApply(asyncResponse::resume)
            .exceptionally(ex -> {
                handleTopicPolicyException("getDelayedDeliveryPolicies", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/delayedDelivery")
    @ApiOperation(value = "Set delayed delivery messages config on a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"), })
    public void setDelayedDeliveryPolicies(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Delayed delivery policies for the specified topic")
                    DelayedDeliveryPolicies deliveryPolicies) {
        validateTopicName(tenant, namespace, encodedTopic);
        validatePoliciesReadOnlyAccess();
        validateTopicPolicyOperation(topicName, PolicyName.DELAYED_DELIVERY, PolicyOperation.WRITE);
        preValidation(authoritative)
            .thenCompose(__ -> internalSetDelayedDeliveryPolicies(deliveryPolicies, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("setDelayedDeliveryPolicies", ex, asyncResponse);
                return null;
            });
    }



    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/delayedDelivery")
    @ApiOperation(value = "Set delayed delivery messages config on a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"), })
    public void deleteDelayedDeliveryPolicies(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validatePoliciesReadOnlyAccess();
        validateTopicPolicyOperation(topicName, PolicyName.DELAYED_DELIVERY, PolicyOperation.WRITE);
        preValidation(authoritative)
            .thenCompose(__ -> internalSetDelayedDeliveryPolicies(null, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("deleteDelayedDeliveryPolicies", ex, asyncResponse);
                return null;
            });
    }

    /**
     * It updates number of partitions of an existing partitioned topic. It requires partitioned-topic to be
     * already exist and number of new partitions must be greater than existing number of partitions. Decrementing
     * number of partitions requires deletion of topic which is not supported.
     */
    @POST
    @Path("/{tenant}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Increment partitions of an existing partitioned topic.",
            notes = "It increments partitions of existing partitioned-topic")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Update topic partition successful."),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Unauthenticated"),
            @ApiResponse(code = 403, message = "Forbidden/Unauthorized"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 422, message = "The number of partitions should be more than 0 and"
                    + " less than or equal to maxNumPartitionsPerPartitionedTopic"
                    + " and number of new partitions must be greater than existing number of partitions"),
            @ApiResponse(code = 412, message = "Partitioned topic name is invalid"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    public void updatePartitionedTopic(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("updateLocalTopicOnly") @DefaultValue("false") boolean updateLocalTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("force") @DefaultValue("false") boolean force,
            @ApiParam(value = "The number of partitions for the topic",
                    required = true, type = "int", defaultValue = "0")
                    int numPartitions) {
        validatePartitionedTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.PARTITION, PolicyOperation.WRITE)
                .thenCompose(__ -> internalUpdatePartitionedTopicAsync(numPartitions, updateLocalTopic, force))
                .thenAccept(__ -> {
                    log.info("[{}][{}] Updated topic partition to {}.", clientAppId(), topicName, numPartitions);
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}][{}] Failed to update partition to {}",
                                clientAppId(), topicName, numPartitions, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }


    @POST
    @Path("/{tenant}/{namespace}/{topic}/createMissedPartitions")
    @ApiOperation(value = "Create missed partitions of an existing partitioned topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message =
                    "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant does not exist"),
            @ApiResponse(code = 409, message = "Partitioned topic does not exist"),
            @ApiResponse(code = 412, message = "Partitioned topic name is invalid"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    public void createMissedPartitions(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic) {

        try {
            validatePartitionedTopicName(tenant, namespace, encodedTopic);
            internalCreateMissedPartitions(asyncResponse);
        } catch (Exception e) {
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Get partitioned topic metadata.", response = PartitionedTopicMetadata.class)
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Partitioned topic does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Partitioned topic name is invalid"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    public void getPartitionedMetadata(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Is check configuration required to automatically create topic")
            @QueryParam("checkAllowAutoCreation") @DefaultValue("false") boolean checkAllowAutoCreation) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalGetPartitionedMetadataAsync(authoritative, checkAllowAutoCreation)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    Throwable t = FutureUtil.unwrapCompletionException(ex);
                    if (!isRedirectException(t)) {
                        if (AdminResource.isNotFoundException(t)) {
                            log.info("[{}] Failed to get partitioned metadata topic {}: {}",
                                    clientAppId(), topicName, ex.getMessage());
                        } else {
                            log.error("[{}] Failed to get partitioned metadata topic {}",
                                    clientAppId(), topicName, t);
                        }
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/properties")
    @ApiOperation(value = "Get topic properties.", response = String.class, responseContainer = "Map")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Topic name is invalid"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    public void getProperties(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validatePersistentTopicName(tenant, namespace, encodedTopic);
        internalGetPropertiesAsync(authoritative)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to get topic {} properties", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/properties")
    @ApiOperation(value = "Update the properties on the given topic.")
    @ApiResponses(value = {
        @ApiResponse(code = 204, message = "Operation successful"),
        @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
        @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or"
            + "subscriber is not authorized to access this operation"),
        @ApiResponse(code = 403, message = "Don't have admin permission"),
        @ApiResponse(code = 404, message = "Namespace or topic or subscription does not exist"),
        @ApiResponse(code = 405, message = "Method Not Allowed"),
        @ApiResponse(code = 500, message = "Internal server error"),
        @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")
    })
    public void updateProperties(
        @Suspended final AsyncResponse asyncResponse,
        @ApiParam(value = "Specify the tenant", required = true)
        @PathParam("tenant") String tenant,
        @ApiParam(value = "Specify the namespace", required = true)
        @PathParam("namespace") String namespace,
        @ApiParam(value = "Specify topic name", required = true)
        @PathParam("topic") @Encoded String encodedTopic,
        @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
        @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
        @ApiParam(value = "Key value pair properties for the topic metadata") Map<String, String> properties){
        validatePersistentTopicName(tenant, namespace, encodedTopic);
        internalUpdatePropertiesAsync(authoritative, properties)
            .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                if (isNot307And404Exception(ex)) {
                    log.error("[{}] Failed to update topic {} properties", clientAppId(), topicName, ex);
                }
                resumeAsyncResponseExceptionally(asyncResponse, ex);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/properties")
    @ApiOperation(value = "Remove the key in properties on the given topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Partitioned topic does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Partitioned topic name is invalid"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    public void removeProperties(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("key") String key,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validatePersistentTopicName(tenant, namespace, encodedTopic);
        internalRemovePropertiesAsync(authoritative, key)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to remove key {} in properties on topic {}",
                                clientAppId(), key, topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Delete a partitioned topic.",
            notes = "It will also delete all the partitions of the topic if it exists.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Partitioned topic does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Partitioned topic name is invalid"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    public void deletePartitionedTopic(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Stop all producer/consumer/replicator and delete topic forcefully",
                    defaultValue = "false", type = "boolean")
            @QueryParam("force") @DefaultValue("false") boolean force,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            if (topicName.isPartitioned()) {
                // There's no way to create the partition topic with `-partition-{index}`, So we can reject it.
                throw new RestException(Response.Status.PRECONDITION_FAILED,
                        "Partitioned Topic Name should not contain '-partition-'");
            }
            internalDeletePartitionedTopic(asyncResponse, authoritative, force);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/unload")
    @ApiOperation(value = "Unload a topic")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Topic name is not valid or can't find owner for topic"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public void unloadTopic(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalUnloadTopic(asyncResponse, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}")
    @ApiOperation(value = "Delete a topic.",
            notes = "The topic cannot be deleted if delete is not forcefully and there's any active "
                    + "subscription or producer connected to the it. "
                    + "Force delete ignores connected clients and deletes topic by explicitly closing them.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 412, message = "Topic has active producers/subscriptions"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void deleteTopic(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Stop all producer/consumer/replicator and delete topic forcefully",
                    defaultValue = "false", type = "boolean")
            @QueryParam("force") @DefaultValue("false") boolean force,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);

        getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .partitionedTopicExistsAsync(topicName).thenAccept(exists -> {
            if (exists) {
                RestException restException = new RestException(Response.Status.CONFLICT,
                        String.format("%s is a partitioned topic, instead of calling delete topic, please call"
                                + " delete-partitioned-topic.", topicName));
                resumeAsyncResponseExceptionally(asyncResponse, restException);
                return;
            }
            internalDeleteTopicAsync(authoritative, force)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    Throwable t = FutureUtil.unwrapCompletionException(ex);
                    if (!force && (t instanceof BrokerServiceException.TopicBusyException)) {
                        ex = new RestException(Response.Status.PRECONDITION_FAILED,
                                t.getMessage());
                    }
                    if (t instanceof IllegalStateException){
                        ex = new RestException(422/* Unprocessable entity*/, t.getMessage());
                    } else if (isManagedLedgerNotFoundException(t)) {
                        ex = new RestException(Response.Status.NOT_FOUND,
                                getTopicNotFoundErrorMessage(topicName.toString()));
                    } else if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to delete topic {}", clientAppId(), topicName, t);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
        });

    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/subscriptions")
    @ApiOperation(
            value = "Get the list of persistent subscriptions for a given topic.",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration"),
    })
    public void getSubscriptions(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalGetSubscriptions(asyncResponse, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/stats")
    @ApiOperation(value = "Get the stats for the topic.", response = PersistentTopicStats.class)
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public void getStats(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "If return precise backlog or imprecise backlog")
            @QueryParam("getPreciseBacklog") @DefaultValue("false") boolean getPreciseBacklog,
            @ApiParam(value = "If return backlog size for each subscription, require locking on ledger so be careful "
                    + "not to use when there's heavy traffic.")
            @QueryParam("subscriptionBacklogSize") @DefaultValue("true") boolean subscriptionBacklogSize,
            @ApiParam(value = "If return time of the earliest message in backlog")
            @QueryParam("getEarliestTimeInBacklog") @DefaultValue("false") boolean getEarliestTimeInBacklog) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalGetStatsAsync(authoritative, getPreciseBacklog, subscriptionBacklogSize, getEarliestTimeInBacklog)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to get stats for {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/internalStats")
    @ApiOperation(value = "Get the internal stats for the topic.", response = PersistentTopicInternalStats.class)
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public void getInternalStats(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("metadata") @DefaultValue("false") boolean metadata) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalGetInternalStatsAsync(authoritative, metadata)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to get internal stats for topic {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/internal-info")
    @ApiOperation(value = "Get the stored topic metadata.", response = PartitionedManagedLedgerInfo.class)
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void getManagedLedgerInfo(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic")
            @Encoded String encodedTopic, @Suspended AsyncResponse asyncResponse) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalGetManagedLedgerInfo(asyncResponse, authoritative);
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/partitioned-stats")
    @ApiOperation(value = "Get the stats for the partitioned topic.", response = PartitionedTopicStatsImpl.class)
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 412, message = "Partitioned topic name is invalid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")
    })
    public void getPartitionedStats(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Get per partition stats")
            @QueryParam("perPartition") @DefaultValue("true") boolean perPartition,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "If return precise backlog or imprecise backlog")
            @QueryParam("getPreciseBacklog") @DefaultValue("false") boolean getPreciseBacklog,
            @ApiParam(value = "If return backlog size for each subscription, require locking on ledger so be careful "
                    + "not to use when there's heavy traffic.")
            @QueryParam("subscriptionBacklogSize") @DefaultValue("true") boolean subscriptionBacklogSize,
            @ApiParam(value = "If return the earliest time in backlog")
            @QueryParam("getEarliestTimeInBacklog") @DefaultValue("false") boolean getEarliestTimeInBacklog) {
        try {
            validatePartitionedTopicName(tenant, namespace, encodedTopic);
            internalGetPartitionedStats(asyncResponse, authoritative, perPartition, getPreciseBacklog,
                    subscriptionBacklogSize, getEarliestTimeInBacklog);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/partitioned-internalStats")
    @ApiOperation(
            value = "Get the stats-internal for the partitioned topic.",
            response = PartitionedTopicInternalStats.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public void getPartitionedStatsInternal(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalGetPartitionedStatsInternal(asyncResponse, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}")
    @ApiOperation(value = "Delete a subscription.",
            notes = "The subscription cannot be deleted if delete is not forcefully and"
                    + " there are any active consumers attached to it. "
                    + "Force delete ignores connected consumers and deletes subscription by explicitly closing them.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic or subscription does not exist"),
            @ApiResponse(code = 412, message = "Subscription has active consumers"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void deleteSubscription(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Subscription to be deleted")
            @PathParam("subName") String encodedSubName,
            @ApiParam(value = "Disconnect and close all consumers and delete subscription forcefully",
                    defaultValue = "false", type = "boolean")
            @QueryParam("force") @DefaultValue("false") boolean force,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
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
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/skip_all")
    @ApiOperation(value = "Skip all messages on a topic subscription.",
            notes = "Completely clears the backlog on the subscription.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or"
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic or subscription does not exist"),
            @ApiResponse(code = 405, message = "Operation not allowed on non-persistent topic"),
            @ApiResponse(code = 412, message = "Can't find owner for topic"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void skipAllMessages(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Name of subscription")
            @PathParam("subName") String encodedSubName,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalSkipAllMessages(asyncResponse, decode(encodedSubName), authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/skip/{numMessages}")
    @ApiOperation(value = "Skipping messages on a topic subscription.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic or subscription does not exist"),
            @ApiResponse(code = 405, message = "Skipping messages on a partitioned topic is not allowed"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")
    })
    public void skipMessages(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Name of subscription")
            @PathParam("subName") String encodedSubName,
            @ApiParam(value = "The number of messages to skip", defaultValue = "0")
            @PathParam("numMessages") int numMessages,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalSkipMessages(asyncResponse, decode(encodedSubName), numMessages, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/expireMessages/{expireTimeInSeconds}")
    @ApiOperation(value = "Expiry messages on a topic subscription.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
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
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Subscription to be Expiry messages on")
            @PathParam("subName") String encodedSubName,
            @ApiParam(value = "Expires beyond the specified number of seconds", defaultValue = "0")
            @PathParam("expireTimeInSeconds") int expireTimeInSeconds,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalExpireMessagesByTimestamp(asyncResponse, decode(encodedSubName),
                    expireTimeInSeconds, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/expireMessages")
    @ApiOperation(value = "Expiry messages on a topic subscription.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
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
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
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
            validateTopicName(tenant, namespace, encodedTopic);
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
    @Path("/{tenant}/{namespace}/{topic}/all_subscription/expireMessages/{expireTimeInSeconds}")
    @ApiOperation(value = "Expiry messages on all subscriptions of topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or"
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic or subscription does not exist"),
            @ApiResponse(code = 405, message = "Expiry messages on a non-persistent topic is not allowed"),
            @ApiResponse(code = 412, message = "Can't find owner for topic"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void expireMessagesForAllSubscriptions(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Expires beyond the specified number of seconds", defaultValue = "0")
            @PathParam("expireTimeInSeconds") int expireTimeInSeconds,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalExpireMessagesForAllSubscriptions(asyncResponse, expireTimeInSeconds, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subscriptionName}")
    @ApiOperation(value = "Create a subscription on the topic.",
            notes = "Creates a subscription on the topic at the specified message id")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 400, message = "Create subscription on non persistent topic is not supported"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or"
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic or subscription does not exist"),
            @ApiResponse(code = 405, message = "Not supported for partitioned topics"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void createSubscription(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String topic,
            @ApiParam(value = "Subscription to create position on", required = true)
            @PathParam("subscriptionName") String encodedSubName,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(name = "messageId", value = "messageId where to create the subscription. "
                    + "It can be 'latest', 'earliest' or (ledgerId:entryId)",
                    defaultValue = "latest",
                    allowableValues = "latest, earliest, ledgerId:entryId"
            )
                    ResetCursorData resetCursorData,
            @ApiParam(value = "Is replicated required to perform this operation")
            @QueryParam("replicated") boolean replicated
    ) {
        try {
            validateTopicName(tenant, namespace, topic);
            if (!topicName.isPersistent()) {
                throw new RestException(Response.Status.BAD_REQUEST, "Create subscription on non-persistent topic "
                        + "can only be done through client");
            }
            Map<String, String> subscriptionProperties = resetCursorData == null ? null :
                    resetCursorData.getProperties();
            MessageIdImpl messageId = resetCursorData == null ? null :
                    new MessageIdImpl(resetCursorData.getLedgerId(), resetCursorData.getEntryId(),
                            resetCursorData.getPartitionIndex());
            internalCreateSubscription(asyncResponse, decode(encodedSubName), messageId, authoritative,
                    replicated, subscriptionProperties);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/resetcursor/{timestamp}")
    @ApiOperation(value = "Reset subscription to message position closest to absolute timestamp (in ms).",
            notes = "It fence cursor and disconnects all active consumers before resetting cursor.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or"
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic or subscription does not exist"),
            @ApiResponse(code = 405, message = "Method Not Allowed"),
            @ApiResponse(code = 412, message = "Failed to reset cursor on subscription or "
                    + "Unable to find position for timestamp specified"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")
    })
    public void resetCursor(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Subscription to reset position on", required = true)
            @PathParam("subName") String encodedSubName,
            @ApiParam(value = "the timestamp to reset back")
            @PathParam("timestamp") long timestamp,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
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

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/properties")
    @ApiOperation(value = "Replace all the properties on the given subscription")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or"
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic or subscription does not exist"),
            @ApiResponse(code = 405, message = "Method Not Allowed"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")
    })
    public void updateSubscriptionProperties(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Subscription to update", required = true)
            @PathParam("subName") String encodedSubName,
            @ApiParam(value = "The new properties") Map<String, String> subscriptionProperties,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalUpdateSubscriptionProperties(asyncResponse, decode(encodedSubName),
                    subscriptionProperties, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/properties")
    @ApiOperation(value = "Return all the properties on the given subscription",
            response = String.class, responseContainer = "Map")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or"
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic or subscription does not exist"),
            @ApiResponse(code = 405, message = "Method Not Allowed"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")
    })
    public void getSubscriptionProperties(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Subscription", required = true)
            @PathParam("subName") String encodedSubName,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalGetSubscriptionProperties(asyncResponse, decode(encodedSubName),
                    authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/analyzeBacklog")
    @ApiOperation(value = "Analyse a subscription, by scanning all the unprocessed messages")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or"
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic or subscription does not exist"),
            @ApiResponse(code = 405, message = "Method Not Allowed"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")
    })
    public void analyzeSubscriptionBacklog(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Subscription", required = true)
            @PathParam("subName") String encodedSubName,
            @ApiParam(name = "position", value = "messageId to start the analysis")
            ResetCursorData position,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            Optional<Position> positionImpl;
            if (position != null) {
                positionImpl = Optional.of(new PositionImpl(position.getLedgerId(),
                        position.getEntryId()));
            } else {
                positionImpl = Optional.empty();
            }
            validateTopicName(tenant, namespace, encodedTopic);
            internalAnalyzeSubscriptionBacklog(asyncResponse, decode(encodedSubName),
                    positionImpl, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/resetcursor")
    @ApiOperation(value = "Reset subscription to message position closest to given position.",
            notes = "It fence cursor and disconnects all active consumers before resetting cursor.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or"
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic or subscription does not exist"),
            @ApiResponse(code = 405, message = "Not supported for partitioned topics"),
            @ApiResponse(code = 412, message = "Unable to find position for position specified"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void resetCursorOnPosition(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(name = "subName", value = "Subscription to reset position on", required = true)
            @PathParam("subName") String encodedSubName,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(name = "messageId", value = "messageId to reset back to (ledgerId:entryId)")
                    ResetCursorData resetCursorData) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalResetCursorOnPosition(asyncResponse, decode(encodedSubName), authoritative
                    , new MessageIdImpl(resetCursorData.getLedgerId(),
                            resetCursorData.getEntryId(), resetCursorData.getPartitionIndex())
                    , resetCursorData.isExcluded(), resetCursorData.getBatchIndex());
        } catch (Exception e) {
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/position/{messagePosition}")
    @ApiOperation(value = "Peek nth message on a topic subscription.")
    @ApiResponses(value = {
            @ApiResponse(
                    code = 200,
                    message = "Successfully retrieved the message. The response is a binary byte stream "
                            + "containing the message data. Clients need to parse this binary stream based"
                            + " on the message metadata provided in the response headers.",
                    response = byte[].class
            ),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or"
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic, subscription or the message position does not"
                    + " exist"),
            @ApiResponse(code = 405, message = "Skipping messages on a non-persistent topic is not allowed"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void peekNthMessage(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(name = "subName", value = "Subscribed message expired", required = true)
            @PathParam("subName") String encodedSubName,
            @ApiParam(value = "The number of messages (default 1)", defaultValue = "1")
            @PathParam("messagePosition") int messagePosition,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalPeekNthMessageAsync(decode(encodedSubName), messagePosition, authoritative)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to get peek nth message for topic {} subscription {}", clientAppId(),
                                topicName, decode(encodedSubName), ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/examinemessage")
    @ApiOperation(value =
            "Examine a specific message on a topic by position relative to the earliest or the latest message.")
    @ApiResponses(value = {
            @ApiResponse(
                    code = 200,
                    message = "Successfully retrieved the message. The response is a binary byte stream "
                            + "containing the message data. Clients need to parse this binary stream based"
                            + " on the message metadata provided in the response headers.",
                    response = byte[].class
            ),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic, the message position does not exist"),
            @ApiResponse(code = 405, message = "If given partitioned topic"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void examineMessage(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(name = "initialPosition", value = "Relative start position to examine message."
                    + "It can be 'latest' or 'earliest'",
                    defaultValue = "latest",
                    allowableValues = "latest, earliest"
            )
            @QueryParam("initialPosition") String initialPosition,
            @ApiParam(value = "The position of messages (default 1)", defaultValue = "1")
            @QueryParam("messagePosition") long messagePosition,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicOperationAsync(topicName, TopicOperation.PEEK_MESSAGES)
            .thenCompose(__ -> internalExamineMessageAsync(initialPosition, messagePosition, authoritative))
            .thenAccept(asyncResponse::resume)
            .exceptionally(ex -> {
                if (isNot307And404Exception(ex)) {
                    log.error("[{}] Failed to examine a specific message on the topic {}", clientAppId(), topicName,
                            ex);
                }
                resumeAsyncResponseExceptionally(asyncResponse, ex);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/ledger/{ledgerId}/entry/{entryId}")
    @ApiOperation(value = "Get message by its messageId.")
    @ApiResponses(value = {
            @ApiResponse(
                    code = 200,
                    message = "Successfully retrieved the message. The response is a binary byte stream "
                            + "containing the message data. Clients need to parse this binary stream based"
                            + " on the message metadata provided in the response headers.",
                    response = byte[].class
            ),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or"
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic, subscription or the message position does not"
                    + " exist"),
            @ApiResponse(code = 405, message = "Skipping messages on a non-persistent topic is not allowed"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void getMessageById(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "The ledger id", required = true)
            @PathParam("ledgerId") long ledgerId,
            @ApiParam(value = "The entry id", required = true)
            @PathParam("entryId") long entryId,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalGetMessageById(ledgerId, entryId, authoritative)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to get message with ledgerId {} entryId {} from {}",
                                clientAppId(), ledgerId, entryId, topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/messageid/{timestamp}")
    @ApiOperation(value = "Get message ID published at or just after this absolute timestamp (in ms).",
            response = MessageIdAdv.class)
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or"
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 405, message = "Topic is not non-partitioned and persistent"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void getMessageIdByTimestamp(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Specify the timestamp", required = true)
            @PathParam("timestamp") long timestamp,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalGetMessageIdByTimestampAsync(timestamp, authoritative)
                .thenAccept(messageId -> {
                    if (messageId == null) {
                        asyncResponse.resume(new RestException(Response.Status.NOT_FOUND, "Message ID not found"));
                    } else {
                        asyncResponse.resume(messageId);
                    }
                })
                .exceptionally(ex -> {
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to get message ID by timestamp {} from {}",
                            clientAppId(), timestamp, topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/backlog")
    @ApiOperation(value = "Get estimated backlog for offline topic.", response = PersistentOfflineTopicStats.class)
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void getBacklog(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicOperationAsync(topicName, TopicOperation.GET_BACKLOG_SIZE)
                .thenCompose(__ ->  internalGetBacklogAsync(authoritative))
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    Throwable t = FutureUtil.unwrapCompletionException(ex);
                    if (t instanceof MetadataStoreException.NotFoundException) {
                        log.warn("[{}] Failed to get topic backlog {}: Namespace does not exist", clientAppId(),
                                namespaceName);
                        ex = new RestException(Response.Status.NOT_FOUND, "Namespace does not exist");
                    } else if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to get estimated backlog for topic {}", clientAppId(), encodedTopic, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/backlogSize")
    @ApiOperation(value = "Calculate backlog size by a message ID (in bytes).", response = Long.class)
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public void getBacklogSizeByMessageId(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative, MessageIdImpl messageId) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalGetBacklogSizeByMessageId(asyncResponse, messageId, authoritative);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/backlogQuotaMap")
    @ApiOperation(value = "Get backlog quota map on a topic.", response = BacklogQuota.class, responseContainer = "Map")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic policy or namespace does not exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry")})
    public void getBacklogQuotaMap(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.BACKLOG, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetBacklogQuota(applied, isGlobal))
            .thenAccept(asyncResponse::resume)
            .exceptionally(ex -> {
                handleTopicPolicyException("getBacklogQuotaMap", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/backlogQuota")
    @ApiOperation(value = "Set a backlog quota for a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 412, message = "Specified backlog quota exceeds retention quota."
                    + " Increase retention quota and retry request")})
    public void setBacklogQuota(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @QueryParam("backlogQuotaType") BacklogQuotaType backlogQuotaType,
            @ApiParam(value = "backlog quota policies for the specified topic") BacklogQuotaImpl backlogQuota) {
        validateTopicName(tenant, namespace, encodedTopic);
        preValidation(authoritative)
            .thenCompose(__ -> internalSetBacklogQuota(backlogQuotaType, backlogQuota, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("setBacklogQuota", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/backlogQuota")
    @ApiOperation(value = "Remove a backlog quota policy from a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeBacklogQuota(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("backlogQuotaType") BacklogQuotaType backlogQuotaType,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal) {
        validateTopicName(tenant, namespace, encodedTopic);
        preValidation(authoritative)
            .thenCompose(__ -> internalSetBacklogQuota(backlogQuotaType, null, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("removeBacklogQuota", ex, asyncResponse);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/replication")
    @ApiOperation(
            value = "Get the replication clusters for a topic",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405, message =
                    "Topic level policy is disabled, enable the topic level policy and retry")})
    public void getReplicationClusters(@Suspended final AsyncResponse asyncResponse,
                              @PathParam("tenant") String tenant,
                              @PathParam("namespace") String namespace,
                              @PathParam("topic") @Encoded String encodedTopic,
                              @QueryParam("applied") @DefaultValue("false") boolean applied,
                              @ApiParam(value = "Whether leader broker redirected this call to this broker. "
                                      + "For internal use.")
                              @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.REPLICATION, PolicyOperation.READ)
                .thenCompose(__ -> preValidation(authoritative))
                .thenCompose(__ -> getTopicPoliciesAsyncWithRetry(topicName))
                .thenAccept(op -> {
                    asyncResponse.resume(op.map(TopicPolicies::getReplicationClustersSet).orElseGet(() -> {
                        if (applied) {
                            return getNamespacePolicies(namespaceName).replication_clusters;
                        }
                        return null;
                    }));
                })
                .exceptionally(ex -> {
                    handleTopicPolicyException("getReplicationClusters", ex, asyncResponse);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/replication")
    @ApiOperation(value = "Set the replication clusters for a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 412, message = "Topic is not global or invalid cluster ids")})
    public void setReplicationClusters(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "List of replication clusters", required = true) List<String> clusterIds) {
        validateTopicName(tenant, namespace, encodedTopic);
        preValidation(authoritative)
                .thenCompose(__ -> internalSetReplicationClusters(clusterIds))
                .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    handleTopicPolicyException("setReplicationClusters", ex, asyncResponse);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/replication")
    @ApiOperation(value = "Remove the replication clusters from a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeReplicationClusters(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("backlogQuotaType") BacklogQuotaType backlogQuotaType,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        preValidation(authoritative)
                .thenCompose(__ -> internalRemoveReplicationClusters())
                .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    handleTopicPolicyException("removeReplicationClusters", ex, asyncResponse);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/messageTTL")
    @ApiOperation(value = "Get message TTL in seconds for a topic", response = Integer.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405, message =
                    "Topic level policy is disabled, enable the topic level policy and retry")})
    public void getMessageTTL(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.TTL, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> getTopicPoliciesAsyncWithRetry(topicName, isGlobal))
            .thenAccept(op -> asyncResponse.resume(op
                .map(TopicPolicies::getMessageTTLInSeconds)
                .orElseGet(() -> {
                    if (applied) {
                        Integer otherLevelTTL = getNamespacePolicies(namespaceName).message_ttl_in_seconds;
                        return otherLevelTTL == null ? pulsar().getConfiguration().getTtlDurationDefaultInSeconds()
                                : otherLevelTTL;
                    }
                    return null;
                })))
            .exceptionally(ex -> {
                handleTopicPolicyException("getMessageTTL", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/messageTTL")
    @ApiOperation(value = "Set message TTL in seconds for a topic")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message =
            "Not authenticate to perform the request or policy is read only"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405, message =
                    "Topic level policy is disabled, enable the topic level policy and retry"),
            @ApiResponse(code = 412, message = "Invalid message TTL value")})
    public void setMessageTTL(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "TTL in seconds for the specified topic", required = true)
            @QueryParam("messageTTL") Integer messageTTL,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.TTL, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetMessageTTL(messageTTL, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("setMessageTTL", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/messageTTL")
    @ApiOperation(value = "Remove message TTL in seconds for a topic")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403,
                    message = "Not authenticate to perform the request or policy is read only"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, enable the topic level policy and retry"),
            @ApiResponse(code = 412, message = "Invalid message TTL value")})
    public void removeMessageTTL(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.TTL, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetMessageTTL(null, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("removeMessageTTL", ex, asyncResponse);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/deduplicationEnabled")
    @ApiOperation(value = "Get deduplication configuration of a topic.", response = Boolean.class)
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry")})
    public void getDeduplication(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.DEDUPLICATION, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetDeduplication(applied, isGlobal))
            .thenApply(asyncResponse::resume)
            .exceptionally(ex -> {
                handleTopicPolicyException("getDeduplication", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/deduplicationEnabled")
    @ApiOperation(value = "Set deduplication enabled on a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry")})
    public void setDeduplication(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "DeduplicationEnabled policies for the specified topic")
                    Boolean enabled) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.DEDUPLICATION, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetDeduplication(enabled, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("setDeduplication", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/deduplicationEnabled")
    @ApiOperation(value = "Remove deduplication configuration for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeDeduplication(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.DEDUPLICATION, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetDeduplication(null, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("removeDeduplication", ex, asyncResponse);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/retention")
    @ApiOperation(value = "Get retention configuration for specified topic.", response = RetentionPolicies.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getRetention(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.RETENTION, PolicyOperation.READ)
                .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetRetention(applied, isGlobal))
            .thenAccept(asyncResponse::resume)
            .exceptionally(ex -> {
                handleTopicPolicyException("getRetention", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/retention")
    @ApiOperation(value = "Set retention configuration for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405, message =
                    "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Retention Quota must exceed backlog quota")})
    public void setRetention(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Retention policies for the specified topic") RetentionPolicies retention) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.RETENTION, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetRetention(retention, isGlobal))
            .thenRun(() -> {
                try {
                    log.info("[{}] Successfully updated retention: namespace={}, topic={}, retention={}",
                            clientAppId(),
                            namespaceName,
                            topicName.getLocalName(),
                            objectWriter().writeValueAsString(retention));
                } catch (JsonProcessingException ignore) {
                }
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("setRetention", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/retention")
    @ApiOperation(value = "Remove retention configuration for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Retention Quota must exceed backlog quota")})
    public void removeRetention(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.RETENTION, PolicyOperation.WRITE)
                .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalRemoveRetention(isGlobal))
            .thenRun(() -> {
                log.info("[{}] Successfully remove retention: namespace={}, topic={}",
                        clientAppId(),
                        namespaceName,
                        topicName.getLocalName());
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("removeRetention", ex, asyncResponse);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/persistence")
    @ApiOperation(
            value = "Get configuration of persistence policies for specified topic.",
            response = PersistencePolicies.class
    )
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getPersistence(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.PERSISTENCE, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetPersistence(applied, isGlobal))
            .thenApply(asyncResponse::resume)
            .exceptionally(ex -> {
                handleTopicPolicyException("getPersistence", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/persistence")
    @ApiOperation(value = "Set configuration of persistence policies for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 400, message = "Invalid persistence policies")})
    public void setPersistence(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Bookkeeper persistence policies for specified topic")
                                           PersistencePolicies persistencePolicies) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.PERSISTENCE, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetPersistence(persistencePolicies, isGlobal))
            .thenRun(() -> {
                try {
                    log.info("[{}] Successfully updated persistence policies: "
                                    + "namespace={}, topic={}, persistencePolicies={}",
                            clientAppId(),
                            namespaceName,
                            topicName.getLocalName(),
                            objectWriter().writeValueAsString(persistencePolicies));
                } catch (JsonProcessingException ignore) {
                }
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("setPersistence", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/persistence")
    @ApiOperation(value = "Remove configuration of persistence policies for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removePersistence(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.PERSISTENCE, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalRemovePersistence(isGlobal))
            .thenRun(() -> {
                log.info("[{}] Successfully remove persistence policies: namespace={}, topic={}",
                        clientAppId(),
                        namespaceName,
                        topicName.getLocalName());
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("removePersistence", ex, asyncResponse);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/maxSubscriptionsPerTopic")
    @ApiOperation(value = "Get maxSubscriptionsPerTopic config for specified topic.", response = Integer.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getMaxSubscriptionsPerTopic(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.MAX_SUBSCRIPTIONS, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetMaxSubscriptionsPerTopic(isGlobal))
            .thenAccept(op -> asyncResponse.resume(op.isPresent() ? op.get()
                    : Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("getMaxSubscriptions", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/maxSubscriptionsPerTopic")
    @ApiOperation(value = "Set maxSubscriptionsPerTopic config for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Invalid value of maxSubscriptionsPerTopic")})
    public void setMaxSubscriptionsPerTopic(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "The max subscriptions of the topic") int maxSubscriptionsPerTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.MAX_SUBSCRIPTIONS, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetMaxSubscriptionsPerTopic(maxSubscriptionsPerTopic, isGlobal))
            .thenRun(() -> {
                log.info("[{}] Successfully updated maxSubscriptionsPerTopic: namespace={}, topic={}"
                                + ", maxSubscriptions={}, isGlobal={}"
                        , clientAppId(), namespaceName, topicName.getLocalName(), maxSubscriptionsPerTopic, isGlobal);
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("setMaxSubscriptions", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/maxSubscriptionsPerTopic")
    @ApiOperation(value = "Remove maxSubscriptionsPerTopic config for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeMaxSubscriptionsPerTopic(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.MAX_SUBSCRIPTIONS, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetMaxSubscriptionsPerTopic(null, isGlobal))
            .thenRun(() -> {
                log.info("[{}] Successfully remove maxSubscriptionsPerTopic: namespace={}, topic={}",
                        clientAppId(), namespaceName, topicName.getLocalName());
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("removeMaxSubscriptions", ex, asyncResponse);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/replicatorDispatchRate")
    @ApiOperation(value = "Get replicatorDispatchRate config for specified topic.", response = DispatchRate.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getReplicatorDispatchRate(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.REPLICATION_RATE, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetReplicatorDispatchRate(applied, isGlobal))
            .thenApply(asyncResponse::resume)
            .exceptionally(ex -> {
                handleTopicPolicyException("getReplicatorDispatchRate", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/replicatorDispatchRate")
    @ApiOperation(value = "Set replicatorDispatchRate config for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Invalid value of replicatorDispatchRate")})
    public void setReplicatorDispatchRate(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Replicator dispatch rate of the topic") DispatchRateImpl dispatchRate) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.REPLICATION_RATE, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetReplicatorDispatchRate(dispatchRate, isGlobal))
            .thenRun(() -> {
                log.info("[{}] Successfully updated replicatorDispatchRate: namespace={}, topic={}"
                                + ", replicatorDispatchRate={}, isGlobal={}",
                        clientAppId(), namespaceName, topicName.getLocalName(), dispatchRate, isGlobal);
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("setReplicatorDispatchRate", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/replicatorDispatchRate")
    @ApiOperation(value = "Remove replicatorDispatchRate config for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeReplicatorDispatchRate(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.REPLICATION_RATE, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetReplicatorDispatchRate(null, isGlobal))
            .thenRun(() -> {
                log.info("[{}] Successfully remove replicatorDispatchRate limit: namespace={}, topic={}",
                        clientAppId(), namespaceName, topicName.getLocalName());
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("removeReplicatorDispatchRate", ex, asyncResponse);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/maxProducers")
    @ApiOperation(value = "Get maxProducers config for specified topic.", response = Integer.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getMaxProducers(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.MAX_PRODUCERS, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetMaxProducers(applied, isGlobal))
            .thenApply(asyncResponse::resume)
            .exceptionally(ex -> {
                handleTopicPolicyException("getMaxProducers", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/maxProducers")
    @ApiOperation(value = "Set maxProducers config for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Invalid value of maxProducers")})
    public void setMaxProducers(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "The max producers of the topic") int maxProducers) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.MAX_PRODUCERS, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetMaxProducers(maxProducers, isGlobal))
            .thenRun(() -> {
                log.info("[{}] Successfully updated max producers: namespace={}, topic={}, maxProducers={}",
                        clientAppId(),
                        namespaceName,
                        topicName.getLocalName(),
                        maxProducers);
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("setMaxProducers", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/maxProducers")
    @ApiOperation(value = "Remove maxProducers config for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeMaxProducers(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.MAX_PRODUCERS, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalRemoveMaxProducers(isGlobal))
            .thenRun(() -> {
                log.info("[{}] Successfully remove max producers: namespace={}, topic={}",
                        clientAppId(),
                        namespaceName,
                        topicName.getLocalName());
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("removeMaxProducers", ex, asyncResponse);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/maxConsumers")
    @ApiOperation(value = "Get maxConsumers config for specified topic.", response = Integer.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getMaxConsumers(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.MAX_CONSUMERS, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetMaxConsumers(applied, isGlobal))
            .thenApply(asyncResponse::resume)
            .exceptionally(ex -> {
                handleTopicPolicyException("getMaxConsumers", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/maxConsumers")
    @ApiOperation(value = "Set maxConsumers config for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Invalid value of maxConsumers")})
    public void setMaxConsumers(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "The max consumers of the topic") int maxConsumers) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.MAX_CONSUMERS, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetMaxConsumers(maxConsumers, isGlobal))
            .thenRun(() -> {
                log.info("[{}] Successfully updated max consumers: namespace={}, topic={}, maxConsumers={}",
                        clientAppId(),
                        namespaceName,
                        topicName.getLocalName(),
                        maxConsumers);
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("setMaxConsumers", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/maxConsumers")
    @ApiOperation(value = "Remove maxConsumers config for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeMaxConsumers(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.MAX_CONSUMERS, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalRemoveMaxConsumers(isGlobal))
            .thenRun(() -> {
                log.info("[{}] Successfully remove max consumers: namespace={}, topic={}",
                        clientAppId(),
                        namespaceName,
                        topicName.getLocalName());
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("removeMaxConsumers", ex, asyncResponse);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/maxMessageSize")
    @ApiOperation(value = "Get maxMessageSize config for specified topic.", response = Integer.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getMaxMessageSize(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateAdminAccessForTenantAsync(topicName.getTenant())
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetMaxMessageSize(isGlobal))
            .thenAccept(policies -> {
                asyncResponse.resume(policies.isPresent() ? policies.get() : Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("getMaxMessageSize", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/maxMessageSize")
    @ApiOperation(value = "Set maxMessageSize config for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Invalid value of maxConsumers")})
    public void setMaxMessageSize(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "The max message size of the topic") int maxMessageSize) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateAdminAccessForTenantAsync(topicName.getTenant())
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetMaxMessageSize(maxMessageSize, isGlobal))
            .thenRun(() -> {
                log.info(
                        "[{}] Successfully set max message size: namespace={}, topic={}, maxMessageSiz={}, isGlobal={}",
                        clientAppId(),
                        namespaceName,
                        topicName.getLocalName(),
                        maxMessageSize,
                        isGlobal);
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("setMaxMessageSize", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/maxMessageSize")
    @ApiOperation(value = "Remove maxMessageSize config for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeMaxMessageSize(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateAdminAccessForTenantAsync(topicName.getTenant())
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetMaxMessageSize(null, isGlobal))
            .thenRun(() -> {
                log.info("[{}] Successfully remove max message size: namespace={}, topic={}",
                        clientAppId(),
                        namespaceName,
                        topicName.getLocalName());
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("removeMaxMessageSize", ex, asyncResponse);
                return null;
            });
    }


    @POST
    @Path("/{tenant}/{namespace}/{topic}/terminate")
    @ApiOperation(value = "Terminate a topic. A topic that is terminated will not accept any more "
            + "messages to be published and will let consumer to drain existing messages in backlog")
    @ApiResponses(value = {
            @ApiResponse(
                    code = 200,
                    message = "Operation terminated successfully. The response includes the 'lastMessageId',"
                            + " which is the identifier of the last message processed.",
                    response = MessageIdAdv.class
            ),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or"
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 405, message = "Termination of a partitioned topic is not allowed"),
            @ApiResponse(code = 406, message = "Need to provide a persistent topic name"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void terminate(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validatePersistentTopicName(tenant, namespace, encodedTopic);
        internalTerminateAsync(authoritative)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to terminated topic {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/terminate/partitions")
    @ApiOperation(value = "Terminate all partitioned topic. A topic that is terminated will not accept any more "
            + "messages to be published and will let consumer to drain existing messages in backlog")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or"
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 405, message = "Termination of a non-partitioned topic is not allowed"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void terminatePartitionedTopic(@Suspended final AsyncResponse asyncResponse,
                                          @ApiParam(value = "Specify the tenant", required = true)
                                          @PathParam("tenant") String tenant,
                                          @ApiParam(value = "Specify the namespace", required = true)
                                          @PathParam("namespace") String namespace,
                                          @ApiParam(value = "Specify topic name", required = true)
                                          @PathParam("topic") @Encoded String encodedTopic,
                                          @ApiParam(value = "Whether leader broker redirected this call to this broker."
                                                  + " For internal use.")
                                          @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalTerminatePartitionedTopic(asyncResponse, authoritative);
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/compaction")
    @ApiOperation(value = "Trigger a compaction operation on a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or"
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 405, message = "Operation is not allowed on the persistent topic"),
            @ApiResponse(code = 409, message = "Compaction already running"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void compact(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalTriggerCompaction(asyncResponse, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/compaction")
    @ApiOperation(value = "Get the status of a compaction operation for a topic.",
            response = LongRunningProcessStatus.class)
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or"
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist, or compaction hasn't run"),
            @ApiResponse(code = 405, message = "Operation is not allowed on the persistent topic"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void compactionStatus(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalCompactionStatusAsync(authoritative)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to get the status of a compaction operation for the topic {}",
                                clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/offload")
    @ApiOperation(value = "Offload a prefix of a topic to long term storage")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 400, message = "Message ID is null"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or"
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 405, message = "Operation is not allowed on the persistent topic"),
            @ApiResponse(code = 409, message = "Offload already running"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void triggerOffload(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
                               MessageIdImpl messageId) {
        try {
            if (messageId == null) {
                throw new RestException(Response.Status.BAD_REQUEST, "messageId is null");
            }
            validateTopicName(tenant, namespace, encodedTopic);
            internalTriggerOffload(asyncResponse, authoritative, messageId);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/offload")
    @ApiOperation(value = "Offload a prefix of a topic to long term storage", response = OffloadProcessStatus.class)
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
    public void offloadStatus(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalOffloadStatus(asyncResponse, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/lastMessageId")
    @ApiOperation(value = "Return the last commit message id of topic", response = MessageIdAdv.class)
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
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalGetLastMessageId(asyncResponse, authoritative);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/trim")
    @ApiOperation(value = " Trim a topic")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or"
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 405, message = "Operation is not allowed on the persistent topic"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void trimTopic(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalTrimTopic(asyncResponse, authoritative).exceptionally(ex -> {
                // If the exception is not redirect exception we need to log it.
                if (isNot307And404Exception(ex)) {
                    log.error("[{}] Failed to trim topic {}", clientAppId(), topicName, ex);
                }
                resumeAsyncResponseExceptionally(asyncResponse, ex);
                return null;
            });
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/dispatchRate")
    @ApiOperation(value = "Get dispatch rate configuration for specified topic.", response = DispatchRateImpl.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getDispatchRate(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.RATE, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetDispatchRate(applied, isGlobal))
            .thenApply(asyncResponse::resume)
            .exceptionally(ex -> {
                handleTopicPolicyException("getDispatchRate", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/dispatchRate")
    @ApiOperation(value = "Set message dispatch rate configuration for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void setDispatchRate(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Dispatch rate for the specified topic") DispatchRateImpl dispatchRate) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.RATE, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetDispatchRate(dispatchRate, isGlobal))
            .thenRun(() -> {
                try {
                    log.info("[{}] Successfully set topic dispatch rate:"
                                    + " tenant={}, namespace={}, topic={}, dispatchRate={}",
                            clientAppId(),
                            tenant,
                            namespace,
                            topicName.getLocalName(),
                            objectWriter().writeValueAsString(dispatchRate));
                } catch (JsonProcessingException ignore) {}
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("setDispatchRate", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/dispatchRate")
    @ApiOperation(value = "Remove message dispatch rate configuration for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeDispatchRate(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.RATE, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalRemoveDispatchRate(isGlobal))
            .thenRun(() -> {
                log.info("[{}] Successfully remove topic dispatch rate: tenant={}, namespace={}, topic={}",
                        clientAppId(),
                        tenant,
                        namespace,
                        topicName.getLocalName());
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("removeDispatchRate", ex, asyncResponse);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/subscriptionDispatchRate")
    @ApiOperation(
            value = "Get subscription message dispatch rate configuration for specified topic.",
            response = DispatchRate.class
    )
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getSubscriptionDispatchRate(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.RATE, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetSubscriptionDispatchRate(applied, isGlobal))
            .thenApply(asyncResponse::resume)
            .exceptionally(ex -> {
                handleTopicPolicyException("getSubscriptionDispatchRate", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscriptionDispatchRate")
    @ApiOperation(value = "Set subscription message dispatch rate configuration for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void setSubscriptionDispatchRate(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Subscription message dispatch rate for the specified topic")
                    DispatchRateImpl dispatchRate) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.RATE, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetSubscriptionDispatchRate(dispatchRate, isGlobal))
            .thenRun(() -> {
                try {
                    log.info("[{}] Successfully set topic subscription dispatch rate:"
                                    + " tenant={}, namespace={}, topic={}, dispatchRate={}",
                            clientAppId(),
                            tenant,
                            namespace,
                            topicName.getLocalName(),
                            objectWriter().writeValueAsString(dispatchRate));
                } catch (JsonProcessingException ignore) {}
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("setSubscriptionDispatchRate", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/subscriptionDispatchRate")
    @ApiOperation(value = "Remove subscription message dispatch rate configuration for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeSubscriptionDispatchRate(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.RATE, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalRemoveSubscriptionDispatchRate(isGlobal))
            .thenRun(() -> {
                log.info("[{}] Successfully remove topic subscription dispatch rate: tenant={}, namespace={}, topic={}",
                        clientAppId(),
                        tenant,
                        namespace,
                        topicName.getLocalName());
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("removeSubscriptionDispatchRate", ex, asyncResponse);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/{subName}/dispatchRate")
    @ApiOperation(value = "Get message dispatch rate configuration for specified subscription.",
            response = DispatchRate.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getSubscriptionLevelDispatchRate(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @PathParam("subName") @Encoded String encodedSubscriptionName,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.RATE, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetSubscriptionLevelDispatchRate(
                    Codec.decode(encodedSubscriptionName), applied, isGlobal))
            .thenApply(asyncResponse::resume)
            .exceptionally(ex -> {
                handleTopicPolicyException("getSubscriptionLevelDispatchRate", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/{subName}/dispatchRate")
    @ApiOperation(value = "Set message dispatch rate configuration for specified subscription.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void setSubscriptionLevelDispatchRate(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @PathParam("subName") @Encoded String encodedSubscriptionName,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Subscription message dispatch rate for the specified topic")
                    DispatchRateImpl dispatchRate) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.RATE, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetSubscriptionLevelDispatchRate(
                    Codec.decode(encodedSubscriptionName), dispatchRate, isGlobal))
            .thenRun(() -> {
                log.info("[{}] Successfully set subscription level dispatch rate:"
                                + " tenant={}, namespace={}, topic={}, sub={}, dispatchRate={}",
                        clientAppId(),
                        tenant,
                        namespace,
                        topicName.getLocalName(),
                        encodedSubscriptionName,
                        dispatchRate);
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("setSubscriptionLevelDispatchRate", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/{subName}/dispatchRate")
    @ApiOperation(value = "Remove message dispatch rate configuration for specified subscription.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeSubscriptionLevelDispatchRate(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @PathParam("subName") @Encoded String encodedSubscriptionName,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.RATE, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalRemoveSubscriptionLevelDispatchRate(
                    Codec.decode(encodedSubscriptionName), isGlobal))
            .thenRun(() -> {
                log.info("[{}] Successfully remove subscription level dispatch rate: "
                                + "tenant={}, namespace={}, topic={}, sub={}",
                        clientAppId(), tenant, namespace, topicName.getLocalName(), encodedSubscriptionName);
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("removeSubscriptionLevelDispatchRate", ex, asyncResponse);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/compactionThreshold")
    @ApiOperation(value = "Get compaction threshold configuration for specified topic.", response = Long.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getCompactionThreshold(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.COMPACTION, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetCompactionThreshold(applied, isGlobal))
            .thenApply(asyncResponse::resume)
            .exceptionally(ex -> {
                handleTopicPolicyException("getCompactionThreshold", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/compactionThreshold")
    @ApiOperation(value = "Set compaction threshold configuration for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void setCompactionThreshold(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Dispatch rate for the specified topic") long compactionThreshold) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.COMPACTION, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetCompactionThreshold(compactionThreshold, isGlobal))
            .thenRun(() -> {
                try {
                    log.info("[{}] Successfully set topic compaction threshold:"
                                    + " tenant={}, namespace={}, topic={}, compactionThreshold={}",
                            clientAppId(),
                            tenant,
                            namespace,
                            topicName.getLocalName(),
                            objectWriter().writeValueAsString(compactionThreshold));
                } catch (JsonProcessingException ignore) {}
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("setCompactionThreshold", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/compactionThreshold")
    @ApiOperation(value = "Remove compaction threshold configuration for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeCompactionThreshold(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.COMPACTION, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalRemoveCompactionThreshold(isGlobal))
            .thenRun(() -> {
                log.info("[{}] Successfully remove topic compaction threshold: tenant={}, namespace={}, topic={}",
                        clientAppId(),
                        tenant,
                        namespace,
                        topicName.getLocalName());
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("removeCompactionThreshold", ex, asyncResponse);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/maxConsumersPerSubscription")
    @ApiOperation(
            value = "Get max consumers per subscription configuration for specified topic.",
            response = Integer.class
    )
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getMaxConsumersPerSubscription(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.MAX_CONSUMERS, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetMaxConsumersPerSubscription(isGlobal))
            .thenAccept(op -> asyncResponse.resume(op.isPresent() ? op.get()
                    : Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("getMaxConsumersPerSubscription", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/maxConsumersPerSubscription")
    @ApiOperation(value = "Set max consumers per subscription configuration for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void setMaxConsumersPerSubscription(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Dispatch rate for the specified topic") int maxConsumersPerSubscription) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.MAX_CONSUMERS, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetMaxConsumersPerSubscription(maxConsumersPerSubscription, isGlobal))
            .thenRun(() -> {
                try {
                    log.info("[{}] Successfully set topic max consumers per subscription:"
                                    + " tenant={}, namespace={}, topic={}, maxConsumersPerSubscription={}",
                            clientAppId(),
                            tenant,
                            namespace,
                            topicName.getLocalName(),
                            objectWriter().writeValueAsString(maxConsumersPerSubscription));
                } catch (JsonProcessingException ignore) {}
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("setMaxConsumersPerSubscription", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/maxConsumersPerSubscription")
    @ApiOperation(value = "Remove max consumers per subscription configuration for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeMaxConsumersPerSubscription(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.MAX_CONSUMERS, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalRemoveMaxConsumersPerSubscription(isGlobal))
            .thenRun(() -> {
                log.info("[{}] Successfully remove topic max consumers per subscription:"
                                + " tenant={}, namespace={}, topic={}",
                        clientAppId(),
                        tenant,
                        namespace,
                        topicName.getLocalName());
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("removeMaxConsumersPerSubscription", ex, asyncResponse);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/publishRate")
    @ApiOperation(value = "Get publish rate configuration for specified topic.", response = PublishRate.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getPublishRate(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.RATE, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetPublishRate(isGlobal))
            .thenAccept(op -> asyncResponse.resume(op.isPresent() ? op.get()
                    : Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("getPublishRate", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/publishRate")
    @ApiOperation(value = "Set message publish rate configuration for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void setPublishRate(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Dispatch rate for the specified topic") PublishRate publishRate) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.RATE, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetPublishRate(publishRate, isGlobal))
            .thenRun(() -> {
                try {
                    log.info("[{}] Successfully set topic publish rate:"
                                    + " tenant={}, namespace={}, topic={}, isGlobal={}, publishRate={}",
                            clientAppId(),
                            tenant,
                            namespace,
                            topicName.getLocalName(),
                            isGlobal,
                            objectWriter().writeValueAsString(publishRate));
                } catch (JsonProcessingException ignore) {}
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("setPublishRate", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/publishRate")
    @ApiOperation(value = "Remove message publish rate configuration for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removePublishRate(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.RATE, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalRemovePublishRate(isGlobal))
            .thenRun(() -> {
                log.info("[{}] Successfully remove topic publish rate: tenant={}, namespace={}, topic={}, isGlobal={}",
                        clientAppId(),
                        tenant,
                        namespace,
                        topicName.getLocalName(),
                        isGlobal);
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("removePublishRate", ex, asyncResponse);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/subscriptionTypesEnabled")
    @ApiOperation(
            value = "Get is enable sub type fors specified topic.",
            response = CommandSubscribe.SubType.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getSubscriptionTypesEnabled(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.SUBSCRIPTION_AUTH_MODE, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetSubscriptionTypesEnabled(isGlobal))
            .thenAccept(op -> {
                asyncResponse.resume(op.isPresent() ? op.get()
                        : Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("getSubscriptionTypesEnabled", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscriptionTypesEnabled")
    @ApiOperation(value = "Set is enable sub types for specified topic")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void setSubscriptionTypesEnabled(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Enable sub types for the specified topic")
            Set<SubscriptionType> subscriptionTypesEnabled) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.SUBSCRIPTION_AUTH_MODE, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetSubscriptionTypesEnabled(subscriptionTypesEnabled, isGlobal))
            .thenRun(() -> {
                try {
                    log.info("[{}] Successfully set topic is enabled sub types :"
                                    + " tenant={}, namespace={}, topic={}, subscriptionTypesEnabled={}",
                            clientAppId(),
                            tenant,
                            namespace,
                            topicName.getLocalName(),
                            objectWriter().writeValueAsString(subscriptionTypesEnabled));
                } catch (JsonProcessingException ignore) {}
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("setSubscriptionTypesEnabled", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/subscriptionTypesEnabled")
    @ApiOperation(value = "Remove subscription types enabled for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeSubscriptionTypesEnabled(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.SUBSCRIPTION_AUTH_MODE, PolicyOperation.WRITE)
                .thenCompose(__ -> preValidation(authoritative))
                .thenCompose(__ -> internalRemoveSubscriptionTypesEnabled(isGlobal))
                .thenRun(() -> {
                    log.info("[{}] Successfully remove subscription types enabled: namespace={}, topic={}",
                            clientAppId(),
                            namespaceName,
                            topicName.getLocalName());
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    handleTopicPolicyException("removeSubscriptionTypesEnabled", ex, asyncResponse);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/subscribeRate")
    @ApiOperation(value = "Get subscribe rate configuration for specified topic.", response = SubscribeRate.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getSubscribeRate(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.RATE, PolicyOperation.READ)
                .thenCompose(__ -> preValidation(authoritative))
                .thenCompose(__ -> internalGetSubscribeRate(applied, isGlobal))
                .thenApply(asyncResponse::resume).exceptionally(ex -> {
            handleTopicPolicyException("getSubscribeRate", ex, asyncResponse);
            return null;
        });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscribeRate")
    @ApiOperation(value = "Set subscribe rate configuration for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void setSubscribeRate(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Subscribe rate for the specified topic") SubscribeRate subscribeRate) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.RATE, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetSubscribeRate(subscribeRate, isGlobal))
            .thenRun(() -> {
                try {
                    log.info("[{}] Successfully set topic subscribe rate:"
                                    + " tenant={}, namespace={}, topic={}, isGlobal={} subscribeRate={}",
                            clientAppId(),
                            tenant,
                            namespace,
                            topicName.getLocalName(),
                            isGlobal,
                            objectWriter().writeValueAsString(subscribeRate));
                } catch (JsonProcessingException ignore) {}
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("setSubscribeRate", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/subscribeRate")
    @ApiOperation(value = "Remove subscribe rate configuration for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeSubscribeRate(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Subscribe rate for the specified topic") SubscribeRate subscribeRate) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.RATE, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalRemoveSubscribeRate(isGlobal))
            .thenRun(() -> {
                log.info(
                        "[{}] Successfully remove topic subscribe rate: tenant={}, namespace={}, topic={}, isGlobal={}",
                        clientAppId(),
                        tenant,
                        namespace,
                        topicName.getLocalName(),
                        isGlobal);
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("removeSubscribeRate", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/truncate")
    @ApiOperation(value = "Truncate a topic.",
            notes = "The truncate operation will move all cursors to the end of the topic "
                    + "and delete all inactive ledgers.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void truncateTopic(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative){
        validateTopicName(tenant, namespace, encodedTopic);
        internalTruncateTopicAsync(authoritative)
            .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                Throwable t = FutureUtil.unwrapCompletionException(ex);
                if (!isRedirectException(t)) {
                    log.error("[{}] Failed to truncate topic {}", clientAppId(), topicName, t);
                }
                if (t instanceof PulsarAdminException.NotFoundException) {
                    t = new RestException(Response.Status.NOT_FOUND, t.getMessage());
                }
                resumeAsyncResponseExceptionally(asyncResponse, t);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/replicatedSubscriptionStatus")
    @ApiOperation(value = "Enable or disable a replicated subscription on a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or "
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic or subscription does not exist"),
            @ApiResponse(code = 405, message = "Operation not allowed on this topic"),
            @ApiResponse(code = 412, message = "Can't find owner for topic"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void setReplicatedSubscriptionStatus(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
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
            validateTopicName(tenant, namespace, encodedTopic);
            internalSetReplicatedSubscriptionStatus(asyncResponse, decode(encodedSubName), authoritative, enabled);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/replicatedSubscriptionStatus")
    @ApiOperation(
            value = "Get replicated subscription status on a topic.",
            response = Boolean.class,
            responseContainer = "Map"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 412, message = "Can't find owner for topic"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void getReplicatedSubscriptionStatus(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Name of subscription", required = true)
            @PathParam("subName") String encodedSubName,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalGetReplicatedSubscriptionStatus(asyncResponse, decode(encodedSubName), authoritative);
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/replicateSubscriptionState")
    @ApiOperation(value = "Enable or disable subscription replication on a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or "
                    + "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic or subscription does not exist"),
            @ApiResponse(code = 405, message = "Operation not allowed on this topic"),
            @ApiResponse(code = 412, message = "Can't find owner for topic"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void setReplicateSubscriptionState(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Whether to enable subscription replication", required = true)
            Boolean enabled) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicOperationAsync(topicName, TopicOperation.SET_REPLICATED_SUBSCRIPTION_STATUS)
                .thenCompose(__ -> preValidation(authoritative))
                .thenCompose(__ -> internalSetEnableReplicatedSubscription(enabled, isGlobal))
                .thenRun(() -> {
                    log.info(
                            "[{}] Successfully set topic replicated subscription enabled: tenant={}, namespace={}, "
                                    + "topic={}, isGlobal={}",
                            clientAppId(),
                            tenant,
                            namespace,
                            topicName.getLocalName(),
                            isGlobal);
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    handleTopicPolicyException("setReplicateSubscriptionState", ex, asyncResponse);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/replicateSubscriptionState")
    @ApiOperation(value = "Get the enabled status of subscription replication on a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 412, message = "Can't find owner for topic"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void getReplicateSubscriptionState(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicOperationAsync(topicName, TopicOperation.GET_REPLICATED_SUBSCRIPTION_STATUS)
                .thenCompose(__ -> preValidation(authoritative))
                .thenCompose(__ -> internalGetReplicateSubscriptionState(applied, isGlobal))
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    handleTopicPolicyException("getReplicateSubscriptionState", ex, asyncResponse);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/schemaCompatibilityStrategy")
    @ApiOperation(value = "Get schema compatibility strategy on a topic", response = SchemaCompatibilityStrategy.class)
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 405, message = "Operation not allowed on persistent topic"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist")})
    public void getSchemaCompatibilityStrategy(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the cluster", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);

        preValidation(authoritative)
                .thenCompose(__-> internalGetSchemaCompatibilityStrategy(applied))
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    handleTopicPolicyException("getSchemaCompatibilityStrategy", ex, asyncResponse);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/schemaCompatibilityStrategy")
    @ApiOperation(value = "Set schema compatibility strategy on a topic")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 405, message = "Operation not allowed on persistent topic"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist")})
    public void setSchemaCompatibilityStrategy(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Strategy used to check the compatibility of new schema")
                    SchemaCompatibilityStrategy strategy) {
        validateTopicName(tenant, namespace, encodedTopic);

        preValidation(authoritative)
                .thenCompose(__ -> internalSetSchemaCompatibilityStrategy(strategy))
                .thenRun(() -> {
                    log.info(
                            "[{}] Successfully set topic schema compatibility strategy: tenant={}, namespace={}, "
                                    + "topic={}, schemaCompatibilityStrategy={}",
                            clientAppId(),
                            tenant,
                            namespace,
                            topicName.getLocalName(),
                            strategy);
                    asyncResponse.resume(Response.noContent().build());
                }).exceptionally(ex -> {
                    handleTopicPolicyException("setSchemaCompatibilityStrategy", ex, asyncResponse);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/schemaCompatibilityStrategy")
    @ApiOperation(value = "Remove schema compatibility strategy on a topic")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 405, message = "Operation not allowed on persistent topic"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist")})
    public void removeSchemaCompatibilityStrategy(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Strategy used to check the compatibility of new schema")
                    SchemaCompatibilityStrategy strategy) {
        validateTopicName(tenant, namespace, encodedTopic);

        preValidation(authoritative)
                .thenCompose(__ -> internalSetSchemaCompatibilityStrategy(null))
                .thenRun(() -> {
                    log.info(
                            "[{}] Successfully remove topic schema compatibility strategy: tenant={}, namespace={}, "
                                    + "topic={}",
                            clientAppId(),
                            tenant,
                            namespace,
                            topicName.getLocalName());
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    handleTopicPolicyException("removeSchemaCompatibilityStrategy", ex, asyncResponse);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/schemaValidationEnforced")
    @ApiOperation(value = "Get schema validation enforced flag for topic.", response = Boolean.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenants or Namespace doesn't exist") })
    public void getSchemaValidationEnforced(@Suspended AsyncResponse asyncResponse,
                                            @ApiParam(value = "Specify the tenant", required = true)
                                            @PathParam("tenant") String tenant,
                                            @ApiParam(value = "Specify the namespace", required = true)
                                            @PathParam("namespace") String namespace,
                                            @ApiParam(value = "Specify topic name", required = true)
                                            @PathParam("topic") @Encoded String encodedTopic,
                                            @QueryParam("applied") @DefaultValue("false") boolean applied,
                                            @ApiParam(value = "Whether leader broker redirected this call to this "
                                                    + "broker. For internal use.")
                                            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.SCHEMA_COMPATIBILITY_STRATEGY, PolicyOperation.READ)
                .thenCompose(__ -> preValidation(authoritative))
                .thenCompose(__ -> internalGetSchemaValidationEnforced(applied))
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    handleTopicPolicyException("getSchemaValidationEnforced", ex, asyncResponse);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/schemaValidationEnforced")
    @ApiOperation(value = "Set schema validation enforced flag on topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or Namespace doesn't exist"),
            @ApiResponse(code = 412, message = "schemaValidationEnforced value is not valid")})
    public void setSchemaValidationEnforced(@Suspended AsyncResponse asyncResponse,
                                            @ApiParam(value = "Specify the tenant", required = true)
                                            @PathParam("tenant") String tenant,
                                            @ApiParam(value = "Specify the namespace", required = true)
                                            @PathParam("namespace") String namespace,
                                            @ApiParam(value = "Specify topic name", required = true)
                                            @PathParam("topic") @Encoded String encodedTopic,
                                            @ApiParam(value = "Whether leader broker redirected this call to this "
                                                    + "broker. For internal use.")
                                            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
                                            @ApiParam(required = true) boolean schemaValidationEnforced) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.SCHEMA_COMPATIBILITY_STRATEGY, PolicyOperation.WRITE)
                .thenCompose(__ -> preValidation(authoritative))
                .thenCompose(__ -> internalSetSchemaValidationEnforced(schemaValidationEnforced))
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    handleTopicPolicyException("setSchemaValidationEnforced", ex, asyncResponse);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/entryFilters")
    @ApiOperation(value = "Get entry filters for a topic.", response = EntryFilters.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenants or Namespace doesn't exist") })
    public void getEntryFilters(@Suspended AsyncResponse asyncResponse,
                                @ApiParam(value = "Specify the tenant", required = true)
                                @PathParam("tenant") String tenant,
                                @ApiParam(value = "Specify the namespace", required = true)
                                @PathParam("namespace") String namespace,
                                @ApiParam(value = "Specify topic name", required = true)
                                @PathParam("topic") @Encoded String encodedTopic,
                                @QueryParam("applied") @DefaultValue("false") boolean applied,
                                @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
                                @ApiParam(value = "Whether leader broker redirected this call to this "
                                        + "broker. For internal use.")
                                @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        preValidation(authoritative)
                .thenCompose(__ -> internalGetEntryFilters(applied, isGlobal))
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    handleTopicPolicyException("getEntryFilters", ex, asyncResponse);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/entryFilters")
    @ApiOperation(value = "Set entry filters for specified topic")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void setEntryFilters(@Suspended final AsyncResponse asyncResponse,
                                            @PathParam("tenant") String tenant,
                                            @PathParam("namespace") String namespace,
                                            @PathParam("topic") @Encoded String encodedTopic,
                                            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
                                            @ApiParam(value = "Whether leader broker redirected this"
                                                    + "call to this broker. For internal use.")
                                            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
                                            @ApiParam(value = "Entry filters for the specified topic")
                                        EntryFilters entryFilters) {
        validateTopicName(tenant, namespace, encodedTopic);
        preValidation(authoritative)
                .thenCompose(__ -> internalSetEntryFilters(entryFilters, isGlobal))
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    handleTopicPolicyException("setEntryFilters", ex, asyncResponse);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/entryFilters")
    @ApiOperation(value = "Remove entry filters for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeEntryFilters(@Suspended final AsyncResponse asyncResponse,
                                    @PathParam("tenant") String tenant,
                                    @PathParam("namespace") String namespace,
                                    @PathParam("topic") @Encoded String encodedTopic,
                                    @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
                                    @ApiParam(value = "Whether leader broker redirected this"
                                            + "call to this broker. For internal use.")
                                    @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        preValidation(authoritative)
                .thenCompose(__ -> internalRemoveEntryFilters(isGlobal))
                .thenRun(() -> {
                    log.info(
                            "[{}] Successfully remove entry filters: tenant={}, namespace={}, topic={}, isGlobal={}",
                            clientAppId(),
                            tenant,
                            namespace,
                            topicName.getLocalName(),
                            isGlobal);
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    handleTopicPolicyException("removeEntryFilters", ex, asyncResponse);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/shadowTopics")
    @ApiOperation(value = "Get the shadow topic list for a topic",
            response = String.class, responseContainer = "List")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405, message =
                    "Topic level policy is disabled, enable the topic level policy and retry")})
    public void getShadowTopics(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        preValidation(authoritative)
                .thenCompose(__ -> validateTopicPolicyOperationAsync(topicName, PolicyName.SHADOW_TOPIC,
                        PolicyOperation.READ))
                .thenCompose(__ -> getTopicPoliciesAsyncWithRetry(topicName))
                .thenAccept(op -> asyncResponse.resume(op.map(TopicPolicies::getShadowTopics).orElse(null)))
                .exceptionally(ex -> {
                    handleTopicPolicyException("getShadowTopics", ex, asyncResponse);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/shadowTopics")
    @ApiOperation(value = "Set shadow topic list for a topic")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405, message =
                    "Topic level policy is disabled, enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
    })
    public void setShadowTopics(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "List of shadow topics", required = true) List<String> shadowTopics) {
        validateTopicName(tenant, namespace, encodedTopic);
        preValidation(authoritative)
                .thenCompose(__ -> internalSetShadowTopic(shadowTopics))
                .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    handleTopicPolicyException("setShadowTopic", ex, asyncResponse);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/shadowTopics")
    @ApiOperation(value = "Delete shadow topics for a topic")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace or topic doesn't exist"),
            @ApiResponse(code = 405, message =
                    "Topic level policy is disabled, enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
    })
    public void deleteShadowTopics(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        preValidation(authoritative)
                .thenCompose(__ -> internalDeleteShadowTopics())
                .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    handleTopicPolicyException("deleteShadowTopic", ex, asyncResponse);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/autoSubscriptionCreation")
    @ApiOperation(value = "Override namespace's allowAutoSubscriptionCreation setting for a topic")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic doesn't exist"),
            @ApiResponse(code = 405, message =
                    "Topic level policy is disabled, enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void setAutoSubscriptionCreation(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Settings for automatic subscription creation")
            AutoSubscriptionCreationOverrideImpl autoSubscriptionCreationOverride) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.AUTO_SUBSCRIPTION_CREATION, PolicyOperation.WRITE)
                .thenCompose(__ -> preValidation(authoritative))
                .thenCompose(__ -> internalSetAutoSubscriptionCreation(autoSubscriptionCreationOverride, isGlobal))
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    handleTopicPolicyException("setAutoSubscriptionCreation", ex, asyncResponse);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/autoSubscriptionCreation")
    @ApiOperation(value = "Get autoSubscriptionCreation info in a topic",
            response = AutoSubscriptionCreationOverrideImpl.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getAutoSubscriptionCreation(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.AUTO_SUBSCRIPTION_CREATION, PolicyOperation.READ)
                .thenCompose(__ -> preValidation(authoritative))
                .thenCompose(__ -> internalGetAutoSubscriptionCreation(applied, isGlobal))
                .thenApply(asyncResponse::resume).exceptionally(ex -> {
                    handleTopicPolicyException("getAutoSubscriptionCreation", ex, asyncResponse);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/autoSubscriptionCreation")
    @ApiOperation(value = "Remove autoSubscriptionCreation ina a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405,
                    message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeAutoSubscriptionCreation(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.AUTO_SUBSCRIPTION_CREATION, PolicyOperation.WRITE)
                .thenCompose(__ -> preValidation(authoritative))
                .thenCompose(__ -> internalSetAutoSubscriptionCreation(null, isGlobal))
                .thenRun(() -> {
                    log.info(
                            "[{}] Successfully remove topic removeAutoSubscriptionCreation: "
                                    + "tenant={}, namespace={}, topic={}, isGlobal={}",
                            clientAppId(),
                            tenant,
                            namespace,
                            topicName.getLocalName(),
                            isGlobal);
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    handleTopicPolicyException("removeAutoSubscriptionCreation", ex, asyncResponse);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/resourceGroup")
    @ApiOperation(value = "Set ResourceGroup for a topic")
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic doesn't exist"),
            @ApiResponse(code = 405, message =
                    "Topic level policy is disabled, enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")
    })
    public void setResourceGroup(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "ResourceGroup name", required = true) String resourceGroupName) {
        validateTopicName(tenant, namespace, encodedTopic);
        preValidation(authoritative)
                .thenCompose(__ -> internalSetResourceGroup(resourceGroupName, isGlobal))
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    handleTopicPolicyException("setResourceGroup", ex, asyncResponse);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/resourceGroup")
    @ApiOperation(value = "Get ResourceGroup for a topic")
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic doesn't exist"),
            @ApiResponse(code = 405, message =
                    "Topic level policy is disabled, enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")
    })
    public void getResourceGroup(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") String encodedTopic,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        preValidation(authoritative)
                .thenCompose(__ -> internalGetResourceGroup(applied, isGlobal))
                .thenApply(asyncResponse::resume)
                .exceptionally(ex -> {
                    handleTopicPolicyException("getResourceGroup", ex, asyncResponse);
                    return null;
                });
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentTopics.class);
}
