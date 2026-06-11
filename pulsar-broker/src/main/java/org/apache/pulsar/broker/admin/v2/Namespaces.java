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

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.admin.impl.NamespacesBase;
import org.apache.pulsar.broker.admin.impl.OffloaderObjectsScannerUtils;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.GrantTopicPermissionOptions;
import org.apache.pulsar.client.admin.RevokeTopicPermissionOptions;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.EntryFilters;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;
import org.apache.pulsar.common.policies.data.TopicHashPositions;
import org.apache.pulsar.common.policies.data.impl.AutoSubscriptionCreationOverrideImpl;
import org.apache.pulsar.common.policies.data.impl.AutoTopicCreationOverrideImpl;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.common.policies.data.impl.BookieAffinityGroupDataImpl;
import org.apache.pulsar.common.policies.data.impl.BundlesDataImpl;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;

@Path("/namespaces")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "namespaces", description = "Namespaces admin apis")
@SuppressWarnings("deprecation")
public class Namespaces extends NamespacesBase {

    @GET
    @Path("/{tenant}")
    @Operation(summary = "Get the list of all the namespaces for a certain tenant.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the list of all the namespaces for a certain tenant.",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class),
                            uniqueItems = true))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant doesn't exist")})
    public void getTenantNamespaces(@Suspended final AsyncResponse response,
                                    @PathParam("tenant") String tenant) {
        internalGetTenantNamespaces(tenant)
                .thenAccept(response::resume)
                .exceptionally(ex -> {
                    log.error()
                            .attr("tenant", tenant)
                            .exceptionMessage(ex)
                            .log("Failed to get namespaces list for tenant");
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/topics")
    @Operation(summary = "Get the list of all the topics under a certain namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Get the list of all the topics under a certain namespace.",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class),
                            uniqueItems = true))),
            @ApiResponse(responseCode = "403", description = "Don't have admin or operate permission on the namespace"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist")})
    public void getTopics(@Suspended AsyncResponse response,
                          @PathParam("tenant") String tenant,
                          @PathParam("namespace") String namespace,
                          @QueryParam("mode") @DefaultValue("PERSISTENT") Mode mode,
                          @Parameter(description = "Include system topic")
                          @QueryParam("includeSystemTopic") boolean includeSystemTopic) {
        validateNamespaceName(tenant, namespace);
        validateNamespaceOperationAsync(NamespaceName.get(tenant, namespace), NamespaceOperation.GET_TOPICS)
                // Validate that namespace exists, throws 404 if it doesn't exist
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenCompose(policies -> internalGetListOfTopics(response, policies, mode))
                .thenApply(topics -> filterSystemTopic(topics, includeSystemTopic))
                .thenAccept(response::resume)
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get topics list for namespace");
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}")
    @Operation(summary = "Get the dump all the policies specified for a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the dump all the policies specified for a namespace.",
                    content = @Content(schema = @Schema(implementation = Policies.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist") })
    public void getPolicies(@Suspended AsyncResponse response,
                            @PathParam("tenant") String tenant,
                            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(NamespaceName.get(tenant, namespace), PolicyName.ALL,
                PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(response::resume)
                .exceptionally(ex -> {
                    if (AdminResource.isNotFoundOrConflictException(ex)) {
                        log.info()
                                .attr("namespace", namespaceName)
                                .exceptionMessage(ex)
                                .log("Failed to get policies for namespace");
                    } else {
                        log.error()
                                .attr("namespace", namespaceName)
                                .exception(ex)
                                .log("Failed to get policies for namespace");
                    }
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}")
    @Operation(summary = "Creates a new namespace with the specified policies")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Namespace already exists"),
            @ApiResponse(responseCode = "412", description = "Namespace name is not valid") })
    public void createNamespace(@Suspended AsyncResponse response,
                                @PathParam("tenant") String tenant,
                                @PathParam("namespace") String namespace,
                                @RequestBody(description = "Policies for the namespace") Policies policies) {
        validateNamespaceName(tenant, namespace);
        policies = getDefaultPolicesIfNull(policies);
        internalCreateNamespace(policies)
                .thenAccept(__ -> response.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    Throwable root = FutureUtil.unwrapCompletionException(ex);
                    if (root instanceof MetadataStoreException.AlreadyExistsException) {
                        response.resume(new RestException(Response.Status.CONFLICT, "Namespace already exists"));
                    } else {
                        log.error()
                                .attr("namespace", namespaceName)
                                .exception(ex)
                                .log("Failed to create namespace");
                        resumeAsyncResponseExceptionally(response, ex);
                    }
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}")
    @Operation(summary = "Delete a namespace and all the topics under it.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "307", description = "Current broker doesn't serve the namespace"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "405", description = "Broker doesn't allow forced deletion of namespaces"),
            @ApiResponse(responseCode = "409", description = "Namespace is not empty") })
    public void deleteNamespace(@Suspended final AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                @PathParam("namespace") String namespace,
                                @QueryParam("force") @DefaultValue("false") boolean force,
                                @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        internalDeleteNamespaceAsync(force)
                .thenAccept(__ -> {
                    log.info()
                            .attr("namespace", namespace)
                            .log("Successful delete namespace");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    if (!isRedirectException(ex)) {
                        log.error()
                                .attr("namespace", namespaceName)
                                .exception(ex)
                                .log("Failed to delete namespace");
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{bundle}")
    @Operation(summary = "Delete a namespace bundle and all the topics under it.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "307", description = "Current broker doesn't serve the namespace"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Namespace bundle is not empty")})
    public void deleteNamespaceBundle(@Suspended AsyncResponse response, @PathParam("tenant") String tenant,
                                      @PathParam("namespace") String namespace,
                                      @PathParam("bundle") String bundleRange,
                                      @QueryParam("force") @DefaultValue("false") boolean force,
                                      @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        internalDeleteNamespaceBundleAsync(bundleRange, authoritative, force)
                .thenRun(() -> response.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    if (!isRedirectException(ex)) {
                        log.error()
                                .attr("namespace", namespaceName)
                                .exception(ex)
                                .log("Failed to delete namespace bundle");
                    }
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/permissions")
    @Operation(summary = "Retrieve the permissions for a namespace.",
            description = "Returns a map structure: Map<String, Set<AuthAction>>.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Retrieve the permissions for a namespace.",
                    content = @Content(schema = @Schema(type = "object"),
                            additionalPropertiesArraySchema = @ArraySchema(
                            schema = @Schema(implementation = AuthAction.class), uniqueItems = true))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Namespace is not empty") })
    public void getPermissions(@Suspended AsyncResponse response,
                                                       @PathParam("tenant") String tenant,
                                                       @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespaceOperationAsync(NamespaceName.get(tenant, namespace), NamespaceOperation.GET_PERMISSION)
                .thenCompose(__ -> getAuthorizationService().getPermissionsAsync(namespaceName))
                .thenAccept(permissions -> response.resume(permissions))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get permissions for namespace");
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/permissions/subscription")
    @Operation(summary = "Retrieve the permissions for a subscription.",
            description = "Returns a map structure: Map<String, Set<String>>.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Retrieve the permissions for a subscription.",
                    content = @Content(schema = @Schema(type = "object"),
                            additionalPropertiesArraySchema = @ArraySchema(
                            schema = @Schema(implementation = String.class), uniqueItems = true))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Namespace is not empty")})
    public void getPermissionOnSubscription(@Suspended AsyncResponse response,
                                            @PathParam("tenant") String tenant,
                                            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespaceOperationAsync(NamespaceName.get(tenant, namespace), NamespaceOperation.GET_PERMISSION)
                .thenCompose(__ -> getAuthorizationService().getSubscriptionPermissionsAsync(namespaceName))
                .thenAccept(permissions -> response.resume(permissions))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exceptionMessage(ex.getCause())
                            .exception(ex)
                            .log("Failed to get permissions on subscription for namespace");
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/permissions/{role}")
    @Operation(summary = "Grant a new permission to a role on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "501", description = "Authorization is not enabled")})
    public void grantPermissionOnNamespace(@Suspended AsyncResponse asyncResponse,
                                           @PathParam("tenant") String tenant,
                                           @PathParam("namespace") String namespace,
                                           @PathParam("role") String role,
            @RequestBody(description = "List of permissions for the specified role") Set<AuthAction> actions) {
        validateNamespaceName(tenant, namespace);
        internalGrantPermissionOnNamespaceAsync(role, actions)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exceptionMessage(ex.getCause())
                            .exception(ex)
                            .log("Failed to set permissions for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/grantPermissionsOnTopics")
    @Operation(summary = "Grant new permissions to a role on multi-topics.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "401",
                    description = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "tenant/namespace/topic doesn't exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error") })
    public void grantPermissionsOnTopics(@Suspended final AsyncResponse asyncResponse,
                                 List<GrantTopicPermissionOptions> options) {
        internalGrantPermissionOnTopicsAsync(options)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("permissions", options)
                            .exception(ex)
                            .log("Failed to grant permissions");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/revokePermissionsOnTopics")
    @Operation(summary = "Revoke new permissions to a role on multi-topics.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "401",
                    description = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "tenant/namespace/topic doesn't exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error") })
    public void revokePermissionsOnTopics(@Suspended final AsyncResponse asyncResponse,
                                         List<RevokeTopicPermissionOptions> options) {
        internalRevokePermissionOnTopicsAsync(options)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("permissions", options)
                            .exception(ex)
                            .log("Failed to revoke permissions");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/permissions/subscription/{subscription}")
    @Operation(hidden = true, summary = "Grant a new permission to roles for a subscription."
            + "[Tenant admin is allowed to perform this operation]")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "501", description = "Authorization is not enabled") })
    public void grantPermissionOnSubscription(@Suspended AsyncResponse asyncResponse,
                                              @PathParam("tenant") String tenant,
                                              @PathParam("namespace") String namespace,
                                              @PathParam("subscription") String subscription,
            @RequestBody(description = "List of roles for the specified subscription") Set<String> roles) {
        validateNamespaceName(tenant, namespace);
        internalGrantPermissionOnSubscriptionAsync(subscription, roles)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("role", roles)
                            .attr("subscription", subscription)
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to grant permission on subscription");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/permissions/{role}")
    @Operation(summary = "Revoke all permissions to a role on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist") })
    public void revokePermissionsOnNamespace(@Suspended AsyncResponse asyncResponse,
                                             @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("role") String role) {
        validateNamespaceName(tenant, namespace);
        internalRevokePermissionsOnNamespaceAsync(role)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("role", role)
                            .attr("namespace", namespace)
                            .exceptionMessage(ex.getCause())
                            .exception(ex)
                            .log("Failed to revoke permission on role - namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/permissions/{subscription}/{role}")
    @Operation(hidden = true, summary = "Revoke subscription admin-api access permission for a role.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or namespace doesn't exist") })
    public void revokePermissionOnSubscription(@Suspended AsyncResponse asyncResponse,
                                               @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("subscription") String subscription,
            @PathParam("role") String role) {
        validateNamespaceName(tenant, namespace);
        internalRevokePermissionsOnSubscriptionAsync(subscription, role)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("role", role)
                            .attr("subscription", subscription)
                            .attr("namespace", namespace)
                            .exceptionMessage(ex.getCause())
                            .exception(ex)
                            .log("Failed to revoke permission on subscription for role : - namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/replication")
    @Operation(summary = "Get the replication clusters for a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the replication clusters for a namespace.",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class),
                            uniqueItems = true))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "412", description = "Namespace is not global")})
    public void getNamespaceReplicationClusters(@Suspended AsyncResponse asyncResponse,
                                                @PathParam("tenant") String tenant,
                                                @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetNamespaceReplicationClustersAsync()
                .thenAccept(asyncResponse::resume)
                .exceptionally(e -> {
                    log.error()
                            .attr("namespace", namespace)
                            .exception(e)
                            .log("Failed to get namespace replication clusters on namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, e);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/replication")
    @Operation(summary = "Set the replication clusters for a namespace. "
            + "When removing a cluster: "
            + "with shared configuration store, data will be deleted from the removed cluster; "
            + "with separate configuration store, only replication stops but data is preserved.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Peer-cluster can't be part of replication-cluster"),
            @ApiResponse(responseCode = "412", description = "Namespace is not global or invalid cluster ids") })
    public void setNamespaceReplicationClusters(@Suspended AsyncResponse asyncResponse,
                                                @PathParam("tenant") String tenant,
                                                @PathParam("namespace") String namespace,
            @RequestBody(description = "List of replication clusters", required = true) List<String> clusterIds,
            @QueryParam(value = "compareTopicPartitions") boolean compareTopicPartitions) {
        validateNamespaceName(tenant, namespace);
        internalSetNamespaceReplicationClusters(clusterIds, compareTopicPartitions)
                .thenAccept(asyncResponse::resume)
                .exceptionally(e -> {
                    log.error()
                            .attr("namespace", namespace)
                            .exception(e)
                            .log("Failed to set namespace replication clusters on namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, e);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/messageTTL")
    @Operation(summary = "Get the message TTL for the namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the message TTL for the namespace",
                    content = @Content(schema = @Schema(implementation = Integer.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist") })
    public void getNamespaceMessageTTL(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(NamespaceName.get(tenant, namespace), PolicyName.TTL,
                PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.message_ttl_in_seconds))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get namespace message TTL for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/messageTTL")
    @Operation(summary = "Set message TTL in seconds for namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "412", description = "Invalid TTL") })
    public void setNamespaceMessageTTL(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                       @PathParam("namespace") String namespace,
                                       @RequestBody(description = "TTL in seconds for the specified namespace",
                                               required = true)
                                               int messageTTL) {
        validateNamespaceName(tenant, namespace);
        internalSetNamespaceMessageTTLAsync(messageTTL)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to set namespace message TTL for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/messageTTL")
    @Operation(summary = "Remove message TTL in seconds for namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "412", description = "Invalid TTL")})
    public void removeNamespaceMessageTTL(@Suspended AsyncResponse asyncResponse,
                                          @PathParam("tenant") String tenant,
                                          @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetNamespaceMessageTTLAsync(null)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to remove namespace message TTL for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/subscriptionExpirationTime")
    @Operation(summary = "Get the subscription expiration time for the namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the subscription expiration time for the namespace",
                    content = @Content(schema = @Schema(implementation = Integer.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist") })
    public void getSubscriptionExpirationTime(@Suspended AsyncResponse asyncResponse,
                                                 @PathParam("tenant") String tenant,
                                                 @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.SUBSCRIPTION_EXPIRATION_TIME,
                PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.subscription_expiration_time_minutes))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exceptionMessage(ex.getCause())
                            .exception(ex)
                            .log("Failed to get subscription expiration time for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/subscriptionExpirationTime")
    @Operation(summary = "Set subscription expiration time in minutes for namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "412", description = "Invalid expiration time")})
    public void setSubscriptionExpirationTime(@Suspended AsyncResponse asyncResponse,
                                              @PathParam("tenant") String tenant,
                                              @PathParam("namespace") String namespace,
                                              @RequestBody(description =
                                                      "Expiration time in minutes for the specified namespace",
                                                      required = true) int expirationTime) {
        validateNamespaceName(tenant, namespace);
        internalSetSubscriptionExpirationTimeAsync(expirationTime)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exceptionMessage(ex.getCause())
                            .exception(ex)
                            .log("Failed to set subscription expiration time for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/subscriptionExpirationTime")
    @Operation(summary = "Remove subscription expiration time for namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist")})
    public void removeSubscriptionExpirationTime(@Suspended AsyncResponse asyncResponse,
                                                 @PathParam("tenant") String tenant,
                                                 @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetSubscriptionExpirationTimeAsync(null)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exceptionMessage(ex.getCause())
                            .exception(ex)
                            .log("Failed to remove subscription expiration time for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/deduplication")
    @Operation(summary = "Get broker side deduplication for all topics in a namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Get broker side deduplication for all topics in a namespace",
                    content = @Content(schema = @Schema(implementation = Boolean.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist") })
    public void getDeduplication(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                 @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetDeduplicationAsync()
                .thenAccept(deduplication -> asyncResponse.resume(deduplication))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespace)
                            .exception(ex)
                            .log("Failed to get broker deduplication config for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/deduplication")
    @Operation(summary = "Enable or disable broker side deduplication for all topics in a namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist") })
    public void modifyDeduplication(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                    @PathParam("namespace") String namespace,
                                    @RequestBody(description = "Flag for disabling or enabling broker side "
                                            + "deduplication for all topics in the specified namespace",
                                            required = true)
                                            boolean enableDeduplication) {
        validateNamespaceName(tenant, namespace);
        internalModifyDeduplicationAsync(enableDeduplication)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to modify broker deduplication config for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/deduplication")
    @Operation(summary = "Remove broker side deduplication for all topics in a namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist") })
    public void removeDeduplication(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                    @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalModifyDeduplicationAsync(null)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(e -> {
                    Throwable ex = FutureUtil.unwrapCompletionException(e);
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to remove broker deduplication config for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/autoTopicCreation")
    @Operation(summary = "Get autoTopicCreation info in a namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get autoTopicCreation info in a namespace",
                    content = @Content(schema = @Schema(implementation = AutoTopicCreationOverrideImpl.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or namespace doesn't exist")})
    public void getAutoTopicCreation(@Suspended AsyncResponse asyncResponse,
                                                          @PathParam("tenant") String tenant,
                                                          @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetAutoTopicCreationAsync()
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get autoTopicCreation info for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/autoTopicCreation")
    @Operation(summary = "Override broker's allowAutoTopicCreation setting for a namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "406", description = "The number of partitions should be less than or"
                    + " equal to maxNumPartitionsPerPartitionedTopic"),
            @ApiResponse(responseCode = "400", description = "Invalid autoTopicCreation override")})
    public void setAutoTopicCreation(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @RequestBody(description = "Settings for automatic topic creation", required = true)
                    AutoTopicCreationOverride autoTopicCreationOverride) {
        validateNamespaceName(tenant, namespace);
        internalSetAutoTopicCreationAsync(autoTopicCreationOverride)
                .thenAccept(__ -> {
                    String autoOverride = (autoTopicCreationOverride != null
                            && autoTopicCreationOverride.isAllowAutoTopicCreation()) ? "enabled" : "disabled";
                    log.info()
                            .attr("successfully", autoOverride)
                            .attr("namespace", namespaceName)
                            .log("Successfully autoTopicCreation on namespace");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(e -> {
                    Throwable ex = FutureUtil.unwrapCompletionException(e);
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to set autoTopicCreation status on namespace");
                    if (ex instanceof MetadataStoreException.NotFoundException) {
                        asyncResponse.resume(new RestException(Response.Status.NOT_FOUND, "Namespace does not exist"));
                    } else {
                        resumeAsyncResponseExceptionally(asyncResponse, ex);
                    }
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/autoTopicCreation")
    @Operation(summary = "Remove override of broker's allowAutoTopicCreation in a namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist") })
    public void removeAutoTopicCreation(@Suspended final AsyncResponse asyncResponse,
                                        @PathParam("tenant") String tenant, @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetAutoTopicCreationAsync(null)
                .thenAccept(__ -> {
                    log.info()
                            .attr("namespace", namespaceName)
                            .log("Successfully remove autoTopicCreation on namespace");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(e -> {
                    Throwable ex = FutureUtil.unwrapCompletionException(e);
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to remove autoTopicCreation status on namespace");
                    if (ex instanceof MetadataStoreException.NotFoundException) {
                        asyncResponse.resume(new RestException(Response.Status.NOT_FOUND, "Namespace does not exist"));
                    } else {
                        resumeAsyncResponseExceptionally(asyncResponse, ex);
                    }
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/autoSubscriptionCreation")
    @Operation(summary = "Override broker's allowAutoSubscriptionCreation setting for a namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "400", description = "Invalid autoSubscriptionCreation override")})
    public void setAutoSubscriptionCreation(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @RequestBody(description = "Settings for automatic subscription creation")
                    AutoSubscriptionCreationOverride autoSubscriptionCreationOverride) {
        validateNamespaceName(tenant, namespace);
        internalSetAutoSubscriptionCreationAsync(autoSubscriptionCreationOverride)
                .thenAccept(__ -> {
                    log.info()
                            .attr("namespace", namespaceName)
                            .log("Successfully set autoSubscriptionCreation on namespace");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(e -> {
                    Throwable ex = FutureUtil.unwrapCompletionException(e);
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to set autoSubscriptionCreation on namespace");
                    if (ex instanceof MetadataStoreException.NotFoundException) {
                        asyncResponse.resume(new RestException(Response.Status.NOT_FOUND, "Namespace does not exist"));
                    } else {
                        resumeAsyncResponseExceptionally(asyncResponse, ex);
                    }
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/autoSubscriptionCreation")
    @Operation(summary = "Get autoSubscriptionCreation info in a namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get autoSubscriptionCreation info in a namespace",
                    content = @Content(schema =
                            @Schema(implementation = AutoSubscriptionCreationOverrideImpl.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or namespace doesn't exist")})
    public void getAutoSubscriptionCreation(@Suspended final AsyncResponse asyncResponse,
                                                                        @PathParam("tenant") String tenant,
                                                                        @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetAutoSubscriptionCreationAsync()
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get autoSubscriptionCreation for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/autoSubscriptionCreation")
    @Operation(summary = "Remove override of broker's allowAutoSubscriptionCreation in a namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist") })
    public void removeAutoSubscriptionCreation(@Suspended final AsyncResponse asyncResponse,
                                        @PathParam("tenant") String tenant, @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetAutoSubscriptionCreationAsync(null)
                .thenAccept(__ -> {
                    log.info()
                            .attr("namespace", namespaceName)
                            .log("Successfully set autoSubscriptionCreation on namespace");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(e -> {
                    Throwable ex = FutureUtil.unwrapCompletionException(e);
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to set autoSubscriptionCreation on namespace");
                    if (ex instanceof MetadataStoreException.NotFoundException) {
                        asyncResponse.resume(new RestException(Response.Status.NOT_FOUND, "Namespace does not exist"));
                    } else {
                        resumeAsyncResponseExceptionally(asyncResponse, ex);
                    }
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/bundles")
    @Operation(summary = "Get the bundles split data.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the bundles split data.",
                    content = @Content(schema = @Schema(implementation = BundlesDataImpl.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "412", description = "Namespace is not setup to split in bundles") })
    public void getBundlesData(@Suspended final AsyncResponse asyncResponse,
                                      @PathParam("tenant") String tenant,
                                      @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validatePoliciesReadOnlyAccessAsync()
                .thenCompose(__ -> validateNamespaceOperationAsync(NamespaceName.get(tenant, namespace),
                        NamespaceOperation.GET_BUNDLE))
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.bundles))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get bundle data for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/unload")
    @Operation(summary = "Unload namespace",
            description = "Unload an active namespace from the current broker serving it. Performing this operation"
                    + " will make the broker remove all producers, consumers, and connections using this namespace,"
                    + " and close all topics (including their persistent store). During that operation,"
                    + " the namespace is marked as tentatively unavailable until the broker completes "
                    + "the unloading action. This operation requires strictly super user privileges,"
                    + " since it would result in non-persistent message loss and"
                    + " unexpected connection closure to the clients.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "307", description = "Current broker doesn't serve the namespace"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or namespace doesn't exist"),
            @ApiResponse(responseCode = "412",
                    description = "Namespace is already unloaded or Namespace has bundles activated")})
    public void unloadNamespace(@Suspended final AsyncResponse asyncResponse,
                                @PathParam("tenant") String tenant,
                                @PathParam("namespace") String namespace) {
        try {
            validateNamespaceName(tenant, namespace);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
            return;
        }
        internalUnloadNamespaceAsync()
                .thenAccept(__ -> {
                    log.info()
                            .attr("namespace", namespaceName)
                            .log("Successfully unloaded all the bundles in namespace");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    if (!isRedirectException(ex)) {
                        log.error()
                                .attr("namespace", namespaceName)
                                .exception(ex)
                                .log("Failed to unload namespace");
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/{bundle}/unload")
    @Operation(summary = "Unload a namespace bundle")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "307", description = "Current broker doesn't serve the namespace"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission") })
    public void unloadNamespaceBundle(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
                                      @QueryParam("destinationBroker") String destinationBroker) {
        validateNamespaceName(tenant, namespace);
        internalUnloadNamespaceBundleAsync(bundleRange, destinationBroker, authoritative)
                .thenAccept(__ -> {
                    log.info()
                            .attr("bundle", bundleRange)
                            .log("Successfully unloaded namespace bundle");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    if (!isRedirectException(ex)) {
                        log.error()
                                .attr("namespace", namespaceName)
                                .attr("bundleRange", bundleRange)
                                .exception(ex)
                                .log("Failed to unload namespace bundle");
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/{bundle}/split")
    @Operation(summary = "Split a namespace bundle")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "307", description = "Current broker doesn't serve the namespace"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission") })
    public void splitNamespaceBundle(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("unload") @DefaultValue("false") boolean unload,
            @QueryParam("splitAlgorithmName") String splitAlgorithmName,
            @RequestBody(description = "splitBoundaries") List<Long> splitBoundaries) {
        validateNamespaceName(tenant, namespace);
        internalSplitNamespaceBundleAsync(bundleRange, authoritative, unload, splitAlgorithmName, splitBoundaries)
                .thenAccept(__ -> {
                    log.info()
                            .attr("bundle", bundleRange)
                            .log("Successfully split namespace bundle");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    if (!isRedirectException(ex)) {
                        log.error()
                                .attr("namespace", namespaceName)
                                .attr("bundleRange", bundleRange)
                                .exceptionMessage(ex)
                                .log("Failed to split namespace bundle");
                    }
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    if (realCause instanceof IllegalArgumentException) {
                        asyncResponse.resume(new RestException(Response.Status.PRECONDITION_FAILED,
                                "Split bundle failed due to invalid request"));
                    } else {
                        resumeAsyncResponseExceptionally(asyncResponse, ex);
                    }
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/{bundle}/topicHashPositions")
    @Operation(summary = "Get hash positions for topics")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get hash positions for topics",
                    content = @Content(schema = @Schema(implementation = TopicHashPositions.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist")})
    public void getTopicHashPositions(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("bundle") String bundleRange,
            @QueryParam("topics") List<String> topics,
            @Suspended AsyncResponse asyncResponse) {
        validateNamespaceName(tenant, namespace);
        internalGetTopicHashPositionsAsync(bundleRange, topics)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    if (!isRedirectException(ex)) {
                        log.error()
                                .attr("namespaceName", namespaceName)
                                .attr("bundle", bundleRange)
                                .log("Failed to get topic list for bundle .");
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/publishRate")
    @Operation(hidden = true, summary = "Set publish-rate throttling for all topics of the namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission") })
    public void setPublishRate(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @RequestBody(description = "Publish rate for all topics of the specified namespace")
                    PublishRate publishRate) {
        validateNamespaceName(tenant, namespace);
        internalSetPublishRateAsync(publishRate)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/publishRate")
    @Operation(hidden = true, summary = "Set publish-rate throttling for all topics of the namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission") })
    public void removePublishRate(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalRemovePublishRateAsync()
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to remove the publish_max_message_rate for cluster on namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/publishRate")
    @Operation(hidden = true,
            summary = "Get publish-rate configured for the namespace, null means publish-rate not configured, "
                    + "-1 means msg-publish-rate or byte-publish-rate not configured in publish-rate yet")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Get publish-rate configured for the namespace, null means publish-rate "
                            + "not configured, -1 means msg-publish-rate or byte-publish-rate not configured "
                            + "in publish-rate yet",
                    content = @Content(schema = @Schema(implementation = PublishRate.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist")})
    public void getPublishRate(@Suspended AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetPublishRateAsync()
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get publish rate for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/dispatchRate")
    @Operation(summary = "Set dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission") })
    public void setDispatchRate(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @RequestBody(description = "Dispatch rate for all topics of the specified namespace")
                    DispatchRateImpl dispatchRate) {
        validateNamespaceName(tenant, namespace);
        internalSetTopicDispatchRateAsync(dispatchRate)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to update the dispatchRate for cluster on namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/dispatchRate")
    @Operation(summary = "Delete dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission") })
    public void deleteDispatchRate(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalDeleteTopicDispatchRateAsync()
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to delete the dispatchRate for cluster on namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/dispatchRate")
    @Operation(summary = "Get dispatch-rate configured for the namespace, null means dispatch-rate not configured, "
            + "-1 means msg-dispatch-rate or byte-dispatch-rate not configured in dispatch-rate yet")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Get dispatch-rate configured for the namespace, null means dispatch-rate "
                            + "not configured, -1 means msg-dispatch-rate or byte-dispatch-rate not configured "
                            + "in dispatch-rate yet",
                    content = @Content(schema = @Schema(implementation = DispatchRate.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist") })
    public void getDispatchRate(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetTopicDispatchRateAsync()
                .thenAccept(dispatchRate -> asyncResponse.resume(dispatchRate))
                .exceptionally(ex -> {
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/subscriptionDispatchRate")
    @Operation(summary = "Set Subscription dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission")})
    public void setSubscriptionDispatchRate(@Suspended AsyncResponse asyncResponse,
                                            @PathParam("tenant") String tenant,
                                            @PathParam("namespace") String namespace,
                                            @RequestBody(description =
                                            "Subscription dispatch rate for all topics of the specified namespace")
                                                        DispatchRateImpl dispatchRate) {
        validateNamespaceName(tenant, namespace);
        internalSetSubscriptionDispatchRateAsync(dispatchRate)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to update the subscription dispatchRate for cluster on namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/subscriptionDispatchRate")
    @Operation(summary = "Get subscription dispatch-rate configured for the namespace, null means subscription "
            + "dispatch-rate not configured, -1 means msg-dispatch-rate or byte-dispatch-rate not configured "
            + "in dispatch-rate yet")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Get subscription dispatch-rate configured for the namespace, null means "
                            + "subscription dispatch-rate not configured, -1 means msg-dispatch-rate or "
                            + "byte-dispatch-rate not configured in dispatch-rate yet",
                    content = @Content(schema = @Schema(implementation = DispatchRate.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist")})
    public void getSubscriptionDispatchRate(@Suspended AsyncResponse asyncResponse,
                                                    @PathParam("tenant") String tenant,
                                                    @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetSubscriptionDispatchRateAsync()
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get the subscription dispatchRate for cluster on namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/subscriptionDispatchRate")
    @Operation(summary = "Delete Subscription dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission") })
    public void deleteSubscriptionDispatchRate(@Suspended AsyncResponse asyncResponse,
                                               @PathParam("tenant") String tenant,
                                               @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalDeleteSubscriptionDispatchRateAsync()
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to delete the subscription dispatchRate for cluster on namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/subscribeRate")
    @Operation(summary = "Delete subscribe-rate throttling for all topics of the namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission")})
    public void deleteSubscribeRate(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                    @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalDeleteSubscribeRateAsync()
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to delete the subscribeRate for cluster on namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/subscribeRate")
    @Operation(summary = "Set subscribe-rate throttling for all topics of the namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission")})
    public void setSubscribeRate(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                 @PathParam("namespace") String namespace,
                                 @RequestBody(description = "Subscribe rate for all topics of the specified namespace")
                                         SubscribeRate subscribeRate) {
        validateNamespaceName(tenant, namespace);
        internalSetSubscribeRateAsync(subscribeRate)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to update the subscribeRate for cluster on namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/subscribeRate")
    @Operation(summary = "Get subscribe-rate configured for the namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get subscribe-rate configured for the namespace",
                    content = @Content(schema = @Schema(implementation = SubscribeRate.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist")})
    public void getSubscribeRate(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                 @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetSubscribeRateAsync()
                .thenAccept(subscribeRate -> asyncResponse.resume(subscribeRate))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get subscribe rate for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/replicatorDispatchRate")
    @Operation(summary = "Remove replicator dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission")})
    public void removeReplicatorDispatchRate(@Suspended AsyncResponse asyncResponse,
                                             @PathParam("tenant") String tenant,
                                             @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalRemoveReplicatorDispatchRate(asyncResponse);
    }

    @POST
    @Path("/{tenant}/{namespace}/replicatorDispatchRate")
    @Operation(summary = "Set replicator dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission")})
    public void setReplicatorDispatchRate(@Suspended AsyncResponse asyncResponse,
                                          @PathParam("tenant") String tenant,
                                          @PathParam("namespace") String namespace,
                                          @RequestBody(description =
            "Replicator dispatch rate for all topics of the specified namespace") DispatchRateImpl dispatchRate) {
        validateNamespaceName(tenant, namespace);
        internalSetReplicatorDispatchRate(asyncResponse, dispatchRate);
    }

    @GET
    @Path("/{tenant}/{namespace}/replicatorDispatchRate")
    @Operation(summary = "Get replicator dispatch-rate configured for the namespace, null means replicator "
            + "dispatch-rate not configured, -1 means msg-dispatch-rate or byte-dispatch-rate not configured "
            + "in dispatch-rate yet")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Get replicator dispatch-rate configured for the namespace, null means "
                            + "replicator dispatch-rate not configured, -1 means msg-dispatch-rate or "
                            + "byte-dispatch-rate not configured in dispatch-rate yet",
                    content = @Content(schema = @Schema(implementation = DispatchRateImpl.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist") })
    public void getReplicatorDispatchRate(@Suspended final AsyncResponse asyncResponse,
                                          @PathParam("tenant") String tenant,
                                          @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetReplicatorDispatchRate(asyncResponse);
    }

    @GET
    @Path("/{tenant}/{namespace}/backlogQuotaMap")
    @Operation(summary = "Get backlog quota map on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get backlog quota map on a namespace.",
                    content = @Content(schema = @Schema(type = "object",
                            additionalPropertiesSchema = BacklogQuotaImpl.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist") })
    public void getBacklogQuotaMap(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetBacklogQuotaMap(asyncResponse);
    }

    @POST
    @Path("/{tenant}/{namespace}/backlogQuota")
    @Operation(summary = "Set a backlog quota for all the topics on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "412",
                    description = "Specified backlog quota exceeds retention quota."
                            + " Increase retention quota and retry request")})
    public void setBacklogQuota(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @QueryParam("backlogQuotaType") BacklogQuotaType backlogQuotaType,
            @RequestBody(description = "Backlog quota for all topics of the specified namespace")
                    BacklogQuota backlogQuota) {
        validateNamespaceName(tenant, namespace);
        internalSetBacklogQuota(asyncResponse, backlogQuotaType, backlogQuota);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/backlogQuota")
    @Operation(summary = "Remove a backlog quota policy from a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification") })
    public void removeBacklogQuota(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @QueryParam("backlogQuotaType") BacklogQuotaType backlogQuotaType) {
        validateNamespaceName(tenant, namespace);
        internalRemoveBacklogQuota(asyncResponse, backlogQuotaType);
    }

    @GET
    @Path("/{tenant}/{namespace}/retention")
    @Operation(summary = "Get retention config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get retention config on a namespace.",
                    content = @Content(schema = @Schema(implementation = RetentionPolicies.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist") })
    public void getRetention(@Suspended final AsyncResponse asyncResponse,
                             @PathParam("tenant") String tenant,
                             @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.RETENTION, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.retention_policies))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get retention config on a namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/retention")
    @Operation(summary = "Set retention configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "412", description = "Retention Quota must exceed backlog quota") })
    public void setRetention(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @RequestBody(description = "Retention policies for the specified namespace") RetentionPolicies retention) {
        validateNamespaceName(tenant, namespace);
        internalSetRetention(retention);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/retention")
    @Operation(summary = "Remove retention configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "412", description = "Retention Quota must exceed backlog quota") })
    public void removeRetention(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @RequestBody(description = "Retention policies for the specified namespace") RetentionPolicies retention) {
        validateNamespaceName(tenant, namespace);
        internalSetRetention(null);
    }

    @POST
    @Path("/{tenant}/{namespace}/persistence")
    @Operation(summary = "Set the persistence configuration for all the topics on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "400", description = "Invalid persistence policies")})
    public void setPersistence(@Suspended final AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                               @PathParam("namespace") String namespace,
                               @RequestBody(description = "Persistence policies for the specified namespace",
                                       required = true)
                                       PersistencePolicies persistence) {
        validateNamespaceName(tenant, namespace);
        internalSetPersistenceAsync(persistence)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to update the persistence for a namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/persistence")
    @Operation(summary = "Delete the persistence configuration for all topics on a namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission") })
    public void deletePersistence(@Suspended final AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                  @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalDeletePersistenceAsync()
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to delete the persistence for a namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/persistence/bookieAffinity")
    @Operation(summary = "Set the bookie-affinity-group to namespace-persistent policy.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "307", description = "Current broker doesn't serve the namespace"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification")})
    public void setBookieAffinityGroup(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                       @PathParam("namespace") String namespace,
                                       @RequestBody(description = "Bookie affinity group for the specified namespace")
                                       BookieAffinityGroupData bookieAffinityGroup) {
        validateNamespaceName(tenant, namespace);
        internalSetBookieAffinityGroupAsync(bookieAffinityGroup)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to set bookie affinity group for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/persistence/bookieAffinity")
    @Operation(summary = "Get the bookie-affinity-group from namespace-local policy.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Get the bookie-affinity-group from namespace-local policy.",
                    content = @Content(schema = @Schema(implementation = BookieAffinityGroupDataImpl.class))),
            @ApiResponse(responseCode = "307", description = "Current broker doesn't serve the namespace"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification") })
    public void getBookieAffinityGroup(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                       @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetBookieAffinityGroupAsync()
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get bookie affinity group for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/persistence/bookieAffinity")
    @Operation(summary = "Delete the bookie-affinity-group from namespace-local policy.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification") })
    public void deleteBookieAffinityGroup(@Suspended AsyncResponse asyncResponse,
                                          @PathParam("tenant") String tenant,
                                          @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalDeleteBookieAffinityGroupAsync()
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to delete bookie affinity group for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/persistence")
    @Operation(summary = "Get the persistence configuration for a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the persistence configuration for a namespace.",
                    content = @Content(schema = @Schema(implementation = PersistencePolicies.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification") })
    public void getPersistence(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.PERSISTENCE, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.persistence))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get persistence configuration for a namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/clearBacklog")
    @Operation(summary = "Clear backlog for all topics on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin or operate permission on the namespace"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist") })
    public void clearNamespaceBacklog(@Suspended final AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        internalClearNamespaceBacklogAsync(authoritative)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to clear backlog on namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/{bundle}/clearBacklog")
    @Operation(summary = "Clear backlog for all topics on a namespace bundle.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "307", description = "Current broker doesn't serve the namespace"),
            @ApiResponse(responseCode = "403", description = "Don't have admin or operate permission on the namespace"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist") })
    public void clearNamespaceBundleBacklog(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        internalClearNamespaceBundleBacklogAsync(bundleRange, authoritative)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .attr("bundleRange", bundleRange)
                            .exception(ex)
                            .log("Failed to clear backlog on namespace bundle");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/clearBacklog/{subscription}")
    @Operation(summary = "Clear backlog for a given subscription on all topics on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin or operate permission on the namespace"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist") })
    public void clearNamespaceBacklogForSubscription(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("subscription") String subscription,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        internalClearNamespaceBacklogForSubscriptionAsync(subscription, authoritative)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("subscription", subscription)
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to clear backlog for subscription on namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/{bundle}/clearBacklog/{subscription}")
    @Operation(summary = "Clear backlog for a given subscription on all topics on a namespace bundle.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "307", description = "Current broker doesn't serve the namespace"),
            @ApiResponse(responseCode = "403", description = "Don't have admin or operate permission on the namespace"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist") })
    public void clearNamespaceBundleBacklogForSubscription(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("subscription") String subscription,
            @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        internalClearNamespaceBundleBacklogForSubscriptionAsync(subscription, bundleRange, authoritative)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("subscription", subscription)
                            .attr("namespace", namespaceName)
                            .attr("bundleRange", bundleRange)
                            .exception(ex)
                            .log("Failed to clear backlog for subscription on namespace bundle");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/unsubscribe/{subscription}")
    @Operation(summary = "Unsubscribes the given subscription on all topics on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403",
                    description = "Don't have admin or operate permission on the namespace"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist") })
    public void unsubscribeNamespace(@Suspended final AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("subscription") String subscription,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        internalUnsubscribeNamespaceAsync(subscription, authoritative)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("subscription", subscription)
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to unsubscribe on namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/{bundle}/unsubscribe/{subscription}")
    @Operation(summary = "Unsubscribes the given subscription on all topics on a namespace bundle.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin or operate permission on the namespace"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist") })
    public void unsubscribeNamespaceBundle(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("subscription") String subscription,
            @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        internalUnsubscribeNamespaceBundleAsync(subscription, bundleRange, authoritative)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("subscription", subscription)
                            .attr("namespace", namespaceName)
                            .attr("bundleRange", bundleRange)
                            .exception(ex)
                            .log("Failed to unsubscribe on namespace bundle");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/subscriptionAuthMode")
    @Operation(summary = "Set a subscription auth mode for all the topics on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification")})
    public void setSubscriptionAuthMode(@PathParam("tenant") String tenant,
                                        @PathParam("namespace") String namespace, @RequestBody(description =
            "Subscription auth mode for all topics of the specified namespace")
                                                    SubscriptionAuthMode subscriptionAuthMode) {
        validateNamespaceName(tenant, namespace);
        internalSetSubscriptionAuthMode(subscriptionAuthMode);
    }

    @GET
    @Path("/{tenant}/{namespace}/subscriptionAuthMode")
    @Operation(summary = "Get subscription auth mode in a namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get subscription auth mode in a namespace",
                    content = @Content(schema = @Schema(implementation = SubscriptionAuthMode.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or namespace doesn't exist")})
    public void getSubscriptionAuthMode(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.SUBSCRIPTION_AUTH_MODE, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.subscription_auth_mode))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get subscription auth mode in a namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/encryptionRequired")
    @Operation(summary = "Message encryption is required or not for all topics in a namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"), })
    public void modifyEncryptionRequired(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @RequestBody(description = "Flag defining if message encryption is required", required = true)
                    boolean encryptionRequired) {
        validateNamespaceName(tenant, namespace);
        internalModifyEncryptionRequired(encryptionRequired);
    }

    @GET
    @Path("/{tenant}/{namespace}/encryptionRequired")
    @Operation(summary = "Get message encryption required status in a namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get message encryption required status in a namespace",
                    content = @Content(schema = @Schema(implementation = Boolean.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or namespace doesn't exist")})
    public void getEncryptionRequired(@Suspended AsyncResponse asyncResponse,
                                      @PathParam("tenant") String tenant,
                                      @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.ENCRYPTION, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.encryption_required))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get message encryption required status in a namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/delayedDelivery")
    @Operation(summary = "Get delayed delivery messages config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get delayed delivery messages config on a namespace.",
                    content = @Content(schema = @Schema(implementation = DelayedDeliveryPolicies.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"), })
    public void getDelayedDeliveryPolicies(@Suspended final AsyncResponse asyncResponse,
                                           @PathParam("tenant") String tenant,
                                           @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.DELAYED_DELIVERY, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.delayed_delivery_policies))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get delayed delivery messages config on a namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/delayedDelivery")
    @Operation(summary = "Set delayed delivery messages config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"), })
    public void setDelayedDeliveryPolicies(@PathParam("tenant") String tenant,
                                           @PathParam("namespace") String namespace,
                                           @RequestBody(description = "Delayed delivery policies for the specified "
                                                   + "namespace")
                                                   DelayedDeliveryPolicies deliveryPolicies) {
        validateNamespaceName(tenant, namespace);
        internalSetDelayedDelivery(deliveryPolicies);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/delayedDelivery")
    @Operation(summary = "Delete delayed delivery messages config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"), })
    public void removeDelayedDeliveryPolicies(@PathParam("tenant") String tenant,
                                           @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetDelayedDelivery(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/inactiveTopicPolicies")
    @Operation(summary = "Get inactive topic policies config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get inactive topic policies config on a namespace.",
                    content = @Content(schema = @Schema(implementation = InactiveTopicPolicies.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"), })
    public void getInactiveTopicPolicies(@Suspended final AsyncResponse asyncResponse,
                                         @PathParam("tenant") String tenant,
                                         @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.INACTIVE_TOPIC, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.inactive_topic_policies))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get inactive topic policies config on a namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/inactiveTopicPolicies")
    @Operation(summary = "Remove inactive topic policies from a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification")})
    public void removeInactiveTopicPolicies(@PathParam("tenant") String tenant,
                                            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetInactiveTopic(null);
    }

    @POST
    @Path("/{tenant}/{namespace}/inactiveTopicPolicies")
    @Operation(summary = "Set inactive topic policies config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"), })
    public void setInactiveTopicPolicies(@PathParam("tenant") String tenant,
                                         @PathParam("namespace") String namespace,
                                         @RequestBody(description = "Inactive topic policies for the specified "
                                                 + "namespace")
                                                 InactiveTopicPolicies inactiveTopicPolicies) {
        validateNamespaceName(tenant, namespace);
        internalSetInactiveTopic(inactiveTopicPolicies);
    }

    @GET
    @Path("/{tenant}/{namespace}/maxProducersPerTopic")
    @Operation(summary = "Get maxProducersPerTopic config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get maxProducersPerTopic config on a namespace.",
                    content = @Content(schema = @Schema(implementation = Integer.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist") })
    public void getMaxProducersPerTopic(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.MAX_PRODUCERS, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.max_producers_per_topic))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get maxProducersPerTopic config on a namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/maxProducersPerTopic")
    @Operation(summary = "Set maxProducersPerTopic configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "412", description = "maxProducersPerTopic value is not valid") })
    public void setMaxProducersPerTopic(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @RequestBody(description = "Number of maximum producers per topic", required = true)
                    int maxProducersPerTopic) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxProducersPerTopic(maxProducersPerTopic);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/maxProducersPerTopic")
    @Operation(summary = "Remove maxProducersPerTopic configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification") })
    public void removeMaxProducersPerTopic(@PathParam("tenant") String tenant,
                                               @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxProducersPerTopic(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/deduplicationSnapshotInterval")
    @Operation(summary = "Get deduplicationSnapshotInterval config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get deduplicationSnapshotInterval config on a namespace.",
                    content = @Content(schema = @Schema(implementation = Integer.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist") })
    public void getDeduplicationSnapshotInterval(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.DEDUPLICATION_SNAPSHOT, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.deduplicationSnapshotIntervalSeconds))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get deduplicationSnapshotInterval config on a namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/deduplicationSnapshotInterval")
    @Operation(summary = "Set deduplicationSnapshotInterval config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist")})
    public void setDeduplicationSnapshotInterval(@PathParam("tenant") String tenant
            , @PathParam("namespace") String namespace
            , @RequestBody(description = "Interval to take deduplication snapshot per topic", required = true)
                                                         Integer interval) {
        validateNamespaceName(tenant, namespace);
        internalSetDeduplicationSnapshotInterval(interval);
    }

    @GET
    @Path("/{tenant}/{namespace}/maxConsumersPerTopic")
    @Operation(summary = "Get maxConsumersPerTopic config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get maxConsumersPerTopic config on a namespace.",
                    content = @Content(schema = @Schema(implementation = Integer.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist") })
    public void getMaxConsumersPerTopic(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.MAX_CONSUMERS, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.max_consumers_per_topic))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get maxConsumersPerTopic config on a namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/maxConsumersPerTopic")
    @Operation(summary = "Set maxConsumersPerTopic configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "412", description = "maxConsumersPerTopic value is not valid") })
    public void setMaxConsumersPerTopic(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @RequestBody(description = "Number of maximum consumers per topic", required = true)
                    int maxConsumersPerTopic) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxConsumersPerTopic(maxConsumersPerTopic);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/maxConsumersPerTopic")
    @Operation(summary = "Remove maxConsumersPerTopic configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification") })
    public void removeMaxConsumersPerTopic(@PathParam("tenant") String tenant,
                                               @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxConsumersPerTopic(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/maxConsumersPerSubscription")
    @Operation(summary = "Get maxConsumersPerSubscription config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get maxConsumersPerSubscription config on a namespace.",
                    content = @Content(schema = @Schema(implementation = Integer.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist") })
    public void getMaxConsumersPerSubscription(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.MAX_CONSUMERS, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(polices -> asyncResponse.resume(polices.max_consumers_per_subscription))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exceptionMessage(ex.getCause())
                            .exception(ex)
                            .log("Failed to get maxConsumersPerSubscription config on namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/maxConsumersPerSubscription")
    @Operation(summary = "Set maxConsumersPerSubscription configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "412", description = "maxConsumersPerSubscription value is not valid")})
    public void setMaxConsumersPerSubscription(@PathParam("tenant") String tenant,
                                               @PathParam("namespace") String namespace,
                                               @RequestBody(description = "Number of maximum consumers per "
                                                       + "subscription", required = true)
                                                           int maxConsumersPerSubscription) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxConsumersPerSubscription(maxConsumersPerSubscription);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/maxConsumersPerSubscription")
    @Operation(summary = "Set maxConsumersPerSubscription configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "412", description = "maxConsumersPerSubscription value is not valid")})
    public void removeMaxConsumersPerSubscription(@PathParam("tenant") String tenant,
                                               @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxConsumersPerSubscription(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/maxUnackedMessagesPerConsumer")
    @Operation(summary = "Get maxUnackedMessagesPerConsumer config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get maxUnackedMessagesPerConsumer config on a namespace.",
                    content = @Content(schema = @Schema(implementation = Integer.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist") })
    public void getMaxUnackedMessagesPerConsumer(@Suspended final AsyncResponse asyncResponse,
                                                 @PathParam("tenant") String tenant,
                                                 @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.MAX_UNACKED, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.max_unacked_messages_per_consumer))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get maxUnackedMessagesPerConsumer config on a namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/maxUnackedMessagesPerConsumer")
    @Operation(summary = "Set maxConsumersPerTopic configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "412", description = "maxUnackedMessagesPerConsumer value is not valid")})
    public void setMaxUnackedMessagesPerConsumer(@PathParam("tenant") String tenant,
                                                 @PathParam("namespace") String namespace,
                                                 @RequestBody(description = "Number of maximum unacked messages "
                                                         + "per consumer", required = true)
                                                             int maxUnackedMessagesPerConsumer) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxUnackedMessagesPerConsumer(maxUnackedMessagesPerConsumer);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/maxUnackedMessagesPerConsumer")
    @Operation(summary = "Remove maxUnackedMessagesPerConsumer config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist") })
    public void removeMaxUnackedmessagesPerConsumer(@PathParam("tenant") String tenant,
                                                        @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxUnackedMessagesPerConsumer(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/maxUnackedMessagesPerSubscription")
    @Operation(summary = "Get maxUnackedMessagesPerSubscription config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Get maxUnackedMessagesPerSubscription config on a namespace.",
                    content = @Content(schema = @Schema(implementation = Integer.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist") })
    public void getMaxUnackedmessagesPerSubscription(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.MAX_UNACKED, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.max_unacked_messages_per_subscription))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get maxUnackedMessagesPerSubscription config on a namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/maxUnackedMessagesPerSubscription")
    @Operation(summary = "Set maxUnackedMessagesPerSubscription configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "412", description = "maxUnackedMessagesPerSubscription value is not valid")})
    public void setMaxUnackedMessagesPerSubscription(
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @RequestBody(description = "Number of maximum unacked messages per subscription", required = true)
                    int maxUnackedMessagesPerSubscription) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxUnackedMessagesPerSubscription(maxUnackedMessagesPerSubscription);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/maxUnackedMessagesPerSubscription")
    @Operation(summary = "Remove maxUnackedMessagesPerSubscription config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist") })
    public void removeMaxUnackedmessagesPerSubscription(@PathParam("tenant") String tenant,
                                                        @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxUnackedMessagesPerSubscription(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/maxSubscriptionsPerTopic")
    @Operation(summary = "Get maxSubscriptionsPerTopic config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get maxSubscriptionsPerTopic config on a namespace.",
                    content = @Content(schema = @Schema(implementation = Integer.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist") })
    public void getMaxSubscriptionsPerTopic(@Suspended final AsyncResponse asyncResponse,
                                            @PathParam("tenant") String tenant,
                                            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.MAX_SUBSCRIPTIONS, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.max_subscriptions_per_topic))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get maxSubscriptionsPerTopic config on a namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/maxSubscriptionsPerTopic")
    @Operation(summary = "Set maxSubscriptionsPerTopic configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "412", description = "maxUnackedMessagesPerSubscription value is not valid")})
    public void setMaxSubscriptionsPerTopic(
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @RequestBody(description = "Number of maximum subscriptions per topic", required = true)
                    int maxSubscriptionsPerTopic) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxSubscriptionsPerTopic(maxSubscriptionsPerTopic);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/maxSubscriptionsPerTopic")
    @Operation(summary = "Remove maxSubscriptionsPerTopic configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification") })
    public void removeMaxSubscriptionsPerTopic(@PathParam("tenant") String tenant,
                                                 @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxSubscriptionsPerTopic(null);
    }

    @POST
    @Path("/{tenant}/{namespace}/antiAffinity")
    @Operation(summary = "Set anti-affinity group for a namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "412", description = "Invalid antiAffinityGroup")})
    public void setNamespaceAntiAffinityGroup(@Suspended AsyncResponse asyncResponse,
                                              @PathParam("tenant") String tenant,
                                              @PathParam("namespace") String namespace,
                                              @RequestBody(description = "Anti-affinity group for the specified "
                                                      + "namespace", required = true)
                                              String antiAffinityGroup) {
        validateNamespaceName(tenant, namespace);
        internalSetNamespaceAntiAffinityGroupAsync(antiAffinityGroup)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("tenant", tenant)
                            .attr("namespace", namespace)
                            .attr("antiAffinityGroup", antiAffinityGroup)
                            .exception(ex)
                            .log("Failed to set namespace anti-affinity group");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/antiAffinity")
    @Operation(summary = "Get anti-affinity group of a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get anti-affinity group of a namespace.",
                    content = @Content(schema = @Schema(implementation = String.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist") })
    public void getNamespaceAntiAffinityGroup(@Suspended AsyncResponse asyncResponse,
                                                @PathParam("tenant") String tenant,
                                                @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetNamespaceAntiAffinityGroupAsync()
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error()
                            .attr("tenant", tenant)
                            .attr("namespace", namespace)
                            .exception(ex)
                            .log("Failed to get namespace anti-affinity group");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/antiAffinity")
    @Operation(summary = "Remove anti-affinity group of a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification") })
    public void removeNamespaceAntiAffinityGroup(@Suspended AsyncResponse asyncResponse,
                                                 @PathParam("tenant") String tenant,
                                                 @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalRemoveNamespaceAntiAffinityGroupAsync()
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("tenant", tenant)
                            .attr("namespace", namespace)
                            .exception(ex)
                            .log("Failed to remove namespace anti-affinity group");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("{cluster}/antiAffinity/{group}")
    @Operation(summary = "Get all namespaces that are grouped by the given anti-affinity group in a given cluster."
            + " This API can only be accessed by an admin of any of the existing tenants.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Get all namespaces that are grouped by the given anti-affinity group in a given"
                            + " cluster. This API can only be accessed by an admin of any of the existing tenants.",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "412", description = "Cluster not exist/Anti-affinity group can't be empty.")})
    public void getAntiAffinityNamespaces(@Suspended AsyncResponse asyncResponse,
                                                  @PathParam("cluster") String cluster,
                                                  @PathParam("group") String antiAffinityGroup,
                                                  @QueryParam("tenant") String tenant) {
        internalGetAntiAffinityNamespacesAsync(cluster, antiAffinityGroup, tenant)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error()
                            .attr("cluster", cluster)
                            .attr("tenant", tenant)
                            .attr("antiAffinityGroup", antiAffinityGroup)
                            .exception(ex)
                            .log("Failed to get all namespaces in cluster of given anti-affinity group");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/compactionThreshold")
    @Operation(summary = "Maximum number of uncompacted bytes in topics before compaction is triggered.",
                  description = "The backlog size is compared to the threshold periodically. "
                          + "A threshold of 0 disables automatic compaction")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Maximum number of uncompacted bytes in topics before compaction is triggered.",
                    content = @Content(schema = @Schema(implementation = Long.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist") })
    public void getCompactionThreshold(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.COMPACTION, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.compaction_threshold))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get compaction threshold on namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/compactionThreshold")
    @Operation(summary = "Set maximum number of uncompacted bytes in a topic before compaction is triggered.",
            description = "The backlog size is compared to the threshold periodically. "
                    + "A threshold of 0 disables automatic compaction")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "412", description = "compactionThreshold value is not valid")})
    public void setCompactionThreshold(@PathParam("tenant") String tenant,
                                       @PathParam("namespace") String namespace,
                                       @RequestBody(description = "Maximum number of uncompacted bytes"
                                               + " in a topic of the specified namespace",
                                               required = true) long newThreshold) {
        validateNamespaceName(tenant, namespace);
        internalSetCompactionThreshold(newThreshold);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/compactionThreshold")
    @Operation(summary = "Delete maximum number of uncompacted bytes in a topic before compaction is triggered.",
            description = "The backlog size is compared to the threshold periodically. "
                    + "A threshold of 0 disables automatic compaction")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification")})
    public void deleteCompactionThreshold(@PathParam("tenant") String tenant,
                                       @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetCompactionThreshold(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/offloadThreshold")
    @Operation(summary = "Maximum number of bytes stored on the pulsar cluster for a topic,"
                          + " before the broker will start offloading to longterm storage",
                  description = "A negative value disables automatic offloading")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Maximum number of bytes stored on the pulsar cluster for a topic,"
                            + " before the broker will start offloading to longterm storage",
                    content = @Content(schema = @Schema(implementation = Long.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist") })
    public void getOffloadThreshold(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.OFFLOAD, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> {
                    if (policies.offload_policies == null
                            || policies.offload_policies.getManagedLedgerOffloadThresholdInBytes() == null) {
                        asyncResponse.resume(policies.offload_threshold);
                    } else {
                        asyncResponse.resume(policies.offload_policies.getManagedLedgerOffloadThresholdInBytes());
                    }
                })
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get offload threshold on namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/offloadThreshold")
    @Operation(summary = "Set maximum number of bytes stored on the pulsar cluster for a topic,"
            + " before the broker will start offloading to longterm storage",
            description = "-1 will revert to using the cluster default."
                    + " A negative value disables automatic offloading. ")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "412", description = "offloadThreshold value is not valid")})
    public void setOffloadThreshold(@PathParam("tenant") String tenant,
                                    @PathParam("namespace") String namespace,
                                    @RequestBody(description =
                                            "Maximum number of bytes stored on the pulsar cluster"
                                                    + " for a topic of the specified namespace",
                                            required = true) long newThreshold) {
        validateNamespaceName(tenant, namespace);
        internalSetOffloadThreshold(newThreshold);
    }

    @GET
    @Path("/{tenant}/{namespace}/offloadThresholdInSeconds")
    @Operation(summary = "Maximum number of bytes stored on the pulsar cluster for a topic,"
            + " before the broker will start offloading to longterm storage",
            description = "A negative value disables automatic offloading")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Maximum number of bytes stored on the pulsar cluster for a topic,"
                            + " before the broker will start offloading to longterm storage",
                    content = @Content(schema = @Schema(implementation = Long.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist") })
    public void getOffloadThresholdInSeconds(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.OFFLOAD, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> {
                    if (policies.offload_policies == null
                            || policies.offload_policies.getManagedLedgerOffloadThresholdInSeconds() == null) {
                        asyncResponse.resume(policies.offload_threshold_in_seconds);
                    } else {
                        asyncResponse.resume(policies.offload_policies.getManagedLedgerOffloadThresholdInSeconds());
                    }
                })
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get offload threshold on namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/offloadThresholdInSeconds")
    @Operation(summary = "Set maximum number of seconds stored on the pulsar cluster for a topic,"
            + " before the broker will start offloading to longterm storage",
            description = "A negative value disables automatic offloading")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "412", description = "offloadThresholdInSeconds value is not valid") })
    public void setOffloadThresholdInSeconds(
            @Suspended final AsyncResponse response,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            long newThreshold) {
        validateNamespaceName(tenant, namespace);
        internalSetOffloadThresholdInSecondsAsync(newThreshold)
                .thenAccept(response::resume)
                .exceptionally(t -> {
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/offloadDeletionLagMs")
    @Operation(summary = "Number of milliseconds to wait before deleting a ledger segment which has been offloaded"
                          + " from the Pulsar cluster's local storage (i.e. BookKeeper)",
                  description = "A negative value denotes that deletion has been completely disabled."
                          + " 'null' denotes that the topics in the namespace will fall back to the"
                          + " broker default for deletion lag.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Number of milliseconds to wait before deleting a ledger segment which has been "
                            + "offloaded from the Pulsar cluster's local storage (i.e. BookKeeper)",
                    content = @Content(schema = @Schema(implementation = Long.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist") })
    public void getOffloadDeletionLag(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.OFFLOAD, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> {
                    if (policies.offload_policies == null) {
                        asyncResponse.resume(policies.offload_deletion_lag_ms);
                    } else {
                        asyncResponse.resume(policies.offload_policies.getManagedLedgerOffloadDeletionLagInMillis());
                    }
                })
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get offload deletion lag milliseconds on namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/offloadDeletionLagMs")
    @Operation(summary = "Set number of milliseconds to wait before deleting a ledger segment which has been offloaded"
            + " from the Pulsar cluster's local storage (i.e. BookKeeper)",
            description = "A negative value disables the deletion completely.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "412", description = "offloadDeletionLagMs value is not valid")})
    public void setOffloadDeletionLag(@PathParam("tenant") String tenant,
                                      @PathParam("namespace") String namespace,
                                      @RequestBody(description =
                                              "New number of milliseconds to wait before deleting a ledger segment"
                                                      + " which has been offloaded",
                                              required = true) long newDeletionLagMs) {
        validateNamespaceName(tenant, namespace);
        internalSetOffloadDeletionLag(newDeletionLagMs);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/offloadDeletionLagMs")
    @Operation(summary = "Clear the namespace configured offload deletion lag. The topics in the namespace"
                          + " will fallback to using the default configured deletion lag for the broker")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification") })
    public void clearOffloadDeletionLag(@PathParam("tenant") String tenant,
                                        @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetOffloadDeletionLag(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/schemaAutoUpdateCompatibilityStrategy")
    @Operation(summary = "The strategy used to check the compatibility of new schemas,"
                          + " provided by producers, before automatically updating the schema",
                  description = "The value AutoUpdateDisabled prevents producers from updating the schema. "
                          + " If set to AutoUpdateDisabled, schemas must be updated through the REST api")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "The strategy used to check the compatibility of new schemas,"
                            + " provided by producers, before automatically updating the schema",
                    content = @Content(schema =
                            @Schema(implementation = SchemaAutoUpdateCompatibilityStrategy.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification") })
    @SuppressWarnings("deprecation")
    public SchemaAutoUpdateCompatibilityStrategy getSchemaAutoUpdateCompatibilityStrategy(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetSchemaAutoUpdateCompatibilityStrategy();
    }

    @PUT
    @Path("/{tenant}/{namespace}/schemaAutoUpdateCompatibilityStrategy")
    @Operation(summary = "Update the strategy used to check the compatibility of new schemas,"
            + " provided by producers, before automatically updating the schema",
            description = "The value AutoUpdateDisabled prevents producers from updating the schema. "
                    + " If set to AutoUpdateDisabled, schemas must be updated through the REST api")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification")})
    @SuppressWarnings("deprecation")
    public void setSchemaAutoUpdateCompatibilityStrategy(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @RequestBody(description = "Strategy used to check the compatibility of new schemas")
                    SchemaAutoUpdateCompatibilityStrategy strategy) {
        validateNamespaceName(tenant, namespace);
        internalSetSchemaAutoUpdateCompatibilityStrategy(strategy);
    }

    @GET
    @Path("/{tenant}/{namespace}/schemaCompatibilityStrategy")
    @Operation(summary = "The strategy of the namespace schema compatibility ")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The strategy of the namespace schema compatibility ",
                    content = @Content(schema = @Schema(implementation = SchemaCompatibilityStrategy.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification") })
    public void getSchemaCompatibilityStrategy(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.SCHEMA_COMPATIBILITY_STRATEGY,
                PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.schema_compatibility_strategy))
                .exceptionally(ex -> {
                    log.error()
                            .attr("compatibility", namespaceName)
                            .exception(ex)
                            .log("Failed to get the strategy of the namespace schema compatibility");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/schemaCompatibilityStrategy")
    @Operation(summary = "Update the strategy used to check the compatibility of new schema")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification")})
    public void setSchemaCompatibilityStrategy(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @RequestBody(description = "Strategy used to check the compatibility of new schema")
                    SchemaCompatibilityStrategy strategy) {
        validateNamespaceName(tenant, namespace);
        internalSetSchemaCompatibilityStrategy(strategy);
    }

    @GET
    @Path("/{tenant}/{namespace}/isAllowAutoUpdateSchema")
    @Operation(summary = "The flag of whether to allow auto update schema")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The flag of whether to allow auto update schema",
                    content = @Content(schema = @Schema(implementation = Boolean.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification") })
    public void getIsAllowAutoUpdateSchema(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.SCHEMA_COMPATIBILITY_STRATEGY,
                PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> {
                    if (policies.is_allow_auto_update_schema == null) {
                        asyncResponse.resume(pulsar().getConfig().isAllowAutoUpdateSchemaEnabled());
                    } else {
                        asyncResponse.resume(policies.is_allow_auto_update_schema);
                    }
                })
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get the flag of whether allow auto update schema on a namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/isAllowAutoUpdateSchema")
    @Operation(summary = "Update flag of whether to allow auto update schema")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification")})
    public void setIsAllowAutoUpdateSchema(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @QueryParam("allowAutoUpdateSchemaWithReplicator")
            @Parameter(description = "Allow replicator to auto update schema")
                    Boolean allowAutoUpdateSchemaWithReplicator,
            @RequestBody(description = "Flag of whether to allow auto update schema", required = true)
                    boolean isAllowAutoUpdateSchema) {
        validateNamespaceName(tenant, namespace);
        if (isAllowAutoUpdateSchema && allowAutoUpdateSchemaWithReplicator != null
                && !allowAutoUpdateSchemaWithReplicator) {
            throw new RestException(Response.Status.BAD_REQUEST, "Can not enable for all producers but denies for"
                    + " replicators, which is meaningless");
        }
        internalSetIsAllowAutoUpdateSchema(isAllowAutoUpdateSchema, allowAutoUpdateSchemaWithReplicator);
    }

    @GET
    @Path("/{tenant}/{namespace}/subscriptionTypesEnabled")
    @Operation(summary = "The set of enabled subscription types")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The set of enabled subscription types",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = SubscriptionType.class),
                            uniqueItems = true))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification") })
    public void getSubscriptionTypesEnabled(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.SUBSCRIPTION_AUTH_MODE, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> {
                    Set<SubscriptionType> subscriptionTypes = new HashSet<>();
                    policies.subscription_types_enabled.forEach(
                            subType -> subscriptionTypes.add(SubscriptionType.valueOf(subType)));
                    asyncResponse.resume(subscriptionTypes);
                })
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get the set of whether allow subscription types on a namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/subscriptionTypesEnabled")
    @Operation(summary = "Update the set of enabled subscription types")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification")})
    public void setSubscriptionTypesEnabled(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @RequestBody(description = "Set of enabled subscription types", required = true)
                    Set<SubscriptionType> subscriptionTypesEnabled) {
        validateNamespaceName(tenant, namespace);
        internalSetSubscriptionTypesEnabled(subscriptionTypesEnabled);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/subscriptionTypesEnabled")
    @Operation(summary = "Remove subscription types enabled on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification")})
    public void removeSubscriptionTypesEnabled(@PathParam("tenant") String tenant,
                                               @PathParam("namespace") String namespace) {
            validateNamespaceName(tenant, namespace);
            internalSetSubscriptionTypesEnabled(new HashSet<>());
    }

    @GET
    @Path("/{tenant}/{namespace}/allowedTopicPropertyKeysForMetrics")
    @Operation(summary = "Get allowed topic property keys for metrics for a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Get allowed topic property keys for metrics for a namespace.",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class),
                            uniqueItems = true))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification") })
    public void getAllowedTopicPropertyKeysForMetrics(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.ALLOW_CUSTOM_METRIC_LABELS,
            PolicyOperation.READ)
            .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
            .thenAccept(policies -> asyncResponse.resume(policies.allowed_topic_property_keys_for_metrics))
            .exceptionally(ex -> {
                log.error()
                        .attr("namespace", namespaceName)
                        .exception(ex)
                        .log("Failed to get allowed topic property keys for metrics for namespace");
                resumeAsyncResponseExceptionally(asyncResponse, ex);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/allowedTopicPropertyKeysForMetrics")
    @Operation(summary = "Set allowed topic property keys for metrics for a namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification")})
    public void setAllowedTopicPropertyKeysForMetrics(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @RequestBody(description = "Set of allowed topic property keys for metrics", required = true)
                    Set<String> allowedKeys) {
        validateNamespaceName(tenant, namespace);
        internalSetAllowedTopicPropertyKeysForMetricsAsync(allowedKeys)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to set allowed topic property keys for metrics for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/allowedTopicPropertyKeysForMetrics")
    @Operation(summary = "Remove allowed topic property keys for metrics on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification")})
    public void removeAllowedTopicPropertyKeysForMetrics(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetAllowedTopicPropertyKeysForMetricsAsync(null)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to remove allowed topic property keys for metrics for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/schemaValidationEnforced")
    @Operation(summary = "Get schema validation enforced flag for namespace.",
                  description = "If the flag is set to true, when a producer without a schema attempts to produce "
                          + "to a topic with schema in this namespace, the producer will fail to connect. "
                          + "PLEASE be careful when using this, since non-Java clients don't support schema. If you "
                          + "enable this setting, it will cause non-Java clients to fail to produce.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get schema validation enforced flag for namespace.",
                    content = @Content(schema = @Schema(implementation = Boolean.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenants or Namespace doesn't exist") })
    public void getSchemaValidtionEnforced(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @QueryParam("applied") @DefaultValue("false") boolean applied) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.SCHEMA_COMPATIBILITY_STRATEGY,
                PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> {
                    boolean schemaValidationEnforced = policies.schema_validation_enforced;
                    if (!schemaValidationEnforced && applied) {
                        asyncResponse.resume(pulsar().getConfiguration().isSchemaValidationEnforced());
                    } else {
                        asyncResponse.resume(schemaValidationEnforced);
                    }
                })
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get schema validation enforced flag for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/schemaValidationEnforced")
    @Operation(summary = "Set schema validation enforced flag on namespace.",
            description = "If the flag is set to true, when a producer without a schema attempts to produce to a topic"
                    + " with schema in this namespace, the producer will fail to connect. PLEASE be"
                    + " careful when using this, since non-Java clients don't support schema. If you enable"
                    + " this setting, it will cause non-Java clients to fail to produce.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or Namespace doesn't exist"),
            @ApiResponse(responseCode = "412", description = "schemaValidationEnforced value is not valid")})
    public void setSchemaValidationEnforced(@PathParam("tenant") String tenant,
                                           @PathParam("namespace") String namespace,
                                           @RequestBody(description =
                                                   "Flag of whether validation is enforced on the specified namespace",
                                                   required = true)
                                                       boolean schemaValidationEnforced) {
        validateNamespaceName(tenant, namespace);
        internalSetSchemaValidationEnforced(schemaValidationEnforced);
    }

    @POST
    @Path("/{tenant}/{namespace}/offloadPolicies")
    @Operation(summary = "Set offload configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "412",
                    description = "OffloadPolicies is empty or driver is not supported or bucket is not valid")})
    public void setOffloadPolicies(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
                                   @RequestBody(description = "Offload policies for the specified namespace",
                                           required = true)
                                           OffloadPoliciesImpl offload,
                                   @Suspended final AsyncResponse asyncResponse) {
        try {
            validateNamespaceName(tenant, namespace);
            internalSetOffloadPolicies(asyncResponse, offload);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @DELETE
    @Path("/{tenant}/{namespace}/removeOffloadPolicies")
    @Operation(summary = "Set offload configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "412",
                    description = "OffloadPolicies is empty or driver is not supported or bucket is not valid")})
    public void removeOffloadPolicies(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
                                      @Suspended final AsyncResponse asyncResponse) {
        try {
            validateNamespaceName(tenant, namespace);
            internalRemoveOffloadPolicies(asyncResponse);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("/{tenant}/{namespace}/offloadPolicies")
    @Operation(summary = "Get offload configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get offload configuration on a namespace.",
                    content = @Content(schema = @Schema(implementation = OffloadPolicies.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist")})
    public void getOffloadPolicies(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.OFFLOAD, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.offload_policies))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get offload policies on a namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/maxTopicsPerNamespace")
    @Operation(summary = "Get maxTopicsPerNamespace config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get maxTopicsPerNamespace config on a namespace.",
                    content = @Content(schema = @Schema(implementation = Integer.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or namespace does not exist") })
    public void getMaxTopicsPerNamespace(@Suspended final AsyncResponse asyncResponse,
                                         @PathParam("tenant") String tenant,
                                         @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.MAX_TOPICS, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> {
                    int maxTopicsPerNamespace =
                            policies.max_topics_per_namespace != null ? policies.max_topics_per_namespace : 0;
                    asyncResponse.resume(maxTopicsPerNamespace);
                })
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get maxTopicsPerNamespace config on a namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/maxTopicsPerNamespace")
    @Operation(summary = "Set maxTopicsPerNamespace config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or namespace doesn't exist"), })
    public void setMaxTopicsPerNamespace(@PathParam("tenant") String tenant,
                                         @PathParam("namespace") String namespace,
                                         @RequestBody(description = "Number of maximum topics for specific namespace",
                                                 required = true) int maxTopicsPerNamespace) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxTopicsPerNamespace(maxTopicsPerNamespace);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/maxTopicsPerNamespace")
    @Operation(summary = "Remove maxTopicsPerNamespace config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or namespace doesn't exist"), })
    public void removeMaxTopicsPerNamespace(@PathParam("tenant") String tenant,
                                         @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalRemoveMaxTopicsPerNamespace();
    }

    @PUT
    @Path("/{tenant}/{namespace}/property/{key}/{value}")
    @Operation(summary = "Put a key value pair property on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or namespace doesn't exist"), })
    public void setProperty(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("key") String key,
            @PathParam("value") String value) {
        validateNamespaceName(tenant, namespace);
        internalSetProperty(key, value, asyncResponse);
    }

    @GET
    @Path("/{tenant}/{namespace}/property/{key}")
    @Operation(summary = "Get property value for a given key on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get property value for a given key on a namespace.",
                    content = @Content(schema = @Schema(implementation = String.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or namespace doesn't exist"), })
    public void getProperty(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("key") String key) {
        validateNamespaceName(tenant, namespace);
        internalGetProperty(key, asyncResponse);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/property/{key}")
    @Operation(summary = "Remove property value for a given key on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or namespace doesn't exist"), })
    public void removeProperty(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("key") String key) {
        validateNamespaceName(tenant, namespace);
        internalRemoveProperty(key, asyncResponse);
    }

    @PUT
    @Path("/{tenant}/{namespace}/properties")
    @Operation(summary = "Put key value pairs property on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or namespace doesn't exist"), })
    public void setProperties(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @RequestBody(description = "Key value pair properties for the namespace", required = true)
                    Map<String, String> properties) {
        validateNamespaceName(tenant, namespace);
        internalSetProperties(properties, asyncResponse);
    }

    @GET
    @Path("/{tenant}/{namespace}/properties")
    @Operation(summary = "Get key value pair properties for a given namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get key value pair properties for a given namespace.",
                    content = @Content(schema = @Schema(type = "object", additionalPropertiesSchema = String.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or namespace doesn't exist"), })
    public void getProperties(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetProperties(asyncResponse);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/properties")
    @Operation(summary = "Clear properties on a given namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or namespace doesn't exist"), })
    public void clearProperties(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalClearProperties(asyncResponse);
    }

    @GET
    @Path("/{tenant}/{namespace}/resourcegroup")
    @Operation(summary = "Get the resource group attached to the namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the resource group attached to the namespace",
                    content = @Content(schema = @Schema(implementation = String.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist") })
    public void getNamespaceResourceGroup(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(NamespaceName.get(tenant, namespace), PolicyName.RESOURCEGROUP,
                PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.resource_group_name))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to get the resource group attached to the namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/resourcegroup/{resourcegroup}")
    @Operation(summary = "Set resourcegroup for a namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "412", description = "Invalid resourcegroup") })
    public void setNamespaceResourceGroup(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
                                          @PathParam("resourcegroup") String rgName) {
        validateNamespaceName(tenant, namespace);
        internalSetNamespaceResourceGroup(rgName);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/resourcegroup")
    @Operation(summary = "Delete resourcegroup for a namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "412", description = "Invalid resourcegroup")})
    public void removeNamespaceResourceGroup(@PathParam("tenant") String tenant,
                                          @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetNamespaceResourceGroup(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/scanOffloadedLedgers")
    @Operation(summary = "Trigger the scan of offloaded Ledgers on the LedgerOffloader for the given namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successful get of offloaded ledger data",
                content = @Content(mediaType = "application/json",
                        schema = @Schema(implementation = String.class),
                        examples = @ExampleObject(
                                value = "{\"objects\":[{\"key1\":\"value1\",\"key2\":\"value2\"}],"
                                        + "\"total\":100,\"errors\":5,\"unknown\":3}"))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist") })
    public Response scanOffloadedLedgers(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        try {
            StreamingOutput output = (outputStream) -> {
                try {
                    OutputStreamWriter out = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
                    out.append("{objects:[\n");
                    internalScanOffloadedLedgers(new OffloaderObjectsScannerUtils.ScannerResultSink() {
                        boolean first = true;
                        @Override
                        public void object(Map<String, Object> data) throws Exception {
                            if (!first) {
                                out.write(',');
                            } else {
                                first = true;
                            }
                            String json = objectWriter().writeValueAsString(data);
                            out.write(json);
                        }

                        @Override
                        public void finished(int total, int errors, int unknown) throws Exception {
                            out.append("]\n");
                            out.append("\"total\": " + total + ",\n");
                            out.append("\"errors\": " + errors + ",\n");
                            out.append("\"unknown\": " + unknown + "\n");
                        }
                    });
                    out.append("}");
                    out.flush();
                    outputStream.flush();
                } catch (Exception err) {
                    log.error().exception(err).log("error");
                    throw new RuntimeException(err);
                }
            };
            return Response.ok(output).type(MediaType.APPLICATION_JSON_TYPE).build();
        } catch (Throwable err) {
            log.error()
                    .attr("namespace", namespaceName)
                    .exception(err)
                    .log("Error while scanning offloaded ledgers for namespace");
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR,
                    "Error while scanning ledgers for " + namespaceName);
        }
    }

    @GET
    @Path("/{tenant}/{namespace}/entryFilters")
    @Operation(summary = "Get maxConsumersPerSubscription config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get maxConsumersPerSubscription config on a namespace.",
                    content = @Content(schema = @Schema(implementation = EntryFilters.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist") })
    public void getEntryFiltersPerTopic(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.ENTRY_FILTERS, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(polices -> asyncResponse.resume(polices.entryFilters))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exceptionMessage(ex.getCause())
                            .exception(ex)
                            .log("Failed to get entry filters config on namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/entryFilters")
    @Operation(summary = "Set entry filters for namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "400", description = "Specified entry filters are not valid"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist")
    })
    public void setEntryFiltersPerTopic(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                       @PathParam("namespace") String namespace,
                                       @RequestBody(description = "entry filters", required = true)
                                               EntryFilters entryFilters) {
        validateNamespaceName(tenant, namespace);
        internalSetEntryFiltersPerTopicAsync(entryFilters)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to set entry filters for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/entryFilters")
    @Operation(summary = "Remove entry filters for namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "412", description = "Invalid TTL")})
    public void removeNamespaceEntryFilters(@Suspended AsyncResponse asyncResponse,
                                          @PathParam("tenant") String tenant,
                                          @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetEntryFiltersPerTopicAsync(null)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("namespace", namespaceName)
                            .exception(ex)
                            .log("Failed to remove entry filters for namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/migration")
    @Operation(hidden = true, summary = "Update migration for all topics in a namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Property or cluster or namespace doesn't exist") })
    public void enableMigration(@Suspended AsyncResponse asyncResponse,
                                @PathParam("tenant") String tenant,
                                @PathParam("namespace") String namespace,
                                boolean migrated) {
        validateNamespaceName(tenant, namespace);
        internalEnableMigrationAsync(migrated)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error()
                            .attr("tenant", tenant)
                            .attr("namespace", namespace)
                            .exception(ex)
                            .log("Failed to update migration");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/dispatcherPauseOnAckStatePersistent")
    @Operation(summary = "Set dispatcher pause on ack state persistent configuration for specified namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification")})
    public void setDispatcherPauseOnAckStatePersistent(@Suspended final AsyncResponse asyncResponse,
                                                       @PathParam("tenant") String tenant,
                                                       @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetDispatcherPauseOnAckStatePersistentAsync(true)
                .thenRun(() -> {
                    log.info()
                            .attr("namespace", namespaceName)
                            .log("Successfully enabled dispatcherPauseOnAckStatePersistent: namespace");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/dispatcherPauseOnAckStatePersistent")
    @Operation(summary = "Remove dispatcher pause on ack state persistent configuration for specified namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification")})
    public void removeDispatcherPauseOnAckStatePersistent(@Suspended final AsyncResponse asyncResponse,
                                                          @PathParam("tenant") String tenant,
                                                          @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetDispatcherPauseOnAckStatePersistentAsync(false)
                .thenRun(() -> {
                    log.info()
                            .attr("namespace", namespaceName)
                            .log("Successfully remove dispatcherPauseOnAckStatePersistent: namespace");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/dispatcherPauseOnAckStatePersistent")
    @Operation(summary = "Get dispatcher pause on ack state persistent config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Get dispatcher pause on ack state persistent config on a namespace.",
                    content = @Content(schema = @Schema(implementation = Boolean.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist") })
    public void getDispatcherPauseOnAckStatePersistent(@Suspended final AsyncResponse asyncResponse,
                                                       @PathParam("tenant") String tenant,
                                                       @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetDispatcherPauseOnAckStatePersistentAsync()
                .thenApply(asyncResponse::resume)
                .exceptionally(ex -> {
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/allowedClusters")
    @Operation(summary = "Set the allowed clusters for a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "400",
                    description = "The list of allowed clusters should include all replication clusters."),
            @ApiResponse(responseCode = "403", description = "The requester does not have admin permissions."),
            @ApiResponse(responseCode = "404",
                    description = "The specified tenant, cluster, or namespace does not exist."),
            @ApiResponse(responseCode = "409", description = "A peer-cluster cannot be part of an allowed-cluster."),
            @ApiResponse(responseCode = "412",
                    description = "The namespace is not global or the provided cluster IDs are invalid.")})
    public void setNamespaceAllowedClusters(@Suspended AsyncResponse asyncResponse,
                                                @PathParam("tenant") String tenant,
                                                @PathParam("namespace") String namespace,
                                                @RequestBody(description = "List of allowed clusters", required = true)
                                                List<String> clusterIds) {
        validateNamespaceName(tenant, namespace);
        internalSetNamespaceAllowedClusters(clusterIds)
                .thenAccept(asyncResponse::resume)
                .exceptionally(e -> {
                    log.error()
                            .attr("namespace", namespace)
                            .exception(e)
                            .log("Failed to set namespace allowed clusters on namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, e);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/allowedClusters")
    @Operation(summary = "Get the allowed clusters for a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the allowed clusters for a namespace.",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(responseCode = "412", description = "Namespace is not global")})
    public void getNamespaceAllowedClusters(@Suspended AsyncResponse asyncResponse,
                                                @PathParam("tenant") String tenant,
                                                @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetNamespaceAllowedClustersAsync()
                .thenAccept(asyncResponse::resume)
                .exceptionally(e -> {
                    log.error()
                            .attr("namespace", namespace)
                            .exception(e)
                            .log("Failed to get namespace allowed clusters on namespace");
                    resumeAsyncResponseExceptionally(asyncResponse, e);
                    return null;
                });
    }
}
