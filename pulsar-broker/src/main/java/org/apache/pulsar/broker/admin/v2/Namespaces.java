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

import static org.apache.pulsar.common.policies.data.PoliciesUtil.getBundles;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Example;
import io.swagger.annotations.ExampleProperty;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
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
import javax.ws.rs.core.StreamingOutput;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.admin.impl.NamespacesBase;
import org.apache.pulsar.broker.admin.impl.OffloaderObjectsScannerUtils;
import org.apache.pulsar.broker.web.RestException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/namespaces")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(value = "/namespaces", description = "Namespaces admin apis", tags = "namespaces")
public class Namespaces extends NamespacesBase {

    @GET
    @Path("/{tenant}")
    @ApiOperation(value = "Get the list of all the namespaces for a certain tenant.",
            response = String.class, responseContainer = "Set")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant doesn't exist")})
    public void getTenantNamespaces(@Suspended final AsyncResponse response,
                                    @PathParam("tenant") String tenant) {
        internalGetTenantNamespaces(tenant)
                .thenAccept(response::resume)
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get namespaces list: {}", clientAppId(), ex);
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/topics")
    @ApiOperation(value = "Get the list of all the topics under a certain namespace.",
            response = String.class, responseContainer = "Set")
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "Don't have admin or operate permission on the namespace"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist")})
    public void getTopics(@Suspended AsyncResponse response,
                          @PathParam("tenant") String tenant,
                          @PathParam("namespace") String namespace,
                          @QueryParam("mode") @DefaultValue("PERSISTENT") Mode mode,
                          @ApiParam(value = "Include system topic")
                          @QueryParam("includeSystemTopic") boolean includeSystemTopic) {
        validateNamespaceName(tenant, namespace);
        validateNamespaceOperationAsync(NamespaceName.get(tenant, namespace), NamespaceOperation.GET_TOPICS)
                // Validate that namespace exists, throws 404 if it doesn't exist
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenCompose(policies -> internalGetListOfTopics(policies, mode))
                .thenApply(topics -> filterSystemTopic(topics, includeSystemTopic))
                .thenAccept(response::resume)
                .exceptionally(ex -> {
                    log.error("Failed to get topics list for namespace {}", namespaceName, ex);
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}")
    @ApiOperation(value = "Get the dump all the policies specified for a namespace.", response = Policies.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
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
                        log.info("Failed to get policies for namespace {}: {}", namespaceName, ex.getMessage());
                    } else {
                        log.error("Failed to get policies for namespace {}", namespaceName, ex);
                    }
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}")
    @ApiOperation(value = "Creates a new namespace with the specified policies")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster doesn't exist"),
            @ApiResponse(code = 409, message = "Namespace already exists"),
            @ApiResponse(code = 412, message = "Namespace name is not valid") })
    public void createNamespace(@Suspended AsyncResponse response,
                                @PathParam("tenant") String tenant,
                                @PathParam("namespace") String namespace,
                                @ApiParam(value = "Policies for the namespace") Policies policies) {
        validateNamespaceName(tenant, namespace);
        policies = getDefaultPolicesIfNull(policies);
        internalCreateNamespace(policies)
                .thenAccept(__ -> response.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    Throwable root = FutureUtil.unwrapCompletionException(ex);
                    if (root instanceof MetadataStoreException.AlreadyExistsException) {
                        response.resume(new RestException(Response.Status.CONFLICT, "Namespace already exists"));
                    } else {
                        log.error("[{}] Failed to create namespace {}", clientAppId(), namespaceName, ex);
                        resumeAsyncResponseExceptionally(response, ex);
                    }
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}")
    @ApiOperation(value = "Delete a namespace and all the topics under it.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 405, message = "Broker doesn't allow forced deletion of namespaces"),
            @ApiResponse(code = 409, message = "Namespace is not empty") })
    public void deleteNamespace(@Suspended final AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                @PathParam("namespace") String namespace,
                                @QueryParam("force") @DefaultValue("false") boolean force,
                                @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        internalDeleteNamespaceAsync(force)
                .thenAccept(__ -> {
                    log.info("[{}] Successful delete namespace {}", clientAppId(), namespace);
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to delete namespace {}", clientAppId(), namespaceName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{bundle}")
    @ApiOperation(value = "Delete a namespace bundle and all the topics under it.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Namespace bundle is not empty")})
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
                        log.error("[{}] Failed to delete namespace bundle {}", clientAppId(), namespaceName, ex);
                    }
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/permissions")
    @ApiOperation(value = "Retrieve the permissions for a namespace.",
            notes = "Returns a nested map structure which Swagger does not fully support for display. "
                    + "Structure: Map<String, Set<AuthAction>>. Please refer to this structure for details.",
            response = AuthAction.class, responseContainer = "Map")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Namespace is not empty") })
    public void getPermissions(@Suspended AsyncResponse response,
                                                       @PathParam("tenant") String tenant,
                                                       @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespaceOperationAsync(NamespaceName.get(tenant, namespace), NamespaceOperation.GET_PERMISSION)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> response.resume(policies.auth_policies.getNamespaceAuthentication()))
                .exceptionally(ex -> {
                    log.error("Failed to get permissions for namespace {}", namespaceName, ex);
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/permissions/subscription")
    @ApiOperation(value = "Retrieve the permissions for a subscription.",
            notes = "Returns a nested map structure which Swagger does not fully support for display. "
            + "Structure: Map<String, Set<String>>. Please refer to this structure for details.",
            response = String.class, responseContainer = "Map")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Namespace is not empty")})
    public void getPermissionOnSubscription(@Suspended AsyncResponse response,
                                            @PathParam("tenant") String tenant,
                                            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespaceOperationAsync(NamespaceName.get(tenant, namespace), NamespaceOperation.GET_PERMISSION)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> response.resume(policies.auth_policies.getSubscriptionAuthentication()))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get permissions on subscription for namespace {}: {} ", clientAppId(),
                            namespaceName, ex.getCause().getMessage(), ex);
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/permissions/{role}")
    @ApiOperation(value = "Grant a new permission to a role on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 501, message = "Authorization is not enabled")})
    public void grantPermissionOnNamespace(@Suspended AsyncResponse asyncResponse,
                                           @PathParam("tenant") String tenant,
                                           @PathParam("namespace") String namespace,
                                           @PathParam("role") String role,
            @ApiParam(value = "List of permissions for the specified role") Set<AuthAction> actions) {
        validateNamespaceName(tenant, namespace);
        internalGrantPermissionOnNamespaceAsync(role, actions)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to set permissions for namespace {}: {}",
                            clientAppId(), namespaceName, ex.getCause().getMessage(), ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{property}/{namespace}/permissions/subscription/{subscription}")
    @ApiOperation(hidden = true, value = "Grant a new permission to roles for a subscription."
            + "[Tenant admin is allowed to perform this operation]")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 501, message = "Authorization is not enabled") })
    public void grantPermissionOnSubscription(@Suspended AsyncResponse asyncResponse,
                                              @PathParam("property") String property,
                                              @PathParam("namespace") String namespace,
                                              @PathParam("subscription") String subscription,
            @ApiParam(value = "List of roles for the specified subscription") Set<String> roles) {
        validateNamespaceName(property, namespace);
        internalGrantPermissionOnSubscriptionAsync(subscription, roles)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to grant permission on subscription for role {}:{} - "
                                    + "namespaceName {}: {}",
                            clientAppId(), roles, subscription, namespaceName, ex.getCause().getMessage(), ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/permissions/{role}")
    @ApiOperation(value = "Revoke all permissions to a role on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
    public void revokePermissionsOnNamespace(@Suspended AsyncResponse asyncResponse,
                                             @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("role") String role) {
        validateNamespaceName(tenant, namespace);
        internalRevokePermissionsOnNamespaceAsync(role)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to revoke permission on role {} - namespace {}: {}",
                            clientAppId(), role, namespace, ex.getCause().getMessage(), ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{property}/{namespace}/permissions/{subscription}/{role}")
    @ApiOperation(hidden = true, value = "Revoke subscription admin-api access permission for a role.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist") })
    public void revokePermissionOnSubscription(@Suspended AsyncResponse asyncResponse,
                                               @PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("subscription") String subscription,
            @PathParam("role") String role) {
        validateNamespaceName(property, namespace);
        internalRevokePermissionsOnSubscriptionAsync(subscription, role)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to revoke permission on subscription for role {}:{} - namespace {}: {}",
                            clientAppId(), role, subscription, namespace, ex.getCause().getMessage(), ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/replication")
    @ApiOperation(value = "Get the replication clusters for a namespace.",
            response = String.class, responseContainer = "Set")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Namespace is not global")})
    public void getNamespaceReplicationClusters(@Suspended AsyncResponse asyncResponse,
                                                @PathParam("tenant") String tenant,
                                                @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetNamespaceReplicationClustersAsync()
                .thenAccept(asyncResponse::resume)
                .exceptionally(e -> {
                    log.error("[{}] Failed to get namespace replication clusters on namespace {}", clientAppId(),
                            namespace, e);
                    resumeAsyncResponseExceptionally(asyncResponse, e);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/replication")
    @ApiOperation(value = "Set the replication clusters for a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Peer-cluster can't be part of replication-cluster"),
            @ApiResponse(code = 412, message = "Namespace is not global or invalid cluster ids") })
    public void setNamespaceReplicationClusters(@Suspended AsyncResponse asyncResponse,
                                                @PathParam("tenant") String tenant,
                                                @PathParam("namespace") String namespace,
            @ApiParam(value = "List of replication clusters", required = true) List<String> clusterIds) {
        validateNamespaceName(tenant, namespace);
        internalSetNamespaceReplicationClusters(clusterIds)
                .thenAccept(asyncResponse::resume)
                .exceptionally(e -> {
                    log.error("[{}] Failed to set namespace replication clusters on namespace {}",
                            clientAppId(), namespace, e);
                    resumeAsyncResponseExceptionally(asyncResponse, e);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/messageTTL")
    @ApiOperation(value = "Get the message TTL for the namespace", response = Integer.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
    public void getNamespaceMessageTTL(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(NamespaceName.get(tenant, namespace), PolicyName.TTL,
                PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.message_ttl_in_seconds))
                .exceptionally(ex -> {
                    log.error("Failed to get namespace message TTL for namespace {}", namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/messageTTL")
    @ApiOperation(value = "Set message TTL in seconds for namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Invalid TTL") })
    public void setNamespaceMessageTTL(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                       @PathParam("namespace") String namespace,
                                       @ApiParam(value = "TTL in seconds for the specified namespace", required = true)
                                               int messageTTL) {
        validateNamespaceName(tenant, namespace);
        internalSetNamespaceMessageTTLAsync(messageTTL)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("Failed to set namespace message TTL for namespace {}", namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/messageTTL")
    @ApiOperation(value = "Remove message TTL in seconds for namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Invalid TTL")})
    public void removeNamespaceMessageTTL(@Suspended AsyncResponse asyncResponse,
                                          @PathParam("tenant") String tenant,
                                          @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetNamespaceMessageTTLAsync(null)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("Failed to remove namespace message TTL for namespace {}", namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/subscriptionExpirationTime")
    @ApiOperation(value = "Get the subscription expiration time for the namespace", response = Integer.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
    public void getSubscriptionExpirationTime(@Suspended AsyncResponse asyncResponse,
                                                 @PathParam("tenant") String tenant,
                                                 @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.SUBSCRIPTION_EXPIRATION_TIME,
                PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.subscription_expiration_time_minutes))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get subscription expiration time for namespace {}: {} ", clientAppId(),
                            namespaceName, ex.getCause().getMessage(), ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/subscriptionExpirationTime")
    @ApiOperation(value = "Set subscription expiration time in minutes for namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Invalid expiration time")})
    public void setSubscriptionExpirationTime(@Suspended AsyncResponse asyncResponse,
                                              @PathParam("tenant") String tenant,
                                              @PathParam("namespace") String namespace,
                                              @ApiParam(value =
                                                      "Expiration time in minutes for the specified namespace",
                                                      required = true) int expirationTime) {
        validateNamespaceName(tenant, namespace);
        internalSetSubscriptionExpirationTimeAsync(expirationTime)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to set subscription expiration time for namespace {}: {} ", clientAppId(),
                            namespaceName, ex.getCause().getMessage(), ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/subscriptionExpirationTime")
    @ApiOperation(value = "Remove subscription expiration time for namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist")})
    public void removeSubscriptionExpirationTime(@Suspended AsyncResponse asyncResponse,
                                                 @PathParam("tenant") String tenant,
                                                 @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetSubscriptionExpirationTimeAsync(null)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to remove subscription expiration time for namespace {}: {} ", clientAppId(),
                            namespaceName, ex.getCause().getMessage(), ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/deduplication")
    @ApiOperation(value = "Get broker side deduplication for all topics in a namespace", response = Boolean.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
    public void getDeduplication(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                 @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetDeduplicationAsync()
                .thenAccept(deduplication -> asyncResponse.resume(deduplication))
                .exceptionally(ex -> {
                    log.error("Failed to get broker deduplication config for namespace {}", namespace, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/deduplication")
    @ApiOperation(value = "Enable or disable broker side deduplication for all topics in a namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
    public void modifyDeduplication(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                    @PathParam("namespace") String namespace,
                                    @ApiParam(value = "Flag for disabling or enabling broker side deduplication "
                                            + "for all topics in the specified namespace", required = true)
                                            boolean enableDeduplication) {
        validateNamespaceName(tenant, namespace);
        internalModifyDeduplicationAsync(enableDeduplication)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("Failed to modify broker deduplication config for namespace {}", namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/deduplication")
    @ApiOperation(value = "Remove broker side deduplication for all topics in a namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
    public void removeDeduplication(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                    @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalModifyDeduplicationAsync(null)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(e -> {
                    Throwable ex = FutureUtil.unwrapCompletionException(e);
                    log.error("Failed to remove broker deduplication config for namespace {}", namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/autoTopicCreation")
    @ApiOperation(value = "Get autoTopicCreation info in a namespace", response = AutoTopicCreationOverrideImpl.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or namespace doesn't exist")})
    public void getAutoTopicCreation(@Suspended AsyncResponse asyncResponse,
                                                          @PathParam("tenant") String tenant,
                                                          @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetAutoTopicCreationAsync()
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error("Failed to get autoTopicCreation info for namespace {}", namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/autoTopicCreation")
    @ApiOperation(value = "Override broker's allowAutoTopicCreation setting for a namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 406, message = "The number of partitions should be less than or"
                    + " equal to maxNumPartitionsPerPartitionedTopic"),
            @ApiResponse(code = 400, message = "Invalid autoTopicCreation override")})
    public void setAutoTopicCreation(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @ApiParam(value = "Settings for automatic topic creation", required = true)
                    AutoTopicCreationOverride autoTopicCreationOverride) {
        validateNamespaceName(tenant, namespace);
        internalSetAutoTopicCreationAsync(autoTopicCreationOverride)
                .thenAccept(__ -> {
                    String autoOverride = (autoTopicCreationOverride != null
                            && autoTopicCreationOverride.isAllowAutoTopicCreation()) ? "enabled" : "disabled";
                    log.info("[{}] Successfully {} autoTopicCreation on namespace {}", clientAppId(),
                            autoOverride, namespaceName);
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(e -> {
                    Throwable ex = FutureUtil.unwrapCompletionException(e);
                    log.error("[{}] Failed to set autoTopicCreation status on namespace {}", clientAppId(),
                            namespaceName,
                            ex);
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
    @ApiOperation(value = "Remove override of broker's allowAutoTopicCreation in a namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
    public void removeAutoTopicCreation(@Suspended final AsyncResponse asyncResponse,
                                        @PathParam("tenant") String tenant, @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetAutoTopicCreationAsync(null)
                .thenAccept(__ -> {
                    log.info("[{}] Successfully remove autoTopicCreation on namespace {}",
                            clientAppId(), namespaceName);
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(e -> {
                    Throwable ex = FutureUtil.unwrapCompletionException(e);
                    log.error("[{}] Failed to remove autoTopicCreation status on namespace {}", clientAppId(),
                            namespaceName,
                            ex);
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
    @ApiOperation(value = "Override broker's allowAutoSubscriptionCreation setting for a namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 400, message = "Invalid autoSubscriptionCreation override")})
    public void setAutoSubscriptionCreation(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @ApiParam(value = "Settings for automatic subscription creation")
                    AutoSubscriptionCreationOverride autoSubscriptionCreationOverride) {
        validateNamespaceName(tenant, namespace);
        internalSetAutoSubscriptionCreationAsync(autoSubscriptionCreationOverride)
                .thenAccept(__ -> {
                    log.info("[{}] Successfully set autoSubscriptionCreation on namespace {}",
                            clientAppId(), namespaceName);
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(e -> {
                    Throwable ex = FutureUtil.unwrapCompletionException(e);
                    log.error("[{}] Failed to set autoSubscriptionCreation on namespace {}", clientAppId(),
                            namespaceName, ex);
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
    @ApiOperation(value = "Get autoSubscriptionCreation info in a namespace",
            response = AutoSubscriptionCreationOverrideImpl.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or namespace doesn't exist")})
    public void getAutoSubscriptionCreation(@Suspended final AsyncResponse asyncResponse,
                                                                        @PathParam("tenant") String tenant,
                                                                        @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetAutoSubscriptionCreationAsync()
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error("Failed to get autoSubscriptionCreation for namespace {}", namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/autoSubscriptionCreation")
    @ApiOperation(value = "Remove override of broker's allowAutoSubscriptionCreation in a namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
    public void removeAutoSubscriptionCreation(@Suspended final AsyncResponse asyncResponse,
                                        @PathParam("tenant") String tenant, @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetAutoSubscriptionCreationAsync(null)
                .thenAccept(__ -> {
                    log.info("[{}] Successfully set autoSubscriptionCreation on namespace {}",
                            clientAppId(), namespaceName);
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(e -> {
                    Throwable ex = FutureUtil.unwrapCompletionException(e);
                    log.error("[{}] Failed to set autoSubscriptionCreation on namespace {}", clientAppId(),
                            namespaceName, ex);
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
    @ApiOperation(value = "Get the bundles split data.", response = BundlesDataImpl.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Namespace is not setup to split in bundles") })
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
                    log.error("[{}] Failed to get bundle data for namespace {}", clientAppId(),
                            namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/unload")
    @ApiOperation(value = "Unload namespace",
            notes = "Unload an active namespace from the current broker serving it. Performing this operation will"
                    + " let the brokerremoves all producers, consumers, and connections using this namespace,"
                    + " and close all topics (includingtheir persistent store). During that operation,"
                    + " the namespace is marked as tentatively unavailable until thebroker completes "
                    + "the unloading action. This operation requires strictly super user privileges,"
                    + " since it wouldresult in non-persistent message loss and"
                    + " unexpected connection closure to the clients.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Namespace is already unloaded or Namespace has bundles activated")})
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
                    log.info("[{}] Successfully unloaded all the bundles in namespace {}", clientAppId(),
                            namespaceName);
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to unload namespace {}", clientAppId(), namespaceName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/{bundle}/unload")
    @ApiOperation(value = "Unload a namespace bundle")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void unloadNamespaceBundle(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
                                      @QueryParam("destinationBroker") String destinationBroker) {
        validateNamespaceName(tenant, namespace);
        internalUnloadNamespaceBundleAsync(bundleRange, destinationBroker, authoritative)
                .thenAccept(__ -> {
                    log.info("[{}] Successfully unloaded namespace bundle {}",
                            clientAppId(), bundleRange);
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to unload namespace bundle {}/{}",
                                clientAppId(), namespaceName, bundleRange, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/{bundle}/split")
    @ApiOperation(value = "Split a namespace bundle")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void splitNamespaceBundle(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("unload") @DefaultValue("false") boolean unload,
            @QueryParam("splitAlgorithmName") String splitAlgorithmName,
            @ApiParam("splitBoundaries") List<Long> splitBoundaries) {
        validateNamespaceName(tenant, namespace);
        internalSplitNamespaceBundleAsync(bundleRange, authoritative, unload, splitAlgorithmName, splitBoundaries)
                .thenAccept(__ -> {
                    log.info("[{}] Successfully split namespace bundle {}", clientAppId(), bundleRange);
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to split namespace bundle {}/{} due to {}",
                                clientAppId(), namespaceName, bundleRange, ex.getMessage());
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
    @ApiOperation(value = "Get hash positions for topics", response = TopicHashPositions.class)
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist")})
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
                        log.error("[{}] {} Failed to get topic list for bundle {}.", clientAppId(),
                                namespaceName, bundleRange);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{property}/{namespace}/publishRate")
    @ApiOperation(hidden = true, value = "Set publish-rate throttling for all topics of the namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void setPublishRate(@Suspended AsyncResponse asyncResponse, @PathParam("property") String property,
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Publish rate for all topics of the specified namespace") PublishRate publishRate) {
        validateNamespaceName(property, namespace);
        internalSetPublishRateAsync(publishRate)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{property}/{namespace}/publishRate")
    @ApiOperation(hidden = true, value = "Set publish-rate throttling for all topics of the namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void removePublishRate(@Suspended AsyncResponse asyncResponse, @PathParam("property") String property,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, namespace);
        internalRemovePublishRateAsync()
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to remove the publish_max_message_rate for cluster on namespace {}",
                            clientAppId(), namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{property}/{namespace}/publishRate")
    @ApiOperation(hidden = true,
            value = "Get publish-rate configured for the namespace, null means publish-rate not configured, "
                    + "-1 means msg-publish-rate or byte-publish-rate not configured in publish-rate yet",
            response = PublishRate.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist")})
    public void getPublishRate(@Suspended AsyncResponse asyncResponse,
            @PathParam("property") String property,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, namespace);
        internalGetPublishRateAsync()
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error("Failed to get publish rate for namespace {}", namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/dispatchRate")
    @ApiOperation(value = "Set dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void setDispatchRate(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Dispatch rate for all topics of the specified namespace")
                    DispatchRateImpl dispatchRate) {
        validateNamespaceName(tenant, namespace);
        internalSetTopicDispatchRateAsync(dispatchRate)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to update the dispatchRate for cluster on namespace {}", clientAppId(),
                            namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/dispatchRate")
    @ApiOperation(value = "Delete dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void deleteDispatchRate(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalDeleteTopicDispatchRateAsync()
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to delete the dispatchRate for cluster on namespace {}", clientAppId(),
                            namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/dispatchRate")
    @ApiOperation(value = "Get dispatch-rate configured for the namespace, null means dispatch-rate not configured, "
            + "-1 means msg-dispatch-rate or byte-dispatch-rate not configured in dispatch-rate yet",
            response = DispatchRate.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
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
    @ApiOperation(value = "Set Subscription dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission")})
    public void setSubscriptionDispatchRate(@Suspended AsyncResponse asyncResponse,
                                            @PathParam("tenant") String tenant,
                                            @PathParam("namespace") String namespace,
                                            @ApiParam(value =
                                            "Subscription dispatch rate for all topics of the specified namespace")
                                                        DispatchRateImpl dispatchRate) {
        validateNamespaceName(tenant, namespace);
        internalSetSubscriptionDispatchRateAsync(dispatchRate)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to update the subscription dispatchRate for cluster on namespace {}",
                            clientAppId(), namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/subscriptionDispatchRate")
    @ApiOperation(value = "Get subscription dispatch-rate configured for the namespace, null means subscription "
            + "dispatch-rate not configured, -1 means msg-dispatch-rate or byte-dispatch-rate not configured "
            + "in dispatch-rate yet", response = DispatchRate.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist")})
    public void getSubscriptionDispatchRate(@Suspended AsyncResponse asyncResponse,
                                                    @PathParam("tenant") String tenant,
                                                    @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetSubscriptionDispatchRateAsync()
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get the subscription dispatchRate for cluster on namespace {}",
                            clientAppId(), namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/subscriptionDispatchRate")
    @ApiOperation(value = "Delete Subscription dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void deleteSubscriptionDispatchRate(@Suspended AsyncResponse asyncResponse,
                                               @PathParam("tenant") String tenant,
                                               @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalDeleteSubscriptionDispatchRateAsync()
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("Failed to delete the subscription dispatchRate for cluster on namespace {}",
                            clientAppId(), namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/subscribeRate")
    @ApiOperation(value = "Delete subscribe-rate throttling for all topics of the namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission")})
    public void deleteSubscribeRate(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                    @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalDeleteSubscribeRateAsync()
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to delete the subscribeRate for cluster on namespace {}",
                            clientAppId(), namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/subscribeRate")
    @ApiOperation(value = "Set subscribe-rate throttling for all topics of the namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission")})
    public void setSubscribeRate(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                 @PathParam("namespace") String namespace,
                                 @ApiParam(value = "Subscribe rate for all topics of the specified namespace")
                                         SubscribeRate subscribeRate) {
        validateNamespaceName(tenant, namespace);
        internalSetSubscribeRateAsync(subscribeRate)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to update the subscribeRate for cluster on namespace {}",
                            clientAppId(), namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/subscribeRate")
    @ApiOperation(value = "Get subscribe-rate configured for the namespace", response = SubscribeRate.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist")})
    public void getSubscribeRate(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                 @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetSubscribeRateAsync()
                .thenAccept(subscribeRate -> asyncResponse.resume(subscribeRate))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get subscribe rate for namespace {}", clientAppId(), namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/replicatorDispatchRate")
    @ApiOperation(value = "Remove replicator dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission")})
    public void removeReplicatorDispatchRate(@Suspended AsyncResponse asyncResponse,
                                             @PathParam("tenant") String tenant,
                                             @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalRemoveReplicatorDispatchRate(asyncResponse);
    }

    @POST
    @Path("/{tenant}/{namespace}/replicatorDispatchRate")
    @ApiOperation(value = "Set replicator dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission")})
    public void setReplicatorDispatchRate(@Suspended AsyncResponse asyncResponse,
                                          @PathParam("tenant") String tenant,
                                          @PathParam("namespace") String namespace,
                                          @ApiParam(value =
            "Replicator dispatch rate for all topics of the specified namespace") DispatchRateImpl dispatchRate) {
        validateNamespaceName(tenant, namespace);
        internalSetReplicatorDispatchRate(asyncResponse, dispatchRate);
    }

    @GET
    @Path("/{tenant}/{namespace}/replicatorDispatchRate")
    @ApiOperation(value = "Get replicator dispatch-rate configured for the namespace, null means replicator "
            + "dispatch-rate not configured, -1 means msg-dispatch-rate or byte-dispatch-rate not configured "
            + "in dispatch-rate yet", response = DispatchRateImpl.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
    @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void getReplicatorDispatchRate(@Suspended final AsyncResponse asyncResponse,
                                          @PathParam("tenant") String tenant,
                                          @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetReplicatorDispatchRate(asyncResponse);
    }

    @GET
    @Path("/{tenant}/{namespace}/backlogQuotaMap")
    @ApiOperation(value = "Get backlog quota map on a namespace.",
            response = BacklogQuotaImpl.class, responseContainer = "Map")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void getBacklogQuotaMap(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetBacklogQuotaMap(asyncResponse);
    }

    @POST
    @Path("/{tenant}/{namespace}/backlogQuota")
    @ApiOperation(value = " Set a backlog quota for all the topics on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412,
                    message = "Specified backlog quota exceeds retention quota."
                            + " Increase retention quota and retry request")})
    public void setBacklogQuota(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @QueryParam("backlogQuotaType") BacklogQuotaType backlogQuotaType,
            @ApiParam(value = "Backlog quota for all topics of the specified namespace") BacklogQuota backlogQuota) {
        validateNamespaceName(tenant, namespace);
        internalSetBacklogQuota(asyncResponse, backlogQuotaType, backlogQuota);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/backlogQuota")
    @ApiOperation(value = "Remove a backlog quota policy from a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void removeBacklogQuota(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @QueryParam("backlogQuotaType") BacklogQuotaType backlogQuotaType) {
        validateNamespaceName(tenant, namespace);
        internalRemoveBacklogQuota(asyncResponse, backlogQuotaType);
    }

    @GET
    @Path("/{tenant}/{namespace}/retention")
    @ApiOperation(value = "Get retention config on a namespace.", response = RetentionPolicies.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void getRetention(@Suspended final AsyncResponse asyncResponse,
                             @PathParam("tenant") String tenant,
                             @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.RETENTION, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.retention_policies))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get retention config on a namespace {}", clientAppId(), namespaceName,
                            ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/retention")
    @ApiOperation(value = " Set retention configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Retention Quota must exceed backlog quota") })
    public void setRetention(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @ApiParam(value = "Retention policies for the specified namespace") RetentionPolicies retention) {
        validateNamespaceName(tenant, namespace);
        internalSetRetention(retention);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/retention")
    @ApiOperation(value = " Remove retention configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Retention Quota must exceed backlog quota") })
    public void removeRetention(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @ApiParam(value = "Retention policies for the specified namespace") RetentionPolicies retention) {
        validateNamespaceName(tenant, namespace);
        internalSetRetention(null);
    }

    @POST
    @Path("/{tenant}/{namespace}/persistence")
    @ApiOperation(value = "Set the persistence configuration for all the topics on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 400, message = "Invalid persistence policies")})
    public void setPersistence(@Suspended final AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                               @PathParam("namespace") String namespace,
                               @ApiParam(value = "Persistence policies for the specified namespace", required = true)
                                       PersistencePolicies persistence) {
        validateNamespaceName(tenant, namespace);
        internalSetPersistenceAsync(persistence)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to update the persistence for a namespace {}", clientAppId(), namespaceName,
                            ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/persistence")
    @ApiOperation(value = "Delete the persistence configuration for all topics on a namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void deletePersistence(@Suspended final AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                  @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalDeletePersistenceAsync()
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to delete the persistence for a namespace {}", clientAppId(), namespaceName,
                            ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/persistence/bookieAffinity")
    @ApiOperation(value = "Set the bookie-affinity-group to namespace-persistent policy.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void setBookieAffinityGroup(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
                                       @ApiParam(value = "Bookie affinity group for the specified namespace")
                                               BookieAffinityGroupData bookieAffinityGroup) {
        validateNamespaceName(tenant, namespace);
        internalSetBookieAffinityGroup(bookieAffinityGroup);
    }

    @GET
    @Path("/{property}/{namespace}/persistence/bookieAffinity")
    @ApiOperation(value = "Get the bookie-affinity-group from namespace-local policy.",
            response = BookieAffinityGroupDataImpl.class)
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public BookieAffinityGroupData getBookieAffinityGroup(@PathParam("property") String property,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, namespace);
        return internalGetBookieAffinityGroup();
    }

    @DELETE
    @Path("/{property}/{namespace}/persistence/bookieAffinity")
    @ApiOperation(value = "Delete the bookie-affinity-group from namespace-local policy.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void deleteBookieAffinityGroup(@PathParam("property") String property,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, namespace);
        internalDeleteBookieAffinityGroup();
    }

    @GET
    @Path("/{tenant}/{namespace}/persistence")
    @ApiOperation(value = "Get the persistence configuration for a namespace.", response = PersistencePolicies.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void getPersistence(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.PERSISTENCE, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.persistence))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get persistence configuration for a namespace {}", clientAppId(),
                            namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/clearBacklog")
    @ApiOperation(value = "Clear backlog for all topics on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin or operate permission on the namespace"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void clearNamespaceBacklog(@Suspended final AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateNamespaceName(tenant, namespace);
            internalClearNamespaceBacklog(asyncResponse, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{bundle}/clearBacklog")
    @ApiOperation(value = "Clear backlog for all topics on a namespace bundle.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin or operate permission on the namespace"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void clearNamespaceBundleBacklog(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        internalClearNamespaceBundleBacklog(bundleRange, authoritative);
    }

    @POST
    @Path("/{tenant}/{namespace}/clearBacklog/{subscription}")
    @ApiOperation(value = "Clear backlog for a given subscription on all topics on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin or operate permission on the namespace"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void clearNamespaceBacklogForSubscription(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("subscription") String subscription,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateNamespaceName(tenant, namespace);
            internalClearNamespaceBacklogForSubscription(asyncResponse, subscription, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{bundle}/clearBacklog/{subscription}")
    @ApiOperation(value = "Clear backlog for a given subscription on all topics on a namespace bundle.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin or operate permission on the namespace"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void clearNamespaceBundleBacklogForSubscription(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("subscription") String subscription,
            @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        internalClearNamespaceBundleBacklogForSubscription(subscription, bundleRange, authoritative);
    }

    @POST
    @Path("/{tenant}/{namespace}/unsubscribe/{subscription}")
    @ApiOperation(value = "Unsubscribes the given subscription on all topics on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin or operate permission on the namespacen"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void unsubscribeNamespace(@Suspended final AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("subscription") String subscription,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateNamespaceName(tenant, namespace);
            internalUnsubscribeNamespace(asyncResponse, subscription, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{bundle}/unsubscribe/{subscription}")
    @ApiOperation(value = "Unsubscribes the given subscription on all topics on a namespace bundle.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin or operate permission on the namespace"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void unsubscribeNamespaceBundle(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("subscription") String subscription,
            @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        internalUnsubscribeNamespaceBundle(subscription, bundleRange, authoritative);
    }

    @POST
    @Path("/{tenant}/{namespace}/subscriptionAuthMode")
    @ApiOperation(value = " Set a subscription auth mode for all the topics on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void setSubscriptionAuthMode(@PathParam("tenant") String tenant,
                                        @PathParam("namespace") String namespace, @ApiParam(value =
            "Subscription auth mode for all topics of the specified namespace")
                                                    SubscriptionAuthMode subscriptionAuthMode) {
        validateNamespaceName(tenant, namespace);
        internalSetSubscriptionAuthMode(subscriptionAuthMode);
    }

    @GET
    @Path("/{tenant}/{namespace}/subscriptionAuthMode")
    @ApiOperation(value = "Get subscription auth mode in a namespace", response = SubscriptionAuthMode.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or namespace doesn't exist")})
    public void getSubscriptionAuthMode(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.SUBSCRIPTION_AUTH_MODE, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.subscription_auth_mode))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get subscription auth mode in a namespace {}", clientAppId(),
                            namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/encryptionRequired")
    @ApiOperation(value = "Message encryption is required or not for all topics in a namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"), })
    public void modifyEncryptionRequired(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Flag defining if message encryption is required", required = true)
                    boolean encryptionRequired) {
        validateNamespaceName(tenant, namespace);
        internalModifyEncryptionRequired(encryptionRequired);
    }

    @GET
    @Path("/{tenant}/{namespace}/encryptionRequired")
    @ApiOperation(value = "Get message encryption required status in a namespace", response = Boolean.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or namespace doesn't exist")})
    public void getEncryptionRequired(@Suspended AsyncResponse asyncResponse,
                                      @PathParam("tenant") String tenant,
                                      @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.ENCRYPTION, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.encryption_required))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get message encryption required status in a namespace {}", clientAppId(),
                            namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/delayedDelivery")
    @ApiOperation(value = "Get delayed delivery messages config on a namespace.",
            response = DelayedDeliveryPolicies.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"), })
    public void getDelayedDeliveryPolicies(@Suspended final AsyncResponse asyncResponse,
                                           @PathParam("tenant") String tenant,
                                           @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.DELAYED_DELIVERY, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.delayed_delivery_policies))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get delayed delivery messages config on a namespace {}", clientAppId(),
                            namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/delayedDelivery")
    @ApiOperation(value = "Set delayed delivery messages config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"), })
    public void setDelayedDeliveryPolicies(@PathParam("tenant") String tenant,
                                           @PathParam("namespace") String namespace,
                                           @ApiParam(value = "Delayed delivery policies for the specified namespace")
                                                   DelayedDeliveryPolicies deliveryPolicies) {
        validateNamespaceName(tenant, namespace);
        internalSetDelayedDelivery(deliveryPolicies);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/delayedDelivery")
    @ApiOperation(value = "Delete delayed delivery messages config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"), })
    public void removeDelayedDeliveryPolicies(@PathParam("tenant") String tenant,
                                           @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetDelayedDelivery(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/inactiveTopicPolicies")
    @ApiOperation(value = "Get inactive topic policies config on a namespace.", response = InactiveTopicPolicies.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"), })
    public void getInactiveTopicPolicies(@Suspended final AsyncResponse asyncResponse,
                                         @PathParam("tenant") String tenant,
                                         @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.INACTIVE_TOPIC, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.inactive_topic_policies))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get inactive topic policies config on a namespace {}", clientAppId(),
                            namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/inactiveTopicPolicies")
    @ApiOperation(value = "Remove inactive topic policies from a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeInactiveTopicPolicies(@PathParam("tenant") String tenant,
                                            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetInactiveTopic(null);
    }

    @POST
    @Path("/{tenant}/{namespace}/inactiveTopicPolicies")
    @ApiOperation(value = "Set inactive topic policies config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"), })
    public void setInactiveTopicPolicies(@PathParam("tenant") String tenant,
                                         @PathParam("namespace") String namespace,
                                         @ApiParam(value = "Inactive topic policies for the specified namespace")
                                                 InactiveTopicPolicies inactiveTopicPolicies) {
        validateNamespaceName(tenant, namespace);
        internalSetInactiveTopic(inactiveTopicPolicies);
    }

    @GET
    @Path("/{tenant}/{namespace}/maxProducersPerTopic")
    @ApiOperation(value = "Get maxProducersPerTopic config on a namespace.", response = Integer.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void getMaxProducersPerTopic(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.MAX_PRODUCERS, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.max_producers_per_topic))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get maxProducersPerTopic config on a namespace {}", clientAppId(),
                            namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/maxProducersPerTopic")
    @ApiOperation(value = " Set maxProducersPerTopic configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "maxProducersPerTopic value is not valid") })
    public void setMaxProducersPerTopic(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @ApiParam(value = "Number of maximum producers per topic", required = true) int maxProducersPerTopic) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxProducersPerTopic(maxProducersPerTopic);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/maxProducersPerTopic")
    @ApiOperation(value = "Remove maxProducersPerTopic configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void removeMaxProducersPerTopic(@PathParam("tenant") String tenant,
                                               @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxProducersPerTopic(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/deduplicationSnapshotInterval")
    @ApiOperation(value = "Get deduplicationSnapshotInterval config on a namespace.", response = Integer.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void getDeduplicationSnapshotInterval(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.DEDUPLICATION_SNAPSHOT, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.deduplicationSnapshotIntervalSeconds))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get deduplicationSnapshotInterval config on a namespace {}",
                            clientAppId(), namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/deduplicationSnapshotInterval")
    @ApiOperation(value = "Set deduplicationSnapshotInterval config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist")})
    public void setDeduplicationSnapshotInterval(@PathParam("tenant") String tenant
            , @PathParam("namespace") String namespace
            , @ApiParam(value = "Interval to take deduplication snapshot per topic", required = true)
                                                         Integer interval) {
        validateNamespaceName(tenant, namespace);
        internalSetDeduplicationSnapshotInterval(interval);
    }

    @GET
    @Path("/{tenant}/{namespace}/maxConsumersPerTopic")
    @ApiOperation(value = "Get maxConsumersPerTopic config on a namespace.", response = Integer.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void getMaxConsumersPerTopic(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.MAX_CONSUMERS, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.max_consumers_per_topic))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get maxConsumersPerTopic config on a namespace {}", clientAppId(),
                            namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/maxConsumersPerTopic")
    @ApiOperation(value = " Set maxConsumersPerTopic configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "maxConsumersPerTopic value is not valid") })
    public void setMaxConsumersPerTopic(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @ApiParam(value = "Number of maximum consumers per topic", required = true) int maxConsumersPerTopic) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxConsumersPerTopic(maxConsumersPerTopic);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/maxConsumersPerTopic")
    @ApiOperation(value = "Remove maxConsumersPerTopic configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void removeMaxConsumersPerTopic(@PathParam("tenant") String tenant,
                                               @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxConsumersPerTopic(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/maxConsumersPerSubscription")
    @ApiOperation(value = "Get maxConsumersPerSubscription config on a namespace.", response = Integer.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void getMaxConsumersPerSubscription(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.MAX_CONSUMERS, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(polices -> asyncResponse.resume(polices.max_consumers_per_subscription))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get maxConsumersPerSubscription config on namespace {}: {} ",
                            clientAppId(), namespaceName, ex.getCause().getMessage(), ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/maxConsumersPerSubscription")
    @ApiOperation(value = " Set maxConsumersPerSubscription configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "maxConsumersPerSubscription value is not valid")})
    public void setMaxConsumersPerSubscription(@PathParam("tenant") String tenant,
                                               @PathParam("namespace") String namespace,
                                               @ApiParam(value = "Number of maximum consumers per subscription",
                                                       required = true)
                                                           int maxConsumersPerSubscription) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxConsumersPerSubscription(maxConsumersPerSubscription);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/maxConsumersPerSubscription")
    @ApiOperation(value = " Set maxConsumersPerSubscription configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "maxConsumersPerSubscription value is not valid")})
    public void removeMaxConsumersPerSubscription(@PathParam("tenant") String tenant,
                                               @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxConsumersPerSubscription(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/maxUnackedMessagesPerConsumer")
    @ApiOperation(value = "Get maxUnackedMessagesPerConsumer config on a namespace.", response = Integer.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void getMaxUnackedMessagesPerConsumer(@Suspended final AsyncResponse asyncResponse,
                                                 @PathParam("tenant") String tenant,
                                                 @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.MAX_UNACKED, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.max_unacked_messages_per_consumer))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get maxUnackedMessagesPerConsumer config on a namespace {}",
                            clientAppId(), namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/maxUnackedMessagesPerConsumer")
    @ApiOperation(value = " Set maxConsumersPerTopic configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "maxUnackedMessagesPerConsumer value is not valid")})
    public void setMaxUnackedMessagesPerConsumer(@PathParam("tenant") String tenant,
                                                 @PathParam("namespace") String namespace,
                                                 @ApiParam(value = "Number of maximum unacked messages per consumer",
                                                         required = true)
                                                             int maxUnackedMessagesPerConsumer) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxUnackedMessagesPerConsumer(maxUnackedMessagesPerConsumer);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/maxUnackedMessagesPerConsumer")
    @ApiOperation(value = "Remove maxUnackedMessagesPerConsumer config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void removeMaxUnackedmessagesPerConsumer(@PathParam("tenant") String tenant,
                                                        @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxUnackedMessagesPerConsumer(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/maxUnackedMessagesPerSubscription")
    @ApiOperation(value = "Get maxUnackedMessagesPerSubscription config on a namespace.", response = Integer.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void getMaxUnackedmessagesPerSubscription(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.MAX_UNACKED, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.max_unacked_messages_per_subscription))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get maxUnackedMessagesPerSubscription config on a namespace {}",
                            clientAppId(), namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/maxUnackedMessagesPerSubscription")
    @ApiOperation(value = " Set maxUnackedMessagesPerSubscription configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "maxUnackedMessagesPerSubscription value is not valid")})
    public void setMaxUnackedMessagesPerSubscription(
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @ApiParam(value = "Number of maximum unacked messages per subscription", required = true)
                    int maxUnackedMessagesPerSubscription) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxUnackedMessagesPerSubscription(maxUnackedMessagesPerSubscription);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/maxUnackedMessagesPerSubscription")
    @ApiOperation(value = "Remove maxUnackedMessagesPerSubscription config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void removeMaxUnackedmessagesPerSubscription(@PathParam("tenant") String tenant,
                                                        @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxUnackedMessagesPerSubscription(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/maxSubscriptionsPerTopic")
    @ApiOperation(value = "Get maxSubscriptionsPerTopic config on a namespace.", response = Integer.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void getMaxSubscriptionsPerTopic(@Suspended final AsyncResponse asyncResponse,
                                            @PathParam("tenant") String tenant,
                                            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.MAX_SUBSCRIPTIONS, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.max_subscriptions_per_topic))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get maxSubscriptionsPerTopic config on a namespace {}", clientAppId(),
                            namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/maxSubscriptionsPerTopic")
    @ApiOperation(value = " Set maxSubscriptionsPerTopic configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "maxUnackedMessagesPerSubscription value is not valid")})
    public void setMaxSubscriptionsPerTopic(
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @ApiParam(value = "Number of maximum subscriptions per topic", required = true)
                    int maxSubscriptionsPerTopic) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxSubscriptionsPerTopic(maxSubscriptionsPerTopic);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/maxSubscriptionsPerTopic")
    @ApiOperation(value = "Remove maxSubscriptionsPerTopic configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void removeMaxSubscriptionsPerTopic(@PathParam("tenant") String tenant,
                                                 @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxSubscriptionsPerTopic(null);
    }

    @POST
    @Path("/{tenant}/{namespace}/antiAffinity")
    @ApiOperation(value = "Set anti-affinity group for a namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Invalid antiAffinityGroup")})
    public void setNamespaceAntiAffinityGroup(@PathParam("tenant") String tenant,
                                              @PathParam("namespace") String namespace,
                                              @ApiParam(value = "Anti-affinity group for the specified namespace",
                                                      required = true)
                                                          String antiAffinityGroup) {
        validateNamespaceName(tenant, namespace);
        internalSetNamespaceAntiAffinityGroup(antiAffinityGroup);
    }

    @GET
    @Path("/{tenant}/{namespace}/antiAffinity")
    @ApiOperation(value = "Get anti-affinity group of a namespace.", response = String.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
    public String getNamespaceAntiAffinityGroup(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetNamespaceAntiAffinityGroup();
    }

    @DELETE
    @Path("/{tenant}/{namespace}/antiAffinity")
    @ApiOperation(value = "Remove anti-affinity group of a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void removeNamespaceAntiAffinityGroup(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalRemoveNamespaceAntiAffinityGroup();
    }

    @GET
    @Path("{cluster}/antiAffinity/{group}")
    @ApiOperation(value = "Get all namespaces that are grouped by given anti-affinity group in a given cluster."
            + " api can be only accessed by admin of any of the existing tenant",
            response = String.class, responseContainer = "List")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 412, message = "Cluster not exist/Anti-affinity group can't be empty.")})
    public List<String> getAntiAffinityNamespaces(@PathParam("cluster") String cluster,
            @PathParam("group") String antiAffinityGroup, @QueryParam("tenant") String tenant) {
        return internalGetAntiAffinityNamespaces(cluster, antiAffinityGroup, tenant);
    }

    private Policies getDefaultPolicesIfNull(Policies policies) {
        if (policies == null) {
            policies = new Policies();
        }

        int defaultNumberOfBundles = config().getDefaultNumberOfNamespaceBundles();

        if (policies.bundles == null) {
            policies.bundles = getBundles(defaultNumberOfBundles);
        }

        return policies;
    }

    @GET
    @Path("/{tenant}/{namespace}/compactionThreshold")
    @ApiOperation(value = "Maximum number of uncompacted bytes in topics before compaction is triggered.",
                  notes = "The backlog size is compared to the threshold periodically. "
                          + "A threshold of 0 disabled automatic compaction", response = Long.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
    public void getCompactionThreshold(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.COMPACTION, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.compaction_threshold))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get compaction threshold on namespace {}", clientAppId(), namespaceName,
                            ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/compactionThreshold")
    @ApiOperation(value = "Set maximum number of uncompacted bytes in a topic before compaction is triggered.",
            notes = "The backlog size is compared to the threshold periodically. "
                    + "A threshold of 0 disabled automatic compaction")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "compactionThreshold value is not valid")})
    public void setCompactionThreshold(@PathParam("tenant") String tenant,
                                       @PathParam("namespace") String namespace,
                                       @ApiParam(value = "Maximum number of uncompacted bytes"
                                               + " in a topic of the specified namespace",
                                               required = true) long newThreshold) {
        validateNamespaceName(tenant, namespace);
        internalSetCompactionThreshold(newThreshold);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/compactionThreshold")
    @ApiOperation(value = "Delete maximum number of uncompacted bytes in a topic before compaction is triggered.",
            notes = "The backlog size is compared to the threshold periodically. "
                    + "A threshold of 0 disabled automatic compaction")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void deleteCompactionThreshold(@PathParam("tenant") String tenant,
                                       @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetCompactionThreshold(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/offloadThreshold")
    @ApiOperation(value = "Maximum number of bytes stored on the pulsar cluster for a topic,"
                          + " before the broker will start offloading to longterm storage",
                  notes = "A negative value disables automatic offloading", response = Long.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
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
                    log.error("[{}] Failed to get offload threshold on namespace {}", clientAppId(), namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/offloadThreshold")
    @ApiOperation(value = "Set maximum number of bytes stored on the pulsar cluster for a topic,"
            + " before the broker will start offloading to longterm storage",
            notes = "-1 will revert to using the cluster default."
                    + " A negative value disables automatic offloading. ")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "offloadThreshold value is not valid")})
    public void setOffloadThreshold(@PathParam("tenant") String tenant,
                                    @PathParam("namespace") String namespace,
                                    @ApiParam(value =
                                            "Maximum number of bytes stored on the pulsar cluster"
                                                    + " for a topic of the specified namespace",
                                            required = true) long newThreshold) {
        validateNamespaceName(tenant, namespace);
        internalSetOffloadThreshold(newThreshold);
    }

    @GET
    @Path("/{tenant}/{namespace}/offloadThresholdInSeconds")
    @ApiOperation(value = "Maximum number of bytes stored on the pulsar cluster for a topic,"
            + " before the broker will start offloading to longterm storage",
            notes = "A negative value disables automatic offloading", response = Long.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
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
                    log.error("[{}] Failed to get offload threshold on namespace {}", clientAppId(), namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/offloadThresholdInSeconds")
    @ApiOperation(value = "Set maximum number of seconds stored on the pulsar cluster for a topic,"
            + " before the broker will start offloading to longterm storage",
            notes = "A negative value disables automatic offloading")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "offloadThresholdInSeconds value is not valid") })
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
    @ApiOperation(value = "Number of milliseconds to wait before deleting a ledger segment which has been offloaded"
                          + " from the Pulsar cluster's local storage (i.e. BookKeeper)",
                  notes = "A negative value denotes that deletion has been completely disabled."
                          + " 'null' denotes that the topics in the namespace will fall back to the"
                          + " broker default for deletion lag.", response = Long.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
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
                    log.error("[{}] Failed to get offload deletion lag milliseconds on namespace {}", clientAppId(),
                            namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/offloadDeletionLagMs")
    @ApiOperation(value = "Set number of milliseconds to wait before deleting a ledger segment which has been offloaded"
            + " from the Pulsar cluster's local storage (i.e. BookKeeper)",
            notes = "A negative value disables the deletion completely.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "offloadDeletionLagMs value is not valid")})
    public void setOffloadDeletionLag(@PathParam("tenant") String tenant,
                                      @PathParam("namespace") String namespace,
                                      @ApiParam(value =
                                              "New number of milliseconds to wait before deleting a ledger segment"
                                                      + " which has been offloaded",
                                              required = true) long newDeletionLagMs) {
        validateNamespaceName(tenant, namespace);
        internalSetOffloadDeletionLag(newDeletionLagMs);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/offloadDeletionLagMs")
    @ApiOperation(value = "Clear the namespace configured offload deletion lag. The topics in the namespace"
                          + " will fallback to using the default configured deletion lag for the broker")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 200, message = "Operation successful"),
                            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
                            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void clearOffloadDeletionLag(@PathParam("tenant") String tenant,
                                        @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetOffloadDeletionLag(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/schemaAutoUpdateCompatibilityStrategy")
    @ApiOperation(value = "The strategy used to check the compatibility of new schemas,"
                          + " provided by producers, before automatically updating the schema",
                  notes = "The value AutoUpdateDisabled prevents producers from updating the schema. "
                          + " If set to AutoUpdateDisabled, schemas must be updated through the REST api",
            response = SchemaAutoUpdateCompatibilityStrategy.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
                            @ApiResponse(code = 409, message = "Concurrent modification") })
    public SchemaAutoUpdateCompatibilityStrategy getSchemaAutoUpdateCompatibilityStrategy(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetSchemaAutoUpdateCompatibilityStrategy();
    }

    @PUT
    @Path("/{tenant}/{namespace}/schemaAutoUpdateCompatibilityStrategy")
    @ApiOperation(value = "Update the strategy used to check the compatibility of new schemas,"
            + " provided by producers, before automatically updating the schema",
            notes = "The value AutoUpdateDisabled prevents producers from updating the schema. "
                    + " If set to AutoUpdateDisabled, schemas must be updated through the REST api")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void setSchemaAutoUpdateCompatibilityStrategy(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Strategy used to check the compatibility of new schemas")
                    SchemaAutoUpdateCompatibilityStrategy strategy) {
        validateNamespaceName(tenant, namespace);
        internalSetSchemaAutoUpdateCompatibilityStrategy(strategy);
    }

    @GET
    @Path("/{tenant}/{namespace}/schemaCompatibilityStrategy")
    @ApiOperation(value = "The strategy of the namespace schema compatibility ",
            response = SchemaCompatibilityStrategy.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
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
                    log.error("[{}] Failed to get the strategy of the namespace schema compatibility {}", clientAppId(),
                            namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/schemaCompatibilityStrategy")
    @ApiOperation(value = "Update the strategy used to check the compatibility of new schema")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void setSchemaCompatibilityStrategy(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Strategy used to check the compatibility of new schema")
                    SchemaCompatibilityStrategy strategy) {
        validateNamespaceName(tenant, namespace);
        internalSetSchemaCompatibilityStrategy(strategy);
    }

    @GET
    @Path("/{tenant}/{namespace}/isAllowAutoUpdateSchema")
    @ApiOperation(value = "The flag of whether allow auto update schema", response = Boolean.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
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
                    log.error("[{}] Failed to get the flag of whether allow auto update schema on a namespace {}",
                            clientAppId(), namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/isAllowAutoUpdateSchema")
    @ApiOperation(value = "Update flag of whether allow auto update schema")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void setIsAllowAutoUpdateSchema(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Flag of whether to allow auto update schema", required = true)
                    boolean isAllowAutoUpdateSchema) {
        validateNamespaceName(tenant, namespace);
        internalSetIsAllowAutoUpdateSchema(isAllowAutoUpdateSchema);
    }

    @GET
    @Path("/{tenant}/{namespace}/subscriptionTypesEnabled")
    @ApiOperation(value = "The set of whether allow subscription types",
            response = SubscriptionType.class, responseContainer = "Set")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
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
                    log.error("[{}] Failed to get the set of whether allow subscription types on a namespace {}",
                            clientAppId(), namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/subscriptionTypesEnabled")
    @ApiOperation(value = "Update set of whether allow share sub type")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void setSubscriptionTypesEnabled(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Set of whether allow subscription types", required = true)
                    Set<SubscriptionType> subscriptionTypesEnabled) {
        validateNamespaceName(tenant, namespace);
        internalSetSubscriptionTypesEnabled(subscriptionTypesEnabled);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/subscriptionTypesEnabled")
    @ApiOperation(value = " Remove subscription types enabled on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeSubscriptionTypesEnabled(@PathParam("tenant") String tenant,
                                               @PathParam("namespace") String namespace) {
            validateNamespaceName(tenant, namespace);
            internalSetSubscriptionTypesEnabled(new HashSet<>());
    }

    @GET
    @Path("/{tenant}/{namespace}/schemaValidationEnforced")
    @ApiOperation(value = "Get schema validation enforced flag for namespace.",
                  notes = "If the flag is set to true, when a producer without a schema attempts to produce to a topic"
                          + " with schema in this namespace, the producer will be failed to connect. PLEASE be"
                          + " carefully on using this, since non-java clients don't support schema.if you enable"
                          + " this setting, it will cause non-java clients failed to produce.",
            response = Boolean.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 404, message = "Tenants or Namespace doesn't exist") })
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
                    log.error("[{}] Failed to get schema validation enforced flag for namespace {}", clientAppId(),
                            namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/schemaValidationEnforced")
    @ApiOperation(value = "Set schema validation enforced flag on namespace.",
            notes = "If the flag is set to true, when a producer without a schema attempts to produce to a topic"
                    + " with schema in this namespace, the producer will be failed to connect. PLEASE be"
                    + " carefully on using this, since non-java clients don't support schema.if you enable"
                    + " this setting, it will cause non-java clients failed to produce.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or Namespace doesn't exist"),
            @ApiResponse(code = 412, message = "schemaValidationEnforced value is not valid")})
    public void setSchemaValidationEnforced(@PathParam("tenant") String tenant,
                                           @PathParam("namespace") String namespace,
                                           @ApiParam(value =
                                                   "Flag of whether validation is enforced on the specified namespace",
                                                   required = true)
                                                       boolean schemaValidationEnforced) {
        validateNamespaceName(tenant, namespace);
        internalSetSchemaValidationEnforced(schemaValidationEnforced);
    }

    @POST
    @Path("/{tenant}/{namespace}/offloadPolicies")
    @ApiOperation(value = "Set offload configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412,
                    message = "OffloadPolicies is empty or driver is not supported or bucket is not valid")})
    public void setOffloadPolicies(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
                                   @ApiParam(value = "Offload policies for the specified namespace", required = true)
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
    @ApiOperation(value = " Set offload configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412,
                    message = "OffloadPolicies is empty or driver is not supported or bucket is not valid")})
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
    @ApiOperation(value = "Get offload configuration on a namespace.", response = OffloadPolicies.class)
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist")})
    public void getOffloadPolicies(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.OFFLOAD, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(policies -> asyncResponse.resume(policies.offload_policies))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get offload policies on a namespace {}", clientAppId(),
                            namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/maxTopicsPerNamespace")
    @ApiOperation(value = "Get maxTopicsPerNamespace config on a namespace.", response = Integer.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or namespace does not exist") })
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
                    log.error("[{}] Failed to get maxTopicsPerNamespace config on a namespace {}", clientAppId(),
                            namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/maxTopicsPerNamespace")
    @ApiOperation(value = "Set maxTopicsPerNamespace config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or namespace doesn't exist"), })
    public void setInactiveTopicPolicies(@PathParam("tenant") String tenant,
                                         @PathParam("namespace") String namespace,
                                         @ApiParam(value = "Number of maximum topics for specific namespace",
                                                 required = true) int maxTopicsPerNamespace) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxTopicsPerNamespace(maxTopicsPerNamespace);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/maxTopicsPerNamespace")
    @ApiOperation(value = "Remove maxTopicsPerNamespace config on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or namespace doesn't exist"), })
    public void setInactiveTopicPolicies(@PathParam("tenant") String tenant,
                                         @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalRemoveMaxTopicsPerNamespace();
    }

    @PUT
    @Path("/{tenant}/{namespace}/property/{key}/{value}")
    @ApiOperation(value = "Put a key value pair property on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or namespace doesn't exist"), })
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
    @ApiOperation(value = "Get property value for a given key on a namespace.", response = String.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or namespace doesn't exist"), })
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
    @ApiOperation(value = "Remove property value for a given key on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or namespace doesn't exist"), })
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
    @ApiOperation(value = "Put key value pairs property on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or namespace doesn't exist"), })
    public void setProperties(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Key value pair properties for the namespace", required = true)
                    Map<String, String> properties) {
        validateNamespaceName(tenant, namespace);
        internalSetProperties(properties, asyncResponse);
    }

    @GET
    @Path("/{tenant}/{namespace}/properties")
    @ApiOperation(value = "Get key value pair properties for a given namespace.",
            response = String.class, responseContainer = "Map")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or namespace doesn't exist"), })
    public void getProperties(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetProperties(asyncResponse);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/properties")
    @ApiOperation(value = "Clear properties on a given namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or namespace doesn't exist"), })
    public void clearProperties(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalClearProperties(asyncResponse);
    }

    @GET
    @Path("/{tenant}/{namespace}/resourcegroup")
    @ApiOperation(value = "Get the resource group attached to the namespace", response = String.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
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
                    log.error("Failed to get the resource group attached to the namespace {}", namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/resourcegroup/{resourcegroup}")
    @ApiOperation(value = "Set resourcegroup for a namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Invalid resourcegroup") })
    public void setNamespaceResourceGroup(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
                                          @PathParam("resourcegroup") String rgName) {
        validateNamespaceName(tenant, namespace);
        internalSetNamespaceResourceGroup(rgName);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/resourcegroup")
    @ApiOperation(value = "Delete resourcegroup for a namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Invalid resourcegroup")})
    public void removeNamespaceResourceGroup(@PathParam("tenant") String tenant,
                                          @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetNamespaceResourceGroup(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/scanOffloadedLedgers")
    @ApiOperation(value = "Trigger the scan of offloaded Ledgers on the LedgerOffloader for the given namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successful get of offloaded ledger data", response = String.class,
                examples = @Example(value = { @ExampleProperty(mediaType = "application/json",
                        value = "{\"objects\":[{\"key1\":\"value1\",\"key2\":\"value2\"}],"
                                + "\"total\":100,\"errors\":5,\"unknown\":3}")
            })),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
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
                    log.error("error", err);
                    throw new RuntimeException(err);
                }
            };
            return Response.ok(output).type(MediaType.APPLICATION_JSON_TYPE).build();
        } catch (Throwable err) {
            log.error("Error while scanning offloaded ledgers for namespace {}", namespaceName, err);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR,
                    "Error while scanning ledgers for " + namespaceName);
        }
    }

    @GET
    @Path("/{tenant}/{namespace}/entryFilters")
    @ApiOperation(value = "Get maxConsumersPerSubscription config on a namespace.", response = EntryFilters.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void getEntryFiltersPerTopic(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.ENTRY_FILTERS, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenAccept(polices -> asyncResponse.resume(polices.entryFilters))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get entry filters config on namespace {}: {} ",
                            clientAppId(), namespaceName, ex.getCause().getMessage(), ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/entryFilters")
    @ApiOperation(value = "Set entry filters for namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 400, message = "Specified entry filters are not valid"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist")
    })
    public void setEntryFiltersPerTopic(@Suspended AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
                                       @PathParam("namespace") String namespace,
                                       @ApiParam(value = "entry filters", required = true)
                                               EntryFilters entryFilters) {
        validateNamespaceName(tenant, namespace);
        internalSetEntryFiltersPerTopicAsync(entryFilters)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("Failed to set entry filters for namespace {}", namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/entryFilters")
    @ApiOperation(value = "Remove entry filters for namespace")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Invalid TTL")})
    public void removeNamespaceEntryFilters(@Suspended AsyncResponse asyncResponse,
                                          @PathParam("tenant") String tenant,
                                          @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetEntryFiltersPerTopicAsync(null)
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("Failed to remove entry filters for namespace {}", namespaceName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/allowedClusters")
    @ApiOperation(value = "Set the allowed clusters for a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "The list of allowed clusters should include all replication clusters."),
            @ApiResponse(code = 403, message = "The requester does not have admin permissions."),
            @ApiResponse(code = 404, message = "The specified tenant, cluster, or namespace does not exist."),
            @ApiResponse(code = 409, message = "A peer-cluster cannot be part of an allowed-cluster."),
            @ApiResponse(code = 412, message = "The namespace is not global or the provided cluster IDs are invalid.")})
    public void setNamespaceAllowedClusters(@Suspended AsyncResponse asyncResponse,
                                                @PathParam("tenant") String tenant,
                                                @PathParam("namespace") String namespace,
                                                @ApiParam(value = "List of allowed clusters", required = true)
                                                List<String> clusterIds) {
        validateNamespaceName(tenant, namespace);
        internalSetNamespaceAllowedClusters(clusterIds)
                .thenAccept(asyncResponse::resume)
                .exceptionally(e -> {
                    log.error("[{}] Failed to set namespace allowed clusters on namespace {}",
                            clientAppId(), namespace, e);
                    resumeAsyncResponseExceptionally(asyncResponse, e);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/allowedClusters")
    @ApiOperation(value = "Get the allowed clusters for a namespace.",
            response = String.class, responseContainer = "List")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Namespace is not global")})
    public void getNamespaceAllowedClusters(@Suspended AsyncResponse asyncResponse,
                                                @PathParam("tenant") String tenant,
                                                @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalGetNamespaceAllowedClustersAsync()
                .thenAccept(asyncResponse::resume)
                .exceptionally(e -> {
                    log.error("[{}] Failed to get namespace allowed clusters on namespace {}", clientAppId(),
                            namespace, e);
                    resumeAsyncResponseExceptionally(asyncResponse, e);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/replicateSubscriptionState")
    @ApiOperation(value = "Get the enabled status of subscription replication on a namespace.", response =
            Boolean.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist")})
    public Boolean getReplicateSubscriptionState(@PathParam("tenant") String tenant,
                                                     @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperation(NamespaceName.get(tenant, namespace),
                PolicyName.REPLICATED_SUBSCRIPTION, PolicyOperation.READ);

        Policies policies = getNamespacePolicies(namespaceName);
        return policies.replicate_subscription_state;
    }

    @POST
    @Path("/{tenant}/{namespace}/replicateSubscriptionState")
    @ApiOperation(value = "Enable or disable subscription replication on a namespace.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist")})
    public void setReplicateSubscriptionState(@Suspended final AsyncResponse asyncResponse,
                                                 @PathParam("tenant") String tenant,
                                                 @PathParam("namespace") String namespace,
                                                 @ApiParam(value = "Whether to enable subscription replication",
                                                         required = true)
                                                 Boolean enabled) {
        validateNamespaceName(tenant, namespace);
        internalSetReplicateSubscriptionStateAsync(enabled)
                .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("set replicate subscription state failed", ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    private static final Logger log = LoggerFactory.getLogger(Namespaces.class);
}
