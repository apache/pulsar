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

import static org.apache.pulsar.common.policies.data.PoliciesUtil.getBundles;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
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
import org.apache.pulsar.broker.admin.impl.NamespacesBase;
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
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
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
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;
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
    public List<String> getTenantNamespaces(@PathParam("tenant") String tenant) {
        return internalGetTenantNamespaces(tenant);
    }

    @GET
    @Path("/{tenant}/{namespace}/topics")
    @ApiOperation(value = "Get the list of all the topics under a certain namespace.",
            response = String.class, responseContainer = "Set")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist")})
    public void getTopics(@PathParam("tenant") String tenant,
                                  @PathParam("namespace") String namespace,
                                  @QueryParam("mode") @DefaultValue("PERSISTENT") Mode mode,
                                  @Suspended AsyncResponse asyncResponse) {
        validateNamespaceName(tenant, namespace);
        validateNamespaceOperation(NamespaceName.get(tenant, namespace), NamespaceOperation.GET_TOPICS);

        // Validate that namespace exists, throws 404 if it doesn't exist
        getNamespacePolicies(namespaceName);

        pulsar().getNamespaceService().getListOfTopics(namespaceName, mode)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error("Failed to get topics list for namespace {}", namespaceName, ex);
                    asyncResponse.resume(ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}")
    @ApiOperation(value = "Get the dump all the policies specified for a namespace.", response = Policies.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
    public Policies getPolicies(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperation(NamespaceName.get(tenant, namespace), PolicyName.ALL, PolicyOperation.READ);
        return getNamespacePolicies(namespaceName);
    }

    @PUT
    @Path("/{tenant}/{namespace}")
    @ApiOperation(value = "Creates a new namespace with the specified policies")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster doesn't exist"),
            @ApiResponse(code = 409, message = "Namespace already exists"),
            @ApiResponse(code = 412, message = "Namespace name is not valid") })
    public void createNamespace(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @ApiParam(value = "Policies for the namespace") Policies policies) {
        validateNamespaceName(tenant, namespace);
        policies = getDefaultPolicesIfNull(policies);
        internalCreateNamespace(policies);
    }

    @DELETE
    @Path("/{tenant}/{namespace}")
    @ApiOperation(value = "Delete a namespace and all the topics under it.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 405, message = "Broker doesn't allow forced deletion of namespaces"),
            @ApiResponse(code = 409, message = "Namespace is not empty") })
    public void deleteNamespace(@Suspended final AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @QueryParam("force") @DefaultValue("false") boolean force,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateNamespaceName(tenant, namespace);
            internalDeleteNamespace(asyncResponse, authoritative, force);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{bundle}")
    @ApiOperation(value = "Delete a namespace bundle and all the topics under it.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Namespace bundle is not empty") })
    public void deleteNamespaceBundle(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("bundle") String bundleRange,
            @QueryParam("force") @DefaultValue("false") boolean force,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        internalDeleteNamespaceBundle(bundleRange, authoritative, force);
    }

    @GET
    @Path("/{tenant}/{namespace}/permissions")
    @ApiOperation(value = "Retrieve the permissions for a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Namespace is not empty") })
    public Map<String, Set<AuthAction>> getPermissions(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespaceOperation(NamespaceName.get(tenant, namespace), NamespaceOperation.GET_PERMISSION);

        Policies policies = getNamespacePolicies(namespaceName);
        return policies.auth_policies.getNamespaceAuthentication();
    }

    @POST
    @Path("/{tenant}/{namespace}/permissions/{role}")
    @ApiOperation(value = "Grant a new permission to a role on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 501, message = "Authorization is not enabled")})
    public void grantPermissionOnNamespace(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("role") String role,
            @ApiParam(value = "List of permissions for the specified role") Set<AuthAction> actions) {
        validateNamespaceName(tenant, namespace);
        internalGrantPermissionOnNamespace(role, actions);
    }

    @POST
    @Path("/{property}/{namespace}/permissions/subscription/{subscription}")
    @ApiOperation(hidden = true, value = "Grant a new permission to roles for a subscription."
            + "[Tenant admin is allowed to perform this operation]")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 501, message = "Authorization is not enabled") })
    public void grantPermissionOnSubscription(@PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("subscription") String subscription,
            @ApiParam(value = "List of roles for the specified subscription") Set<String> roles) {
        validateNamespaceName(property, namespace);
        internalGrantPermissionOnSubscription(subscription, roles);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/permissions/{role}")
    @ApiOperation(value = "Revoke all permissions to a role on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
    public void revokePermissionsOnNamespace(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("role") String role) {
        validateNamespaceName(tenant, namespace);
        internalRevokePermissionsOnNamespace(role);
    }

    @DELETE
    @Path("/{property}/{namespace}/permissions/{subscription}/{role}")
    @ApiOperation(hidden = true, value = "Revoke subscription admin-api access permission for a role.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist") })
    public void revokePermissionOnSubscription(@PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("subscription") String subscription,
            @PathParam("role") String role) {
        validateNamespaceName(property, namespace);
        internalRevokePermissionsOnSubscription(subscription, role);
    }

    @GET
    @Path("/{tenant}/{namespace}/replication")
    @ApiOperation(value = "Get the replication clusters for a namespace.",
            response = String.class, responseContainer = "List")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Namespace is not global")})
    public Set<String> getNamespaceReplicationClusters(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetNamespaceReplicationClusters();
    }

    @POST
    @Path("/{tenant}/{namespace}/replication")
    @ApiOperation(value = "Set the replication clusters for a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Peer-cluster can't be part of replication-cluster"),
            @ApiResponse(code = 412, message = "Namespace is not global or invalid cluster ids") })
    public void setNamespaceReplicationClusters(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @ApiParam(value = "List of replication clusters", required = true) List<String> clusterIds) {
        validateNamespaceName(tenant, namespace);
        internalSetNamespaceReplicationClusters(clusterIds);
    }

    @GET
    @Path("/{tenant}/{namespace}/messageTTL")
    @ApiOperation(value = "Get the message TTL for the namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
    public Integer getNamespaceMessageTTL(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperation(NamespaceName.get(tenant, namespace), PolicyName.TTL, PolicyOperation.READ);

        Policies policies = getNamespacePolicies(namespaceName);
        return policies.message_ttl_in_seconds;
    }

    @POST
    @Path("/{tenant}/{namespace}/messageTTL")
    @ApiOperation(value = "Set message TTL in seconds for namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Invalid TTL") })
    public void setNamespaceMessageTTL(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @ApiParam(value = "TTL in seconds for the specified namespace", required = true) int messageTTL) {
        validateNamespaceName(tenant, namespace);
        internalSetNamespaceMessageTTL(messageTTL);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/messageTTL")
    @ApiOperation(value = "Set message TTL in seconds for namespace")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Invalid TTL")})
    public void removeNamespaceMessageTTL(@PathParam("tenant") String tenant,
                                          @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetNamespaceMessageTTL(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/subscriptionExpirationTime")
    @ApiOperation(value = "Get the subscription expiration time for the namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
    public Integer getSubscriptionExpirationTime(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateAdminAccessForTenant(tenant);
        validateNamespaceName(tenant, namespace);

        Policies policies = getNamespacePolicies(namespaceName);
        return policies.subscription_expiration_time_minutes;
    }

    @POST
    @Path("/{tenant}/{namespace}/subscriptionExpirationTime")
    @ApiOperation(value = "Set subscription expiration time in minutes for namespace")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Invalid expiration time")})
    public void setSubscriptionExpirationTime(@PathParam("tenant") String tenant,
                                              @PathParam("namespace") String namespace,
                                              @ApiParam(value =
                                                      "Expiration time in minutes for the specified namespace",
                                                      required = true) int expirationTime) {
        validateNamespaceName(tenant, namespace);
        internalSetSubscriptionExpirationTime(expirationTime);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/subscriptionExpirationTime")
    @ApiOperation(value = "Remove subscription expiration time for namespace")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist")})
    public void removeSubscriptionExpirationTime(@PathParam("tenant") String tenant,
                                                 @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetSubscriptionExpirationTime(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/deduplication")
    @ApiOperation(value = "Get broker side deduplication for all topics in a namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
    public Boolean getDeduplication(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetDeduplication();
    }

    @POST
    @Path("/{tenant}/{namespace}/deduplication")
    @ApiOperation(value = "Enable or disable broker side deduplication for all topics in a namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
    public void modifyDeduplication(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
                                    @ApiParam(value = "Flag for disabling or enabling broker side deduplication "
                                            + "for all topics in the specified namespace", required = true)
                                            boolean enableDeduplication) {
        validateNamespaceName(tenant, namespace);
        internalModifyDeduplication(enableDeduplication);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/deduplication")
    @ApiOperation(value = "Remove broker side deduplication for all topics in a namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
    public void removeDeduplication(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalModifyDeduplication(null);
    }

    @POST
    @Path("/{tenant}/{namespace}/autoTopicCreation")
    @ApiOperation(value = "Override broker's allowAutoTopicCreation setting for a namespace")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 406, message = "The number of partitions should be less than or"
                    + " equal to maxNumPartitionsPerPartitionedTopic"),
            @ApiResponse(code = 400, message = "Invalid autoTopicCreation override")})
    public void setAutoTopicCreation(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @ApiParam(value = "Settings for automatic topic creation", required = true)
                    AutoTopicCreationOverride autoTopicCreationOverride) {
        try {
            validateNamespaceName(tenant, namespace);
            internalSetAutoTopicCreation(asyncResponse, autoTopicCreationOverride);
        } catch (RestException e) {
            asyncResponse.resume(e);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @DELETE
    @Path("/{tenant}/{namespace}/autoTopicCreation")
    @ApiOperation(value = "Remove override of broker's allowAutoTopicCreation in a namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
    public void removeAutoTopicCreation(@Suspended final AsyncResponse asyncResponse,
                                        @PathParam("tenant") String tenant, @PathParam("namespace") String namespace) {
        try {
            validateNamespaceName(tenant, namespace);
            internalRemoveAutoTopicCreation(asyncResponse);
        } catch (RestException e) {
            asyncResponse.resume(e);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/autoSubscriptionCreation")
    @ApiOperation(value = "Override broker's allowAutoSubscriptionCreation setting for a namespace")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 400, message = "Invalid autoSubscriptionCreation override")})
    public void setAutoSubscriptionCreation(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @ApiParam(value = "Settings for automatic subscription creation")
                    AutoSubscriptionCreationOverride autoSubscriptionCreationOverride) {
        try {
            validateNamespaceName(tenant, namespace);
            internalSetAutoSubscriptionCreation(asyncResponse, autoSubscriptionCreationOverride);
        } catch (RestException e) {
            asyncResponse.resume(e);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @DELETE
    @Path("/{tenant}/{namespace}/autoSubscriptionCreation")
    @ApiOperation(value = "Remove override of broker's allowAutoSubscriptionCreation in a namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
    public void removeAutoSubscriptionCreation(@Suspended final AsyncResponse asyncResponse,
                                        @PathParam("tenant") String tenant, @PathParam("namespace") String namespace) {
        try {
            validateNamespaceName(tenant, namespace);
            internalRemoveAutoSubscriptionCreation(asyncResponse);
        } catch (RestException e) {
            asyncResponse.resume(e);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("/{tenant}/{namespace}/bundles")
    @ApiOperation(value = "Get the bundles split data.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Namespace is not setup to split in bundles") })
    public BundlesData getBundlesData(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validatePoliciesReadOnlyAccess();
        validateNamespaceName(tenant, namespace);
        validateNamespaceOperation(NamespaceName.get(tenant, namespace), NamespaceOperation.GET_BUNDLE);

        Policies policies = getNamespacePolicies(namespaceName);

        return policies.bundles;
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
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Namespace is already unloaded or Namespace has bundles activated")})
    public void unloadNamespace(@Suspended final AsyncResponse asyncResponse, @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        try {
            validateNamespaceName(tenant, namespace);
            internalUnloadNamespace(asyncResponse);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @PUT
    @Path("/{tenant}/{namespace}/{bundle}/unload")
    @ApiOperation(value = "Unload a namespace bundle")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void unloadNamespaceBundle(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        internalUnloadNamespaceBundle(asyncResponse, bundleRange, authoritative);
    }

    @PUT
    @Path("/{tenant}/{namespace}/{bundle}/split")
    @ApiOperation(value = "Split a namespace bundle")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void splitNamespaceBundle(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("unload") @DefaultValue("false") boolean unload,
            @QueryParam("splitAlgorithmName") String splitAlgorithmName) {

        try {
            validateNamespaceName(tenant, namespace);
            internalSplitNamespaceBundle(asyncResponse, bundleRange, authoritative, unload, splitAlgorithmName);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{property}/{namespace}/publishRate")
    @ApiOperation(hidden = true, value = "Set publish-rate throttling for all topics of the namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void setPublishRate(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @ApiParam(value = "Publish rate for all topics of the specified namespace") PublishRate publishRate) {
        validateNamespaceName(property, namespace);
        internalSetPublishRate(publishRate);
    }

    @DELETE
    @Path("/{property}/{namespace}/publishRate")
    @ApiOperation(hidden = true, value = "Set publish-rate throttling for all topics of the namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void removePublishRate(@PathParam("property") String property, @PathParam("namespace") String namespace) {
        validateNamespaceName(property, namespace);
        internalRemovePublishRate();
    }

    @GET
    @Path("/{property}/{namespace}/publishRate")
    @ApiOperation(hidden = true,
            value = "Get publish-rate configured for the namespace, -1 represents not configured yet")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist")})
    public PublishRate getPublishRate(
            @PathParam("property") String property,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, namespace);
        return internalGetPublishRate();
    }

    @POST
    @Path("/{tenant}/{namespace}/dispatchRate")
    @ApiOperation(value = "Set dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void setDispatchRate(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @ApiParam(value = "Dispatch rate for all topics of the specified namespace")
                    DispatchRateImpl dispatchRate) {
        validateNamespaceName(tenant, namespace);
        internalSetTopicDispatchRate(dispatchRate);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/dispatchRate")
    @ApiOperation(value = "Delete dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void deleteDispatchRate(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalDeleteTopicDispatchRate();
    }

    @GET
    @Path("/{tenant}/{namespace}/dispatchRate")
    @ApiOperation(value = "Get dispatch-rate configured for the namespace, -1 represents not configured yet")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public DispatchRate getDispatchRate(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetTopicDispatchRate();
    }

    @POST
    @Path("/{tenant}/{namespace}/subscriptionDispatchRate")
    @ApiOperation(value = "Set Subscription dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission")})
    public void setSubscriptionDispatchRate(@PathParam("tenant") String tenant,
                                            @PathParam("namespace") String namespace, @ApiParam(value =
            "Subscription dispatch rate for all topics of the specified namespace")
                                                        DispatchRateImpl dispatchRate) {
        validateNamespaceName(tenant, namespace);
        internalSetSubscriptionDispatchRate(dispatchRate);
    }

    @GET
    @Path("/{tenant}/{namespace}/subscriptionDispatchRate")
    @ApiOperation(
            value = "Get Subscription dispatch-rate configured for the namespace, -1 represents not configured yet")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist")})
    public DispatchRate getSubscriptionDispatchRate(@PathParam("tenant") String tenant,
                                                    @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetSubscriptionDispatchRate();
    }

    @DELETE
    @Path("/{tenant}/{namespace}/subscriptionDispatchRate")
    @ApiOperation(value = "Delete Subscription dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void deleteSubscriptionDispatchRate(@PathParam("tenant") String tenant,
                                               @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalDeleteSubscriptionDispatchRate();
    }

    @DELETE
    @Path("/{tenant}/{namespace}/subscribeRate")
    @ApiOperation(value = "Delete subscribe-rate throttling for all topics of the namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void deleteSubscribeRate(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalDeleteSubscribeRate();
    }

    @POST
    @Path("/{tenant}/{namespace}/subscribeRate")
    @ApiOperation(value = "Set subscribe-rate throttling for all topics of the namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void setSubscribeRate(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @ApiParam(value = "Subscribe rate for all topics of the specified namespace") SubscribeRate subscribeRate) {
        validateNamespaceName(tenant, namespace);
        internalSetSubscribeRate(subscribeRate);
    }

    @GET
    @Path("/{tenant}/{namespace}/subscribeRate")
    @ApiOperation(value = "Get subscribe-rate configured for the namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public SubscribeRate getSubscribeRate(@PathParam("tenant") String tenant,
                                        @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetSubscribeRate();
    }

    @DELETE
    @Path("/{tenant}/{namespace}/replicatorDispatchRate")
    @ApiOperation(value = "Remove replicator dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission")})
    public void removeReplicatorDispatchRate(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalRemoveReplicatorDispatchRate();
    }

    @POST
    @Path("/{tenant}/{namespace}/replicatorDispatchRate")
    @ApiOperation(value = "Set replicator dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission")})
    public void setReplicatorDispatchRate(@PathParam("tenant") String tenant,
                                          @PathParam("namespace") String namespace, @ApiParam(value =
            "Replicator dispatch rate for all topics of the specified namespace")
                                                      DispatchRateImpl dispatchRate) {
        validateNamespaceName(tenant, namespace);
        internalSetReplicatorDispatchRate(dispatchRate);
    }

    @GET
    @Path("/{tenant}/{namespace}/replicatorDispatchRate")
    @ApiOperation(value = "Get replicator dispatch-rate configured for the namespace, -1 represents not configured yet")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
        @ApiResponse(code = 404, message = "Namespace does not exist") })
    public DispatchRate getReplicatorDispatchRate(@PathParam("tenant") String tenant,
                                                    @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetReplicatorDispatchRate();
    }

    @GET
    @Path("/{tenant}/{namespace}/backlogQuotaMap")
    @ApiOperation(value = "Get backlog quota map on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public Map<BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperation(
                NamespaceName.get(tenant, namespace), PolicyName.BACKLOG, PolicyOperation.READ);
        Policies policies = getNamespacePolicies(namespaceName);
        return policies.backlog_quota_map;
    }

    @POST
    @Path("/{tenant}/{namespace}/backlogQuota")
    @ApiOperation(value = " Set a backlog quota for all the topics on a namespace.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412,
                    message = "Specified backlog quota exceeds retention quota."
                            + " Increase retention quota and retry request")})
    public void setBacklogQuota(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @QueryParam("backlogQuotaType") BacklogQuotaType backlogQuotaType,
            @ApiParam(value = "Backlog quota for all topics of the specified namespace") BacklogQuota backlogQuota) {
        validateNamespaceName(tenant, namespace);
        internalSetBacklogQuota(backlogQuotaType, backlogQuota);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/backlogQuota")
    @ApiOperation(value = "Remove a backlog quota policy from a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void removeBacklogQuota(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @QueryParam("backlogQuotaType") BacklogQuotaType backlogQuotaType) {
        validateNamespaceName(tenant, namespace);
        internalRemoveBacklogQuota(backlogQuotaType);
    }

    @GET
    @Path("/{tenant}/{namespace}/retention")
    @ApiOperation(value = "Get retention config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public RetentionPolicies getRetention(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetRetention();
    }

    @POST
    @Path("/{tenant}/{namespace}/retention")
    @ApiOperation(value = " Set retention configuration on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 400, message = "Invalid persistence policies")})
    public void setPersistence(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
                               @ApiParam(value = "Persistence policies for the specified namespace", required = true)
                                       PersistencePolicies persistence) {
        validateNamespaceName(tenant, namespace);
        internalSetPersistence(persistence);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/persistence")
    @ApiOperation(value = "Delete the persistence configuration for all topics on a namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void deletePersistence(@PathParam("tenant") String tenant,
                                               @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalDeletePersistence();
    }

    @POST
    @Path("/{tenant}/{namespace}/persistence/bookieAffinity")
    @ApiOperation(value = "Set the bookie-affinity-group to namespace-persistent policy.")
    @ApiResponses(value = {
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
    @ApiOperation(value = "Get the bookie-affinity-group from namespace-local policy.")
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
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void deleteBookieAffinityGroup(@PathParam("property") String property,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, namespace);
        internalDeleteBookieAffinityGroup();
    }

    @GET
    @Path("/{tenant}/{namespace}/persistence")
    @ApiOperation(value = "Get the persistence configuration for a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public PersistencePolicies getPersistence(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetPersistence();
    }

    @POST
    @Path("/{tenant}/{namespace}/clearBacklog")
    @ApiOperation(value = "Clear backlog for all topics on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
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
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
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
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void setSubscriptionAuthMode(@PathParam("tenant") String tenant,
                                        @PathParam("namespace") String namespace, @ApiParam(value =
            "Subscription auth mode for all topics of the specified namespace")
                                                    SubscriptionAuthMode subscriptionAuthMode) {
        validateNamespaceName(tenant, namespace);
        internalSetSubscriptionAuthMode(subscriptionAuthMode);
    }

    @POST
    @Path("/{tenant}/{namespace}/encryptionRequired")
    @ApiOperation(value = "Message encryption is required or not for all topics in a namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @Path("/{tenant}/{namespace}/delayedDelivery")
    @ApiOperation(value = "Get delayed delivery messages config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"), })
    public DelayedDeliveryPolicies getDelayedDeliveryPolicies(@PathParam("tenant") String tenant,
                                         @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetDelayedDelivery();
    }

    @POST
    @Path("/{tenant}/{namespace}/delayedDelivery")
    @ApiOperation(value = "Set delayed delivery messages config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"), })
    public void removeDelayedDeliveryPolicies(@PathParam("tenant") String tenant,
                                           @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetDelayedDelivery(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/inactiveTopicPolicies")
    @ApiOperation(value = "Get inactive topic policies config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"), })
    public InactiveTopicPolicies getInactiveTopicPolicies(@PathParam("tenant") String tenant,
                                                              @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetInactiveTopic();
    }

    @DELETE
    @Path("/{tenant}/{namespace}/inactiveTopicPolicies")
    @ApiOperation(value = "Remove inactive topic policies from a namespace.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiOperation(value = "Get maxProducersPerTopic config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public Integer getMaxProducersPerTopic(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetMaxProducersPerTopic();
    }

    @POST
    @Path("/{tenant}/{namespace}/maxProducersPerTopic")
    @ApiOperation(value = " Set maxProducersPerTopic configuration on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void removeMaxProducersPerTopic(@PathParam("tenant") String tenant,
                                               @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxProducersPerTopic(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/deduplicationSnapshotInterval")
    @ApiOperation(value = "Get deduplicationSnapshotInterval config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public Integer getDeduplicationSnapshotInterval(@PathParam("tenant") String tenant,
                                                @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetDeduplicationSnapshotInterval();
    }

    @POST
    @Path("/{tenant}/{namespace}/deduplicationSnapshotInterval")
    @ApiOperation(value = "Set deduplicationSnapshotInterval config on a namespace.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiOperation(value = "Get maxConsumersPerTopic config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public Integer getMaxConsumersPerTopic(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetMaxConsumersPerTopic();
    }

    @POST
    @Path("/{tenant}/{namespace}/maxConsumersPerTopic")
    @ApiOperation(value = " Set maxConsumersPerTopic configuration on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void removeMaxConsumersPerTopic(@PathParam("tenant") String tenant,
                                               @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxConsumersPerTopic(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/maxConsumersPerSubscription")
    @ApiOperation(value = "Get maxConsumersPerSubscription config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public Integer getMaxConsumersPerSubscription(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetMaxConsumersPerSubscription();
    }

    @POST
    @Path("/{tenant}/{namespace}/maxConsumersPerSubscription")
    @ApiOperation(value = " Set maxConsumersPerSubscription configuration on a namespace.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiOperation(value = "Get maxUnackedMessagesPerConsumer config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public Integer getMaxUnackedMessagesPerConsumer(@PathParam("tenant") String tenant,
                                       @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetMaxUnackedMessagesPerConsumer();
    }

    @POST
    @Path("/{tenant}/{namespace}/maxUnackedMessagesPerConsumer")
    @ApiOperation(value = " Set maxConsumersPerTopic configuration on a namespace.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void removeMaxUnackedmessagesPerConsumer(@PathParam("tenant") String tenant,
                                                        @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxUnackedMessagesPerConsumer(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/maxUnackedMessagesPerSubscription")
    @ApiOperation(value = "Get maxUnackedMessagesPerSubscription config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public Integer getMaxUnackedmessagesPerSubscription(@PathParam("tenant") String tenant,
                                              @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetMaxUnackedMessagesPerSubscription();
    }

    @POST
    @Path("/{tenant}/{namespace}/maxUnackedMessagesPerSubscription")
    @ApiOperation(value = " Set maxUnackedMessagesPerSubscription configuration on a namespace.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void removeMaxUnackedmessagesPerSubscription(@PathParam("tenant") String tenant,
                                                        @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetMaxUnackedMessagesPerSubscription(null);
    }

    @GET
    @Path("/{tenant}/{namespace}/maxSubscriptionsPerTopic")
    @ApiOperation(value = "Get maxSubscriptionsPerTopic config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public Integer getMaxSubscriptionsPerTopic(@PathParam("tenant") String tenant,
                                              @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetMaxSubscriptionsPerTopic();
    }

    @POST
    @Path("/{tenant}/{namespace}/maxSubscriptionsPerTopic")
    @ApiOperation(value = " Set maxSubscriptionsPerTopic configuration on a namespace.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiOperation(value = "Get anti-affinity group of a namespace.")
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
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
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
            + " api can be only accessed by admin of any of the existing tenant")
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
                          + "A threshold of 0 disabled automatic compaction")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
    public Long getCompactionThreshold(@PathParam("tenant") String tenant,
                                       @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetCompactionThreshold();
    }

    @PUT
    @Path("/{tenant}/{namespace}/compactionThreshold")
    @ApiOperation(value = "Set maximum number of uncompacted bytes in a topic before compaction is triggered.",
            notes = "The backlog size is compared to the threshold periodically. "
                    + "A threshold of 0 disabled automatic compaction")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
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
                  notes = "A negative value disables automatic offloading")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
    public long getOffloadThreshold(@PathParam("tenant") String tenant,
                                       @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetOffloadThreshold();
    }

    @PUT
    @Path("/{tenant}/{namespace}/offloadThreshold")
    @ApiOperation(value = "Set maximum number of bytes stored on the pulsar cluster for a topic,"
            + " before the broker will start offloading to longterm storage",
            notes = "-1 will revert to using the cluster default."
                    + " A negative value disables automatic offloading. ")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @Path("/{tenant}/{namespace}/offloadDeletionLagMs")
    @ApiOperation(value = "Number of milliseconds to wait before deleting a ledger segment which has been offloaded"
                          + " from the Pulsar cluster's local storage (i.e. BookKeeper)",
                  notes = "A negative value denotes that deletion has been completely disabled."
                          + " 'null' denotes that the topics in the namespace will fall back to the"
                          + " broker default for deletion lag.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
    public Long getOffloadDeletionLag(@PathParam("tenant") String tenant,
                                      @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetOffloadDeletionLag();
    }

    @PUT
    @Path("/{tenant}/{namespace}/offloadDeletionLagMs")
    @ApiOperation(value = "Set number of milliseconds to wait before deleting a ledger segment which has been offloaded"
            + " from the Pulsar cluster's local storage (i.e. BookKeeper)",
            notes = "A negative value disables the deletion completely.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
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
                          + " If set to AutoUpdateDisabled, schemas must be updated through the REST api")
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
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiOperation(value = "The strategy of the namespace schema compatibility ")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public SchemaCompatibilityStrategy getSchemaCompatibilityStrategy(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetSchemaCompatibilityStrategy();
    }

    @PUT
    @Path("/{tenant}/{namespace}/schemaCompatibilityStrategy")
    @ApiOperation(value = "Update the strategy used to check the compatibility of new schema")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiOperation(value = "The flag of whether allow auto update schema")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public boolean getIsAllowAutoUpdateSchema(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetIsAllowAutoUpdateSchema();
    }

    @POST
    @Path("/{tenant}/{namespace}/isAllowAutoUpdateSchema")
    @ApiOperation(value = "Update flag of whether allow auto update schema")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiOperation(value = "The set of whether allow subscription types")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public Set<SubscriptionType> getSubscriptionTypesEnabled(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetSubscriptionTypesEnabled();
    }

    @POST
    @Path("/{tenant}/{namespace}/subscriptionTypesEnabled")
    @ApiOperation(value = "Update set of whether allow share sub type")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
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


    @GET
    @Path("/{tenant}/{namespace}/schemaValidationEnforced")
    @ApiOperation(value = "Get schema validation enforced flag for namespace.",
                  notes = "If the flag is set to true, when a producer without a schema attempts to produce to a topic"
                          + " with schema in this namespace, the producer will be failed to connect. PLEASE be"
                          + " carefully on using this, since non-java clients don't support schema.if you enable"
                          + " this setting, it will cause non-java clients failed to produce.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 404, message = "Tenants or Namespace doesn't exist") })
    public boolean getSchemaValidtionEnforced(@PathParam("tenant") String tenant,
                                           @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetSchemaValidationEnforced();
    }

    @POST
    @Path("/{tenant}/{namespace}/schemaValidationEnforced")
    @ApiOperation(value = "Set schema validation enforced flag on namespace.",
            notes = "If the flag is set to true, when a producer without a schema attempts to produce to a topic"
                    + " with schema in this namespace, the producer will be failed to connect. PLEASE be"
                    + " carefully on using this, since non-java clients don't support schema.if you enable"
                    + " this setting, it will cause non-java clients failed to produce.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or Namespace doesn't exist"),
            @ApiResponse(code = 412, message = "schemaValidationEnforced value is not valid")})
    public void setSchemaValidtionEnforced(@PathParam("tenant") String tenant,
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
    @ApiOperation(value = " Set offload configuration on a namespace.")
    @ApiResponses(value = {
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
    @ApiOperation(value = "Get offload configuration on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist")})
    public OffloadPoliciesImpl getOffloadPolicies(@PathParam("tenant") String tenant,
                                                  @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetOffloadPolicies();
    }

    @GET
    @Path("/{tenant}/{namespace}/maxTopicsPerNamespace")
    @ApiOperation(value = "Get maxTopicsPerNamespace config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or namespace does not exist") })
    public Integer getMaxTopicsPerNamespace(@PathParam("tenant") String tenant,
                                              @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetMaxTopicsPerNamespace();
    }

    @POST
    @Path("/{tenant}/{namespace}/maxTopicsPerNamespace")
    @ApiOperation(value = "Set maxTopicsPerNamespace config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiOperation(value = "Set maxTopicsPerNamespace config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or namespace doesn't exist"), })
    public void setInactiveTopicPolicies(@PathParam("tenant") String tenant,
                                         @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalRemoveMaxTopicsPerNamespace();
    }

    @PUT
    @Path("/{tenant}/{namespace}/property/{key}/{value}")
    @ApiOperation(value = "Put a key value pair property on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiOperation(value = "Get property value for a given key on a namespace.")
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
    @ApiOperation(value = "Get property value for a given key on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiOperation(value = "Get key value pair properties for a given namespace.")
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
    @ApiOperation(value = "Get property value for a given key on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiOperation(value = "Get the resourcegroup attached to the namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
    public String getNamespaceResourceGroup(@PathParam("tenant") String tenant,
                                      @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        validateNamespacePolicyOperation(NamespaceName.get(tenant, namespace), PolicyName.RESOURCEGROUP,
                PolicyOperation.READ);

        Policies policies = getNamespacePolicies(namespaceName);
        return policies.resource_group_name;
    }

    @POST
    @Path("/{tenant}/{namespace}/resourcegroup/{resourcegroup}")
    @ApiOperation(value = "Set resourcegroup for a namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
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
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Invalid resourcegroup")})
    public void removeNamespaceResourceGroup(@PathParam("tenant") String tenant,
                                          @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        internalSetNamespaceResourceGroup(null);
    }

    private static final Logger log = LoggerFactory.getLogger(Namespaces.class);
}
