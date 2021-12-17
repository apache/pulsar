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
package org.apache.pulsar.broker.admin.v1;

import static org.apache.pulsar.common.policies.data.PoliciesUtil.getBundles;
import com.google.common.collect.Lists;
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
import javax.ws.rs.core.Response.Status;
import org.apache.pulsar.broker.admin.impl.NamespacesBase;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/namespaces")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(value = "/namespaces", description = "Namespaces admin apis", tags = "namespaces", hidden = true)
@SuppressWarnings("deprecation")
public class Namespaces extends NamespacesBase {

    @GET
    @Path("/{property}")
    @ApiOperation(value = "Get the list of all the namespaces for a certain property.",
            response = String.class, responseContainer = "Set")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property doesn't exist")})
    public List<String> getTenantNamespaces(@PathParam("property") String property) {
        return internalGetTenantNamespaces(property);
    }

    @GET
    @Path("/{property}/{cluster}")
    @ApiOperation(hidden = true, value = "Get the list of all the namespaces for a certain property on single cluster.",
            response = String.class, responseContainer = "Set")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster doesn't exist")})
    public List<String> getNamespacesForCluster(@PathParam("property") String tenant,
            @PathParam("cluster") String cluster) {
        validateTenantOperation(tenant, TenantOperation.LIST_NAMESPACES);
        List<String> namespaces = Lists.newArrayList();
        if (!clusters().contains(cluster)) {
            log.warn("[{}] Failed to get namespace list for tenant: {}/{} - Cluster does not exist", clientAppId(),
                    tenant, cluster);
            throw new RestException(Status.NOT_FOUND, "Cluster does not exist");
        }
        try {
            for (String namespace : clusterResources().getNamespacesForCluster(tenant, cluster)) {
                namespaces.add(NamespaceName.get(tenant, cluster, namespace).toString());
            }
        } catch (NotFoundException e) {
            // NoNode means there are no namespaces for this property on the specified cluster, returning empty list
        } catch (Exception e) {
            log.error("[{}] Failed to get namespaces list: {}", clientAppId(), e);
            throw new RestException(e);
        }

        namespaces.sort(null);
        return namespaces;
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/destinations")
    @ApiOperation(hidden = true, value = "Get the list of all the topics under a certain namespace.",
            response = String.class, responseContainer = "Set")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist")})
    public void getTopics(@PathParam("property") String property,
                                  @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
                                  @QueryParam("mode") @DefaultValue("PERSISTENT") Mode mode,
                                  @Suspended AsyncResponse asyncResponse) {
        validateNamespaceName(property, cluster, namespace);
        validateNamespaceOperation(NamespaceName.get(property, namespace), NamespaceOperation.GET_TOPICS);

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
    @Path("/{property}/{cluster}/{namespace}")
    @ApiOperation(hidden = true, value = "Get the dump all the policies specified for a namespace.",
            response = Policies.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist")})
    public Policies getPolicies(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        validateNamespacePolicyOperation(NamespaceName.get(property, namespace), PolicyName.ALL, PolicyOperation.READ);
        return getNamespacePolicies(namespaceName);
    }

    @SuppressWarnings("deprecation")
    @PUT
    @Path("/{property}/{cluster}/{namespace}")
    @ApiOperation(hidden = true, value = "Creates a new empty namespace with no policies attached.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Namespace already exists"),
            @ApiResponse(code = 412, message = "Namespace name is not valid") })
    public void createNamespace(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, BundlesData initialBundles) {
        validateNamespaceName(property, cluster, namespace);

        if (!namespaceName.isGlobal()) {
            // If the namespace is non global, make sure property has the access on the cluster. For global namespace,
            // same check is made at the time of setting replication.
            validateClusterForTenant(namespaceName.getTenant(), namespaceName.getCluster());
        }

        Policies policies = new Policies();
        if (initialBundles != null && initialBundles.getNumBundles() > 0) {
            if (initialBundles.getBoundaries() == null || initialBundles.getBoundaries().size() == 0) {
                policies.bundles = getBundles(initialBundles.getNumBundles());
            } else {
                policies.bundles = validateBundlesData(initialBundles);
            }
        } else {
            int defaultNumberOfBundles = config().getDefaultNumberOfNamespaceBundles();
            policies.bundles = getBundles(defaultNumberOfBundles);
        }

        internalCreateNamespace(policies);
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}")
    @ApiOperation(hidden = true, value = "Delete a namespace and all the topics under it.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 405, message = "Broker doesn't allow forced deletion of namespaces"),
            @ApiResponse(code = 409, message = "Namespace is not empty") })
    public void deleteNamespace(@Suspended final AsyncResponse asyncResponse, @PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @QueryParam("force") @DefaultValue("false") boolean force,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateNamespaceName(property, cluster, namespace);
            internalDeleteNamespace(asyncResponse, authoritative, force);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/{bundle}")
    @ApiOperation(hidden = true, value = "Delete a namespace bundle and all the topics under it.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Namespace bundle is not empty") })
    public void deleteNamespaceBundle(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("bundle") String bundleRange,
            @QueryParam("force") @DefaultValue("false") boolean force,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(property, cluster, namespace);
        internalDeleteNamespaceBundle(bundleRange, authoritative, force);
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/permissions")
    @ApiOperation(hidden = true, value = "Retrieve the permissions for a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Namespace is not empty") })
    public Map<String, Set<AuthAction>> getPermissions(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        validateNamespaceOperation(NamespaceName.get(property, namespace), NamespaceOperation.GET_PERMISSION);

        Policies policies = getNamespacePolicies(namespaceName);
        return policies.auth_policies.getNamespaceAuthentication();
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/permissions/subscription")
    @ApiOperation(value = "Retrieve the permissions for a subscription.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Namespace is not empty")})
    public Map<String, Set<String>> getPermissionOnSubscription(@PathParam("property") String property,
                                                                @PathParam("cluster") String cluster,
                                                                @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        validateNamespaceOperation(NamespaceName.get(property, namespace), NamespaceOperation.GET_PERMISSION);

        Policies policies = getNamespacePolicies(namespaceName);
        return policies.auth_policies.getSubscriptionAuthentication();
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/permissions/{role}")
    @ApiOperation(hidden = true, value = "Grant a new permission to a role on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 501, message = "Authorization is not enabled")})
    public void grantPermissionOnNamespace(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("role") String role, Set<AuthAction> actions) {
        validateNamespaceName(property, cluster, namespace);
        internalGrantPermissionOnNamespace(role, actions);
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/permissions/subscription/{subscription}")
    @ApiOperation(hidden = true, value = "Grant a new permission to roles for a subscription. "
            + "[Tenant admin is allowed to perform this operation]")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 501, message = "Authorization is not enabled")})
    public void grantPermissionOnSubscription(
            @PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("subscription") String subscription,
            Set<String> roles) {
        validateNamespaceName(property, cluster, namespace);
        internalGrantPermissionOnSubscription(subscription, roles);
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/permissions/{role}")
    @ApiOperation(hidden = true, value = "Revoke all permissions to a role on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist") })
    public void revokePermissionsOnNamespace(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("role") String role) {
        validateNamespaceName(property, cluster, namespace);
        internalRevokePermissionsOnNamespace(role);
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/permissions/{subscription}/{role}")
    @ApiOperation(hidden = true, value = "Revoke subscription admin-api access permission for a role.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist") })
    public void revokePermissionOnSubscription(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("subscription") String subscription, @PathParam("role") String role) {
        validateNamespaceName(property, cluster, namespace);
        internalRevokePermissionsOnSubscription(subscription, role);
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/replication")
    @ApiOperation(hidden = true, value = "Get the replication clusters for a namespace.",
            response = String.class, responseContainer = "List")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Namespace is not global")})
    public Set<String> getNamespaceReplicationClusters(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        validateNamespacePolicyOperation(NamespaceName.get(property, namespace),
                PolicyName.REPLICATION, PolicyOperation.READ);

        return internalGetNamespaceReplicationClusters();
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/replication")
    @ApiOperation(hidden = true, value = "Set the replication clusters for a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Peer-cluster can't be part of replication-cluster"),
            @ApiResponse(code = 412, message = "Namespace is not global or invalid cluster ids") })
    public void setNamespaceReplicationClusters(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace, List<String> clusterIds) {
        validateNamespaceName(property, cluster, namespace);
        internalSetNamespaceReplicationClusters(clusterIds);
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/messageTTL")
    @ApiOperation(hidden = true, value = "Get the message TTL for the namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist") })
    public Integer getNamespaceMessageTTL(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        validateNamespacePolicyOperation(NamespaceName.get(property, namespace), PolicyName.TTL, PolicyOperation.READ);

        Policies policies = getNamespacePolicies(namespaceName);
        return policies.message_ttl_in_seconds;
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/messageTTL")
    @ApiOperation(hidden = true, value = "Set message TTL in seconds for namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Invalid TTL") })
    public void setNamespaceMessageTTL(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, int messageTTL) {
        validateNamespaceName(property, cluster, namespace);
        internalSetNamespaceMessageTTL(messageTTL);
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/messageTTL")
    @ApiOperation(value = "Set message TTL in seconds for namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Invalid TTL") })
    public void removeNamespaceMessageTTL(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        internalSetNamespaceMessageTTL(null);
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/subscriptionExpirationTime")
    @ApiOperation(hidden = true, value = "Get the subscription expiration time for the namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist") })
    public Integer getSubscriptionExpirationTime(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace) {
        validateAdminAccessForTenant(property);
        validateNamespaceName(property, cluster, namespace);

        Policies policies = getNamespacePolicies(namespaceName);
        return policies.subscription_expiration_time_minutes;
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/subscriptionExpirationTime")
    @ApiOperation(hidden = true, value = "Set subscription expiration time in minutes for namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Invalid expiration time") })
    public void setSubscriptionExpirationTime(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace, int expirationTime) {
        validateNamespaceName(property, cluster, namespace);
        internalSetSubscriptionExpirationTime(expirationTime);
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/subscriptionExpirationTime")
    @ApiOperation(hidden = true, value = "Remove subscription expiration time for namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist") })
    public void removeSubscriptionExpirationTime(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        internalSetSubscriptionExpirationTime(null);
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/antiAffinity")
    @ApiOperation(value = "Set anti-affinity group for a namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Invalid antiAffinityGroup") })
    public void setNamespaceAntiAffinityGroup(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace, String antiAffinityGroup) {
        validateNamespaceName(property, cluster, namespace);
        internalSetNamespaceAntiAffinityGroup(antiAffinityGroup);
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/antiAffinity")
    @ApiOperation(value = "Get anti-affinity group of a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist") })
    public String getNamespaceAntiAffinityGroup(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        return internalGetNamespaceAntiAffinityGroup();
    }

    @GET
    @Path("{cluster}/antiAffinity/{group}")
    @ApiOperation(value = "Get all namespaces that are grouped by given anti-affinity group in a given cluster."
            + " api can be only accessed by admin of any of the existing property")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 412, message = "Cluster not exist/Anti-affinity group can't be empty.")})
    public List<String> getAntiAffinityNamespaces(@PathParam("cluster") String cluster,
                                                  @PathParam("group") String antiAffinityGroup,
                                                  @QueryParam("property") String property) {
        return internalGetAntiAffinityNamespaces(cluster, antiAffinityGroup, property);
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/antiAffinity")
    @ApiOperation(value = "Remove anti-affinity group of a namespace.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeNamespaceAntiAffinityGroup(@PathParam("property") String property,
                                                 @PathParam("cluster") String cluster,
                                                 @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        internalRemoveNamespaceAntiAffinityGroup();
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/deduplication")
    @ApiOperation(hidden = true, value = "Enable or disable broker side deduplication for all topics in a namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist") })
    public void modifyDeduplication(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, boolean enableDeduplication) {
        validateNamespaceName(property, cluster, namespace);
        internalModifyDeduplication(enableDeduplication);
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/autoTopicCreation")
    @ApiOperation(value = "Get autoTopicCreation info in a namespace")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist")})
    public AutoTopicCreationOverride getAutoTopicCreation(@PathParam("property") String property,
                                                          @PathParam("cluster") String cluster,
                                                          @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        return internalGetAutoTopicCreation();
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/autoTopicCreation")
    @ApiOperation(value = "Override broker's allowAutoTopicCreation setting for a namespace")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 406, message = "The number of partitions should be less than or equal to"
                    + " maxNumPartitionsPerPartitionedTopic"),
            @ApiResponse(code = 400, message = "Invalid autoTopicCreation override")})
    public void setAutoTopicCreation(@Suspended final AsyncResponse asyncResponse,
                                     @PathParam("property") String property, @PathParam("cluster") String cluster,
                                     @PathParam("namespace") String namespace,
                                     AutoTopicCreationOverride autoTopicCreationOverride) {
        try {
            validateNamespaceName(property, cluster, namespace);
            internalSetAutoTopicCreation(asyncResponse, autoTopicCreationOverride);
        } catch (RestException e) {
            asyncResponse.resume(e);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/autoTopicCreation")
    @ApiOperation(value = "Remove override of broker's allowAutoTopicCreation in a namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
    public void removeAutoTopicCreation(@Suspended final AsyncResponse asyncResponse,
                                        @PathParam("property") String property, @PathParam("cluster") String cluster,
                                        @PathParam("namespace") String namespace) {
        try {
            validateNamespaceName(property, cluster, namespace);
            internalRemoveAutoTopicCreation(asyncResponse);
        } catch (RestException e) {
            asyncResponse.resume(e);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/autoSubscriptionCreation")
    @ApiOperation(value = "Override broker's allowAutoSubscriptionCreation setting for a namespace")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist"),
            @ApiResponse(code = 400, message = "Invalid autoSubscriptionCreation override")})
    public void setAutoSubscriptionCreation(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace,
            AutoSubscriptionCreationOverride autoSubscriptionCreationOverride) {
        try {
            validateNamespaceName(property, cluster, namespace);
            internalSetAutoSubscriptionCreation(asyncResponse, autoSubscriptionCreationOverride);
        } catch (RestException e) {
            asyncResponse.resume(e);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/autoSubscriptionCreation")
    @ApiOperation(value = "Get autoSubscriptionCreation info in a namespace")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist")})
    public AutoSubscriptionCreationOverride getAutoSubscriptionCreation(@PathParam("property") String property,
                                                                        @PathParam("cluster") String cluster,
                                                                        @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        return internalGetAutoSubscriptionCreation();
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/autoSubscriptionCreation")
    @ApiOperation(value = "Remove override of broker's allowAutoSubscriptionCreation in a namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace doesn't exist") })
    public void removeAutoSubscriptionCreation(@Suspended final AsyncResponse asyncResponse,
                                        @PathParam("property") String property, @PathParam("cluster") String cluster,
                                        @PathParam("namespace") String namespace) {
        try {
            validateNamespaceName(property, cluster, namespace);
            internalRemoveAutoSubscriptionCreation(asyncResponse);
        } catch (RestException e) {
            asyncResponse.resume(e);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/bundles")
    @ApiOperation(hidden = true, value = "Get the bundles split data.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Namespace is not setup to split in bundles") })
    public BundlesData getBundlesData(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace) {
        validatePoliciesReadOnlyAccess();
        validateNamespaceName(property, cluster, namespace);
        validateNamespaceOperation(NamespaceName.get(property, namespace), NamespaceOperation.GET_BUNDLE);

        Policies policies = getNamespacePolicies(namespaceName);

        return policies.bundles;
    }

    @PUT
    @Path("/{property}/{cluster}/{namespace}/unload")
    @ApiOperation(hidden = true, value = "Unload namespace",
            notes = "Unload an active namespace from the current broker serving it."
                    + " Performing this operation will let the brokerremoves all producers,"
                    + " consumers, and connections using this namespace, and close all topics "
                    + "(includingtheir persistent store). During that operation, the namespace is marked "
                    + "as tentatively unavailable until thebroker completes the unloading action. "
                    + "This operation requires strictly super user privileges, since it wouldresult in"
                    + " non-persistent message loss and unexpected connection closure to the clients.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Namespace is already unloaded or Namespace has bundles activated")})
    public void unloadNamespace(@Suspended final AsyncResponse asyncResponse, @PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace) {
        try {
            validateNamespaceName(property, cluster, namespace);
            internalUnloadNamespace(asyncResponse);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @PUT
    @Path("/{property}/{cluster}/{namespace}/{bundle}/unload")
    @ApiOperation(hidden = true, value = "Unload a namespace bundle")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void unloadNamespaceBundle(@Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(property, cluster, namespace);
        internalUnloadNamespaceBundle(asyncResponse, bundleRange, authoritative);
    }

    @PUT
    @Path("/{property}/{cluster}/{namespace}/{bundle}/split")
    @ApiOperation(hidden = true, value = "Split a namespace bundle")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void splitNamespaceBundle(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property,
            @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace,
            @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("unload") @DefaultValue("false") boolean unload) {
        try {
            validateNamespaceName(property, cluster, namespace);
            internalSplitNamespaceBundle(asyncResponse, bundleRange, authoritative, unload,
                    NamespaceBundleSplitAlgorithm.RANGE_EQUALLY_DIVIDE_NAME);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/publishRate")
    @ApiOperation(hidden = true, value = "Set publish-rate throttling for all topics of the namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void setPublishRate(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, PublishRate publishRate) {
        validateNamespaceName(property, cluster, namespace);
        internalSetPublishRate(publishRate);
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/publishRate")
    @ApiOperation(hidden = true,
            value = "Get publish-rate configured for the namespace, null means publish-rate not configured, "
                    + "-1 means msg-publish-rate or byte-publish-rate not configured in publish-rate yet")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist")})
    public PublishRate getPublishRate(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        return internalGetPublishRate();
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/dispatchRate")
    @ApiOperation(hidden = true, value = "Set dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void setDispatchRate(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, DispatchRateImpl dispatchRate) {
        validateNamespaceName(property, cluster, namespace);
        internalSetTopicDispatchRate(dispatchRate);
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/dispatchRate")
    @ApiOperation(hidden = true,
            value = "Get dispatch-rate configured for the namespace, null means dispatch-rate not configured, "
                    + "-1 means msg-dispatch-rate or byte-dispatch-rate not configured in dispatch-rate yet")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist")})
    public DispatchRate getDispatchRate(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        return internalGetTopicDispatchRate();
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/subscriptionDispatchRate")
    @ApiOperation(value = "Set Subscription dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void setSubscriptionDispatchRate(@PathParam("property") String property,
                                            @PathParam("cluster") String cluster,
                                            @PathParam("namespace") String namespace,
                                            DispatchRateImpl dispatchRate) {
        validateNamespaceName(property, cluster, namespace);
        internalSetSubscriptionDispatchRate(dispatchRate);
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/subscriptionDispatchRate")
    @ApiOperation(value = "Get subscription dispatch-rate configured for the namespace, null means subscription "
            + "dispatch-rate not configured, -1 means msg-dispatch-rate or byte-dispatch-rate not configured "
            + "in dispatch-rate yet")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist")})
    public DispatchRate getSubscriptionDispatchRate(@PathParam("property") String property,
                                                    @PathParam("cluster") String cluster,
                                                    @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        return internalGetSubscriptionDispatchRate();
    }

    @POST
    @Path("/{tenant}/{cluster}/{namespace}/replicatorDispatchRate")
    @ApiOperation(value = "Set replicator dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission")})
    public void setReplicatorDispatchRate(
            @PathParam("tenant") String tenant,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @ApiParam(value = "Replicator dispatch rate for all topics of the specified namespace")
                    DispatchRateImpl dispatchRate) {
        validateNamespaceName(tenant, cluster, namespace);
        internalSetReplicatorDispatchRate(dispatchRate);
    }

    @GET
    @Path("/{tenant}/{cluster}/{namespace}/replicatorDispatchRate")
    @ApiOperation(value = "Get replicator dispatch-rate configured for the namespace, null means replicator "
            + "dispatch-rate not configured, -1 means msg-dispatch-rate or byte-dispatch-rate not configured "
            + "in dispatch-rate yet")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
        @ApiResponse(code = 404, message = "Namespace does not exist") })
    public DispatchRate getReplicatorDispatchRate(@PathParam("tenant") String tenant,
                                                    @PathParam("cluster") String cluster,
                                                    @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, cluster, namespace);
        return internalGetReplicatorDispatchRate();
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/backlogQuotaMap")
    @ApiOperation(hidden = true, value = "Get backlog quota map on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public Map<BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        validateNamespacePolicyOperation(NamespaceName.get(property, namespace),
                PolicyName.BACKLOG, PolicyOperation.READ);

        Policies policies = getNamespacePolicies(namespaceName);
        return policies.backlog_quota_map;
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/backlogQuota")
    @ApiOperation(hidden = true, value = " Set a backlog quota for all the topics on a namespace.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Specified backlog quota exceeds retention quota."
                    + " Increase retention quota and retry request")})
    public void setBacklogQuota(
            @PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace,
            @QueryParam("backlogQuotaType") BacklogQuotaType backlogQuotaType,
            BacklogQuota backlogQuota) {
        validateNamespaceName(property, cluster, namespace);
        internalSetBacklogQuota(backlogQuotaType, backlogQuota);
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/backlogQuota")
    @ApiOperation(hidden = true, value = "Remove a backlog quota policy from a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void removeBacklogQuota(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace,
            @QueryParam("backlogQuotaType") BacklogQuotaType backlogQuotaType) {
        validateNamespaceName(property, cluster, namespace);
        internalRemoveBacklogQuota(backlogQuotaType);
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/retention")
    @ApiOperation(hidden = true, value = "Get retention config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public RetentionPolicies getRetention(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        return internalGetRetention();
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/retention")
    @ApiOperation(hidden = true, value = " Set retention configuration on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Retention Quota must exceed backlog quota") })
    public void setRetention(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, RetentionPolicies retention) {
        validateNamespaceName(property, cluster, namespace);
        internalSetRetention(retention);
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/persistence")
    @ApiOperation(hidden = true, value = "Set the persistence configuration for all the topics on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 400, message = "Invalid persistence policies") })
    public void setPersistence(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, PersistencePolicies persistence) {
        validateNamespaceName(property, cluster, namespace);
        internalSetPersistence(persistence);
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/persistence/bookieAffinity")
    @ApiOperation(hidden = true, value = "Set the bookie-affinity-group to namespace-local policy.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void setBookieAffinityGroup(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, BookieAffinityGroupData bookieAffinityGroup) {
        validateNamespaceName(property, cluster, namespace);
        internalSetBookieAffinityGroup(bookieAffinityGroup);
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/persistence/bookieAffinity")
    @ApiOperation(hidden = true, value = "Get the bookie-affinity-group from namespace-local policy.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public BookieAffinityGroupData getBookieAffinityGroup(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        return internalGetBookieAffinityGroup();
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/persistence/bookieAffinity")
    @ApiOperation(hidden = true, value = "Delete the bookie-affinity-group from namespace-local policy.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void deleteBookieAffinityGroup(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        internalDeleteBookieAffinityGroup();
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/persistence")
    @ApiOperation(hidden = true, value = "Get the persistence configuration for a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public PersistencePolicies getPersistence(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        return internalGetPersistence();
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/clearBacklog")
    @ApiOperation(hidden = true, value = "Clear backlog for all topics on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void clearNamespaceBacklog(@Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateNamespaceName(property, cluster, namespace);
            internalClearNamespaceBacklog(asyncResponse, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{bundle}/clearBacklog")
    @ApiOperation(hidden = true, value = "Clear backlog for all topics on a namespace bundle.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void clearNamespaceBundleBacklog(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(property, cluster, namespace);
        internalClearNamespaceBundleBacklog(bundleRange, authoritative);
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/clearBacklog/{subscription}")
    @ApiOperation(hidden = true, value = "Clear backlog for a given subscription on all topics on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void clearNamespaceBacklogForSubscription(@Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("subscription") String subscription,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateNamespaceName(property, cluster, namespace);
            internalClearNamespaceBacklogForSubscription(asyncResponse, subscription, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{bundle}/clearBacklog/{subscription}")
    @ApiOperation(hidden = true, value = "Clear backlog for a given subscription on all topics on a namespace bundle.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void clearNamespaceBundleBacklogForSubscription(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("subscription") String subscription, @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(property, cluster, namespace);
        internalClearNamespaceBundleBacklogForSubscription(subscription, bundleRange, authoritative);
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/unsubscribe/{subscription}")
    @ApiOperation(hidden = true, value = "Unsubscribes the given subscription on all topics on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void unsubscribeNamespace(@Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("subscription") String subscription,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateNamespaceName(property, cluster, namespace);
            internalUnsubscribeNamespace(asyncResponse, subscription, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{bundle}/unsubscribe/{subscription}")
    @ApiOperation(hidden = true, value = "Unsubscribes the given subscription on all topics on a namespace bundle.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void unsubscribeNamespaceBundle(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("subscription") String subscription,
            @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(property, cluster, namespace);
        internalUnsubscribeNamespaceBundle(subscription, bundleRange, authoritative);
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/subscriptionAuthMode")
    @ApiOperation(value = " Set a subscription auth mode for all the topics on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void setSubscriptionAuthMode(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, SubscriptionAuthMode subscriptionAuthMode) {
        validateNamespaceName(property, cluster, namespace);
        internalSetSubscriptionAuthMode(subscriptionAuthMode);
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/subscriptionAuthMode")
    @ApiOperation(value = "Get subscription auth mode in a namespace")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist")})
    public SubscriptionAuthMode getSubscriptionAuthMode(@PathParam("property") String property,
                                                        @PathParam("cluster") String cluster,
                                                        @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        return internalGetSubscriptionAuthMode();
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/encryptionRequired")
    @ApiOperation(hidden = true, value = "Message encryption is required or not for all topics in a namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"), })
    public void modifyEncryptionRequired(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, boolean encryptionRequired) {
        validateNamespaceName(property, cluster, namespace);
        internalModifyEncryptionRequired(encryptionRequired);
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/encryptionRequired")
    @ApiOperation(value = "Get message encryption required status in a namespace")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist")})
    public Boolean getEncryptionRequired(@PathParam("property") String property,
                                         @PathParam("cluster") String cluster,
                                         @PathParam("namespace") String namespace) {
        validateAdminAccessForTenant(property);
        validateNamespaceName(property, cluster, namespace);
        return internalGetEncryptionRequired();
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/maxProducersPerTopic")
    @ApiOperation(value = "Get maxProducersPerTopic config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public int getMaxProducersPerTopic(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        return internalGetMaxProducersPerTopic();
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/maxProducersPerTopic")
    @ApiOperation(value = " Set maxProducersPerTopic configuration on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "maxProducersPerTopic value is not valid") })
    public void setMaxProducersPerTopic(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, int maxProducersPerTopic) {
        validateNamespaceName(property, cluster, namespace);
        internalSetMaxProducersPerTopic(maxProducersPerTopic);
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/maxConsumersPerTopic")
    @ApiOperation(value = "Get maxConsumersPerTopic config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public Integer getMaxConsumersPerTopic(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        return internalGetMaxConsumersPerTopic();
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/maxConsumersPerTopic")
    @ApiOperation(value = " Set maxConsumersPerTopic configuration on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "maxConsumersPerTopic value is not valid") })
    public void setMaxConsumersPerTopic(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, int maxConsumersPerTopic) {
        validateNamespaceName(property, cluster, namespace);
        internalSetMaxConsumersPerTopic(maxConsumersPerTopic);
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/maxConsumersPerSubscription")
    @ApiOperation(value = "Get maxConsumersPerSubscription config on a namespace.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist")})
    public Integer getMaxConsumersPerSubscription(@PathParam("property") String property,
                                              @PathParam("cluster") String cluster,
                                              @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        return internalGetMaxConsumersPerSubscription();
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/maxConsumersPerSubscription")
    @ApiOperation(value = " Set maxConsumersPerSubscription configuration on a namespace.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "maxConsumersPerSubscription value is not valid")})
    public void setMaxConsumersPerSubscription(
            @PathParam("property") String property,
            @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, int maxConsumersPerSubscription) {
        validateNamespaceName(property, cluster, namespace);
        internalSetMaxConsumersPerSubscription(maxConsumersPerSubscription);
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/maxConsumersPerSubscription")
    @ApiOperation(value = "Remove maxConsumersPerSubscription configuration on a namespace.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeMaxConsumersPerSubscription(
            @PathParam("property") String property,
            @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        internalSetMaxConsumersPerSubscription(null);
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/compactionThreshold")
    @ApiOperation(value = "Maximum number of uncompacted bytes in topics before compaction is triggered.",
                  notes = "The backlog size is compared to the threshold periodically. "
                          + "A threshold of 0 disabled automatic compaction")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
    public Long getCompactionThreshold(@PathParam("property") String property,
                                       @PathParam("cluster") String cluster,
                                       @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        return internalGetCompactionThreshold();
    }

    @PUT
    @Path("/{property}/{cluster}/{namespace}/compactionThreshold")
    @ApiOperation(value = "Set maximum number of uncompacted bytes in a topic before compaction is triggered.",
                  notes = "The backlog size is compared to the threshold periodically. "
                          + "A threshold of 0 disabled automatic compaction")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
                            @ApiResponse(code = 409, message = "Concurrent modification"),
                            @ApiResponse(code = 412, message = "compactionThreshold value is not valid") })
    public void setCompactionThreshold(@PathParam("property") String property,
                                       @PathParam("cluster") String cluster,
                                       @PathParam("namespace") String namespace,
                                       long newThreshold) {
        validateNamespaceName(property, cluster, namespace);
        internalSetCompactionThreshold(newThreshold);
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/offloadThreshold")
    @ApiOperation(value = "Maximum number of bytes stored on the pulsar cluster for a topic,"
                          + " before the broker will start offloading to longterm storage",
                  notes = "A negative value disables automatic offloading")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
    public long getOffloadThreshold(@PathParam("property") String property,
                                    @PathParam("cluster") String cluster,
                                    @PathParam("namespace") String namespace) {
        validateNamespaceName(property, cluster, namespace);
        return internalGetOffloadThreshold();
    }

    @PUT
    @Path("/{property}/{cluster}/{namespace}/offloadThreshold")
    @ApiOperation(value = "Set maximum number of bytes stored on the pulsar cluster for a topic,"
                          + " before the broker will start offloading to longterm storage",
                  notes = "A negative value disables automatic offloading")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
                            @ApiResponse(code = 409, message = "Concurrent modification"),
                            @ApiResponse(code = 412, message = "offloadThreshold value is not valid") })
    public void setOffloadThreshold(@PathParam("property") String property,
                                    @PathParam("cluster") String cluster,
                                    @PathParam("namespace") String namespace,
                                    long newThreshold) {
        validateNamespaceName(property, cluster, namespace);
        internalSetOffloadThreshold(newThreshold);
    }

    @GET
    @Path("/{tenant}/{cluster}/{namespace}/schemaAutoUpdateCompatibilityStrategy")
    @ApiOperation(value = "The strategy used to check the compatibility of new schemas,"
                          + " provided by producers, before automatically updating the schema",
                  notes = "The value AutoUpdateDisabled prevents producers from updating the schema. "
                          + " If set to AutoUpdateDisabled, schemas must be updated through the REST api")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
                            @ApiResponse(code = 409, message = "Concurrent modification") })
    public SchemaAutoUpdateCompatibilityStrategy getSchemaAutoUpdateCompatibilityStrategy(
            @PathParam("tenant") String tenant,
            @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, cluster, namespace);
        return internalGetSchemaAutoUpdateCompatibilityStrategy();
    }

    @PUT
    @Path("/{tenant}/{cluster}/{namespace}/schemaAutoUpdateCompatibilityStrategy")
    @ApiOperation(value = "Update the strategy used to check the compatibility of new schemas,"
                          + " provided by producers, before automatically updating the schema",
                  notes = "The value AutoUpdateDisabled prevents producers from updating the schema. "
                          + " If set to AutoUpdateDisabled, schemas must be updated through the REST api")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
                            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void setSchemaAutoUpdateCompatibilityStrategy(@PathParam("tenant") String tenant,
                                                         @PathParam("cluster") String cluster,
                                                         @PathParam("namespace") String namespace,
                                                         SchemaAutoUpdateCompatibilityStrategy strategy) {
        validateNamespaceName(tenant, cluster, namespace);
        internalSetSchemaAutoUpdateCompatibilityStrategy(strategy);
    }

    private static final Logger log = LoggerFactory.getLogger(Namespaces.class);
}
