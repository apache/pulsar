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
import javax.ws.rs.core.MediaType;

import org.apache.pulsar.broker.admin.impl.NamespacesBase;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Path("/namespaces")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(value = "/namespaces", description = "Namespaces admin apis", tags = "namespaces")
public class Namespaces extends NamespacesBase {

    @GET
    @Path("/{property}")
    @ApiOperation(value = "Get the list of all the namespaces for a certain property.", response = String.class, responseContainer = "Set")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property doesn't exist") })
    public List<String> getPropertyNamespaces(@PathParam("property") String property) {
        return internalGetPropertyNamespaces(property);
    }

    @GET
    @Path("/{property}/{namespace}/topics")
    @ApiOperation(value = "Get the list of all the topics under a certain namespace.", response = String.class, responseContainer = "Set")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist") })
    public List<String> getTopics(@PathParam("property") String property,
            @PathParam("namespace") String namespace) {
        validateAdminAccessOnProperty(property);
        validateNamespaceName(property, namespace);

        // Validate that namespace exists, throws 404 if it doesn't exist
        getNamespacePolicies(namespaceName);

        try {
            return pulsar().getNamespaceService().getListOfTopics(namespaceName);
        } catch (Exception e) {
            log.error("Failed to get topics list for namespace {}", namespaceName, e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{property}/{namespace}")
    @ApiOperation(value = "Get the dump all the policies specified for a namespace.", response = Policies.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist") })
    public Policies getPolicies(@PathParam("property") String property, @PathParam("namespace") String namespace) {
        validateAdminAccessOnProperty(property);
        validateNamespaceName(property, namespace);
        return getNamespacePolicies(namespaceName);
    }

    @PUT
    @Path("/{property}/{namespace}")
    @ApiOperation(value = "Creates a new namespace with the specified policies")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster doesn't exist"),
            @ApiResponse(code = 409, message = "Namespace already exists"),
            @ApiResponse(code = 412, message = "Namespace name is not valid") })
    public void createNamespace(@PathParam("property") String property, @PathParam("namespace") String namespace,
            Policies policies) {
        validateNamespaceName(property, namespace);

        policies = getDefaultPolicesIfNull(policies);
        internalCreateNamespace(policies);
    }

    @DELETE
    @Path("/{property}/{namespace}")
    @ApiOperation(value = "Delete a namespace and all the topics under it.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Namespace is not empty") })
    public void deleteNamespace(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(property, namespace);
        internalDeleteNamespace(authoritative);
    }

    @DELETE
    @Path("/{property}/{namespace}/bundle/{bundle}")
    @ApiOperation(value = "Delete a namespace bundle and all the topics under it.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Namespace bundle is not empty") })
    public void deleteNamespaceBundle(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(property, namespace);
        internalDeleteNamespaceBundle(bundleRange, authoritative);
    }

    @GET
    @Path("/{property}/{namespace}/permissions")
    @ApiOperation(value = "Retrieve the permissions for a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Namespace is not empty") })
    public Map<String, Set<AuthAction>> getPermissions(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace) {
        validateAdminAccessOnProperty(property);
        validateNamespaceName(property, namespace);

        Policies policies = getNamespacePolicies(namespaceName);
        return policies.auth_policies.namespace_auth;
    }

    @POST
    @Path("/{property}/{namespace}/permissions/{role}")
    @ApiOperation(value = "Grant a new permission to a role on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void grantPermissionOnNamespace(@PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("role") String role, Set<AuthAction> actions) {
        validateNamespaceName(property, namespace);
        internalGrantPermissionOnNamespace(role, actions);
    }

    @DELETE
    @Path("/{property}/{namespace}/permissions/{role}")
    @ApiOperation(value = "Revoke all permissions to a role on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist") })
    public void revokePermissionsOnNamespace(@PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("role") String role) {
        validateNamespaceName(property, namespace);
        internalRevokePermissionsOnNamespace(role);
    }

    @GET
    @Path("/{property}/{namespace}/replication")
    @ApiOperation(value = "Get the replication clusters for a namespace.", response = String.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Namespace is not global") })
    public List<String> getNamespaceReplicationClusters(@PathParam("property") String property,
            @PathParam("namespace") String namespace) {
        validateAdminAccessOnProperty(property);
        validateNamespaceName(property, namespace);

        return internalGetNamespaceReplicationClusters();
    }

    @POST
    @Path("/{property}/{namespace}/replication")
    @ApiOperation(value = "Set the replication clusters for a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Peer-cluster can't be part of replication-cluster"),
            @ApiResponse(code = 412, message = "Namespace is not global or invalid cluster ids") })
    public void setNamespaceReplicationClusters(@PathParam("property") String property,
            @PathParam("namespace") String namespace, List<String> clusterIds) {
        validateNamespaceName(property, namespace);
        internalSetNamespaceReplicationClusters(clusterIds);
    }

    @GET
    @Path("/{property}/{namespace}/messageTTL")
    @ApiOperation(value = "Get the message TTL for the namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist") })
    public int getNamespaceMessageTTL(@PathParam("property") String property,
            @PathParam("namespace") String namespace) {

        validateAdminAccessOnProperty(property);
        validateNamespaceName(property, namespace);

        Policies policies = getNamespacePolicies(namespaceName);
        return policies.message_ttl_in_seconds;
    }

    @POST
    @Path("/{property}/{namespace}/messageTTL")
    @ApiOperation(value = "Set message TTL in seconds for namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Invalid TTL") })
    public void setNamespaceMessageTTL(@PathParam("property") String property, @PathParam("namespace") String namespace,
            int messageTTL) {
        validateNamespaceName(property, namespace);
        internalSetNamespaceMessageTTL(messageTTL);
    }

    @POST
    @Path("/{property}/{namespace}/deduplication")
    @ApiOperation(value = "Enable or disable broker side deduplication for all topics in a namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist") })
    public void modifyDeduplication(@PathParam("property") String property, @PathParam("namespace") String namespace,
            boolean enableDeduplication) {
        validateNamespaceName(property, namespace);
        internalModifyDeduplication(enableDeduplication);
    }

    @GET
    @Path("/{property}/{namespace}/bundles")
    @ApiOperation(value = "Get the bundles split data.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Namespace is not setup to split in bundles") })
    public BundlesData getBundlesData(@PathParam("property") String property,
            @PathParam("namespace") String namespace) {
        validateAdminAccessOnProperty(property);
        validatePoliciesReadOnlyAccess();
        validateNamespaceName(property, namespace);

        Policies policies = getNamespacePolicies(namespaceName);

        return policies.bundles;
    }

    @PUT
    @Path("/{property}/{namespace}/unload")
    @ApiOperation(value = "Unload namespace", notes = "Unload an active namespace from the current broker serving it. Performing this operation will let the broker"
            + "removes all producers, consumers, and connections using this namespace, and close all topics (including"
            + "their persistent store). During that operation, the namespace is marked as tentatively unavailable until the"
            + "broker completes the unloading action. This operation requires strictly super user privileges, since it would"
            + "result in non-persistent message loss and unexpected connection closure to the clients.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Namespace is already unloaded or Namespace has bundles activated") })
    public void unloadNamespace(@PathParam("property") String property, @PathParam("namespace") String namespace) {
        validateNamespaceName(property, namespace);
        internalUnloadNamespace();
    }

    @PUT
    @Path("/{property}/{namespace}/{bundle}/unload")
    @ApiOperation(value = "Unload a namespace bundle")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void unloadNamespaceBundle(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(property, namespace);
        internalUnloadNamespaceBundle(bundleRange, authoritative);
    }

    @PUT
    @Path("/{property}/{namespace}/{bundle}/split")
    @ApiOperation(value = "Split a namespace bundle")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void splitNamespaceBundle(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("unload") @DefaultValue("false") boolean unload) {
        validateNamespaceName(property, namespace);
        internalSplitNamespaceBundle(bundleRange, authoritative, unload);
    }

    @POST
    @Path("/{property}/{namespace}/dispatchRate")
    @ApiOperation(value = "Set dispatch-rate throttling for all topics of the namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void setDispatchRate(@PathParam("property") String property, @PathParam("namespace") String namespace,
            DispatchRate dispatchRate) {
        validateNamespaceName(property, namespace);
        internalSetDispatchRate(dispatchRate);
    }

    @GET
    @Path("/{property}/{namespace}/dispatchRate")
    @ApiOperation(value = "Get dispatch-rate configured for the namespace, -1 represents not configured yet")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public DispatchRate getDispatchRate(@PathParam("property") String property,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, namespace);
        return internalGetDispatchRate();
    }

    @GET
    @Path("/{property}/{namespace}/backlogQuotaMap")
    @ApiOperation(value = "Get backlog quota map on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public Map<BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(@PathParam("property") String property,
            @PathParam("namespace") String namespace) {
        validateAdminAccessOnProperty(property);
        validateNamespaceName(property, namespace);

        Policies policies = getNamespacePolicies(namespaceName);
        return policies.backlog_quota_map;
    }

    @POST
    @Path("/{property}/{namespace}/backlogQuota")
    @ApiOperation(value = " Set a backlog quota for all the topics on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Specified backlog quota exceeds retention quota. Increase retention quota and retry request") })
    public void setBacklogQuota(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @QueryParam("backlogQuotaType") BacklogQuotaType backlogQuotaType, BacklogQuota backlogQuota) {
        validateNamespaceName(property, namespace);
        internalSetBacklogQuota(backlogQuotaType, backlogQuota);
    }

    @DELETE
    @Path("/{property}/{namespace}/backlogQuota")
    @ApiOperation(value = "Remove a backlog quota policy from a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void removeBacklogQuota(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @QueryParam("backlogQuotaType") BacklogQuotaType backlogQuotaType) {
        validateNamespaceName(property, namespace);
        internalRemoveBacklogQuota(backlogQuotaType);
    }

    @GET
    @Path("/{property}/{namespace}/retention")
    @ApiOperation(value = "Get retention config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public RetentionPolicies getRetention(@PathParam("property") String property,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, namespace);
        return internalGetRetention();
    }

    @POST
    @Path("/{property}/{namespace}/retention")
    @ApiOperation(value = " Set retention configuration on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Retention Quota must exceed backlog quota") })
    public void setRetention(@PathParam("property") String property, @PathParam("namespace") String namespace,
            RetentionPolicies retention) {
        validateNamespaceName(property, namespace);
        internalSetRetention(retention);
    }

    @POST
    @Path("/{property}/{namespace}/persistence")
    @ApiOperation(value = "Set the persistence configuration for all the topics on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 400, message = "Invalid persistence policies") })
    public void setPersistence(@PathParam("property") String property, @PathParam("namespace") String namespace,
            PersistencePolicies persistence) {
        validateNamespaceName(property, namespace);
        internalSetPersistence(persistence);
    }

    @GET
    @Path("/{property}/{namespace}/persistence")
    @ApiOperation(value = "Get the persistence configuration for a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public PersistencePolicies getPersistence(@PathParam("property") String property,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, namespace);
        return internalGetPersistence();
    }

    @POST
    @Path("/{property}/{namespace}/clearBacklog")
    @ApiOperation(value = "Clear backlog for all topics on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void clearNamespaceBacklog(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(property, namespace);
        internalClearNamespaceBacklog(authoritative);
    }

    @POST
    @Path("/{property}/{namespace}/{bundle}/clearBacklog")
    @ApiOperation(value = "Clear backlog for all topics on a namespace bundle.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void clearNamespaceBundleBacklog(@PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(property, namespace);
        internalClearNamespaceBundleBacklog(bundleRange, authoritative);
    }

    @POST
    @Path("/{property}/{namespace}/clearBacklog/{subscription}")
    @ApiOperation(value = "Clear backlog for a given subscription on all topics on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void clearNamespaceBacklogForSubscription(@PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("subscription") String subscription,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(property, namespace);
        internalClearNamespaceBacklogForSubscription(subscription, authoritative);
    }

    @POST
    @Path("/{property}/{namespace}/{bundle}/clearBacklog/{subscription}")
    @ApiOperation(value = "Clear backlog for a given subscription on all topics on a namespace bundle.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void clearNamespaceBundleBacklogForSubscription(@PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("subscription") String subscription,
            @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(property, namespace);
        internalClearNamespaceBundleBacklogForSubscription(subscription, bundleRange, authoritative);
    }

    @POST
    @Path("/{property}/{namespace}/unsubscribe/{subscription}")
    @ApiOperation(value = "Unsubscribes the given subscription on all topics on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void unsubscribeNamespace(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("subscription") String subscription,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(property, namespace);
        internalUnsubscribeNamespace(subscription, authoritative);
    }

    @POST
    @Path("/{property}/{namespace}/{bundle}/unsubscribe/{subscription}")
    @ApiOperation(value = "Unsubscribes the given subscription on all topics on a namespace bundle.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void unsubscribeNamespaceBundle(@PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("subscription") String subscription,
            @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(property, namespace);
        internalUnsubscribeNamespaceBundle(subscription, bundleRange, authoritative);
    }

    @POST
    @Path("/{property}/{namespace}/subscriptionAuthMode")
    @ApiOperation(value = " Set a subscription auth mode for all the topics on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void setSubscriptionAuthMode(@PathParam("property") String property,
            @PathParam("namespace") String namespace, SubscriptionAuthMode subscriptionAuthMode) {
        validateNamespaceName(property, namespace);
        internalSetSubscriptionAuthMode(subscriptionAuthMode);
    }

    @POST
    @Path("/{property}/{namespace}/encryptionRequired")
    @ApiOperation(value = "Message encryption is required or not for all topics in a namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"), })
    public void modifyEncryptionRequired(@PathParam("property") String property,
            @PathParam("namespace") String namespace, boolean encryptionRequired) {
        validateNamespaceName(property, namespace);
        internalModifyEncryptionRequired(encryptionRequired);
    }

    @GET
    @Path("/{property}/{namespace}/maxProducersPerTopic")
    @ApiOperation(value = "Get maxProducersPerTopic config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public int getMaxProducersPerTopic(@PathParam("property") String property,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, namespace);
        return internalGetMaxProducersPerTopic();
    }

    @POST
    @Path("/{property}/{namespace}/maxProducersPerTopic")
    @ApiOperation(value = " Set maxProducersPerTopic configuration on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "maxProducersPerTopic value is not valid") })
    public void setMaxProducersPerTopic(@PathParam("property") String property, @PathParam("namespace") String namespace,
            int maxProducersPerTopic) {
        validateNamespaceName(property, namespace);
        internalSetMaxProducersPerTopic(maxProducersPerTopic);
    }

    @GET
    @Path("/{property}/{namespace}/maxConsumersPerTopic")
    @ApiOperation(value = "Get maxConsumersPerTopic config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public int getMaxConsumersPerTopic(@PathParam("property") String property,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, namespace);
        return internalGetMaxConsumersPerTopic();
    }

    @POST
    @Path("/{property}/{namespace}/maxConsumersPerTopic")
    @ApiOperation(value = " Set maxConsumersPerTopic configuration on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "maxConsumersPerTopic value is not valid") })
    public void setMaxConsumersPerTopic(@PathParam("property") String property, @PathParam("namespace") String namespace,
            int maxConsumersPerTopic) {
        validateNamespaceName(property, namespace);
        internalSetMaxConsumersPerTopic(maxConsumersPerTopic);
    }

    @GET
    @Path("/{property}/{namespace}/maxConsumersPerSubscription")
    @ApiOperation(value = "Get maxConsumersPerSubscription config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public int getMaxConsumersPerSubscription(@PathParam("property") String property,
            @PathParam("namespace") String namespace) {
        validateNamespaceName(property, namespace);
        return internalGetMaxConsumersPerSubscription();
    }

    @POST
    @Path("/{property}/{namespace}/maxConsumersPerSubscription")
    @ApiOperation(value = " Set maxConsumersPerSubscription configuration on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "maxConsumersPerSubscription value is not valid") })
    public void setMaxConsumersPerSubscription(@PathParam("property") String property, @PathParam("namespace") String namespace,
            int maxConsumersPerSubscription) {
        validateNamespaceName(property, namespace);
        internalSetMaxConsumersPerSubscription(maxConsumersPerSubscription);
    }

    private Policies getDefaultPolicesIfNull(Policies policies) {
        if (policies != null) {
            return policies;
        }

        Policies defaultPolicies = new Policies();
        int defaultNumberOfBundles = config().getDefaultNumberOfNamespaceBundles();
        defaultPolicies.bundles = getBundles(defaultNumberOfBundles);
        return defaultPolicies;
    }

    private static final Logger log = LoggerFactory.getLogger(Namespaces.class);
}
