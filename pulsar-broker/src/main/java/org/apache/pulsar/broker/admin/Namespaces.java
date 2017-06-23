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
import static org.apache.pulsar.broker.cache.LocalZooKeeperCacheService.LOCAL_POLICIES_ROOT;

import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;

import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionBusyException;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.util.FutureUtil;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.naming.NamedEntity;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Path("/namespaces")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(value = "/namespaces", description = "Namespaces admin apis", tags = "namespaces")
public class Namespaces extends AdminResource {

    public static final String GLOBAL_CLUSTER = "global";
    private static final long MAX_BUNDLES = ((long) 1) << 32;

    @GET
    @Path("/{property}")
    @ApiOperation(value = "Get the list of all the namespaces for a certain property.", response = String.class, responseContainer = "Set")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property doesn't exist") })
    public List<String> getPropertyNamespaces(@PathParam("property") String property) {
        validateAdminAccessOnProperty(property);

        try {
            return getListOfNamespaces(property);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to get namespace list for propery: {} - Does not exist", clientAppId(), property);
            throw new RestException(Status.NOT_FOUND, "Property does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to get namespaces list: {}", clientAppId(), e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{property}/{cluster}")
    @ApiOperation(value = "Get the list of all the namespaces for a certain property on single cluster.", response = String.class, responseContainer = "Set")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster doesn't exist") })
    public List<String> getNamespacesForCluster(@PathParam("property") String property,
            @PathParam("cluster") String cluster) {
        validateAdminAccessOnProperty(property);
        List<String> namespaces = Lists.newArrayList();
        if (!clusters().contains(cluster)) {
            log.warn("[{}] Failed to get namespace list for property: {}/{} - Cluster does not exist", clientAppId(),
                    property, cluster);
            throw new RestException(Status.NOT_FOUND, "Cluster does not exist");
        }

        try {
            for (String namespace : globalZk().getChildren(path("policies", property, cluster), false)) {
                namespaces.add(String.format("%s/%s/%s", property, cluster, namespace));
            }
        } catch (KeeperException.NoNodeException e) {
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
    @ApiOperation(value = "Get the list of all the destinations under a certain namespace.", response = String.class, responseContainer = "Set")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist") })
    public List<String> getDestinations(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace) {
        validateAdminAccessOnProperty(property);

        // Validate that namespace exists, throws 404 if it doesn't exist
        getNamespacePolicies(property, cluster, namespace);

        try {
            return pulsar().getNamespaceService().getListOfDestinations(property, cluster, namespace);
        } catch (Exception e) {
            log.error("Failed to get topics list for namespace {}/{}/{}", property, cluster, namespace, e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}")
    @ApiOperation(value = "Get the dump all the policies specified for a namespace.", response = Policies.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist") })
    public Policies getPolicies(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace) {
        validateAdminAccessOnProperty(property);

        return getNamespacePolicies(property, cluster, namespace);
    }

    @PUT
    @Path("/{property}/{cluster}/{namespace}")
    @ApiOperation(value = "Creates a new empty namespace with no policies attached.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Namespace already exists"),
            @ApiResponse(code = 412, message = "Namespace name is not valid") })
    public void createNamespace(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, BundlesData initialBundles) {
        validateAdminAccessOnProperty(property);
        validatePoliciesReadOnlyAccess();
        // If the namespace is non global, make sure property has the access on the cluster. For global namespace, same
        // check is made at the time of setting replication.
        if (!cluster.equals(GLOBAL_CLUSTER)) {
            validateClusterForProperty(property, cluster);
        }
        if (!clusters().contains(cluster)) {
            log.warn("[{}] Failed to create namespace. Cluster {} does not exist", clientAppId(), cluster);
            throw new RestException(Status.NOT_FOUND, "Cluster does not exist");
        }
        try {
            checkNotNull(propertiesCache().get(path("policies", property)));
        } catch (NoNodeException nne) {
            log.warn("[{}] Failed to create namespace. Property {} does not exist", clientAppId(), property);
            throw new RestException(Status.NOT_FOUND, "Property does not exist");
        } catch (RestException e) {
            throw e;
        } catch (Exception e) {
            throw new RestException(e);
        }
        try {
            NamedEntity.checkName(namespace);
            policiesCache().invalidate(path("policies", property, cluster, namespace));
            Policies policies = new Policies();
            if (initialBundles != null && initialBundles.getNumBundles() > 0) {
                if (initialBundles.getBoundaries() == null || initialBundles.getBoundaries().size() == 0) {
                    policies.bundles = getBundles(initialBundles.getNumBundles());
                } else {
                    policies.bundles = validateBundlesData(initialBundles);
                }
            }

            zkCreateOptimistic(path("policies", property, cluster, namespace),
                    jsonMapper().writeValueAsBytes(policies));
            log.info("[{}] Created namespace {}/{}/{}", clientAppId(), property, cluster, namespace);
        } catch (KeeperException.NodeExistsException e) {
            log.warn("[{}] Failed to create namespace {}/{}/{} - already exists", clientAppId(), property, cluster,
                    namespace);
            throw new RestException(Status.CONFLICT, "Namespace already exists");
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Failed to create namespace with invalid name {}", clientAppId(), property, e);
            throw new RestException(Status.PRECONDITION_FAILED, "Namespace name is not valid");
        } catch (Exception e) {
            log.error("[{}] Failed to create namespace {}/{}/{}", clientAppId(), property, cluster, namespace, e);
            throw new RestException(e);
        }
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}")
    @ApiOperation(value = "Delete a namespace and all the destinations under it.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Namespace is not empty") })
    public void deleteNamespace(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {

        NamespaceName nsName = new NamespaceName(property, cluster, namespace);

        validateAdminAccessOnProperty(property);
        validatePoliciesReadOnlyAccess();

        // ensure that non-global namespace is directed to the correct cluster
        validateClusterOwnership(cluster);

        Entry<Policies, Stat> policiesNode = null;
        Policies policies = null;

        // ensure the local cluster is the only cluster for the global namespace configuration
        try {
            policiesNode = policiesCache().getWithStat(path("policies", property, cluster, namespace))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Namespace " + nsName + " does not exist."));

            policies = policiesNode.getKey();
            if (cluster.equals(Namespaces.GLOBAL_CLUSTER)) {
                if (policies.replication_clusters.size() > 1) {
                    // There are still more than one clusters configured for the global namespace
                    throw new RestException(Status.PRECONDITION_FAILED, "Cannot delete the global namespace " + nsName
                            + ". There are still more than one replication clusters configured.");
                }
                if (policies.replication_clusters.size() == 1
                        && !policies.replication_clusters.contains(config().getClusterName())) {
                    // the only replication cluster is other cluster, redirect
                    String replCluster = policies.replication_clusters.get(0);
                    ClusterData replClusterData = clustersCache().get(AdminResource.path("clusters", replCluster))
                            .orElseThrow(() -> new RestException(Status.NOT_FOUND,
                                    "Cluser " + replCluster + " does not exist"));
                    URL replClusterUrl;
                    if (!config().isTlsEnabled()) {
                        replClusterUrl = new URL(replClusterData.getServiceUrl());
                    } else if (!replClusterData.getServiceUrlTls().isEmpty()) {
                        replClusterUrl = new URL(replClusterData.getServiceUrlTls());
                    } else {
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "The replication cluster does not provide TLS encrypted service");
                    }
                    URI redirect = UriBuilder.fromUri(uri.getRequestUri()).host(replClusterUrl.getHost())
                            .port(replClusterUrl.getPort()).replaceQueryParam("authoritative", false).build();
                    log.debug("[{}] Redirecting the rest call to {}: cluster={}", clientAppId(), redirect, cluster);
                    throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                }
            }
        } catch (WebApplicationException wae) {
            throw wae;
        } catch (Exception e) {
            throw new RestException(e);
        }

        List<String> destinations = getDestinations(property, cluster, namespace);
        if (!destinations.isEmpty()) {
            log.info("Found destinations: {}", destinations);
            throw new RestException(Status.CONFLICT, "Cannot delete non empty namespace");
        }

        // set the policies to deleted so that somebody else cannot acquire this namespace
        try {
            policies.deleted = true;
            globalZk().setData(path("policies", property, cluster, namespace), jsonMapper().writeValueAsBytes(policies),
                    policiesNode.getValue().getVersion());
            policiesCache().invalidate(path("policies", property, cluster, namespace));
        } catch (Exception e) {
            log.error("[{}] Failed to delete namespace on global ZK {}/{}/{}", clientAppId(), property, cluster,
                    namespace, e);
            throw new RestException(e);
        }

        // remove from owned namespace map and ephemeral node from ZK
        try {
            NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory().getBundles(nsName);
            for (NamespaceBundle bundle : bundles.getBundles()) {
                // check if the bundle is owned by any broker, if not then we do not need to delete the bundle
                if (pulsar().getNamespaceService().getOwner(bundle).isPresent()) {
                    pulsar().getAdminClient().namespaces().deleteNamespaceBundle(nsName.toString(),
                            bundle.getBundleRange());
                }
            }

            // we have successfully removed all the ownership for the namespace, the policies znode can be deleted now
            final String globalZkPolicyPath = path("policies", property, cluster, namespace);
            final String lcaolZkPolicyPath = joinPath(LOCAL_POLICIES_ROOT, property, cluster, namespace);
            globalZk().delete(globalZkPolicyPath, -1);
            localZk().delete(lcaolZkPolicyPath, -1);
            policiesCache().invalidate(globalZkPolicyPath);
            localCacheService().policiesCache().invalidate(lcaolZkPolicyPath);
        } catch (PulsarAdminException cae) {
            throw new RestException(cae);
        } catch (Exception e) {
            log.error(String.format("[%s] Failed to remove owned namespace %s/%s/%s", clientAppId(), property, cluster,
                    namespace), e);
            // avoid throwing exception in case of the second failure
        }

    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/{bundle}")
    @ApiOperation(value = "Delete a namespace bundle and all the destinations under it.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Namespace bundle is not empty") })
    public void deleteNamespaceBundle(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        NamespaceName nsName = new NamespaceName(property, cluster, namespace);

        validateAdminAccessOnProperty(property);
        validatePoliciesReadOnlyAccess();

        // ensure that non-global namespace is directed to the correct cluster
        validateClusterOwnership(cluster);
        Policies policies = getNamespacePolicies(property, cluster, namespace);
        // ensure the local cluster is the only cluster for the global namespace configuration
        try {
            if (cluster.equals(Namespaces.GLOBAL_CLUSTER)) {
                if (policies.replication_clusters.size() > 1) {
                    // There are still more than one clusters configured for the global namespace
                    throw new RestException(Status.PRECONDITION_FAILED, "Cannot delete the global namespace " + nsName
                            + ". There are still more than one replication clusters configured.");
                }
                if (policies.replication_clusters.size() == 1
                        && !policies.replication_clusters.contains(config().getClusterName())) {
                    // the only replication cluster is other cluster, redirect
                    String replCluster = policies.replication_clusters.get(0);
                    ClusterData replClusterData = clustersCache().get(AdminResource.path("clusters", replCluster))
                            .orElseThrow(() -> new RestException(Status.NOT_FOUND,
                                    "Cluser " + replCluster + " does not exist"));
                    URL replClusterUrl;
                    if (!config().isTlsEnabled()) {
                        replClusterUrl = new URL(replClusterData.getServiceUrl());
                    } else if (!replClusterData.getServiceUrlTls().isEmpty()) {
                        replClusterUrl = new URL(replClusterData.getServiceUrlTls());
                    } else {
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "The replication cluster does not provide TLS encrypted service");
                    }
                    URI redirect = UriBuilder.fromUri(uri.getRequestUri()).host(replClusterUrl.getHost())
                            .port(replClusterUrl.getPort()).replaceQueryParam("authoritative", false).build();
                    log.debug("[{}] Redirecting the rest call to {}: cluster={}", clientAppId(), redirect, cluster);
                    throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                }
            }
        } catch (WebApplicationException wae) {
            throw wae;
        } catch (Exception e) {
            throw new RestException(e);
        }

        NamespaceBundle bundle = validateNamespaceBundleOwnership(nsName, policies.bundles, bundleRange, authoritative,
                true);
        try {
            List<String> destinations = getDestinations(property, cluster, namespace);
            for (String destination : destinations) {
                NamespaceBundle destinationBundle = (NamespaceBundle) pulsar().getNamespaceService()
                        .getBundle(DestinationName.get(destination));
                if (bundle.equals(destinationBundle)) {
                    throw new RestException(Status.CONFLICT, "Cannot delete non empty bundle");
                }
            }

            // remove from owned namespace map and ephemeral node from ZK
            pulsar().getNamespaceService().removeOwnedServiceUnit(bundle);
        } catch (WebApplicationException wae) {
            throw wae;
        } catch (Exception e) {
            log.error("[{}] Failed to remove namespace bundle {}/{}", clientAppId(), nsName.toString(), bundleRange, e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/permissions")
    @ApiOperation(value = "Retrieve the permissions for a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Namespace is not empty") })
    public Map<String, Set<AuthAction>> getPermissions(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace) {
        validateAdminAccessOnProperty(property);

        Policies policies = getNamespacePolicies(property, cluster, namespace);
        return policies.auth_policies.namespace_auth;
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/permissions/{role}")
    @ApiOperation(value = "Grant a new permission to a role on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void grantPermissionOnNamespace(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("role") String role, Set<AuthAction> actions) {
        validateAdminAccessOnProperty(property);
        validatePoliciesReadOnlyAccess();

        try {
            Stat nodeStat = new Stat();
            byte[] content = globalZk().getData(path("policies", property, cluster, namespace), null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);
            policies.auth_policies.namespace_auth.put(role, actions);

            // Write back the new policies into zookeeper
            globalZk().setData(path("policies", property, cluster, namespace), jsonMapper().writeValueAsBytes(policies),
                    nodeStat.getVersion());

            policiesCache().invalidate(path("policies", property, cluster, namespace));

            log.info("[{}] Successfully granted access for role {}: {} - namespace {}/{}/{}", clientAppId(), role,
                    actions, property, cluster, namespace);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to set permissions for namespace {}/{}/{}: does not exist", clientAppId(), property,
                    cluster, namespace);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to set permissions for namespace {}/{}/{}: concurrent modification", clientAppId(),
                    property, cluster, namespace);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to get permissions for namespace {}/{}/{}", clientAppId(), property, cluster,
                    namespace, e);
            throw new RestException(e);
        }
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/permissions/{role}")
    @ApiOperation(value = "Revoke all permissions to a role on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist") })
    public void revokePermissionsOnNamespace(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("role") String role) {
        validateAdminAccessOnProperty(property);
        validatePoliciesReadOnlyAccess();

        try {
            Stat nodeStat = new Stat();
            byte[] content = globalZk().getData(path("policies", property, cluster, namespace), null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);
            policies.auth_policies.namespace_auth.remove(role);

            // Write back the new policies into zookeeper
            globalZk().setData(path("policies", property, cluster, namespace), jsonMapper().writeValueAsBytes(policies),
                    nodeStat.getVersion());

            policiesCache().invalidate(path("policies", property, cluster, namespace));
            log.info("[{}] Successfully revoked access for role {} - namespace {}/{}/{}", clientAppId(), role, property,
                    cluster, namespace);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to revoke permissions for namespace {}/{}/{}: does not exist", clientAppId(),
                    property, cluster, namespace);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to revoke permissions on namespace {}/{}/{}: concurrent modification", clientAppId(),
                    property, cluster, namespace);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to revoke permissions on namespace {}/{}/{}", clientAppId(), property, cluster,
                    namespace, e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/replication")
    @ApiOperation(value = "Get the replication clusters for a namespace.", response = String.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Namespace is not global") })
    public List<String> getNamespaceReplicationClusters(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace) {
        validateAdminAccessOnProperty(property);

        if (!cluster.equals("global")) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "Cannot get the replication clusters for a non-global namespace");
        }

        Policies policies = getNamespacePolicies(property, cluster, namespace);
        return policies.replication_clusters;
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/replication")
    @ApiOperation(value = "Set the replication clusters for a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Namespace is not global or invalid cluster ids") })
    public void setNamespaceReplicationClusters(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace, List<String> clusterIds) {
        validateAdminAccessOnProperty(property);
        validatePoliciesReadOnlyAccess();

        if (!cluster.equals("global")) {
            throw new RestException(Status.PRECONDITION_FAILED, "Cannot set replication on a non-global namespace");
        }

        if (clusterIds.contains("global")) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "Cannot specify global in the list of replication clusters");
        }

        Set<String> clusters = clusters();
        for (String clusterId : clusterIds) {
            if (!clusters.contains(clusterId)) {
                throw new RestException(Status.FORBIDDEN, "Invalid cluster id: " + clusterId);
            }
        }

        for (String clusterId : clusterIds) {
            validateClusterForProperty(property, clusterId);
        }

        Entry<Policies, Stat> policiesNode = null;
        NamespaceName nsName = new NamespaceName(property, cluster, namespace);

        try {
            // Force to read the data s.t. the watch to the cache content is setup.
            policiesNode = policiesCache().getWithStat(path("policies", property, cluster, namespace))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Namespace " + nsName + " does not exist"));
            policiesNode.getKey().replication_clusters = clusterIds;

            // Write back the new policies into zookeeper
            globalZk().setData(path("policies", property, cluster, namespace),
                    jsonMapper().writeValueAsBytes(policiesNode.getKey()), policiesNode.getValue().getVersion());
            policiesCache().invalidate(path("policies", property, cluster, namespace));

            log.info("[{}] Successfully updated the replication clusters on namespace {}/{}/{}", clientAppId(),
                    property, cluster, namespace);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update the replication clusters for namespace {}/{}/{}: does not exist",
                    clientAppId(), property, cluster, namespace);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn(
                    "[{}] Failed to update the replication clusters on namespace {}/{}/{} expected policy node version={} : concurrent modification",
                    clientAppId(), property, cluster, namespace, policiesNode.getValue().getVersion());

            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to update the replication clusters on namespace {}/{}/{}", clientAppId(), property,
                    cluster, namespace, e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/messageTTL")
    @ApiOperation(value = "Get the message TTL for the namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist") })
    public int getNamespaceMessageTTL(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace) {

        validateAdminAccessOnProperty(property);

        Policies policies = getNamespacePolicies(property, cluster, namespace);
        return policies.message_ttl_in_seconds;
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/messageTTL")
    @ApiOperation(value = "Set message TTL in seconds for namespace")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Invalid TTL") })
    public void setNamespaceMessageTTL(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, int messageTTL) {
        validateAdminAccessOnProperty(property);
        validatePoliciesReadOnlyAccess();

        if (messageTTL < 0) {
            throw new RestException(Status.PRECONDITION_FAILED, "Invalid value for message TTL");
        }

        NamespaceName nsName = new NamespaceName(property, cluster, namespace);
        Entry<Policies, Stat> policiesNode = null;

        try {
            // Force to read the data s.t. the watch to the cache content is setup.
            policiesNode = policiesCache().getWithStat(path("policies", property, cluster, namespace))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Namespace " + nsName + " does not exist"));
            policiesNode.getKey().message_ttl_in_seconds = messageTTL;

            // Write back the new policies into zookeeper
            globalZk().setData(path("policies", property, cluster, namespace),
                    jsonMapper().writeValueAsBytes(policiesNode.getKey()), policiesNode.getValue().getVersion());
            policiesCache().invalidate(path("policies", property, cluster, namespace));

            log.info("[{}] Successfully updated the message TTL on namespace {}/{}/{}", clientAppId(), property,
                    cluster, namespace);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update the message TTL for namespace {}/{}/{}: does not exist", clientAppId(),
                    property, cluster, namespace);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn(
                    "[{}] Failed to update the message TTL on namespace {}/{}/{} expected policy node version={} : concurrent modification",
                    clientAppId(), property, cluster, namespace, policiesNode.getValue().getVersion());

            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to update the message TTL on namespace {}/{}/{}", clientAppId(), property, cluster,
                    namespace, e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/bundles")
    @ApiOperation(value = "Get the bundles split data.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Namespace is not setup to split in bundles") })
    public BundlesData getBundlesData(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace) {
        validateAdminAccessOnProperty(property);
        validatePoliciesReadOnlyAccess();

        Policies policies = getNamespacePolicies(property, cluster, namespace);

        return policies.bundles;
    }

    private BundlesData validateBundlesData(BundlesData initialBundles) {
        SortedSet<String> partitions = new TreeSet<String>();
        for (String partition : initialBundles.getBoundaries()) {
            Long partBoundary = Long.decode(partition);
            partitions.add(String.format("0x%08x", partBoundary));
        }
        if (partitions.size() != initialBundles.getBoundaries().size()) {
            log.debug("Input bundles included repeated partition points. Ignored.");
        }
        try {
            NamespaceBundleFactory.validateFullRange(partitions);
        } catch (IllegalArgumentException iae) {
            throw new RestException(Status.BAD_REQUEST, "Input bundles do not cover the whole hash range. first:"
                    + partitions.first() + ", last:" + partitions.last());
        }
        List<String> bundles = Lists.newArrayList();
        bundles.addAll(partitions);
        return new BundlesData(bundles);
    }

    private BundlesData getBundles(int numBundles) {
        if (numBundles <= 0 || numBundles > MAX_BUNDLES) {
            throw new RestException(Status.BAD_REQUEST,
                    "Invalid number of bundles. Number of numbles has to be in the range of (0, 2^32].");
        }
        Long maxVal = ((long) 1) << 32;
        Long segSize = maxVal / numBundles;
        List<String> partitions = Lists.newArrayList();
        partitions.add(String.format("0x%08x", 0l));
        Long curPartition = segSize;
        for (int i = 0; i < numBundles; i++) {
            if (i != numBundles - 1) {
                partitions.add(String.format("0x%08x", curPartition));
            } else {
                partitions.add(String.format("0x%08x", maxVal - 1));
            }
            curPartition += segSize;
        }
        return new BundlesData(partitions);
    }

    @PUT
    @Path("/{property}/{cluster}/{namespace}/unload")
    @ApiOperation(value = "Unload namespace", notes = "Unload an active namespace from the current broker serving it. Performing this operation will let the broker"
            + "removes all producers, consumers, and connections using this namespace, and close all destinations (including"
            + "their persistent store). During that operation, the namespace is marked as tentatively unavailable until the"
            + "broker completes the unloading action. This operation requires strictly super user privileges, since it would"
            + "result in non-persistent message loss and unexpected connection closure to the clients.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property or cluster or namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Namespace is already unloaded or Namespace has bundles activated") })
    public void unloadNamespace(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace) {
        log.info("[{}] Unloading namespace {}/{}/{}", clientAppId(), property, cluster, namespace);

        validateSuperUserAccess();

        if (!cluster.equals(Namespaces.GLOBAL_CLUSTER)) {
            validateClusterOwnership(cluster);
            validateClusterForProperty(property, cluster);
        }

        Policies policies = getNamespacePolicies(property, cluster, namespace);
        NamespaceName nsName = new NamespaceName(property, cluster, namespace);

        List<String> boundaries = policies.bundles.getBoundaries();
        for (int i = 0; i < boundaries.size() - 1; i++) {
            String bundle = String.format("%s_%s", boundaries.get(i), boundaries.get(i + 1));
            try {
                pulsar().getAdminClient().namespaces().unloadNamespaceBundle(nsName.toString(), bundle);
            } catch (PulsarServerException | PulsarAdminException e) {
                log.error(String.format("[%s] Failed to unload namespace %s/%s/%s", clientAppId(), property, cluster,
                        namespace), e);
                throw new RestException(e);
            }
        }
        log.info("[{}] Successfully unloaded all the bundles in namespace {}/{}/{}", clientAppId(), property, cluster,
                namespace);
    }

    @PUT
    @Path("/{property}/{cluster}/{namespace}/{bundle}/unload")
    @ApiOperation(value = "Unload a namespace bundle")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void unloadNamespaceBundle(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        log.info("[{}] Unloading namespace bundle {}/{}/{}/{}", clientAppId(), property, cluster, namespace,
                bundleRange);

        validateSuperUserAccess();
        Policies policies = getNamespacePolicies(property, cluster, namespace);

        if (!cluster.equals(Namespaces.GLOBAL_CLUSTER)) {
            validateClusterOwnership(cluster);
            validateClusterForProperty(property, cluster);
        }

        NamespaceName fqnn = new NamespaceName(property, cluster, namespace);
        validatePoliciesReadOnlyAccess();
        NamespaceBundle nsBundle = validateNamespaceBundleOwnership(fqnn, policies.bundles, bundleRange, authoritative,
                true);

        try {
            pulsar().getNamespaceService().unloadNamespaceBundle(nsBundle);
            log.info("[{}] Successfully unloaded namespace bundle {}", clientAppId(), nsBundle.toString());
        } catch (Exception e) {
            log.error("[{}] Failed to unload namespace bundle {}/{}", clientAppId(), fqnn.toString(), bundleRange, e);
            throw new RestException(e);
        }
    }

    @PUT
    @Path("/{property}/{cluster}/{namespace}/{bundle}/split")
    @ApiOperation(value = "Split a namespace bundle")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void splitNamespaceBundle(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        log.info("[{}] Split namespace bundle {}/{}/{}/{}", clientAppId(), property, cluster, namespace, bundleRange);

        validateSuperUserAccess();
        Policies policies = getNamespacePolicies(property, cluster, namespace);

        if (!cluster.equals(Namespaces.GLOBAL_CLUSTER)) {
            validateClusterOwnership(cluster);
            validateClusterForProperty(property, cluster);
        }

        NamespaceName fqnn = new NamespaceName(property, cluster, namespace);
        validatePoliciesReadOnlyAccess();
        NamespaceBundle nsBundle = validateNamespaceBundleOwnership(fqnn, policies.bundles, bundleRange, authoritative,
                true);

        try {
            pulsar().getNamespaceService().splitAndOwnBundle(nsBundle).get();
            log.info("[{}] Successfully split namespace bundle {}", clientAppId(), nsBundle.toString());
        } catch (Exception e) {
            log.error("[{}] Failed to split namespace bundle {}/{}", clientAppId(), fqnn.toString(), bundleRange, e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/backlogQuotaMap")
    @ApiOperation(value = "Get backlog quota map on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public Map<BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace) {
        validateAdminAccessOnProperty(property);

        Policies policies = getNamespacePolicies(property, cluster, namespace);
        return policies.backlog_quota_map;
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/backlogQuota")
    @ApiOperation(value = " Set a backlog quota for all the destinations on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Specified backlog quota exceeds retention quota. Increase retention quota and retry request") })
    public void setBacklogQuota(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @QueryParam("backlogQuotaType") BacklogQuotaType backlogQuotaType,
            BacklogQuota backlogQuota) {
        validateAdminAccessOnProperty(property);
        validatePoliciesReadOnlyAccess();

        if (backlogQuotaType == null) {
            backlogQuotaType = BacklogQuotaType.destination_storage;
        }

        try {
            Stat nodeStat = new Stat();
            final String path = path("policies", property, cluster, namespace);
            byte[] content = globalZk().getData(path, null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);
            RetentionPolicies r = policies.retention_policies;
            if (r != null) {
                Policies p = new Policies();
                p.backlog_quota_map.put(backlogQuotaType, backlogQuota);
                if (!checkQuotas(p, r)) {
                    log.warn(
                            "[{}] Failed to update backlog configuration for namespace {}/{}/{}: conflicts with retention quota",
                            clientAppId(), property, cluster, namespace);
                    throw new RestException(Status.PRECONDITION_FAILED,
                            "Backlog Quota exceeds configured retention quota for namespace. Please increase retention quota and retry");
                }
            }
            policies.backlog_quota_map.put(backlogQuotaType, backlogQuota);
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());
            policiesCache().invalidate(path("policies", property, cluster, namespace));
            log.info("[{}] Successfully updated backlog quota map: namespace={}/{}/{}, map={}", clientAppId(), property,
                    cluster, namespace, jsonMapper().writeValueAsString(policies.backlog_quota_map));

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update backlog quota map for namespace {}/{}/{}: does not exist", clientAppId(),
                    property, cluster, namespace);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to update backlog quota map for namespace {}/{}/{}: concurrent modification",
                    clientAppId(), property, cluster, namespace);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update backlog quota map for namespace {}/{}/{}", clientAppId(), property,
                    cluster, namespace, e);
            throw new RestException(e);
        }
    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/backlogQuota")
    @ApiOperation(value = "Remove a backlog quota policy from a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void removeBacklogQuota(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace,
            @QueryParam("backlogQuotaType") BacklogQuotaType backlogQuotaType) {
        validateAdminAccessOnProperty(property);
        validatePoliciesReadOnlyAccess();

        if (backlogQuotaType == null) {
            backlogQuotaType = BacklogQuotaType.destination_storage;
        }

        try {
            Stat nodeStat = new Stat();
            final String path = path("policies", property, cluster, namespace);
            byte[] content = globalZk().getData(path, null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);
            policies.backlog_quota_map.remove(backlogQuotaType);
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());
            policiesCache().invalidate(path("policies", property, cluster, namespace));
            log.info("[{}] Successfully removed backlog namespace={}/{}/{}, quota={}", clientAppId(), property, cluster,
                    namespace, backlogQuotaType);

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update backlog quota map for namespace {}/{}/{}: does not exist", clientAppId(),
                    property, cluster, namespace);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to update backlog quota map for namespace {}/{}/{}: concurrent modification",
                    clientAppId(), property, cluster, namespace);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to update backlog quota map for namespace {}/{}/{}", clientAppId(), property,
                    cluster, namespace, e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/retention")
    @ApiOperation(value = "Get retention config on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public RetentionPolicies getRetention(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace) {

        validateAdminAccessOnProperty(property);

        Policies policies = getNamespacePolicies(property, cluster, namespace);
        if (policies.retention_policies == null) {
            return new RetentionPolicies(config().getDefaultRetentionTimeInMinutes(),
                    config().getDefaultRetentionSizeInMB());
        } else {
            return policies.retention_policies;
        }
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/retention")
    @ApiOperation(value = " Set retention configuration on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Retention Quota must exceed backlog quota") })
    public void setRetention(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, RetentionPolicies retention) {
        validatePoliciesReadOnlyAccess();

        try {
            Stat nodeStat = new Stat();
            final String path = path("policies", property, cluster, namespace);
            byte[] content = globalZk().getData(path, null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);
            if (!checkQuotas(policies, retention)) {
                log.warn(
                        "[{}] Failed to update retention configuration for namespace {}/{}/{}: conflicts with backlog quota",
                        clientAppId(), property, cluster, namespace);
                throw new RestException(Status.PRECONDITION_FAILED,
                        "Retention Quota must exceed configured backlog quota for namespace.");
            }
            policies.retention_policies = retention;
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());
            policiesCache().invalidate(path("policies", property, cluster, namespace));
            log.info("[{}] Successfully updated retention configuration: namespace={}/{}/{}, map={}", clientAppId(),
                    property, cluster, namespace, jsonMapper().writeValueAsString(policies.retention_policies));

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update retention configuration for namespace {}/{}/{}: does not exist",
                    clientAppId(), property, cluster, namespace);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to update retention configuration for namespace {}/{}/{}: concurrent modification",
                    clientAppId(), property, cluster, namespace);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update retention configuration for namespace {}/{}/{}", clientAppId(), property,
                    cluster, namespace, e);
            throw new RestException(e);
        }

    }

    private boolean checkQuotas(Policies policies, RetentionPolicies retention) {
        Map<BacklogQuota.BacklogQuotaType, BacklogQuota> backlog_quota_map = policies.backlog_quota_map;
        if (backlog_quota_map.isEmpty() || retention.getRetentionSizeInMB() == 0) {
            return true;
        }
        BacklogQuota quota = backlog_quota_map.get(BacklogQuotaType.destination_storage);
        if (quota == null) {
            quota = pulsar().getBrokerService().getBacklogQuotaManager().getDefaultQuota();
        }
        if (quota.getLimit() >= ((long) retention.getRetentionSizeInMB() * 1024 * 1024)) {
            return false;
        }
        return true;
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/persistence")
    @ApiOperation(value = "Set the persistence configuration for all the destinations on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void setPersistence(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, PersistencePolicies persistence) {
        validatePoliciesReadOnlyAccess();

        try {
            Stat nodeStat = new Stat();
            final String path = path("policies", property, cluster, namespace);
            byte[] content = globalZk().getData(path, null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);
            policies.persistence = persistence;
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());
            policiesCache().invalidate(path("policies", property, cluster, namespace));
            log.info("[{}] Successfully updated persistence configuration: namespace={}/{}/{}, map={}", clientAppId(),
                    property, cluster, namespace, jsonMapper().writeValueAsString(policies.persistence));

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update persistence configuration for namespace {}/{}/{}: does not exist",
                    clientAppId(), property, cluster, namespace);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to update persistence configuration for namespace {}/{}/{}: concurrent modification",
                    clientAppId(), property, cluster, namespace);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to update persistence configuration for namespace {}/{}/{}", clientAppId(), property,
                    cluster, namespace, e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/persistence")
    @ApiOperation(value = "Get the persistence configuration for a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public PersistencePolicies getPersistence(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace) {
        validateAdminAccessOnProperty(property);

        Policies policies = getNamespacePolicies(property, cluster, namespace);
        if (policies.persistence == null) {
            return new PersistencePolicies(config().getManagedLedgerDefaultEnsembleSize(),
                    config().getManagedLedgerDefaultWriteQuorum(), config().getManagedLedgerDefaultAckQuorum(), 0.0d);
        } else {
            return policies.persistence;
        }
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/clearBacklog")
    @ApiOperation(value = "Clear backlog for all destinations on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void clearNamespaceBacklog(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateAdminAccessOnProperty(property);

        NamespaceName nsName = new NamespaceName(property, cluster, namespace);
        try {
            NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory().getBundles(nsName);
            Exception exception = null;
            for (NamespaceBundle nsBundle : bundles.getBundles()) {
                try {
                    // check if the bundle is owned by any broker, if not then there is no backlog on this bundle to
                    // clear
                    if (pulsar().getNamespaceService().getOwner(nsBundle).isPresent()) {
                        // TODO: make this admin call asynchronous
                        pulsar().getAdminClient().namespaces().clearNamespaceBundleBacklog(nsName.toString(),
                                nsBundle.getBundleRange());
                    }
                } catch (Exception e) {
                    if (exception == null) {
                        exception = e;
                    }
                }
            }
            if (exception != null) {
                if (exception instanceof PulsarAdminException) {
                    throw new RestException((PulsarAdminException) exception);
                } else {
                    throw new RestException(exception.getCause());
                }
            }
        } catch (WebApplicationException wae) {
            throw wae;
        } catch (Exception e) {
            throw new RestException(e);
        }
        log.info("[{}] Successfully cleared backlog on all the bundles for namespace {}", clientAppId(),
                nsName.toString());
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{bundle}/clearBacklog")
    @ApiOperation(value = "Clear backlog for all destinations on a namespace bundle.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void clearNamespaceBundleBacklog(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateAdminAccessOnProperty(property);

        Policies policies = getNamespacePolicies(property, cluster, namespace);

        if (!cluster.equals(Namespaces.GLOBAL_CLUSTER)) {
            validateClusterOwnership(cluster);
            validateClusterForProperty(property, cluster);
        }

        NamespaceName nsName = new NamespaceName(property, cluster, namespace);
        validateNamespaceBundleOwnership(nsName, policies.bundles, bundleRange, authoritative, true);

        clearBacklog(nsName, bundleRange, null);
        log.info("[{}] Successfully cleared backlog on namespace bundle {}/{}", clientAppId(), nsName.toString(),
                bundleRange);
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/clearBacklog/{subscription}")
    @ApiOperation(value = "Clear backlog for a given subscription on all destinations on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void clearNamespaceBacklogForSubscription(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("subscription") String subscription,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateAdminAccessOnProperty(property);

        NamespaceName nsName = new NamespaceName(property, cluster, namespace);
        try {
            NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory().getBundles(nsName);
            Exception exception = null;
            for (NamespaceBundle nsBundle : bundles.getBundles()) {
                try {
                    // check if the bundle is owned by any broker, if not then there is no backlog on this bundle to
                    // clear
                    if (pulsar().getNamespaceService().getOwner(nsBundle).isPresent()) {
                        // TODO: make this admin call asynchronous
                        pulsar().getAdminClient().namespaces().clearNamespaceBundleBacklogForSubscription(
                                nsName.toString(), nsBundle.getBundleRange(), subscription);
                    }
                } catch (Exception e) {
                    if (exception == null) {
                        exception = e;
                    }
                }
            }
            if (exception != null) {
                if (exception instanceof PulsarAdminException) {
                    throw new RestException((PulsarAdminException) exception);
                } else {
                    throw new RestException(exception.getCause());
                }
            }
        } catch (WebApplicationException wae) {
            throw wae;
        } catch (Exception e) {
            throw new RestException(e);
        }
        log.info("[{}] Successfully cleared backlog for subscription {} on all the bundles for namespace {}",
                clientAppId(), subscription, nsName.toString());
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{bundle}/clearBacklog/{subscription}")
    @ApiOperation(value = "Clear backlog for a given subscription on all destinations on a namespace bundle.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void clearNamespaceBundleBacklogForSubscription(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("subscription") String subscription, @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateAdminAccessOnProperty(property);

        Policies policies = getNamespacePolicies(property, cluster, namespace);

        if (!cluster.equals(Namespaces.GLOBAL_CLUSTER)) {
            validateClusterOwnership(cluster);
            validateClusterForProperty(property, cluster);
        }

        NamespaceName nsName = new NamespaceName(property, cluster, namespace);
        validateNamespaceBundleOwnership(nsName, policies.bundles, bundleRange, authoritative, true);

        clearBacklog(nsName, bundleRange, subscription);
        log.info("[{}] Successfully cleared backlog for subscription {} on namespace bundle {}/{}", clientAppId(),
                subscription, nsName.toString(), bundleRange);
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/unsubscribe/{subscription}")
    @ApiOperation(value = "Unsubscribes the given subscription on all destinations on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void unsubscribeNamespace(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("subscription") String subscription,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateAdminAccessOnProperty(property);

        NamespaceName nsName = new NamespaceName(property, cluster, namespace);
        try {
            NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory().getBundles(nsName);
            Exception exception = null;
            for (NamespaceBundle nsBundle : bundles.getBundles()) {
                try {
                    // check if the bundle is owned by any broker, if not then there are no subscriptions
                    if (pulsar().getNamespaceService().getOwner(nsBundle).isPresent()) {
                        // TODO: make this admin call asynchronous
                        pulsar().getAdminClient().namespaces().unsubscribeNamespaceBundle(nsName.toString(),
                                nsBundle.getBundleRange(), subscription);
                    }
                } catch (Exception e) {
                    if (exception == null) {
                        exception = e;
                    }
                }
            }
            if (exception != null) {
                if (exception instanceof PulsarAdminException) {
                    throw new RestException((PulsarAdminException) exception);
                } else {
                    throw new RestException(exception.getCause());
                }
            }
        } catch (WebApplicationException wae) {
            throw wae;
        } catch (Exception e) {
            throw new RestException(e);
        }
        log.info("[{}] Successfully unsubscribed {} on all the bundles for namespace {}", clientAppId(), subscription,
                nsName.toString());
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{bundle}/unsubscribe/{subscription}")
    @ApiOperation(value = "Unsubscribes the given subscription on all destinations on a namespace bundle.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void unsubscribeNamespaceBundle(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("subscription") String subscription,
            @PathParam("bundle") String bundleRange,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateAdminAccessOnProperty(property);

        Policies policies = getNamespacePolicies(property, cluster, namespace);

        if (!cluster.equals(Namespaces.GLOBAL_CLUSTER)) {
            validateClusterOwnership(cluster);
            validateClusterForProperty(property, cluster);
        }

        NamespaceName nsName = new NamespaceName(property, cluster, namespace);
        validateNamespaceBundleOwnership(nsName, policies.bundles, bundleRange, authoritative, true);

        unsubscribe(nsName, bundleRange, subscription);
        log.info("[{}] Successfully unsubscribed {} on namespace bundle {}/{}", clientAppId(), subscription,
                nsName.toString(), bundleRange);
    }

    private void clearBacklog(NamespaceName nsName, String bundleRange, String subscription) {
        try {
            List<PersistentTopic> topicList = pulsar().getBrokerService()
                    .getAllTopicsFromNamespaceBundle(nsName.toString(), nsName.toString() + "/" + bundleRange);

            List<CompletableFuture<Void>> futures = Lists.newArrayList();
            if (subscription != null) {
                if (subscription.startsWith(pulsar().getConfiguration().getReplicatorPrefix())) {
                    subscription = PersistentReplicator.getRemoteCluster(subscription);
                }
                for (PersistentTopic topic : topicList) {
                    futures.add(topic.clearBacklog(subscription));
                }
            } else {
                for (PersistentTopic topic : topicList) {
                    futures.add(topic.clearBacklog());
                }
            }

            FutureUtil.waitForAll(futures).get();
        } catch (Exception e) {
            log.error("[{}] Failed to clear backlog for namespace {}/{}, subscription: {}", clientAppId(),
                    nsName.toString(), bundleRange, subscription, e);
            throw new RestException(e);
        }
    }

    private void unsubscribe(NamespaceName nsName, String bundleRange, String subscription) {
        try {
            List<PersistentTopic> topicList = pulsar().getBrokerService()
                    .getAllTopicsFromNamespaceBundle(nsName.toString(), nsName.toString() + "/" + bundleRange);
            List<CompletableFuture<Void>> futures = Lists.newArrayList();
            if (subscription.startsWith(pulsar().getConfiguration().getReplicatorPrefix())) {
                throw new RestException(Status.PRECONDITION_FAILED, "Cannot unsubscribe a replication cursor");
            } else {
                for (PersistentTopic topic : topicList) {
                    PersistentSubscription sub = topic.getPersistentSubscription(subscription);
                    if (sub != null) {
                        futures.add(sub.delete());
                    }
                }
            }

            FutureUtil.waitForAll(futures).get();
        } catch (RestException re) {
            throw re;
        } catch (Exception e) {
            log.error("[{}] Failed to unsubscribe {} for namespace {}/{}", clientAppId(), subscription,
                    nsName.toString(), bundleRange, e);
            if (e.getCause() instanceof SubscriptionBusyException) {
                throw new RestException(Status.PRECONDITION_FAILED, "Subscription has active connected consumers");
            }
            throw new RestException(e.getCause());
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Namespaces.class);
}
