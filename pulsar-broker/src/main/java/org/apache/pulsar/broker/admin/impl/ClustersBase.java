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
package org.apache.pulsar.broker.admin.impl;

import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.apache.pulsar.broker.namespace.NamespaceService.NAMESPACE_ISOLATION_POLICIES;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import io.swagger.annotations.Example;
import io.swagger.annotations.ExampleProperty;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.Constants;
import org.apache.pulsar.common.naming.NamedEntity;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicies;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicyImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClustersBase extends AdminResource {

    @GET
    @ApiOperation(
            value = "Get the list of all the Pulsar clusters.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Return a list of clusters."),
            @ApiResponse(code = 500, message = "Internal server error.")
    })
    public Set<String> getClusters() throws Exception {
        try {
            // Remove "global" cluster from returned list
            Set<String> clusters = clustersListCache().get().stream()
                    .filter(cluster -> !Constants.GLOBAL_CLUSTER.equals(cluster)).collect(Collectors.toSet());
            return clusters;
        } catch (Exception e) {
            log.error("[{}] Failed to get clusters list", clientAppId(), e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{cluster}")
    @ApiOperation(
        value = "Get the configuration for the specified cluster.",
        response = ClusterData.class,
        notes = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Return the cluster data.", response = ClusterData.class),
            @ApiResponse(code = 403, message = "Don't have admin permission."),
            @ApiResponse(code = 404, message = "Cluster doesn't exist."),
            @ApiResponse(code = 500, message = "Internal server error.")
    })
    public ClusterData getCluster(
        @ApiParam(
            value = "The cluster name",
            required = true
        )
        @PathParam("cluster") String cluster
    ) {
        validateSuperUserAccess();

        try {
            return clustersCache().get(path("clusters", cluster))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Cluster does not exist"));
        } catch (Exception e) {
            log.error("[{}] Failed to get cluster {}", clientAppId(), cluster, e);
            if (e instanceof RestException) {
                throw (RestException) e;
            } else {
                throw new RestException(e);
            }
        }
    }

    @PUT
    @Path("/{cluster}")
    @ApiOperation(
        value = "Create a new cluster.",
        notes = "This operation requires Pulsar superuser privileges, and the name cannot contain the '/' characters."
    )
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Cluster has been created."),
            @ApiResponse(code = 403, message = "You don't have admin permission to create the cluster."),
            @ApiResponse(code = 409, message = "Cluster already exists."),
            @ApiResponse(code = 412, message = "Cluster name is not valid."),
            @ApiResponse(code = 500, message = "Internal server error.")
    })
    public void createCluster(
        @ApiParam(
            value = "The cluster name",
            required = true
        )
        @PathParam("cluster") String cluster,
        @ApiParam(
            value = "The cluster data",
            required = true,
            examples = @Example(
                value = @ExampleProperty(
                    mediaType = MediaType.APPLICATION_JSON,
                    value =
                          "{\n"
                        + "   'serviceUrl': 'http://pulsar.example.com:8080',\n"
                        + "   'brokerServiceUrl': 'pulsar://pulsar.example.com:6651',\n"
                        + "}"
                )
            )
        )
        ClusterData clusterData
    ) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        try {
            NamedEntity.checkName(cluster);
            zkCreate(path("clusters", cluster), jsonMapper().writeValueAsBytes(clusterData));
            log.info("[{}] Created cluster {}", clientAppId(), cluster);
        } catch (KeeperException.NodeExistsException e) {
            log.warn("[{}] Failed to create already existing cluster {}", clientAppId(), cluster);
            throw new RestException(Status.CONFLICT, "Cluster already exists");
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Failed to create cluster with invalid name {}", clientAppId(), cluster, e);
            throw new RestException(Status.PRECONDITION_FAILED, "Cluster name is not valid");
        } catch (Exception e) {
            log.error("[{}] Failed to create cluster {}", clientAppId(), cluster, e);
            throw new RestException(e);
        }
    }

    @POST
    @Path("/{cluster}")
    @ApiOperation(
        value = "Update the configuration for a cluster.",
        notes = "This operation requires Pulsar superuser privileges.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Cluster has been updated."),
            @ApiResponse(code = 403, message = "Don't have admin permission or policies are read-only."),
            @ApiResponse(code = 404, message = "Cluster doesn't exist."),
            @ApiResponse(code = 500, message = "Internal server error.")
    })
    public void updateCluster(
        @ApiParam(
            value = "The cluster name",
            required = true
        )
        @PathParam("cluster") String cluster,
        @ApiParam(
            value = "The cluster data",
            required = true,
            examples = @Example(
                value = @ExampleProperty(
                    mediaType = MediaType.APPLICATION_JSON,
                    value =
                          "{\n"
                        + "   'serviceUrl': 'http://pulsar.example.com:8080',\n"
                        + "   'brokerServiceUrl': 'pulsar://pulsar.example.com:6651'\n"
                        + "}"
                )
            )
        )
        ClusterData clusterData
    ) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        try {
            String clusterPath = path("clusters", cluster);
            Stat nodeStat = new Stat();
            byte[] content = globalZk().getData(clusterPath, null, nodeStat);
            ClusterData currentClusterData = null;
            if (content.length > 0) {
                currentClusterData = jsonMapper().readValue(content, ClusterData.class);
                // only update cluster-url-data and not overwrite other metadata such as peerClusterNames
                currentClusterData.update(clusterData);
            } else {
                currentClusterData = clusterData;
            }
            // Write back the new updated ClusterData into zookeeper
            globalZk().setData(clusterPath, jsonMapper().writeValueAsBytes(currentClusterData),
                    nodeStat.getVersion());
            globalZkCache().invalidate(clusterPath);
            log.info("[{}] Updated cluster {}", clientAppId(), cluster);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update cluster {}: Does not exist", clientAppId(), cluster);
            throw new RestException(Status.NOT_FOUND, "Cluster does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to update cluster {}", clientAppId(), cluster, e);
            throw new RestException(e);
        }
    }

    @POST
    @Path("/{cluster}/peers")
    @ApiOperation(
        value = "Update peer-cluster-list for a cluster.",
        notes = "This operation requires Pulsar superuser privileges.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Cluster has been updated."),
            @ApiResponse(code = 403, message = "Don't have admin permission or policies are read-only."),
            @ApiResponse(code = 404, message = "Cluster doesn't exist."),
            @ApiResponse(code = 412, message = "Peer cluster doesn't exist."),
            @ApiResponse(code = 500, message = "Internal server error.")
    })
    public void setPeerClusterNames(
        @ApiParam(
            value = "The cluster name",
            required = true
        )
        @PathParam("cluster") String cluster,
        @ApiParam(
            value = "The list of peer cluster names",
            required = true,
            examples = @Example(
                value = @ExampleProperty(
                    mediaType = MediaType.APPLICATION_JSON,
                    value =
                          "[\n"
                        + "   'cluster-a',\n"
                        + "   'cluster-b'\n"
                        + "]"
                )
            )
        )
        LinkedHashSet<String> peerClusterNames
    ) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        // validate if peer-cluster exist
        if (peerClusterNames != null && !peerClusterNames.isEmpty()) {
            for (String peerCluster : peerClusterNames) {
                try {
                    if (cluster.equalsIgnoreCase(peerCluster)) {
                        throw new RestException(Status.PRECONDITION_FAILED,
                                cluster + " itself can't be part of peer-list");
                    }
                    clustersCache().get(path("clusters", peerCluster))
                            .orElseThrow(() -> new RestException(Status.PRECONDITION_FAILED,
                                    "Peer cluster " + peerCluster + " does not exist"));
                } catch (RestException e) {
                    log.warn("[{}] Peer cluster doesn't exist from {}, {}", clientAppId(), peerClusterNames,
                            e.getMessage());
                    throw e;
                } catch (Exception e) {
                    log.warn("[{}] Failed to validate peer-cluster list {}, {}", clientAppId(), peerClusterNames,
                            e.getMessage());
                    throw new RestException(e);
                }
            }
        }

        try {
            String clusterPath = path("clusters", cluster);
            Stat nodeStat = new Stat();
            byte[] content = globalZk().getData(clusterPath, null, nodeStat);
            ClusterData currentClusterData = jsonMapper().readValue(content, ClusterData.class);
            currentClusterData.setPeerClusterNames(peerClusterNames);
            // Write back the new updated ClusterData into zookeeper
            globalZk().setData(clusterPath, jsonMapper().writeValueAsBytes(currentClusterData), nodeStat.getVersion());
            globalZkCache().invalidate(clusterPath);
            log.info("[{}] Successfully added peer-cluster {} for {}", clientAppId(), peerClusterNames, cluster);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update cluster {}: Does not exist", clientAppId(), cluster);
            throw new RestException(Status.NOT_FOUND, "Cluster does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to update cluster {}", clientAppId(), cluster, e);
            throw new RestException(e);
        }
    }

	@GET
	@Path("/{cluster}/peers")
	@ApiOperation(
	    value = "Get the peer-cluster data for the specified cluster.",
        response = String.class,
        responseContainer = "Set",
        notes = "This operation requires Pulsar superuser privileges."
    )
	@ApiResponses(value = {
	    @ApiResponse(code = 403, message = "Don't have admin permission."),
        @ApiResponse(code = 404, message = "Cluster doesn't exist."),
        @ApiResponse(code = 500, message = "Internal server error.")
	})
	public Set<String> getPeerCluster(
        @ApiParam(
            value = "The cluster name",
            required = true
        )
	    @PathParam("cluster") String cluster
    ) {
		validateSuperUserAccess();

		try {
			String clusterPath = path("clusters", cluster);
			byte[] content = globalZk().getData(clusterPath, null, null);
			ClusterData clusterData = jsonMapper().readValue(content, ClusterData.class);
			return clusterData.getPeerClusterNames();
		} catch (KeeperException.NoNodeException e) {
			log.warn("[{}] Failed to get cluster {}: Does not exist", clientAppId(), cluster);
			throw new RestException(Status.NOT_FOUND, "Cluster does not exist");
		} catch (Exception e) {
			log.error("[{}] Failed to get cluster {}", clientAppId(), cluster, e);
			throw new RestException(e);
		}
	}

    @DELETE
    @Path("/{cluster}")
    @ApiOperation(
        value = "Delete an existing cluster.",
        notes = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Cluster has been deleted."),
            @ApiResponse(code = 403, message = "Don't have admin permission or policies are read-only."),
            @ApiResponse(code = 404, message = "Cluster doesn't exist."),
            @ApiResponse(code = 412, message = "Cluster is not empty."),
            @ApiResponse(code = 500, message = "Internal server error.")
    })
    public void deleteCluster(
        @ApiParam(
            value = "The cluster name",
            required = true
        )
        @PathParam("cluster") String cluster
    ) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        // Check that the cluster is not used by any property (eg: no namespaces provisioned there)
        boolean isClusterUsed = false;
        try {
            for (String property : globalZk().getChildren(path(POLICIES), false)) {
                if (globalZk().exists(path(POLICIES, property, cluster), false) == null) {
                    continue;
                }

                if (!globalZk().getChildren(path(POLICIES, property, cluster), false).isEmpty()) {
                    // We found a property that has at least a namespace in this cluster
                    isClusterUsed = true;
                    break;
                }
            }

            // check the namespaceIsolationPolicies associated with the cluster
            String path = path("clusters", cluster, NAMESPACE_ISOLATION_POLICIES);
            Optional<NamespaceIsolationPolicies> nsIsolationPolicies = namespaceIsolationPoliciesCache().get(path);

            // Need to delete the isolation policies if present
            if (nsIsolationPolicies.isPresent()) {
                if (nsIsolationPolicies.get().getPolicies().isEmpty()) {
                    globalZk().delete(path, -1);
                    namespaceIsolationPoliciesCache().invalidate(path);
                } else {
                    isClusterUsed = true;
                }
            }
        } catch (Exception e) {
            log.error("[{}] Failed to get cluster usage {}", clientAppId(), cluster, e);
            throw new RestException(e);
        }

        if (isClusterUsed) {
            log.warn("[{}] Failed to delete cluster {} - Cluster not empty", clientAppId(), cluster);
            throw new RestException(Status.PRECONDITION_FAILED, "Cluster not empty");
        }

        try {
            String clusterPath = path("clusters", cluster);
            deleteFailureDomain(clusterPath);
            globalZk().delete(clusterPath, -1);
            globalZkCache().invalidate(clusterPath);
            log.info("[{}] Deleted cluster {}", clientAppId(), cluster);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to delete cluster {} - Does not exist", clientAppId(), cluster);
            throw new RestException(Status.NOT_FOUND, "Cluster does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to delete cluster {}", clientAppId(), cluster, e);
            throw new RestException(e);
        }
    }

    private void deleteFailureDomain(String clusterPath) {
        try {
            String failureDomain = joinPath(clusterPath, ConfigurationCacheService.FAILURE_DOMAIN);
            if (globalZk().exists(failureDomain, false) == null) {
                return;
            }
            for (String domain : globalZk().getChildren(failureDomain, false)) {
                String domainPath = joinPath(failureDomain, domain);
                globalZk().delete(domainPath, -1);
            }
            globalZk().delete(failureDomain, -1);
            failureDomainCache().clear();
            failureDomainListCache().clear();
        } catch (Exception e) {
            log.warn("Failed to delete failure-domain under cluster {}", clusterPath);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{cluster}/namespaceIsolationPolicies")
    @ApiOperation(
        value = "Get the namespace isolation policies assigned to the cluster.",
        response = NamespaceIsolationData.class,
        responseContainer = "Map",
        notes = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "Don't have admin permission."),
            @ApiResponse(code = 404, message = "Cluster doesn't exist."),
            @ApiResponse(code = 500, message = "Internal server error.")
    })
    public Map<String, NamespaceIsolationData> getNamespaceIsolationPolicies(
        @ApiParam(
            value = "The cluster name",
            required = true
        )
        @PathParam("cluster") String cluster
    ) throws Exception {
        validateSuperUserAccess();
        if (!clustersCache().get(path("clusters", cluster)).isPresent()) {
            throw new RestException(Status.NOT_FOUND, "Cluster " + cluster + " does not exist.");
        }

        try {
            NamespaceIsolationPolicies nsIsolationPolicies = namespaceIsolationPoliciesCache()
                    .get(path("clusters", cluster, NAMESPACE_ISOLATION_POLICIES))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND,
                            "NamespaceIsolationPolicies for cluster " + cluster + " does not exist"));
            // construct the response to Namespace isolation data map
            return nsIsolationPolicies.getPolicies();
        } catch (Exception e) {
            log.error("[{}] Failed to get clusters/{}/namespaceIsolationPolicies", clientAppId(), cluster, e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{cluster}/namespaceIsolationPolicies/{policyName}")
    @ApiOperation(
            value = "Get the single namespace isolation policy assigned to the cluster.",
            response = NamespaceIsolationData.class,
            notes = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "Don't have admin permission."),
            @ApiResponse(code = 404, message = "Policy doesn't exist."),
            @ApiResponse(code = 412, message = "Cluster doesn't exist."),
            @ApiResponse(code = 500, message = "Internal server error.")
    })
    public NamespaceIsolationData getNamespaceIsolationPolicy(
        @ApiParam(
            value = "The cluster name",
            required = true
        )
        @PathParam("cluster") String cluster,
        @ApiParam(
            value = "The name of the namespace isolation policy",
            required = true
        )
        @PathParam("policyName") String policyName
    ) throws Exception {
        validateSuperUserAccess();
        validateClusterExists(cluster);

        try {
            NamespaceIsolationPolicies nsIsolationPolicies = namespaceIsolationPoliciesCache()
                    .get(path("clusters", cluster, NAMESPACE_ISOLATION_POLICIES))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND,
                            "NamespaceIsolationPolicies for cluster " + cluster + " does not exist"));
            // construct the response to Namespace isolation data map
            if (!nsIsolationPolicies.getPolicies().containsKey(policyName)) {
                log.info("[{}] Cannot find NamespaceIsolationPolicy {} for cluster {}", clientAppId(), policyName, cluster);
                throw new RestException(Status.NOT_FOUND,
                        "Cannot find NamespaceIsolationPolicy " + policyName + " for cluster " + cluster);
            }
            return nsIsolationPolicies.getPolicies().get(policyName);
        } catch (RestException re) {
            throw re;
        } catch (Exception e) {
            log.error("[{}] Failed to get clusters/{}/namespaceIsolationPolicies/{}", clientAppId(), cluster, e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{cluster}/namespaceIsolationPolicies/brokers")
    @ApiOperation(
        value = "Get list of brokers with namespace-isolation policies attached to them.",
        response = BrokerNamespaceIsolationData.class,
        responseContainer = "set",
        notes = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(code = 403, message = "Don't have admin permission."),
        @ApiResponse(code = 404, message = "Namespace-isolation policies not found."),
        @ApiResponse(code = 412, message = "Cluster doesn't exist."),
        @ApiResponse(code = 500, message = "Internal server error.")
    })
    public List<BrokerNamespaceIsolationData> getBrokersWithNamespaceIsolationPolicy(
            @ApiParam(
                value = "The cluster name",
                required = true
            )
            @PathParam("cluster") String cluster) {
        validateSuperUserAccess();
        validateClusterExists(cluster);

        Set<String> availableBrokers;
        final String nsIsolationPoliciesPath = AdminResource.path("clusters", cluster, NAMESPACE_ISOLATION_POLICIES);
        Map<String, NamespaceIsolationData> nsPolicies;
        try {
            availableBrokers = pulsar().getLoadManager().get().getAvailableBrokers();
        } catch (Exception e) {
            log.error("[{}] Failed to get list of brokers in cluster {}", clientAppId(), cluster, e);
            throw new RestException(e);
        }
        try {
            Optional<NamespaceIsolationPolicies> nsPoliciesResult = namespaceIsolationPoliciesCache()
                    .get(nsIsolationPoliciesPath);
            if (!nsPoliciesResult.isPresent()) {
                throw new RestException(Status.NOT_FOUND, "namespace-isolation policies not found for " + cluster);
            }
            nsPolicies = nsPoliciesResult.get().getPolicies();
        } catch (Exception e) {
            log.error("[{}] Failed to get namespace isolation-policies {}", clientAppId(), cluster, e);
            throw new RestException(e);
        }
        return availableBrokers.stream().map(broker -> {
            BrokerNamespaceIsolationData brokerIsolationData = new BrokerNamespaceIsolationData();
            brokerIsolationData.brokerName = broker;
            if (nsPolicies != null) {
                nsPolicies.forEach((name, policyData) -> {
                    NamespaceIsolationPolicyImpl nsPolicyImpl = new NamespaceIsolationPolicyImpl(policyData);
                    if (nsPolicyImpl.isPrimaryBroker(broker) || nsPolicyImpl.isSecondaryBroker(broker)) {
                        if (brokerIsolationData.namespaceRegex == null) {
                            brokerIsolationData.namespaceRegex = Lists.newArrayList();
                        }
                        brokerIsolationData.namespaceRegex.addAll(policyData.namespaces);
                    }
                });
            }
            return brokerIsolationData;
        }).collect(Collectors.toList());
    }

    @GET
    @Path("/{cluster}/namespaceIsolationPolicies/brokers/{broker}")
    @ApiOperation(
        value = "Get a broker with namespace-isolation policies attached to it.",
        response = BrokerNamespaceIsolationData.class,
        notes = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(code = 403, message = "Don't have admin permission."),
        @ApiResponse(code = 404, message = "Namespace-isolation policies/ Broker not found."),
        @ApiResponse(code = 412, message = "Cluster doesn't exist."),
        @ApiResponse(code = 500, message = "Internal server error.")
    })
    public BrokerNamespaceIsolationData getBrokerWithNamespaceIsolationPolicy(
        @ApiParam(
            value = "The cluster name",
            required = true
        )
        @PathParam("cluster") String cluster,
        @ApiParam(
            value = "The broker name (<broker-hostname>:<web-service-port>)",
            required = true,
            example = "broker1:8080"
        )
        @PathParam("broker") String broker) {
        validateSuperUserAccess();
        validateClusterExists(cluster);

        final String nsIsolationPoliciesPath = AdminResource.path("clusters", cluster, NAMESPACE_ISOLATION_POLICIES);
        Map<String, NamespaceIsolationData> nsPolicies;
        try {
            Optional<NamespaceIsolationPolicies> nsPoliciesResult = namespaceIsolationPoliciesCache()
                    .get(nsIsolationPoliciesPath);
            if (!nsPoliciesResult.isPresent()) {
                throw new RestException(Status.NOT_FOUND, "namespace-isolation policies not found for " + cluster);
            }
            nsPolicies = nsPoliciesResult.get().getPolicies();
        } catch (Exception e) {
            log.error("[{}] Failed to get namespace isolation-policies {}", clientAppId(), cluster, e);
            throw new RestException(e);
        }
        BrokerNamespaceIsolationData brokerIsolationData = new BrokerNamespaceIsolationData();
        brokerIsolationData.brokerName = broker;
        if (nsPolicies != null) {
            nsPolicies.forEach((name, policyData) -> {
                NamespaceIsolationPolicyImpl nsPolicyImpl = new NamespaceIsolationPolicyImpl(policyData);
                boolean isPrimary = nsPolicyImpl.isPrimaryBroker(broker);
                if (isPrimary || nsPolicyImpl.isSecondaryBroker(broker)) {
                    if (brokerIsolationData.namespaceRegex == null) {
                        brokerIsolationData.namespaceRegex = Lists.newArrayList();
                    }
                    brokerIsolationData.namespaceRegex.addAll(policyData.namespaces);
                    brokerIsolationData.isPrimary = isPrimary;
                    brokerIsolationData.policyName = name;
                }
            });
        }
        return brokerIsolationData;
    }

    @POST
    @Path("/{cluster}/namespaceIsolationPolicies/{policyName}")
    @ApiOperation(
        value = "Set namespace isolation policy.",
        notes = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "Namespace isolation policy data is invalid."),
        @ApiResponse(code = 403, message = "Don't have admin permission or policies are read-only."),
        @ApiResponse(code = 404, message = "Namespace isolation policy doesn't exist."),
        @ApiResponse(code = 412, message = "Cluster doesn't exist."),
        @ApiResponse(code = 500, message = "Internal server error.")
    })
    public void setNamespaceIsolationPolicy(
        @ApiParam(
            value = "The cluster name",
            required = true
        )
        @PathParam("cluster") String cluster,
        @ApiParam(
            value = "The namespace isolation policy name",
            required = true
        )
        @PathParam("policyName") String policyName,
        @ApiParam(
            value = "The namespace isolation policy data",
            required = true
        )
        NamespaceIsolationData policyData
    ) throws Exception {
        validateSuperUserAccess();
        validateClusterExists(cluster);
        validatePoliciesReadOnlyAccess();

        try {
            // validate the policy data before creating the node
            policyData.validate();

            String nsIsolationPolicyPath = path("clusters", cluster, NAMESPACE_ISOLATION_POLICIES);
            NamespaceIsolationPolicies nsIsolationPolicies = namespaceIsolationPoliciesCache()
                    .get(nsIsolationPolicyPath).orElseGet(() -> {
                        try {
                            this.createZnodeIfNotExist(nsIsolationPolicyPath, Optional.of(Collections.emptyMap()));
                            return new NamespaceIsolationPolicies();
                        } catch (KeeperException | InterruptedException e) {
                            throw new RestException(e);
                        }
                    });

            nsIsolationPolicies.setPolicy(policyName, policyData);
            globalZk().setData(nsIsolationPolicyPath, jsonMapper().writeValueAsBytes(nsIsolationPolicies.getPolicies()),
                    -1);
            // make sure that the cache content will be refreshed for the next read access
            namespaceIsolationPoliciesCache().invalidate(nsIsolationPolicyPath);
        } catch (IllegalArgumentException iae) {
            log.info("[{}] Failed to update clusters/{}/namespaceIsolationPolicies/{}. Input data is invalid",
                    clientAppId(), cluster, policyName, iae);
            String jsonInput = ObjectMapperFactory.create().writeValueAsString(policyData);
            throw new RestException(Status.BAD_REQUEST,
                    "Invalid format of input policy data. policy: " + policyName + "; data: " + jsonInput);
        } catch (KeeperException.NoNodeException nne) {
            log.warn("[{}] Failed to update clusters/{}/namespaceIsolationPolicies: Does not exist", clientAppId(),
                    cluster);
            throw new RestException(Status.NOT_FOUND,
                    "NamespaceIsolationPolicies for cluster " + cluster + " does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to update clusters/{}/namespaceIsolationPolicies/{}", clientAppId(), cluster,
                    policyName, e);
            throw new RestException(e);
        }
    }

    private boolean createZnodeIfNotExist(String path, Optional<Object> value) throws KeeperException, InterruptedException {
        // create persistent node on ZooKeeper
        if (globalZk().exists(path, false) == null) {
            // create all the intermediate nodes
            try {
                ZkUtils.createFullPathOptimistic(globalZk(), path,
                        value.isPresent() ? jsonMapper().writeValueAsBytes(value.get()) : null, Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                return true;
            } catch (KeeperException.NodeExistsException nee) {
                if(log.isDebugEnabled()) {
                    log.debug("Other broker preempted the full path [{}] already. Continue...", path);
                }
            } catch (JsonGenerationException e) {
                // ignore json error as it is empty hash
            } catch (JsonMappingException e) {
            } catch (IOException e) {
            }
        }
        return false;
    }

    @DELETE
    @Path("/{cluster}/namespaceIsolationPolicies/{policyName}")
    @ApiOperation(
        value = "Delete namespace isolation policy.",
        notes = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(code = 403, message = "Don't have admin permission or policies are read only."),
        @ApiResponse(code = 404, message = "Namespace isolation policy doesn't exist."),
        @ApiResponse(code = 412, message = "Cluster doesn't exist."),
        @ApiResponse(code = 500, message = "Internal server error.")
    })
    public void deleteNamespaceIsolationPolicy(
        @ApiParam(
            value = "The cluster name",
            required = true
        )
        @PathParam("cluster") String cluster,
        @ApiParam(
            value = "The namespace isolation policy name",
            required = true
        )
        @PathParam("policyName") String policyName
    ) throws Exception {
        validateSuperUserAccess();
        validateClusterExists(cluster);
        validatePoliciesReadOnlyAccess();

        try {

            String nsIsolationPolicyPath = path("clusters", cluster, NAMESPACE_ISOLATION_POLICIES);
            NamespaceIsolationPolicies nsIsolationPolicies = namespaceIsolationPoliciesCache()
                    .get(nsIsolationPolicyPath).orElseGet(() -> {
                        try {
                            this.createZnodeIfNotExist(nsIsolationPolicyPath, Optional.of(Collections.emptyMap()));
                            return new NamespaceIsolationPolicies();
                        } catch (KeeperException | InterruptedException e) {
                            throw new RestException(e);
                        }
                    });

            nsIsolationPolicies.deletePolicy(policyName);
            globalZk().setData(nsIsolationPolicyPath, jsonMapper().writeValueAsBytes(nsIsolationPolicies.getPolicies()),
                    -1);
            // make sure that the cache content will be refreshed for the next read access
            namespaceIsolationPoliciesCache().invalidate(nsIsolationPolicyPath);
        } catch (KeeperException.NoNodeException nne) {
            log.warn("[{}] Failed to update brokers/{}/namespaceIsolationPolicies: Does not exist", clientAppId(),
                    cluster);
            throw new RestException(Status.NOT_FOUND,
                    "NamespaceIsolationPolicies for cluster " + cluster + " does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to update brokers/{}/namespaceIsolationPolicies/{}", clientAppId(), cluster,
                    policyName, e);
            throw new RestException(e);
        }
    }

    @POST
    @Path("/{cluster}/failureDomains/{domainName}")
    @ApiOperation(
        value = "Set the failure domain of the cluster.",
        notes = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(code = 403, message = "Don't have admin permission."),
        @ApiResponse(code = 404, message = "Failure domain doesn't exist."),
        @ApiResponse(code = 409, message = "Broker already exists in another domain."),
        @ApiResponse(code = 412, message = "Cluster doesn't exist."),
        @ApiResponse(code = 500, message = "Internal server error.")
    })
    public void setFailureDomain(
        @ApiParam(
            value = "The cluster name",
            required = true
        )
        @PathParam("cluster") String cluster,
        @ApiParam(
            value = "The failure domain name",
            required = true
        )
        @PathParam("domainName") String domainName,
        @ApiParam(
            value = "The configuration data of a failure domain",
            required = true
        )
        FailureDomain domain
    ) throws Exception {
        validateSuperUserAccess();
        validateClusterExists(cluster);
        validateBrokerExistsInOtherDomain(cluster, domainName, domain);

        try {
            String domainPath = joinPath(pulsar().getConfigurationCache().CLUSTER_FAILURE_DOMAIN_ROOT, domainName);
            if (this.createZnodeIfNotExist(domainPath, Optional.ofNullable(domain))) {
                // clear domains-children cache
                this.failureDomainListCache().clear();
            } else {
                globalZk().setData(domainPath, jsonMapper().writeValueAsBytes(domain), -1);
                // make sure that the domain-cache will be refreshed for the next read access
                failureDomainCache().invalidate(domainPath);
            }
        } catch (KeeperException.NoNodeException nne) {
            log.warn("[{}] Failed to update domain {}. clusters {}  Does not exist", clientAppId(), cluster,
                    domainName);
            throw new RestException(Status.NOT_FOUND,
                    "Domain " + domainName + " for cluster " + cluster + " does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to update clusters/{}/domainName/{}", clientAppId(), cluster, domainName, e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{cluster}/failureDomains")
    @ApiOperation(
        value = "Get the cluster failure domains.",
        response = FailureDomain.class,
        responseContainer = "Map",
        notes = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(code = 403, message = "Don't have admin permission"),
        @ApiResponse(code = 500, message = "Internal server error")
    })
    public Map<String, FailureDomain> getFailureDomains(
        @ApiParam(
            value = "The cluster name",
            required = true
        )
        @PathParam("cluster") String cluster
    ) throws Exception {
        validateSuperUserAccess();

        Map<String, FailureDomain> domains = Maps.newHashMap();
        try {
            final String failureDomainRootPath = pulsar().getConfigurationCache().CLUSTER_FAILURE_DOMAIN_ROOT;
            for (String domainName : failureDomainListCache().get()) {
                try {
                    Optional<FailureDomain> domain = failureDomainCache()
                            .get(joinPath(failureDomainRootPath, domainName));
                    domain.ifPresent(failureDomain -> domains.put(domainName, failureDomain));
                } catch (Exception e) {
                    log.warn("Failed to get domain {}", domainName, e);
                }
            }
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failure-domain is not configured for cluster {}", clientAppId(), cluster, e);
            return Collections.emptyMap();
        } catch (Exception e) {
            log.error("[{}] Failed to get failure-domains for cluster {}", clientAppId(), cluster, e);
            throw new RestException(e);
        }
        return domains;
    }

    @GET
    @Path("/{cluster}/failureDomains/{domainName}")
    @ApiOperation(
        value = "Get a domain in a cluster",
        response = FailureDomain.class,
        notes = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(code = 403, message = "Don't have admin permission"),
        @ApiResponse(code = 404, message = "FailureDomain doesn't exist"),
        @ApiResponse(code = 412, message = "Cluster doesn't exist"),
        @ApiResponse(code = 500, message = "Internal server error")
    })
    public FailureDomain getDomain(
        @ApiParam(
            value = "The cluster name",
            required = true
        )
        @PathParam("cluster") String cluster,
        @ApiParam(
            value = "The failure domain name",
            required = true
        )
        @PathParam("domainName") String domainName
    ) throws Exception {
        validateSuperUserAccess();
        validateClusterExists(cluster);

        try {
            final String failureDomainRootPath = pulsar().getConfigurationCache().CLUSTER_FAILURE_DOMAIN_ROOT;
            return failureDomainCache().get(joinPath(failureDomainRootPath, domainName))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND,
                            "Domain " + domainName + " for cluster " + cluster + " does not exist"));
        } catch (RestException re) {
            throw re;
        } catch (Exception e) {
            log.error("[{}] Failed to get domain {} for cluster {}", clientAppId(), domainName, cluster, e);
            throw new RestException(e);
        }
    }

    @DELETE
    @Path("/{cluster}/failureDomains/{domainName}")
    @ApiOperation(
        value = "Delete the failure domain of the cluster",
        notes = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(code = 403, message = "Don't have admin permission or policy is read only"),
        @ApiResponse(code = 404, message = "FailureDomain doesn't exist"),
        @ApiResponse(code = 412, message = "Cluster doesn't exist"),
        @ApiResponse(code = 500, message = "Internal server error")
    })
    public void deleteFailureDomain(
        @ApiParam(
            value = "The cluster name",
            required = true
        )
        @PathParam("cluster") String cluster,
        @ApiParam(
            value = "The failure domain name",
            required = true
        )
        @PathParam("domainName") String domainName
    ) throws Exception {
        validateSuperUserAccess();
        validateClusterExists(cluster);

        try {
            final String domainPath = joinPath(pulsar().getConfigurationCache().CLUSTER_FAILURE_DOMAIN_ROOT, domainName);
            globalZk().delete(domainPath, -1);
            // clear domain cache
            failureDomainCache().invalidate(domainPath);
            failureDomainListCache().clear();
        } catch (KeeperException.NoNodeException nne) {
            log.warn("[{}] Domain {} does not exist in {}", clientAppId(), domainName, cluster);
            throw new RestException(Status.NOT_FOUND,
                    "Domain-name " + domainName + " or cluster " + cluster + " does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to delete domain {} in cluster {}", clientAppId(), domainName, cluster, e);
            throw new RestException(e);
        }
    }

    private void validateBrokerExistsInOtherDomain(final String cluster, final String inputDomainName,
            final FailureDomain inputDomain) {
        if (inputDomain != null && inputDomain.brokers != null) {
            try {
                final String failureDomainRootPath = pulsar().getConfigurationCache().CLUSTER_FAILURE_DOMAIN_ROOT;
                for (String domainName : failureDomainListCache().get()) {
                    if (inputDomainName.equals(domainName)) {
                        continue;
                    }
                    try {
                        Optional<FailureDomain> domain = failureDomainCache().get(joinPath(failureDomainRootPath, domainName));
                        if (domain.isPresent() && domain.get().brokers != null) {
                            List<String> duplicateBrokers = domain.get().brokers.stream().parallel()
                                    .filter(inputDomain.brokers::contains).collect(Collectors.toList());
                            if (!duplicateBrokers.isEmpty()) {
                                throw new RestException(Status.CONFLICT,
                                        duplicateBrokers + " already exists in " + domainName);
                            }
                        }
                    } catch (Exception e) {
                        if (e instanceof RestException) {
                            throw e;
                        }
                        log.warn("Failed to get domain {}", domainName, e);
                    }
                }
            } catch (KeeperException.NoNodeException e) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Domain is not configured for cluster", clientAppId(), e);
                }
            } catch (Exception e) {
                log.error("[{}] Failed to get domains for cluster {}", clientAppId(), e);
                throw new RestException(e);
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ClustersBase.class);

}
