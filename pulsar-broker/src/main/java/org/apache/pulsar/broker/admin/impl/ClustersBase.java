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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Example;
import io.swagger.annotations.ExampleProperty;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.resources.ClusterResources.FailureDomainResources;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.common.naming.Constants;
import org.apache.pulsar.common.naming.NamedEntity;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationData;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.FailureDomainImpl;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicies;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicyImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
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
    public void getClusters(@Suspended AsyncResponse asyncResponse) {
        clusterResources().listAsync()
                .thenApply(clusters -> clusters.stream()
                        // Remove "global" cluster from returned list
                        .filter(cluster -> !Constants.GLOBAL_CLUSTER.equals(cluster))
                        .collect(Collectors.toSet()))
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get clusters {}", clientAppId(), ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{cluster}")
    @ApiOperation(
        value = "Get the configuration for the specified cluster.",
        response = ClusterDataImpl.class,
        notes = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Return the cluster data.", response = ClusterDataImpl.class),
            @ApiResponse(code = 403, message = "Don't have admin permission."),
            @ApiResponse(code = 404, message = "Cluster doesn't exist."),
            @ApiResponse(code = 500, message = "Internal server error.")
    })
    public void getCluster(@Suspended AsyncResponse asyncResponse,
                           @ApiParam(value = "The cluster name", required = true)
                           @PathParam("cluster") String cluster) {
        validateSuperUserAccessAsync()
                .thenCompose(__ -> clusterResources().getClusterAsync(cluster))
                .thenAccept(clusterData -> {
                    asyncResponse.resume(clusterData
                            .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Cluster does not exist")));
                }).exceptionally(ex -> {
                    log.error("[{}] Failed to get cluster {}", clientAppId(), cluster, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
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
        @Suspended AsyncResponse asyncResponse,
        @ApiParam(value = "The cluster name", required = true)
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
        ) ClusterDataImpl clusterData) {
        validateSuperUserAccessAsync()
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> {
                    NamedEntity.checkName(cluster);
                    return clusterResources().getClusterAsync(cluster);
                }).thenCompose(clusterOpt -> {
                    if (clusterOpt.isPresent()) {
                        throw new RestException(Status.CONFLICT, "Cluster already exists");
                    }
                    return clusterResources().createClusterAsync(cluster, clusterData);
                }).thenAccept(__ -> {
                    log.info("[{}] Created cluster {}", clientAppId(), cluster);
                    asyncResponse.resume(Response.ok().build());
                }).exceptionally(ex -> {
                    log.error("[{}] Failed to create cluster {}", clientAppId(), cluster, ex);
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    if (realCause instanceof IllegalArgumentException) {
                        asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                                "Cluster name is not valid"));
                        return null;
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
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
        @Suspended AsyncResponse asyncResponse,
        @ApiParam(value = "The cluster name", required = true)
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
        ) ClusterDataImpl clusterData) {
        validateSuperUserAccessAsync()
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> clusterResources().updateClusterAsync(cluster, old -> clusterData))
                .thenAccept(__ -> {
                    log.info("[{}] Updated cluster {}", clientAppId(), cluster);
                    asyncResponse.resume(Response.ok().build());
                }).exceptionally(ex -> {
                    log.error("[{}] Failed to update cluster {}", clientAppId(), cluster, ex);
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    if (realCause instanceof MetadataStoreException.NotFoundException) {
                        asyncResponse.resume(new RestException(Status.NOT_FOUND, "Cluster does not exist"));
                        return null;
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
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
    public void setPeerClusterNames(@Suspended AsyncResponse asyncResponse,
                                    @ApiParam(value = "The cluster name", required = true)
                                    @PathParam("cluster") String cluster,
                                    @ApiParam(
                                        value = "The list of peer cluster names",
                                        required = true,
                                        examples = @Example(
                                        value = @ExampleProperty(mediaType = MediaType.APPLICATION_JSON,
                                        value = "[\n"
                                                + "   'cluster-a',\n"
                                                + "   'cluster-b'\n"
                                                + "]")))
                                    LinkedHashSet<String> peerClusterNames) {
        validateSuperUserAccessAsync()
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> innerSetPeerClusterNamesAsync(cluster, peerClusterNames))
                .thenAccept(__ -> {
                    log.info("[{}] Successfully added peer-cluster {} for {}",
                            clientAppId(), peerClusterNames, cluster);
                    asyncResponse.resume(Response.noContent().build());
                }).exceptionally(ex -> {
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    log.error("[{}] Failed to validate peer-cluster list {}, {}", clientAppId(), peerClusterNames, ex);
                    if (realCause instanceof NotFoundException) {
                        asyncResponse.resume(new RestException(Status.NOT_FOUND, "Cluster does not exist"));
                        return null;
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });

    }

    private CompletableFuture<Void> innerSetPeerClusterNamesAsync(String cluster,
                                                                LinkedHashSet<String> peerClusterNames) {
        // validate if peer-cluster exist
        CompletableFuture<Void> future;
        if (CollectionUtils.isNotEmpty(peerClusterNames)) {
            future = FutureUtil.waitForAll(peerClusterNames.stream().map(peerCluster -> {
                if (cluster.equalsIgnoreCase(peerCluster)) {
                    return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                            cluster + " itself can't be part of peer-list"));
                }
                return clusterResources().getClusterAsync(peerCluster)
                        .thenAccept(peerClusterOpt -> {
                            if (!peerClusterOpt.isPresent()) {
                                throw new RestException(Status.PRECONDITION_FAILED,
                                        "Peer cluster " + peerCluster + " does not exist");
                            }
                        });
            }).collect(Collectors.toList()));
        } else {
            future = CompletableFuture.completedFuture(null);
        }
        return future.thenCompose(__ -> clusterResources().updateClusterAsync(cluster,
                old -> old.clone().peerClusterNames(peerClusterNames).build()));
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
    public void getPeerCluster(@Suspended AsyncResponse asyncResponse,
                               @ApiParam(value = "The cluster name", required = true)
                               @PathParam("cluster") String cluster) {
        validateSuperUserAccessAsync()
                .thenCompose(__ -> clusterResources().getClusterAsync(cluster))
                .thenAccept(clusterOpt -> {
                    ClusterData clusterData =
                            clusterOpt.orElseThrow(() -> new RestException(Status.NOT_FOUND, "Cluster does not exist"));
                    asyncResponse.resume(clusterData.getPeerClusterNames());
                }).exceptionally(ex -> {
                    log.error("[{}] Failed to get cluster {}", clientAppId(), cluster, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
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
    public void deleteCluster(@Suspended AsyncResponse asyncResponse,
                              @ApiParam(value = "The cluster name", required = true)
                              @PathParam("cluster") String cluster) {
        validateSuperUserAccessAsync()
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> internalDeleteClusterAsync(cluster))
                .thenAccept(__ -> {
                    log.info("[{}] Deleted cluster {}", clientAppId(), cluster);
                    asyncResponse.resume(Response.noContent().build());
                }).exceptionally(ex -> {
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    if (realCause instanceof NotFoundException) {
                        log.warn("[{}] Failed to delete cluster {} - Does not exist", clientAppId(), cluster);
                        asyncResponse.resume(new RestException(Status.NOT_FOUND, "Cluster does not exist"));
                        return null;
                    }
                    log.error("[{}] Failed to delete cluster {}", clientAppId(), cluster, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    private CompletableFuture<Void> internalDeleteClusterAsync(String cluster) {
        // Check that the cluster is not used by any tenant (eg: no namespaces provisioned there)
        return pulsar().getPulsarResources().getClusterResources().isClusterUsedAsync(cluster)
                .thenCompose(isClusterUsed -> {
                    if (isClusterUsed) {
                        throw new RestException(Status.PRECONDITION_FAILED, "Cluster not empty");
                    }
                    // check the namespaceIsolationPolicies associated with the cluster
                    return namespaceIsolationPolicies().getIsolationDataPoliciesAsync(cluster);
                }).thenCompose(nsIsolationPoliciesOpt -> {
                    if (nsIsolationPoliciesOpt.isPresent()) {
                        if (!nsIsolationPoliciesOpt.get().getPolicies().isEmpty()) {
                            throw new RestException(Status.PRECONDITION_FAILED, "Cluster not empty");
                        }
                        // Need to delete the isolation policies if present
                        return namespaceIsolationPolicies().deleteIsolationDataAsync(cluster);
                    }
                    return CompletableFuture.completedFuture(null);
                }).thenCompose(unused -> clusterResources()
                        .getFailureDomainResources().deleteFailureDomainsAsync(cluster)
                        .thenCompose(__ -> clusterResources().deleteClusterAsync(cluster)));
    }

    @GET
    @Path("/{cluster}/namespaceIsolationPolicies")
    @ApiOperation(
        value = "Get the namespace isolation policies assigned to the cluster.",
        response = NamespaceIsolationDataImpl.class,
        responseContainer = "Map",
        notes = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "Don't have admin permission."),
            @ApiResponse(code = 404, message = "Cluster doesn't exist."),
            @ApiResponse(code = 500, message = "Internal server error.")
    })
    public Map<String, ? extends NamespaceIsolationData> getNamespaceIsolationPolicies(
        @ApiParam(
            value = "The cluster name",
            required = true
        )
        @PathParam("cluster") String cluster
    ) throws Exception {
        validateSuperUserAccess();
        if (!clusterResources().clusterExists(cluster)) {
            throw new RestException(Status.NOT_FOUND, "Cluster " + cluster + " does not exist.");
        }

        try {
            NamespaceIsolationPolicies nsIsolationPolicies = namespaceIsolationPolicies()
                    .getIsolationDataPolicies(cluster)
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
            response = NamespaceIsolationDataImpl.class,
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
            NamespaceIsolationPolicies nsIsolationPolicies = namespaceIsolationPolicies()
                    .getIsolationDataPolicies(cluster)
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND,
                            "NamespaceIsolationPolicies for cluster " + cluster + " does not exist"));
            // construct the response to Namespace isolation data map
            if (!nsIsolationPolicies.getPolicies().containsKey(policyName)) {
                log.info("[{}] Cannot find NamespaceIsolationPolicy {} for cluster {}",
                        clientAppId(), policyName, cluster);
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
        response = BrokerNamespaceIsolationDataImpl.class,
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
        Map<String, ? extends NamespaceIsolationData> nsPolicies;
        try {
            availableBrokers = pulsar().getLoadManager().get().getAvailableBrokers();
        } catch (Exception e) {
            log.error("[{}] Failed to get list of brokers in cluster {}", clientAppId(), cluster, e);
            throw new RestException(e);
        }
        try {
            Optional<NamespaceIsolationPolicies> nsPoliciesResult = namespaceIsolationPolicies()
                    .getIsolationDataPolicies(cluster);
            if (!nsPoliciesResult.isPresent()) {
                throw new RestException(Status.NOT_FOUND, "namespace-isolation policies not found for " + cluster);
            }
            nsPolicies = nsPoliciesResult.get().getPolicies();
        } catch (Exception e) {
            log.error("[{}] Failed to get namespace isolation-policies {}", clientAppId(), cluster, e);
            throw new RestException(e);
        }
        return availableBrokers.stream().map(broker -> {
            BrokerNamespaceIsolationData.Builder brokerIsolationData = BrokerNamespaceIsolationData.builder()
                    .brokerName(broker);
            if (nsPolicies != null) {
                List<String> namespaceRegexes = new ArrayList<>();
                nsPolicies.forEach((name, policyData) -> {
                    NamespaceIsolationPolicyImpl nsPolicyImpl = new NamespaceIsolationPolicyImpl(policyData);
                    if (nsPolicyImpl.isPrimaryBroker(broker) || nsPolicyImpl.isSecondaryBroker(broker)) {
                        namespaceRegexes.addAll(policyData.getNamespaces());
                        if (nsPolicyImpl.isPrimaryBroker(broker)) {
                            brokerIsolationData.primary(true);
                        }
                    }
                });

                brokerIsolationData.namespaceRegex(namespaceRegexes);
            }
            return brokerIsolationData.build();
        }).collect(Collectors.toList());
    }

    @GET
    @Path("/{cluster}/namespaceIsolationPolicies/brokers/{broker}")
    @ApiOperation(
        value = "Get a broker with namespace-isolation policies attached to it.",
        response = BrokerNamespaceIsolationDataImpl.class,
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

        Map<String, ? extends NamespaceIsolationData> nsPolicies;
        try {
            Optional<NamespaceIsolationPolicies> nsPoliciesResult = namespaceIsolationPolicies()
                    .getIsolationDataPolicies(cluster);
            if (!nsPoliciesResult.isPresent()) {
                throw new RestException(Status.NOT_FOUND, "namespace-isolation policies not found for " + cluster);
            }
            nsPolicies = nsPoliciesResult.get().getPolicies();
        } catch (Exception e) {
            log.error("[{}] Failed to get namespace isolation-policies {}", clientAppId(), cluster, e);
            throw new RestException(e);
        }
        BrokerNamespaceIsolationData.Builder brokerIsolationData = BrokerNamespaceIsolationData.builder()
                .brokerName(broker);
        if (nsPolicies != null) {
            List<String> namespaceRegexes = new ArrayList<>();
            nsPolicies.forEach((name, policyData) -> {
                NamespaceIsolationPolicyImpl nsPolicyImpl = new NamespaceIsolationPolicyImpl(policyData);
                boolean isPrimary = nsPolicyImpl.isPrimaryBroker(broker);
                if (isPrimary || nsPolicyImpl.isSecondaryBroker(broker)) {
                    namespaceRegexes.addAll(policyData.getNamespaces());
                    brokerIsolationData.primary(isPrimary);
                    brokerIsolationData.policyName(name);
                }
            });
            brokerIsolationData.namespaceRegex(namespaceRegexes);
        }
        return brokerIsolationData.build();
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
        @Suspended final AsyncResponse asyncResponse,
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
                NamespaceIsolationDataImpl policyData
    ) {
        validateSuperUserAccess();
        validateClusterExists(cluster);
        validatePoliciesReadOnlyAccess();

        String jsonInput = null;
        try {
            // validate the policy data before creating the node
            policyData.validate();
            jsonInput = ObjectMapperFactory.create().writeValueAsString(policyData);

            NamespaceIsolationPolicies nsIsolationPolicies = namespaceIsolationPolicies()
                    .getIsolationDataPolicies(cluster).orElseGet(() -> {
                        try {
                            namespaceIsolationPolicies().setIsolationDataWithCreate(cluster,
                                    (p) -> Collections.emptyMap());
                            return new NamespaceIsolationPolicies();
                        } catch (Exception e) {
                            throw new RestException(e);
                        }
                    });

            nsIsolationPolicies.setPolicy(policyName, policyData);
            namespaceIsolationPolicies().setIsolationData(cluster, old -> nsIsolationPolicies.getPolicies());

            // whether or not make the isolation update on time.
            if (pulsar().getConfiguration().isEnableNamespaceIsolationUpdateOnTime()) {
                filterAndUnloadMatchedNameSpaces(asyncResponse, policyData);
            } else {
                asyncResponse.resume(Response.noContent().build());
                return;
            }
        } catch (IllegalArgumentException iae) {
            log.info("[{}] Failed to update clusters/{}/namespaceIsolationPolicies/{}. Input data is invalid",
                    clientAppId(), cluster, policyName, iae);
            asyncResponse.resume(new RestException(Status.BAD_REQUEST,
                    "Invalid format of input policy data. policy: " + policyName + "; data: " + jsonInput));
        } catch (NotFoundException nne) {
            log.warn("[{}] Failed to update clusters/{}/namespaceIsolationPolicies: Does not exist", clientAppId(),
                    cluster);
            asyncResponse.resume(new RestException(Status.NOT_FOUND,
                    "NamespaceIsolationPolicies for cluster " + cluster + " does not exist"));
        } catch (Exception e) {
            log.error("[{}] Failed to update clusters/{}/namespaceIsolationPolicies/{}", clientAppId(), cluster,
                    policyName, e);
            asyncResponse.resume(new RestException(e));
        }
    }

    // get matched namespaces; call unload for each namespaces;
    private void filterAndUnloadMatchedNameSpaces(AsyncResponse asyncResponse,
                                                  NamespaceIsolationDataImpl policyData) throws Exception {
        Namespaces namespaces = pulsar().getAdminClient().namespaces();

        List<String> nssToUnload = Lists.newArrayList();

        pulsar().getAdminClient().tenants().getTenantsAsync()
            .whenComplete((tenants, ex) -> {
                if (ex != null) {
                    log.error("[{}] Failed to get tenants when setNamespaceIsolationPolicy.", clientAppId(), ex);
                    return;
                }
                AtomicInteger tenantsNumber = new AtomicInteger(tenants.size());
                // get all tenants now, for each tenants, get its namespaces
                tenants.forEach(tenant -> namespaces.getNamespacesAsync(tenant)
                    .whenComplete((nss, e) -> {
                        int leftTenantsToHandle = tenantsNumber.decrementAndGet();
                        if (e != null) {
                            log.error("[{}] Failed to get namespaces for tenant {} when setNamespaceIsolationPolicy.",
                                clientAppId(), tenant, e);

                            if (leftTenantsToHandle == 0) {
                                unloadMatchedNamespacesList(asyncResponse, nssToUnload, namespaces);
                            }

                            return;
                        }

                        AtomicInteger nssNumber = new AtomicInteger(nss.size());

                        // get all namespaces for this tenant now.
                        nss.forEach(namespaceName -> {
                            int leftNssToHandle = nssNumber.decrementAndGet();

                            // if namespace match any policy regex, add it to ns list to be unload.
                            if (policyData.getNamespaces().stream()
                                .anyMatch(nsnameRegex -> namespaceName.matches(nsnameRegex))) {
                                nssToUnload.add(namespaceName);
                            }

                            // all the tenants & namespaces get filtered.
                            if (leftNssToHandle == 0 && leftTenantsToHandle == 0) {
                                unloadMatchedNamespacesList(asyncResponse, nssToUnload, namespaces);
                            }
                        });
                    }));
            });
    }

    private void unloadMatchedNamespacesList(AsyncResponse asyncResponse,
                                             List<String> nssToUnload,
                                             Namespaces namespaces) {
        if (nssToUnload.size() == 0) {
            asyncResponse.resume(Response.noContent().build());
            return;
        }

        List<CompletableFuture<Void>> futures = nssToUnload.stream()
            .map(namespaceName -> namespaces.unloadAsync(namespaceName))
            .collect(Collectors.toList());

        FutureUtil.waitForAll(futures).whenComplete((result, exception) -> {
            if (exception != null) {
                log.error("[{}] Failed to unload namespace while setNamespaceIsolationPolicy.",
                    clientAppId(), exception);
                asyncResponse.resume(new RestException(exception));
                return;
            }

            try {
                // write load info to load manager to make the load happens fast
                pulsar().getLoadManager().get().writeLoadReportOnZookeeper(true);
            } catch (Exception e) {
                log.warn("[{}] Failed to writeLoadReportOnZookeeper.", clientAppId(), e);
            }

            asyncResponse.resume(Response.noContent().build());
            return;
        });
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

            NamespaceIsolationPolicies nsIsolationPolicies = namespaceIsolationPolicies()
                    .getIsolationDataPolicies(cluster).orElseGet(() -> {
                        try {
                            namespaceIsolationPolicies().setIsolationDataWithCreate(cluster,
                                    (p) -> Collections.emptyMap());
                            return new NamespaceIsolationPolicies();
                        } catch (Exception e) {
                            throw new RestException(e);
                        }
                    });

            nsIsolationPolicies.deletePolicy(policyName);
            namespaceIsolationPolicies().setIsolationData(cluster, old -> nsIsolationPolicies.getPolicies());
        } catch (NotFoundException nne) {
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
                FailureDomainImpl domain
    ) throws Exception {
        validateSuperUserAccess();
        validateClusterExists(cluster);
        validateBrokerExistsInOtherDomain(cluster, domainName, domain);

        try {
            clusterResources().getFailureDomainResources()
                    .setFailureDomainWithCreate(cluster, domainName, old -> domain);
        } catch (NotFoundException nne) {
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
        response = FailureDomainImpl.class,
        responseContainer = "Map",
        notes = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(code = 403, message = "Don't have admin permission"),
        @ApiResponse(code = 500, message = "Internal server error")
    })
    public Map<String, FailureDomainImpl> getFailureDomains(
        @ApiParam(
            value = "The cluster name",
            required = true
        )
        @PathParam("cluster") String cluster
    ) throws Exception {
        validateSuperUserAccess();

        Map<String, FailureDomainImpl> domains = Maps.newHashMap();
        try {
            FailureDomainResources fdr = clusterResources().getFailureDomainResources();
            for (String domainName : fdr.listFailureDomains(cluster)) {
                try {
                    Optional<FailureDomainImpl> domain = fdr.getFailureDomain(cluster, domainName);
                    domain.ifPresent(failureDomain -> domains.put(domainName, failureDomain));
                } catch (Exception e) {
                    log.warn("Failed to get domain {}", domainName, e);
                }
            }
        } catch (NotFoundException e) {
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
        response = FailureDomainImpl.class,
        notes = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(code = 403, message = "Don't have admin permission"),
        @ApiResponse(code = 404, message = "FailureDomain doesn't exist"),
        @ApiResponse(code = 412, message = "Cluster doesn't exist"),
        @ApiResponse(code = 500, message = "Internal server error")
    })
    public FailureDomainImpl getDomain(
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
            return clusterResources().getFailureDomainResources().getFailureDomain(cluster, domainName)
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
            clusterResources().getFailureDomainResources().deleteFailureDomain(cluster, domainName);
        } catch (NotFoundException nne) {
            log.warn("[{}] Domain {} does not exist in {}", clientAppId(), domainName, cluster);
            throw new RestException(Status.NOT_FOUND,
                    "Domain-name " + domainName + " or cluster " + cluster + " does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to delete domain {} in cluster {}", clientAppId(), domainName, cluster, e);
            throw new RestException(e);
        }
    }

    private void validateBrokerExistsInOtherDomain(final String cluster, final String inputDomainName,
            final FailureDomainImpl inputDomain) {
        if (inputDomain != null && inputDomain.brokers != null) {
            try {
                for (String domainName : clusterResources().getFailureDomainResources()
                        .listFailureDomains(cluster)) {
                    if (inputDomainName.equals(domainName)) {
                        continue;
                    }
                    try {
                        Optional<FailureDomainImpl> domain =
                                clusterResources().getFailureDomainResources().getFailureDomain(cluster, domainName);
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
            } catch (NotFoundException e) {
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
