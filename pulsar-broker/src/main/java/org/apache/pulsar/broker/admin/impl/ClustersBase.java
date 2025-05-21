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
package org.apache.pulsar.broker.admin.impl;

import static javax.ws.rs.core.Response.Status.PRECONDITION_FAILED;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Example;
import io.swagger.annotations.ExampleProperty;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.Constants;
import org.apache.pulsar.common.naming.NamedEntity;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationData;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.ClusterOperation;
import org.apache.pulsar.common.policies.data.ClusterPolicies.ClusterUrl;
import org.apache.pulsar.common.policies.data.ClusterPoliciesImpl;
import org.apache.pulsar.common.policies.data.FailureDomainImpl;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.NamespaceIsolationPolicyUnloadScope;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicies;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicyImpl;
import org.apache.pulsar.common.util.FutureUtil;
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
        validateBothSuperuserAndClusterOperation(cluster, ClusterOperation.GET_CLUSTER)
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
            @ApiResponse(code = 200, message = "Cluster has been created."),
            @ApiResponse(code = 400, message = "Bad request parameter."),
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
                    value = """
                            {
                               "serviceUrl": "http://pulsar.example.com:8080",
                               "brokerServiceUrl": "pulsar://pulsar.example.com:6651",
                            }
                            """
                )
            )
        ) ClusterDataImpl clusterData) {
        validateBothSuperuserAndClusterOperation(cluster, ClusterOperation.CREATE_CLUSTER)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> {
                    NamedEntity.checkName(cluster);
                    if (clusterData == null) {
                        throw new RestException(Status.BAD_REQUEST, "cluster data is required");
                    }
                    try {
                        clusterData.checkPropertiesIfPresent();
                    } catch (IllegalArgumentException ex) {
                        throw new RestException(Status.BAD_REQUEST, ex.getMessage());
                    }
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
                        asyncResponse.resume(new RestException(PRECONDITION_FAILED,
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
            @ApiResponse(code = 200, message = "Cluster has been updated."),
            @ApiResponse(code = 400, message = "Bad request parameter."),
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
                    value = """
                            {
                               "serviceUrl": "http://pulsar.example.com:8080",
                               "brokerServiceUrl": "pulsar://pulsar.example.com:6651"
                            }
                            """
                )
            )
        ) ClusterDataImpl clusterData) {
        validateBothSuperuserAndClusterOperation(cluster, ClusterOperation.UPDATE_CLUSTER)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> {
                    try {
                        clusterData.checkPropertiesIfPresent();
                    } catch (IllegalArgumentException ex) {
                        throw new RestException(Status.BAD_REQUEST, ex.getMessage());
                    }
                    return clusterResources().updateClusterAsync(cluster, old -> clusterData);
                }).thenAccept(__ -> {
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

    @GET
    @Path("/{cluster}/migrate")
    @ApiOperation(
        value = "Get the cluster migration configuration for the specified cluster.",
        response = ClusterDataImpl.class,
        notes = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Return the cluster data.", response = ClusterDataImpl.class),
            @ApiResponse(code = 403, message = "Don't have admin permission."),
            @ApiResponse(code = 404, message = "Cluster doesn't exist."),
            @ApiResponse(code = 500, message = "Internal server error.")
    })
    public void getClusterMigration(
        @Suspended AsyncResponse asyncResponse,
        @ApiParam(
            value = "The cluster name",
            required = true
        )
        @PathParam("cluster") String cluster) {
        validateBothSuperuserAndClusterPolicyOperation(cluster, PolicyName.CLUSTER_MIGRATION, PolicyOperation.READ)
                .thenCompose(__ -> clusterResources().getClusterPoliciesResources().getClusterPoliciesAsync(cluster))
                .thenAccept(policies -> {
                    asyncResponse.resume(
                            policies.orElseThrow(() -> new RestException(Status.NOT_FOUND, "Cluster does not exist")));
                })
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get cluster {} migration", clientAppId(), cluster, ex);
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
    @Path("/{cluster}/migrate")
    @ApiOperation(
        value = "Update the configuration for a cluster migration.",
        notes = "This operation requires Pulsar superuser privileges.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Cluster has been updated."),
            @ApiResponse(code = 400, message = "Cluster url must not be empty."),
            @ApiResponse(code = 403, message = "Don't have admin permission or policies are read-only."),
            @ApiResponse(code = 404, message = "Cluster doesn't exist."),
            @ApiResponse(code = 500, message = "Internal server error.")
    })
    public void updateClusterMigration(
        @Suspended AsyncResponse asyncResponse,
        @ApiParam(value = "The cluster name", required = true)
        @PathParam("cluster") String cluster,
        @ApiParam(value = "Is cluster migrated", required = true)
        @QueryParam("migrated") boolean isMigrated,
        @ApiParam(
            value = "The cluster url data",
            required = true,
            examples = @Example(
                value = @ExampleProperty(
                    mediaType = MediaType.APPLICATION_JSON,
                    value = """
                            {
                               "serviceUrl": "http://pulsar.example.com:8080",
                               "brokerServiceUrl": "pulsar://pulsar.example.com:6651"
                            }
                            """
                )
            )
        ) ClusterUrl clusterUrl) {
        if (isMigrated && clusterUrl.isEmpty()) {
            asyncResponse.resume(new RestException(Status.BAD_REQUEST, "Cluster url must not be empty"));
            return;
        }
        validateBothSuperuserAndClusterPolicyOperation(cluster, PolicyName.CLUSTER_MIGRATION, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> clusterResources().getClusterPoliciesResources().setPoliciesWithCreateAsync(cluster,
                        old -> {
                    ClusterPoliciesImpl data = old.orElse(new ClusterPoliciesImpl());
                    data.setMigrated(isMigrated);
                    data.setMigratedClusterUrl(clusterUrl);
                    return data;
                }))
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
                                        value = """
                                                [
                                                   "cluster-a",
                                                   "cluster-b"
                                                ]""")))
                                    LinkedHashSet<String> peerClusterNames) {
        validateBothSuperuserAndClusterOperation(cluster, ClusterOperation.UPDATE_PEER_CLUSTER)
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
                    return FutureUtil.failedFuture(new RestException(PRECONDITION_FAILED,
                            cluster + " itself can't be part of peer-list"));
                }
                return clusterResources().getClusterAsync(peerCluster)
                        .thenAccept(peerClusterOpt -> {
                            if (!peerClusterOpt.isPresent()) {
                                throw new RestException(PRECONDITION_FAILED,
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
        validateBothSuperuserAndClusterOperation(cluster, ClusterOperation.GET_PEER_CLUSTER)
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
        validateBothSuperuserAndClusterOperation(cluster, ClusterOperation.DELETE_CLUSTER)
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
                        throw new RestException(PRECONDITION_FAILED, "Cluster not empty");
                    }
                    // check the namespaceIsolationPolicies associated with the cluster
                    return namespaceIsolationPolicies().getIsolationDataPoliciesAsync(cluster);
                }).thenCompose(nsIsolationPoliciesOpt -> {
                    if (nsIsolationPoliciesOpt.isPresent()) {
                        if (!nsIsolationPoliciesOpt.get().getPolicies().isEmpty()) {
                            throw new RestException(PRECONDITION_FAILED, "Cluster not empty");
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
    public void getNamespaceIsolationPolicies(
        @Suspended AsyncResponse asyncResponse,
        @ApiParam(value = "The cluster name", required = true) @PathParam("cluster") String cluster
    ) {
        validateBothSuperuserAndClusterPolicyOperation(cluster, PolicyName.NAMESPACE_ISOLATION, PolicyOperation.READ)
                .thenCompose(__ -> validateClusterExistAsync(cluster, Status.NOT_FOUND))
                .thenCompose(__ -> internalGetNamespaceIsolationPolicies(cluster))
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get clusters/{}/namespaceIsolationPolicies", clientAppId(), cluster, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    /**
     * Verify that the cluster exists.
     * For compatibility to avoid breaking changes, we can specify a REST status code when it doesn't exist.
     * @param cluster Cluster name
     * @param notExistStatus REST status code
     */
    private CompletableFuture<Void> validateClusterExistAsync(String cluster, Status notExistStatus) {
        return clusterResources().clusterExistsAsync(cluster)
                .thenAccept(clusterExist -> {
                    if (!clusterExist) {
                        throw new RestException(notExistStatus, "Cluster " + cluster + " does not exist.");
                    }
                });
    }

    private CompletableFuture<Map<String, NamespaceIsolationDataImpl>> internalGetNamespaceIsolationPolicies(
            String cluster) {
            return namespaceIsolationPolicies().getIsolationDataPoliciesAsync(cluster)
                    .thenApply(namespaceIsolationPolicies -> {
                        if (!namespaceIsolationPolicies.isPresent()) {
                            throw new RestException(Status.NOT_FOUND,
                                    "NamespaceIsolationPolicies for cluster " + cluster + " does not exist");
                        }
                        return namespaceIsolationPolicies.get().getPolicies();
                    });
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
    public void getNamespaceIsolationPolicy(
        @Suspended AsyncResponse asyncResponse,
        @ApiParam(value = "The cluster name", required = true) @PathParam("cluster") String cluster,
        @ApiParam(value = "The name of the namespace isolation policy", required = true)
        @PathParam("policyName") String policyName
    ) {
        validateBothSuperuserAndClusterPolicyOperation(cluster, PolicyName.NAMESPACE_ISOLATION, PolicyOperation.READ)
                .thenCompose(__ -> validateClusterExistAsync(cluster, Status.PRECONDITION_FAILED))
                .thenCompose(__ -> internalGetNamespaceIsolationPolicies(cluster))
                .thenAccept(policies -> {
                    // construct the response to Namespace isolation data map
                    if (!policies.containsKey(policyName)) {
                        throw new RestException(Status.NOT_FOUND,
                                "Cannot find NamespaceIsolationPolicy " + policyName + " for cluster " + cluster);
                    }
                    asyncResponse.resume(policies.get(policyName));
                }).exceptionally(ex -> {
                    log.error("[{}] Failed to get clusters/{}/namespaceIsolationPolicies/{}",
                            clientAppId(), cluster, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
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
    public void getBrokersWithNamespaceIsolationPolicy(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "The cluster name", required = true)
            @PathParam("cluster") String cluster) {
        validateBothSuperuserAndClusterPolicyOperation(cluster, PolicyName.NAMESPACE_ISOLATION, PolicyOperation.READ)
                .thenCompose(__ -> validateClusterExistAsync(cluster, Status.PRECONDITION_FAILED))
                .thenCompose(__ -> pulsar().getLoadManager().get().getAvailableBrokersAsync())
                .thenCompose(availableBrokers -> internalGetNamespaceIsolationPolicies(cluster)
                        .thenApply(policies -> availableBrokers.stream()
                                .map(broker -> internalGetBrokerNsIsolationData(broker, policies))
                                .collect(Collectors.toList())))
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get namespace isolation-policies {}", clientAppId(), cluster, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }


    private BrokerNamespaceIsolationData internalGetBrokerNsIsolationData(
            String broker,
            Map<String, NamespaceIsolationDataImpl> policies) {
        BrokerNamespaceIsolationData.Builder brokerIsolationData =
                BrokerNamespaceIsolationData.builder().brokerName(broker);
        if (policies == null) {
            return brokerIsolationData.build();
        }
        List<String> namespaceRegexes = new ArrayList<>();
        policies.forEach((name, policyData) -> {
            NamespaceIsolationPolicyImpl nsPolicyImpl = new NamespaceIsolationPolicyImpl(policyData);
            if (nsPolicyImpl.isPrimaryBroker(broker) || nsPolicyImpl.isSecondaryBroker(broker)) {
                namespaceRegexes.addAll(policyData.getNamespaces());
                brokerIsolationData.primary(nsPolicyImpl.isPrimaryBroker(broker));
                brokerIsolationData.policyName(name);
            }
        });
        brokerIsolationData.namespaceRegex(namespaceRegexes);
        return brokerIsolationData.build();
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
    public void getBrokerWithNamespaceIsolationPolicy(
        @Suspended AsyncResponse asyncResponse,
        @ApiParam(value = "The cluster name", required = true)
        @PathParam("cluster") String cluster,
        @ApiParam(value = "The broker name (<broker-hostname>:<web-service-port>)", required = true,
            example = "broker1:8080")
        @PathParam("broker") String broker) {
        validateBothSuperuserAndClusterPolicyOperation(cluster, PolicyName.NAMESPACE_ISOLATION, PolicyOperation.READ)
                .thenCompose(__ -> validateClusterExistAsync(cluster, PRECONDITION_FAILED))
                .thenCompose(__ -> internalGetNamespaceIsolationPolicies(cluster))
                .thenApply(policies -> internalGetBrokerNsIsolationData(broker, policies))
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get namespace isolation-policies {}", clientAppId(), cluster, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{cluster}/namespaceIsolationPolicies/{policyName}")
    @ApiOperation(
        value = "Set namespace isolation policy.",
        notes = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(code = 204, message = "Set namespace isolation policy successfully."),
        @ApiResponse(code = 400, message = "Namespace isolation policy data is invalid."),
        @ApiResponse(code = 403, message = "Don't have admin permission or policies are read-only."),
        @ApiResponse(code = 404, message = "Namespace isolation policy doesn't exist."),
        @ApiResponse(code = 412, message = "Cluster doesn't exist."),
        @ApiResponse(code = 500, message = "Internal server error.")
    })
    public void setNamespaceIsolationPolicy(
        @Suspended final AsyncResponse asyncResponse,
        @ApiParam(value = "The cluster name", required = true)
        @PathParam("cluster") String cluster,
        @ApiParam(value = "The namespace isolation policy name", required = true)
        @PathParam("policyName") String policyName,
        @ApiParam(value = "The namespace isolation policy data", required = true)
        NamespaceIsolationDataImpl policyData
    ) {
        validateBothSuperuserAndClusterPolicyOperation(cluster, PolicyName.NAMESPACE_ISOLATION, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> validateClusterExistAsync(cluster, PRECONDITION_FAILED))
                .thenCompose(__ -> {
                    // validate the policy data before creating the node
                    policyData.validate();
                    return namespaceIsolationPolicies().getIsolationDataPoliciesAsync(cluster);
                }).thenCompose(nsIsolationPoliciesOpt ->
                        nsIsolationPoliciesOpt.map(CompletableFuture::completedFuture)
                                .orElseGet(() -> namespaceIsolationPolicies()
                                        .setIsolationDataWithCreateAsync(cluster, (p) -> Collections.emptyMap())
                                        .thenApply(__ -> new NamespaceIsolationPolicies()))
                ).thenCompose(nsIsolationPolicies -> {
                    NamespaceIsolationDataImpl oldPolicy = nsIsolationPolicies
                            .getPolicies().getOrDefault(policyName, null);
                    nsIsolationPolicies.setPolicy(policyName, policyData);
                    return namespaceIsolationPolicies()
                            .setIsolationDataAsync(cluster, old -> nsIsolationPolicies.getPolicies())
                            .thenApply(__ -> oldPolicy);
                }).thenCompose(oldPolicy -> filterAndUnloadMatchedNamespaceAsync(cluster, policyData, oldPolicy))
                .thenAccept(__ -> {
                    log.info("[{}] Successful to update clusters/{}/namespaceIsolationPolicies/{}.",
                            clientAppId(), cluster, policyName);
                    asyncResponse.resume(Response.noContent().build());
                }).exceptionally(ex -> {
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    if (realCause instanceof IllegalArgumentException) {
                        String jsonData;
                        try {
                            jsonData = JsonUtil.toJson(policyData);
                        } catch (JsonUtil.ParseJsonException e) {
                            jsonData = "[Failed to serialize]";
                        }
                        asyncResponse.resume(new RestException(Status.BAD_REQUEST,
                                "Invalid format of input policy data. policy: " + policyName + "; data: " + jsonData));
                        return null;
                    } else if (realCause instanceof NotFoundException) {
                        log.warn("[{}] Failed to update clusters/{}/namespaceIsolationPolicies: Does not exist",
                                clientAppId(), cluster);
                        asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                "NamespaceIsolationPolicies for cluster " + cluster + " does not exist"));
                        return null;
                    }
                    log.info("[{}] Failed to update clusters/{}/namespaceIsolationPolicies/{}. Input data is invalid",
                            clientAppId(), cluster, policyName, realCause);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    /**
     * Get matched namespaces; call unload for each namespaces.
     */
    private CompletableFuture<Void> filterAndUnloadMatchedNamespaceAsync(String cluster,
                                                                         NamespaceIsolationDataImpl policyData,
                                                                         NamespaceIsolationDataImpl oldPolicy) {
        // exit early if none of the namespaces need to be unloaded
        if (NamespaceIsolationPolicyUnloadScope.none.equals(policyData.getUnloadScope())) {
            return CompletableFuture.completedFuture(null);
        }

        PulsarAdmin adminClient;
        try {
            adminClient = pulsar().getAdminClient();
        } catch (PulsarServerException e) {
            return FutureUtil.failedFuture(e);
        }
        Set<String> combinedNamespaces = new HashSet<>(policyData.getNamespaces());
        final List<String> oldNamespaces = new ArrayList<>();
        if (oldPolicy != null) {
            oldNamespaces.addAll(oldPolicy.getNamespaces());
            combinedNamespaces.addAll(oldNamespaces);
        }
        return adminClient.tenants().getTenantsAsync().thenCompose(tenants -> {
            List<CompletableFuture<List<String>>> filteredNamespacesForEachTenant = tenants.stream()
                    .map(tenant -> adminClient.namespaces().getNamespacesAsync(tenant).thenCompose(namespaces -> {
                        List<CompletableFuture<String>> namespaceNamesInCluster = namespaces.stream()
                                .map(namespaceName -> adminClient.namespaces().getPoliciesAsync(namespaceName)
                                        .thenApply(policies -> policies.replication_clusters.contains(cluster)
                                                ? namespaceName : null))
                                .collect(Collectors.toList());
                        return FutureUtil.waitForAll(namespaceNamesInCluster).thenApply(
                                __ -> namespaceNamesInCluster.stream()
                                        .map(CompletableFuture::join)
                                        .filter(Objects::nonNull)
                                        .collect(Collectors.toList()));
                    })).toList();
            return FutureUtil.waitForAll(filteredNamespacesForEachTenant)
                    .thenApply(__ -> filteredNamespacesForEachTenant.stream()
                            .map(CompletableFuture::join)
                            .flatMap(List::stream)
                            .collect(Collectors.toList()));
        }).thenCompose(clusterLocalNamespaces -> {
            if (CollectionUtils.isEmpty(clusterLocalNamespaces)) {
                return CompletableFuture.completedFuture(null);
            }
            // If unload type is 'changed', we need to figure out a further subset of namespaces whose placement might
            // actually have been changed.

            log.debug("Old policy: {} ; new policy: {}", oldPolicy, policyData);

            boolean unloadAllNamespaces = false;
            // We also compare that the previous primary broker list is same as current, in case all namespaces need
            // to be placed again anyway.
            if (NamespaceIsolationPolicyUnloadScope.all_matching.equals(policyData.getUnloadScope())
                    || (oldPolicy != null
                    && !CollectionUtils.isEqualCollection(oldPolicy.getPrimary(), policyData.getPrimary()))) {
                unloadAllNamespaces = true;
            }
            // list is same, so we continue finding the changed namespaces.

            // We create a intersection of the old and new regexes. These won't need to be unloaded.
            Set<String> commonNamespaces = new HashSet<>(policyData.getNamespaces());
            commonNamespaces.retainAll(oldNamespaces);

            log.debug("combined regexes: {}; common regexes:{}", combinedNamespaces, commonNamespaces);

            if (!unloadAllNamespaces) {
                // Find the changed regexes ((new U old) - (new ∩ old)).
                combinedNamespaces.removeAll(commonNamespaces);
                log.debug("changed regexes: {}", commonNamespaces);
            }

            // Now we further filter the filtered namespaces based on this combinedNamespaces set
            List<Pattern> namespacePatterns = combinedNamespaces.stream().map(Pattern::compile).toList();
            clusterLocalNamespaces = clusterLocalNamespaces.stream()
                    .filter(name -> namespacePatterns.stream().anyMatch(pattern -> pattern.matcher(name).matches()))
                    .toList();

            List<CompletableFuture<Void>> futures = clusterLocalNamespaces.stream()
                    .map(namespaceName -> adminClient.namespaces().unloadAsync(namespaceName))
                    .collect(Collectors.toList());
            return FutureUtil.waitForAll(futures).thenAccept(__ -> {
                try {
                    // write load info to load manager to make the load happens fast
                    pulsar().getLoadManager().get().writeLoadReportOnZookeeper(true);
                } catch (Exception e) {
                    log.warn("[{}] Failed to writeLoadReportOnZookeeper.", clientAppId(), e);
                }
            });
        });
    }

    @DELETE
    @Path("/{cluster}/namespaceIsolationPolicies/{policyName}")
    @ApiOperation(
        value = "Delete namespace isolation policy.",
        notes = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(code = 204, message = "Delete namespace isolation policy successfully."),
        @ApiResponse(code = 403, message = "Don't have admin permission or policies are read only."),
        @ApiResponse(code = 404, message = "Namespace isolation policy doesn't exist."),
        @ApiResponse(code = 412, message = "Cluster doesn't exist."),
        @ApiResponse(code = 500, message = "Internal server error.")
    })
    public void deleteNamespaceIsolationPolicy(
        @Suspended AsyncResponse asyncResponse,
        @ApiParam(value = "The cluster name", required = true)
        @PathParam("cluster") String cluster,
        @ApiParam(value = "The namespace isolation policy name", required = true)
        @PathParam("policyName") String policyName
    ) {
        validateBothSuperuserAndClusterPolicyOperation(cluster, PolicyName.NAMESPACE_ISOLATION, PolicyOperation.WRITE)
                .thenCompose(__ -> validateClusterExistAsync(cluster, PRECONDITION_FAILED))
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> namespaceIsolationPolicies().getIsolationDataPoliciesAsync(cluster))
                .thenCompose(nsIsolationPoliciesOpt -> nsIsolationPoliciesOpt.map(CompletableFuture::completedFuture)
                        .orElseGet(() -> namespaceIsolationPolicies()
                                .setIsolationDataWithCreateAsync(cluster, (p) -> Collections.emptyMap())
                                .thenApply(__ -> new NamespaceIsolationPolicies())))
                .thenCompose(policies -> {
                    policies.deletePolicy(policyName);
                    return namespaceIsolationPolicies().setIsolationDataAsync(cluster, old -> policies.getPolicies());
                }).thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    if (realCause instanceof NotFoundException) {
                        log.warn("[{}] Failed to update brokers/{}/namespaceIsolationPolicies: Does not exist",
                                clientAppId(), cluster);
                        asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                "NamespaceIsolationPolicies for cluster " + cluster + " does not exist"));
                        return null;
                    }
                    log.error("[{}] Failed to update brokers/{}/namespaceIsolationPolicies/{}", clientAppId(), cluster,
                            policyName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{cluster}/failureDomains/{domainName}")
    @ApiOperation(
        value = "Set the failure domain of the cluster.",
        notes = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(code = 204, message = "Set the failure domain of the cluster successfully."),
        @ApiResponse(code = 403, message = "Don't have admin permission."),
        @ApiResponse(code = 404, message = "Failure domain doesn't exist."),
        @ApiResponse(code = 409, message = "Broker already exists in another domain."),
        @ApiResponse(code = 412, message = "Cluster doesn't exist."),
        @ApiResponse(code = 500, message = "Internal server error.")
    })
    public void setFailureDomain(
        @Suspended AsyncResponse asyncResponse,
        @ApiParam(value = "The cluster name", required = true)
        @PathParam("cluster") String cluster,
        @ApiParam(value = "The failure domain name", required = true)
        @PathParam("domainName") String domainName,
        @ApiParam(value = "The configuration data of a failure domain", required = true) FailureDomainImpl domain
    ) {
        validateBothSuperuserAndClusterOperation(cluster, ClusterOperation.UPDATE_FAILURE_DOMAIN)
                .thenCompose(__ -> validateClusterExistAsync(cluster, PRECONDITION_FAILED))
                .thenCompose(__ -> validateBrokerExistsInOtherDomain(cluster, domainName, domain))
                .thenCompose(__ -> clusterResources().getFailureDomainResources()
                        .setFailureDomainWithCreateAsync(cluster, domainName, old -> domain))
                .thenAccept(__ -> {
                    log.info("[{}] Successful set failure domain {} for cluster {}",
                            clientAppId(), domainName, cluster);
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    if (realCause instanceof NotFoundException) {
                        log.warn("[{}] Failed to update domain {}. clusters {}  Does not exist", clientAppId(), cluster,
                                domainName);
                        asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                "Domain " + domainName + " for cluster " + cluster + " does not exist"));
                        return null;
                    }
                    log.error("[{}] Failed to update clusters/{}/domainName/{}",
                            clientAppId(), cluster, domainName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
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
    public void getFailureDomains(
        @Suspended AsyncResponse asyncResponse,
        @ApiParam(value = "The cluster name", required = true)
        @PathParam("cluster") String cluster
    ) {
        validateBothSuperuserAndClusterOperation(cluster, ClusterOperation.GET_FAILURE_DOMAIN)
                .thenCompose(__ -> clusterResources().getFailureDomainResources()
                        .listFailureDomainsAsync(cluster)
                        .thenCompose(domainNames -> {
                            List<CompletableFuture<Pair<String, Optional<FailureDomainImpl>>>> futures =
                                domainNames.stream()
                                    .map(domainName -> clusterResources().getFailureDomainResources()
                                            .getFailureDomainAsync(cluster, domainName)
                                            .thenApply(failureDomainImpl -> Pair.of(domainName, failureDomainImpl))
                                            .exceptionally(ex -> {
                                                log.warn("Failed to get domain {}", domainName, ex);
                                                return null;
                                            })).collect(Collectors.toList());
                            return FutureUtil.waitForAll(futures)
                                    .thenApply(unused -> futures.stream()
                                            .map(CompletableFuture::join)
                                            .filter(Objects::nonNull)
                                            .filter(v -> v.getRight().isPresent())
                                            .collect(Collectors.toMap(Pair::getLeft, v -> v.getRight().get())));
                        }).exceptionally(ex -> {
                            Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                            if (realCause instanceof NotFoundException) {
                                log.warn("[{}] Failure-domain is not configured for cluster {}",
                                        clientAppId(), cluster, ex);
                                return Collections.emptyMap();
                            }
                            throw FutureUtil.wrapToCompletionException(ex);
                        })
                ).thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get failure-domains for cluster {}", clientAppId(), cluster, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
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
    public void getDomain(
        @Suspended AsyncResponse asyncResponse,
        @ApiParam(value = "The cluster name", required = true)
        @PathParam("cluster") String cluster,
        @ApiParam(value = "The failure domain name", required = true)
        @PathParam("domainName") String domainName
    ) {
        validateBothSuperuserAndClusterOperation(cluster, ClusterOperation.GET_FAILURE_DOMAIN)
                .thenCompose(__ -> validateClusterExistAsync(cluster, PRECONDITION_FAILED))
                .thenCompose(__ -> clusterResources().getFailureDomainResources()
                        .getFailureDomainAsync(cluster, domainName))
                .thenAccept(domain -> {
                    FailureDomainImpl failureDomain = domain.orElseThrow(() -> new RestException(Status.NOT_FOUND,
                            "Domain " + domainName + " for cluster " + cluster + " does not exist"));
                    asyncResponse.resume(failureDomain);
                }).exceptionally(ex -> {
                    log.error("[{}] Failed to get domain {} for cluster {}", clientAppId(), domainName, cluster, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{cluster}/failureDomains/{domainName}")
    @ApiOperation(
        value = "Delete the failure domain of the cluster",
        notes = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "Delete the failure domain of the cluster successfully"),
        @ApiResponse(code = 403, message = "Don't have admin permission or policy is read only"),
        @ApiResponse(code = 404, message = "FailureDomain doesn't exist"),
        @ApiResponse(code = 412, message = "Cluster doesn't exist"),
        @ApiResponse(code = 500, message = "Internal server error")
    })
    public void deleteFailureDomain(
        @Suspended AsyncResponse asyncResponse,
        @ApiParam(value = "The cluster name", required = true)
        @PathParam("cluster") String cluster,
        @ApiParam(value = "The failure domain name", required = true)
        @PathParam("domainName") String domainName
    ) {
        validateBothSuperuserAndClusterOperation(cluster, ClusterOperation.DELETE_FAILURE_DOMAIN)
                .thenCompose(__ -> validateClusterExistAsync(cluster, PRECONDITION_FAILED))
                .thenCompose(__ -> clusterResources()
                        .getFailureDomainResources().deleteFailureDomainAsync(cluster, domainName))
                .thenAccept(__ -> {
                    log.info("[{}] Successful delete domain {} in cluster {}", clientAppId(), domainName, cluster);
                    asyncResponse.resume(Response.ok().build());
                }).exceptionally(ex -> {
                    Throwable cause = FutureUtil.unwrapCompletionException(ex);
                    if (cause instanceof NotFoundException) {
                        log.warn("[{}] Domain {} does not exist in {}", clientAppId(), domainName, cluster);
                        asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                "Domain-name " + domainName + " or cluster " + cluster + " does not exist"));
                        return null;
                    }
                    log.error("[{}] Failed to delete domain {} in cluster {}", clientAppId(), domainName, cluster, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    private CompletableFuture<Void> validateBrokerExistsInOtherDomain(final String cluster,
                                                                      final String inputDomainName,
                                                                      final FailureDomainImpl inputDomain) {
        if (inputDomain == null || inputDomain.brokers == null) {
            return CompletableFuture.completedFuture(null);
        }
        return clusterResources().getFailureDomainResources()
                .listFailureDomainsAsync(cluster)
                .thenCompose(domainNames -> {
                    List<CompletableFuture<Void>> futures = domainNames.stream()
                            .filter(domainName -> !domainName.equals(inputDomainName))
                            .map(domainName -> clusterResources()
                                    .getFailureDomainResources().getFailureDomainAsync(cluster, domainName)
                                    .thenAccept(failureDomainOpt -> {
                                        if (failureDomainOpt.isPresent()
                                                && CollectionUtils.isNotEmpty(failureDomainOpt.get().getBrokers())) {
                                            List<String> duplicateBrokers = failureDomainOpt.get()
                                                    .getBrokers().stream().parallel()
                                                    .filter(inputDomain.brokers::contains)
                                                    .collect(Collectors.toList());
                                            if (CollectionUtils.isNotEmpty(duplicateBrokers)) {
                                                throw new RestException(Status.CONFLICT,
                                                        duplicateBrokers + " already exists in " + domainName);
                                            }
                                        }
                                    }).exceptionally(ex -> {
                                        Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                                        if (realCause instanceof WebApplicationException) {
                                            throw FutureUtil.wrapToCompletionException(ex);
                                        }
                                        if (realCause instanceof NotFoundException) {
                                            if (log.isDebugEnabled()) {
                                                log.debug("[{}] Domain is not configured for cluster",
                                                        clientAppId(), ex);
                                            }
                                            return null;
                                        }
                                        log.warn("Failed to get domain {}", domainName, ex);
                                        return null;
                                    })
                            ).collect(Collectors.toList());
                    return FutureUtil.waitForAll(futures);
                });
    }



    private CompletableFuture<Void> validateBothSuperuserAndClusterOperation(String clusterName,
                                                                             ClusterOperation operation) {
        final var superUserAccessValidation = validateSuperUserAccessAsync();
        final var clusterOperationValidation = validateClusterOperationAsync(clusterName, operation);
        return FutureUtil.waitForAll(List.of(superUserAccessValidation, clusterOperationValidation))
                .handle((result, err) -> {
                    if (!superUserAccessValidation.isCompletedExceptionally()
                        || !clusterOperationValidation.isCompletedExceptionally()) {
                        return null;
                    }
                    if (log.isDebugEnabled()) {
                        Throwable superUserValidationException = null;
                        try {
                            superUserAccessValidation.join();
                        } catch (Throwable ex) {
                            superUserValidationException = FutureUtil.unwrapCompletionException(ex);
                        }
                        Throwable clusterOperationValidationException = null;
                        try {
                            clusterOperationValidation.join();
                        } catch (Throwable ex) {
                            clusterOperationValidationException = FutureUtil.unwrapCompletionException(ex);
                        }
                        log.debug("validateBothSuperuserAndClusterOperation failed."
                                  + " originalPrincipal={} clientAppId={} operation={} cluster={} "
                                  + "superuserValidationError={} clusterOperationValidationError={}",
                                originalPrincipal(), clientAppId(), operation.toString(), clusterName,
                                superUserValidationException, clusterOperationValidationException);
                    }
                    throw new RestException(Status.UNAUTHORIZED,
                            String.format("Unauthorized to validateBothSuperuserAndClusterOperation for"
                                          + " originalPrincipal [%s] and clientAppId [%s] "
                                          + "about operation [%s] on cluster [%s]",
                                    originalPrincipal(), clientAppId(), operation.toString(), clusterName));
                });
    }


    private CompletableFuture<Void> validateBothSuperuserAndClusterPolicyOperation(String clusterName, PolicyName name,
                                                                                   PolicyOperation operation) {
        final var superUserAccessValidation = validateSuperUserAccessAsync();
        final var clusterOperationValidation = validateClusterPolicyOperationAsync(clusterName, name, operation);
        return FutureUtil.waitForAll(List.of(superUserAccessValidation, clusterOperationValidation))
                .handle((result, err) -> {
                    if (!superUserAccessValidation.isCompletedExceptionally()
                        || !clusterOperationValidation.isCompletedExceptionally()) {
                        return null;
                    }
                    if (log.isDebugEnabled()) {
                        Throwable superUserValidationException = null;
                        try {
                            superUserAccessValidation.join();
                        } catch (Throwable ex) {
                            superUserValidationException = FutureUtil.unwrapCompletionException(ex);
                        }
                        Throwable clusterOperationValidationException = null;
                        try {
                            clusterOperationValidation.join();
                        } catch (Throwable ex) {
                            clusterOperationValidationException = FutureUtil.unwrapCompletionException(ex);
                        }
                        log.debug("validateBothSuperuserAndClusterPolicyOperation failed."
                                  + " originalPrincipal={} clientAppId={} operation={} cluster={} "
                                  + "superuserValidationError={} clusterOperationValidationError={}",
                                originalPrincipal(), clientAppId(), operation.toString(), clusterName,
                                superUserValidationException, clusterOperationValidationException);
                    }
                    throw new RestException(Status.UNAUTHORIZED,
                            String.format("Unauthorized to validateBothSuperuserAndClusterPolicyOperation for"
                                          + " originalPrincipal [%s] and clientAppId [%s] "
                                          + "about operation [%s] on cluster [%s]",
                                    originalPrincipal(), clientAppId(), operation.toString(), clusterName));
                });
    }




    private CompletableFuture<Void> validateClusterOperationAsync(String cluster, ClusterOperation operation) {
        final var pulsar = pulsar();
        if (pulsar.getBrokerService().isAuthenticationEnabled()
            && pulsar.getBrokerService().isAuthorizationEnabled()) {
            return pulsar.getBrokerService().getAuthorizationService()
                    .allowClusterOperationAsync(cluster, operation, originalPrincipal(),
                            clientAppId(), clientAuthData())
                    .thenAccept(isAuthorized -> {
                        if (!isAuthorized) {
                            throw new RestException(Status.UNAUTHORIZED,
                                    String.format("Unauthorized to validateClusterOperation for"
                                                  + " originalPrincipal [%s] and clientAppId [%s] "
                                                  + "about operation [%s] on cluster [%s]",
                                            originalPrincipal(), clientAppId(), operation.toString(), cluster));
                        }
                    });
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> validateClusterPolicyOperationAsync(String cluster, PolicyName policyName,
                                                                        PolicyOperation operation) {
        final var pulsar = pulsar();
        if (pulsar.getBrokerService().isAuthenticationEnabled()
            && pulsar.getBrokerService().isAuthorizationEnabled()) {
            return pulsar.getBrokerService().getAuthorizationService()
                    .allowClusterPolicyOperationAsync(cluster, policyName, operation, originalPrincipal(),
                            clientAppId(), clientAuthData())
                    .thenAccept(isAuthorized -> {
                        if (!isAuthorized) {
                            throw new RestException(Status.UNAUTHORIZED,
                                    String.format("Unauthorized to validateClusterPolicyOperation for"
                                                  + " originalPrincipal [%s] and clientAppId [%s] "
                                                  + "about operation [%s] on cluster [%s]",
                                            originalPrincipal(), clientAppId(), operation.toString(), cluster));
                        }
                    });
        }
        return CompletableFuture.completedFuture(null);
    }

    private static final Logger log = LoggerFactory.getLogger(ClustersBase.class);
}
