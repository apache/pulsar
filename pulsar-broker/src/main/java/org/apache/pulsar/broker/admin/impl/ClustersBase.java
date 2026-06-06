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

import static jakarta.ws.rs.core.Response.Status.PRECONDITION_FAILED;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
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
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.NamedEntity;
import org.apache.pulsar.common.naming.NamespaceName;
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

public class ClustersBase extends AdminResource {

    @GET
    @Operation(summary = "Get the list of all the Pulsar clusters.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Return a list of clusters.",
                    content = @Content(array = @ArraySchema(
                            schema = @Schema(implementation = String.class), uniqueItems = true))),
            @ApiResponse(responseCode = "500", description = "Internal server error.")
    })
    public void getClusters(@Suspended AsyncResponse asyncResponse) {
        clusterResources().listAsync()
                .thenApply(HashSet::new)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error().exception(ex).log("Failed to get clusters");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{cluster}")
    @Operation(
        summary = "Get the configuration for the specified cluster.",
        description = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Return the cluster data.",
                    content = @Content(schema = @Schema(implementation = ClusterDataImpl.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission."),
            @ApiResponse(responseCode = "404", description = "Cluster doesn't exist."),
            @ApiResponse(responseCode = "500", description = "Internal server error.")
    })
    public void getCluster(@Suspended AsyncResponse asyncResponse,
                           @Parameter(description = "The cluster name", required = true)
                           @PathParam("cluster") String cluster) {
        validateBothSuperuserAndClusterOperation(cluster, ClusterOperation.GET_CLUSTER)
                .thenCompose(__ -> clusterResources().getClusterAsync(cluster))
                .thenAccept(clusterData -> {
                    asyncResponse.resume(clusterData
                            .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Cluster does not exist")));
                }).exceptionally(ex -> {
                    log.error()
                            .attr("cluster", cluster)
                            .exception(ex)
                            .log("Failed to get cluster");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{cluster}")
    @Operation(
        summary = "Create a new cluster.",
        description = "This operation requires Pulsar superuser privileges, and the name cannot contain the '/'"
                + " characters."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Cluster has been created."),
            @ApiResponse(responseCode = "400", description = "Bad request parameter."),
            @ApiResponse(responseCode = "403", description = "You don't have admin permission to create the cluster."),
            @ApiResponse(responseCode = "409", description = "Cluster already exists."),
            @ApiResponse(responseCode = "412", description = "Cluster name is not valid."),
            @ApiResponse(responseCode = "500", description = "Internal server error.")
    })
    public void createCluster(
        @Suspended AsyncResponse asyncResponse,
        @Parameter(description = "The cluster name", required = true)
        @PathParam("cluster") String cluster,
        @RequestBody(
            description = "The cluster data",
            required = true,
            content = @Content(
                mediaType = MediaType.APPLICATION_JSON,
                examples = @ExampleObject(
                    value = """
                            {
                               "serviceUrl": "http://pulsar.example.com:8080",
                               "brokerServiceUrl": "pulsar://pulsar.example.com:6651"
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
                    log.info().attr("cluster", cluster).log("Created cluster");
                    asyncResponse.resume(Response.ok().build());
                }).exceptionally(ex -> {
                    log.error()
                            .attr("cluster", cluster)
                            .exception(ex)
                            .log("Failed to create cluster");
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
    @Operation(
        summary = "Update the configuration for a cluster.",
        description = "This operation requires Pulsar superuser privileges.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Cluster has been updated."),
            @ApiResponse(responseCode = "400", description = "Bad request parameter."),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission or policies are read-only."),
            @ApiResponse(responseCode = "404", description = "Cluster doesn't exist."),
            @ApiResponse(responseCode = "500", description = "Internal server error.")
    })
    public void updateCluster(
        @Suspended AsyncResponse asyncResponse,
        @Parameter(description = "The cluster name", required = true)
        @PathParam("cluster") String cluster,
        @RequestBody(
            description = "The cluster data",
            required = true,
            content = @Content(
                mediaType = MediaType.APPLICATION_JSON,
                examples = @ExampleObject(
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
                    log.info().attr("cluster", cluster).log("Updated cluster");
                    asyncResponse.resume(Response.ok().build());
                }).exceptionally(ex -> {
                    log.error()
                            .attr("cluster", cluster)
                            .exception(ex)
                            .log("Failed to update cluster");
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
    @Operation(
        summary = "Get the cluster migration configuration for the specified cluster.",
        description = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Return the cluster data.",
                    content = @Content(schema = @Schema(implementation = ClusterDataImpl.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission."),
            @ApiResponse(responseCode = "404", description = "Cluster doesn't exist."),
            @ApiResponse(responseCode = "500", description = "Internal server error.")
    })
    public void getClusterMigration(
        @Suspended AsyncResponse asyncResponse,
        @Parameter(
            description = "The cluster name",
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
                    log.error()
                            .attr("cluster", cluster)
                            .exception(ex)
                            .log("Failed to get cluster migration");
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
    @Operation(
        summary = "Update the configuration for a cluster migration.",
        description = "This operation requires Pulsar superuser privileges.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Cluster has been updated."),
            @ApiResponse(responseCode = "400", description = "Cluster url must not be empty."),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission or policies are read-only."),
            @ApiResponse(responseCode = "404", description = "Cluster doesn't exist."),
            @ApiResponse(responseCode = "500", description = "Internal server error.")
    })
    public void updateClusterMigration(
        @Suspended AsyncResponse asyncResponse,
        @Parameter(description = "The cluster name", required = true)
        @PathParam("cluster") String cluster,
        @Parameter(description = "Is cluster migrated", required = true)
        @QueryParam("migrated") boolean isMigrated,
        @RequestBody(
            description = "The cluster url data",
            required = true,
            content = @Content(
                mediaType = MediaType.APPLICATION_JSON,
                examples = @ExampleObject(
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
                    log.info().attr("cluster", cluster).log("Updated cluster");
                    asyncResponse.resume(Response.ok().build());
                }).exceptionally(ex -> {
                    log.error()
                            .attr("cluster", cluster)
                            .exception(ex)
                            .log("Failed to update cluster");
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
    @Operation(
        summary = "Update peer-cluster-list for a cluster.",
        description = "This operation requires Pulsar superuser privileges.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Cluster has been updated."),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission or policies are read-only."),
            @ApiResponse(responseCode = "404", description = "Cluster doesn't exist."),
            @ApiResponse(responseCode = "412", description = "Peer cluster doesn't exist."),
            @ApiResponse(responseCode = "500", description = "Internal server error.")
    })
    public void setPeerClusterNames(@Suspended AsyncResponse asyncResponse,
                                    @Parameter(description = "The cluster name", required = true)
                                    @PathParam("cluster") String cluster,
                                    @RequestBody(
                                        description = "The list of peer cluster names",
                                        required = true,
                                        content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                        examples = @ExampleObject(value = """
                                                [
                                                   "cluster-a",
                                                   "cluster-b"
                                                ]""")))
                                    LinkedHashSet<String> peerClusterNames) {
        validateBothSuperuserAndClusterOperation(cluster, ClusterOperation.UPDATE_PEER_CLUSTER)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> innerSetPeerClusterNamesAsync(cluster, peerClusterNames))
                .thenAccept(__ -> {
                    log.info()
                            .attr("peerClusters", peerClusterNames)
                            .attr("cluster", cluster)
                            .log("Successfully added peer-cluster");
                    asyncResponse.resume(Response.noContent().build());
                }).exceptionally(ex -> {
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    log.error()
                            .attr("peerClusters", peerClusterNames)
                            .exceptionMessage(ex)
                            .log("Failed to validate peer-cluster list");
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
    @Operation(
            summary = "Get the peer-cluster data for the specified cluster.",
            description = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the peer-cluster data for the specified cluster.",
                    content = @Content(array = @ArraySchema(
                            schema = @Schema(implementation = String.class), uniqueItems = true))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission."),
            @ApiResponse(responseCode = "404", description = "Cluster doesn't exist."),
            @ApiResponse(responseCode = "500", description = "Internal server error.")
    })
    public void getPeerCluster(@Suspended AsyncResponse asyncResponse,
                               @Parameter(description = "The cluster name", required = true)
                               @PathParam("cluster") String cluster) {
        validateBothSuperuserAndClusterOperation(cluster, ClusterOperation.GET_PEER_CLUSTER)
                .thenCompose(__ -> clusterResources().getClusterAsync(cluster))
                .thenAccept(clusterOpt -> {
                    ClusterData clusterData =
                            clusterOpt.orElseThrow(() -> new RestException(Status.NOT_FOUND, "Cluster does not exist"));
                    asyncResponse.resume(clusterData.getPeerClusterNames());
                }).exceptionally(ex -> {
                    log.error()
                            .attr("cluster", cluster)
                            .exception(ex)
                            .log("Failed to get cluster");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{cluster}")
    @Operation(
        summary = "Delete an existing cluster.",
        description = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Cluster has been deleted."),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission or policies are read-only."),
            @ApiResponse(responseCode = "404", description = "Cluster doesn't exist."),
            @ApiResponse(responseCode = "412", description = "Cluster is not empty."),
            @ApiResponse(responseCode = "500", description = "Internal server error.")
    })
    public void deleteCluster(@Suspended AsyncResponse asyncResponse,
                              @Parameter(description = "The cluster name", required = true)
                              @PathParam("cluster") String cluster) {
        validateBothSuperuserAndClusterOperation(cluster, ClusterOperation.DELETE_CLUSTER)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> internalDeleteClusterAsync(cluster))
                .thenAccept(__ -> {
                    log.info().attr("cluster", cluster).log("Deleted cluster");
                    asyncResponse.resume(Response.noContent().build());
                }).exceptionally(ex -> {
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    if (realCause instanceof NotFoundException) {
                        log.warn()
                                .attr("cluster", cluster)
                                .log("Failed to delete cluster - Does not exist");
                        asyncResponse.resume(new RestException(Status.NOT_FOUND, "Cluster does not exist"));
                        return null;
                    }
                    log.error()
                            .attr("cluster", cluster)
                            .exception(ex)
                            .log("Failed to delete cluster");
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
    @Operation(
        summary = "Get the namespace isolation policies assigned to the cluster.",
        description = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Get the namespace isolation policies assigned to the cluster.",
                    content = @Content(schema = @Schema(type = "object",
                            additionalPropertiesSchema = NamespaceIsolationDataImpl.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission."),
            @ApiResponse(responseCode = "404", description = "Cluster doesn't exist."),
            @ApiResponse(responseCode = "500", description = "Internal server error.")
    })
    public void getNamespaceIsolationPolicies(
        @Suspended AsyncResponse asyncResponse,
        @Parameter(description = "The cluster name", required = true) @PathParam("cluster") String cluster
    ) {
        validateBothSuperuserAndClusterPolicyOperation(cluster, PolicyName.NAMESPACE_ISOLATION, PolicyOperation.READ)
                .thenCompose(__ -> validateClusterExistAsync(cluster, Status.NOT_FOUND))
                .thenCompose(__ -> internalGetNamespaceIsolationPolicies(cluster))
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error()
                            .attr("cluster", cluster)
                            .exception(ex)
                            .log("Failed to get namespace isolation policies");
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
    @Operation(
            summary = "Get the single namespace isolation policy assigned to the cluster.",
            description = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Get the single namespace isolation policy assigned to the cluster.",
                    content = @Content(schema = @Schema(implementation = NamespaceIsolationDataImpl.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission."),
            @ApiResponse(responseCode = "404", description = "Policy doesn't exist."),
            @ApiResponse(responseCode = "412", description = "Cluster doesn't exist."),
            @ApiResponse(responseCode = "500", description = "Internal server error.")
    })
    public void getNamespaceIsolationPolicy(
        @Suspended AsyncResponse asyncResponse,
        @Parameter(description = "The cluster name", required = true) @PathParam("cluster") String cluster,
        @Parameter(description = "The name of the namespace isolation policy", required = true)
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
                    log.error()
                            .attr("cluster", cluster)
                            .exception(ex)
                            .log("Failed to get namespace isolation policies");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{cluster}/namespaceIsolationPolicies/brokers")
    @Operation(
        summary = "Get list of brokers with namespace-isolation policies attached to them.",
        description = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
                description = "Get list of brokers with namespace-isolation policies attached to them.",
                content = @Content(array = @ArraySchema(
                        schema = @Schema(implementation = BrokerNamespaceIsolationDataImpl.class),
                        uniqueItems = true))),
        @ApiResponse(responseCode = "403", description = "Don't have admin permission."),
        @ApiResponse(responseCode = "404", description = "Namespace-isolation policies not found."),
        @ApiResponse(responseCode = "412", description = "Cluster doesn't exist."),
        @ApiResponse(responseCode = "500", description = "Internal server error.")
    })
    public void getBrokersWithNamespaceIsolationPolicy(
            @Suspended AsyncResponse asyncResponse,
            @Parameter(description = "The cluster name", required = true)
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
                    log.error()
                            .attr("cluster", cluster)
                            .exception(ex)
                            .log("Failed to get namespace isolation policies");
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
    @Operation(
        summary = "Get a broker with namespace-isolation policies attached to it.",
        description = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
                description = "Get a broker with namespace-isolation policies attached to it.",
                content = @Content(schema = @Schema(implementation = BrokerNamespaceIsolationDataImpl.class))),
        @ApiResponse(responseCode = "403", description = "Don't have admin permission."),
        @ApiResponse(responseCode = "404", description = "Namespace-isolation policies/ Broker not found."),
        @ApiResponse(responseCode = "412", description = "Cluster doesn't exist."),
        @ApiResponse(responseCode = "500", description = "Internal server error.")
    })
    public void getBrokerWithNamespaceIsolationPolicy(
        @Suspended AsyncResponse asyncResponse,
        @Parameter(description = "The cluster name", required = true)
        @PathParam("cluster") String cluster,
        @Parameter(description = "The broker name (<broker-hostname>:<web-service-port>)", required = true,
            example = "broker1:8080")
        @PathParam("broker") String broker) {
        validateBothSuperuserAndClusterPolicyOperation(cluster, PolicyName.NAMESPACE_ISOLATION, PolicyOperation.READ)
                .thenCompose(__ -> validateClusterExistAsync(cluster, PRECONDITION_FAILED))
                .thenCompose(__ -> internalGetNamespaceIsolationPolicies(cluster))
                .thenApply(policies -> internalGetBrokerNsIsolationData(broker, policies))
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error()
                            .attr("cluster", cluster)
                            .attr("broker", broker)
                            .exception(ex)
                            .log("Failed to get namespace isolation policies");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{cluster}/namespaceIsolationPolicies/{policyName}")
    @Operation(
        summary = "Set namespace isolation policy.",
        description = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "204", description = "Set namespace isolation policy successfully."),
        @ApiResponse(responseCode = "400", description = "Namespace isolation policy data is invalid."),
        @ApiResponse(responseCode = "403", description = "Don't have admin permission or policies are read-only."),
        @ApiResponse(responseCode = "404", description = "Namespace isolation policy doesn't exist."),
        @ApiResponse(responseCode = "412", description = "Cluster doesn't exist."),
        @ApiResponse(responseCode = "500", description = "Internal server error.")
    })
    public void setNamespaceIsolationPolicy(
        @Suspended final AsyncResponse asyncResponse,
        @Parameter(description = "The cluster name", required = true)
        @PathParam("cluster") String cluster,
        @Parameter(description = "The namespace isolation policy name", required = true)
        @PathParam("policyName") String policyName,
        @RequestBody(description = "The namespace isolation policy data", required = true)
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
                    log.info()
                            .attr("cluster", cluster)
                            .attr("policy", policyName)
                            .log("Successfully updated namespace isolation policies");
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
                        log.warn()
                                .attr("cluster", cluster)
                                .log("Failed to update namespace isolation policies: Does not exist");
                        asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                "NamespaceIsolationPolicies for cluster " + cluster + " does not exist"));
                        return null;
                    }
                    log.info()
                            .attr("cluster", cluster)
                            .attr("policy", policyName)
                            .exception(realCause)
                            .log("Failed to update namespace isolation policies: input data is invalid");
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
                                    .thenApply(policies -> {
                                        boolean allowed = pulsar().getBrokerService()
                                            .isCurrentClusterAllowed(NamespaceName.get(namespaceName), policies);
                                        return allowed ? namespaceName : null;
                                    })).collect(Collectors.toList());
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

            log.debug().attr("oldPolicy", oldPolicy).attr("newPolicy", policyData).log("Old and new policy");

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

            log.debug()
                    .attr("combinedNamespaces", combinedNamespaces)
                    .attr("commonNamespaces", commonNamespaces)
                    .log("combined and common regexes");

            if (!unloadAllNamespaces) {
                // Find the changed regexes ((new U old) - (new ∩ old)).
                combinedNamespaces.removeAll(commonNamespaces);
                log.debug().attr("commonNamespaces", commonNamespaces).log("Changed regexes");
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
                    log.warn()
                            .exception(e)
                            .log("Failed to writeLoadReportOnZookeeper.");
                }
            });
        });
    }

    @DELETE
    @Path("/{cluster}/namespaceIsolationPolicies/{policyName}")
    @Operation(
        summary = "Delete namespace isolation policy.",
        description = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "204", description = "Delete namespace isolation policy successfully."),
        @ApiResponse(responseCode = "403", description = "Don't have admin permission or policies are read only."),
        @ApiResponse(responseCode = "404", description = "Namespace isolation policy doesn't exist."),
        @ApiResponse(responseCode = "412", description = "Cluster doesn't exist."),
        @ApiResponse(responseCode = "500", description = "Internal server error.")
    })
    public void deleteNamespaceIsolationPolicy(
        @Suspended AsyncResponse asyncResponse,
        @Parameter(description = "The cluster name", required = true)
        @PathParam("cluster") String cluster,
        @Parameter(description = "The namespace isolation policy name", required = true)
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
                        log.warn()
                                .attr("cluster", cluster)
                                .log("Failed to delete namespace isolation policies: Does not exist");
                        asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                "NamespaceIsolationPolicies for cluster " + cluster + " does not exist"));
                        return null;
                    }
                    log.error()
                            .attr("cluster", cluster)
                            .attr("policy", policyName)
                            .exception(ex)
                            .log("Failed to delete namespace isolation policy");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{cluster}/failureDomains/{domainName}")
    @Operation(
        summary = "Set the failure domain of the cluster.",
        description = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "204", description = "Set the failure domain of the cluster successfully."),
        @ApiResponse(responseCode = "403", description = "Don't have admin permission."),
        @ApiResponse(responseCode = "404", description = "Failure domain doesn't exist."),
        @ApiResponse(responseCode = "409", description = "Broker already exists in another domain."),
        @ApiResponse(responseCode = "412", description = "Cluster doesn't exist."),
        @ApiResponse(responseCode = "500", description = "Internal server error.")
    })
    public void setFailureDomain(
        @Suspended AsyncResponse asyncResponse,
        @Parameter(description = "The cluster name", required = true)
        @PathParam("cluster") String cluster,
        @Parameter(description = "The failure domain name", required = true)
        @PathParam("domainName") String domainName,
        @RequestBody(description = "The configuration data of a failure domain", required = true)
        FailureDomainImpl domain
    ) {
        validateBothSuperuserAndClusterOperation(cluster, ClusterOperation.UPDATE_FAILURE_DOMAIN)
                .thenCompose(__ -> validateClusterExistAsync(cluster, PRECONDITION_FAILED))
                .thenCompose(__ -> validateBrokerExistsInOtherDomain(cluster, domainName, domain))
                .thenCompose(__ -> clusterResources().getFailureDomainResources()
                        .setFailureDomainWithCreateAsync(cluster, domainName, old -> domain))
                .thenAccept(__ -> {
                    log.info()
                            .attr("domain", domainName)
                            .attr("cluster", cluster)
                            .log("Successful set failure domain for cluster");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    if (realCause instanceof NotFoundException) {
                        log.warn()
                                .attr("cluster", cluster)
                                .attr("domain", domainName)
                                .log("Failed to update failure domain: Does not exist");
                        asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                "Domain " + domainName + " for cluster " + cluster + " does not exist"));
                        return null;
                    }
                    log.error()
                            .attr("cluster", cluster)
                            .attr("domain", domainName)
                            .exception(ex)
                            .log("Failed to update failure domain");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{cluster}/failureDomains")
    @Operation(
        summary = "Get the cluster failure domains.",
        description = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Get the cluster failure domains.",
                content = @Content(schema = @Schema(type = "object",
                        additionalPropertiesSchema = FailureDomainImpl.class))),
        @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public void getFailureDomains(
        @Suspended AsyncResponse asyncResponse,
        @Parameter(description = "The cluster name", required = true)
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
                                                log.warn()
                                                        .attr("domain", domainName)
                                                        .exception(ex)
                                                        .log("Failed to get domain");
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
                                log.warn()
                                        .attr("cluster", cluster)
                                        .exception(ex)
                                        .log("Failure-domain is not configured for cluster");
                                return Collections.emptyMap();
                            }
                            throw FutureUtil.wrapToCompletionException(ex);
                        })
                ).thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error()
                            .attr("cluster", cluster)
                            .exception(ex)
                            .log("Failed to get failure-domains for cluster");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{cluster}/failureDomains/{domainName}")
    @Operation(
        summary = "Get a domain in a cluster",
        description = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Get a domain in a cluster",
                content = @Content(schema = @Schema(implementation = FailureDomainImpl.class))),
        @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
        @ApiResponse(responseCode = "404", description = "FailureDomain doesn't exist"),
        @ApiResponse(responseCode = "412", description = "Cluster doesn't exist"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public void getDomain(
        @Suspended AsyncResponse asyncResponse,
        @Parameter(description = "The cluster name", required = true)
        @PathParam("cluster") String cluster,
        @Parameter(description = "The failure domain name", required = true)
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
                    log.error()
                            .attr("domain", domainName)
                            .attr("cluster", cluster)
                            .exception(ex)
                            .log("Failed to get domain for cluster");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{cluster}/failureDomains/{domainName}")
    @Operation(
        summary = "Delete the failure domain of the cluster",
        description = "This operation requires Pulsar superuser privileges."
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Delete the failure domain of the cluster successfully"),
        @ApiResponse(responseCode = "403", description = "Don't have admin permission or policy is read only"),
        @ApiResponse(responseCode = "404", description = "FailureDomain doesn't exist"),
        @ApiResponse(responseCode = "412", description = "Cluster doesn't exist"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public void deleteFailureDomain(
        @Suspended AsyncResponse asyncResponse,
        @Parameter(description = "The cluster name", required = true)
        @PathParam("cluster") String cluster,
        @Parameter(description = "The failure domain name", required = true)
        @PathParam("domainName") String domainName
    ) {
        validateBothSuperuserAndClusterOperation(cluster, ClusterOperation.DELETE_FAILURE_DOMAIN)
                .thenCompose(__ -> validateClusterExistAsync(cluster, PRECONDITION_FAILED))
                .thenCompose(__ -> clusterResources()
                        .getFailureDomainResources().deleteFailureDomainAsync(cluster, domainName))
                .thenAccept(__ -> {
                    log.info()
                            .attr("domain", domainName)
                            .attr("cluster", cluster)
                            .log("Successful delete domain in cluster");
                    asyncResponse.resume(Response.ok().build());
                }).exceptionally(ex -> {
                    Throwable cause = FutureUtil.unwrapCompletionException(ex);
                    if (cause instanceof NotFoundException) {
                        log.warn()
                                .attr("domain", domainName)
                                .attr("cluster", cluster)
                                .log("Domain does not exist");
                        asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                "Domain-name " + domainName + " or cluster " + cluster + " does not exist"));
                        return null;
                    }
                    log.error()
                            .attr("domain", domainName)
                            .attr("cluster", cluster)
                            .exception(ex)
                            .log("Failed to delete domain in cluster");
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
                                                log.debug()
                                                        .exception(ex)
                                                        .log("Domain is not configured for cluster");
                                                                                        return null;
                                        }
                                        log.warn().attr("domain", domainName).exception(ex).log("Failed to get domain");
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
                    log.debug().attr("originalPrincipal", originalPrincipal())
                            .attr("operation", operation.toString())
                            .attr("cluster", clusterName)
                            .attr("superuserValidationError", superUserValidationException)
                            .attr("clusterOperationValidationError", clusterOperationValidationException)
                            .log("validateBothSuperuserAndClusterOperation failed");
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
                    log.debug().attr("originalPrincipal", originalPrincipal())
                            .attr("operation", operation.toString())
                            .attr("cluster", clusterName)
                            .attr("superuserValidationError", superUserValidationException)
                            .attr("clusterOperationValidationError", clusterOperationValidationException)
                            .log("validateBothSuperuserAndClusterPolicyOperation failed");
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
}
