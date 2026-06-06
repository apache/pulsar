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

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.NamedEntity;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.util.FutureUtil;

public class TenantsBase extends PulsarWebResource {
    @GET
    @Operation(summary = "Get the list of existing tenants.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the list of existing tenants.",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "404", description = "Tenant doesn't exist")})
    public void getTenants(@Suspended final AsyncResponse asyncResponse) {
        final String clientAppId = clientAppId();
        validateBothSuperUserAndTenantOperation(null, TenantOperation.LIST_TENANTS)
                .thenCompose(__ -> tenantResources().listTenantsAsync())
                .thenAccept(tenants -> {
                    // deep copy the tenants to avoid concurrent sort exception
                    List<String> deepCopy = new ArrayList<>(tenants);
                    deepCopy.sort(null);
                    asyncResponse.resume(deepCopy);
                }).exceptionally(ex -> {
                    log.error().exception(ex).log("Failed to get tenants list");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}")
    @Operation(summary = "Get the admin configuration for a given tenant.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the admin configuration for a given tenant.",
                    content = @Content(schema = @Schema(implementation = TenantInfo.class))),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "404", description = "Tenant does not exist")})
    public void getTenantAdmin(@Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "The tenant name") @PathParam("tenant") String tenant) {
        final String clientAppId = clientAppId();
        validateBothSuperUserAndTenantOperation(tenant, TenantOperation.GET_TENANT)
                .thenCompose(__ -> tenantResources().getTenantAsync(tenant))
                .thenApply(tenantInfo -> {
                    if (!tenantInfo.isPresent()) {
                        throw new RestException(Status.NOT_FOUND, "Tenant does not exist");
                    }
                    return tenantInfo.get();
                })
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error().exception(ex).log("Failed to get tenant admin");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}")
    @Operation(summary = "Create a new tenant.", description = "This operation requires Pulsar super-user privileges.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "409", description = "Tenant already exists"),
            @ApiResponse(responseCode = "412", description = "Tenant name is not valid"),
            @ApiResponse(responseCode = "412", description = "Clusters can not be empty"),
            @ApiResponse(responseCode = "412", description = "Clusters do not exist")})
    public void createTenant(@Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "The tenant name") @PathParam("tenant") String tenant,
            @RequestBody(description = "TenantInfo") TenantInfoImpl tenantInfo) {
        final String clientAppId = clientAppId();
        try {
            NamedEntity.checkName(tenant);
        } catch (IllegalArgumentException e) {
            log.warn()
                    .attr("tenant", tenant)
                    .exception(e)
                    .log("Failed to create tenant with invalid name");
            asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED, "Tenant name is not valid"));
            return;
        }
        validateBothSuperUserAndTenantOperation(tenant, TenantOperation.CREATE_TENANT)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> validateClustersAsync(tenantInfo))
                .thenCompose(__ -> validateAdminRoleAsync(tenantInfo))
                .thenCompose(__ -> tenantResources().tenantExistsAsync(tenant))
                .thenAccept(exist -> {
                    if (exist) {
                        throw new RestException(Status.CONFLICT, "Tenant already exist");
                    }
                })
                .thenCompose(__ -> tenantResources().listTenantsAsync())
                .thenAccept(tenants -> {
                    int maxTenants = pulsar().getConfiguration().getMaxTenants();
                    // Due to the cost of distributed locks, no locks are added here.
                    // In a concurrent scenario, the threshold will be exceeded.
                    if (maxTenants > 0) {
                        if (tenants != null && tenants.size() >= maxTenants) {
                            throw new RestException(Status.PRECONDITION_FAILED, "Exceed the maximum number of tenants");
                        }
                    }
                })
                .thenCompose(__ -> tenantResources().createTenantAsync(tenant, tenantInfo))
                .thenAccept(__ -> {
                    log.info().attr("tenant", tenant).log("Created tenant");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    log.error()
                            .attr("tenant", tenant)
                            .exception(ex)
                            .log("Failed to create tenant");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}")
    @Operation(summary = "Update the admins for a tenant.",
            description = "This operation requires Pulsar super-user privileges.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "404", description = "Tenant does not exist"),
            @ApiResponse(responseCode = "409", description = "Tenant already exists"),
            @ApiResponse(responseCode = "412", description = "Clusters can not be empty"),
            @ApiResponse(responseCode = "412", description = "Clusters do not exist")})
    public void updateTenant(@Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "The tenant name") @PathParam("tenant") String tenant,
            @RequestBody(description = "TenantInfo") TenantInfoImpl newTenantAdmin) {
        final String clientAppId = clientAppId();
        validateBothSuperUserAndTenantOperation(tenant, TenantOperation.UPDATE_TENANT)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> validateClustersAsync(newTenantAdmin))
                .thenCompose(__ -> validateAdminRoleAsync(newTenantAdmin))
                .thenCompose(__ -> tenantResources().getTenantAsync(tenant))
                .thenCompose(tenantAdmin -> {
                    if (!tenantAdmin.isPresent()) {
                        throw new RestException(Status.NOT_FOUND, "Tenant " + tenant + " not found");
                    }
                    TenantInfo oldTenantAdmin = tenantAdmin.get();
                    Set<String> newClusters = new HashSet<>(newTenantAdmin.getAllowedClusters());
                    return canUpdateCluster(tenant, oldTenantAdmin.getAllowedClusters(), newClusters);
                })
                .thenCompose(__ -> tenantResources().updateTenantAsync(tenant, old -> newTenantAdmin))
                .thenAccept(__ -> {
                    log.info()
                            .attr("info", tenant)
                            .log("Successfully updated tenant info");
                    asyncResponse.resume(Response.noContent().build());
                }).exceptionally(ex -> {
                    log.warn()
                            .attr("tenant", tenant)
                            .exception(ex)
                            .log("Failed to update tenant");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}")
    @Operation(summary = "Delete a tenant and all namespaces and topics under it.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "404", description = "Tenant does not exist"),
            @ApiResponse(responseCode = "405", description = "Broker doesn't allow forced deletion of tenants"),
            @ApiResponse(responseCode = "409", description = "The tenant still has active namespaces")})
    public void deleteTenant(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") @Parameter(description = "The tenant name") String tenant,
            @QueryParam("force") @DefaultValue("false") boolean force) {
        final String clientAppId = clientAppId();
        validateBothSuperUserAndTenantOperation(tenant, TenantOperation.DELETE_TENANT)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> internalDeleteTenant(tenant, force))
                .thenAccept(__ -> {
                    log.info().attr("tenant", tenant).log("Deleted tenant");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    Throwable cause = FutureUtil.unwrapCompletionException(ex);
                    log.error()
                            .attr("tenant", tenant)
                            .exception(cause)
                            .log("Failed to delete tenant");
                    if (cause instanceof IllegalStateException) {
                        asyncResponse.resume(new RestException(Status.CONFLICT, cause));
                    } else {
                        resumeAsyncResponseExceptionally(asyncResponse, cause);
                    }
                    return null;
                });
    }

    protected CompletableFuture<Void> internalDeleteTenant(String tenant, boolean force) {
        return force ? internalDeleteTenantAsyncForcefully(tenant) : internalDeleteTenantAsync(tenant);
    }

    protected CompletableFuture<Void> internalDeleteTenantAsync(String tenant) {
        return tenantResources().tenantExistsAsync(tenant)
                .thenAccept(exists -> {
                    if (!exists) {
                        throw new RestException(Status.NOT_FOUND, "Tenant doesn't exist");
                    }
                })
                .thenCompose(__ -> hasActiveNamespace(tenant))
                .thenCompose(__ -> tenantResources().deleteTenantAsync(tenant))
                .thenCompose(__ -> pulsar().getPulsarResources().getTopicResources().clearTenantPersistence(tenant))
                .thenCompose(__ -> pulsar().getPulsarResources().getNamespaceResources().deleteTenantAsync(tenant))
                .thenCompose(__ -> pulsar().getPulsarResources().getNamespaceResources()
                            .getPartitionedTopicResources().clearPartitionedTopicTenantAsync(tenant))
                .thenCompose(__ -> pulsar().getPulsarResources().getLocalPolicies()
                            .deleteLocalPoliciesTenantAsync(tenant))
                .thenCompose(__ -> pulsar().getPulsarResources().getLoadBalanceResources().getBundleDataResources()
                            .deleteBundleDataTenantAsync(tenant));
    }

    protected CompletableFuture<Void> internalDeleteTenantAsyncForcefully(String tenant) {
        if (!pulsar().getConfiguration().isForceDeleteTenantAllowed()) {
            return FutureUtil.failedFuture(
                    new RestException(Status.METHOD_NOT_ALLOWED, "Broker doesn't allow forced deletion of tenants"));
        }
        return tenantResources().getListOfNamespacesAsync(tenant)
                .thenApply(namespaces -> {
                    final List<CompletableFuture<Void>> futures = new ArrayList<>();
                    try {
                        PulsarAdmin adminClient = pulsar().getAdminClient();
                        for (String namespace : namespaces) {
                            futures.add(adminClient.namespaces().deleteNamespaceAsync(namespace, true));
                        }
                    } catch (Exception e) {
                        log.error()
                                .attr("namespaces", namespaces)
                                .exception(e)
                                .log("Failed to force delete namespaces");
                        throw new RestException(e);
                    }
                    return futures;
                })
                .thenCompose(futures -> FutureUtil.waitForAll(futures))
                .thenCompose(__ -> internalDeleteTenantAsync(tenant));
    }

    private CompletableFuture<Void> validateClustersAsync(TenantInfo info) {
        if (info == null) {
            return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED, "TenantInfo is null"));
        }

        Set<String> allowedClusters = info.getAllowedClusters();
        if (allowedClusters == null) {
            return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED, "Clusters cannot be null"));
        }

        Set<String> cleanedClusters = allowedClusters.stream()
                .filter(c -> !StringUtils.isBlank(c))
                .collect(Collectors.toSet());
        if (cleanedClusters.isEmpty() || allowedClusters.stream().anyMatch(StringUtils::isBlank)) {
            log.warn("Validation failed: allowed clusters are empty or contain blanks");
            return FutureUtil.failedFuture(
                    new RestException(Status.PRECONDITION_FAILED, "Clusters cannot be empty or blank"));
        }

        return clusterResources().listAsync().thenAccept(availableClusters -> {
            List<String> nonexistentClusters = allowedClusters.stream()
                    .filter(cluster -> !availableClusters.contains(cluster))
                    .collect(Collectors.toList());
            if (nonexistentClusters.size() > 0) {
                log.warn()
                        .attr("clusters", nonexistentClusters)
                        .log("Failed to validate due to clusters do not exist");
                throw new RestException(Status.PRECONDITION_FAILED, "Clusters do not exist");
            }
        });
    }

    private CompletableFuture<Void> validateAdminRoleAsync(TenantInfoImpl info) {
        if (info.getAdminRoles() != null && !info.getAdminRoles().isEmpty()) {
            for (String adminRole : info.getAdminRoles()) {
                if (!StringUtils.trim(adminRole).equals(adminRole)) {
                    log.warn()
                            .attr("adminRole", adminRole)
                            .log("Failed to validate due to adminRole contains whitespace in the beginning or end.");
                    return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                            "AdminRoles contains whitespace in the beginning or end."));
                }
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Boolean> validateBothSuperUserAndTenantOperation(String tenant,
                                                                               TenantOperation operation) {
        final var superUserValidationFuture = validateSuperUserAccessAsync();
        final var tenantOperationValidationFuture = validateTenantOperationAsync(tenant, operation);
        return CompletableFuture.allOf(superUserValidationFuture, tenantOperationValidationFuture)
                .handle((__, err) -> {
                    if (!superUserValidationFuture.isCompletedExceptionally()
                        || !tenantOperationValidationFuture.isCompletedExceptionally()) {
                        return true;
                    }
                    Throwable superUserValidationException = null;
                    try {
                        superUserValidationFuture.join();
                    } catch (Throwable ex) {
                        superUserValidationException = FutureUtil.unwrapCompletionException(ex);
                    }
                    Throwable tenantOperationValidationException = null;
                    try {
                        tenantOperationValidationFuture.join();
                    } catch (Throwable ex) {
                        tenantOperationValidationException = FutureUtil.unwrapCompletionException(ex);
                    }
                    log.debug().attr("originalPrincipal", originalPrincipal())
                            .attr("operation", operation.toString())
                            .attr("superuserValidationError", superUserValidationException)
                            .attr("tenantOperationValidationError", tenantOperationValidationException)
                            .log("validateBothTenantOperationAndSuperUser failed");
                    throw new RestException(Status.UNAUTHORIZED,
                            String.format("Unauthorized to validateBothTenantOperationAndSuperUser for"
                                          + " originalPrincipal [%s] and clientAppId [%s] "
                                          + "about operation [%s] ",
                                    originalPrincipal(), clientAppId(), operation.toString()));
                });
    }
}
