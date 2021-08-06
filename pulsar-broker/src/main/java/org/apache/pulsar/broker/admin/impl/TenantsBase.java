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
import com.google.common.collect.Lists;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.Constants;
import org.apache.pulsar.common.naming.NamedEntity;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TenantsBase extends PulsarWebResource {

    private static final Logger log = LoggerFactory.getLogger(TenantsBase.class);

    @GET
    @ApiOperation(value = "Get the list of existing tenants.", response = String.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "Tenant doesn't exist") })
    public void getTenants(@Suspended final AsyncResponse asyncResponse) {
        final String clientAppId = clientAppId();
        try {
            validateSuperUserAccess();
        } catch (Exception e) {
            asyncResponse.resume(e);
            return;
        }
        tenantResources().getChildrenAsync(path(POLICIES)).whenComplete((tenants, e) -> {
            if (e != null) {
                log.error("[{}] Failed to get tenants list", clientAppId, e);
                asyncResponse.resume(new RestException(e));
                return;
            }
            tenants.sort(null);
            asyncResponse.resume(tenants);
        });
    }

    @GET
    @Path("/{tenant}")
    @ApiOperation(value = "Get the admin configuration for a given tenant.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "Tenant does not exist") })
    public void getTenantAdmin(@Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "The tenant name") @PathParam("tenant") String tenant) {
        final String clientAppId = clientAppId();
        try {
            validateSuperUserAccess();
        } catch (Exception e) {
            asyncResponse.resume(e);
        }

        tenantResources().getAsync(path(POLICIES, tenant)).whenComplete((tenantInfo, e) -> {
            if (e != null) {
                log.error("[{}] Failed to get Tenant {}", clientAppId, e.getMessage());
                asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR, "Failed to get Tenant"));
                return;
            }
            boolean response = tenantInfo.isPresent() ? asyncResponse.resume(tenantInfo.get())
                    : asyncResponse.resume(new RestException(Status.NOT_FOUND, "Tenant does not exist"));
            return;
        });
    }

    @PUT
    @Path("/{tenant}")
    @ApiOperation(value = "Create a new tenant.", notes = "This operation requires Pulsar super-user privileges.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 409, message = "Tenant already exists"),
            @ApiResponse(code = 412, message = "Tenant name is not valid"),
            @ApiResponse(code = 412, message = "Clusters can not be empty"),
            @ApiResponse(code = 412, message = "Clusters do not exist") })
    public void createTenant(@Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "The tenant name") @PathParam("tenant") String tenant,
            @ApiParam(value = "TenantInfo") TenantInfoImpl tenantInfo) {

        final String clientAppId = clientAppId();
        try {
            validateSuperUserAccess();
            validatePoliciesReadOnlyAccess();
            validateClusters(tenantInfo);
            NamedEntity.checkName(tenant);
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Failed to create tenant with invalid name {}", clientAppId(), tenant, e);
            asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED, "Tenant name is not valid"));
            return;
        } catch (Exception e) {
            asyncResponse.resume(e);
            return;
        }

        tenantResources().getChildrenAsync(path(POLICIES)).whenComplete((tenants, e) -> {
            if (e != null) {
                log.error("[{}] Failed to create tenant ", clientAppId, e.getCause());
                asyncResponse.resume(new RestException(e));
                return;
            }

            int maxTenants = pulsar().getConfiguration().getMaxTenants();
            // Due to the cost of distributed locks, no locks are added here.
            // In a concurrent scenario, the threshold will be exceeded.
            if (maxTenants > 0) {
                if (tenants != null && tenants.size() >= maxTenants) {
                    asyncResponse.resume(
                            new RestException(Status.PRECONDITION_FAILED, "Exceed the maximum number of tenants"));
                    return;
                }
            }
            tenantResources().existsAsync(path(POLICIES, tenant)).thenAccept(exist ->{
                if (exist) {
                    asyncResponse.resume(new RestException(Status.CONFLICT, "Tenant already exist"));
                    return;
                }
                tenantResources().createAsync(path(POLICIES, tenant), tenantInfo).thenAccept((r) -> {
                    log.info("[{}] Created tenant {}", clientAppId(), tenant);
                    asyncResponse.resume(Response.noContent().build());
                }).exceptionally(ex -> {
                    log.error("[{}] Failed to create tenant {}", clientAppId, tenant, e);
                    asyncResponse.resume(new RestException(ex));
                    return null;
                });
            }).exceptionally(ex -> {
                log.error("[{}] Failed to create tenant {}", clientAppId(), tenant, e);
                asyncResponse.resume(new RestException(ex));
                return null;
            });
        });
    }

    @POST
    @Path("/{tenant}")
    @ApiOperation(value = "Update the admins for a tenant.",
    notes = "This operation requires Pulsar super-user privileges.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "Tenant does not exist"),
            @ApiResponse(code = 409, message = "Tenant already exists"),
            @ApiResponse(code = 412, message = "Clusters can not be empty"),
            @ApiResponse(code = 412, message = "Clusters do not exist") })
    public void updateTenant(@Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "The tenant name") @PathParam("tenant") String tenant,
            @ApiParam(value = "TenantInfo") TenantInfoImpl newTenantAdmin) {
        try {
            validateSuperUserAccess();
            validatePoliciesReadOnlyAccess();
            validateClusters(newTenantAdmin);
        } catch (Exception e) {
            asyncResponse.resume(e);
            return;
        }

        final String clientAddId = clientAppId();
        tenantResources().getAsync(path(POLICIES, tenant)).thenAccept(tenantAdmin -> {
            if (!tenantAdmin.isPresent()) {
                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Tenant " + tenant + " not found"));
                return;
            }
            TenantInfo oldTenantAdmin = tenantAdmin.get();
            Set<String> newClusters = new HashSet<>(newTenantAdmin.getAllowedClusters());
            canUpdateCluster(tenant, oldTenantAdmin.getAllowedClusters(), newClusters).thenApply(r -> {
                tenantResources().setAsync(path(POLICIES, tenant), old -> {
                    return newTenantAdmin;
                }).thenAccept(done -> {
                    log.info("Successfully updated tenant info {}", tenant);
                    asyncResponse.resume(Response.noContent().build());
                }).exceptionally(ex -> {
                    log.warn("Failed to update tenant {}", tenant, ex.getCause());
                    asyncResponse.resume(new RestException(ex));
                    return null;
                });
                return null;
            }).exceptionally(nsEx -> {
                asyncResponse.resume(nsEx.getCause());
                return null;
            });
        }).exceptionally(ex -> {
            log.error("[{}] Failed to get tenant {}", clientAddId, tenant, ex.getCause());
            asyncResponse.resume(new RestException(ex));
            return null;
        });
    }

    @DELETE
    @Path("/{tenant}")
    @ApiOperation(value = "Delete a tenant and all namespaces and topics under it.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "Tenant does not exist"),
            @ApiResponse(code = 405, message = "Broker doesn't allow forced deletion of tenants"),
            @ApiResponse(code = 409, message = "The tenant still has active namespaces") })
    public void deleteTenant(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") @ApiParam(value = "The tenant name") String tenant,
            @QueryParam("force") @DefaultValue("false") boolean force) {
        try {
            validateSuperUserAccess();
            validatePoliciesReadOnlyAccess();
        } catch (Exception e) {
            asyncResponse.resume(e);
            return;
        }
        internalDeleteTenant(asyncResponse, tenant, force);
    }

    protected void internalDeleteTenant(AsyncResponse asyncResponse, String tenant, boolean force) {
        if (force) {
            internalDeleteTenantForcefully(asyncResponse, tenant);
        } else {
            internalDeleteTenant(asyncResponse, tenant);
        }
    }

    protected void internalDeleteTenant(AsyncResponse asyncResponse, String tenant) {
        tenantResources().existsAsync(path(POLICIES, tenant)).thenApply(exists -> {
            if (!exists) {
                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Tenant doesn't exist"));
                return null;
            }
            return hasActiveNamespace(tenant).thenAccept(ns -> {
                try {
                    // already fetched children and they should be in the cache
                    List<CompletableFuture<Void>> clusterList = Lists.newArrayList();
                    for (String cluster : tenantResources().getChildrenAsync(path(POLICIES, tenant)).get()) {
                        clusterList.add(tenantResources().deleteAsync(path(POLICIES, tenant, cluster)));
                    }
                    FutureUtil.waitForAll(clusterList).thenAccept(c -> {
                        tenantResources().deleteAsync(path(POLICIES, tenant)).thenAccept(t -> {
                            log.info("[{}] Deleted tenant {}", clientAppId(), tenant);
                            asyncResponse.resume(Response.noContent().build());
                        }).exceptionally(ex -> {
                            log.error("Failed to delete tenant {}", tenant, ex.getCause());
                            asyncResponse.resume(new RestException(ex));
                            return null;
                        });
                    }).exceptionally(ex -> {
                        log.error("Failed to delete clusters under tenant {}", tenant, ex.getCause());
                        asyncResponse.resume(new RestException(ex));
                        return null;
                    });
                    log.info("[{}] Deleted tenant {}", clientAppId(), tenant);
                } catch (Exception e) {
                    log.error("[{}] Failed to delete tenant {}", clientAppId(), tenant, e);
                    asyncResponse.resume(new RestException(e));
                }
            }).exceptionally(ex -> {
                log.error("Failed to delete tenant due to active namespace {}", tenant, ex.getCause());
                asyncResponse.resume(new RestException(ex));
                return null;
            });
        });
    }

    protected void internalDeleteTenantForcefully(AsyncResponse asyncResponse, String tenant) {
        if (!pulsar().getConfiguration().isForceDeleteTenantAllowed()) {
            asyncResponse.resume(
                    new RestException(Status.METHOD_NOT_ALLOWED, "Broker doesn't allow forced deletion of tenants"));
            return;
        }

        List<String> namespaces;
        try {
            namespaces = getListOfNamespaces(tenant);
        } catch (Exception e) {
            log.error("[{}] Failed to get namespaces list of {}", clientAppId(), tenant, e);
            asyncResponse.resume(new RestException(e));
            return;
        }

        final List<CompletableFuture<Void>> futures = Lists.newArrayList();
        try {
            for (String namespace : namespaces) {
                futures.add(pulsar().getAdminClient().namespaces().deleteNamespaceAsync(namespace, true));
            }
        } catch (Exception e) {
            log.error("[{}] Failed to force delete namespaces {}", clientAppId(), namespaces, e);
            asyncResponse.resume(new RestException(e));
        }

        FutureUtil.waitForAll(futures).handle((result, exception) -> {
            if (exception != null) {
                if (exception.getCause() instanceof PulsarAdminException) {
                    asyncResponse.resume(new RestException((PulsarAdminException) exception.getCause()));
                } else {
                    log.error("[{}] Failed to force delete namespaces {}", clientAppId(), namespaces, exception);
                    asyncResponse.resume(new RestException(exception.getCause()));
                }
                return null;
            }
            // delete tenant normally
            internalDeleteTenant(asyncResponse, tenant);

            asyncResponse.resume(Response.noContent().build());
            return null;
        });
    }

    private void validateClusters(TenantInfo info) {
        // empty cluster shouldn't be allowed
        if (info == null || info.getAllowedClusters().stream().filter(c -> !StringUtils.isBlank(c))
                .collect(Collectors.toSet()).isEmpty()
                || info.getAllowedClusters().stream().anyMatch(ac -> StringUtils.isBlank(ac))) {
            log.warn("[{}] Failed to validate due to clusters are empty", clientAppId());
            throw new RestException(Status.PRECONDITION_FAILED, "Clusters can not be empty");
        }

        List<String> nonexistentClusters;
        try {
            Set<String> availableClusters = clusterResources().list();
            Set<String> allowedClusters = info.getAllowedClusters();
            nonexistentClusters = allowedClusters.stream().filter(
                    cluster -> !(availableClusters.contains(cluster) || Constants.GLOBAL_CLUSTER.equals(cluster)))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("[{}] Failed to get available clusters", clientAppId(), e);
            throw new RestException(e);
        }
        if (nonexistentClusters.size() > 0) {
            log.warn("[{}] Failed to validate due to clusters {} do not exist", clientAppId(), nonexistentClusters);
            throw new RestException(Status.PRECONDITION_FAILED, "Clusters do not exist");
        }
    }
}
