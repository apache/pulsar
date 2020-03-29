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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.Constants;
import org.apache.pulsar.common.naming.NamedEntity;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;

public class TenantsBase extends AdminResource {

    @GET
    @ApiOperation(value = "Get the list of existing tenants.", response = String.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "Tenant doesn't exist") })
    public List<String> getTenants() {
        validateSuperUserAccess();

        try {
            List<String> tenants = globalZk().getChildren(path(POLICIES), false);
            tenants.sort(null);
            return tenants;
        } catch (Exception e) {
            log.error("[{}] Failed to get tenants list", clientAppId(), e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{tenant}")
    @ApiOperation(value = "Get the admin configuration for a given tenant.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "Tenant does not exist") })
    public TenantInfo getTenantAdmin(
        @ApiParam(value = "The tenant name")
        @PathParam("tenant") String tenant) {
        validateSuperUserAccess();

        try {
            return tenantsCache().get(path(POLICIES, tenant))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Tenant does not exist"));
        } catch (Exception e) {
            log.error("[{}] Failed to get tenant {}", clientAppId(), tenant, e);
            throw new RestException(e);
        }
    }

    @PUT
    @Path("/{tenant}")
    @ApiOperation(value = "Create a new tenant.", notes = "This operation requires Pulsar super-user privileges.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 409, message = "Tenant already exists"),
            @ApiResponse(code = 412, message = "Tenant name is not valid"),
            @ApiResponse(code = 412, message = "Clusters can not be empty"),
            @ApiResponse(code = 412, message = "Clusters do not exist") })
    public void createTenant(
        @ApiParam(value = "The tenant name")
        @PathParam("tenant") String tenant,
        @ApiParam(value = "TenantInfo") TenantInfo config) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();
        validateClusters(config);

        try {
            NamedEntity.checkName(tenant);
            zkCreate(path(POLICIES, tenant), jsonMapper().writeValueAsBytes(config));
            log.info("[{}] Created tenant {}", clientAppId(), tenant);
        } catch (KeeperException.NodeExistsException e) {
            log.warn("[{}] Failed to create already existing tenant {}", clientAppId(), tenant);
            throw new RestException(Status.CONFLICT, "Tenant already exists");
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Failed to create tenant with invalid name {}", clientAppId(), tenant, e);
            throw new RestException(Status.PRECONDITION_FAILED, "Tenant name is not valid");
        } catch (Exception e) {
            log.error("[{}] Failed to create tenant {}", clientAppId(), tenant, e);
            throw new RestException(e);
        }
    }

    @POST
    @Path("/{tenant}")
    @ApiOperation(value = "Update the admins for a tenant.", notes = "This operation requires Pulsar super-user privileges.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "Tenant does not exist"),
            @ApiResponse(code = 409, message = "Tenant already exists"),
            @ApiResponse(code = 412, message = "Clusters can not be empty"),
            @ApiResponse(code = 412, message = "Clusters do not exist") })
    public void updateTenant(
        @ApiParam(value = "The tenant name")
        @PathParam("tenant") String tenant,
        @ApiParam(value = "TenantInfo") TenantInfo newTenantAdmin) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();
        validateClusters(newTenantAdmin);

        Stat nodeStat = new Stat();
        try {
            byte[] content = globalZk().getData(path(POLICIES, tenant), null, nodeStat);
            TenantInfo oldTenantAdmin = jsonMapper().readValue(content, TenantInfo.class);
            List<String> clustersWithActiveNamespaces = Lists.newArrayList();
            if (oldTenantAdmin.getAllowedClusters().size() > newTenantAdmin.getAllowedClusters().size()) {
                // Get the colo(s) being removed from the list
                oldTenantAdmin.getAllowedClusters().removeAll(newTenantAdmin.getAllowedClusters());
                log.debug("Following clusters are being removed : [{}]", oldTenantAdmin.getAllowedClusters());
                for (String cluster : oldTenantAdmin.getAllowedClusters()) {
                    if (Constants.GLOBAL_CLUSTER.equals(cluster)) {
                        continue;
                    }
                    List<String> activeNamespaces = Lists.newArrayList();
                    try {
                        activeNamespaces = globalZk().getChildren(path(POLICIES, tenant, cluster), false);
                        if (activeNamespaces.size() != 0) {
                            // There are active namespaces in this cluster
                            clustersWithActiveNamespaces.add(cluster);
                        }
                    } catch (KeeperException.NoNodeException nne) {
                        // Fine, some cluster does not have active namespace. Move on!
                    }
                }
                if (!clustersWithActiveNamespaces.isEmpty()) {
                    // Throw an exception because colos being removed are having active namespaces
                    String msg = String.format(
                            "Failed to update the tenant because active namespaces are present in colos %s. Please delete those namespaces first",
                            clustersWithActiveNamespaces);
                    throw new RestException(Status.CONFLICT, msg);
                }
            }
            String tenantPath = path(POLICIES, tenant);
            globalZk().setData(tenantPath, jsonMapper().writeValueAsBytes(newTenantAdmin), -1);
            globalZkCache().invalidate(tenantPath);
            log.info("[{}] updated tenant {}", clientAppId(), tenant);
        } catch (RestException re) {
            throw re;
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update tenant {}: does not exist", clientAppId(), tenant);
            throw new RestException(Status.NOT_FOUND, "Tenant does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to update tenant {}", clientAppId(), tenant, e);
            throw new RestException(e);
        }
    }

    @DELETE
    @Path("/{tenant}")
    @ApiOperation(value = "Delete a tenant and all namespaces and topics under it.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "Tenant does not exist"),
            @ApiResponse(code = 409, message = "The tenant still has active namespaces") })
    public void deleteTenant(
        @PathParam("tenant")
        @ApiParam(value = "The tenant name")
        String tenant) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        boolean isTenantEmpty;
        try {
            isTenantEmpty = getListOfNamespaces(tenant).isEmpty();
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to delete tenant {}: does not exist", clientAppId(), tenant);
            throw new RestException(Status.NOT_FOUND, "The tenant does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to get tenant status {}", clientAppId(), tenant, e);
            throw new RestException(e);
        }

        if (!isTenantEmpty) {
            log.warn("[{}] Failed to delete tenant {}: not empty", clientAppId(), tenant);
            throw new RestException(Status.CONFLICT, "The tenant still has active namespaces");
        }

        try {
            // First try to delete every cluster z-node
            for (String cluster : globalZk().getChildren(path(POLICIES, tenant), false)) {
                globalZk().delete(path(POLICIES, tenant, cluster), -1);
            }

            globalZk().delete(path(POLICIES, tenant), -1);
            log.info("[{}] Deleted tenant {}", clientAppId(), tenant);
        } catch (Exception e) {
            log.error("[{}] Failed to delete tenant {}", clientAppId(), tenant, e);
            throw new RestException(e);
        }
    }

    private void validateClusters(TenantInfo info) {
        // empty cluster shouldn't be allowed
        if (info == null || info.getAllowedClusters().stream().filter(c -> !StringUtils.isBlank(c)).collect(Collectors.toSet()).isEmpty()
            || info.getAllowedClusters().stream().anyMatch(ac -> StringUtils.isBlank(ac))) {
            log.warn("[{}] Failed to validate due to clusters are empty", clientAppId());
            throw new RestException(Status.PRECONDITION_FAILED, "Clusters can not be empty");
        }

        List<String> nonexistentClusters;
        try {
            Set<String> availableClusters = clustersListCache().get();
            Set<String> allowedClusters = info.getAllowedClusters();
            nonexistentClusters = allowedClusters.stream()
                .filter(cluster -> !(availableClusters.contains(cluster) || Constants.GLOBAL_CLUSTER.equals(cluster)))
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

    private static final Logger log = LoggerFactory.getLogger(TenantsBase.class);
}
