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

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response.Status;

import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.NamedEntity;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;

public class TenantsBase extends AdminResource {

    @GET
    @ApiOperation(value = "Get the list of tenants.", response = String.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property doesn't exist") })
    public List<String> getProperties() {
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
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property doesn't exist") })
    public TenantInfo getPropertyAdmin(@PathParam("tenant") String tenant) {
        validateSuperUserAccess();

        try {
            return tenantsCache().get(path(POLICIES, tenant))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Property does not exist"));
        } catch (Exception e) {
            log.error("[{}] Failed to get tenant {}", clientAppId(), tenant, e);
            throw new RestException(e);
        }
    }

    @PUT
    @Path("/{tenant}")
    @ApiOperation(value = "Create a new tenant.", notes = "This operation requires Pulsar super-user privileges.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 409, message = "Property already exist"),
            @ApiResponse(code = 412, message = "Property name is not valid") })
    public void createProperty(@PathParam("tenant") String tenant, TenantInfo config) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        try {
            NamedEntity.checkName(tenant);
            zkCreate(path(POLICIES, tenant), jsonMapper().writeValueAsBytes(config));
            log.info("[{}] Created tenant {}", clientAppId(), tenant);
        } catch (KeeperException.NodeExistsException e) {
            log.warn("[{}] Failed to create already existing tenant {}", clientAppId(), tenant);
            throw new RestException(Status.CONFLICT, "Property already exist");
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Failed to create tenant with invalid name {}", clientAppId(), tenant, e);
            throw new RestException(Status.PRECONDITION_FAILED, "Property name is not valid");
        } catch (Exception e) {
            log.error("[{}] Failed to create tenant {}", clientAppId(), tenant, e);
            throw new RestException(e);
        }
    }

    @POST
    @Path("/{tenant}")
    @ApiOperation(value = "Update the admins for a tenant.", notes = "This operation requires Pulsar super-user privileges.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property does not exist"),
            @ApiResponse(code = 409, message = "Property already exist") })
    public void updateProperty(@PathParam("tenant") String tenant, TenantInfo newPropertyAdmin) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        Stat nodeStat = new Stat();
        try {
            byte[] content = globalZk().getData(path(POLICIES, tenant), null, nodeStat);
            TenantInfo oldPropertyAdmin = jsonMapper().readValue(content, TenantInfo.class);
            List<String> clustersWithActiveNamespaces = Lists.newArrayList();
            if (oldPropertyAdmin.getAllowedClusters().size() > newPropertyAdmin.getAllowedClusters().size()) {
                // Get the colo(s) being removed from the list
                oldPropertyAdmin.getAllowedClusters().removeAll(newPropertyAdmin.getAllowedClusters());
                log.debug("Following clusters are being removed : [{}]", oldPropertyAdmin.getAllowedClusters());
                for (String cluster : oldPropertyAdmin.getAllowedClusters()) {
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
            globalZk().setData(tenantPath, jsonMapper().writeValueAsBytes(newPropertyAdmin), -1);
            globalZkCache().invalidate(tenantPath);
            log.info("[{}] updated tenant {}", clientAppId(), tenant);
        } catch (RestException re) {
            throw re;
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update tenant {}: does not exist", clientAppId(), tenant);
            throw new RestException(Status.NOT_FOUND, "Property does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to update tenant {}", clientAppId(), tenant, e);
            throw new RestException(e);
        }
    }

    @DELETE
    @Path("/{tenant}")
    @ApiOperation(value = "elete a tenant and all namespaces and topics under it.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property does not exist"),
            @ApiResponse(code = 409, message = "The tenant still has active namespaces") })
    public void deleteProperty(@PathParam("tenant") String tenant) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        boolean isPropertyEmpty = false;
        try {
            isPropertyEmpty = getListOfNamespaces(tenant).isEmpty();
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to delete tenant {}: does not exist", clientAppId(), tenant);
            throw new RestException(Status.NOT_FOUND, "The tenant does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to get tenant status {}", clientAppId(), tenant, e);
            throw new RestException(e);
        }

        if (!isPropertyEmpty) {
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

    private static final Logger log = LoggerFactory.getLogger(TenantsBase.class);
}
