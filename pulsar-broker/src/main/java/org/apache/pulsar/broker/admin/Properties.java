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

import java.util.Collections;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.NamedEntity;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Path("/properties")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(value = "/properties", description = "Properties admin apis", tags = "properties")
public class Properties extends AdminResource {

    @GET
    @ApiOperation(value = "Get the list of properties.", response = String.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property doesn't exist") })
    public List<String> getProperties() {
        validateSuperUserAccess();

        try {
            List<String> properties = globalZk().getChildren(path("policies"), false);
            properties.sort(null);
            return properties;
        } catch (Exception e) {
            log.error("[{}] Failed to get properties list", clientAppId(), e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{property}")
    @ApiOperation(value = "Get the admin configuration for a given property.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property doesn't exist") })
    public PropertyAdmin getPropertyAdmin(@PathParam("property") String property) {
        validateSuperUserAccess();

        try {
            return propertiesCache().get(path("policies", property))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Property does not exist"));
        } catch (Exception e) {
            log.error("[{}] Failed to get property {}", clientAppId(), property, e);
            throw new RestException(e);
        }
    }

    @PUT
    @Path("/{property}")
    @ApiOperation(value = "Create a new property.", notes = "This operation requires Pulsar super-user privileges.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 409, message = "Property already exist"),
            @ApiResponse(code = 412, message = "Property name is not valid") })
    public void createProperty(@PathParam("property") String property, PropertyAdmin config) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        try {
            NamedEntity.checkName(property);
            zkCreate(path("policies", property), jsonMapper().writeValueAsBytes(config));
            log.info("[{}] Created property {}", clientAppId(), property);
        } catch (KeeperException.NodeExistsException e) {
            log.warn("[{}] Failed to create already existing property {}", clientAppId(), property);
            throw new RestException(Status.CONFLICT, "Property already exist");
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Failed to create property with invalid name {}", clientAppId(), property, e);
            throw new RestException(Status.PRECONDITION_FAILED, "Property name is not valid");
        } catch (Exception e) {
            log.error("[{}] Failed to create property {}", clientAppId(), property, e);
            throw new RestException(e);
        }
    }

    @POST
    @Path("/{property}")
    @ApiOperation(value = "Update the admins for a property.", notes = "This operation requires Pulsar super-user privileges.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property does not exist"),
            @ApiResponse(code = 409, message = "Property already exist") })
    public void updateProperty(@PathParam("property") String property, PropertyAdmin newPropertyAdmin) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        Stat nodeStat = new Stat();
        try {
            byte[] content = globalZk().getData(path("policies", property), null, nodeStat);
            PropertyAdmin oldPropertyAdmin = jsonMapper().readValue(content, PropertyAdmin.class);
            List<String> clustersWithActiveNamespaces = Lists.newArrayList();
            if (oldPropertyAdmin.getAllowedClusters().size() > newPropertyAdmin.getAllowedClusters().size()) {
                // Get the colo(s) being removed from the list
                oldPropertyAdmin.getAllowedClusters().removeAll(newPropertyAdmin.getAllowedClusters());
                log.debug("Following clusters are being removed : [{}]", oldPropertyAdmin.getAllowedClusters());
                for (String cluster : oldPropertyAdmin.getAllowedClusters()) {
                    List<String> activeNamespaces = Lists.newArrayList();
                    try {
                        activeNamespaces = globalZk().getChildren(path("policies", property, cluster), false);
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
                            "Failed to update the property because active namespaces are present in colos %s. Please delete those namespaces first",
                            clustersWithActiveNamespaces);
                    throw new RestException(Status.CONFLICT, msg);
                }
            }
            String propertyPath = path("policies", property);
            globalZk().setData(propertyPath, jsonMapper().writeValueAsBytes(newPropertyAdmin), -1);
            globalZkCache().invalidate(propertyPath);
            log.info("[{}] updated property {}", clientAppId(), property);
        } catch (RestException re) {
            throw re;
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update property {}: does not exist", clientAppId(), property);
            throw new RestException(Status.NOT_FOUND, "Property does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to update property {}", clientAppId(), property, e);
            throw new RestException(e);
        }
    }

    @DELETE
    @Path("/{property}")
    @ApiOperation(value = "elete a property and all namespaces and destinations under it.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Property does not exist"),
            @ApiResponse(code = 409, message = "The property still has active namespaces") })
    public void deleteProperty(@PathParam("property") String property) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        boolean isPropertyEmpty = false;
        try {
            isPropertyEmpty = getListOfNamespaces(property).isEmpty();
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to delete property {}: does not exist", clientAppId(), property);
            throw new RestException(Status.NOT_FOUND, "The property does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to get property status {}", clientAppId(), property, e);
            throw new RestException(e);
        }

        if (!isPropertyEmpty) {
            log.warn("[{}] Failed to delete property {}: not empty", clientAppId(), property);
            throw new RestException(Status.CONFLICT, "The property still has active namespaces");
        }

        try {
            // First try to delete every cluster z-node
            for (String cluster : globalZk().getChildren(path("policies", property), false)) {
                globalZk().delete(path("policies", property, cluster), -1);
            }

            globalZk().delete(path("policies", property), -1);
            log.info("[{}] Deleted property {}", clientAppId(), property);
        } catch (Exception e) {
            log.error("[{}] Failed to delete property {}", clientAppId(), property, e);
            throw new RestException(e);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Properties.class);
}
