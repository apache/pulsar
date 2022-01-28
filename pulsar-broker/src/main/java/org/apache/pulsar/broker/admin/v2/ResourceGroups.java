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
package org.apache.pulsar.broker.admin.v2;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.pulsar.broker.admin.impl.ResourceGroupsBase;
import org.apache.pulsar.common.policies.data.ResourceGroup;

@Path("/resourcegroups")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(value = "/resourcegroups", description = "ResourceGroups admin apis", tags = "resourcegroups")
public class ResourceGroups extends ResourceGroupsBase {

    @GET
    @ApiOperation(value = "Get the list of all the resourcegroups.",
            response = String.class, responseContainer = "Set")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission")})
    public List<String> getResourceGroups() {
        return internalGetResourceGroups();
    }

    @GET
    @Path("/{resourcegroup}")
    @ApiOperation(value = "Get the rate limiters specified for a resourcegroup.", response = ResourceGroup.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "ResourceGroup doesn't exist")})
    public ResourceGroup getResourceGroup(@PathParam("resourcegroup") String resourcegroup) {
        return internalGetResourceGroup(resourcegroup);
    }

    @PUT
    @Path("/{resourcegroup}")
    @ApiOperation(value = "Creates a new resourcegroup with the specified rate limiters")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "cluster doesn't exist")})
    public void createOrUpdateResourceGroup(@PathParam("resourcegroup") String name,
                                    @ApiParam(value = "Rate limiters for the resourcegroup")
                                            ResourceGroup resourcegroup) {
        internalCreateOrUpdateResourceGroup(name, resourcegroup);
    }

    @DELETE
    @Path("/{resourcegroup}")
    @ApiOperation(value = "Delete a resourcegroup.")
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "ResourceGroup doesn't exist"),
            @ApiResponse(code = 409, message = "ResourceGroup is in use")})
    public void deleteResourceGroup(@PathParam("resourcegroup") String resourcegroup) {
        internalDeleteResourceGroup(resourcegroup);
    }
}

