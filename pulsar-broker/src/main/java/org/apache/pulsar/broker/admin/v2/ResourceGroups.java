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
package org.apache.pulsar.broker.admin.v2;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import org.apache.pulsar.broker.admin.impl.ResourceGroupsBase;
import org.apache.pulsar.common.policies.data.ResourceGroup;

@Path("/resourcegroups")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "resourcegroups", description = "ResourceGroups admin apis")
@SuppressWarnings("deprecation")
public class ResourceGroups extends ResourceGroupsBase {

    @GET
    @Operation(summary = "Get the list of all the resourcegroups.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the list of all the resourcegroups.",
                    content = @Content(array = @ArraySchema(uniqueItems = true,
                            schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission")})
    public List<String> getResourceGroups() {
        return internalGetResourceGroups();
    }

    @GET
    @Path("/{resourcegroup}")
    @Operation(summary = "Get the rate limiters specified for a resourcegroup.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the rate limiters specified for a resourcegroup.",
                    content = @Content(schema = @Schema(implementation = ResourceGroup.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "ResourceGroup doesn't exist")})
    public ResourceGroup getResourceGroup(@PathParam("resourcegroup") String resourcegroup) {
        return internalGetResourceGroup(resourcegroup);
    }

    @PUT
    @Path("/{resourcegroup}")
    @Operation(summary = "Creates a new resourcegroup with the specified rate limiters")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "cluster doesn't exist")})
    public void createOrUpdateResourceGroup(@PathParam("resourcegroup") String name,
                                    @RequestBody(description = "Rate limiters for the resourcegroup")
                                            ResourceGroup resourcegroup) {
        internalCreateOrUpdateResourceGroup(name, resourcegroup);
    }

    @DELETE
    @Path("/{resourcegroup}")
    @Operation(summary = "Delete a resourcegroup.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "ResourceGroup doesn't exist"),
            @ApiResponse(responseCode = "409", description = "ResourceGroup is in use")})
    public void deleteResourceGroup(@PathParam("resourcegroup") String resourcegroup) {
        internalDeleteResourceGroup(resourcegroup);
    }
}

