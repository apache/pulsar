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
import io.swagger.v3.oas.annotations.Parameter;
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
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.pulsar.broker.admin.impl.ResourceQuotasBase;
import org.apache.pulsar.common.policies.data.ResourceQuota;

@Path("/resource-quotas")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "resource-quotas", description = "Quota admin APIs")
@SuppressWarnings("deprecation")
public class ResourceQuotas extends ResourceQuotasBase {

    @GET
    @Operation(summary = "Get the default quota")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the default quota",
                    content = @Content(array = @ArraySchema(uniqueItems = true,
                            schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission") })
    public void getDefaultResourceQuota(@Suspended AsyncResponse response) {
        getDefaultResourceQuotaAsync()
                .thenAccept(response::resume)
                .exceptionally(ex -> {
                    log.error("Failed to get default resource quota");
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @POST
    @Operation(summary = "Set the default quota")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Set the default quota",
                    content = @Content(array = @ArraySchema(uniqueItems = true,
                            schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission") })
    public void setDefaultResourceQuota(
            @Suspended AsyncResponse response,
            @RequestBody(description = "Default resource quota") ResourceQuota quota) {
        setDefaultResourceQuotaAsync(quota)
                .thenAccept(__ -> response.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("Failed to set default resource quota");
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
        });
    }

    @GET
    @Path("/{tenant}/{namespace}/{bundle}")
    @Operation(summary = "Get resource quota of a namespace bundle.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get resource quota of a namespace bundle.",
                    content = @Content(schema = @Schema(implementation = ResourceQuota.class))),
            @ApiResponse(responseCode = "307", description = "Current broker doesn't serve the namespace"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace does not exist") })
    public void getNamespaceBundleResourceQuota(
            @Suspended AsyncResponse response,
            @Parameter(description = "Tenant name")
            @PathParam("tenant") String tenant,
            @Parameter(description = "Namespace name within the specified tenant")
            @PathParam("namespace") String namespace,
            @Parameter(description = "Namespace bundle range")
            @PathParam("bundle") String bundleRange) {
        validateNamespaceName(tenant, namespace);
        internalGetNamespaceBundleResourceQuota(bundleRange)
                .thenAccept(response::resume)
                .exceptionally(ex -> {
                    log.error()
                            .attr("bundle", bundleRange)
                            .exception(ex)
                            .log("Failed to get namespace resource quota for bundle");
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/{bundle}")
    @Operation(summary = "Set resource quota on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "307", description = "Current broker doesn't serve the namespace"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification") })
    public void setNamespaceBundleResourceQuota(
            @Suspended AsyncResponse response,
            @Parameter(description = "Tenant name")
            @PathParam("tenant") String tenant,
            @Parameter(description = "Namespace name within the specified tenant")
            @PathParam("namespace") String namespace,
            @Parameter(description = "Namespace bundle range")
            @PathParam("bundle") String bundleRange,
            @RequestBody(description = "Resource quota for the specified namespace") ResourceQuota quota) {
        validateNamespaceName(tenant, namespace);
        internalSetNamespaceBundleResourceQuota(bundleRange, quota)
                .thenAccept(__ -> {
                    log.info()
                            .attr("quota", bundleRange)
                            .log("Successfully set namespace bundle resource quota");
                    response.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    log.error()
                            .attr("bundle", bundleRange)
                            .exception(ex)
                            .log("Failed to set namespace resource quota for bundle");
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{bundle}")
    @Operation(summary = "Remove resource quota for a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "307", description = "Current broker doesn't serve the namespace"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification") })
    public void removeNamespaceBundleResourceQuota(
            @Suspended AsyncResponse response,
            @Parameter(description = "Tenant name")
            @PathParam("tenant") String tenant,
            @Parameter(description = "Namespace name within the specified tenant")
            @PathParam("namespace") String namespace,
            @Parameter(description = "Namespace bundle range")
            @PathParam("bundle") String bundleRange) {
        validateNamespaceName(tenant, namespace);
        internalRemoveNamespaceBundleResourceQuota(bundleRange)
                .thenAccept(__ -> {
                    log.info()
                            .attr("quota", bundleRange)
                            .log("Successfully remove namespace bundle resource quota");
                    response.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    log.error()
                            .attr("quota", bundleRange)
                            .exception(ex)
                            .log("Failed to remove namespace bundle resource quota");
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }
}
