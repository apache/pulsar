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
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.admin.impl.ResourceQuotasBase;
import org.apache.pulsar.common.policies.data.ResourceQuota;

@Slf4j
@Path("/resource-quotas")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(value = "/resource-quotas", description = "Quota admin APIs", tags = "resource-quotas")
public class ResourceQuotas extends ResourceQuotasBase {

    @GET
    @ApiOperation(value = "Get the default quota", response = String.class, responseContainer = "Set")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void getDefaultResourceQuota(@Suspended AsyncResponse response) {
        getDefaultResourceQuotaAsync()
                .thenAccept(response::resume)
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get default resource quota", clientAppId());
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @POST
    @ApiOperation(value = "Set the default quota", response = String.class, responseContainer = "Set")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void setDefaultResourceQuota(
            @Suspended AsyncResponse response,
            @ApiParam(value = "Default resource quota") ResourceQuota quota) {
        setDefaultResourceQuotaAsync(quota)
                .thenAccept(__ -> response.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error("[{}] Failed to set default resource quota", clientAppId());
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
        });
    }

    @GET
    @Path("/{tenant}/{namespace}/{bundle}")
    @ApiOperation(value = "Get resource quota of a namespace bundle.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public void getNamespaceBundleResourceQuota(
            @Suspended AsyncResponse response,
            @ApiParam(value = "Tenant name")
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Namespace name within the specified tenant")
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Namespace bundle range")
            @PathParam("bundle") String bundleRange) {
        validateNamespaceName(tenant, namespace);
        internalGetNamespaceBundleResourceQuota(bundleRange)
                .thenAccept(response::resume)
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get namespace resource quota for bundle {}",
                            clientAppId(), bundleRange, ex);
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/{bundle}")
    @ApiOperation(value = "Set resource quota on a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void setNamespaceBundleResourceQuota(
            @Suspended AsyncResponse response,
            @ApiParam(value = "Tenant name")
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Namespace name within the specified tenant")
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Namespace bundle range")
            @PathParam("bundle") String bundleRange,
            @ApiParam(value = "Resource quota for the specified namespace") ResourceQuota quota) {
        validateNamespaceName(tenant, namespace);
        internalSetNamespaceBundleResourceQuota(bundleRange, quota)
                .thenAccept(__ -> {
                    log.info("[{}] Successfully set namespace bundle resource quota {}", clientAppId(), bundleRange);
                    response.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    log.error("[{}] Failed to set namespace resource quota for bundle {}",
                            clientAppId(), bundleRange, ex);
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{bundle}")
    @ApiOperation(value = "Remove resource quota for a namespace.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void removeNamespaceBundleResourceQuota(
            @Suspended AsyncResponse response,
            @ApiParam(value = "Tenant name")
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Namespace name within the specified tenant")
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Namespace bundle range")
            @PathParam("bundle") String bundleRange) {
        validateNamespaceName(tenant, namespace);
        internalRemoveNamespaceBundleResourceQuota(bundleRange)
                .thenAccept(__ -> {
                    log.info("[{}] Successfully remove namespace bundle resource quota {}", clientAppId(), bundleRange);
                    response.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    log.error("[{}] Failed to remove namespace bundle resource quota {}",
                            clientAppId(), bundleRange, ex);
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }
}
