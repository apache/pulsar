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
package org.apache.pulsar.functions.worker.rest.api.v3;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import lombok.CustomLog;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionDefinition;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsDataImpl;
import org.apache.pulsar.common.policies.data.FunctionStatsImpl;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.FunctionApiResource;
import org.apache.pulsar.functions.worker.service.api.Functions;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

@CustomLog
@Path("/functions")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class FunctionsApiV3Resource extends FunctionApiResource {

    Functions<? extends WorkerService> functions() {
        return get().getFunctions();
    }

    @POST
    @Path("/{tenant}/{namespace}/{functionName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void registerFunction(final @PathParam("tenant") String tenant,
                                 final @PathParam("namespace") String namespace,
                                 final @PathParam("functionName") String functionName,
                                 final @FormDataParam("data") InputStream uploadedInputStream,
                                 final @FormDataParam("data") FormDataContentDisposition fileDetail,
                                 final @FormDataParam("url") String functionPkgUrl,
                                 final @FormDataParam("functionConfig") FunctionConfig functionConfig) {

        functions().registerFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail,
                functionPkgUrl, functionConfig, authParams());

    }

    @PUT
    @Path("/{tenant}/{namespace}/{functionName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void updateFunction(final @PathParam("tenant") String tenant,
                               final @PathParam("namespace") String namespace,
                               final @PathParam("functionName") String functionName,
                               final @FormDataParam("data") InputStream uploadedInputStream,
                               final @FormDataParam("data") FormDataContentDisposition fileDetail,
                               final @FormDataParam("url") String functionPkgUrl,
                               final @FormDataParam("functionConfig") FunctionConfig functionConfig,
                               final @FormDataParam("updateOptions") UpdateOptionsImpl updateOptions) {

        functions().updateFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail,
                functionPkgUrl, functionConfig, authParams(), updateOptions);

    }

    @DELETE
    @Path("/{tenant}/{namespace}/{functionName}")
    public void deregisterFunction(final @PathParam("tenant") String tenant,
                                   final @PathParam("namespace") String namespace,
                                   final @PathParam("functionName") String functionName) {
        functions().deregisterFunction(tenant, namespace, functionName, authParams());
    }

    @GET
    @Path("/{tenant}/{namespace}/{functionName}")
    public FunctionConfig getFunctionInfo(final @PathParam("tenant") String tenant,
                                          final @PathParam("namespace") String namespace,
                                          final @PathParam("functionName") String functionName) {
        return functions().getFunctionInfo(tenant, namespace, functionName, authParams());
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}")
    public List<String> listFunctions(final @PathParam("tenant") String tenant,
                                    final @PathParam("namespace") String namespace) {
        return functions().listFunctions(tenant, namespace, authParams());
    }

    @GET
    @Operation(
            summary = "Displays the status of a Pulsar Function instance"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Displays the status of a Pulsar Function instance",
                    content = @Content(schema = @Schema(
                            implementation = FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData.class))),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this function"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "404", description = "The function doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/status")
    public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData getFunctionInstanceStatus(
            final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace,
            final @PathParam("functionName") String functionName,
            final @PathParam("instanceId") String instanceId) throws IOException {
        return functions().getFunctionInstanceStatus(
                tenant, namespace, functionName, instanceId, uri.getRequestUri(), authParams());
    }

    @GET
    @Operation(
            summary = "Displays the status of a Pulsar Function"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Displays the status of a Pulsar Function",
                    content = @Content(schema = @Schema(implementation = FunctionStatus.class))),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this function"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "404", description = "The function doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{functionName}/status")
    public FunctionStatus getFunctionStatus(
            final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace,
            final @PathParam("functionName") String functionName) throws IOException {
        return functions().getFunctionStatus(
                tenant, namespace, functionName, uri.getRequestUri(), authParams());
    }

    @GET
    @Operation(
            summary = "Displays the stats of a Pulsar Function"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Displays the stats of a Pulsar Function",
                    content = @Content(schema = @Schema(implementation = FunctionStatsImpl.class))),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this function"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "404", description = "The function doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{functionName}/stats")
    public FunctionStatsImpl getFunctionStats(final @PathParam("tenant") String tenant,
                                              final @PathParam("namespace") String namespace,
                                              final @PathParam("functionName") String functionName) throws IOException {
        return functions().getFunctionStats(tenant, namespace, functionName,
                uri.getRequestUri(), authParams());
    }

    @GET
    @Operation(
            summary = "Displays the stats of a Pulsar Function instance"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Displays the stats of a Pulsar Function instance",
                    content = @Content(schema = @Schema(implementation = FunctionInstanceStatsDataImpl.class))),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this function"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "404", description = "The function doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/stats")
    public FunctionInstanceStatsDataImpl getFunctionInstanceStats(
            final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace,
            final @PathParam("functionName") String functionName,
            final @PathParam("instanceId") String instanceId) throws IOException {
        return functions().getFunctionsInstanceStats(
                tenant, namespace, functionName, instanceId, uri.getRequestUri(), authParams());
    }

    @POST
    @Path("/{tenant}/{namespace}/{functionName}/trigger")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public String triggerFunction(final @PathParam("tenant") String tenant,
                                  final @PathParam("namespace") String namespace,
                                  final @PathParam("functionName") String functionName,
                                  final @FormDataParam("data") String input,
                                  final @FormDataParam("dataStream") InputStream uploadedInputStream,
                                  final @FormDataParam("topic") String topic) {
        return functions().triggerFunction(tenant, namespace, functionName, input,
                uploadedInputStream, topic, authParams());
    }

    @POST
    @Operation(summary = "Restart function instance")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Restart function instance",
                    content = @Content(schema = @Schema(implementation = Void.class))),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this function"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartFunction(final @PathParam("tenant") String tenant,
                                final @PathParam("namespace") String namespace,
                                final @PathParam("functionName") String functionName,
                                final @PathParam("instanceId") String instanceId) {
        functions().restartFunctionInstance(tenant, namespace, functionName, instanceId,
                this.uri.getRequestUri(), authParams());
    }

    @POST
    @Operation(summary = "Restart all function instances")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Restart all function instances",
                    content = @Content(schema = @Schema(implementation = Void.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartFunction(final @PathParam("tenant") String tenant,
                                final @PathParam("namespace") String namespace,
                                final @PathParam("functionName") String functionName) {
        functions().restartFunctionInstances(tenant, namespace, functionName, authParams());
    }

    @POST
    @Operation(summary = "Stop function instance")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Stop function instance",
                    content = @Content(schema = @Schema(implementation = Void.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopFunction(final @PathParam("tenant") String tenant,
                             final @PathParam("namespace") String namespace,
                             final @PathParam("functionName") String functionName,
                             final @PathParam("instanceId") String instanceId) {
        functions().stopFunctionInstance(tenant, namespace, functionName, instanceId,
                this.uri.getRequestUri(), authParams());
    }

    @POST
    @Operation(summary = "Stop all function instances")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Stop all function instances",
                    content = @Content(schema = @Schema(implementation = Void.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopFunction(final @PathParam("tenant") String tenant,
                             final @PathParam("namespace") String namespace,
                             final @PathParam("functionName") String functionName) {
        functions().stopFunctionInstances(tenant, namespace, functionName, authParams());
    }

    @POST
    @Operation(summary = "Start function instance")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Start function instance",
                    content = @Content(schema = @Schema(implementation = Void.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startFunction(final @PathParam("tenant") String tenant,
                              final @PathParam("namespace") String namespace,
                              final @PathParam("functionName") String functionName,
                              final @PathParam("instanceId") String instanceId) {
        functions().startFunctionInstance(tenant, namespace, functionName, instanceId,
                this.uri.getRequestUri(), authParams());
    }

    @POST
    @Operation(summary = "Start all function instances")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Start all function instances",
                    content = @Content(schema = @Schema(implementation = Void.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startFunction(final @PathParam("tenant") String tenant,
                              final @PathParam("namespace") String namespace,
                              final @PathParam("functionName") String functionName) {
        functions().startFunctionInstances(tenant, namespace, functionName, authParams());
    }

    @POST
    @Path("/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void uploadFunction(final @FormDataParam("data") InputStream uploadedInputStream,
                               final @FormDataParam("path") String path) {
        functions().uploadFunction(uploadedInputStream, path, authParams());
    }

    @GET
    @Path("/download")
    public StreamingOutput downloadFunction(final @QueryParam("path") String path) {
        return functions().downloadFunction(path, authParams());
    }

    @GET
    @Operation(
            summary = "Downloads Pulsar Function file data",
            hidden = true
    )
    @Path("/{tenant}/{namespace}/{functionName}/download")
    public StreamingOutput downloadFunction(
            @Parameter(description = "The tenant of functions")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of functions")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of functions")
            final @PathParam("functionName") String functionName,
            @Parameter(description = "Whether to download the transform function")
            final @QueryParam("transform-function") boolean transformFunction) {

        return functions()
                .downloadFunction(tenant, namespace, functionName, authParams(), transformFunction);
    }

    /**
     * Deprecated in favor of moving endpoint to {@link org.apache.pulsar.broker.admin.v2.Worker}.
     */
    @GET
    @Path("/connectors")
    @Deprecated
    public List<ConnectorDefinition> getConnectorsList() throws IOException {
        return functions().getListOfConnectors();
    }

    @POST
    @Operation(
        summary = "Reload the built-in Functions"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "401", description = "This operation requires super-user access"),
        @ApiResponse(responseCode = "503",
            description = "Function worker service is now initializing. Please try again later."),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @Path("/builtins/reload")
    public void reloadBuiltinFunctions() throws IOException {
        functions().reloadBuiltinFunctions(authParams());
    }

    @GET
    @Operation(
        summary = "Fetches the list of built-in Pulsar functions"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Fetches the list of built-in Pulsar functions",
            content = @Content(array = @ArraySchema(schema = @Schema(implementation = FunctionDefinition.class)))),
        @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
        @ApiResponse(responseCode = "400", description = "Invalid request"),
        @ApiResponse(responseCode = "408", description = "Request timeout")
    })
    @Path("/builtins")
    @Produces(MediaType.APPLICATION_JSON)
    public List<FunctionDefinition> getBuiltinFunctions() {
        return functions().getBuiltinFunctions(authParams());
    }

    @GET
    @Path("/{tenant}/{namespace}/{functionName}/state/{key}")
    public FunctionState getFunctionState(final @PathParam("tenant") String tenant,
                                          final @PathParam("namespace") String namespace,
                                          final @PathParam("functionName") String functionName,
                                          final @PathParam("key") String key) throws IOException {
        return functions().getFunctionState(tenant, namespace, functionName, key, authParams());
    }

    @POST
    @Path("/{tenant}/{namespace}/{functionName}/state/{key}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void putFunctionState(final @PathParam("tenant") String tenant,
                                 final @PathParam("namespace") String namespace,
                                 final @PathParam("functionName") String functionName,
                                 final @PathParam("key") String key,
                                 final @FormDataParam("state") FunctionState stateJson) throws IOException {
        functions().putFunctionState(tenant, namespace, functionName, key, stateJson, authParams());
    }

    @PUT
    @Operation(summary = "Updates a Pulsar Function on the worker leader", hidden = true)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "403", description = "The requester doesn't have super-user permissions"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "307", description = "Redirecting to the worker leader"),
            @ApiResponse(responseCode = "200", description = "Pulsar Function successfully updated")
    })
    @Path("/leader/{tenant}/{namespace}/{functionName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void updateFunctionOnWorkerLeader(final @PathParam("tenant") String tenant,
                                             final @PathParam("namespace") String namespace,
                                             final @PathParam("functionName") String functionName,
                                             final @FormDataParam("functionMetaData")
                                                     InputStream uploadedInputStream,
                                             final @FormDataParam("delete") boolean delete) {

        functions().updateFunctionOnWorkerLeader(tenant, namespace, functionName, uploadedInputStream,
                delete, uri.getRequestUri(), authParams());
    }
}
