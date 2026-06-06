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
package org.apache.pulsar.functions.worker.rest.api.v2;

import io.swagger.v3.oas.annotations.Operation;
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
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import lombok.CustomLog;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.FunctionApiResource;
import org.apache.pulsar.functions.worker.service.api.FunctionsV2;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

@CustomLog
@Path("/functions")
public class FunctionsApiV2Resource extends FunctionApiResource {

    FunctionsV2<? extends WorkerService> functions() {
        return get().getFunctionsV2();
    }

    @POST
    @Operation(summary = "Creates a new Pulsar Function in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "400", description = "Invalid request (function already exists, etc.)"),
            @ApiResponse(responseCode = "408", description = "Request timeout"),
            @ApiResponse(responseCode = "200", description = "Pulsar Function successfully created")
    })
    @Path("/{tenant}/{namespace}/{functionName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response registerFunction(final @PathParam("tenant") String tenant,
                                     final @PathParam("namespace") String namespace,
                                     final @PathParam("functionName") String functionName,
                                     final @FormDataParam("data") InputStream uploadedInputStream,
                                     final @FormDataParam("data") FormDataContentDisposition fileDetail,
                                     final @FormDataParam("url") String functionPkgUrl,
                                     final @FormDataParam("functionDetails") String functionDetailsJson) {

        return functions().registerFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail,
                functionPkgUrl, functionDetailsJson, authParams());
    }

    @PUT
    @Operation(summary = "Updates a Pulsar Function currently running in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "400", description = "Invalid request (function doesn't exist, etc.)"),
            @ApiResponse(responseCode = "200", description = "Pulsar Function successfully updated")
    })
    @Path("/{tenant}/{namespace}/{functionName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response updateFunction(final @PathParam("tenant") String tenant,
                                   final @PathParam("namespace") String namespace,
                                   final @PathParam("functionName") String functionName,
                                   final @FormDataParam("data") InputStream uploadedInputStream,
                                   final @FormDataParam("data") FormDataContentDisposition fileDetail,
                                   final @FormDataParam("url") String functionPkgUrl,
                                   final @FormDataParam("functionDetails") String functionDetailsJson) {

        return functions().updateFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail,
                functionPkgUrl, functionDetailsJson, authParams());
    }


    @DELETE
    @Operation(summary = "Deletes a Pulsar Function currently running in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function doesn't exist"),
            @ApiResponse(responseCode = "408", description = "Request timeout"),
            @ApiResponse(responseCode = "200", description = "The function was successfully deleted")
    })
    @Path("/{tenant}/{namespace}/{functionName}")
    public Response deregisterFunction(final @PathParam("tenant") String tenant,
                                       final @PathParam("namespace") String namespace,
                                       final @PathParam("functionName") String functionName) {
        return functions().deregisterFunction(tenant, namespace, functionName, authParams());
    }

    @GET
    @Operation(
            summary = "Fetches information about a Pulsar Function currently running in cluster mode"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "408", description = "Request timeout"),
            @ApiResponse(responseCode = "404", description = "The function doesn't exist"),
            @ApiResponse(responseCode = "200",
                    description = "Fetches information about a Pulsar Function currently running in cluster mode",
                    content = @Content(schema = @Schema(type = "object",
                            description = "FunctionMetaData (protobuf JSON format)")))
    })
    @Path("/{tenant}/{namespace}/{functionName}")
    public Response getFunctionInfo(final @PathParam("tenant") String tenant,
                                    final @PathParam("namespace") String namespace,
                                    final @PathParam("functionName") String functionName) throws IOException {

        return functions().getFunctionInfo(tenant, namespace, functionName, authParams());
    }

    @GET
    @Operation(
            summary = "Displays the status of a Pulsar Function instance"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this function"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "404", description = "The function doesn't exist"),
            @ApiResponse(responseCode = "200",
                    description = "Displays the status of a Pulsar Function instance",
                    content = @Content(schema = @Schema(type = "object",
                            description = "InstanceCommunication.FunctionStatus (protobuf JSON format)")))
    })
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/status")
    public Response getFunctionInstanceStatus(final @PathParam("tenant") String tenant,
                                              final @PathParam("namespace") String namespace,
                                              final @PathParam("functionName") String functionName,
                                              final @PathParam("instanceId") String instanceId) throws IOException {

        return functions().getFunctionInstanceStatus(tenant, namespace, functionName, instanceId, uri.getRequestUri(),
                authParams());
    }

    @GET
    @Operation(
            summary = "Displays the status of a Pulsar Function running in cluster mode"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this function"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "200",
                    description = "Displays the status of a Pulsar Function running in cluster mode",
                    content = @Content(schema = @Schema(type = "object",
                            description = "InstanceCommunication.FunctionStatus (protobuf JSON format)")))
    })
    @Path("/{tenant}/{namespace}/{functionName}/status")
    public Response getFunctionStatus(final @PathParam("tenant") String tenant,
                                      final @PathParam("namespace") String namespace,
                                      final @PathParam("functionName") String functionName) throws IOException {
        return functions().getFunctionStatusV2(tenant, namespace, functionName, uri.getRequestUri(), authParams());
    }

    @GET
    @Operation(
            summary = "Lists all Pulsar Functions currently deployed in a given namespace"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "200",
                    description = "Lists all Pulsar Functions currently deployed in a given namespace",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class))))
    })
    @Path("/{tenant}/{namespace}")
    public Response listFunctions(final @PathParam("tenant") String tenant,
                                  final @PathParam("namespace") String namespace) {
        return functions().listFunctions(tenant, namespace, authParams());
    }

    @POST
    @Operation(
            summary = "Triggers a Pulsar Function with a user-specified value or file data"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "408", description = "Request timeout"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "200",
                    description = "Triggers a Pulsar Function with a user-specified value or file data",
                    content = @Content(schema = @Schema(implementation = Message.class)))
    })
    @Path("/{tenant}/{namespace}/{functionName}/trigger")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response triggerFunction(final @PathParam("tenant") String tenant,
                                    final @PathParam("namespace") String namespace,
                                    final @PathParam("functionName") String functionName,
                                    final @FormDataParam("data") String triggerValue,
                                    final @FormDataParam("dataStream") InputStream triggerStream,
                                    final @FormDataParam("topic") String topic) {
        return functions().triggerFunction(tenant, namespace, functionName, triggerValue, triggerStream, topic,
                authParams());
    }

    @GET
    @Operation(
            summary = "Fetch the current state associated with a Pulsar Function"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "404", description = "The key does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "200",
                    description = "Fetch the current state associated with a Pulsar Function",
                    content = @Content(schema = @Schema(implementation = String.class)))
    })
    @Path("/{tenant}/{namespace}/{functionName}/state/{key}")
    public Response getFunctionState(final @PathParam("tenant") String tenant,
                                     final @PathParam("namespace") String namespace,
                                     final @PathParam("functionName") String functionName,
                                     final @PathParam("key") String key) {
        return functions().getFunctionState(tenant, namespace, functionName, key, authParams());
    }

    @POST
    @Operation(summary = "Restart function instance")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this function"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "200", description = "Restart function instance",
                    content = @Content(schema = @Schema(implementation = Void.class)))})
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response restartFunction(final @PathParam("tenant") String tenant,
                                    final @PathParam("namespace") String namespace,
                                    final @PathParam("functionName") String functionName,
                                    final @PathParam("instanceId") String instanceId) {
        return functions().restartFunctionInstance(tenant, namespace, functionName, instanceId, uri.getRequestUri(),
                authParams());
    }

    @POST
    @Operation(summary = "Restart all function instances")
    @ApiResponses(value = {@ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "200", description = "Restart all function instances",
                    content = @Content(schema = @Schema(implementation = Void.class)))})
    @Path("/{tenant}/{namespace}/{functionName}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response restartFunction(final @PathParam("tenant") String tenant,
                                    final @PathParam("namespace") String namespace,
                                    final @PathParam("functionName") String functionName) {
        return functions().restartFunctionInstances(tenant, namespace, functionName, authParams());
    }

    @POST
    @Operation(summary = "Stop function instance")
    @ApiResponses(value = {@ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "200", description = "Stop function instance",
                    content = @Content(schema = @Schema(implementation = Void.class)))})
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response stopFunction(final @PathParam("tenant") String tenant,
                                 final @PathParam("namespace") String namespace,
                                 final @PathParam("functionName") String functionName,
                                 final @PathParam("instanceId") String instanceId) {
        return functions().stopFunctionInstance(tenant, namespace, functionName, instanceId, uri.getRequestUri(),
                authParams());
    }

    @POST
    @Operation(summary = "Stop all function instances")
    @ApiResponses(value = {@ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "200", description = "Stop all function instances",
                    content = @Content(schema = @Schema(implementation = Void.class)))})
    @Path("/{tenant}/{namespace}/{functionName}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response stopFunction(final @PathParam("tenant") String tenant,
                                 final @PathParam("namespace") String namespace,
                                 final @PathParam("functionName") String functionName) {
        return functions().stopFunctionInstances(tenant, namespace, functionName, authParams());
    }

    @POST
    @Operation(
            summary = "Uploads Pulsar Function file data (admin only)",
            hidden = true
    )
    @Path("/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response uploadFunction(final @FormDataParam("data") InputStream uploadedInputStream,
                                   final @FormDataParam("path") String path) {
        return functions().uploadFunction(uploadedInputStream, path, authParams());
    }

    @GET
    @Operation(
            summary = "Downloads Pulsar Function file data (admin only)",
            hidden = true
    )
    @Path("/download")
    public Response downloadFunction(final @QueryParam("path") String path) {
        return functions().downloadFunction(path, authParams());
    }

    /**
     * Deprecated in favor of moving endpoint to {@link org.apache.pulsar.broker.admin.v2.Worker}.
     */
    @GET
    @Operation(
            summary = "Fetches a list of supported Pulsar IO connectors currently running in cluster mode"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "408", description = "Request timeout"),
            @ApiResponse(responseCode = "200",
                    description = "Fetches a list of supported Pulsar IO connectors currently running in cluster mode",
                    content = @Content(schema = @Schema(implementation = List.class)))
    })
    @Path("/connectors")
    @Deprecated
    public List<ConnectorDefinition> getConnectorsList() throws IOException {
        return functions().getListOfConnectors();
    }
}
