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
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import lombok.CustomLog;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.io.ConfigFieldDefinition;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.FunctionApiResource;
import org.apache.pulsar.functions.worker.service.api.Sinks;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

@CustomLog
@SuppressWarnings("deprecation")
@Tag(name = "sinks", description = "Sinks admin apis")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Path("/sinks")
public class SinksApiV3Resource extends FunctionApiResource {

    Sinks<? extends WorkerService> sinks() {
        return get().getSinks();
    }

    @POST
    @Path("/{tenant}/{namespace}/{sinkName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void registerSink(final @PathParam("tenant") String tenant,
                             final @PathParam("namespace") String namespace,
                             final @PathParam("sinkName") String sinkName,
                             final @FormDataParam("data") InputStream uploadedInputStream,
                             final @FormDataParam("data") FormDataContentDisposition fileDetail,
                             final @FormDataParam("url") String functionPkgUrl,
                             final @FormDataParam("sinkConfig") SinkConfig sinkConfig) {

        sinks().registerSink(tenant, namespace, sinkName, uploadedInputStream, fileDetail,
                functionPkgUrl, sinkConfig, authParams());
    }

    @PUT
    @Path("/{tenant}/{namespace}/{sinkName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void updateSink(final @PathParam("tenant") String tenant,
                           final @PathParam("namespace") String namespace,
                           final @PathParam("sinkName") String sinkName,
                           final @FormDataParam("data") InputStream uploadedInputStream,
                           final @FormDataParam("data") FormDataContentDisposition fileDetail,
                           final @FormDataParam("url") String functionPkgUrl,
                           final @FormDataParam("sinkConfig") SinkConfig sinkConfig,
                           final @FormDataParam("updateOptions") UpdateOptionsImpl updateOptions) {

        sinks().updateSink(tenant, namespace, sinkName, uploadedInputStream, fileDetail,
                functionPkgUrl, sinkConfig, authParams(), updateOptions);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{sinkName}")
    public void deregisterSink(final @PathParam("tenant") String tenant,
                               final @PathParam("namespace") String namespace,
                               final @PathParam("sinkName") String sinkName) {
        sinks().deregisterFunction(tenant, namespace, sinkName, authParams());
    }

    @GET
    @Path("/{tenant}/{namespace}/{sinkName}")
    public SinkConfig getSinkInfo(final @PathParam("tenant") String tenant,
                                  final @PathParam("namespace") String namespace,
                                  final @PathParam("sinkName") String sinkName)
            throws IOException {
        return sinks().getSinkInfo(tenant, namespace, sinkName, authParams());
    }

    @GET
    @Operation(summary = "Displays the status of a Pulsar Sink instance")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Displays the status of a Pulsar Sink instance",
                    content = @Content(schema =
                            @Schema(implementation = SinkStatus.SinkInstanceStatus.SinkInstanceStatusData.class))),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this sink"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "404", description = "The sink doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{sinkName}/{instanceId}/status")
    public SinkStatus.SinkInstanceStatus.SinkInstanceStatusData getSinkInstanceStatus(
            final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace,
            final @PathParam("sinkName") String sinkName,
            final @PathParam("instanceId") String instanceId) throws IOException {
        return sinks().getSinkInstanceStatus(tenant, namespace, sinkName, instanceId, uri.getRequestUri(),
                authParams());
    }

    @GET
    @Operation(summary = "Displays the status of a Pulsar Sink running in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Displays the status of a Pulsar Sink running in cluster mode",
                    content = @Content(schema = @Schema(implementation = SinkStatus.class))),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this sink"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "404", description = "The sink doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{sinkName}/status")
    public SinkStatus getSinkStatus(final @PathParam("tenant") String tenant,
                                    final @PathParam("namespace") String namespace,
                                    final @PathParam("sinkName") String sinkName) throws IOException {
        return sinks().getSinkStatus(tenant, namespace, sinkName, uri.getRequestUri(), authParams());
    }

    @GET
    @Path("/{tenant}/{namespace}")
    public List<String> listSink(final @PathParam("tenant") String tenant,
                                 final @PathParam("namespace") String namespace) {
        return sinks().listFunctions(tenant, namespace, authParams());
    }

    @POST
    @Operation(summary = "Restart sink instance")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Restart sink instance",
                content = @Content(schema = @Schema(implementation = Void.class))),
        @ApiResponse(responseCode = "307", description = "Current broker doesn't serve the namespace of this sink"),
        @ApiResponse(responseCode = "400", description = "Invalid request"),
        @ApiResponse(responseCode = "404", description = "The function does not exist"),
        @ApiResponse(responseCode = "500", description = "Internal server error") })
    @Path("/{tenant}/{namespace}/{sinkName}/{instanceId}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartSink(final @PathParam("tenant") String tenant,
                            final @PathParam("namespace") String namespace,
                            final @PathParam("sinkName") String sinkName,
                            final @PathParam("instanceId") String instanceId) {
        sinks().restartFunctionInstance(tenant, namespace, sinkName, instanceId, this.uri.getRequestUri(),
                authParams());
    }

    @POST
    @Operation(summary = "Restart all sink instances")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Restart all sink instances",
                    content = @Content(schema = @Schema(implementation = Void.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    @Path("/{tenant}/{namespace}/{sinkName}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartSink(final @PathParam("tenant") String tenant,
                            final @PathParam("namespace") String namespace,
                            final @PathParam("sinkName") String sinkName) {
        sinks().restartFunctionInstances(tenant, namespace, sinkName, authParams());
    }

    @POST
    @Operation(summary = "Stop sink instance")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Stop sink instance",
                    content = @Content(schema = @Schema(implementation = Void.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    @Path("/{tenant}/{namespace}/{sinkName}/{instanceId}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopSink(final @PathParam("tenant") String tenant,
                         final @PathParam("namespace") String namespace,
                         final @PathParam("sinkName") String sinkName,
                         final @PathParam("instanceId") String instanceId) {
        sinks().stopFunctionInstance(tenant, namespace, sinkName, instanceId, this.uri.getRequestUri(),
                authParams());
    }

    @POST
    @Operation(summary = "Stop all sink instances")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Stop all sink instances",
                    content = @Content(schema = @Schema(implementation = Void.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error") })
    @Path("/{tenant}/{namespace}/{sinkName}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopSink(final @PathParam("tenant") String tenant,
                         final @PathParam("namespace") String namespace,
                         final @PathParam("sinkName") String sinkName) {
        sinks().stopFunctionInstances(tenant, namespace, sinkName, authParams());
    }

    @POST
    @Operation(summary = "Start sink instance")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Start sink instance",
                    content = @Content(schema = @Schema(implementation = Void.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    @Path("/{tenant}/{namespace}/{sinkName}/{instanceId}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startSink(final @PathParam("tenant") String tenant,
                          final @PathParam("namespace") String namespace,
                          final @PathParam("sinkName") String sinkName,
                          final @PathParam("instanceId") String instanceId) {
        sinks().startFunctionInstance(tenant, namespace, sinkName, instanceId, this.uri.getRequestUri(),
                authParams());
    }

    @POST
    @Operation(summary = "Start all sink instances")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Start all sink instances",
                    content = @Content(schema = @Schema(implementation = Void.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    @Path("/{tenant}/{namespace}/{sinkName}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startSink(final @PathParam("tenant") String tenant,
                          final @PathParam("namespace") String namespace,
                          final @PathParam("sinkName") String sinkName) {
        sinks().startFunctionInstances(tenant, namespace, sinkName, authParams());
    }

    @GET
    @Path("/builtinsinks")
    public List<ConnectorDefinition> getSinkList() {
        return sinks().getSinkList();
    }

    @GET
    @Operation(
            summary = "Fetches information about config fields associated with the specified builtin sink")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Fetches information about config fields associated with the specified builtin sink",
                    content = @Content(array =
                            @ArraySchema(schema = @Schema(implementation = ConfigFieldDefinition.class)))),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "404", description = "builtin sink does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/builtinsinks/{name}/configdefinition")
    public List<ConfigFieldDefinition> getSinkConfigDefinition(
            @Parameter(description = "The name of the builtin sink")
            final @PathParam("name") String name) throws IOException {
        return sinks().getSinkConfigDefinition(name);
    }

    @POST
    @Operation(summary = "Reload the built-in connectors, including Sources and Sinks")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Reload the built-in connectors, including Sources and Sinks",
                    content = @Content(schema = @Schema(implementation = Void.class))),
            @ApiResponse(responseCode = "401", description = "This operation requires super-user access"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later."),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @Path("/reloadBuiltInSinks")
    public void reloadSinks() {
        sinks().reloadConnectors(authParams());
    }
}
