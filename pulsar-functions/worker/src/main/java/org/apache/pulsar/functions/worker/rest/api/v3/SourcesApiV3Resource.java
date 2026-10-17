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
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.FunctionApiResource;
import org.apache.pulsar.functions.worker.service.api.Sources;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

@CustomLog
@SuppressWarnings("deprecation")
@Tag(name = "sources", description = "Sources admin apis")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Path("/sources")
public class SourcesApiV3Resource extends FunctionApiResource {

    Sources<? extends WorkerService> sources() {
        return get().getSources();
    }

    @POST
    @Path("/{tenant}/{namespace}/{sourceName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void registerSource(final @PathParam("tenant") String tenant,
                                   final @PathParam("namespace") String namespace,
                                   final @PathParam("sourceName") String sourceName,
                                   final @FormDataParam("data") InputStream uploadedInputStream,
                                   final @FormDataParam("data") FormDataContentDisposition fileDetail,
                                   final @FormDataParam("url") String functionPkgUrl,
                                   final @FormDataParam("sourceConfig") SourceConfig sourceConfig) {

        sources().registerSource(tenant, namespace, sourceName, uploadedInputStream, fileDetail,
                functionPkgUrl, sourceConfig, authParams());

    }

    @PUT
    @Path("/{tenant}/{namespace}/{sourceName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void updateSource(final @PathParam("tenant") String tenant,
                             final @PathParam("namespace") String namespace,
                             final @PathParam("sourceName") String sourceName,
                             final @FormDataParam("data") InputStream uploadedInputStream,
                             final @FormDataParam("data") FormDataContentDisposition fileDetail,
                             final @FormDataParam("url") String functionPkgUrl,
                             final @FormDataParam("sourceConfig") SourceConfig sourceConfig,
                             final @FormDataParam("updateOptions") UpdateOptionsImpl updateOptions) {

        sources().updateSource(tenant, namespace, sourceName, uploadedInputStream, fileDetail,
                functionPkgUrl, sourceConfig, authParams(), updateOptions);
    }


    @DELETE
    @Path("/{tenant}/{namespace}/{sourceName}")
    public void deregisterSource(final @PathParam("tenant") String tenant,
                                 final @PathParam("namespace") String namespace,
                                 final @PathParam("sourceName") String sourceName) {
        sources().deregisterFunction(tenant, namespace, sourceName, authParams());
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{sourceName}")
    public SourceConfig getSourceInfo(final @PathParam("tenant") String tenant,
                                      final @PathParam("namespace") String namespace,
                                      final @PathParam("sourceName") String sourceName)
            throws IOException {
        return sources().getSourceInfo(tenant, namespace, sourceName, authParams());
    }

    @GET
    @Operation(
            summary = "Displays the status of a Pulsar Source instance"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Displays the status of a Pulsar Source instance",
                    content = @Content(schema = @Schema(
                            implementation = SourceStatus.SourceInstanceStatus.SourceInstanceStatusData.class))),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this source"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "404", description = "The source doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{sourceName}/{instanceId}/status")
    public SourceStatus.SourceInstanceStatus.SourceInstanceStatusData getSourceInstanceStatus(
            final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace,
            final @PathParam("sourceName") String sourceName,
            final @PathParam("instanceId") String instanceId) throws IOException {
        return sources().getSourceInstanceStatus(tenant, namespace, sourceName, instanceId, uri.getRequestUri(),
                authParams());
    }

    @GET
    @Operation(
            summary = "Displays the status of a Pulsar Source running in cluster mode"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Displays the status of a Pulsar Source running in cluster mode",
                    content = @Content(schema = @Schema(implementation = SourceStatus.class))),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this source"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "404", description = "The source doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{sourceName}/status")
    public SourceStatus getSourceStatus(final @PathParam("tenant") String tenant,
                                    final @PathParam("namespace") String namespace,
                                    final @PathParam("sourceName") String sourceName) throws IOException {
        return sources()
                .getSourceStatus(tenant, namespace, sourceName, uri.getRequestUri(), authParams());
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}")
    public List<String> listSources(final @PathParam("tenant") String tenant,
                                    final @PathParam("namespace") String namespace) {
        return sources().listFunctions(tenant, namespace, authParams());
    }

    @POST
    @Operation(summary = "Restart source instance")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Restart source instance",
                    content = @Content(schema = @Schema(implementation = Void.class))),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this source"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    @Path("/{tenant}/{namespace}/{sourceName}/{instanceId}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartSource(final @PathParam("tenant") String tenant,
                              final @PathParam("namespace") String namespace,
                              final @PathParam("sourceName") String sourceName,
                              final @PathParam("instanceId") String instanceId) {
        sources().restartFunctionInstance(tenant, namespace, sourceName, instanceId, this.uri.getRequestUri(),
                authParams());
    }

    @POST
    @Operation(summary = "Restart all source instances")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Restart all source instances",
                    content = @Content(schema = @Schema(implementation = Void.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error") })
    @Path("/{tenant}/{namespace}/{sourceName}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartSource(final @PathParam("tenant") String tenant,
                              final @PathParam("namespace") String namespace,
                              final @PathParam("sourceName") String sourceName) {
        sources().restartFunctionInstances(tenant, namespace, sourceName, authParams());
    }

    @POST
    @Operation(summary = "Stop source instance")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Stop source instance",
                    content = @Content(schema = @Schema(implementation = Void.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    @Path("/{tenant}/{namespace}/{sourceName}/{instanceId}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopSource(final @PathParam("tenant") String tenant,
                           final @PathParam("namespace") String namespace,
                           final @PathParam("sourceName") String sourceName,
                           final @PathParam("instanceId") String instanceId) {
        sources().stopFunctionInstance(tenant, namespace, sourceName, instanceId, this.uri.getRequestUri(),
                authParams());
    }

    @POST
    @Operation(summary = "Stop all source instances")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Stop all source instances",
                    content = @Content(schema = @Schema(implementation = Void.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    @Path("/{tenant}/{namespace}/{sourceName}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopSource(final @PathParam("tenant") String tenant,
                           final @PathParam("namespace") String namespace,
                           final @PathParam("sourceName") String sourceName) {
        sources().stopFunctionInstances(tenant, namespace, sourceName, authParams());
    }

    @POST
    @Operation(summary = "Start source instance")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Start source instance",
                    content = @Content(schema = @Schema(implementation = Void.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    @Path("/{tenant}/{namespace}/{sourceName}/{instanceId}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startSource(final @PathParam("tenant") String tenant,
                            final @PathParam("namespace") String namespace,
                            final @PathParam("sourceName") String sourceName,
                            final @PathParam("instanceId") String instanceId) {
        sources().startFunctionInstance(tenant, namespace, sourceName, instanceId, this.uri.getRequestUri(),
                authParams());
    }

    @POST
    @Operation(summary = "Start all source instances")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Start all source instances",
                    content = @Content(schema = @Schema(implementation = Void.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    @Path("/{tenant}/{namespace}/{sourceName}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startSource(final @PathParam("tenant") String tenant,
                            final @PathParam("namespace") String namespace,
                            final @PathParam("sourceName") String sourceName) {
        sources().startFunctionInstances(tenant, namespace, sourceName, authParams());
    }

    @GET
    @Operation(
            summary = "Fetches a list of supported Pulsar IO source connectors currently running in cluster mode"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Fetches a list of supported Pulsar IO source connectors currently "
                            + "running in cluster mode",
                    content = @Content(schema = @Schema(implementation = List.class))),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "408", description = "Request timeout")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/builtinsources")
    public List<ConnectorDefinition> getSourceList() {
        return sources().getSourceList();
    }

    @GET
    @Operation(
            summary = "Fetches information about config fields associated with the specified builtin source"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Fetches information about config fields associated with the specified "
                            + "builtin source",
                    content = @Content(array = @ArraySchema(
                            schema = @Schema(implementation = ConfigFieldDefinition.class)))),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "404", description = "builtin source does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/builtinsources/{name}/configdefinition")
    public List<ConfigFieldDefinition> getSourceConfigDefinition(
            @Parameter(description = "The name of the builtin source") final @PathParam("name") String name)
            throws IOException {
        return sources().getSourceConfigDefinition(name);
    }

    @POST
    @Operation(
            summary = "Reload the built-in connectors, including Sources and Sinks"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Reload the built-in connectors, including Sources and Sinks",
                    content = @Content(schema = @Schema(implementation = Void.class))),
            @ApiResponse(responseCode = "401", description = "This operation requires super-user access"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later."),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @Path("/reloadBuiltInSources")
    public void reloadSources() {
        sources().reloadConnectors(authParams());
    }
}
