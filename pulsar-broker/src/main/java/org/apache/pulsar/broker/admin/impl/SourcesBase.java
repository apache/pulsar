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
package org.apache.pulsar.broker.admin.impl;

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
import jakarta.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.io.ConfigFieldDefinition;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.service.api.Sources;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

public class SourcesBase extends AdminResource {

    Sources<? extends WorkerService> sources() {
        return validateAndGetWorkerService().getSources();
    }

    @POST
    @Operation(summary = "Creates a new Pulsar Source in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Pulsar Source successfully created"),
            @ApiResponse(responseCode = "400", description =
                    "Invalid request (The Pulsar Source already exists or Tenant,"
                            + " Namespace or Name is not provided, etc.)"),
            @ApiResponse(responseCode = "401", description = "Client is not authorized to perform operation"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")

    })
    @Path("/{tenant}/{namespace}/{sourceName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void registerSource(
            @Parameter(description = "The tenant of a Pulsar Source")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Source")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Source")
            final @PathParam("sourceName") String sourceName,
            final @FormDataParam("data") InputStream uploadedInputStream,
            final @FormDataParam("data") FormDataContentDisposition fileDetail,
            final @FormDataParam("url") String sourcePkgUrl,
            @Parameter(description =
                    "You can submit a source (in any languages that you are familiar with) to a Pulsar cluster. "
                            + "Follow the steps below.\n"
                            + "1. Create a JSON object using some of the following parameters.\n"
                            + "A JSON value presenting configuration payload of a Pulsar Source."
                            + " An example of the expected Pulsar Source can be found here.\n"
                            + "- **classname**\n"
                            + "  The class name of a Pulsar Source if archive is file-url-path (file://).\n"
                            + "- **topicName**\n"
                            + "  The Pulsar topic to which data is sent.\n"
                            + "- **serdeClassName**\n"
                            + "  The SerDe classname for the Pulsar Source.\n"
                            + "- **schemaType**\n"
                            + "  The schema type (either a builtin schema like 'avro', 'json', etc. or  "
                            + "  custom Schema class name to be used to"
                            + " encode messages emitted from the Pulsar Source)\n"
                            + "- **configs**\n"
                            + "  Source config key/values\n"
                            + "- **secrets**\n"
                            + "  This is a map of secretName (that is how the secret is going"
                            + " to be accessed in the function via context) to an object that"
                            + "  encapsulates how the secret is fetched by the underlying secrets provider."
                            + " The type of a value here can be found by the"
                            + "  SecretProviderConfigurator.getSecretObjectType() method. \n"
                            + "- **parallelism**\n"
                            + "  The parallelism factor of a Pulsar Source"
                            + " (i.e. the number of a Pulsar Source instances to run).\n"
                            + "- **processingGuarantees**\n"
                            + "  The processing guarantees (aka delivery semantics) applied to the Pulsar Source.  "
                            + "  Possible Values: [ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE]\n"
                            + "- **resources**\n"
                            + "  The size of the system resources allowed by the Pulsar Source runtime."
                            + " The resources include: cpu, ram, disk.\n"
                            + "- **archive**\n"
                            + "  The path to the NAR archive for the Pulsar Source. It also supports url-path "
                            + "  [http/https/file (file protocol assumes that file already exists on worker host)] "
                            + "  from which worker can download the package.\n"
                            + "- **runtimeFlags**\n"
                            + "  Any flags that you want to pass to the runtime.\n"
                            + "2. Encapsulate the JSON object to a multipart object.")
            final @FormDataParam("sourceConfig") SourceConfig sourceConfig) {
        sources().registerSource(tenant, namespace, sourceName, uploadedInputStream, fileDetail,
            sourcePkgUrl, sourceConfig, authParams());
    }

    @PUT
    @Operation(summary = "Updates a Pulsar Source currently running in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "400", description =
                    "Invalid request (The Pulsar Source already exists or Tenant,"
                            + " Namespace or Name is not provided, etc.)"),
            @ApiResponse(responseCode = "401", description = "Client is not authorized to perform operation"),
            @ApiResponse(responseCode = "200", description = "Pulsar Source successfully updated"),
            @ApiResponse(responseCode = "404", description = "Not Found (The Pulsar Source doesn't exist)"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sourceName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void updateSource(
            @Parameter(description = "The tenant of a Pulsar Source")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Source")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Source")
            final @PathParam("sourceName") String sourceName,
            final @FormDataParam("data") InputStream uploadedInputStream,
            final @FormDataParam("data") FormDataContentDisposition fileDetail,
            final @FormDataParam("url") String sourcePkgUrl,
            @Parameter(
                    description = "A JSON value presenting configuration payload of a Pulsar Source."
                            + " An example of the expected functions can be found here.\n"
                            + "- **classname**\n"
                            + "  The class name of a Pulsar Source if archive is file-url-path (file://).\n"
                            + "- **topicName**\n"
                            + "  The Pulsar topic to which data is sent.\n"
                            + "- **serdeClassName**\n"
                            + "  The SerDe classname for the Pulsar Source.\n"
                            + "- **schemaType**\n"
                            + "  The schema type (either a builtin schema like 'avro', 'json', etc. or  "
                            + "  custom Schema class name to be used to encode"
                            + " messages emitted from the Pulsar Source)\n"
                            + "- **configs**\n"
                            + "  Pulsar Source config key/values\n"
                            + "- **secrets**\n"
                            + "  This is a map of secretName (that is how the secret is going to"
                            + " be accessed in the function via context) to an object that"
                            + "  encapsulates how the secret is fetched by the underlying secrets provider."
                            + " The type of a value here can be found by the"
                            + "  SecretProviderConfigurator.getSecretObjectType() method.\n"
                            + "- **parallelism**\n"
                            + "  The parallelism factor of a Pulsar Source"
                            + " (i.e. the number of a Pulsar Source instances to run).\n"
                            + "- **processingGuarantees**\n"
                            + "  The processing guarantees (aka delivery semantics) applied to the Pulsar Source.  "
                            + "  Possible Values: [ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE]\n"
                            + "- **resources**\n"
                            + "  The size of the system resources allowed by the Pulsar Source runtime."
                            + " The resources include: cpu, ram, disk.\n"
                            + "- **archive**\n"
                            + "  The path to the NAR archive for the Pulsar Source. It also supports url-path "
                            + "  [http/https/file (file protocol assumes that file already exists on worker host)] "
                            + "  from which worker can download the package.\n"
                            + "- **runtimeFlags**\n"
                            + "  Any flags that you want to pass to the runtime.\n")
            final @FormDataParam("sourceConfig") SourceConfig sourceConfig,
            @Parameter(description = "Update options for Pulsar Source")
            final @FormDataParam("updateOptions") UpdateOptionsImpl updateOptions) {
        sources().updateSource(tenant, namespace, sourceName, uploadedInputStream, fileDetail,
            sourcePkgUrl, sourceConfig, authParams(), updateOptions);
    }


    @DELETE
    @Operation(summary = "Deletes a Pulsar Source currently running in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "401", description = "Client is not authorized to perform operation"),
            @ApiResponse(responseCode = "404", description = "Not Found (The Pulsar Source doesn't exist)"),
            @ApiResponse(responseCode = "408", description = "Request timeout"),
            @ApiResponse(responseCode = "200", description = "The Pulsar Source was successfully deleted"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sourceName}")
    public void deregisterSource(
            @Parameter(description = "The tenant of a Pulsar Source")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Source")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Source")
            final @PathParam("sourceName") String sourceName) {
        sources().deregisterFunction(tenant, namespace, sourceName, authParams());
    }

    @GET
    @Operation(
            summary = "Fetches information about a Pulsar Source currently running in cluster mode"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Fetches information about a Pulsar Source currently running in cluster mode",
                    content = @Content(schema = @Schema(implementation = SourceConfig.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "Not Found (The Pulsar Source doesn't exist)"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sourceName}")
    public SourceConfig getSourceInfo(
            @Parameter(description = "The tenant of a Pulsar Source")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Source")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Source")
            final @PathParam("sourceName") String sourceName) throws IOException {
        return sources().getSourceInfo(tenant, namespace, sourceName, authParams());
    }

    @GET
    @Operation(
            summary = "Displays the status of a Pulsar Source instance"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Displays the status of a Pulsar Source instance",
                    content = @Content(schema = @Schema(
                            implementation = SourceStatus.SourceInstanceStatus.SourceInstanceStatusData.class))),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this source"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{sourceName}/{instanceId}/status")
    public SourceStatus.SourceInstanceStatus.SourceInstanceStatusData getSourceInstanceStatus(
            @Parameter(description = "The tenant of a Pulsar Source") final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Source") final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Source") final @PathParam("sourceName") String sourceName,
            @Parameter(description = "The instanceId of a Pulsar Source"
                    + " (if instance-id is not provided, the stats of all instances is returned).") final @PathParam(
                    "instanceId") String instanceId) throws IOException {
        return sources().getSourceInstanceStatus(
            tenant, namespace, sourceName, instanceId, uri.getRequestUri(), authParams());
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
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{sourceName}/status")
    public SourceStatus getSourceStatus(
            @Parameter(description = "The tenant of a Pulsar Source")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Source")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Source")
            final @PathParam("sourceName") String sourceName) throws IOException {
        return sources().getSourceStatus(tenant, namespace, sourceName, uri.getRequestUri(), authParams());
    }

    @GET
    @Operation(
            summary = "Lists all Pulsar Sources currently deployed in a given namespace"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Lists all Pulsar Sources currently deployed in a given namespace",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "401", description = "Client is not authorized to perform operation"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}")
    public List<String> listSources(
            @Parameter(description = "The tenant of a Pulsar Source")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Source")
            final @PathParam("namespace") String namespace) {
        return sources().listFunctions(tenant, namespace, authParams());
    }

    @POST
    @Operation(summary = "Restart an instance of a Pulsar Source")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this source"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "401", description = "Client is not authorized to perform operation"),
            @ApiResponse(responseCode = "404", description = "Not Found (The Pulsar Source doesn't exist)"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sourceName}/{instanceId}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartSource(
            @Parameter(description = "The tenant of a Pulsar Source") final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Source") final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Source") final @PathParam("sourceName") String sourceName,
            @Parameter(description = "The instanceId of a Pulsar Source"
                    + " (if instance-id is not provided, the stats of all instances is returned).") final @PathParam(
                    "instanceId") String instanceId) {
        sources().restartFunctionInstance(tenant, namespace, sourceName, instanceId,
                uri.getRequestUri(), authParams());
    }

    @POST
    @Operation(summary = "Restart all instances of a Pulsar Source")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "401", description = "Client is not authorized to perform operation"),
            @ApiResponse(responseCode = "404", description = "Not Found (The Pulsar Source doesn't exist)"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sourceName}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartSource(
            @Parameter(description = "The tenant of a Pulsar Source")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Source")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Source")
            final @PathParam("sourceName") String sourceName) {
        sources().restartFunctionInstances(tenant, namespace, sourceName, authParams());
    }

    @POST
    @Operation(summary = "Stop instance of a Pulsar Source")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "401", description = "Client is not authorized to perform operation"),
            @ApiResponse(responseCode = "404", description = "Not Found (The Pulsar Source doesn't exist)"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sourceName}/{instanceId}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopSource(
            @Parameter(description = "The tenant of a Pulsar Source") final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Source") final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Source") final @PathParam("sourceName") String sourceName,
            @Parameter(description = "The instanceId of a Pulsar Source (if instance-id is not provided,"
                    + " the stats of all instances is returned).") final @PathParam("instanceId") String instanceId) {
        sources().stopFunctionInstance(tenant, namespace, sourceName, instanceId,
                uri.getRequestUri(), authParams());
    }

    @POST
    @Operation(summary = "Stop all instances of a Pulsar Source")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "401", description = "Client is not authorized to perform operation"),
            @ApiResponse(responseCode = "404", description = "Not Found (The Pulsar Source doesn't exist)"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sourceName}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopSource(
            @Parameter(description = "The tenant of a Pulsar Source")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Source")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Source")
            final @PathParam("sourceName") String sourceName) {
        sources().stopFunctionInstances(tenant, namespace, sourceName, authParams());
    }

    @POST
    @Operation(summary = "Start an instance of a Pulsar Source")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "401", description = "Client is not authorized to perform operation"),
            @ApiResponse(responseCode = "404", description = "Not Found (The Pulsar Source doesn't exist)"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sourceName}/{instanceId}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startSource(
            @Parameter(description = "The tenant of a Pulsar Source") final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Source") final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Source") final @PathParam("sourceName") String sourceName,
            @Parameter(description = "The instanceId of a Pulsar Source (if instance-id is not provided,"
                    + " the stats of all instances is returned).") final @PathParam("instanceId") String instanceId) {
        sources().startFunctionInstance(tenant, namespace, sourceName, instanceId,
                uri.getRequestUri(), authParams());
    }

    @POST
    @Operation(summary = "Start all instances of a Pulsar Source")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "401", description = "Client is not authorized to perform operation"),
            @ApiResponse(responseCode = "404", description = "Not Found (The Pulsar Source doesn't exist)"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sourceName}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startSource(
            @Parameter(description = "The tenant of a Pulsar Source")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Source")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Source")
            final @PathParam("sourceName") String sourceName) {
        sources().startFunctionInstances(tenant, namespace, sourceName, authParams());
    }

    @GET
    @Operation(
            summary = "Fetches the list of built-in Pulsar IO sources"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Fetches the list of built-in Pulsar IO sources",
                    content = @Content(array = @ArraySchema(
                            schema = @Schema(implementation = ConnectorDefinition.class)))),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "408", description = "Request timeout"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
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
                    description =
                            "Fetches information about config fields associated with the specified builtin source",
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
            @Parameter(description = "The name of the builtin source")
            final @PathParam("name") String name) throws IOException {
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
