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
import io.swagger.v3.oas.annotations.media.ExampleObject;
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
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.service.api.Sinks;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

public class SinksBase extends AdminResource {

    Sinks<? extends WorkerService> sinks() {
        return validateAndGetWorkerService().getSinks();
    }

    @POST
    @Operation(summary = "Creates a new Pulsar Sink in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "400", description = "Invalid request (The Pulsar Sink already exists, etc.)"),
            @ApiResponse(responseCode = "200", description = "Pulsar Sink successfully created"),
            @ApiResponse(responseCode = "500", description =
                    "Internal server error (failed to authorize,"
                            + " failed to get tenant data, failed to process package, etc.)"),
            @ApiResponse(responseCode = "401", description = "Client is not authorized to perform operation"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sinkName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void registerSink(@Parameter(description = "The tenant of a Pulsar Sink")
                             final @PathParam("tenant") String tenant,
                             @Parameter(description = "The namespace of a Pulsar Sink") final @PathParam("namespace")
                                     String namespace,
                             @Parameter(description = "The name of a Pulsar Sink") final @PathParam("sinkName")
                                         String sinkName,
                             final @FormDataParam("data") InputStream uploadedInputStream,
                             final @FormDataParam("data") FormDataContentDisposition fileDetail,
                             final @FormDataParam("url") String sinkPkgUrl,
                             @Parameter(description =
                                     "You can submit a sink (in any languages that you are familiar with) "
                                             + "to a Pulsar cluster. Follow the steps below.\n"
                                             + "1. Create a JSON object using some of the following parameters.\n"
                                             + "A JSON value presenting config payload of a Pulsar Sink."
                                             + " All available configuration options are:\n"
                                             + "- **classname**\n"
                                             + "   The class name of a Pulsar Sink if"
                                             + " archive is file-url-path (file://)\n"
                                             + "- **sourceSubscriptionName**\n"
                                             + "   Pulsar source subscription name if"
                                             + " user wants a specific\n"
                                             + "   subscription-name for input-topic consumer\n"
                                             + "- **inputs**\n"
                                             + "   The input topic or topics of"
                                             + " a Pulsar Sink (specified as a JSON array)\n"
                                             + "- **topicsPattern**\n"
                                             + "   TopicsPattern to consume from list of topics under a namespace that "
                                             + "   match the pattern. [input] and [topicsPattern] are mutually "
                                             + "   exclusive. Add SerDe class name for a pattern in customSerdeInputs "
                                             + "   (supported for java fun only)\n"
                                             + "- **topicToSerdeClassName**\n"
                                             + "   The map of input topics to SerDe class names"
                                             + " (specified as a JSON object)\n"
                                             + "- **topicToSchemaType**\n"
                                             + "   The map of input topics to Schema types or class names"
                                             + " (specified as a JSON object)\n"
                                             + "- **inputSpecs**\n"
                                             + "   The map of input topics to its consumer configuration,"
                                             + " each configuration has schema of "
                                             + "   {\"schemaType\": \"type-x\", \"serdeClassName\": \"name-x\","
                                             + " \"isRegexPattern\": true, \"receiverQueueSize\": 5}\n"
                                             + "- **configs**\n"
                                             + "   The map of configs (specified as a JSON object)\n"
                                             + "- **secrets**\n"
                                             + "   a map of secretName (aka how the secret is going to be \n"
                                             + "   accessed in the function via context) to an object that \n"
                                             + "   encapsulates how the secret is fetched by the underlying \n"
                                             + "   secrets provider. The type of a value here can be found by the \n"
                                             + "   SecretProviderConfigurator.getSecretObjectType() method."
                                             + " (specified as a JSON object)\n"
                                             + "- **parallelism**\n"
                                             + "   The parallelism factor of a Pulsar Sink"
                                             + " (i.e. the number of a Pulsar Sink instances to run)\n"
                                             + "- **processingGuarantees**\n"
                                             + "   The processing guarantees (aka delivery semantics) applied to"
                                             + " the Pulsar Sink. Possible Values: \"ATLEAST_ONCE\","
                                             + " \"ATMOST_ONCE\", \"EFFECTIVELY_ONCE\"\n"
                                             + "- **retainOrdering**\n"
                                             + "   Boolean denotes whether the Pulsar Sink"
                                             + " consumes and processes messages in order\n"
                                             + "- **resources**\n"
                                             + "   {\"cpu\": 1, \"ram\": 2, \"disk\": 3} The CPU (in cores),"
                                             + " RAM (in bytes) and disk (in bytes) that needs to be "
                                             + "allocated per Pulsar Sink instance "
                                             + "(applicable only to Docker runtime)\n"
                                             + "- **autoAck**\n"
                                             + "   Boolean denotes whether or not the framework"
                                             + " will automatically acknowledge messages\n"
                                             + "- **timeoutMs**\n"
                                             + "   Long denotes the message timeout in milliseconds\n"
                                             + "- **cleanupSubscription**\n"
                                             + "   Boolean denotes whether the subscriptions the functions"
                                             + " created/used should be deleted when the functions is deleted\n"
                                             + "- **runtimeFlags**\n"
                                             + "   Any flags that you want to pass to the runtime as a single string\n"
                                             + "2. Encapsulate the JSON object to a multipart object.",
                                     content = @Content(
                                             mediaType = MediaType.TEXT_PLAIN,
                                             schema = @Schema(implementation = SinkConfig.class),
                                             examples = @ExampleObject(
                                                     value = """
                                                             Example
                                                             1. Create a JSON object.
                                                              {
                                                               "classname": "org.example.MySinkTest",
                                                               "inputs": ["persistent://public/default/sink-input"],
                                                               "processingGuarantees": "EFFECTIVELY_ONCE",
                                                               "parallelism": "10"
                                                              }
                                                             2. Encapsulate the JSON object to a multipart object \
                                                             (in Python).
                                                             from requests_toolbelt.multipart.encoder import \
                                                             MultipartEncoder
                                                             mp_encoder = MultipartEncoder(\
                                                              [('sinkConfig',\
                                                             (None, json.dumps(config), 'application/json'))])
                                                             """
                                             )
                                    )
                             )
                             final @FormDataParam("sinkConfig") SinkConfig sinkConfig) {
        sinks().registerSink(tenant, namespace, sinkName, uploadedInputStream, fileDetail,
                sinkPkgUrl, sinkConfig, authParams());
    }

    @PUT
    @Operation(summary = "Updates a Pulsar Sink currently running in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "400", description =
                    "Invalid request (The Pulsar Sink doesn't exist, update contains no change, etc.)"),
            @ApiResponse(responseCode = "200", description = "Pulsar Sink successfully updated"),
            @ApiResponse(responseCode = "401", description = "Client is not authorized to perform operation"),
            @ApiResponse(responseCode = "404", description = "The Pulsar Sink doesn't exist"),
            @ApiResponse(responseCode = "500", description =
                    "Internal server error (failed to authorize, failed to process package, etc.)"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sinkName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void updateSink(@Parameter(description = "The tenant of a Pulsar Sink")
                           final @PathParam("tenant") String tenant,
                           @Parameter(description = "The namespace of a Pulsar Sink") final @PathParam("namespace")
                                   String namespace,
                           @Parameter(description = "The name of a Pulsar Sink")
                           final @PathParam("sinkName") String sinkName,
                           final @FormDataParam("data") InputStream uploadedInputStream,
                           final @FormDataParam("data") FormDataContentDisposition fileDetail,
                           final @FormDataParam("url") String sinkPkgUrl,
                           @Parameter(description =
                                   "A JSON value presenting config payload of a Pulsar Sink."
                                           + " All available configuration options are:\n"
                                           + "- **classname**\n"
                                           + "   The class name of a Pulsar Sink if"
                                           + " archive is file-url-path (file://)\n"
                                           + "- **sourceSubscriptionName**\n"
                                           + "   Pulsar source subscription name if user wants a specific\n"
                                           + "   subscription-name for input-topic consumer\n"
                                           + "- **inputs**\n"
                                           + "   The input topic or topics of"
                                           + " a Pulsar Sink (specified as a JSON array)\n"
                                           + "- **topicsPattern**\n"
                                           + "   TopicsPattern to consume from list of topics under a namespace that "
                                           + "   match the pattern. [input] and [topicsPattern] are mutually "
                                           + "   exclusive. Add SerDe class name for a pattern in customSerdeInputs "
                                           + "   (supported for java fun only)\n"
                                           + "- **topicToSerdeClassName**\n"
                                           + "   The map of input topics to"
                                           + " SerDe class names (specified as a JSON object)\n"
                                           + "- **topicToSchemaType**\n"
                                           + "   The map of input topics to Schema types or"
                                           + " class names (specified as a JSON object)\n"
                                           + "- **inputSpecs**\n"
                                           + "   The map of input topics to its consumer configuration,"
                                           + " each configuration has schema of "
                                           + "   {\"schemaType\": \"type-x\", \"serdeClassName\": \"name-x\","
                                           + " \"isRegexPattern\": true, \"receiverQueueSize\": 5}\n"
                                           + "- **configs**\n"
                                           + "   The map of configs (specified as a JSON object)\n"
                                           + "- **secrets**\n"
                                           + "   a map of secretName (aka how the secret is going to be \n"
                                           + "   accessed in the function via context) to an object that \n"
                                           + "   encapsulates how the secret is fetched by the underlying \n"
                                           + "   secrets provider. The type of a value here can be found by the \n"
                                           + "   SecretProviderConfigurator.getSecretObjectType() method."
                                           + " (specified as a JSON object)\n"
                                           + "- **parallelism**\n"
                                           + "   The parallelism factor of a Pulsar Sink "
                                           + "(i.e. the number of a Pulsar Sink instances to run)\n"
                                           + "- **processingGuarantees**\n"
                                           + "   The processing guarantees (aka delivery semantics) applied to the"
                                           + " Pulsar Sink. Possible Values: \"ATLEAST_ONCE\", \"ATMOST_ONCE\","
                                           + " \"EFFECTIVELY_ONCE\"\n"
                                           + "- **retainOrdering**\n"
                                           + "   Boolean denotes whether the Pulsar Sink"
                                           + " consumes and processes messages in order\n"
                                           + "- **resources**\n"
                                           + "   {\"cpu\": 1, \"ram\": 2, \"disk\": 3} The CPU (in cores),"
                                           + " RAM (in bytes) and disk (in bytes) that needs to be allocated per"
                                           + " Pulsar Sink instance (applicable only to Docker runtime)\n"
                                           + "- **autoAck**\n"
                                           + "   Boolean denotes whether or not the framework will"
                                           + " automatically acknowledge messages\n"
                                           + "- **timeoutMs**\n"
                                           + "   Long denotes the message timeout in milliseconds\n"
                                           + "- **cleanupSubscription**\n"
                                           + "   Boolean denotes whether the subscriptions the functions"
                                           + " created/used should be deleted when the functions is deleted\n"
                                           + "- **runtimeFlags**\n"
                                           + "   Any flags that you want to pass to the runtime as a single string\n",
                                   content = @Content(
                                           mediaType = MediaType.APPLICATION_JSON,
                                           schema = @Schema(implementation = SinkConfig.class),
                                           examples = @ExampleObject(
                                                   value = """
                                                           {
                                                           "classname": "org.example.SinkStressTest",
                                                           "inputs": ["persistent://public/default/sink-input"],
                                                           "processingGuarantees": "EFFECTIVELY_ONCE",
                                                           "parallelism": 5
                                                           }
                                                           """
                                           )
                               )
                           )
                           final @FormDataParam("sinkConfig") SinkConfig sinkConfig,
                           @Parameter(description = "Update options for the Pulsar Sink")
                           final @FormDataParam("updateOptions") UpdateOptionsImpl updateOptions) {
         sinks().updateSink(tenant, namespace, sinkName, uploadedInputStream, fileDetail,
                sinkPkgUrl, sinkConfig, authParams(), updateOptions);

    }


    @DELETE
    @Operation(summary = "Deletes a Pulsar Sink currently running in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "400", description = "Invalid deregister request"),
            @ApiResponse(responseCode = "404", description = "The Pulsar Sink does not exist"),
            @ApiResponse(responseCode = "200", description = "The Pulsar Sink was successfully deleted"),
            @ApiResponse(responseCode = "401", description = "Client is not authorized to perform operation"),
            @ApiResponse(responseCode = "500", description =
                    "Internal server error (failed to authorize, failed to deregister, etc.)"),
            @ApiResponse(responseCode = "408",
                    description = "Got InterruptedException while deregistering the Pulsar Sink"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sinkName}")
    public void deregisterSink(@Parameter(description = "The tenant of a Pulsar Sink")
                               final @PathParam("tenant") String tenant,
                               @Parameter(description = "The namespace of a Pulsar Sink")
                               final @PathParam("namespace") String namespace,
                               @Parameter(description = "The name of a Pulsar Sink")
                               final @PathParam("sinkName") String sinkName) {
        sinks().deregisterFunction(tenant, namespace, sinkName, authParams());
    }

    @GET
    @Operation(
            summary = "Fetches information about a Pulsar Sink currently running in cluster mode"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Fetches information about a Pulsar Sink currently running in cluster mode",
                    content = @Content(schema = @Schema(implementation = SinkConfig.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The Pulsar Sink does not exist"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sinkName}")
    public SinkConfig getSinkInfo(@Parameter(description = "The tenant of a Pulsar Sink")
                                  final @PathParam("tenant") String tenant,
                                  @Parameter(description = "The namespace of a Pulsar Sink")
                                  final @PathParam("namespace") String namespace,
                                  @Parameter(description = "The name of a Pulsar Sink")
                                  final @PathParam("sinkName") String sinkName) throws IOException {
        return sinks().getSinkInfo(tenant, namespace, sinkName, authParams());
    }

    @GET
    @Operation(
            summary = "Displays the status of a Pulsar Sink instance"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Displays the status of a Pulsar Sink instance",
                    content = @Content(schema =
                            @Schema(implementation = SinkStatus.SinkInstanceStatus.SinkInstanceStatusData.class))),
            @ApiResponse(responseCode = "307", description = "Current broker doesn't serve the namespace of this sink"),
            @ApiResponse(responseCode = "400", description = "The Pulsar Sink instance does not exist"),
            @ApiResponse(responseCode = "404", description = "The Pulsar Sink does not exist"),
            @ApiResponse(responseCode = "500",
                    description = "Internal Server Error (got exception while getting status, etc.)"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{sinkName}/{instanceId}/status")
    public SinkStatus.SinkInstanceStatus.SinkInstanceStatusData getSinkInstanceStatus(
            @Parameter(description = "The tenant of a Pulsar Sink")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Sink")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Sink")
            final @PathParam("sinkName") String sinkName,
            @Parameter(description = "The instanceId of a Pulsar Sink")
            final @PathParam("instanceId") String instanceId) throws IOException {
        return sinks().getSinkInstanceStatus(
            tenant, namespace, sinkName, instanceId, uri.getRequestUri(), authParams());
    }

    @GET
    @Operation(
            summary = "Displays the status of a Pulsar Sink running in cluster mode"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Displays the status of a Pulsar Sink running in"
                    + " cluster mode",
                    content = @Content(schema = @Schema(implementation = SinkStatus.class))),
            @ApiResponse(responseCode = "307", description = "Current broker doesn't serve the namespace of this sink"),
            @ApiResponse(responseCode = "400", description = "Invalid get status request"),
            @ApiResponse(responseCode = "401", description = "The client is not authorized to perform this operation"),
            @ApiResponse(responseCode = "404", description = "The Pulsar Sink does not exist"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later."),
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{sinkName}/status")
    public SinkStatus getSinkStatus(@Parameter(description = "The tenant of a Pulsar Sink")
                                    final @PathParam("tenant") String tenant,
                                    @Parameter(description = "The namespace of a Pulsar Sink")
                                    final @PathParam("namespace") String namespace,
                                    @Parameter(description = "The name of a Pulsar Sink")
                                    final @PathParam("sinkName") String sinkName) throws IOException {
        return sinks().getSinkStatus(tenant, namespace, sinkName, uri.getRequestUri(), authParams());
    }

    @GET
    @Operation(
            summary = "Lists all Pulsar Sinks currently deployed in a given namespace"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Lists all Pulsar Sinks currently deployed in a given namespace",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "400", description = "Invalid list request"),
            @ApiResponse(responseCode = "401", description = "The client is not authorized to perform this operation"),
            @ApiResponse(responseCode = "500", description = "Internal server error (failed to authorize, etc.)"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}")
    public List<String> listSinks(@Parameter(description = "The tenant of a Pulsar Sink")
                                  final @PathParam("tenant") String tenant,
                                  @Parameter(description = "The namespace of a Pulsar Sink")
                                  final @PathParam("namespace") String namespace) {
        return sinks().listFunctions(tenant, namespace, authParams());
    }

    @POST
    @Operation(summary = "Restart an instance of a Pulsar Sink")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "307", description = "Current broker doesn't serve the namespace of this sink"),
            @ApiResponse(responseCode = "400", description = "Invalid restart request"),
            @ApiResponse(responseCode = "401", description = "The client is not authorized to perform this operation"),
            @ApiResponse(responseCode = "404", description = "The Pulsar Sink does not exist"),
            @ApiResponse(responseCode = "500", description =
                    "Internal server error (failed to restart the instance of"
                            + " a Pulsar Sink, failed to authorize, etc.)"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sinkName}/{instanceId}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartSink(@Parameter(description = "The tenant of a Pulsar Sink")
                            final @PathParam("tenant") String tenant,
                            @Parameter(description = "The namespace of a Pulsar Sink")
                            final @PathParam("namespace") String namespace,
                            @Parameter(description = "The name of a Pulsar Sink")
                            final @PathParam("sinkName") String sinkName,
                            @Parameter(description = "The instanceId of a Pulsar Sink")
                            final @PathParam("instanceId") String instanceId) {
        sinks().restartFunctionInstance(tenant, namespace, sinkName, instanceId,
                uri.getRequestUri(), authParams());
    }

    @POST
    @Operation(summary = "Restart all instances of a Pulsar Sink")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "400", description = "Invalid restart request"),
            @ApiResponse(responseCode = "401", description = "The client is not authorized to perform this operation"),
            @ApiResponse(responseCode = "404", description = "The Pulsar Sink does not exist"),
            @ApiResponse(responseCode = "500", description =
                    "Internal server error (failed to restart the Pulsar Sink, failed to authorize, etc.)"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sinkName}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartSink(@Parameter(description = "The tenant of a Pulsar Sink")
                            final @PathParam("tenant") String tenant,
                            @Parameter(description = "The namespace of a Pulsar Sink")
                            final @PathParam("namespace") String namespace,
                            @Parameter(description = "The name of a Pulsar Sink")
                            final @PathParam("sinkName") String sinkName) {
        sinks().restartFunctionInstances(tenant, namespace, sinkName, authParams());
    }

    @POST
    @Operation(summary = "Stop an instance of a Pulsar Sink")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "400", description = "Invalid stop request"),
            @ApiResponse(responseCode = "404", description = "The Pulsar Sink instance does not exist"),
            @ApiResponse(responseCode = "500", description =
                    "Internal server error (failed to stop the Pulsar Sink, failed to authorize, etc.)"),
            @ApiResponse(responseCode = "401", description = "The client is not authorized to perform this operation"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sinkName}/{instanceId}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopSink(@Parameter(description = "The tenant of a Pulsar Sink")
                         final @PathParam("tenant") String tenant,
                         @Parameter(description = "The namespace of a Pulsar Sink")
                         final @PathParam("namespace") String namespace,
                         @Parameter(description = "The name of a Pulsar Sink")
                         final @PathParam("sinkName") String sinkName,
                         @Parameter(description = "The instanceId of a Pulsar Sink")
                         final @PathParam("instanceId") String instanceId) {
        sinks().stopFunctionInstance(tenant, namespace,
                sinkName, instanceId, uri.getRequestUri(), authParams());
    }

    @POST
    @Operation(summary = "Stop all instances of a Pulsar Sink")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "400", description = "Invalid stop request"),
            @ApiResponse(responseCode = "404", description = "The Pulsar Sink does not exist"),
            @ApiResponse(responseCode = "500", description =
                    "Internal server error (failed to stop the Pulsar Sink, failed to authorize, etc.)"),
            @ApiResponse(responseCode = "401", description = "The client is not authorized to perform this operation"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sinkName}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopSink(@Parameter(description = "The tenant of a Pulsar Sink")
                         final @PathParam("tenant") String tenant,
                         @Parameter(description = "The namespace of a Pulsar Sink")
                         final @PathParam("namespace") String namespace,
                         @Parameter(description = "The name of a Pulsar Sink")
                         final @PathParam("sinkName") String sinkName) {
        sinks().stopFunctionInstances(tenant, namespace, sinkName, authParams());
    }

    @POST
    @Operation(summary = "Start an instance of a Pulsar Sink")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "400", description = "Invalid start request"),
            @ApiResponse(responseCode = "404", description = "The Pulsar Sink does not exist"),
            @ApiResponse(responseCode = "500", description =
                    "Internal server error (failed to start the Pulsar Sink, failed to authorize, etc.)"),
            @ApiResponse(responseCode = "401", description = "The client is not authorized to perform this operation"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sinkName}/{instanceId}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startSink(@Parameter(description = "The tenant of a Pulsar Sink")
                          final @PathParam("tenant") String tenant,
                          @Parameter(description = "The namespace of a Pulsar Sink")
                          final @PathParam("namespace") String namespace,
                          @Parameter(description = "The name of a Pulsar Sink")
                          final @PathParam("sinkName") String sinkName,
                          @Parameter(description = "The instanceId of a Pulsar Sink")
                          final @PathParam("instanceId") String instanceId) {
        sinks().startFunctionInstance(tenant, namespace, sinkName, instanceId,
                uri.getRequestUri(), authParams());
    }

    @POST
    @Operation(summary = "Start all instances of a Pulsar Sink")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "400", description = "Invalid start request"),
            @ApiResponse(responseCode = "404", description = "The Pulsar Sink does not exist"),
            @ApiResponse(responseCode = "500", description =
                    "Internal server error (failed to start the Pulsar Sink, failed to authorize, etc.)"),
            @ApiResponse(responseCode = "401", description = "The client is not authorized to perform this operation"),
            @ApiResponse(responseCode = "503",
                    description = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sinkName}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startSink(@Parameter(description = "The tenant of a Pulsar Sink")
                          final @PathParam("tenant") String tenant,
                          @Parameter(description = "The namespace of a Pulsar Sink")
                          final @PathParam("namespace") String namespace,
                          @Parameter(description = "The name of a Pulsar Sink")
                          final @PathParam("sinkName") String sinkName) {
        sinks().startFunctionInstances(tenant, namespace, sinkName, authParams());
    }

    @GET
    @Operation(
            summary = "Fetches the list of built-in Pulsar IO sinks"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get builtin sinks successfully.",
                    content = @Content(array =
                            @ArraySchema(schema = @Schema(implementation = ConnectorDefinition.class))))
    })
    @Path("/builtinsinks")
    public List<ConnectorDefinition> getSinkList() {
        return sinks().getSinkList();
    }

    @GET
    @Operation(
            summary = "Fetches information about config fields associated with the specified builtin sink"
    )
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
    @Path("/reloadBuiltInSinks")
    public void reloadSinks() {
        sinks().reloadConnectors(authParams());
    }
}
