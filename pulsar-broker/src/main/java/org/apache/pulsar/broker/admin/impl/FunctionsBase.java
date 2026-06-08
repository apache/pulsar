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
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionDefinition;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsDataImpl;
import org.apache.pulsar.common.policies.data.FunctionStatsImpl;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.service.api.Functions;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

public class FunctionsBase extends AdminResource {

    Functions<? extends WorkerService> functions() {
        return validateAndGetWorkerService().getFunctions();
    }

    @POST
    @Operation(summary = "Creates a new Pulsar Function in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "400",
                    description = "Invalid request (The Pulsar Function already exists, etc.)"),
            @ApiResponse(responseCode = "408", description = "Request timeout"),
            @ApiResponse(responseCode = "200", description = "Pulsar Function successfully created")
    })
    @Path("/{tenant}/{namespace}/{functionName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void registerFunction(
            @Parameter(description = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName,
            final @FormDataParam("data") InputStream uploadedInputStream,
            final @FormDataParam("data") FormDataContentDisposition fileDetail,
            final @FormDataParam("url") String functionPkgUrl,
            @Parameter(
                    description = "You can submit a function (in any languages that you are familiar with) \n"
                            + "to a Pulsar cluster. Follow the steps below. \n"
                            + "1. Create a JSON object using some of the following parameters.\n"
                            + "A JSON value presenting configuration payload of a Pulsar Function.\n"
                            + " An example of the expected Pulsar Function can be found here.\n"
                            + "- **autoAck**\n"
                            + "  Whether or not the framework acknowledges messages automatically.\n"
                            + "- **runtime**\n"
                            + "  What is the runtime of the Pulsar Function. Possible Values: [JAVA, PYTHON, GO]\n"
                            + "- **resources**\n"
                            + "  The size of the system resources allowed by the Pulsar Function runtime."
                            + " The resources include: cpu, ram, disk.\n"
                            + "- **className**\n"
                            + "  The class name of a Pulsar Function.\n"
                            + "- **customSchemaInputs**\n"
                            + "  The map of input topics to Schema class names (specified as a JSON object).\n"
                            + "- **customSerdeInputs**\n"
                            + "  The map of input topics to SerDe class names (specified as a JSON object).\n"
                            + "- **deadLetterTopic**\n"
                            + "  Messages that are not processed successfully are sent to `deadLetterTopic`.\n"
                            + "- **runtimeFlags**\n"
                            + "  Any flags that you want to pass to the runtime."
                            + " Note that in thread mode, these flags have no impact.\n"
                            + "- **fqfn**\n"
                            + "  The Fully Qualified Function Name (FQFN) for the Pulsar Function.\n"
                            + "- **inputSpecs**\n"
                            + "   The map of input topics to its consumer configuration,"
                            + " each configuration has schema of "
                            + "   {\"schemaType\": \"type-x\", \"serdeClassName\": \"name-x\","
                            + " \"isRegexPattern\": true, \"receiverQueueSize\": 5}\n"
                            + "- **inputs**\n"
                            + "  The input topic or topics (multiple topics can be specified as"
                            + " a comma-separated list) of a Pulsar Function.\n"
                            + "- **jar**\n"
                            + "  Path to the JAR file for the Pulsar Function"
                            + " (if the Pulsar Function is written in Java). "
                            + "  It also supports URL path [http/https/file (file protocol assumes that file "
                            + "  already exists on worker host)] from which worker can download the package.\n"
                            + "- **py**\n"
                            + "  Path to the main Python file or Python wheel file for the"
                            + " Pulsar Function (if the Pulsar Function is written in Python).\n"
                            + "- **go**\n"
                            + "  Path to the main Go executable binary for the Pulsar Function"
                            + " (if the Pulsar Function is written in Go).\n"
                            + "- **logTopic**\n"
                            + "  The topic to which the logs of a Pulsar Function are produced.\n"
                            + "- **maxMessageRetries**\n"
                            + "  How many times should we try to process a message before giving up.\n"
                            + "- **output**\n"
                            + "  The output topic of a Pulsar Function"
                            + " (If none is specified, no output is written).\n"
                            + "- **outputSerdeClassName**\n"
                            + "  The SerDe class to be used for messages output by the Pulsar Function.\n"
                            + "- **parallelism**\n"
                            + "  The parallelism factor of a Pulsar Function"
                            + " (i.e. the number of a Pulsar Function instances to run).\n"
                            + "- **processingGuarantees**\n"
                            + "  The processing guarantees (that is, delivery semantics)"
                            + " applied to the Pulsar Function."
                            + "  Possible Values: [ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE]\n"
                            + "- **retainOrdering**\n"
                            + "  Function consumes and processes messages in order.\n"
                            + "- **outputSchemaType**\n"
                            + "   Represents either a builtin schema type (for example: 'avro', 'json', etc)"
                            + " or the class name for a Schema implementation."
                            + "- **subName**\n"
                            + "  Pulsar source subscription name. User can specify a subscription-name"
                            + " for the input-topic consumer.\n"
                            + "- **windowConfig**\n"
                            + "  The window configuration of a Pulsar Function.\n"
                            + "- **timeoutMs**\n"
                            + "  The message timeout in milliseconds.\n"
                            + "- **topicsPattern**\n"
                            + "  The topic pattern to consume from a list of topics under a namespace"
                            + " that match the pattern."
                            + "  [input] and [topic-pattern] are mutually exclusive. Add SerDe class name for a "
                            + "  pattern in customSerdeInputs (supported for java fun only)\n"
                            + "- **userConfig**\n"
                            + "  A map of user-defined configurations (specified as a JSON object).\n"
                            + "- **secrets**\n"
                            + "  This is a map of secretName (that is how the secret is going to be accessed"
                            + " in the Pulsar Function via context) to an object that"
                            + "  encapsulates how the secret is fetched by the underlying secrets provider."
                            + " The type of a value here can be found by the"
                            + "  SecretProviderConfigurator.getSecretObjectType() method. \n"
                            + "- **cleanupSubscription**\n"
                            + "  Whether the subscriptions of a Pulsar Function created or used should be deleted"
                            + " when the Pulsar Function is deleted.\n"
                            + "2. Encapsulate the JSON object to a multipart object.")
            final @FormDataParam("functionConfig") FunctionConfig functionConfig) {
        functions().registerFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail,
            functionPkgUrl, functionConfig, authParams());
    }

    @PUT
    @Operation(summary = "Updates a Pulsar Function currently running in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "400",
                    description = "Invalid request (The Pulsar Function doesn't exist, etc.)"),
            @ApiResponse(responseCode = "200", description = "Pulsar Function successfully updated")
    })
    @Path("/{tenant}/{namespace}/{functionName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void updateFunction(
            @Parameter(description = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName,
            final @FormDataParam("data") InputStream uploadedInputStream,
            final @FormDataParam("data") FormDataContentDisposition fileDetail,
            final @FormDataParam("url") String functionPkgUrl,
            @Parameter(
                    description = "A JSON value presenting configuration payload of a Pulsar Function."
                            + " An example of the expected Pulsar Function can be found here.\n"
                            + "- **autoAck**\n"
                            + "  Whether or not the framework acknowledges messages automatically.\n"
                            + "- **runtime**\n"
                            + "  What is the runtime of the Pulsar Function. Possible Values: [JAVA, PYTHON, GO]\n"
                            + "- **resources**\n"
                            + "  The size of the system resources allowed by the Pulsar Function runtime."
                            + " The resources include: cpu, ram, disk.\n"
                            + "- **className**\n"
                            + "  The class name of a Pulsar Function.\n"
                            + "- **customSchemaInputs**\n"
                            + "  The map of input topics to Schema class names (specified as a JSON object).\n"
                            + "- **customSerdeInputs**\n"
                            + "  The map of input topics to SerDe class names (specified as a JSON object).\n"
                            + "- **deadLetterTopic**\n"
                            + "  Messages that are not processed successfully are sent to `deadLetterTopic`.\n"
                            + "- **runtimeFlags**\n"
                            + "  Any flags that you want to pass to the runtime."
                            + " Note that in thread mode, these flags have no impact.\n"
                            + "- **fqfn**\n"
                            + "  The Fully Qualified Function Name (FQFN) for the Pulsar Function.\n"
                            + "- **inputSpecs**\n"
                            + "   The map of input topics to its consumer configuration,"
                            + " each configuration has schema of "
                            + "   {\"schemaType\": \"type-x\", \"serdeClassName\": \"name-x\","
                            + " \"isRegexPattern\": true, \"receiverQueueSize\": 5}\n"
                            + "- **inputs**\n"
                            + "  The input topic or topics (multiple topics can be specified as"
                            + " a comma-separated list) of a Pulsar Function.\n"
                            + "- **jar**\n"
                            + "  Path to the JAR file for the Pulsar Function"
                            + " (if the Pulsar Function is written in Java). "
                            + "  It also supports URL path [http/https/file (file protocol assumes that file "
                            + "  already exists on worker host)] from which worker can download the package.\n"
                            + "- **py**\n"
                            + "  Path to the main Python file or Python wheel file for the Pulsar Function"
                            + " (if the Pulsar Function is written in Python).\n"
                            + "- **go**\n"
                            + "  Path to the main Go executable binary for the Pulsar Function"
                            + " (if the Pulsar Function is written in Go).\n"
                            + "- **logTopic**\n"
                            + "  The topic to which the logs of a Pulsar Function are produced.\n"
                            + "- **maxMessageRetries**\n"
                            + "  How many times should we try to process a message before giving up.\n"
                            + "- **output**\n"
                            + "  The output topic of a Pulsar Function (If none is specified, no output is written).\n"
                            + "- **outputSerdeClassName**\n"
                            + "  The SerDe class to be used for messages output by the Pulsar Function.\n"
                            + "- **parallelism**\n"
                            + "  The parallelism factor of a Pulsar Function "
                            + "(i.e. the number of a Pulsar Function instances to run).\n"
                            + "- **processingGuarantees**\n"
                            + "  The processing guarantees (that is, delivery semantics)"
                            + " applied to the Pulsar Function."
                            + "  Possible Values: [ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE]\n"
                            + "- **retainOrdering**\n"
                            + "  Function consumes and processes messages in order.\n"
                            + "- **outputSchemaType**\n"
                            + "   Represents either a builtin schema type (for example: 'avro', 'json', etc)"
                            + " or the class name for a Schema implementation."
                            + "- **subName**\n"
                            + "  Pulsar source subscription name. User can specify"
                            + " a subscription-name for the input-topic consumer.\n"
                            + "- **windowConfig**\n"
                            + "  The window configuration of a Pulsar Function.\n"
                            + "- **timeoutMs**\n"
                            + "  The message timeout in milliseconds.\n"
                            + "- **topicsPattern**\n"
                            + "  The topic pattern to consume from a list of topics"
                            + " under a namespace that match the pattern."
                            + "  [input] and [topic-pattern] are mutually exclusive. Add SerDe class name for a "
                            + "  pattern in customSerdeInputs (supported for java fun only)\n"
                            + "- **userConfig**\n"
                            + "  A map of user-defined configurations (specified as a JSON object).\n"
                            + "- **secrets**\n"
                            + "  This is a map of secretName (that is how the secret is going to be accessed"
                            + " in the Pulsar Function via context) to an object that"
                            + "  encapsulates how the secret is fetched by the underlying secrets provider."
                            + " The type of a value here can be found by the"
                            + "  SecretProviderConfigurator.getSecretObjectType() method. \n"
                            + "- **cleanupSubscription**\n"
                            + "  Whether the subscriptions of a Pulsar Function created or used"
                            + " should be deleted when the Pulsar Function is deleted.\n")
            final @FormDataParam("functionConfig") FunctionConfig functionConfig,
            @Parameter(description = "The update options is for the Pulsar Function that needs to be updated.")
            final @FormDataParam("updateOptions") UpdateOptionsImpl updateOptions) throws IOException {

        functions().updateFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail,
                functionPkgUrl, functionConfig, authParams(), updateOptions);
    }


    @DELETE
    @Operation(summary = "Deletes a Pulsar Function currently running in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The Pulsar Function doesn't exist"),
            @ApiResponse(responseCode = "408", description = "Request timeout"),
            @ApiResponse(responseCode = "200", description = "The Pulsar Function was successfully deleted")
    })
    @Path("/{tenant}/{namespace}/{functionName}")
    public void deregisterFunction(
            @Parameter(description = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName) {
        functions().deregisterFunction(tenant, namespace, functionName, authParams());
    }

    @GET
    @Operation(
            summary = "Fetches information about a Pulsar Function currently running in cluster mode"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Fetches information about a Pulsar Function currently running in cluster mode",
                    content = @Content(schema = @Schema(implementation = FunctionConfig.class))),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "408", description = "Request timeout"),
            @ApiResponse(responseCode = "404", description = "The Pulsar Function doesn't exist")
    })
    @Path("/{tenant}/{namespace}/{functionName}")
    public FunctionConfig getFunctionInfo(
            @Parameter(description = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName) throws IOException {
        return functions().getFunctionInfo(tenant, namespace, functionName, authParams());
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
            @ApiResponse(responseCode = "404", description = "The Pulsar Function doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/status")
    public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData getFunctionInstanceStatus(
            @Parameter(description = "The tenant of a Pulsar Function") final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName,
            @Parameter(description = "The instanceId of a Pulsar Function (if instance-id is not provided,"
                    + " the stats of all instances is returned)") final @PathParam("instanceId")
                    String instanceId) throws IOException {
        return functions().getFunctionInstanceStatus(tenant, namespace, functionName,
                instanceId, uri.getRequestUri(), authParams());
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
            @ApiResponse(responseCode = "404", description = "The Pulsar Function doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{functionName}/status")
    public FunctionStatus getFunctionStatus(
            @Parameter(description = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName) throws IOException {
        return functions().getFunctionStatus(tenant, namespace, functionName, uri.getRequestUri(),
                authParams());
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
            @ApiResponse(responseCode = "404", description = "The Pulsar Function doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{functionName}/stats")
    public FunctionStatsImpl getFunctionStats(
            @Parameter(description = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Function")
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
            @ApiResponse(responseCode = "404", description = "The Pulsar Function doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/stats")
    public FunctionInstanceStatsDataImpl getFunctionInstanceStats(
            @Parameter(description = "The tenant of a Pulsar Function") final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName,
            @Parameter(description = "The instanceId of a Pulsar Function"
                    + " (if instance-id is not provided, the stats of all instances is returned)") final @PathParam(
                    "instanceId") String instanceId) throws IOException {
        return functions().getFunctionsInstanceStats(tenant, namespace, functionName, instanceId,
                uri.getRequestUri(), authParams());
    }

    @GET
    @Operation(
            summary = "Lists all Pulsar Functions currently deployed in a given namespace"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Lists all Pulsar Functions currently deployed in a given namespace",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions")
    })
    @Path("/{tenant}/{namespace}")
    public List<String> listFunctions(
            @Parameter(description = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace) {
        return functions().listFunctions(tenant, namespace, authParams());
    }

    @POST
    @Operation(
            summary = "Triggers a Pulsar Function with a user-specified value or file data"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Triggers a Pulsar Function with a user-specified value or file data",
                    content = @Content(schema = @Schema(implementation = String.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The Pulsar Function does not exist"),
            @ApiResponse(responseCode = "408", description = "Request timeout"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/trigger")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public String triggerFunction(
            @Parameter(description = "The tenant of a Pulsar Function") final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName,
            @Parameter(description = "The value with which you want to trigger the Pulsar Function")
            final @FormDataParam("data") String triggerValue,
            @Parameter(description = "The path to the file that contains the data with"
                    + " which you'd like to trigger the Pulsar Function") final @FormDataParam("dataStream")
                    InputStream triggerStream,
            @Parameter(description = "The specific topic name that the Pulsar Function"
                    + " consumes from which you want to inject the data to") final @FormDataParam("topic")
                    String topic) {
        return functions().triggerFunction(tenant, namespace, functionName, triggerValue,
                triggerStream, topic, authParams());
    }

    @GET
    @Operation(
        summary = "Fetch the current state associated with a Pulsar Function"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Fetch the current state associated with a Pulsar Function",
                content = @Content(schema = @Schema(implementation = FunctionState.class))),
        @ApiResponse(responseCode = "400", description = "Invalid request"),
        @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
        @ApiResponse(responseCode = "404", description = "The key does not exist"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/state/{key}")
    public FunctionState getFunctionState(
            @Parameter(description = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName,
            @Parameter(description = "The stats key")
            final @PathParam("key") String key) {
        return functions().getFunctionState(tenant, namespace, functionName, key, authParams());
    }

    @POST
    @Operation(
            summary = "Put the state associated with a Pulsar Function"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "404", description = "The Pulsar Function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/state/{key}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void putFunctionState(final @PathParam("tenant") String tenant,
                                 final @PathParam("namespace") String namespace,
                                 final @PathParam("functionName") String functionName,
                                 final @PathParam("key") String key,
                                 final @FormDataParam("state") FunctionState stateJson) {
        functions().putFunctionState(tenant, namespace, functionName, key, stateJson, authParams());
    }

    @POST
    @Operation(summary = "Restart an instance of a Pulsar Function")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this function"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The Pulsar Function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartFunction(
            @Parameter(description = "The tenant of a Pulsar Function") final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName,
            @Parameter(description =
                    "The instanceId of a Pulsar Function (if instance-id is not provided, all instances are restarted)")
            final @PathParam("instanceId") String instanceId) {
        functions().restartFunctionInstance(tenant, namespace, functionName, instanceId,
                uri.getRequestUri(), authParams());
    }

    @POST
    @Operation(summary = "Restart all instances of a Pulsar Function")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The Pulsar Function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartFunction(
            @Parameter(description = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName) {
        functions().restartFunctionInstances(tenant, namespace, functionName, authParams());
    }

    @POST
    @Operation(summary = "Stop an instance of a Pulsar Function")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The Pulsar Function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopFunction(
            @Parameter(description = "The tenant of a Pulsar Function") final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName,
            @Parameter(description =
                    "The instanceId of a Pulsar Function (if instance-id is not provided, all instances are stopped.)")
            final @PathParam("instanceId") String instanceId) {
        functions().stopFunctionInstance(tenant, namespace, functionName, instanceId,
                uri.getRequestUri(), authParams());
    }

    @POST
    @Operation(summary = "Stop all instances of a Pulsar Function")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The Pulsar Function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopFunction(
            @Parameter(description = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName) {
        functions().stopFunctionInstances(tenant, namespace, functionName, authParams());
    }

    @POST
    @Operation(summary = "Start an instance of a Pulsar Function")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The Pulsar Function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startFunction(
            @Parameter(description = "The tenant of a Pulsar Function") final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName,
            @Parameter(description = "The instanceId of a Pulsar Function"
                    + " (if instance-id is not provided, all instances are started.)") final @PathParam("instanceId")
                    String instanceId) {
        functions().startFunctionInstance(tenant, namespace, functionName, instanceId,
                uri.getRequestUri(), authParams());
    }

    @POST
    @Operation(summary = "Start all instances of a Pulsar Function")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "404", description = "The Pulsar Function does not exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startFunction(
            @Parameter(description = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName) {
        functions().startFunctionInstances(tenant, namespace, functionName, authParams());
    }

    @POST
    @Operation(
            summary = "Uploads Pulsar Function file data (Admin only)",
            hidden = true
    )
    @Path("/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void uploadFunction(final @FormDataParam("data") InputStream uploadedInputStream,
                               final @FormDataParam("path") String path) {
        functions().uploadFunction(uploadedInputStream, path, authParams());
    }

    @GET
    @Operation(
            summary = "Downloads Pulsar Function file data (Admin only)",
            hidden = true
    )
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
            @Parameter(description = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @Parameter(description = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @Parameter(description = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName,
            @Parameter(description = "Whether to download the transform-function")
            final @QueryParam("transform-function") boolean transformFunction) {

        return functions().downloadFunction(tenant, namespace, functionName, authParams(), transformFunction);
    }

    @GET
    @Operation(
            summary = "Fetches a list of supported Pulsar IO connectors currently running in cluster mode"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Fetches a list of supported Pulsar IO connectors currently running in cluster mode",
                    content = @Content(array =
                            @ArraySchema(schema = @Schema(implementation = ConnectorDefinition.class)))),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "408", description = "Request timeout")
    })
    @Path("/connectors")
    @Deprecated
    /**
     * Deprecated in favor of moving endpoint to {@link org.apache.pulsar.broker.admin.v2.Worker}
     */
    public List<ConnectorDefinition> getConnectorsList() throws IOException {
        return functions().getListOfConnectors();
    }

    @POST
    @Operation(
            summary = "Reload the built-in Functions"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
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
                    content = @Content(array =
                            @ArraySchema(schema = @Schema(implementation = FunctionDefinition.class)))),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "408", description = "Request timeout")
    })
    @Path("/builtins")
    @Produces(MediaType.APPLICATION_JSON)
    public List<FunctionDefinition> getBuiltinFunction() {
        return functions().getBuiltinFunctions(authParams());
    }

    @PUT
    @Operation(summary = "Updates a Pulsar Function on the worker leader", hidden = true)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation successful"),
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
