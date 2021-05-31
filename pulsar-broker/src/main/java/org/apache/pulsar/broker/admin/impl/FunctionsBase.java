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
package org.apache.pulsar.broker.admin.impl;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Example;
import io.swagger.annotations.ExampleProperty;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.functions.FunctionConfig;
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
        return pulsar().getWorkerService().getFunctions();
    }

    @POST
    @ApiOperation(value = "Creates a new Pulsar Function in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 400, message = "Invalid request (The Pulsar Function already exists, etc.)"),
            @ApiResponse(code = 408, message = "Request timeout"),
            @ApiResponse(code = 200, message = "Pulsar Function successfully created")
    })
    @Path("/{tenant}/{namespace}/{functionName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void registerFunction(
            @ApiParam(value = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName,
            final @FormDataParam("data") InputStream uploadedInputStream,
            final @FormDataParam("data") FormDataContentDisposition fileDetail,
            final @FormDataParam("url") String functionPkgUrl,
            @ApiParam(
                    value = "A JSON value presenting configuration payload of a Pulsar Function."
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
                            + "   Represents either a builtin schema type (for example: 'avro', 'json', ect)"
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
                            + "  This is a map of secretName(that is how the secret is going to be accessed"
                            + " in the Pulsar Function via context) to an object that"
                            + "  encapsulates how the secret is fetched by the underlying secrets provider."
                            + " The type of an value here can be found by the"
                            + "  SecretProviderConfigurator.getSecretObjectType() method. \n"
                            + "- **cleanupSubscription**\n"
                            + "  Whether the subscriptions of a Pulsar Function created or used should be deleted"
                            + " when the Pulsar Function is deleted.\n",
                    examples = @Example(
                            value = @ExampleProperty(
                                    mediaType = MediaType.APPLICATION_JSON,
                                    value = "{\n"
                                            + "  \"inputs\": persistent://public/default/input-topic,\n"
                                            + "  \"parallelism\": 4\n"
                                            + "  \"output\": persistent://public/default/output-topic\n"
                                            + "  \"log-topic\": persistent://public/default/log-topic\n"
                                            + "  \"classname\": org.example.test.ExclamationFunction\n"
                                            + "  \"jar\": java-function-1.0-SNAPSHOT.jar\n"
                                            + "}\n"
                            )
                    )
            )
            final @FormDataParam("functionConfig") FunctionConfig functionConfig) {

        functions().registerFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail,
            functionPkgUrl, functionConfig, clientAppId(), clientAuthData());
    }

    @PUT
    @ApiOperation(value = "Updates a Pulsar Function currently running in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 400, message = "Invalid request (The Pulsar Function doesn't exist, etc.)"),
            @ApiResponse(code = 200, message = "Pulsar Function successfully updated")
    })
    @Path("/{tenant}/{namespace}/{functionName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void updateFunction(
            @ApiParam(value = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName,
            final @FormDataParam("data") InputStream uploadedInputStream,
            final @FormDataParam("data") FormDataContentDisposition fileDetail,
            final @FormDataParam("url") String functionPkgUrl,
            @ApiParam(
                    value = "A JSON value presenting configuration payload of a Pulsar Function."
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
                            + "   Represents either a builtin schema type (for example: 'avro', 'json', ect)"
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
                            + "  This is a map of secretName(that is how the secret is going to be accessed"
                            + " in the Pulsar Function via context) to an object that"
                            + "  encapsulates how the secret is fetched by the underlying secrets provider."
                            + " The type of an value here can be found by the"
                            + "  SecretProviderConfigurator.getSecretObjectType() method. \n"
                            + "- **cleanupSubscription**\n"
                            + "  Whether the subscriptions of a Pulsar Function created or used"
                            + " should be deleted when the Pulsar Function is deleted.\n",
                    examples = @Example(
                            value = @ExampleProperty(
                                    mediaType = MediaType.APPLICATION_JSON,
                                    value = "{\n"
                                            + "  \"inputs\": persistent://public/default/input-topic,\n"
                                            + "  \"parallelism\": 4\n"
                                            + "  \"output\": persistent://public/default/output-topic\n"
                                            + "  \"log-topic\": persistent://public/default/log-topic\n"
                                            + "  \"classname\": org.example.test.ExclamationFunction\n"
                                            + "  \"jar\": java-function-1.0-SNAPSHOT.jar\n"
                                            + "}\n"
                            )
                    )
            )
            final @FormDataParam("functionConfig") FunctionConfig functionConfig,
            @ApiParam(value = "The update options is for the Pulsar Function that needs to be updated.")
            final @FormDataParam("updateOptions") UpdateOptionsImpl updateOptions) throws IOException {

        functions().updateFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail,
                functionPkgUrl, functionConfig, clientAppId(), clientAuthData(), updateOptions);
    }


    @DELETE
    @ApiOperation(value = "Deletes a Pulsar Function currently running in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The Pulsar Function doesn't exist"),
            @ApiResponse(code = 408, message = "Request timeout"),
            @ApiResponse(code = 200, message = "The Pulsar Function was successfully deleted")
    })
    @Path("/{tenant}/{namespace}/{functionName}")
    public void deregisterFunction(
            @ApiParam(value = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName) {
        functions().deregisterFunction(tenant, namespace, functionName, clientAppId(), clientAuthData());
    }

    @GET
    @ApiOperation(
            value = "Fetches information about a Pulsar Function currently running in cluster mode",
            response = FunctionConfig.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 408, message = "Request timeout"),
            @ApiResponse(code = 404, message = "The Pulsar Function doesn't exist")
    })
    @Path("/{tenant}/{namespace}/{functionName}")
    public FunctionConfig getFunctionInfo(
            @ApiParam(value = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName) throws IOException {
        return functions().getFunctionInfo(tenant, namespace, functionName, clientAppId(), clientAuthData());
    }

    @GET
    @ApiOperation(
            value = "Displays the status of a Pulsar Function instance",
            response = FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this function"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "The Pulsar Function doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/status")
    public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData getFunctionInstanceStatus(
            @ApiParam(value = "The tenant of a Pulsar Function") final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Function") final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Function") final @PathParam("functionName") String functionName,
            @ApiParam(value = "The instanceId of a Pulsar Function (if instance-id is not provided,"
                    + " the stats of all instances is returned") final @PathParam("instanceId")
                    String instanceId) throws IOException {
        return functions().getFunctionInstanceStatus(tenant, namespace, functionName,
                instanceId, uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @GET
    @ApiOperation(
            value = "Displays the status of a Pulsar Function",
            response = FunctionStatus.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this function"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "The Pulsar Function doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{functionName}/status")
    public FunctionStatus getFunctionStatus(
            @ApiParam(value = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName) throws IOException {
        return functions().getFunctionStatus(tenant, namespace, functionName, uri.getRequestUri(),
                clientAppId(), clientAuthData());
    }

    @GET
    @ApiOperation(
            value = "Displays the stats of a Pulsar Function",
            response = FunctionStatsImpl.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this function"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "The Pulsar Function doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{functionName}/stats")
    public FunctionStatsImpl getFunctionStats(
            @ApiParam(value = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName) throws IOException {
        return functions().getFunctionStats(tenant, namespace, functionName,
                uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @GET
    @ApiOperation(
            value = "Displays the stats of a Pulsar Function instance",
            response = FunctionInstanceStatsDataImpl.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this function"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "The Pulsar Function doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/stats")
    public FunctionInstanceStatsDataImpl getFunctionInstanceStats(
            @ApiParam(value = "The tenant of a Pulsar Function") final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Function") final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Function") final @PathParam("functionName") String functionName,
            @ApiParam(value = "The instanceId of a Pulsar Function"
                    + " (if instance-id is not provided, the stats of all instances is returned") final @PathParam(
                    "instanceId") String instanceId) throws IOException {
        return functions().getFunctionsInstanceStats(tenant, namespace, functionName, instanceId,
                uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @GET
    @ApiOperation(
            value = "Lists all Pulsar Functions currently deployed in a given namespace",
            response = String.class,
            responseContainer = "Collection"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions")
    })
    @Path("/{tenant}/{namespace}")
    public List<String> listFunctions(
            @ApiParam(value = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace) {
        return functions().listFunctions(tenant, namespace, clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(
            value = "Triggers a Pulsar Function with a user-specified value or file data",
            response = Message.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The Pulsar Function does not exist"),
            @ApiResponse(code = 408, message = "Request timeout"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/trigger")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public String triggerFunction(
            @ApiParam(value = "The tenant of a Pulsar Function") final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Function") final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Function") final @PathParam("functionName") String functionName,
            @ApiParam(value = "The value with which you want to trigger the Pulsar Function") final @FormDataParam(
                    "data") String triggerValue,
            @ApiParam(value = "The path to the file that contains the data with"
                    + " which you'd like to trigger the Pulsar Function") final @FormDataParam("dataStream")
                    InputStream triggerStream,
            @ApiParam(value = "The specific topic name that the Pulsar Function"
                    + " consumes from which you want to inject the data to") final @FormDataParam("topic")
                    String topic) {
        return functions().triggerFunction(tenant, namespace, functionName, triggerValue,
                triggerStream, topic, clientAppId(), clientAuthData());
    }

    @GET
    @ApiOperation(
        value = "Fetch the current state associated with a Pulsar Function",
        response = FunctionState.class
    )
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "Invalid request"),
        @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
        @ApiResponse(code = 404, message = "The key does not exist"),
        @ApiResponse(code = 500, message = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/state/{key}")
    public FunctionState getFunctionState(
            @ApiParam(value = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName,
            @ApiParam(value = "The stats key")
            final @PathParam("key") String key) {
        return functions().getFunctionState(tenant, namespace, functionName, key, clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(
            value = "Put the state associated with a Pulsar Function"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "The Pulsar Function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/state/{key}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void putFunctionState(final @PathParam("tenant") String tenant,
                                 final @PathParam("namespace") String namespace,
                                 final @PathParam("functionName") String functionName,
                                 final @PathParam("key") String key,
                                 final @FormDataParam("state") FunctionState stateJson) {
        functions().putFunctionState(tenant, namespace, functionName, key, stateJson, clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Restart an instance of a Pulsar Function", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this function"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The Pulsar Function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartFunction(
            @ApiParam(value = "The tenant of a Pulsar Function") final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Function") final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Function") final @PathParam("functionName") String functionName,
            @ApiParam(value =
                    "The instanceId of a Pulsar Function (if instance-id is not provided, all instances are restarted")
            final @PathParam("instanceId") String instanceId) {
        functions().restartFunctionInstance(tenant, namespace, functionName, instanceId,
                uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Restart all instances of a Pulsar Function", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The Pulsar Function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartFunction(
            @ApiParam(value = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName) {
        functions().restartFunctionInstances(tenant, namespace, functionName, clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Stop an instance of a Pulsar Function", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The Pulsar Function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopFunction(
            @ApiParam(value = "The tenant of a Pulsar Function") final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Function") final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Function") final @PathParam("functionName") String functionName,
            @ApiParam(value =
                    "The instanceId of a Pulsar Function (if instance-id is not provided, all instances are stopped. ")
            final @PathParam("instanceId") String instanceId) {
        functions().stopFunctionInstance(tenant, namespace, functionName, instanceId,
                uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Stop all instances of a Pulsar Function", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The Pulsar Function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopFunction(
            @ApiParam(value = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName) {
        functions().stopFunctionInstances(tenant, namespace, functionName, clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Start an instance of a Pulsar Function", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The Pulsar Function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startFunction(
            @ApiParam(value = "The tenant of a Pulsar Function") final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Function") final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Function") final @PathParam("functionName") String functionName,
            @ApiParam(value = "The instanceId of a Pulsar Function"
                    + " (if instance-id is not provided, all instances sre started. ") final @PathParam("instanceId")
                    String instanceId) {
        functions().startFunctionInstance(tenant, namespace, functionName, instanceId,
                uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Start all instances of a Pulsar Function", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The Pulsar Function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startFunction(
            @ApiParam(value = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName) {
        functions().startFunctionInstances(tenant, namespace, functionName, clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(
            value = "Uploads Pulsar Function file data (Admin only)",
            hidden = true
    )
    @Path("/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void uploadFunction(final @FormDataParam("data") InputStream uploadedInputStream,
                               final @FormDataParam("path") String path) {
        functions().uploadFunction(uploadedInputStream, path, clientAppId(), clientAuthData());
    }

    @GET
    @ApiOperation(
            value = "Downloads Pulsar Function file data (Admin only)",
            hidden = true
    )
    @Path("/download")
    public StreamingOutput downloadFunction(final @QueryParam("path") String path) {
        return functions().downloadFunction(path, clientAppId(), clientAuthData());
    }

    @GET
    @ApiOperation(
            value = "Downloads Pulsar Function file data",
            hidden = true
    )
    @Path("/{tenant}/{namespace}/{functionName}/download")
    public StreamingOutput downloadFunction(
            @ApiParam(value = "The tenant of a Pulsar Function")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Function")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Function")
            final @PathParam("functionName") String functionName) {

        return functions().downloadFunction(tenant, namespace, functionName, clientAppId(), clientAuthData());
    }

    @GET
    @ApiOperation(
            value = "Fetches a list of supported Pulsar IO connectors currently running in cluster mode",
            response = List.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 408, message = "Request timeout")
    })
    @Path("/connectors")
    @Deprecated
    /**
     * Deprecated in favor of moving endpoint to {@link org.apache.pulsar.broker.admin.v2.Worker}
     */
    public List<ConnectorDefinition> getConnectorsList() throws IOException {
        return functions().getListOfConnectors();
    }

    @PUT
    @ApiOperation(value = "Updates a Pulsar Function on the worker leader", hidden = true)
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have super-user permissions"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 307, message = "Redirecting to the worker leader"),
            @ApiResponse(code = 200, message = "Pulsar Function successfully updated")
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
                delete, uri.getRequestUri(), clientAppId(), clientAuthData());
    }
}
