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

import io.swagger.annotations.*;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.policies.data.FunctionStats;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.api.FunctionsImpl;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

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
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.function.Supplier;

public class FunctionsBase extends AdminResource implements Supplier<WorkerService> {

    private final FunctionsImpl functions;

    public FunctionsBase() {
        this.functions = new FunctionsImpl(this);
    }

    @Override
    public WorkerService get() {
        return pulsar().getWorkerService();
    }

    @POST
    @ApiOperation(value = "Creates a new Pulsar Function in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 400, message = "Invalid request (function already exists, etc.)"),
            @ApiResponse(code = 408, message = "Request timeout"),
            @ApiResponse(code = 200, message = "Pulsar Function successfully created")
    })
    @Path("/{tenant}/{namespace}/{functionName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void registerFunction(
            @ApiParam(value = "The function's tenant")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The function's namespace")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The function's name")
            final @PathParam("functionName") String functionName,
            final @FormDataParam("data") InputStream uploadedInputStream,
            final @FormDataParam("data") FormDataContentDisposition fileDetail,
            final @FormDataParam("url") String functionPkgUrl,
            @ApiParam(
                    value = "A JSON value presenting a functions config playload. An example of the expected functions can be found down here.  \n" +
                            "--auto-ack  \n" +
                            "  Whether or not the framework will automatically acknowledge messages  \n" +
                            "--classname  \n" +
                            "  The function's class name  \n" +
                            "--cpu  \n" +
                            "  The cpu in cores that need to be allocated per function instance(applicable only to docker runtime)  \n" +
                            "--custom-schema-inputs  \n" +
                            "  The map of input topics to Schema class names (as a JSON string)  \n" +
                            "--custom-serde-inputs  \n" +
                            "  The map of input topics to SerDe class names (as a JSON string)  \n" +
                            "--dead-letter-topic  \n" +
                            "  The topic where all messages which could not be processed successfully are sent  \n" +
                            "--disk  \n" +
                            "  The disk in bytes that need to be allocated per function instance(applicable only to docker runtime)  \n" +
                            "--fqfn  \n" +
                            "  The Fully Qualified Function Name (FQFN) for the function  \n" +
                            "--function-config-file  \n" +
                            "  The path to a YAML config file specifying the function's configuration  \n" +
                            "--inputs  \n" +
                            "  The function's input topic or topics (multiple topics can be specified as a comma-separated list)  \n" +
                            "--jar  \n" +
                            "  Path to the jar file for the function (if the function is written in Java). " +
                            "  It also supports url-path [http/https/file (file protocol assumes that file " +
                            "  already exists on worker host)] from which worker can download the package.  \n" +
                            "--log-topic  \n" +
                            "  The topic to which the function's logs are produced  \n" +
                            "--max-message-retries  \n" +
                            "  How many times should we try to process a message before giving up  \n" +
                            "--output  \n" +
                            "  The function's output topic (If none is specified, no output is written)  \n" +
                            "--output-serde-classname  \n" +
                            "  The SerDe class to be used for messages output by the function  \n" +
                            "--parallelism  \n" +
                            "  The function's parallelism factor (i.e. the number of function instances to run)  \n" +
                            "--processing-guarantees  \n" +
                            "  The processing guarantees (aka delivery semantics) applied to the function" +
                            "  Possible Values: [ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE]  \n" +
                            "--ram  \n" +
                            "  The ram in bytes that need to be allocated per function instance(applicable only to process/docker runtime)  \n" +
                            "--retain-ordering  \n" +
                            "  Function consumes and processes messages in order  \n" +
                            "--schema-type  \n" +
                            "  The builtin schema type or custom schema class name to be used for messages output by the function" +
                            "  Default: <empty string>  \n" +
                            "--sliding-interval-count  \n" +
                            "  The number of messages after which the window slides  \n" +
                            "--sliding-interval-duration-ms  \n" +
                            "  The time duration after which the window slides  \n" +
                            "--subs-name  \n" +
                            "  Pulsar source subscription name if user wants a specific subscription-name for input-topic consumer  \n" +
                            "--timeout-ms  \n" +
                            "  The message timeout in milliseconds  \n" +
                            "--topics-pattern  \n" +
                            "  The topic pattern to consume from list of topics under a namespace that match the pattern." +
                            "  [--input] and [--topic-pattern] are mutually exclusive. Add SerDe class name for a " +
                            "  pattern in --custom-serde-inputs (supported for java fun only)  \n" +
                            "--user-config  \n" +
                            "  User-defined config key/values  \n" +
                            "--window-length-count  \n" +
                            "  The number of messages per window  \n" +
                            "--window-length-duration-ms  \n" +
                            "  The time duration of the window in milliseconds  \n",
                    examples = @Example(
                            value = @ExampleProperty(
                                    mediaType = MediaType.APPLICATION_JSON,
                                    value = "{\"inputs\": \"persistent://public/default/input-topic\", " +
                                            "\"parallelism\": \" 4 \", " +
                                            "\"output\": \"persistent://public/default/output-topic\", " +
                                            "\"jar\": \" java-function-1.0-SNAPSHOT.jar \", " +
                                            "\"classname\": \" org.example.test.ExclamationFunction \", " +
                                            "\"log-topic\": \" persistent://public/default/log-topic \"}"
                            )
                    )
            )
            final @FormDataParam("functionConfig") String functionConfigJson) {

        functions.registerFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail,
            functionPkgUrl, functionConfigJson, clientAppId(), clientAuthData());
    }

    @PUT
    @ApiOperation(value = "Updates a Pulsar Function currently running in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 400, message = "Invalid request (function doesn't exist, etc.)"),
            @ApiResponse(code = 200, message = "Pulsar Function successfully updated")
    })
    @Path("/{tenant}/{namespace}/{functionName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void updateFunction(
            @ApiParam(value = "The function's tenant")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The function's namespace")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The function's name")
            final @PathParam("functionName") String functionName,
            final @FormDataParam("data") InputStream uploadedInputStream,
            final @FormDataParam("data") FormDataContentDisposition fileDetail,
            final @FormDataParam("url") String functionPkgUrl,
            @ApiParam(
                    value = "A JSON value presenting a functions config playload. An example of the expected functions can be found down here.  \n" +
                            "--auto-ack  \n" +
                            "  Whether or not the framework will automatically acknowledge messages  \n" +
                            "--classname  \n" +
                            "  The function's class name  \n" +
                            "--cpu  \n" +
                            "  The cpu in cores that need to be allocated per function instance(applicable only to docker runtime)  \n" +
                            "--custom-schema-inputs  \n" +
                            "  The map of input topics to Schema class names (as a JSON string)  \n" +
                            "--custom-serde-inputs  \n" +
                            "  The map of input topics to SerDe class names (as a JSON string)  \n" +
                            "--dead-letter-topic  \n" +
                            "  The topic where all messages which could not be processed successfully are sent  \n" +
                            "--disk  \n" +
                            "  The disk in bytes that need to be allocated per function instance(applicable only to docker runtime)  \n" +
                            "--fqfn  \n" +
                            "  The Fully Qualified Function Name (FQFN) for the function  \n" +
                            "--function-config-file  \n" +
                            "  The path to a YAML config file specifying the function's configuration  \n" +
                            "--inputs  \n" +
                            "  The function's input topic or topics (multiple topics can be specified as a comma-separated list)  \n" +
                            "--jar  \n" +
                            "  Path to the jar file for the function (if the function is written in Java). " +
                            "  It also supports url-path [http/https/file (file protocol assumes that file " +
                            "  already exists on worker host)] from which worker can download the package.  \n" +
                            "--log-topic  \n" +
                            "  The topic to which the function's logs are produced  \n" +
                            "--max-message-retries  \n" +
                            "  How many times should we try to process a message before giving up  \n" +
                            "--output  \n" +
                            "  The function's output topic (If none is specified, no output is written)  \n" +
                            "--output-serde-classname  \n" +
                            "  The SerDe class to be used for messages output by the function  \n" +
                            "--parallelism  \n" +
                            "  The function's parallelism factor (i.e. the number of function instances to run)  \n" +
                            "--processing-guarantees  \n" +
                            "  The processing guarantees (aka delivery semantics) applied to the function" +
                            "  Possible Values: [ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE]  \n" +
                            "--ram  \n" +
                            "  The ram in bytes that need to be allocated per function instance(applicable only to process/docker runtime)  \n" +
                            "--retain-ordering  \n" +
                            "  Function consumes and processes messages in order  \n" +
                            "--schema-type  \n" +
                            "  The builtin schema type or custom schema class name to be used for messages output by the function" +
                            "  Default: <empty string>  \n" +
                            "--sliding-interval-count  \n" +
                            "  The number of messages after which the window slides  \n" +
                            "--sliding-interval-duration-ms  \n" +
                            "  The time duration after which the window slides  \n" +
                            "--subs-name  \n" +
                            "  Pulsar source subscription name if user wants a specific subscription-name for input-topic consumer  \n" +
                            "--timeout-ms  \n" +
                            "  The message timeout in milliseconds  \n" +
                            "--topics-pattern  \n" +
                            "  The topic pattern to consume from list of topics under a namespace that match the pattern." +
                            "  [--input] and [--topic-pattern] are mutually exclusive. Add SerDe class name for a " +
                            "  pattern in --custom-serde-inputs (supported for java fun only)  \n" +
                            "--update-auth-data  \n" +
                            "  Whether or not to update the auth data. Default: false  \n" +
                            "--user-config  \n" +
                            "  User-defined config key/values  \n" +
                            "--window-length-count  \n" +
                            "  The number of messages per window  \n" +
                            "--window-length-duration-ms  \n" +
                            "  The time duration of the window in milliseconds  \n",
                    examples = @Example(
                            value = @ExampleProperty(
                                    mediaType = MediaType.APPLICATION_JSON,
                                    value = "{\"inputs\": \"persistent://public/default/input-topic\", " +
                                            "\"parallelism\": \" 4 \", " +
                                            "\"output\": \"persistent://public/default/output-topic\", " +
                                            "\"jar\": \" java-function-1.0-SNAPSHOT.jar \", " +
                                            "\"classname\": \" org.example.test.ExclamationFunction \", " +
                                            "\"log-topic\": \" persistent://public/default/log-topic \"}"
                            )
                    )
            )
            final @FormDataParam("functionConfig") String functionConfigJson,
            final @FormDataParam("updateOptions") UpdateOptions updateOptions) throws IOException {

        functions.updateFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail,
                functionPkgUrl, functionConfigJson, clientAppId(), clientAuthData(), updateOptions);
    }


    @DELETE
    @ApiOperation(value = "Deletes a Pulsar Function currently running in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function doesn't exist"),
            @ApiResponse(code = 408, message = "Request timeout"),
            @ApiResponse(code = 200, message = "The function was successfully deleted")
    })
    @Path("/{tenant}/{namespace}/{functionName}")
    public void deregisterFunction(
            @ApiParam(value = "The function's tenant")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The function's namespace")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The function's name")
            final @PathParam("functionName") String functionName) {
        functions.deregisterFunction(tenant, namespace, functionName, clientAppId(), clientAuthData());
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
            @ApiResponse(code = 404, message = "The function doesn't exist")
    })
    @Path("/{tenant}/{namespace}/{functionName}")
    public FunctionConfig getFunctionInfo(
            @ApiParam(value = "The function's tenant")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The function's namespace")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The function's name")
            final @PathParam("functionName") String functionName) throws IOException {
        return functions.getFunctionInfo(tenant, namespace, functionName, clientAppId(), clientAuthData());
    }

    @GET
    @ApiOperation(
            value = "Displays the status of a Pulsar Function instance",
            response = FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "The function doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/status")
    public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData getFunctionInstanceStatus(
            @ApiParam(value = "The function's tenant")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The function's namespace")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The function's name")
            final @PathParam("functionName") String functionName,
            @ApiParam(value = "The function instanceId (Get-status of all instances if instance-id")
            final @PathParam("instanceId") String instanceId) throws IOException {
        return functions.getFunctionInstanceStatus(tenant, namespace, functionName, instanceId, uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @GET
    @ApiOperation(
            value = "Displays the status of a Pulsar Function",
            response = FunctionStatus.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "The function doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{functionName}/status")
    public FunctionStatus getFunctionStatus(
            @ApiParam(value = "The function's tenant")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The function's namespace")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The function's name")
            final @PathParam("functionName") String functionName) throws IOException {
        return functions.getFunctionStatus(tenant, namespace, functionName, uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @GET
    @ApiOperation(
            value = "Displays the stats of a Pulsar Function",
            response = FunctionStats.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "The function doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{functionName}/stats")
    public FunctionStats getFunctionStats(
            @ApiParam(value = "The function's tenant")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The function's namespace")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The function's name")
            final @PathParam("functionName") String functionName) throws IOException {
        return functions.getFunctionStats(tenant, namespace, functionName, uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @GET
    @ApiOperation(
            value = "Displays the stats of a Pulsar Function instance",
            response = FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "The function doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/stats")
    public FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData getFunctionInstanceStats(
            @ApiParam(value = "The function's tenant")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The function's namespace")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The function's name")
            final @PathParam("functionName") String functionName,
            @ApiParam(value = "The function instanceId (Get-stats of all instances if instance-id is not provided")
            final @PathParam("instanceId") String instanceId) throws IOException {
        return functions.getFunctionsInstanceStats(tenant, namespace, functionName, instanceId, uri.getRequestUri(), clientAppId(), clientAuthData());
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
            @ApiParam(value = "The function's tenant")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The function's namespace")
            final @PathParam("namespace") String namespace) {
        return functions.listFunctions(tenant, namespace, clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(
            value = "Triggers a Pulsar Function with a user-specified value or file data",
            response = Message.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 408, message = "Request timeout"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/trigger")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public String triggerFunction(
            @ApiParam(value = "The function's tenant")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The function's namespace")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The function's name")
            final @PathParam("functionName") String functionName,
            @ApiParam(value = "The value with which you want to trigger the function")
            final @FormDataParam("data") String triggerValue,
            @ApiParam(value = "The path to the file that contains the data with which you'd like to trigger the function")
            final @FormDataParam("dataStream") InputStream triggerStream,
            @ApiParam(value = "The specific topic name that the function consumes from that you want to inject the data to")
            final @FormDataParam("topic") String topic) {
        return functions.triggerFunction(tenant, namespace, functionName, triggerValue, triggerStream, topic, clientAppId(), clientAuthData());
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
            @ApiParam(value = "The function's tenant")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The function's namespace")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The function's name")
            final @PathParam("functionName") String functionName,
            @ApiParam(value = "The stats key")
            final @PathParam("key") String key) {
        return functions.getFunctionState(tenant, namespace, functionName, key, clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Restart function instance", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartFunction(
            @ApiParam(value = "The function's tenant")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The function's namespace")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The function's name")
            final @PathParam("functionName") String functionName,
            @ApiParam(value = "The function instanceId (restart all instances if instance-id is not provided")
            final @PathParam("instanceId") String instanceId) {
        functions.restartFunctionInstance(tenant, namespace, functionName, instanceId, uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Restart all function instances", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartFunction(
            @ApiParam(value = "The function's tenant")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The function's namespace")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The function's name")
            final @PathParam("functionName") String functionName) {
        functions.restartFunctionInstances(tenant, namespace, functionName, clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Stop function instance", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopFunction(
            @ApiParam(value = "The function's tenant")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The function's namespace")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The function's name")
            final @PathParam("functionName") String functionName,
            @ApiParam(value = "The function instanceId (stop all instances if instance-id is not provided")
            final @PathParam("instanceId") String instanceId) {
        functions.stopFunctionInstance(tenant, namespace, functionName, instanceId, uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Stop all function instances", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopFunction(
            @ApiParam(value = "The function's tenant")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The function's namespace")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The function's name")
            final @PathParam("functionName") String functionName) {
        functions.stopFunctionInstances(tenant, namespace, functionName, clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Start function instance", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startFunction(
            @ApiParam(value = "The function's tenant")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The function's namespace")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The function's name")
            final @PathParam("functionName") String functionName,
            @ApiParam(value = "The function instanceId (start all instances if instance-id is not provided")
            final @PathParam("instanceId") String instanceId) {
        functions.startFunctionInstance(tenant, namespace, functionName, instanceId, uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Start all function instances", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startFunction(
            @ApiParam(value = "The function's tenant")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The function's namespace")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The function's name")
            final @PathParam("functionName") String functionName) {
        functions.startFunctionInstances(tenant, namespace, functionName, clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(
            value = "Uploads Pulsar Function file data",
            hidden = true
    )
    @Path("/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void uploadFunction(final @FormDataParam("data") InputStream uploadedInputStream,
                               final @FormDataParam("path") String path) {
        functions.uploadFunction(uploadedInputStream, path);
    }

    @GET
    @ApiOperation(
            value = "Downloads Pulsar Function file data",
            hidden = true
    )
    @Path("/download")
    public StreamingOutput downloadFunction(final @QueryParam("path") String path) {
        return functions.downloadFunction(path);
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
        return functions.getListOfConnectors();
    }
}
