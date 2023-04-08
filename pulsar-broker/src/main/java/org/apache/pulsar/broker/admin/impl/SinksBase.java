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
import javax.ws.rs.core.MediaType;
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
    @ApiOperation(value = "Creates a new Pulsar Sink in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request (The Pulsar Sink already exists, etc.)"),
            @ApiResponse(code = 200, message = "Pulsar Sink successfully created"),
            @ApiResponse(code = 500, message =
                    "Internal server error (failed to authorize,"
                            + " failed to get tenant data, failed to process package, etc.)"),
            @ApiResponse(code = 401, message = "Client is not authorized to perform operation"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sinkName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void registerSink(@ApiParam(value = "The tenant of a Pulsar Sink") final @PathParam("tenant") String tenant,
                             @ApiParam(value = "The namespace of a Pulsar Sink") final @PathParam("namespace")
                                     String namespace,
                             @ApiParam(value = "The name of a Pulsar Sink") final @PathParam("sinkName")
                                         String sinkName,
                             final @FormDataParam("data") InputStream uploadedInputStream,
                             final @FormDataParam("data") FormDataContentDisposition fileDetail,
                             final @FormDataParam("url") String sinkPkgUrl,
                             @ApiParam(value =
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
                                             + "   (supported for java fun only)"
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
                                             + "   a map of secretName(aka how the secret is going to be \n"
                                             + "   accessed in the function via context) to an object that \n"
                                             + "   encapsulates how the secret is fetched by the underlying \n"
                                             + "   secrets provider. The type of an value here can be found by the \n"
                                             + "   SecretProviderConfigurator.getSecretObjectType() method."
                                             + " (specified as a JSON object)\n"
                                             + "- **parallelism**\n"
                                             + "   The parallelism factor of a Pulsar Sink"
                                             + " (i.e. the number of a Pulsar Sink instances to run \n"
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
                                     examples = @Example(
                                             value = {
                                                 @ExampleProperty(
                                                     mediaType = MediaType.TEXT_PLAIN,
                                                     value = "Example \n"
                                                             + "\n"
                                                             + " 1. Create a JSON object. \n"
                                                             + "\n"
                                                             + "{\n"
                                                             + "\t\"classname\": \"org.example.MySinkTest\",\n"
                                                             + "\t\"inputs\": ["
                                                             + "\"persistent://public/default/sink-input\"],\n"
                                                             + "\t\"processingGuarantees\": \"EFFECTIVELY_ONCE\",\n"
                                                             + "\t\"parallelism\": \"10\"\n"
                                                             + "}\n"
                                                             + "\n"
                                                             + "\n"
                                                             + "2. Encapsulate the JSON object to a multipart object "
                                                             + "(in Python).\n"
                                                             + "\n"
                                                             + "from requests_toolbelt.multipart.encoder import "
                                                             + "MultipartEncoder \n"
                                                             + "mp_encoder = MultipartEncoder( \n"
                                                             + "\t[('sinkConfig', "
                                                             + "(None, json.dumps(config), 'application/json'))])\n"
                                                  )
                                            }
                                    )
                             )
                             final @FormDataParam("sinkConfig") SinkConfig sinkConfig) {
        sinks().registerSink(tenant, namespace, sinkName, uploadedInputStream, fileDetail,
                sinkPkgUrl, sinkConfig, authParams());
    }

    @PUT
    @ApiOperation(value = "Updates a Pulsar Sink currently running in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message =
                    "Invalid request (The Pulsar Sink doesn't exist, update contains no change, etc.)"),
            @ApiResponse(code = 200, message = "Pulsar Sink successfully updated"),
            @ApiResponse(code = 401, message = "Client is not authorized to perform operation"),
            @ApiResponse(code = 404, message = "The Pulsar Sink doesn't exist"),
            @ApiResponse(code = 500, message =
                    "Internal server error (failed to authorize, failed to process package, etc.)"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sinkName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void updateSink(@ApiParam(value = "The tenant of a Pulsar Sink") final @PathParam("tenant") String tenant,
                           @ApiParam(value = "The namespace of a Pulsar Sink") final @PathParam("namespace")
                                   String namespace,
                           @ApiParam(value = "The name of a Pulsar Sink") final @PathParam("sinkName") String sinkName,
                           final @FormDataParam("data") InputStream uploadedInputStream,
                           final @FormDataParam("data") FormDataContentDisposition fileDetail,
                           final @FormDataParam("url") String sinkPkgUrl,
                           @ApiParam(value =
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
                                           + "   (supported for java fun only)"
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
                                           + "   a map of secretName(aka how the secret is going to be \n"
                                           + "   accessed in the function via context) to an object that \n"
                                           + "   encapsulates how the secret is fetched by the underlying \n"
                                           + "   secrets provider. The type of an value here can be found by the \n"
                                           + "   SecretProviderConfigurator.getSecretObjectType() method."
                                           + " (specified as a JSON object)\n"
                                           + "- **parallelism**\n"
                                           + "   The parallelism factor of a Pulsar Sink "
                                           + "(i.e. the number of a Pulsar Sink instances to run \n"
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
                                   examples = @Example(
                                           value = @ExampleProperty(
                                                   mediaType = MediaType.APPLICATION_JSON,
                                                   value = "{\n"
                                                           + "\t\"classname\": \"org.example.SinkStressTest\",\n"
                                                           + "\t\"inputs\": ["
                                                           + "\"persistent://public/default/sink-input\"],\n"
                                                           + "\t\"processingGuarantees\": \"EFFECTIVELY_ONCE\",\n"
                                                           + "\t\"parallelism\": 5\n"
                                                           + "}"
                                           )
                               )
                           )
                           final @FormDataParam("sinkConfig") SinkConfig sinkConfig,
                           @ApiParam(value = "Update options for the Pulsar Sink")
                           final @FormDataParam("updateOptions") UpdateOptionsImpl updateOptions) {
         sinks().updateSink(tenant, namespace, sinkName, uploadedInputStream, fileDetail,
                sinkPkgUrl, sinkConfig, authParams(), updateOptions);

    }


    @DELETE
    @ApiOperation(value = "Deletes a Pulsar Sink currently running in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid deregister request"),
            @ApiResponse(code = 404, message = "The Pulsar Sink does not exist"),
            @ApiResponse(code = 200, message = "The Pulsar Sink was successfully deleted"),
            @ApiResponse(code = 401, message = "Client is not authorized to perform operation"),
            @ApiResponse(code = 500, message =
                    "Internal server error (failed to authorize, failed to deregister, etc.)"),
            @ApiResponse(code = 408, message = "Got InterruptedException while deregistering the Pulsar Sink"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sinkName}")
    public void deregisterSink(@ApiParam(value = "The tenant of a Pulsar Sink")
                               final @PathParam("tenant") String tenant,
                               @ApiParam(value = "The namespace of a Pulsar Sink")
                               final @PathParam("namespace") String namespace,
                               @ApiParam(value = "The name of a Pulsar Sink")
                               final @PathParam("sinkName") String sinkName) {
        sinks().deregisterFunction(tenant, namespace, sinkName, authParams());
    }

    @GET
    @ApiOperation(
            value = "Fetches information about a Pulsar Sink currently running in cluster mode",
            response = SinkConfig.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The Pulsar Sink does not exist"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sinkName}")
    public SinkConfig getSinkInfo(@ApiParam(value = "The tenant of a Pulsar Sink")
                                  final @PathParam("tenant") String tenant,
                                  @ApiParam(value = "The namespace of a Pulsar Sink")
                                  final @PathParam("namespace") String namespace,
                                  @ApiParam(value = "The name of a Pulsar Sink")
                                  final @PathParam("sinkName") String sinkName) throws IOException {
        return sinks().getSinkInfo(tenant, namespace, sinkName, authParams());
    }

    @GET
    @ApiOperation(
            value = "Displays the status of a Pulsar Sink instance",
            response = SinkStatus.SinkInstanceStatus.SinkInstanceStatusData.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this sink"),
            @ApiResponse(code = 400, message = "The Pulsar Sink instance does not exist"),
            @ApiResponse(code = 404, message = "The Pulsar Sink does not exist"),
            @ApiResponse(code = 500, message = "Internal Server Error (got exception while getting status, etc.)"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{sinkName}/{instanceId}/status")
    public SinkStatus.SinkInstanceStatus.SinkInstanceStatusData getSinkInstanceStatus(
            @ApiParam(value = "The tenant of a Pulsar Sink")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Sink")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Sink")
            final @PathParam("sinkName") String sinkName,
            @ApiParam(value = "The instanceId of a Pulsar Sink")
            final @PathParam("instanceId") String instanceId) throws IOException {
        return sinks().getSinkInstanceStatus(
            tenant, namespace, sinkName, instanceId, uri.getRequestUri(), authParams());
    }

    @GET
    @ApiOperation(
            value = "Displays the status of a Pulsar Sink running in cluster mode",
            response = SinkStatus.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this sink"),
            @ApiResponse(code = 400, message = "Invalid get status request"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation"),
            @ApiResponse(code = 404, message = "The Pulsar Sink does not exist"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later."),
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{sinkName}/status")
    public SinkStatus getSinkStatus(@ApiParam(value = "The tenant of a Pulsar Sink")
                                    final @PathParam("tenant") String tenant,
                                    @ApiParam(value = "The namespace of a Pulsar Sink")
                                    final @PathParam("namespace") String namespace,
                                    @ApiParam(value = "The name of a Pulsar Sink")
                                    final @PathParam("sinkName") String sinkName) throws IOException {
        return sinks().getSinkStatus(tenant, namespace, sinkName, uri.getRequestUri(), authParams());
    }

    @GET
    @ApiOperation(
            value = "Lists all Pulsar Sinks currently deployed in a given namespace",
            response = String.class,
            responseContainer = "Collection"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid list request"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation"),
            @ApiResponse(code = 500, message = "Internal server error (failed to authorize, etc.)"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}")
    public List<String> listSinks(@ApiParam(value = "The tenant of a Pulsar Sink")
                                  final @PathParam("tenant") String tenant,
                                  @ApiParam(value = "The namespace of a Pulsar Sink")
                                  final @PathParam("namespace") String namespace) {
        return sinks().listFunctions(tenant, namespace, authParams());
    }

    @POST
    @ApiOperation(value = "Restart an instance of a Pulsar Sink", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this sink"),
            @ApiResponse(code = 400, message = "Invalid restart request"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation"),
            @ApiResponse(code = 404, message = "The Pulsar Sink does not exist"),
            @ApiResponse(code = 500, message =
                    "Internal server error (failed to restart the instance of"
                            + " a Pulsar Sink, failed to authorize, etc.)"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sinkName}/{instanceId}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartSink(@ApiParam(value = "The tenant of a Pulsar Sink")
                            final @PathParam("tenant") String tenant,
                            @ApiParam(value = "The namespace of a Pulsar Sink")
                            final @PathParam("namespace") String namespace,
                            @ApiParam(value = "The name of a Pulsar Sink")
                            final @PathParam("sinkName") String sinkName,
                            @ApiParam(value = "The instanceId of a Pulsar Sink")
                            final @PathParam("instanceId") String instanceId) {
        sinks().restartFunctionInstance(tenant, namespace, sinkName, instanceId,
                uri.getRequestUri(), authParams());
    }

    @POST
    @ApiOperation(value = "Restart all instances of a Pulsar Sink", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid restart request"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation"),
            @ApiResponse(code = 404, message = "The Pulsar Sink does not exist"),
            @ApiResponse(code = 500, message =
                    "Internal server error (failed to restart the Pulsar Sink, failed to authorize, etc.)"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sinkName}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartSink(@ApiParam(value = "The tenant of a Pulsar Sink")
                            final @PathParam("tenant") String tenant,
                            @ApiParam(value = "The namespace of a Pulsar Sink")
                            final @PathParam("namespace") String namespace,
                            @ApiParam(value = "The name of a Pulsar Sink")
                            final @PathParam("sinkName") String sinkName) {
        sinks().restartFunctionInstances(tenant, namespace, sinkName, authParams());
    }

    @POST
    @ApiOperation(value = "Stop an instance of a Pulsar Sink", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid stop request"),
            @ApiResponse(code = 404, message = "The Pulsar Sink instance does not exist"),
            @ApiResponse(code = 500, message =
                    "Internal server error (failed to stop the Pulsar Sink, failed to authorize, etc.)"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sinkName}/{instanceId}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopSink(@ApiParam(value = "The tenant of a Pulsar Sink")
                         final @PathParam("tenant") String tenant,
                         @ApiParam(value = "The namespace of a Pulsar Sink")
                         final @PathParam("namespace") String namespace,
                         @ApiParam(value = "The name of a Pulsar Sink")
                         final @PathParam("sinkName") String sinkName,
                         @ApiParam(value = "The instanceId of a Pulsar Sink")
                         final @PathParam("instanceId") String instanceId) {
        sinks().stopFunctionInstance(tenant, namespace,
                sinkName, instanceId, uri.getRequestUri(), authParams());
    }

    @POST
    @ApiOperation(value = "Stop all instances of a Pulsar Sink", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid stop request"),
            @ApiResponse(code = 404, message = "The Pulsar Sink does not exist"),
            @ApiResponse(code = 500, message =
                    "Internal server error (failed to stop the Pulsar Sink, failed to authorize, etc.)"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sinkName}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopSink(@ApiParam(value = "The tenant of a Pulsar Sink")
                         final @PathParam("tenant") String tenant,
                         @ApiParam(value = "The namespace of a Pulsar Sink")
                         final @PathParam("namespace") String namespace,
                         @ApiParam(value = "The name of a Pulsar Sink")
                         final @PathParam("sinkName") String sinkName) {
        sinks().stopFunctionInstances(tenant, namespace, sinkName, authParams());
    }

    @POST
    @ApiOperation(value = "Start an instance of a Pulsar Sink", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid start request"),
            @ApiResponse(code = 404, message = "The Pulsar Sink does not exist"),
            @ApiResponse(code = 500, message =
                    "Internal server error (failed to start the Pulsar Sink, failed to authorize, etc.)"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sinkName}/{instanceId}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startSink(@ApiParam(value = "The tenant of a Pulsar Sink")
                          final @PathParam("tenant") String tenant,
                          @ApiParam(value = "The namespace of a Pulsar Sink")
                          final @PathParam("namespace") String namespace,
                          @ApiParam(value = "The name of a Pulsar Sink")
                          final @PathParam("sinkName") String sinkName,
                          @ApiParam(value = "The instanceId of a Pulsar Sink")
                          final @PathParam("instanceId") String instanceId) {
        sinks().startFunctionInstance(tenant, namespace, sinkName, instanceId,
                uri.getRequestUri(), authParams());
    }

    @POST
    @ApiOperation(value = "Start all instances of a Pulsar Sink", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid start request"),
            @ApiResponse(code = 404, message = "The Pulsar Sink does not exist"),
            @ApiResponse(code = 500, message =
                    "Internal server error (failed to start the Pulsar Sink, failed to authorize, etc.)"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sinkName}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startSink(@ApiParam(value = "The tenant of a Pulsar Sink")
                          final @PathParam("tenant") String tenant,
                          @ApiParam(value = "The namespace of a Pulsar Sink")
                          final @PathParam("namespace") String namespace,
                          @ApiParam(value = "The name of a Pulsar Sink")
                          final @PathParam("sinkName") String sinkName) {
        sinks().startFunctionInstances(tenant, namespace, sinkName, authParams());
    }

    @GET
    @ApiOperation(
            value = "Fetches the list of built-in Pulsar IO sinks",
            response = ConnectorDefinition.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Get builtin sinks successfully.")
    })
    @Path("/builtinsinks")
    public List<ConnectorDefinition> getSinkList() {
        return sinks().getSinkList();
    }

    @GET
    @ApiOperation(
            value = "Fetches information about config fields associated with the specified builtin sink",
            response = ConfigFieldDefinition.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "builtin sink does not exist"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/builtinsinks/{name}/configdefinition")
    public List<ConfigFieldDefinition> getSinkConfigDefinition(
            @ApiParam(value = "The name of the builtin sink")
            final @PathParam("name") String name) throws IOException {
        return sinks().getSinkConfigDefinition(name);
    }

    @POST
    @ApiOperation(
            value = "Reload the built-in connectors, including Sources and Sinks",
            response = Void.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "This operation requires super-user access"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later."),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    @Path("/reloadBuiltInSinks")
    public void reloadSinks() {
        sinks().reloadConnectors(authParams());
    }
}
