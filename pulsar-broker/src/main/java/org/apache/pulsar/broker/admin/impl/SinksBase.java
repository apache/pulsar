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
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.api.SinksImpl;
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
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class SinksBase extends AdminResource implements Supplier<WorkerService> {

    private final SinksImpl sink;

    public SinksBase() {
        this.sink = new SinksImpl(this);
    }

    @Override
    public WorkerService get() {
        return pulsar().getWorkerService();
    }

    @POST
    @ApiOperation(value = "Creates a new Pulsar Sink in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request (sink already exists, etc.)"),
            @ApiResponse(code = 200, message = "Pulsar Sink successfully created"),
            @ApiResponse(code = 500, message = "Internal server error (failed to authorize, failed to get tenant data, failed to process package, etc.)"),
            @ApiResponse(code = 401, message = "Client is not authorized to perform operation")
    })
    @Path("/{tenant}/{namespace}/{sinkName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void registerSink(@ApiParam(value = "The sink's tenant")
                             final @PathParam("tenant") String tenant,
                             @ApiParam(value = "The sink's namespace")
                             final @PathParam("namespace") String namespace,
                             @ApiParam(value = "The sink's name")
                             final @PathParam("sinkName") String sinkName,
                             @ApiParam(value = "Package file for the sink")
                             final @FormDataParam("data") InputStream uploadedInputStream,
                             @ApiParam(value = "Package file description")
                             final @FormDataParam("data") FormDataContentDisposition fileDetail,
                             @ApiParam(value = "Url-path [http/https/file (file protocol assumes that file " +
                                 "already exists on worker host)] from which worker can download the package.")
                             final @FormDataParam("url") String functionPkgUrl,
                             @ApiParam(
                                 value =
                                     "--auto-ack\n" +
                                     "   Whether or not the framework will automatically acknowledge\n" +
                                     "   messages\n" +
                                     "--classname\n" +
                                     "   The sink's class name if archive is file-url-path (file://)\n" +
                                     "--cpu\n" +
                                     "   The CPU (in cores) that needs to be allocated per sink instance\n" +
                                     "   (applicable only to Docker runtime)\n" +
                                     "--custom-schema-inputs\n" +
                                     "   The map of input topics to Schema types or class names (as a JSON\n" +
                                     "   string)\n" +
                                     "--custom-serde-inputs\n" +
                                     "   The map of input topics to SerDe class names (as a JSON string)\n" +
                                     "--disk\n" +
                                     "   The disk (in bytes) that need to be allocated per sink instance\n" +
                                     "   (applicable only to Docker runtime)\n" +
                                     "-i, --inputs\n" +
                                     "   The sink's input topic or topics (multiple topics can be specified\n" +
                                     "   as a comma-separated list)\n" +
                                     "--parallelism\n" +
                                     "   The sink's parallelism factor (i.e. the number of sink instances to\n" +
                                     "   run)\n" +
                                     "--processing-guarantees\n" +
                                     "   The processing guarantees (aka delivery semantics) applied to the\n" +
                                     "   sink\n" +
                                     "   Possible Values: [ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE]\n" +
                                     "--ram\n" +
                                     "   The RAM (in bytes) that need to be allocated per sink instance\n" +
                                     "   (applicable only to the process and Docker runtimes)\n" +
                                     "--retain-ordering\n" +
                                     "   Sink consumes and sinks messages in order\n" +
                                     "--sink-config\n" +
                                     "   User defined configs key/values\n" +
                                     "--sink-config-file\n" +
                                     "   The path to a YAML config file specifying the sink's configuration\n" +
                                     "-t, --sink-type\n" +
                                     "   The sinks's connector provider\n" +
                                     "--subs-name\n" +
                                     "   Pulsar source subscription name if user wants a specific\n" +
                                     "   subscription-name for input-topic consumer\n" +
                                     "--timeout-ms\n" +
                                     "   The message timeout in milliseconds\n" +
                                     "--topics-pattern\n" +
                                     "   TopicsPattern to consume from list of topics under a namespace that\n" +
                                     "   match the pattern. [--input] and [--topicsPattern] are mutually\n" +
                                     "   exclusive. Add SerDe class name for a pattern in --customSerdeInputs\n" +
                                     "   (supported for java fun only)",
                                 examples = @Example(
                                     value = @ExampleProperty(
                                         mediaType = MediaType.APPLICATION_JSON,
                                         value =
                                             "\t\"classname\": \"org.example.MySinkTest\",\n" +
                                             "\t\"inputs\": \"persistent://public/default/sink-input\",\n" +
                                             "\t\"processing-guarantees\": \"EFFECTIVELY_ONCE\",\n" +
                                             "\t\"parallelism\": \"10\"\n" +
                                             "}"
                                     )
                                 )
                             )
                             final @FormDataParam("sinkConfig") String sinkConfigJson) {

        sink.registerFunction(tenant, namespace, sinkName, uploadedInputStream, fileDetail,
                functionPkgUrl, sinkConfigJson, clientAppId(), clientAuthData());
    }

    @PUT
    @ApiOperation(value = "Updates a Pulsar Sink currently running in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request (sink doesn't exist, update contains no change, etc.)"),
            @ApiResponse(code = 200, message = "Pulsar Sink successfully updated"),
            @ApiResponse(code = 401, message = "Client is not authorized to perform operation"),
            @ApiResponse(code = 404, message = "The provided `sinkName` is not a sink"),
            @ApiResponse(code = 500, message = "Internal server error (failed to authorize, failed to process package, etc.)")
    })
    @Path("/{tenant}/{namespace}/{sinkName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void updateSink(@ApiParam(value = "The sink's tenant")
                           final @PathParam("tenant") String tenant,
                           @ApiParam(value = "The sink's namespace")
                           final @PathParam("namespace") String namespace,
                           @ApiParam(value = "The sink's name")
                           final @PathParam("sinkName") String sinkName,
                           @ApiParam(value = "Archive file for the sink")
                           final @FormDataParam("data") InputStream uploadedInputStream,
                           @ApiParam(value = "Archive file description")
                           final @FormDataParam("data") FormDataContentDisposition fileDetail,
                           @ApiParam(value = "Url-path [http/https/file (file protocol assumes that file " +
                               "already exists on worker host)] from which worker can download the package.")
                           final @FormDataParam("url") String functionPkgUrl,
                           @ApiParam(
                               value =
                                   "--auto-ack\n" +
                                       "   Whether or not the framework will automatically acknowledge\n" +
                                       "   messages\n" +
                                       "--classname\n" +
                                       "   The sink's class name if archive is file-url-path (file://)\n" +
                                       "--cpu\n" +
                                       "   The CPU (in cores) that needs to be allocated per sink instance\n" +
                                       "   (applicable only to Docker runtime)\n" +
                                       "--custom-schema-inputs\n" +
                                       "   The map of input topics to Schema types or class names (as a JSON\n" +
                                       "   string)\n" +
                                       "--custom-serde-inputs\n" +
                                       "   The map of input topics to SerDe class names (as a JSON string)\n" +
                                       "--disk\n" +
                                       "   The disk (in bytes) that need to be allocated per sink instance\n" +
                                       "   (applicable only to Docker runtime)\n" +
                                       "-i, --inputs\n" +
                                       "   The sink's input topic or topics (multiple topics can be specified\n" +
                                       "   as a comma-separated list)\n" +
                                       "--parallelism\n" +
                                       "   The sink's parallelism factor (i.e. the number of sink instances to\n" +
                                       "   run)\n" +
                                       "--processing-guarantees\n" +
                                       "   The processing guarantees (aka delivery semantics) applied to the\n" +
                                       "   sink\n" +
                                       "   Possible Values: [ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE]\n" +
                                       "--ram\n" +
                                       "   The RAM (in bytes) that need to be allocated per sink instance\n" +
                                       "   (applicable only to the process and Docker runtimes)\n" +
                                       "--retain-ordering\n" +
                                       "   Sink consumes and sinks messages in order\n" +
                                       "--sink-config\n" +
                                       "   User defined configs key/values\n" +
                                       "--sink-config-file\n" +
                                       "   The path to a YAML config file specifying the sink's configuration\n" +
                                       "-t, --sink-type\n" +
                                       "   The sinks's connector provider\n" +
                                       "--subs-name\n" +
                                       "   Pulsar source subscription name if user wants a specific\n" +
                                       "   subscription-name for input-topic consumer\n" +
                                       "--timeout-ms\n" +
                                       "   The message timeout in milliseconds\n" +
                                       "--topics-pattern\n" +
                                       "   TopicsPattern to consume from list of topics under a namespace that\n" +
                                       "   match the pattern. [--input] and [--topicsPattern] are mutually\n" +
                                       "   exclusive. Add SerDe class name for a pattern in --customSerdeInputs\n" +
                                       "   (supported for java fun only)",
                               examples = @Example(
                                   value = @ExampleProperty(
                                       mediaType = MediaType.APPLICATION_JSON,
                                       value =
                                           "\t\"classname\": \"org.example.SinkStressTest\",\n" +
                                               "\t\"inputs\": \"persistent://public/default/sink-input\",\n" +
                                               "\t\"processing-guarantees\": \"EFFECTIVELY_ONCE\",\n" +
                                               "\t\"parallelism\": \"5\"\n" +
                                               "}"
                                   )
                               )
                           )
                           final @FormDataParam("sinkConfig") String sinkConfigJson,
                           @ApiParam()
                           final @FormDataParam("updateOptions") UpdateOptions updateOptions) {

         sink.updateFunction(tenant, namespace, sinkName, uploadedInputStream, fileDetail,
                functionPkgUrl, sinkConfigJson, clientAppId(), clientAuthData(), updateOptions);

    }


    @DELETE
    @ApiOperation(value = "Deletes a Pulsar Sink currently running in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid deregister request"),
            @ApiResponse(code = 404, message = "The sink doesn't exist"),
            @ApiResponse(code = 200, message = "The sink was successfully deleted"),
            @ApiResponse(code = 401, message = "Client is not authorized to perform operation"),
            @ApiResponse(code = 500, message = "Internal server error (failed to authorize, failed to deregister, etc.)"),
            @ApiResponse(code = 408, message = "Got InterruptedException while deregistering the sink")

    })
    @Path("/{tenant}/{namespace}/{sinkName}")
    public void deregisterSink(@ApiParam(value = "The sink's tenant")
                               final @PathParam("tenant") String tenant,
                               @ApiParam(value = "The sink's namespace")
                               final @PathParam("namespace") String namespace,
                               @ApiParam(value = "The sink's name")
                               final @PathParam("sinkName") String sinkName) {
        sink.deregisterFunction(tenant, namespace, sinkName, clientAppId(), clientAuthData());
    }

    @GET
    @ApiOperation(
            value = "Fetches information about a Pulsar Sink currently running in cluster mode",
            response = SinkConfig.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The sink doesn't exist")
    })
    @Path("/{tenant}/{namespace}/{sinkName}")
    public SinkConfig getSinkInfo(@ApiParam(value = "The sink's tenant")
                                  final @PathParam("tenant") String tenant,
                                  @ApiParam(value = "The sink's namespace")
                                  final @PathParam("namespace") String namespace,
                                  @ApiParam(value = "The sink's name")
                                  final @PathParam("sinkName") String sinkName) throws IOException {
        return sink.getSinkInfo(tenant, namespace, sinkName);
    }

    @GET
    @ApiOperation(
            value = "Displays the status of a Pulsar Sink instance",
            response = SinkStatus.SinkInstanceStatus.SinkInstanceStatusData.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "The sink instance does not exist"),
            @ApiResponse(code = 404, message = "The sink doesn't exist"),
            @ApiResponse(code = 500, message = "Internal Server Error (got exception while getting status, etc.)")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{sinkName}/{instanceId}/status")
    public SinkStatus.SinkInstanceStatus.SinkInstanceStatusData getSinkInstanceStatus(
            @ApiParam(value = "The sink's tenant")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The sink's namespace")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The sink's name")
            final @PathParam("sinkName") String sinkName,
            @ApiParam(value = "The sink instanceId")
            final @PathParam("instanceId") String instanceId) throws IOException {
        return sink.getSinkInstanceStatus(
            tenant, namespace, sinkName, instanceId, uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @GET
    @ApiOperation(
            value = "Displays the status of a Pulsar Sink running in cluster mode",
            response = SinkStatus.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid get status request"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation"),
            @ApiResponse(code = 404, message = "The sink doesn't exist"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later."),
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{sinkName}/status")
    public SinkStatus getSinkStatus(@ApiParam(value = "The sink's tenant")
                                    final @PathParam("tenant") String tenant,
                                    @ApiParam(value = "The sink's namespace")
                                    final @PathParam("namespace") String namespace,
                                    @ApiParam(value = "The sink's name")
                                    final @PathParam("sinkName") String sinkName) throws IOException {
        return sink.getSinkStatus(tenant, namespace, sinkName, uri.getRequestUri(), clientAppId(), clientAuthData());
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

    })
    @Path("/{tenant}/{namespace}")
    public List<String> listSinks(@ApiParam(value = "The sink's tenant")
                                  final @PathParam("tenant") String tenant,
                                  @ApiParam(value = "The sink's namespace")
                                  final @PathParam("namespace") String namespace) {
        return sink.listFunctions(tenant, namespace, clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Restart sink instance", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid restart request"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation"),
            @ApiResponse(code = 404, message = "The sink does not exist"),
            @ApiResponse(code = 500, message = "Internal server error (failed to restart the sink instance, failed to authorize, etc.)"),
    })
    @Path("/{tenant}/{namespace}/{sinkName}/{instanceId}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartSink(@ApiParam(value = "The sink's tenant")
                            final @PathParam("tenant") String tenant,
                            @ApiParam(value = "The sink's namespace")
                            final @PathParam("namespace") String namespace,
                            @ApiParam(value = "The sink's name")
                            final @PathParam("sinkName") String sinkName,
                            @ApiParam(value = "The sink instanceId")
                            final @PathParam("instanceId") String instanceId) {
        sink.restartFunctionInstance(tenant, namespace, sinkName, instanceId, uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Restart all sink instances", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid restart request"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation"),
            @ApiResponse(code = 404, message = "The sink does not exist"),
            @ApiResponse(code = 500, message = "Internal server error (failed to restart the sink, failed to authorize, etc.)"),
    })
    @Path("/{tenant}/{namespace}/{sinkName}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartSink(@ApiParam(value = "The sink's tenant")
                            final @PathParam("tenant") String tenant,
                            @ApiParam(value = "The sink's namespace")
                            final @PathParam("namespace") String namespace,
                            @ApiParam(value = "The sink's name")
                            final @PathParam("sinkName") String sinkName) {
        sink.restartFunctionInstances(tenant, namespace, sinkName, clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Stop sink instance", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid stop request"),
            @ApiResponse(code = 404, message = "The sink instance does not exist"),
            @ApiResponse(code = 500, message = "Internal server error (failed to stop the sink, failed to authorize, etc.)"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation")
    })
    @Path("/{tenant}/{namespace}/{sinkName}/{instanceId}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopSink(@ApiParam(value = "The sink's tenant")
                         final @PathParam("tenant") String tenant,
                         @ApiParam(value = "The sink's namespace")
                         final @PathParam("namespace") String namespace,
                         @ApiParam(value = "The sink's name")
                         final @PathParam("sinkName") String sinkName,
                         @ApiParam(value = "The sink instanceId")
                         final @PathParam("instanceId") String instanceId) {
        sink.stopFunctionInstance(tenant, namespace, sinkName, instanceId, uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Stop all sink instances", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid stop request"),
            @ApiResponse(code = 404, message = "The sink does not exist"),
            @ApiResponse(code = 500, message = "Internal server error (failed to stop the sink, failed to authorize, etc.)"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation")
    })
    @Path("/{tenant}/{namespace}/{sinkName}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopSink(@ApiParam(value = "The sink's tenant")
                         final @PathParam("tenant") String tenant,
                         @ApiParam(value = "The sink's namespace")
                         final @PathParam("namespace") String namespace,
                         @ApiParam(value = "The sink's name")
                         final @PathParam("sinkName") String sinkName) {
        sink.stopFunctionInstances(tenant, namespace, sinkName, clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Start sink instance", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid start request"),
            @ApiResponse(code = 404, message = "The sink does not exist"),
            @ApiResponse(code = 500, message = "Internal server error (failed to start the sink, failed to authorize, etc.)"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation")
    })
    @Path("/{tenant}/{namespace}/{sinkName}/{instanceId}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startSink(@ApiParam(value = "The sink's tenant")
                          final @PathParam("tenant") String tenant,
                          @ApiParam(value = "The sink's namespace")
                          final @PathParam("namespace") String namespace,
                          @ApiParam(value = "The sink's name")
                          final @PathParam("sinkName") String sinkName,
                          @ApiParam(value = "The sink instanceId")
                          final @PathParam("instanceId") String instanceId) {
        sink.startFunctionInstance(tenant, namespace, sinkName, instanceId, uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Start all sink instances", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid start request"),
            @ApiResponse(code = 404, message = "The sink does not exist"),
            @ApiResponse(code = 500, message = "Internal server error (failed to start the sink, failed to authorize, etc.)"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation")
    })
    @Path("/{tenant}/{namespace}/{sinkName}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startSink(@ApiParam(value = "The sink's tenant")
                          final @PathParam("tenant") String tenant,
                          @ApiParam(value = "The sink's namespace")
                          final @PathParam("namespace") String namespace,
                          @ApiParam(value = "The sink's name")
                          final @PathParam("sinkName") String sinkName) {
        sink.startFunctionInstances(tenant, namespace, sinkName, clientAppId(), clientAuthData());
    }

    @GET
    @ApiOperation(
            value = "Fetches a list of supported Pulsar IO sink connectors currently running in cluster mode",
            response = List.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Get builtin sinks successfully.")
    })
    @Path("/builtinsinks")
    public List<ConnectorDefinition> getSinkList() {
        List<ConnectorDefinition> connectorDefinitions = sink.getListOfConnectors();
        List<ConnectorDefinition> retval = new ArrayList<>();
        for (ConnectorDefinition connectorDefinition : connectorDefinitions) {
            if (!StringUtils.isEmpty(connectorDefinition.getSinkClass())) {
                retval.add(connectorDefinition);
            }
        }
        return retval;
    }
}
