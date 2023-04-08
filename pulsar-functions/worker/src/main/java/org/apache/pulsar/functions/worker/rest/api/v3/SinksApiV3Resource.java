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
package org.apache.pulsar.functions.worker.rest.api.v3;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
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
import java.util.List;

@Slf4j
@Api(value = "/sinks", description = "Sinks admin apis", tags = "sinks")
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
    @ApiOperation(
            value = "Displays the status of a Pulsar Sink instance",
            response = SinkStatus.SinkInstanceStatus.SinkInstanceStatusData.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this sink"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "The sink doesn't exist")
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
    @ApiOperation(
            value = "Displays the status of a Pulsar Sink running in cluster mode",
            response = SinkStatus.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this sink"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "The sink doesn't exist")
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
    @ApiOperation(value = "Restart sink instance", response = Void.class)
    @ApiResponses(value = {
        @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this sink"),
        @ApiResponse(code = 400, message = "Invalid request"),
        @ApiResponse(code = 404, message = "The function does not exist"),
        @ApiResponse(code = 500, message = "Internal server error") })
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
    @ApiOperation(value = "Restart all sink instances", response = Void.class)
    @ApiResponses(value = { @ApiResponse(code = 400, message = "Invalid request"),
    @ApiResponse(code = 404, message = "The function does not exist"), @ApiResponse(code = 500, message = "Internal server error") })
    @Path("/{tenant}/{namespace}/{sinkName}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartSink(final @PathParam("tenant") String tenant,
                            final @PathParam("namespace") String namespace,
                            final @PathParam("sinkName") String sinkName) {
        sinks().restartFunctionInstances(tenant, namespace, sinkName, authParams());
    }

    @POST
    @ApiOperation(value = "Stop sink instance", response = Void.class)
    @ApiResponses(value = { @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error") })
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
    @ApiOperation(value = "Stop all sink instances", response = Void.class)
    @ApiResponses(value = { @ApiResponse(code = 400, message = "Invalid request"),
    @ApiResponse(code = 404, message = "The function does not exist"),
    @ApiResponse(code = 500, message = "Internal server error") })
    @Path("/{tenant}/{namespace}/{sinkName}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopSink(final @PathParam("tenant") String tenant,
                         final @PathParam("namespace") String namespace,
                         final @PathParam("sinkName") String sinkName) {
        sinks().stopFunctionInstances(tenant, namespace, sinkName, authParams());
    }

    @POST
    @ApiOperation(value = "Start sink instance", response = Void.class)
    @ApiResponses(value = { @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error") })
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
    @ApiOperation(value = "Start all sink instances", response = Void.class)
    @ApiResponses(value = { @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error") })
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
