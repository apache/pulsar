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

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.apache.pulsar.functions.worker.rest.FunctionApiResource;
import org.apache.pulsar.functions.worker.rest.api.SinkImpl;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Path("/sink")
public class SinkApiResource extends FunctionApiResource {

    protected final SinkImpl sink;

    public SinkApiResource() {
        this.sink = new SinkImpl(this);
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
                             final @FormDataParam("sinkConfig") String sinkConfigJson) {

        sink.registerFunction(tenant, namespace, sinkName, uploadedInputStream, fileDetail,
                functionPkgUrl, null, sinkConfigJson, clientAppId());
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
                           final @FormDataParam("sinkConfig") String sinkConfigJson) {

        sink.updateFunction(tenant, namespace, sinkName, uploadedInputStream, fileDetail,
                functionPkgUrl, null, sinkConfigJson, clientAppId());
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{sinkName}")
    public void deregisterSink(final @PathParam("tenant") String tenant,
                               final @PathParam("namespace") String namespace,
                               final @PathParam("sinkName") String sinkName) {
        sink.deregisterFunction(tenant, namespace, sinkName, clientAppId());
    }

    @GET
    @Path("/{tenant}/{namespace}/{sinkName}")
    public SinkConfig getSinkInfo(final @PathParam("tenant") String tenant,
                                  final @PathParam("namespace") String namespace,
                                  final @PathParam("sinkName") String sinkName)
            throws IOException {
        return sink.getSinkInfo(tenant, namespace, sinkName);
    }

    @GET
    @ApiOperation(
            value = "Displays the status of a Pulsar Sink instance",
            response = SinkStatus.SinkInstanceStatus.SinkInstanceStatusData.class
    )
    @ApiResponses(value = {
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
        return sink.getSinkInstanceStatus(tenant, namespace, sinkName, instanceId, uri.getRequestUri());
    }

    @GET
    @ApiOperation(
            value = "Displays the status of a Pulsar Sink running in cluster mode",
            response = SinkStatus.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "The sink doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{sinkName}/status")
    public SinkStatus getSinkStatus(final @PathParam("tenant") String tenant,
                                    final @PathParam("namespace") String namespace,
                                    final @PathParam("sinkName") String sinkName) throws IOException {
        return sink.getSinkStatus(tenant, namespace, sinkName, uri.getRequestUri());
    }

    @GET
    @Path("/{tenant}/{namespace}")
    public List<String> listSink(final @PathParam("tenant") String tenant,
                                 final @PathParam("namespace") String namespace) {
        return sink.listFunctions(tenant, namespace);
    }

    @POST
    @ApiOperation(value = "Restart sink instance", response = Void.class)
    @ApiResponses(value = { @ApiResponse(code = 400, message = "Invalid request"),
    @ApiResponse(code = 404, message = "The function does not exist"),
    @ApiResponse(code = 500, message = "Internal server error") })
    @Path("/{tenant}/{namespace}/{sinkName}/{instanceId}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartSink(final @PathParam("tenant") String tenant,
                            final @PathParam("namespace") String namespace,
                            final @PathParam("sinkName") String sinkName,
                            final @PathParam("instanceId") String instanceId) {
        sink.restartFunctionInstance(tenant, namespace, sinkName, instanceId, this.uri.getRequestUri());
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
        sink.restartFunctionInstances(tenant, namespace, sinkName);
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
        sink.stopFunctionInstance(tenant, namespace, sinkName, instanceId, this.uri.getRequestUri());
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
        sink.stopFunctionInstances(tenant, namespace, sinkName);
    }

    @GET
    @Path("/builtinsinks")
    public List<ConnectorDefinition> getSinkList() {
        List<ConnectorDefinition> connectorDefinitions = sink.getListOfConnectors();
        List<ConnectorDefinition> retVal = new ArrayList<>();
        for (ConnectorDefinition connectorDefinition : connectorDefinitions) {
            if (!StringUtils.isEmpty(connectorDefinition.getSinkClass())) {
                retVal.add(connectorDefinition);
            }
        }
        return retVal;
    }
}
