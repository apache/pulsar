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
package org.apache.pulsar.functions.worker.rest.api.v2;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.functions.worker.rest.FunctionApiResource;
import org.apache.pulsar.functions.worker.rest.api.FunctionsImpl;
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
public class SinkApiV2Resource extends FunctionApiResource {

    @POST
    @Path("/{tenant}/{namespace}/{sinkName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response registerSink(final @PathParam("tenant") String tenant,
                                 final @PathParam("namespace") String namespace,
                                 final @PathParam("sinkName") String sinkName,
                                 final @FormDataParam("data") InputStream uploadedInputStream,
                                 final @FormDataParam("data") FormDataContentDisposition fileDetail,
                                 final @FormDataParam("url") String functionPkgUrl,
                                 final @FormDataParam("sinkConfig") String sinkConfigJson) {

        return functions.registerFunction(tenant, namespace, sinkName, uploadedInputStream, fileDetail,
                functionPkgUrl, null, sinkConfigJson, FunctionsImpl.SINK, clientAppId());

    }

    @PUT
    @Path("/{tenant}/{namespace}/{sinkName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response updateSink(final @PathParam("tenant") String tenant,
                               final @PathParam("namespace") String namespace,
                               final @PathParam("sinkName") String sinkName,
                               final @FormDataParam("data") InputStream uploadedInputStream,
                               final @FormDataParam("data") FormDataContentDisposition fileDetail,
                               final @FormDataParam("url") String functionPkgUrl,
                               final @FormDataParam("sinkConfig") String sinkConfigJson) {

        return functions.updateFunction(tenant, namespace, sinkName, uploadedInputStream, fileDetail,
                functionPkgUrl, null, sinkConfigJson, FunctionsImpl.SINK, clientAppId());

    }


    @DELETE
    @Path("/{tenant}/{namespace}/{sinkName}")
    public Response deregisterSink(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace, final @PathParam("sinkName") String sinkName) {
        return functions.deregisterFunction(tenant, namespace, sinkName, FunctionsImpl.SINK, clientAppId());
    }

    @GET
    @Path("/{tenant}/{namespace}/{sinkName}")
    public Response getSinkInfo(final @PathParam("tenant") String tenant,
                                final @PathParam("namespace") String namespace,
                                final @PathParam("sinkName") String sinkName)
            throws IOException {
        return functions.getFunctionInfo(tenant, namespace, sinkName, FunctionsImpl.SINK);
    }

    @GET
    @Path("/{tenant}/{namespace}/{sinkName}/{instanceId}/status")
    public Response getSinkInstanceStatus(final @PathParam("tenant") String tenant,
                                          final @PathParam("namespace") String namespace,
                                          final @PathParam("sinkName") String sinkName,
                                          final @PathParam("instanceId") String instanceId) throws IOException {
        return functions.getFunctionInstanceStatus(
            tenant, namespace, sinkName, FunctionsImpl.SINK, instanceId, uri.getRequestUri());
    }

    @GET
    @Path("/{tenant}/{namespace}/{sinkName}/status")
    public Response getSinkStatus(final @PathParam("tenant") String tenant,
                                  final @PathParam("namespace") String namespace,
                                  final @PathParam("sinkName") String sinkName) throws IOException {
        return functions.getFunctionStatus(tenant, namespace, sinkName, FunctionsImpl.SINK, uri.getRequestUri());
    }

    @GET
    @Path("/{tenant}/{namespace}")
    public Response listSink(final @PathParam("tenant") String tenant,
                             final @PathParam("namespace") String namespace) {
        return functions.listFunctions(tenant, namespace, FunctionsImpl.SINK);

    }

    @POST
    @ApiOperation(value = "Restart sink instance", response = Void.class)
    @ApiResponses(value = { @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error") })
    @Path("/{tenant}/{namespace}/{sinkName}/{instanceId}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response restartSink(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace, final @PathParam("sinkName") String sinkName,
            final @PathParam("instanceId") String instanceId) {
        return functions.restartFunctionInstance(tenant, namespace, sinkName, FunctionsImpl.SINK, instanceId, this.uri.getRequestUri());
    }

    @POST
    @ApiOperation(value = "Restart all sink instances", response = Void.class)
    @ApiResponses(value = { @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error") })
    @Path("/{tenant}/{namespace}/{sinkName}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response restartSink(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace, final @PathParam("sinkName") String sinkName) {
        return functions.restartFunctionInstances(tenant, namespace, sinkName, FunctionsImpl.SINK);
    }

    @POST
    @ApiOperation(value = "Stop sink instance", response = Void.class)
    @ApiResponses(value = { @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error") })
    @Path("/{tenant}/{namespace}/{sinkName}/{instanceId}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response stopSink(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace, final @PathParam("sinkName") String sinkName,
            final @PathParam("instanceId") String instanceId) {
        return functions.stopFunctionInstance(tenant, namespace, sinkName, FunctionsImpl.SINK, instanceId, this.uri.getRequestUri());
    }

    @POST
    @ApiOperation(value = "Stop all sink instances", response = Void.class)
    @ApiResponses(value = { @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error") })
    @Path("/{tenant}/{namespace}/{sinkName}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response stopSink(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace, final @PathParam("sinkName") String sinkName) {
        return functions.stopFunctionInstances(tenant, namespace, sinkName, FunctionsImpl.SINK);
    }

    @GET
    @Path("/builtinsinks")
    public List<ConnectorDefinition> getSinkList() {
        List<ConnectorDefinition> connectorDefinitions = functions.getListOfConnectors();
        List<ConnectorDefinition> retval = new ArrayList<>();
        for (ConnectorDefinition connectorDefinition : connectorDefinitions) {
            if (!StringUtils.isEmpty(connectorDefinition.getSinkClass())) {
                retval.add(connectorDefinition);
            }
        }
        return retval;
    }
}
