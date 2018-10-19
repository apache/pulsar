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
@Path("/source")
public class SourceApiV2Resource extends FunctionApiResource {

    @POST
    @Path("/{tenant}/{namespace}/{sourceName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response registerSource(final @PathParam("tenant") String tenant,
                                   final @PathParam("namespace") String namespace,
                                   final @PathParam("sourceName") String sourceName,
                                   final @FormDataParam("data") InputStream uploadedInputStream,
                                   final @FormDataParam("data") FormDataContentDisposition fileDetail,
                                   final @FormDataParam("url") String functionPkgUrl,
                                   final @FormDataParam("sourceConfig") String sourceConfigJson) {

        return functions.registerFunction(tenant, namespace, sourceName, uploadedInputStream, fileDetail,
                functionPkgUrl, null, sourceConfigJson, FunctionsImpl.SOURCE, clientAppId());

    }

    @PUT
    @Path("/{tenant}/{namespace}/{sourceName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response updateSource(final @PathParam("tenant") String tenant,
                                 final @PathParam("namespace") String namespace,
                                 final @PathParam("sourceName") String sourceName,
                                 final @FormDataParam("data") InputStream uploadedInputStream,
                                 final @FormDataParam("data") FormDataContentDisposition fileDetail,
                                 final @FormDataParam("url") String functionPkgUrl,
                                 final @FormDataParam("sourceConfig") String sourceConfigJson) {

        return functions.updateFunction(tenant, namespace, sourceName, uploadedInputStream, fileDetail,
                functionPkgUrl, null, sourceConfigJson, FunctionsImpl.SOURCE, clientAppId());

    }


    @DELETE
    @Path("/{tenant}/{namespace}/{sourceName}")
    public Response deregisterSource(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace, final @PathParam("sourceName") String sourceName) {
        return functions.deregisterFunction(tenant, namespace, sourceName, FunctionsImpl.SOURCE, clientAppId());
    }

    @GET
    @Path("/{tenant}/{namespace}/{sourceName}")
    public Response getSourceInfo(final @PathParam("tenant") String tenant,
                                  final @PathParam("namespace") String namespace,
                                  final @PathParam("sourceName") String sourceName)
            throws IOException {
        return functions.getFunctionInfo(tenant, namespace, sourceName, FunctionsImpl.SOURCE);
    }

    @GET
    @Path("/{tenant}/{namespace}/{sourceName}/{instanceId}/status")
    public Response getSourceInstanceStatus(final @PathParam("tenant") String tenant,
                                            final @PathParam("namespace") String namespace,
                                            final @PathParam("sourceName") String sourceName,
                                            final @PathParam("instanceId") String instanceId) throws IOException {
        return functions.getFunctionInstanceStatus(
            tenant, namespace, sourceName, FunctionsImpl.SOURCE, instanceId, uri.getRequestUri());
    }

    @GET
    @Path("/{tenant}/{namespace}/{sourceName}/status")
    public Response getSourceStatus(final @PathParam("tenant") String tenant,
                                    final @PathParam("namespace") String namespace,
                                    final @PathParam("sourceName") String sourceName) throws IOException {
        return functions.getFunctionStatus(tenant, namespace, sourceName, FunctionsImpl.SOURCE, uri.getRequestUri());
    }

    @GET
    @Path("/{tenant}/{namespace}")
    public Response listSources(final @PathParam("tenant") String tenant,
                                final @PathParam("namespace") String namespace) {
        return functions.listFunctions(tenant, namespace, FunctionsImpl.SOURCE);

    }

    @POST
    @ApiOperation(value = "Restart source instance", response = Void.class)
    @ApiResponses(value = { @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error") })
    @Path("/{tenant}/{namespace}/{sourceName}/{instanceId}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response restartSource(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace, final @PathParam("sourceName") String sourceName,
            final @PathParam("instanceId") String instanceId) {
        return functions.restartFunctionInstance(tenant, namespace, sourceName, FunctionsImpl.SOURCE, instanceId, this.uri.getRequestUri());
    }

    @POST
    @ApiOperation(value = "Restart all source instances", response = Void.class)
    @ApiResponses(value = { @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error") })
    @Path("/{tenant}/{namespace}/{sourceName}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response restartSource(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace, final @PathParam("sourceName") String sourceName) {
        return functions.restartFunctionInstances(tenant, namespace, sourceName, FunctionsImpl.SOURCE);
    }

    @POST
    @ApiOperation(value = "Stop source instance", response = Void.class)
    @ApiResponses(value = { @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error") })
    @Path("/{tenant}/{namespace}/{sourceName}/{instanceId}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response stopSource(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace, final @PathParam("sourceName") String sourceName,
            final @PathParam("instanceId") String instanceId) {
        return functions.stopFunctionInstance(tenant, namespace, sourceName, FunctionsImpl.SOURCE, instanceId, this.uri.getRequestUri());
    }

    @POST
    @ApiOperation(value = "Stop all source instances", response = Void.class)
    @ApiResponses(value = { @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error") })
    @Path("/{tenant}/{namespace}/{sourceName}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response stopSource(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace, final @PathParam("sourceName") String sourceName) {
        return functions.stopFunctionInstances(tenant, namespace, sourceName, FunctionsImpl.SOURCE);
    }

    @GET
    @Path("/builtinsources")
    public List<ConnectorDefinition> getSourceList() {
        List<ConnectorDefinition> connectorDefinitions = functions.getListOfConnectors();
        List<ConnectorDefinition> retval = new ArrayList<>();
        for (ConnectorDefinition connectorDefinition : connectorDefinitions) {
            if (!StringUtils.isEmpty(connectorDefinition.getSourceClass())) {
                retval.add(connectorDefinition);
            }
        }
        return retval;
    }
}
