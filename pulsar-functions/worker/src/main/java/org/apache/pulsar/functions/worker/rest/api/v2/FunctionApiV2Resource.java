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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.worker.rest.FunctionApiResource;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.apache.pulsar.common.io.ConnectorDefinition;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Slf4j
@Path("/functions")
public class FunctionApiV2Resource extends FunctionApiResource {

    @POST
    @Path("/{tenant}/{namespace}/{functionName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response registerFunction(final @PathParam("tenant") String tenant,
                                     final @PathParam("namespace") String namespace,
                                     final @PathParam("functionName") String functionName,
                                     final @FormDataParam("data") InputStream uploadedInputStream,
                                     final @FormDataParam("data") FormDataContentDisposition fileDetail,
                                     final @FormDataParam("url") String functionPkgUrl,
                                     final @FormDataParam("functionDetails") String functionDetailsJson,
                                     final @FormDataParam("functionConfig") String functionConfigJson) {

        return functions.registerFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail,
                functionPkgUrl, functionDetailsJson, functionConfigJson, clientAppId());

    }

    @PUT
    @Path("/{tenant}/{namespace}/{functionName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response updateFunction(final @PathParam("tenant") String tenant,
                                   final @PathParam("namespace") String namespace,
                                   final @PathParam("functionName") String functionName,
                                   final @FormDataParam("data") InputStream uploadedInputStream,
                                   final @FormDataParam("data") FormDataContentDisposition fileDetail,
                                   final @FormDataParam("url") String functionPkgUrl,
                                   final @FormDataParam("functionDetails") String functionDetailsJson,
                                   final @FormDataParam("functionConfig") String functionConfigJson) {

        return functions.updateFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail,
                functionPkgUrl, functionDetailsJson, functionConfigJson, clientAppId());

    }


    @DELETE
    @Path("/{tenant}/{namespace}/{functionName}")
    public Response deregisterFunction(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace, final @PathParam("functionName") String functionName) {
        return functions.deregisterFunction(tenant, namespace, functionName, clientAppId());
    }

    @GET
    @Path("/{tenant}/{namespace}/{functionName}")
    public Response getFunctionInfo(final @PathParam("tenant") String tenant,
                                    final @PathParam("namespace") String namespace,
                                    final @PathParam("functionName") String functionName)
            throws IOException {
        return functions.getFunctionInfo(
            tenant, namespace, functionName);
    }

    @GET
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/status")
    public Response getFunctionInstanceStatus(final @PathParam("tenant") String tenant,
                                              final @PathParam("namespace") String namespace,
                                              final @PathParam("functionName") String functionName,
                                              final @PathParam("instanceId") String instanceId) throws IOException {
        return functions.getFunctionInstanceStatus(
            tenant, namespace, functionName, instanceId, uri.getRequestUri());
    }

    @GET
    @Path("/{tenant}/{namespace}/{functionName}/status")
    public Response getFunctionStatus(final @PathParam("tenant") String tenant,
                                      final @PathParam("namespace") String namespace,
                                      final @PathParam("functionName") String functionName) throws IOException {
        return functions.getFunctionStatus(
            tenant, namespace, functionName, uri.getRequestUri());
    }

    @GET
    @Path("/{tenant}/{namespace}")
    public Response listFunctions(final @PathParam("tenant") String tenant,
                                  final @PathParam("namespace") String namespace) {
        return functions.listFunctions(
            tenant, namespace);

    }

    @POST
    @Path("/{tenant}/{namespace}/{functionName}/trigger")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response triggerFunction(final @PathParam("tenant") String tenant,
                                    final @PathParam("namespace") String namespace,
                                    final @PathParam("functionName") String functionName,
                                    final @FormDataParam("data") String input,
                                    final @FormDataParam("dataStream") InputStream uploadedInputStream,
                                    final @FormDataParam("topic") String topic) {
        return functions.triggerFunction(tenant, namespace, functionName, input, uploadedInputStream, topic);
    }

    @POST
    @ApiOperation(value = "Restart function instance", response = Void.class)
    @ApiResponses(value = { @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error") })
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response restartFunction(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace, final @PathParam("functionName") String functionName,
            final @PathParam("instanceId") String instanceId) {
        return functions.restartFunctionInstance(tenant, namespace, functionName, instanceId, this.uri.getRequestUri());
    }

    @POST
    @ApiOperation(value = "Restart all function instances", response = Void.class)
    @ApiResponses(value = { @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error") })
    @Path("/{tenant}/{namespace}/{functionName}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response restartFunction(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace, final @PathParam("functionName") String functionName) {
        return functions.restartFunctionInstances(tenant, namespace, functionName);
    }

    @POST
    @ApiOperation(value = "Stop function instance", response = Void.class)
    @ApiResponses(value = { @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error") })
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response stopFunction(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace, final @PathParam("functionName") String functionName,
            final @PathParam("instanceId") String instanceId) {
        return functions.stopFunctionInstance(tenant, namespace, functionName, instanceId, this.uri.getRequestUri());
    }

    @POST
    @ApiOperation(value = "Stop all function instances", response = Void.class)
    @ApiResponses(value = { @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error") })
    @Path("/{tenant}/{namespace}/{functionName}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response stopFunction(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace, final @PathParam("functionName") String functionName) {
        return functions.stopFunctionInstances(tenant, namespace, functionName);
    }

    @POST
    @Path("/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response uploadFunction(final @FormDataParam("data") InputStream uploadedInputStream,
                                   final @FormDataParam("path") String path) {
        return functions.uploadFunction(uploadedInputStream, path);
    }

    @GET
    @Path("/download")
    public Response downloadFunction(final @QueryParam("path") String path) {
        return functions.downloadFunction(path);
    }

    @GET
    @Path("/connectors")
    public List<ConnectorDefinition> getConnectorsList() throws IOException {
        return functions.getListOfConnectors();
    }

    @GET
    @Path("/{tenant}/{namespace}/{functionName}/state/{key}")
    public Response getFunctionState(final @PathParam("tenant") String tenant,
                                      final @PathParam("namespace") String namespace,
                                      final @PathParam("functionName") String functionName,
                                     final @PathParam("key") String key) throws IOException {
        return functions.getFunctionState(
            tenant, namespace, functionName, key);
    }
}
