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
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsDataImpl;
import org.apache.pulsar.common.policies.data.FunctionStatsImpl;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.FunctionApiResource;
import org.apache.pulsar.functions.worker.service.api.Functions;
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

@Slf4j
@Path("/functions")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class FunctionsApiV3Resource extends FunctionApiResource {

    Functions<? extends WorkerService> functions() {
        return get().getFunctions();
    }

    @POST
    @Path("/{tenant}/{namespace}/{functionName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void registerFunction(final @PathParam("tenant") String tenant,
                                 final @PathParam("namespace") String namespace,
                                 final @PathParam("functionName") String functionName,
                                 final @FormDataParam("data") InputStream uploadedInputStream,
                                 final @FormDataParam("data") FormDataContentDisposition fileDetail,
                                 final @FormDataParam("url") String functionPkgUrl,
                                 final @FormDataParam("functionConfig") FunctionConfig functionConfig) {

        functions().registerFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail,
                functionPkgUrl, functionConfig, clientAppId(), clientAuthData());

    }

    @PUT
    @Path("/{tenant}/{namespace}/{functionName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void updateFunction(final @PathParam("tenant") String tenant,
                               final @PathParam("namespace") String namespace,
                               final @PathParam("functionName") String functionName,
                               final @FormDataParam("data") InputStream uploadedInputStream,
                               final @FormDataParam("data") FormDataContentDisposition fileDetail,
                               final @FormDataParam("url") String functionPkgUrl,
                               final @FormDataParam("functionConfig") FunctionConfig functionConfig,
                               final @FormDataParam("updateOptions") UpdateOptionsImpl updateOptions) {

        functions().updateFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail,
                functionPkgUrl, functionConfig, clientAppId(), clientAuthData(), updateOptions);

    }

    @DELETE
    @Path("/{tenant}/{namespace}/{functionName}")
    public void deregisterFunction(final @PathParam("tenant") String tenant,
                                   final @PathParam("namespace") String namespace,
                                   final @PathParam("functionName") String functionName) {
        functions().deregisterFunction(tenant, namespace, functionName, clientAppId(), clientAuthData());
    }

    @GET
    @Path("/{tenant}/{namespace}/{functionName}")
    public FunctionConfig getFunctionInfo(final @PathParam("tenant") String tenant,
                                          final @PathParam("namespace") String namespace,
                                          final @PathParam("functionName") String functionName) {
        return functions().getFunctionInfo(tenant, namespace, functionName, clientAppId(), clientAuthData());
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}")
    public List<String> listFunctions(final @PathParam("tenant") String tenant,
                                    final @PathParam("namespace") String namespace) {
        return functions().listFunctions(tenant, namespace, clientAppId(), clientAuthData());
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
            @ApiResponse(code = 404, message = "The function doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/status")
    public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData getFunctionInstanceStatus(
            final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace,
            final @PathParam("functionName") String functionName,
            final @PathParam("instanceId") String instanceId) throws IOException {
        return functions().getFunctionInstanceStatus(
                tenant, namespace, functionName, instanceId, uri.getRequestUri(), clientAppId(), clientAuthData());
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
            @ApiResponse(code = 404, message = "The function doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{functionName}/status")
    public FunctionStatus getFunctionStatus(
            final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace,
            final @PathParam("functionName") String functionName) throws IOException {
        return functions().getFunctionStatus(
                tenant, namespace, functionName, uri.getRequestUri(), clientAppId(), clientAuthData());
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
            @ApiResponse(code = 404, message = "The function doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{functionName}/stats")
    public FunctionStatsImpl getFunctionStats(final @PathParam("tenant") String tenant,
                                              final @PathParam("namespace") String namespace,
                                              final @PathParam("functionName") String functionName) throws IOException {
        return functions().getFunctionStats(tenant, namespace, functionName, uri.getRequestUri(), clientAppId(), clientAuthData());
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
            @ApiResponse(code = 404, message = "The function doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/stats")
    public FunctionInstanceStatsDataImpl getFunctionInstanceStats(
            final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace,
            final @PathParam("functionName") String functionName,
            final @PathParam("instanceId") String instanceId) throws IOException {
        return functions().getFunctionsInstanceStats(
                tenant, namespace, functionName, instanceId, uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @POST
    @Path("/{tenant}/{namespace}/{functionName}/trigger")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public String triggerFunction(final @PathParam("tenant") String tenant,
                                  final @PathParam("namespace") String namespace,
                                  final @PathParam("functionName") String functionName,
                                  final @FormDataParam("data") String input,
                                  final @FormDataParam("dataStream") InputStream uploadedInputStream,
                                  final @FormDataParam("topic") String topic) {
        return functions().triggerFunction(tenant, namespace, functionName, input, uploadedInputStream, topic, clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Restart function instance", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this function"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartFunction(final @PathParam("tenant") String tenant,
                                final @PathParam("namespace") String namespace,
                                final @PathParam("functionName") String functionName,
                                final @PathParam("instanceId") String instanceId) {
        functions().restartFunctionInstance(tenant, namespace, functionName, instanceId, this.uri.getRequestUri(), clientAppId(), clientAuthData());
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
    public void restartFunction(final @PathParam("tenant") String tenant,
                                final @PathParam("namespace") String namespace,
                                final @PathParam("functionName") String functionName) {
        functions().restartFunctionInstances(tenant, namespace, functionName, clientAppId(), clientAuthData());
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
    public void stopFunction(final @PathParam("tenant") String tenant,
                             final @PathParam("namespace") String namespace,
                             final @PathParam("functionName") String functionName,
                             final @PathParam("instanceId") String instanceId) {
        functions().stopFunctionInstance(tenant, namespace, functionName, instanceId, this.uri.getRequestUri(), clientAppId(), clientAuthData());
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
    public void stopFunction(final @PathParam("tenant") String tenant,
                             final @PathParam("namespace") String namespace,
                             final @PathParam("functionName") String functionName) {
        functions().stopFunctionInstances(tenant, namespace, functionName, clientAppId(), clientAuthData());
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
    public void startFunction(final @PathParam("tenant") String tenant,
                              final @PathParam("namespace") String namespace,
                              final @PathParam("functionName") String functionName,
                              final @PathParam("instanceId") String instanceId) {
        functions().startFunctionInstance(tenant, namespace, functionName, instanceId, this.uri.getRequestUri(), clientAppId(), clientAuthData());
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
    public void startFunction(final @PathParam("tenant") String tenant,
                              final @PathParam("namespace") String namespace,
                              final @PathParam("functionName") String functionName) {
        functions().startFunctionInstances(tenant, namespace, functionName, clientAppId(), clientAuthData());
    }

    @POST
    @Path("/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void uploadFunction(final @FormDataParam("data") InputStream uploadedInputStream,
                               final @FormDataParam("path") String path) {
        functions().uploadFunction(uploadedInputStream, path, clientAppId(), clientAuthData());
    }

    @GET
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
            @ApiParam(value = "The tenant of functions")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of functions")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of functions")
            final @PathParam("functionName") String functionName) {

        return functions().downloadFunction(tenant, namespace, functionName, clientAppId(), clientAuthData());
    }

    @GET
    @Path("/connectors")
    /**
     * Deprecated in favor of moving endpoint to {@link org.apache.pulsar.broker.admin.v2.Worker}
     */
    @Deprecated
    public List<ConnectorDefinition> getConnectorsList() throws IOException {
        return functions().getListOfConnectors();
    }

    @GET
    @Path("/{tenant}/{namespace}/{functionName}/state/{key}")
    public FunctionState getFunctionState(final @PathParam("tenant") String tenant,
                                          final @PathParam("namespace") String namespace,
                                          final @PathParam("functionName") String functionName,
                                          final @PathParam("key") String key) throws IOException {
        return functions().getFunctionState(tenant, namespace, functionName, key, clientAppId(), clientAuthData());
    }

    @POST
    @Path("/{tenant}/{namespace}/{functionName}/state/{key}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void putFunctionState(final @PathParam("tenant") String tenant,
                                 final @PathParam("namespace") String namespace,
                                 final @PathParam("functionName") String functionName,
                                 final @PathParam("key") String key,
                                 final @FormDataParam("state") FunctionState stateJson) throws IOException {
        functions().putFunctionState(tenant, namespace, functionName, key, stateJson, clientAppId(), clientAuthData());
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
                                                 final @FormDataParam("functionMetaData") InputStream uploadedInputStream,
                                                 final @FormDataParam("delete") boolean delete) {

        functions().updateFunctionOnWorkerLeader(tenant, namespace, functionName, uploadedInputStream,
                delete, uri.getRequestUri(), clientAppId(), clientAuthData());
    }
}
