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
package org.apache.pulsar.functions.worker.rest.api.v1;

import com.google.gson.Gson;
import javax.ws.rs.core.Response.Status;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.functions.fs.FunctionStatus;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.PackageLocationMetaData;
import org.apache.pulsar.functions.runtime.spawner.Spawner;
import org.apache.pulsar.functions.worker.FunctionRuntimeInfo;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.Utils;
import org.apache.pulsar.functions.worker.request.RequestResult;
import org.apache.pulsar.functions.worker.rest.BaseApiResource;
import org.apache.pulsar.functions.worker.rest.RestUtils;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Slf4j
@Path("/admin/functions")
public class ApiV1Resource extends BaseApiResource {

    @POST
    @Path("/{tenant}/{namespace}/{functionName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response registerFunction(final @PathParam("tenant") String tenant,
                                     final @PathParam("namespace") String namespace,
                                     final @PathParam("functionName") String functionName,
                                     final @FormDataParam("data") InputStream uploadedInputStream,
                                     final @FormDataParam("data") FormDataContentDisposition fileDetail,
                                     final @FormDataParam("functionConfig") String functionConfigJson) {

        FunctionConfig functionConfig;
        // validate parameters
        try {
            functionConfig = validateUpdateRequestParams(tenant, namespace, functionName,
                    uploadedInputStream, fileDetail, functionConfigJson);
        } catch (IllegalArgumentException e) {
            log.error("Invalid register function request @ /{}/{}/{}",
                tenant, namespace, functionName, e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(RestUtils.createMessage(e.getMessage())).build();
        }

        FunctionRuntimeManager functionRuntimeManager = getWorkerFunctionStateManager();

        if (functionRuntimeManager.containsFunction(tenant, namespace, functionName)) {
            log.error("Function {}/{}/{} already exists", tenant, namespace, functionName);
            return Response.status(Response.Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(RestUtils.createMessage(String.format("Function %s already exist", functionName))).build();
        }

        // function state
        FunctionMetaData.Builder functionMetaDataBuilder = FunctionMetaData.newBuilder()
                .setFunctionConfig(functionConfig)
                .setCreateTime(System.currentTimeMillis())
                .setVersion(0);

        WorkerConfig workerConfig = getWorkerConfig();
        PackageLocationMetaData.Builder packageLocationMetaDataBuilder = PackageLocationMetaData.newBuilder()
                .setPackagePath(String.format(
            // TODO: dlog 0.5.0 doesn't support filesystem path
            "%s_%s_%s_%s",
            tenant,
            namespace,
            functionName,
            Utils.getUniquePackageName(fileDetail.getFileName())));
        functionMetaDataBuilder.setPackageLocation(packageLocationMetaDataBuilder);
        functionMetaDataBuilder.setWorkerId(workerConfig.getWorkerId());
        
        return updateRequest(functionMetaDataBuilder.build(), uploadedInputStream);
    }

    @PUT
    @Path("/{tenant}/{namespace}/{functionName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response updateFunction(final @PathParam("tenant") String tenant,
                                   final @PathParam("namespace") String namespace,
                                   final @PathParam("functionName") String functionName,
                                   final @FormDataParam("data") InputStream uploadedInputStream,
                                   final @FormDataParam("data") FormDataContentDisposition fileDetail,
                                   final @FormDataParam("functionConfig") String functionConfigJson) {

        FunctionConfig functionConfig;
        // validate parameters
        try {
            functionConfig = validateUpdateRequestParams(tenant, namespace, functionName,
                    uploadedInputStream, fileDetail, functionConfigJson);
        } catch (IllegalArgumentException e) {
            log.error("Invalid update function request @ /{}/{}/{}",
                    tenant, namespace, functionName, e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(RestUtils.createMessage(e.getMessage())).build();
        }

        FunctionRuntimeManager functionRuntimeManager = getWorkerFunctionStateManager();

        if (!functionRuntimeManager.containsFunction(tenant, namespace, functionName)) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(RestUtils.createMessage(String.format("Function %s doesn't exist", functionName))).build();
        }

        // function state
        FunctionMetaData.Builder functionMetaDataBuilder = FunctionMetaData.newBuilder()
                .setFunctionConfig(functionConfig)
                .setCreateTime(System.currentTimeMillis())
                .setVersion(0);

        WorkerConfig workerConfig = getWorkerConfig();
        PackageLocationMetaData.Builder packageLocationMetaDataBuilder = PackageLocationMetaData.newBuilder()
                .setPackagePath(String.format(
                        // TODO: dlog 0.5.0 doesn't support filesystem path
                        "%s_%s_%s_%s",
                        tenant,
                        namespace,
                        functionName,
                        Utils.getUniquePackageName(fileDetail.getFileName())));
        functionMetaDataBuilder.setPackageLocation(packageLocationMetaDataBuilder);
        functionMetaDataBuilder.setWorkerId(workerConfig.getWorkerId());

        return updateRequest(functionMetaDataBuilder.build(), uploadedInputStream);
    }


    @DELETE
    @Path("/{tenant}/{namespace}/{functionName}")
    public Response deregisterFunction(final @PathParam("tenant") String tenant,
                                       final @PathParam("namespace") String namespace,
                                       final @PathParam("functionName") String functionName) {

        // validate parameters
        try {
            validateDeregisterRequestParams(tenant, namespace, functionName);
        } catch (IllegalArgumentException e) {
            log.error("Invalid deregister function request @ /{}/{}/{}",
                    tenant, namespace, functionName, e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(RestUtils.createMessage(e.getMessage())).build();
        }

        FunctionRuntimeManager functionRuntimeManager = getWorkerFunctionStateManager();
        if (!functionRuntimeManager.containsFunction(tenant, namespace, functionName)) {
            log.error("Function to deregister does not exist @ /{}/{}/{}",
                    tenant, namespace, functionName);
            return Response.status(Status.NOT_FOUND)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(RestUtils.createMessage(String.format("Function %s doesn't exist", functionName))).build();
        }

        CompletableFuture<RequestResult> completableFuture
                = functionRuntimeManager.deregisterFunction(tenant, namespace, functionName);

        RequestResult requestResult = null;
        try {
            requestResult = completableFuture.get();
            if (!requestResult.isSuccess()) {
                return Response.status(Response.Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(requestResult.toJson())
                    .build();
            }
        } catch (ExecutionException e) {
            log.error("Execution Exception while deregistering function @ /{}/{}/{}",
                    tenant, namespace, functionName, e);
            return Response.serverError()
                    .type(MediaType.APPLICATION_JSON)
                    .entity(RestUtils.createMessage(e.getCause().getMessage()))
                    .build();
        } catch (InterruptedException e) {
            log.error("Interrupted Exception while deregistering function @ /{}/{}/{}",
                    tenant, namespace, functionName, e);
            return Response.status(Status.REQUEST_TIMEOUT)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(RestUtils.createMessage(e.getMessage()))
                    .build();
        }

        return Response.status(Response.Status.OK).entity(requestResult.toJson()).build();
    }

    @GET
    @Path("/{tenant}/{namespace}/{functionName}")
    public Response getFunctionInfo(final @PathParam("tenant") String tenant,
                                    final @PathParam("namespace") String namespace,
                                    final @PathParam("functionName") String functionName) throws InvalidProtocolBufferException {

        // validate parameters
        try {
            validateGetFunctionRequestParams(tenant, namespace, functionName);
        } catch (IllegalArgumentException e) {
            log.error("Invalid getFunction request @ /{}/{}/{}",
                    tenant, namespace, functionName, e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(RestUtils.createMessage(e.getMessage())).build();
        }

        FunctionRuntimeManager functionRuntimeManager = getWorkerFunctionStateManager();
        if (!functionRuntimeManager.containsFunction(tenant, namespace, functionName)) {
            log.error("Function in getFunction does not exist @ /{}/{}/{}",
                    tenant, namespace, functionName);
            return Response.status(Status.NOT_FOUND)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(RestUtils.createMessage(String.format("Function %s doesn't exist", functionName))).build();
        }

        FunctionMetaData functionMetaData = functionRuntimeManager.getFunction(tenant, namespace, functionName).getFunctionMetaData();
        String functionConfigJson = JsonFormat.printer().print(functionMetaData.getFunctionConfig());
        return Response.status(Response.Status.OK).entity(functionConfigJson).build();
    }

    @GET
    @Path("/{tenant}/{namespace}/{functionName}/status")
    public Response getFunctionStatus(final @PathParam("tenant") String tenant,
                                    final @PathParam("namespace") String namespace,
                                    final @PathParam("functionName") String functionName) {

        // validate parameters
        try {
            validateGetFunctionRequestParams(tenant, namespace, functionName);
        } catch (IllegalArgumentException e) {
            log.error("Invalid getFunctionStatus request @ /{}/{}/{}",
                    tenant, namespace, functionName, e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(RestUtils.createMessage(e.getMessage())).build();
        }

        FunctionRuntimeManager functionRuntimeManager = getWorkerFunctionStateManager();
        if (!functionRuntimeManager.containsFunction(tenant, namespace, functionName)) {
            log.error("Function in getFunctionStatus does not exist @ /{}/{}/{}",
                    tenant, namespace, functionName);
            return Response.status(Status.NOT_FOUND)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(RestUtils.createMessage(String.format("Function %s doesn't exist", functionName))).build();
        }

        FunctionRuntimeInfo functionRuntimeInfo = functionRuntimeManager.getFunction(tenant, namespace, functionName);
        Spawner spawner = functionRuntimeInfo.getSpawner();
        FunctionStatus functionStatus = new FunctionStatus();
        functionStatus.setRunning(spawner == null ? false : true);
        functionStatus.setStartupException(functionRuntimeInfo.getStartupException());
        if (spawner != null) {
            try {
                FunctionStatus f = spawner.getFunctionStatus().get();
                functionStatus.setNumProcessed(f.getNumProcessed());
                functionStatus.setNumTimeouts(f.getNumTimeouts());
                functionStatus.setNumSystemExceptions(f.getNumSystemExceptions());
                functionStatus.setNumUserExceptions(f.getNumUserExceptions());
                functionStatus.setNumSuccessfullyProcessed(f.getNumSuccessfullyProcessed());
            } catch (Exception ex) {
            }
        }
        return Response.status(Response.Status.OK).entity(new Gson().toJson(functionStatus)).build();
    }

    @GET
    @Path("/{tenant}/{namespace}")
    public Response listFunctions(final @PathParam("tenant") String tenant,
                                  final @PathParam("namespace") String namespace) {

        // validate parameters
        try {
            validateListFunctionRequestParams(tenant, namespace);
        } catch (IllegalArgumentException e) {
            log.error("Invalid listFunctions request @ /{}/{}",
                    tenant, namespace, e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(RestUtils.createMessage(e.getMessage())).build();
        }

        FunctionRuntimeManager functionRuntimeManager = getWorkerFunctionStateManager();

        Collection<String> functionStateList = functionRuntimeManager.listFunction(tenant, namespace);

        return Response.status(Response.Status.OK).entity(new Gson().toJson(functionStateList.toArray())).build();
    }

    private Response updateRequest(FunctionMetaData functionMetaData,
                                   InputStream uploadedInputStream) {
        // Upload to bookkeeper
        try {
            log.info("Uploading function package to {}", functionMetaData.getPackageLocation());

            Utils.uploadToBookeeper(
                getDlogNamespace(),
                uploadedInputStream,
                functionMetaData.getPackageLocation().getPackagePath());
        } catch (IOException e) {
            log.error("Error uploading file {}", functionMetaData.getPackageLocation(), e);
            return Response.serverError()
                    .type(MediaType.APPLICATION_JSON)
                    .entity(RestUtils.createMessage(e.getMessage()))
                    .build();
        }

        // Submit to FMT
        FunctionRuntimeManager functionRuntimeManager = getWorkerFunctionStateManager();

        CompletableFuture<RequestResult> completableFuture
                = functionRuntimeManager.updateFunction(functionMetaData);

        RequestResult requestResult = null;
        try {
            requestResult = completableFuture.get();
            if (!requestResult.isSuccess()) {
                return Response.status(Response.Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(requestResult.toJson())
                    .build();
            }
        } catch (ExecutionException e) {
            return Response.serverError()
                    .type(MediaType.APPLICATION_JSON)
                    .entity(RestUtils.createMessage(e.getCause().getMessage()))
                    .build();
        } catch (InterruptedException e) {
            return Response.status(Status.REQUEST_TIMEOUT)
                .type(MediaType.APPLICATION_JSON)
                .entity(RestUtils.createMessage(e.getCause().getMessage()))
                .build();
        }

        return Response.status(Response.Status.OK).entity(requestResult.toJson()).build();
    }

    private void validateListFunctionRequestParams(String tenant, String namespace) throws IllegalArgumentException {

        if (tenant == null) {
            throw new IllegalArgumentException("Tenant is not provided");
        }
        if (namespace == null) {
            throw new IllegalArgumentException("Namespace is not provided");
        }
    }

    private void validateGetFunctionRequestParams(String tenant,
                                                  String namespace,
                                                  String functionName) throws IllegalArgumentException {

        if (tenant == null) {
            throw new IllegalArgumentException("Tenant is not provided");
        }
        if (namespace == null) {
            throw new IllegalArgumentException("Namespace is not provided");
        }
        if (functionName == null) {
            throw new IllegalArgumentException("Function Name is not provided");
        }
    }

    private void validateDeregisterRequestParams(String tenant,
                                                 String namespace,
                                                 String functionName) throws IllegalArgumentException {

        if (tenant == null) {
            throw new IllegalArgumentException("Tenant is not provided");
        }
        if (namespace == null) {
            throw new IllegalArgumentException("Namespace is not provided");
        }
        if (functionName == null) {
            throw new IllegalArgumentException("Function Name is not provided");
        }
    }

    private FunctionConfig validateUpdateRequestParams(String tenant,
                                             String namespace,
                                             String functionName,
                                             InputStream uploadedInputStream,
                                             FormDataContentDisposition fileDetail,
                                             String functionConfigJson) throws IllegalArgumentException {
        if (tenant == null) {
            throw new IllegalArgumentException("Tenant is not provided");
        }
        if (namespace == null) {
            throw new IllegalArgumentException("Namespace is not provided");
        }
        if (functionName == null) {
            throw new IllegalArgumentException("Function Name is not provided");
        }
        if (uploadedInputStream == null || fileDetail == null) {
            throw new IllegalArgumentException("Function Package is not provided");
        }
        if (functionConfigJson == null) {
            throw new IllegalArgumentException("FunctionConfig is not provided");
        }
        try {
            FunctionConfig.Builder functionConfigBuilder = FunctionConfig.newBuilder();
            JsonFormat.parser().merge(functionConfigJson, functionConfigBuilder);
            FunctionConfig functionConfig = functionConfigBuilder.build();

            List<String> missingFields = new LinkedList<>();
            if (functionConfig.getTenant() == null || functionConfig.getTenant().isEmpty()) {
                missingFields.add("Tenant");
            }
            if (functionConfig.getNamespace() == null || functionConfig.getNamespace().isEmpty()) {
                missingFields.add("Namespace");
            }
            if (functionConfig.getName() == null || functionConfig.getName().isEmpty()) {
                missingFields.add("Name");
            }
            if (functionConfig.getClassName() == null || functionConfig.getClassName().isEmpty()) {
                missingFields.add("ClassName");
            }
            if (functionConfig.getInputSerdeClassName() == null || functionConfig.getInputSerdeClassName().isEmpty()) {
                missingFields.add("InputSerdeClassName");
            }
            if (functionConfig.getOutputSerdeClassName() == null || functionConfig.getOutputSerdeClassName().isEmpty()) {
                missingFields.add("OutputSerdeClassName");
            }
            if (functionConfig.getSourceTopic() == null || functionConfig.getSourceTopic().isEmpty()) {
                missingFields.add("SourceTopic");
            }
            if (!missingFields.isEmpty()) {
                String errorMessage = StringUtils.join(missingFields, ",");
                throw new IllegalArgumentException(errorMessage + " is not provided");
            }
            return functionConfig;
        } catch (IllegalArgumentException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalArgumentException("Invalid FunctionConfig");
        }
    }

}
