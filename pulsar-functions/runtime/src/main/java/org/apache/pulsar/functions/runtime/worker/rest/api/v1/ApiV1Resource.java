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
package org.apache.pulsar.functions.runtime.worker.rest.api.v1;

import com.google.gson.Gson;
import javax.ws.rs.core.Response.Status;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.fs.FunctionConfig;
import org.apache.pulsar.functions.runtime.spawner.LimitsConfig;
import org.apache.pulsar.functions.runtime.worker.FunctionMetaData;
import org.apache.pulsar.functions.runtime.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.runtime.worker.PackageLocationMetaData;
import org.apache.pulsar.functions.runtime.worker.Utils;
import org.apache.pulsar.functions.runtime.worker.request.RequestResult;
import org.apache.pulsar.functions.runtime.worker.WorkerConfig;
import org.apache.pulsar.functions.runtime.worker.rest.BaseApiResource;
import org.apache.pulsar.functions.runtime.worker.rest.RestUtils;
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
                                     final @FormDataParam("sinkTopic") String sinkTopic,
                                     final @FormDataParam("sourceTopic") String sourceTopic,
                                     final @FormDataParam("inputSerdeClassName") String inputSerdeClassName,
                                     final @FormDataParam("outputSerdeClassName") String outputSerdeClassName,
                                     final @FormDataParam("className") String className) {

        // validate parameters
        try {
            validateUpdateRequestParams(tenant, namespace, functionName,
                    uploadedInputStream, fileDetail, sinkTopic, sourceTopic,
                    inputSerdeClassName, outputSerdeClassName, className);
        } catch (IllegalArgumentException e) {
            log.error("Invalid register function request @ /{}/{}/{}",
                tenant, namespace, functionName, e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(RestUtils.createMessage(e.getMessage())).build();
        }

        FunctionMetaDataManager functionMetaDataManager = getWorkerFunctionStateManager();

        if (functionMetaDataManager.containsFunction(tenant, namespace, functionName)) {
            log.error("Function {}/{}/{} already exists", tenant, namespace, functionName);
            return Response.status(Response.Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(RestUtils.createMessage(String.format("Function %s already exist", functionName))).build();
        }

        // function configuration
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(functionName);
        functionConfig.setSourceTopic(sourceTopic);
        functionConfig.setSinkTopic(sinkTopic);
        functionConfig.setInputSerdeClassName(inputSerdeClassName);
        functionConfig.setOutputSerdeClassName(outputSerdeClassName);
        functionConfig.setClassName(className);

        // function resource limits
        LimitsConfig limitsConfig = getWorkerConfig().getDefaultLimits();

        // function state
        FunctionMetaData functionMetaData = new FunctionMetaData();
        functionMetaData.setFunctionConfig(functionConfig);
        functionMetaData.setLimitsConfig(limitsConfig);
        functionMetaData.setCreateTime(System.currentTimeMillis());
        functionMetaData.setVersion(0);

        WorkerConfig workerConfig = getWorkerConfig();
        PackageLocationMetaData packageLocationMetaData = new PackageLocationMetaData();
        packageLocationMetaData.setPackagePath(String.format(
            "%s/%s/%s/%s",
            tenant,
            namespace,
            functionName,
            Utils.getUniquePackageName(fileDetail.getFileName())));
        functionMetaData.setPackageLocation(packageLocationMetaData);
        functionMetaData.setWorkerId(workerConfig.getWorkerId());
        
        return updateRequest(functionMetaData, uploadedInputStream);
    }

    @PUT
    @Path("/{tenant}/{namespace}/{functionName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response updateFunction(final @PathParam("tenant") String tenant,
                                   final @PathParam("namespace") String namespace,
                                   final @PathParam("functionName") String functionName,
                                   final @FormDataParam("data") InputStream uploadedInputStream,
                                   final @FormDataParam("data") FormDataContentDisposition fileDetail,
                                   final @FormDataParam("sinkTopic") String sinkTopic,
                                   final @FormDataParam("sourceTopic") String sourceTopic,
                                   final @FormDataParam("inputSerdeClassName") String inputSerdeClassName,
                                   final @FormDataParam("outputSerdeClassName") String outputSerdeClassName,
                                   final @FormDataParam("className") String className) {

        // validate parameters
        try {
            validateUpdateRequestParams(tenant, namespace, functionName,
                    uploadedInputStream, fileDetail, sinkTopic, sourceTopic,
                    inputSerdeClassName, outputSerdeClassName, className);
        } catch (IllegalArgumentException e) {
            log.error("Invalid update function request @ /{}/{}/{}",
                    tenant, namespace, functionName, e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(RestUtils.createMessage(e.getMessage())).build();
        }

        FunctionMetaDataManager functionMetaDataManager = getWorkerFunctionStateManager();

        if (!functionMetaDataManager.containsFunction(tenant, namespace, functionName)) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(RestUtils.createMessage(String.format("Function %s doesn't exist", functionName))).build();
        }

        // function configuration
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(functionName);
        functionConfig.setSourceTopic(sourceTopic);
        functionConfig.setSinkTopic(sinkTopic);
        functionConfig.setInputSerdeClassName(inputSerdeClassName);
        functionConfig.setOutputSerdeClassName(outputSerdeClassName);
        functionConfig.setClassName(className);

        // function resource limits
        LimitsConfig limitsConfig = getWorkerConfig().getDefaultLimits();

        // function state
        FunctionMetaData functionMetaData = new FunctionMetaData();
        functionMetaData.setFunctionConfig(functionConfig);
        functionMetaData.setLimitsConfig(limitsConfig);
        functionMetaData.setCreateTime(System.currentTimeMillis());
        functionMetaData.setVersion(0);

        WorkerConfig workerConfig = getWorkerConfig();
        PackageLocationMetaData packageLocationMetaData = new PackageLocationMetaData();
        packageLocationMetaData.setPackagePath(String.format(
            "%s/%s/%s/%s",
            tenant,
            namespace,
            functionName,
            Utils.getUniquePackageName(fileDetail.getFileName())));
        functionMetaData.setPackageLocation(packageLocationMetaData);
        functionMetaData.setWorkerId(workerConfig.getWorkerId());

        return updateRequest(functionMetaData, uploadedInputStream);
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

        FunctionMetaDataManager functionMetaDataManager = getWorkerFunctionStateManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, functionName)) {
            log.error("Function to deregister does not exist @ /{}/{}/{}",
                    tenant, namespace, functionName);
            return Response.status(Response.Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(RestUtils.createMessage(String.format("Function %s doesn't exist", functionName))).build();
        }

        CompletableFuture<RequestResult> completableFuture
                = functionMetaDataManager.deregisterFunction(tenant, namespace, functionName);

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
                                    final @PathParam("functionName") String functionName) {

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

        FunctionMetaDataManager functionMetaDataManager = getWorkerFunctionStateManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, functionName)) {
            log.error("Function in getFunction does not exist @ /{}/{}/{}",
                    tenant, namespace, functionName);
            return Response.status(Status.NOT_FOUND)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(RestUtils.createMessage(String.format("Function %s doesn't exist", functionName))).build();
        }

        FunctionMetaData functionMetaData = functionMetaDataManager.getFunction(tenant, namespace, functionName);
        return Response.status(Response.Status.OK).entity(functionMetaData.toJson()).build();
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

        FunctionMetaDataManager functionMetaDataManager = getWorkerFunctionStateManager();

        Collection<String> functionStateList = functionMetaDataManager.listFunction(tenant, namespace);

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
        FunctionMetaDataManager functionMetaDataManager = getWorkerFunctionStateManager();

        CompletableFuture<RequestResult> completableFuture
                = functionMetaDataManager.updateFunction(functionMetaData);

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

    private void validateUpdateRequestParams(String tenant,
                                             String namespace,
                                             String functionName,
                                             InputStream uploadedInputStream,
                                             FormDataContentDisposition fileDetail,
                                             String sinkTpic,
                                             String sourceTopic,
                                             String inputSerdeClassName,
                                             String outputSerdeClassName,
                                             String className) throws IllegalArgumentException {
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
        if (sourceTopic == null) {
            throw new IllegalArgumentException("Source Topic is not provided");
        }
        if (inputSerdeClassName == null) {
            throw new IllegalArgumentException("InputSerdeClassName is not provided");
        }
        if (outputSerdeClassName == null) {
            throw new IllegalArgumentException("OutputSerdeClassName is not provided");
        }
        if (className == null) {
            throw new IllegalArgumentException("ClassName is not provided");
        }
    }

}
