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
package org.apache.pulsar.functions.worker.rest.api;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.gson.Gson;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
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
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import lombok.extern.slf4j.Slf4j;
import net.jodah.typetools.TypeResolver;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.PackageLocationMetaData;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatus;
import static org.apache.pulsar.functions.utils.Reflections.createInstance;
import org.apache.pulsar.functions.utils.functioncache.FunctionClassLoaders;
import static org.apache.pulsar.functions.utils.functioncache.FunctionClassLoaders.create;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.MembershipManager;
import org.apache.pulsar.functions.worker.Utils;
import static org.apache.pulsar.functions.utils.Utils.HTTP;
import static org.apache.pulsar.functions.utils.Utils.FILE;
import static org.apache.pulsar.functions.utils.Utils.isFunctionPackageUrlSupported;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.request.RequestResult;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.Source;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

@Slf4j
public class FunctionsImpl {

    private final Supplier<WorkerService> workerServiceSupplier;

    public FunctionsImpl(Supplier<WorkerService> workerServiceSupplier) {
        this.workerServiceSupplier = workerServiceSupplier;
    }

    private WorkerService worker() {
        try {
            return checkNotNull(workerServiceSupplier.get());
        } catch (Throwable t) {
            log.info("Failed to get worker service", t);
            throw t;
        }
    }

    private boolean isWorkerServiceAvailable() {
        WorkerService workerService = workerServiceSupplier.get();
        if (workerService == null) {
            return false;
        }
        if (!workerService.isInitialized()) {
            return false;
        }
        return true;
    }

    @POST
    @Path("/{tenant}/{namespace}/{functionName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response registerFunction(final @PathParam("tenant") String tenant,
                                     final @PathParam("namespace") String namespace,
                                     final @PathParam("functionName") String functionName,
                                     final @FormDataParam("data") InputStream uploadedInputStream,
                                     final @FormDataParam("data") FormDataContentDisposition fileDetail,
                                     final @FormDataParam("url") String functionPkgUrl,
                                     final @FormDataParam("functionDetails") String functionDetailsJson) {

        if (!isWorkerServiceAvailable()) {
            return getUnavailableResponse();
        }

        FunctionDetails functionDetails;
        boolean isPkgUrlProvided = StringUtils.isNotBlank(functionPkgUrl);
        // validate parameters
        try {
            if(isPkgUrlProvided) {
                functionDetails = validateUpdateRequestParamsWithPkgUrl(tenant, namespace, functionName, functionPkgUrl,
                        functionDetailsJson);
            }else {
                functionDetails = validateUpdateRequestParams(tenant, namespace, functionName,
                        uploadedInputStream, fileDetail, functionDetailsJson);
            }
        } catch (Exception e) {
            log.error("Invalid register function request @ /{}/{}/{}",
                tenant, namespace, functionName, e);
            return Response.status(Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(e.getMessage())).build();
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();

        if (functionMetaDataManager.containsFunction(tenant, namespace, functionName)) {
            log.error("Function {}/{}/{} already exists", tenant, namespace, functionName);
            return Response.status(Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(String.format("Function %s already exists", functionName))).build();
        }

        // function state
        FunctionMetaData.Builder functionMetaDataBuilder = FunctionMetaData.newBuilder()
                .setFunctionDetails(functionDetails)
                .setCreateTime(System.currentTimeMillis())
                .setVersion(0);

        PackageLocationMetaData.Builder packageLocationMetaDataBuilder = PackageLocationMetaData.newBuilder()
                .setPackagePath(isPkgUrlProvided ? functionPkgUrl : createPackagePath(tenant, namespace, functionName, fileDetail.getFileName()));
        functionMetaDataBuilder.setPackageLocation(packageLocationMetaDataBuilder);

        return isPkgUrlProvided ? updateRequest(functionMetaDataBuilder.build()) : updateRequest(functionMetaDataBuilder.build(), uploadedInputStream);
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
                                   final @FormDataParam("functionDetails") String functionDetailsJson) {

        if (!isWorkerServiceAvailable()) {
            return getUnavailableResponse();
        }

        FunctionDetails functionDetails;
        boolean isPkgUrlProvided = StringUtils.isNotBlank(functionPkgUrl);
        // validate parameters
        try {
            if(isPkgUrlProvided) {
                functionDetails = validateUpdateRequestParamsWithPkgUrl(tenant, namespace, functionName,
                        functionPkgUrl, functionDetailsJson);
            }else {
                functionDetails = validateUpdateRequestParams(tenant, namespace, functionName,
                        uploadedInputStream, fileDetail, functionDetailsJson);
            }
        } catch (Exception e) {
            log.error("Invalid register function request @ /{}/{}/{}",
                tenant, namespace, functionName, e);
            return Response.status(Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(e.getMessage())).build();
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();

        if (!functionMetaDataManager.containsFunction(tenant, namespace, functionName)) {
            return Response.status(Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(String.format("Function %s doesn't exist", functionName))).build();
        }

        // function state
        FunctionMetaData.Builder functionMetaDataBuilder = FunctionMetaData.newBuilder()
                .setFunctionDetails(functionDetails)
                .setCreateTime(System.currentTimeMillis())
                .setVersion(0);

        PackageLocationMetaData.Builder packageLocationMetaDataBuilder = PackageLocationMetaData.newBuilder()
                .setPackagePath(isPkgUrlProvided ? functionPkgUrl : createPackagePath(tenant, namespace, functionName, fileDetail.getFileName()));
        functionMetaDataBuilder.setPackageLocation(packageLocationMetaDataBuilder);

        return isPkgUrlProvided ? updateRequest(functionMetaDataBuilder.build()) : updateRequest(functionMetaDataBuilder.build(), uploadedInputStream);
    }


    @DELETE
    @Path("/{tenant}/{namespace}/{functionName}")
    public Response deregisterFunction(final @PathParam("tenant") String tenant,
                                       final @PathParam("namespace") String namespace,
                                       final @PathParam("functionName") String functionName) {

        if (!isWorkerServiceAvailable()) {
            return getUnavailableResponse();
        }

        // validate parameters
        try {
            validateDeregisterRequestParams(tenant, namespace, functionName);
        } catch (IllegalArgumentException e) {
            log.error("Invalid deregister function request @ /{}/{}/{}",
                    tenant, namespace, functionName, e);
            return Response.status(Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(e.getMessage())).build();
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, functionName)) {
            log.error("Function to deregister does not exist @ /{}/{}/{}",
                    tenant, namespace, functionName);
            return Response.status(Status.NOT_FOUND)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(String.format("Function %s doesn't exist", functionName))).build();
        }

        CompletableFuture<RequestResult> completableFuture
                = functionMetaDataManager.deregisterFunction(tenant, namespace, functionName);

        RequestResult requestResult = null;
        try {
            requestResult = completableFuture.get();
            if (!requestResult.isSuccess()) {
                return Response.status(Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(requestResult.getMessage()))
                    .build();
            }
        } catch (ExecutionException e) {
            log.error("Execution Exception while deregistering function @ /{}/{}/{}",
                    tenant, namespace, functionName, e);
            return Response.serverError()
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(e.getCause().getMessage()))
                    .build();
        } catch (InterruptedException e) {
            log.error("Interrupted Exception while deregistering function @ /{}/{}/{}",
                    tenant, namespace, functionName, e);
            return Response.status(Status.REQUEST_TIMEOUT)
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }

        return Response.status(Status.OK).entity(requestResult.toJson()).build();
    }

    @GET
    @Path("/{tenant}/{namespace}/{functionName}")
    public Response getFunctionInfo(final @PathParam("tenant") String tenant,
                                    final @PathParam("namespace") String namespace,
                                    final @PathParam("functionName") String functionName)
            throws IOException {

        if (!isWorkerServiceAvailable()) {
            return getUnavailableResponse();
        }

        // validate parameters
        try {
            validateGetFunctionRequestParams(tenant, namespace, functionName);
        } catch (IllegalArgumentException e) {
            log.error("Invalid getFunction request @ /{}/{}/{}",
                    tenant, namespace, functionName, e);
            return Response.status(Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(e.getMessage())).build();
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, functionName)) {
            log.error("Function in getFunction does not exist @ /{}/{}/{}",
                    tenant, namespace, functionName);
            return Response.status(Status.NOT_FOUND)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(String.format("Function %s doesn't exist", functionName))).build();
        }

        FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace, functionName);
        String functionDetailsJson = org.apache.pulsar.functions.utils.Utils.printJson(functionMetaData.getFunctionDetails());
        return Response.status(Status.OK).entity(functionDetailsJson).build();
    }

    @GET
    @Path("/{tenant}/{namespace}/{functionName}/{instanceId}/status")
    public Response getFunctionInstanceStatus(final @PathParam("tenant") String tenant,
                                              final @PathParam("namespace") String namespace,
                                              final @PathParam("functionName") String functionName,
                                              final @PathParam("instanceId") String instanceId) throws IOException {

        if (!isWorkerServiceAvailable()) {
            return getUnavailableResponse();
        }

        // validate parameters
        try {
            validateGetFunctionInstanceRequestParams(tenant, namespace, functionName, instanceId);
        } catch (IllegalArgumentException e) {
            log.error("Invalid getFunctionStatus request @ /{}/{}/{}",
                    tenant, namespace, functionName, e);
            return Response.status(Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(e.getMessage())).build();
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, functionName)) {
            log.error("Function in getFunctionStatus does not exist @ /{}/{}/{}",
                    tenant, namespace, functionName);
            return Response.status(Status.NOT_FOUND)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(String.format("Function %s doesn't exist", functionName))).build();
        }

        FunctionRuntimeManager functionRuntimeManager = worker().getFunctionRuntimeManager();
        FunctionStatus functionStatus = null;
        try {
            functionStatus = functionRuntimeManager.getFunctionInstanceStatus(
                    tenant, namespace, functionName, Integer.parseInt(instanceId));
        } catch (Exception e) {
            log.error("Got Exception Getting Status", e);
            FunctionStatus.Builder functionStatusBuilder = FunctionStatus.newBuilder();
            functionStatusBuilder.setRunning(false);
            String functionDetailsJson = org.apache.pulsar.functions.utils.Utils.printJson(functionStatusBuilder.build());
            return Response.status(Status.OK).entity(functionDetailsJson).build();
        }

        String jsonResponse = org.apache.pulsar.functions.utils.Utils.printJson(functionStatus);
        return Response.status(Status.OK).entity(jsonResponse).build();
    }

    @GET
    @Path("/{tenant}/{namespace}/{functionName}/status")
    public Response getFunctionStatus(final @PathParam("tenant") String tenant,
                                      final @PathParam("namespace") String namespace,
                                      final @PathParam("functionName") String functionName) throws IOException {

        if (!isWorkerServiceAvailable()) {
            return getUnavailableResponse();
        }

        // validate parameters
        try {
            validateGetFunctionRequestParams(tenant, namespace, functionName);
        } catch (IllegalArgumentException e) {
            log.error("Invalid getFunctionStatus request @ /{}/{}/{}",
                    tenant, namespace, functionName, e);
            return Response.status(Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(e.getMessage())).build();
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, functionName)) {
            log.error("Function in getFunctionStatus does not exist @ /{}/{}/{}",
                    tenant, namespace, functionName);
            return Response.status(Status.NOT_FOUND)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(String.format("Function %s doesn't exist", functionName))).build();
        }

        FunctionRuntimeManager functionRuntimeManager = worker().getFunctionRuntimeManager();
        InstanceCommunication.FunctionStatusList functionStatusList = null;
        try {
            functionStatusList = functionRuntimeManager.getAllFunctionStatus(tenant, namespace, functionName);
        } catch (Exception e) {
            log.error("Got Exception Getting Status", e);
            FunctionStatus.Builder functionStatusBuilder = FunctionStatus.newBuilder();
            functionStatusBuilder.setRunning(false);
            String functionDetailsJson = org.apache.pulsar.functions.utils.Utils.printJson(functionStatusBuilder.build());
            return Response.status(Status.OK).entity(functionDetailsJson).build();
        }

        String jsonResponse = org.apache.pulsar.functions.utils.Utils.printJson(functionStatusList);
        return Response.status(Status.OK).entity(jsonResponse).build();
    }

    @GET
    @Path("/{tenant}/{namespace}")
    public Response listFunctions(final @PathParam("tenant") String tenant,
                                  final @PathParam("namespace") String namespace) {

        if (!isWorkerServiceAvailable()) {
            return getUnavailableResponse();
        }

        // validate parameters
        try {
            validateListFunctionRequestParams(tenant, namespace);
        } catch (IllegalArgumentException e) {
            log.error("Invalid listFunctions request @ /{}/{}",
                    tenant, namespace, e);
            return Response.status(Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(e.getMessage())).build();
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();

        Collection<String> functionStateList = functionMetaDataManager.listFunctions(tenant, namespace);

        return Response.status(Status.OK).entity(new Gson().toJson(functionStateList.toArray())).build();
    }

    private Response updateRequest(FunctionMetaData functionMetaData, InputStream uploadedInputStream) {
        // Upload to bookkeeper
        try {
            log.info("Uploading function package to {}", functionMetaData.getPackageLocation());

            Utils.uploadToBookeeper(worker().getDlogNamespace(), uploadedInputStream,
                    functionMetaData.getPackageLocation().getPackagePath());
        } catch (IOException e) {
            log.error("Error uploading file {}", functionMetaData.getPackageLocation(), e);
            return Response.serverError().type(MediaType.APPLICATION_JSON).entity(new ErrorData(e.getMessage()))
                    .build();
        }
        return updateRequest(functionMetaData);
    }
    
    private Response updateRequest(FunctionMetaData functionMetaData) {

        // Submit to FMT
        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();

        CompletableFuture<RequestResult> completableFuture
                = functionMetaDataManager.updateFunction(functionMetaData);

        RequestResult requestResult = null;
        try {
            requestResult = completableFuture.get();
            if (!requestResult.isSuccess()) {
                return Response.status(Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(requestResult.getMessage()))
                    .build();
            }
        } catch (ExecutionException e) {
            return Response.serverError()
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(e.getCause().getMessage()))
                    .build();
        } catch (InterruptedException e) {
            return Response.status(Status.REQUEST_TIMEOUT)
                .type(MediaType.APPLICATION_JSON)
                .entity(new ErrorData(e.getCause().getMessage()))
                .build();
        }

        return Response.status(Status.OK).build();
    }

    @GET
    @Path("/cluster")
    public Response getCluster() {

        if (!isWorkerServiceAvailable()) {
            return getUnavailableResponse();
        }

        MembershipManager membershipManager = worker().getMembershipManager();
        List<MembershipManager.WorkerInfo> members = membershipManager.getCurrentMembership();
        return Response.status(Status.OK).entity(new Gson().toJson(members)).build();
    }

    @GET
    @Path("/assignments")
    public Response getAssignments() {

        if (!isWorkerServiceAvailable()) {
            return getUnavailableResponse();
        }

        FunctionRuntimeManager functionRuntimeManager = worker().getFunctionRuntimeManager();
        Map<String, Map<String, Function.Assignment>> assignments = functionRuntimeManager.getCurrentAssignments();
        Map<String, Collection<String>> ret = new HashMap<>();
        for (Map.Entry<String, Map<String, Function.Assignment>> entry : assignments.entrySet()) {
            ret.put(entry.getKey(), entry.getValue().keySet());
        }
        return Response.status(Status.OK).entity(
                new Gson().toJson(ret)).build();
    }

    @POST
    @Path("/{tenant}/{namespace}/{functionName}/trigger")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response triggerFunction(final @PathParam("tenant") String tenant,
                                    final @PathParam("namespace") String namespace,
                                    final @PathParam("name") String functionName,
                                    final @FormDataParam("data") String input,
                                    final @FormDataParam("dataStream") InputStream uploadedInputStream,
                                    final @FormDataParam("topic") String topic) {

        if (!isWorkerServiceAvailable()) {
            return getUnavailableResponse();
        }

        FunctionDetails functionDetails;
        // validate parameters
        try {
            validateTriggerRequestParams(tenant, namespace, functionName, topic, input, uploadedInputStream);
        } catch (IllegalArgumentException e) {
            log.error("Invalid trigger function request @ /{}/{}/{}",
                    tenant, namespace, functionName, e);
            return Response.status(Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(e.getMessage())).build();
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, functionName)) {
            log.error("Function in getFunction does not exist @ /{}/{}/{}",
                    tenant, namespace, functionName);
            return Response.status(Status.NOT_FOUND)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(String.format("Function %s doesn't exist", functionName))).build();
        }

        FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace, functionName);

        String inputTopicToWrite;
        if (topic != null) {
            inputTopicToWrite = topic;
        } else if (functionMetaData.getFunctionDetails().getSource().getTopicsToSerDeClassNameMap().size() == 1) {
            inputTopicToWrite =
                    functionMetaData.getFunctionDetails().getSource().getTopicsToSerDeClassNameMap().keySet().iterator().next();
        } else {
            return Response.status(Status.BAD_REQUEST).build();
        }
        if (functionMetaData.getFunctionDetails().getSource().getTopicsToSerDeClassNameMap() == null
            || !functionMetaData.getFunctionDetails().getSource().getTopicsToSerDeClassNameMap().containsKey(inputTopicToWrite)) {
            return Response.status(Status.BAD_REQUEST).build();
        }
        String outputTopic = functionMetaData.getFunctionDetails().getSink().getTopic();
        Reader reader = null;
        Producer producer = null;
        try {
            if (outputTopic != null && !outputTopic.isEmpty()) {
                reader = worker().getClient().newReader().topic(outputTopic).startMessageId(MessageId.latest).create();
            }
            producer = worker().getClient().newProducer().topic(inputTopicToWrite).create();
            byte[] targetArray;
            if (uploadedInputStream != null) {
                targetArray = new byte[uploadedInputStream.available()];
                uploadedInputStream.read(targetArray);
            } else {
                targetArray = input.getBytes();
            }
            MessageId msgId = producer.send(targetArray);
            if (reader == null) {
                return Response.status(Status.OK).build();
            }
            long curTime = System.currentTimeMillis();
            long maxTime = curTime + 1000;
            while (curTime < maxTime) {
                Message msg = reader.readNext(10000, TimeUnit.MILLISECONDS);
                if (msg == null) break;
                if (msg.getProperties().containsKey("__pfn_input_msg_id__") &&
                        msg.getProperties().containsKey("__pfn_input_topic__")) {
                    MessageId newMsgId = MessageId.fromByteArray(Base64.getDecoder().decode((String) msg.getProperties().get("__pfn_input_msg_id__")));
                    if (msgId.equals(newMsgId) && msg.getProperties().get("__pfn_input_topic__").equals(inputTopicToWrite)) {
                        return Response.status(Status.OK).entity(msg.getData()).build();
                    }
                }
                curTime = System.currentTimeMillis();
            }
            return Response.status(Status.REQUEST_TIMEOUT).build();
        } catch (Exception e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        } finally {
            if (reader != null) {
                reader.closeAsync();
            }
            if (producer != null) {
                producer.closeAsync();
            }
        }
    }

    @POST
    @Path("/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response uploadFunction(final @FormDataParam("data") InputStream uploadedInputStream,
                                   final @FormDataParam("path") String path) {
        // validate parameters
        try {
            if (uploadedInputStream == null || path == null) {
                throw new IllegalArgumentException("Function Package is not provided " + path);
            }
        } catch (IllegalArgumentException e) {
            log.error("Invalid upload function request @ /{}", path, e);
            return Response.status(Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(e.getMessage())).build();
        }

        // Upload to bookkeeper
        try {
            log.info("Uploading function package to {}", path);

            Utils.uploadToBookeeper(
                    worker().getDlogNamespace(),
                    uploadedInputStream,
                    Codec.encode(path));
        } catch (IOException e) {
            log.error("Error uploading file {}", path, e);
            return Response.serverError()
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(e.getMessage()))
                    .build();
        }

        return Response.status(Status.OK).build();
    }

    @GET
    @Path("/download")
    public Response downloadFunction(final @QueryParam("path") String path) {
        return Response.status(Status.OK).entity(new StreamingOutput() {
            @Override
            public void write(final OutputStream output) throws IOException {
                if (path.startsWith(HTTP)) {
                    URL url = new URL(path);
                    IOUtils.copy(url.openStream(), output);
                } else if (path.startsWith(FILE)) {
                    URL url = new URL(path);
                    File file;
                    try {
                        file = new File(url.toURI());
                        IOUtils.copy(new FileInputStream(file), output);
                    } catch (URISyntaxException e) {
                        throw new IllegalArgumentException("invalid file url path: " + path);
                    }
                } else {
                    Utils.downloadFromBookkeeper(worker().getDlogNamespace(), output, Codec.encode(path));
                }
            }
        }).build();
    }

    private void validateListFunctionRequestParams(String tenant, String namespace) throws IllegalArgumentException {

        if (tenant == null) {
            throw new IllegalArgumentException("Tenant is not provided");
        }
        if (namespace == null) {
            throw new IllegalArgumentException("Namespace is not provided");
        }
    }

    private void validateGetFunctionInstanceRequestParams(String tenant,
                                                          String namespace,
                                                          String functionName,
                                                          String instanceId) throws IllegalArgumentException {
        validateGetFunctionRequestParams(tenant, namespace, functionName);
        if (instanceId == null) {
            throw new IllegalArgumentException("Function Instance Id is not provided");

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

    private FunctionDetails validateUpdateRequestParamsWithPkgUrl(String tenant, String namespace, String functionName,
            String functionPkgUrl, String functionDetailsJson)
            throws IllegalArgumentException, IOException, URISyntaxException {
        if (!isFunctionPackageUrlSupported(functionPkgUrl)) {
            throw new IllegalArgumentException("Function Package url is not valid. supported url (http/https/file)");
        }
        Utils.validateFileUrl(functionPkgUrl, workerServiceSupplier.get().getWorkerConfig().getDownloadDirectory());
        File jarWithFileUrl = functionPkgUrl.startsWith(FILE) ? (new File((new URL(functionPkgUrl)).toURI())) : null;
        FunctionDetails functionDetails = validateUpdateRequestParams(tenant, namespace, functionName,
                functionDetailsJson, jarWithFileUrl);
        return functionDetails;
    }

    private FunctionDetails validateUpdateRequestParams(String tenant,
            String namespace,
            String functionName,
            InputStream uploadedInputStream,
            FormDataContentDisposition fileDetail,
            String functionDetailsJson) throws IllegalArgumentException {
        if (uploadedInputStream == null || fileDetail == null) {
            throw new IllegalArgumentException("Function Package is not provided");
        }
        return validateUpdateRequestParams(tenant, namespace, functionName, functionDetailsJson, null);
    }
    
    private FunctionDetails validateUpdateRequestParams(String tenant,
                                             String namespace,
                                             String functionName,
                                             String functionDetailsJson,
                                             File jarWithFileUrl) throws IllegalArgumentException {
        if (tenant == null) {
            throw new IllegalArgumentException("Tenant is not provided");
        }
        if (namespace == null) {
            throw new IllegalArgumentException("Namespace is not provided");
        }
        if (functionName == null) {
            throw new IllegalArgumentException("Function Name is not provided");
        }
        
        if (functionDetailsJson == null) {
            throw new IllegalArgumentException("FunctionDetails is not provided");
        }
        try {
            FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
            org.apache.pulsar.functions.utils.Utils.mergeJson(functionDetailsJson, functionDetailsBuilder);
            validateFunctionClassTypes(jarWithFileUrl, functionDetailsBuilder);
            FunctionDetails functionDetails = functionDetailsBuilder.build();

            List<String> missingFields = new LinkedList<>();
            if (functionDetails.getTenant() == null || functionDetails.getTenant().isEmpty()) {
                missingFields.add("Tenant");
            }
            if (functionDetails.getNamespace() == null || functionDetails.getNamespace().isEmpty()) {
                missingFields.add("Namespace");
            }
            if (functionDetails.getName() == null || functionDetails.getName().isEmpty()) {
                missingFields.add("Name");
            }
            if (functionDetails.getClassName() == null || functionDetails.getClassName().isEmpty()) {
                missingFields.add("ClassName");
            }
            // TODO in the future add more check here for functions and connectors
            if (!functionDetails.getSource().isInitialized()) {
                missingFields.add("Source");
            }
            // TODO in the future add more check here for functions and connectors
            if (!functionDetails.getSink().isInitialized()) {
                missingFields.add("Sink");
            }
            if (!missingFields.isEmpty()) {
                String errorMessage = StringUtils.join(missingFields, ",");
                throw new IllegalArgumentException(errorMessage + " is not provided");
            }
            if (functionDetails.getParallelism() <= 0) {
                throw new IllegalArgumentException("Parallelism needs to be set to a positive number");
            }
            return functionDetails;
        } catch (IllegalArgumentException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalArgumentException("Invalid FunctionDetails");
        }
    }

    private void validateFunctionClassTypes(File jarFile, FunctionDetails.Builder functionDetailsBuilder)
            throws MalformedURLException {

        // validate only if jar-file is provided
        if(jarFile == null) {
            return;
        }
        
        if (StringUtils.isBlank(functionDetailsBuilder.getClassName())) {
            throw new IllegalArgumentException("function class-name can't be empty");
        }

        URL[] urls = new URL[1];
        urls[0] = jarFile.toURI().toURL();
        URLClassLoader classLoader = create(urls, FunctionClassLoaders.class.getClassLoader());

        // validate function class-type
        Object functionObject = createInstance(functionDetailsBuilder.getClassName(), classLoader);
        if (!(functionObject instanceof org.apache.pulsar.functions.api.Function) && !(functionObject instanceof java.util.function.Function)) {
            throw new RuntimeException("User class must either be Function or java.util.Function");
        }

        if (functionDetailsBuilder.hasSource() && functionDetailsBuilder.getSource() != null
                && StringUtils.isNotBlank(functionDetailsBuilder.getSource().getClassName())) {
            try {
                String sourceClassName = functionDetailsBuilder.getSource().getClassName();
                String argClassName = getTypeArg(sourceClassName, Source.class, classLoader).getName();
                functionDetailsBuilder.setSource(functionDetailsBuilder.getSourceBuilder()
                        .setTypeClassName(argClassName));
                
                // if sink-class not present then set same arg as source
                if (!functionDetailsBuilder.hasSink()
                        || StringUtils.isBlank(functionDetailsBuilder.getSink().getClassName())) {
                    functionDetailsBuilder
                            .setSink(functionDetailsBuilder.getSinkBuilder().setTypeClassName(argClassName));
                }
                
            } catch (IllegalArgumentException ie) {
                throw ie;
            } catch (Exception e) {
                log.error("Failed to validate source class", e);
                throw new IllegalArgumentException("Failed to validate source class-name", e);
            }
        }

        if (functionDetailsBuilder.hasSink() && functionDetailsBuilder.getSink() != null
                && StringUtils.isNotBlank(functionDetailsBuilder.getSink().getClassName())) {
            try {
                String sinkClassName = functionDetailsBuilder.getSink().getClassName();
                String argClassName = getTypeArg(sinkClassName, Sink.class, classLoader).getName();
                functionDetailsBuilder.setSink(functionDetailsBuilder.getSinkBuilder()
                        .setTypeClassName(argClassName));
                
                // if source-class not present then set same arg as sink
                if (!functionDetailsBuilder.hasSource()
                        || StringUtils.isBlank(functionDetailsBuilder.getSource().getClassName())) {
                    functionDetailsBuilder
                            .setSource(functionDetailsBuilder.getSourceBuilder().setTypeClassName(argClassName));
                }
                
            } catch (IllegalArgumentException ie) {
                throw ie;
            } catch (Exception e) {
                log.error("Failed to validate sink class", e);
                throw new IllegalArgumentException("Failed to validate sink class-name", e);
            }
        }
    }

    private Class<?> getTypeArg(String className, Class<?> funClass, URLClassLoader classLoader)
            throws ClassNotFoundException {
        Class<?> loadedClass = classLoader.loadClass(className);
        if (!funClass.isAssignableFrom(loadedClass)) {
            throw new IllegalArgumentException(
                    String.format("class %s is not type of %s", className, funClass.getName()));
        }
        return TypeResolver.resolveRawArgument(funClass, loadedClass);
    }

    private void validateTriggerRequestParams(String tenant,
                                              String namespace,
                                              String functionName,
                                              String topic,
                                              String input,
                                              InputStream uploadedInputStream) {
        if (tenant == null) {
            throw new IllegalArgumentException("Tenant is not provided");
        }
        if (namespace == null) {
            throw new IllegalArgumentException("Namespace is not provided");
        }
        if (functionName == null) {
            throw new IllegalArgumentException("Function Name is not provided");
        }
        if (uploadedInputStream == null && input == null) {
            throw new IllegalArgumentException("Trigger Data is not provided");
        }
    }

    private Response getUnavailableResponse() {
        return Response.status(Status.SERVICE_UNAVAILABLE)
                .type(MediaType.APPLICATION_JSON)
                .entity(new ErrorData("Function worker service is not done initializing. "
                        + "Please try again in a little while."))
                .build();
    }

    public static String createPackagePath(String tenant, String namespace, String functionName, String fileName) {
        return String.format("%s/%s/%s/%s", tenant, namespace, Codec.encode(functionName),
                Utils.getUniquePackageName(Codec.encode(fileName)));
    }
}
