/*
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

import com.google.gson.Gson;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import javax.ws.rs.core.Response;
import lombok.CustomLog;
import org.apache.pulsar.broker.authentication.AuthenticationParameters;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.proto.FunctionDetails;
import org.apache.pulsar.functions.proto.FunctionMetaData;
import org.apache.pulsar.functions.proto.FunctionStatusList;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.PulsarWorkerService;
import org.apache.pulsar.functions.worker.service.api.Functions;
import org.apache.pulsar.functions.worker.service.api.FunctionsV2;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

@CustomLog
public class FunctionsImplV2 implements FunctionsV2<PulsarWorkerService> {

    private final Functions<PulsarWorkerService> delegate;

    public FunctionsImplV2(Supplier<PulsarWorkerService> workerServiceSupplier) {
        this.delegate = new FunctionsImpl(workerServiceSupplier);
    }

    // For test purposes
    public FunctionsImplV2(FunctionsImpl delegate) {
        this.delegate = delegate;
    }

    @Override
    public Response getFunctionInfo(final String tenant, final String namespace,
                                    final String functionName, AuthenticationParameters authParams) {

        // run just for parameter checks
        delegate.getFunctionInfo(tenant, namespace, functionName, authParams);

        FunctionMetaDataManager functionMetaDataManager = delegate.worker().getFunctionMetaDataManager();

        FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace,
                functionName);
        String functionDetailsJson = functionMetaData.getFunctionDetails().toJson();
        return Response.status(Response.Status.OK).entity(functionDetailsJson).build();
    }

    @Override
    public Response getFunctionInstanceStatus(final String tenant, final String namespace, final String functionName,
                                              final String instanceId, URI uri,
                                              AuthenticationParameters authParams) {

        org.apache.pulsar.common.policies.data.FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData
                functionInstanceStatus = delegate.getFunctionInstanceStatus(tenant, namespace,
                functionName, instanceId, uri, authParams);

        String jsonResponse = toProto(functionInstanceStatus, instanceId).toJson();
        return Response.status(Response.Status.OK).entity(jsonResponse).build();
    }

    @Override
    public Response getFunctionStatusV2(String tenant, String namespace, String functionName,
                                        URI requestUri, AuthenticationParameters authParams) {
        FunctionStatus functionStatus = delegate.getFunctionStatus(tenant, namespace,
                functionName, requestUri, authParams);
        FunctionStatusList functionStatusList = new FunctionStatusList();
        for (FunctionStatus.FunctionInstanceStatus instanceStatus : functionStatus.instances) {
            toProto(functionStatusList.addFunctionStatus(),
                    instanceStatus.getStatus(),
                    String.valueOf(instanceStatus.getInstanceId()));
        }
        String jsonResponse = functionStatusList.toJson();
        return Response.status(Response.Status.OK).entity(jsonResponse).build();
    }

    @Override
    public Response registerFunction(String tenant, String namespace, String functionName, InputStream
            uploadedInputStream, FormDataContentDisposition fileDetail, String functionPkgUrl, String
                                             functionDetailsJson, AuthenticationParameters authParams) {

        FunctionDetails functionDetails = new FunctionDetails();
        try {
            functionDetails.parseFromJson(functionDetailsJson);
        } catch (Exception e) {
            throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
        }
        FunctionConfig functionConfig = FunctionConfigUtils.convertFromDetails(functionDetails);

        delegate.registerFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail,
                functionPkgUrl, functionConfig, authParams);
        return Response.ok().build();
    }

    @Override
    public Response updateFunction(String tenant, String namespace, String functionName,
                                   InputStream uploadedInputStream, FormDataContentDisposition fileDetail,
                                   String functionPkgUrl, String functionDetailsJson,
                                   AuthenticationParameters authParams) {

        FunctionDetails functionDetails = new FunctionDetails();
        try {
            functionDetails.parseFromJson(functionDetailsJson);
        } catch (Exception e) {
            throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
        }
        FunctionConfig functionConfig = FunctionConfigUtils.convertFromDetails(functionDetails);

        delegate.updateFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail,
                functionPkgUrl, functionConfig, authParams, null);
        return Response.ok().build();
    }

    @Override
    public Response deregisterFunction(String tenant, String namespace, String functionName,
                                       AuthenticationParameters authParams) {
        delegate.deregisterFunction(tenant, namespace, functionName, authParams);
        return Response.ok().build();
    }

    @Override
    public Response listFunctions(String tenant, String namespace, AuthenticationParameters authParams) {
        Collection<String> functionStateList = delegate.listFunctions(tenant, namespace, authParams);
        return Response.status(Response.Status.OK).entity(new Gson().toJson(functionStateList.toArray())).build();
    }

    @Override
    public Response triggerFunction(String tenant, String namespace, String functionName, String triggerValue,
                                    InputStream triggerStream, String topic, AuthenticationParameters authParams) {
        String result = delegate.triggerFunction(tenant, namespace, functionName,
                triggerValue, triggerStream, topic, authParams);
        return Response.status(Response.Status.OK).entity(result).build();
    }

    @Override
    public Response getFunctionState(String tenant, String namespace, String functionName,
                                     String key, AuthenticationParameters authParams) {
        FunctionState functionState = delegate.getFunctionState(
                tenant, namespace, functionName, key, authParams);

        String value;
        if (functionState.getNumberValue() != null) {
            value = "value : " + functionState.getNumberValue() + ", version : " + functionState.getVersion();
        } else {
            value = "value : " + functionState.getStringValue() + ", version : " + functionState.getVersion();
        }
        return Response.status(Response.Status.OK)
                .entity(value)
                .build();
    }

    @Override
    public Response restartFunctionInstance(String tenant, String namespace, String functionName, String instanceId, URI
            uri, AuthenticationParameters authParams) {
        delegate.restartFunctionInstance(tenant, namespace, functionName, instanceId, uri, authParams);
        return Response.ok().build();
    }

    @Override
    public Response restartFunctionInstances(String tenant, String namespace, String functionName,
                                             AuthenticationParameters authParams) {
        delegate.restartFunctionInstances(tenant, namespace, functionName, authParams);
        return Response.ok().build();
    }

    @Override
    public Response stopFunctionInstance(String tenant, String namespace, String functionName, String instanceId, URI
            uri, AuthenticationParameters authParams) {
        delegate.stopFunctionInstance(tenant, namespace, functionName, instanceId, uri, authParams);
        return Response.ok().build();
    }

    @Override
    public Response stopFunctionInstances(String tenant, String namespace, String functionName,
                                          AuthenticationParameters authParams) {
        delegate.stopFunctionInstances(tenant, namespace, functionName, authParams);
        return Response.ok().build();
    }

    @Override
    public Response uploadFunction(InputStream uploadedInputStream, String path, AuthenticationParameters authParams) {
        delegate.uploadFunction(uploadedInputStream, path, authParams);
        return Response.ok().build();
    }

    @Override
    public Response downloadFunction(String path, AuthenticationParameters authParams) {
        return Response.status(Response.Status.OK).entity(delegate.downloadFunction(path, authParams)).build();
    }

    @Override
    public List<ConnectorDefinition> getListOfConnectors() {
        return delegate.getListOfConnectors();
    }

    private org.apache.pulsar.functions.proto.FunctionStatus toProto(
            org.apache.pulsar.common.policies.data.FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData
                    functionInstanceStatus, String instanceId) {
        org.apache.pulsar.functions.proto.FunctionStatus status =
                new org.apache.pulsar.functions.proto.FunctionStatus();
        toProto(status, functionInstanceStatus, instanceId);
        return status;
    }

    private void toProto(
            org.apache.pulsar.functions.proto.FunctionStatus status,
            org.apache.pulsar.common.policies.data.FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData
                    functionInstanceStatus, String instanceId) {
        status.setRunning(functionInstanceStatus.isRunning())
              .setFailureException(functionInstanceStatus.getError())
              .setNumRestarts(functionInstanceStatus.getNumRestarts())
              .setNumSuccessfullyProcessed(functionInstanceStatus.getNumSuccessfullyProcessed())
              .setNumUserExceptions(functionInstanceStatus.getNumUserExceptions())
              .setNumSystemExceptions(functionInstanceStatus.getNumSystemExceptions())
              .setAverageLatency(functionInstanceStatus.getAverageLatency())
              .setLastInvocationTime(functionInstanceStatus.getLastInvocationTime())
              .setInstanceId(instanceId)
              .setWorkerId(delegate.worker().getWorkerConfig().getWorkerId());

        for (org.apache.pulsar.common.policies.data.ExceptionInformation ex :
                functionInstanceStatus.getLatestUserExceptions()) {
            status.addLatestUserException()
                  .setExceptionString(ex.getExceptionString())
                  .setMsSinceEpoch(ex.getTimestampMs());
        }
        for (org.apache.pulsar.common.policies.data.ExceptionInformation ex :
                functionInstanceStatus.getLatestSystemExceptions()) {
            status.addLatestSystemException()
                  .setExceptionString(ex.getExceptionString())
                  .setMsSinceEpoch(ex.getTimestampMs());
        }
    }

}
