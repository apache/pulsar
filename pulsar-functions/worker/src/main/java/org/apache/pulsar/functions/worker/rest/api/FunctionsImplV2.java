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

import com.google.gson.Gson;
import com.google.protobuf.util.JsonFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.utils.ComponentType;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.utils.SinkConfigUtils;
import org.apache.pulsar.functions.utils.SourceConfigUtils;
import org.apache.pulsar.functions.utils.ValidatorUtils;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.apache.pulsar.functions.worker.rest.RestException;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.join;
import static org.apache.pulsar.functions.utils.ComponentType.FUNCTION;
import static org.apache.pulsar.functions.utils.ComponentType.SOURCE;
import static org.apache.pulsar.functions.utils.FunctionCommon.extractClassLoader;
import static org.apache.pulsar.functions.utils.FunctionCommon.loadJar;
import static org.apache.pulsar.functions.worker.WorkerUtils.dumpToTmpFile;
import static org.apache.pulsar.functions.worker.WorkerUtils.isFunctionCodeBuiltin;
import static org.apache.pulsar.functions.worker.rest.api.ComponentImpl.throwUnavailableException;

@Slf4j
public class FunctionsImplV2 {

    private FunctionsImpl delegate;
    public FunctionsImplV2(Supplier<WorkerService> workerServiceSupplier) {
        this.delegate = new FunctionsImpl(workerServiceSupplier);
    }

    // For test purposes
    public FunctionsImplV2(FunctionsImpl delegate) {
        this.delegate = delegate;
    }

    public Response getFunctionInfo(final String tenant, final String namespace, final String functionName)
            throws IOException {

        // run just for parameter checks
        delegate.getFunctionInfo(tenant, namespace, functionName, null, null);

        FunctionMetaDataManager functionMetaDataManager = delegate.worker().getFunctionMetaDataManager();

        Function.FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace,
                functionName);
        String functionDetailsJson = FunctionCommon.printJson(functionMetaData.getFunctionDetails());
        return Response.status(Response.Status.OK).entity(functionDetailsJson).build();
    }

    public Response getFunctionInstanceStatus(final String tenant, final String namespace, final String functionName,
                                              final String instanceId, URI uri) throws IOException {

        org.apache.pulsar.common.policies.data.FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData
                functionInstanceStatus = delegate.getFunctionInstanceStatus(tenant, namespace, functionName, instanceId, uri, null, null);

        String jsonResponse = FunctionCommon.printJson(toProto(functionInstanceStatus, instanceId));
        return Response.status(Response.Status.OK).entity(jsonResponse).build();
    }

    public Response getFunctionStatusV2(String tenant, String namespace, String functionName, URI requestUri) throws
            IOException {
        FunctionStatus functionStatus = delegate.getFunctionStatus(tenant, namespace, functionName, requestUri, null, null);
        InstanceCommunication.FunctionStatusList.Builder functionStatusList = InstanceCommunication.FunctionStatusList.newBuilder();
        functionStatus.instances.forEach(functionInstanceStatus -> functionStatusList.addFunctionStatusList(
                toProto(functionInstanceStatus.getStatus(),
                        String.valueOf(functionInstanceStatus.getInstanceId()))));
        String jsonResponse = FunctionCommon.printJson(functionStatusList);
        return Response.status(Response.Status.OK).entity(jsonResponse).build();
    }

    public Response registerFunction(String tenant, String namespace, String functionName, InputStream
            uploadedInputStream, FormDataContentDisposition fileDetail, String functionPkgUrl, String
                                             functionDetailsJson, String clientRole) {
        if (!delegate.isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (tenant == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Tenant is not provided");
        }
        if (namespace == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Namespace is not provided");
        }
        if (functionName == null) {
            throw new RestException(Response.Status.BAD_REQUEST, ComponentType.FUNCTION + " Name is not provided");
        }

        try {
            if (!delegate.isAuthorizedRole(tenant, namespace, clientRole, null)) {
                log.error("{}/{}/{} Client [{}] is not admin and authorized to register {}", tenant, namespace,
                        functionName, clientRole, ComponentType.FUNCTION);
                throw new RestException(Response.Status.UNAUTHORIZED, "client is not authorize to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, functionName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        try {
            // Check tenant exists
            final TenantInfo tenantInfo = delegate.worker().getBrokerAdmin().tenants().getTenantInfo(tenant);

            String qualifiedNamespace = tenant + "/" + namespace;
            if (!delegate.worker().getBrokerAdmin().namespaces().getNamespaces(tenant).contains(qualifiedNamespace)) {
                log.error("{}/{}/{} Namespace {} does not exist", tenant, namespace,
                        functionName, namespace);
                throw new RestException(Response.Status.BAD_REQUEST, "Namespace does not exist");
            }
        } catch (PulsarAdminException.NotAuthorizedException e) {
            log.error("{}/{}/{} Client [{}] is not admin and authorized to operate {} on tenant", tenant, namespace,
                    functionName, clientRole, ComponentType.FUNCTION);
            throw new RestException(Response.Status.UNAUTHORIZED, "client is not authorize to perform operation");
        } catch (PulsarAdminException.NotFoundException e) {
            log.error("{}/{}/{} Tenant {} does not exist", tenant, namespace, functionName, tenant);
            throw new RestException(Response.Status.BAD_REQUEST, "Tenant does not exist");
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Issues getting tenant data", tenant, namespace, functionName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = delegate.worker().getFunctionMetaDataManager();

        if (functionMetaDataManager.containsFunction(tenant, namespace, functionName)) {
            log.error("{} {}/{}/{} already exists", ComponentType.FUNCTION, tenant, namespace, functionName);
            throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s already exists", ComponentType.FUNCTION, functionName));
        }

        Function.FunctionDetails functionDetails;
        boolean isPkgUrlProvided = isNotBlank(functionPkgUrl);
        File functionPackageFile = null;
        try {

            // validate parameters
            try {
                if (isPkgUrlProvided) {

                    if (!Utils.isFunctionPackageUrlSupported(functionPkgUrl)) {
                        throw new IllegalArgumentException("Function Package url is not valid. supported url (http/https/file)");
                    }

                    try {
                        functionPackageFile = FunctionCommon.extractFileFromPkgURL(functionPkgUrl);
                    } catch (Exception e) {
                        throw new IllegalArgumentException(String.format("Encountered error \"%s\" when getting %s package from %s", e.getMessage(), ComponentType.FUNCTION, functionPkgUrl));
                    }                    functionDetails = validateUpdateRequestParams(tenant, namespace, functionName,
                            functionDetailsJson, ComponentType.FUNCTION, functionPkgUrl, functionPackageFile);
                } else {
                    if (uploadedInputStream != null) {
                        functionPackageFile = dumpToTmpFile(uploadedInputStream);
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, functionName,
                            functionDetailsJson, ComponentType.FUNCTION, functionPkgUrl, functionPackageFile);
                    if (!isFunctionCodeBuiltin(functionDetails) && (functionPackageFile == null || fileDetail == null)) {
                        throw new IllegalArgumentException(ComponentType.FUNCTION + " Package is not provided");
                    }
                }
            } catch (Exception e) {
                log.error("Invalid register {} request @ /{}/{}/{}", ComponentType.FUNCTION, tenant, namespace, functionName, e);
                throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
            }

            try {
                delegate.worker().getFunctionRuntimeManager().getRuntimeFactory().doAdmissionChecks(functionDetails);
            } catch (Exception e) {
                log.error("{} {}/{}/{} cannot be admitted by the runtime factory", ComponentType.FUNCTION, tenant, namespace, functionName);
                throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s cannot be admitted:- %s", ComponentType.FUNCTION, functionName, e.getMessage()));
            }

            // function state
            Function.FunctionMetaData.Builder functionMetaDataBuilder = Function.FunctionMetaData.newBuilder()
                    .setFunctionDetails(functionDetails)
                    .setCreateTime(System.currentTimeMillis())
                    .setVersion(0);

            Function.PackageLocationMetaData.Builder packageLocationMetaDataBuilder;
            try {
                packageLocationMetaDataBuilder = delegate.getFunctionPackageLocation(functionDetails,
                        functionPkgUrl, fileDetail, functionPackageFile);
            } catch (Exception e) {
                throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
            }

            functionMetaDataBuilder.setPackageLocation(packageLocationMetaDataBuilder);
            delegate.updateRequest(functionMetaDataBuilder.build());
        } finally {
            if (!(functionPkgUrl != null && functionPkgUrl.startsWith(Utils.FILE))
                    && functionPackageFile != null && functionPackageFile.exists()) {
                functionPackageFile.delete();
            }
        }
        return Response.ok().build();
    }

    public Response updateFunction(String tenant, String namespace, String functionName, InputStream uploadedInputStream,
                                   FormDataContentDisposition fileDetail, String functionPkgUrl, String
                                           functionDetailsJson, String clientRole) {
        if (!delegate.isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (tenant == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Tenant is not provided");
        }
        if (namespace == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Namespace is not provided");
        }
        if (functionName == null) {
            throw new RestException(Response.Status.BAD_REQUEST, ComponentType.FUNCTION + " Name is not provided");
        }

        try {
            if (!delegate.isAuthorizedRole(tenant, namespace, clientRole, null)) {
                log.error("{}/{}/{} Client [{}] is not admin and authorized to update {}", tenant, namespace,
                        functionName, clientRole, ComponentType.FUNCTION);
                throw new RestException(Response.Status.UNAUTHORIZED, ComponentType.FUNCTION + "client is not authorize to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, functionName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = delegate.worker().getFunctionMetaDataManager();

        if (!functionMetaDataManager.containsFunction(tenant, namespace, functionName)) {
            throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s doesn't exist", ComponentType.FUNCTION, functionName));
        }

        String mergedComponentConfigJson;
        String existingComponentConfigJson;

        Function.FunctionMetaData existingComponent = functionMetaDataManager.getFunctionMetaData(tenant, namespace, functionName);

        FunctionConfig existingFunctionConfig = FunctionConfigUtils.convertFromDetails(existingComponent.getFunctionDetails());
        existingComponentConfigJson = new Gson().toJson(existingFunctionConfig);

        Function.FunctionDetails.Builder functionDetailsBuilder = Function.FunctionDetails.newBuilder();
        try {
            FunctionCommon.mergeJson(functionDetailsJson, functionDetailsBuilder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        FunctionConfig functionConfig = FunctionConfigUtils.convertFromDetails(functionDetailsBuilder.build());
        // The rest end points take precedence over whatever is there in functionconfig
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(functionName);
        String mergedFunctionDetailsJson;
        try {
            FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(existingFunctionConfig, functionConfig);
            mergedComponentConfigJson = new Gson().toJson(mergedConfig);
            mergedFunctionDetailsJson = JsonFormat.printer().print(FunctionConfigUtils.convert(mergedConfig, Thread.currentThread().getContextClassLoader()));
        } catch (Exception e) {
            throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
        }

        if (existingComponentConfigJson.equals(mergedComponentConfigJson) && isBlank(functionPkgUrl) && uploadedInputStream == null) {
            log.error("{}/{}/{} Update contains no changes", tenant, namespace, functionName);
            throw new RestException(Response.Status.BAD_REQUEST, "Update contains no change");
        }

        Function.FunctionDetails functionDetails;
        File functionPackageFile = null;
        try {

            // validate parameters
            try {
                if (isNotBlank(functionPkgUrl)) {
                    try {
                        functionPackageFile = FunctionCommon.extractFileFromPkgURL(functionPkgUrl);
                    } catch (Exception e) {
                        throw new IllegalArgumentException(String.format("Encountered error \"%s\" when getting %s package from %s", e.getMessage(), ComponentType.FUNCTION, functionPkgUrl));
                    }                    functionDetails = validateUpdateRequestParams(tenant, namespace, functionName,
                            mergedFunctionDetailsJson, ComponentType.FUNCTION, functionPkgUrl, functionPackageFile);
                } else if (uploadedInputStream != null) {

                    functionPackageFile = WorkerUtils.dumpToTmpFile(uploadedInputStream);
                    functionDetails = validateUpdateRequestParams(tenant, namespace, functionName,
                            mergedFunctionDetailsJson, ComponentType.FUNCTION, functionPkgUrl, functionPackageFile);

                } else if (existingComponent.getPackageLocation().getPackagePath().startsWith("builtin://")) {
                    functionDetails = validateUpdateRequestParams(tenant, namespace, functionName,
                            mergedFunctionDetailsJson, ComponentType.FUNCTION, functionPkgUrl, functionPackageFile);
                    if (!isFunctionCodeBuiltin(functionDetails) && (functionPackageFile == null || fileDetail == null)) {
                        throw new IllegalArgumentException(ComponentType.FUNCTION + " Package is not provided");
                    }
                } else {
                    functionPackageFile = File.createTempFile("functions", null);
                    functionPackageFile.deleteOnExit();
                    WorkerUtils.downloadFromBookkeeper(delegate.worker().getDlogNamespace(), functionPackageFile, existingComponent.getPackageLocation().getPackagePath());
                    functionDetails = validateUpdateRequestParams(tenant, namespace, functionName,
                            mergedFunctionDetailsJson, ComponentType.FUNCTION.FUNCTION, functionPkgUrl, functionPackageFile);
                }
            } catch (Exception e) {
                log.error("Invalid update {} request @ /{}/{}/{}", ComponentType.FUNCTION, tenant, namespace, functionName, e);
                throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
            }

            try {
                delegate.worker().getFunctionRuntimeManager().getRuntimeFactory().doAdmissionChecks(functionDetails);
            } catch (Exception e) {
                log.error("Updated {} {}/{}/{} cannot be submitted to runtime factory", ComponentType.FUNCTION, tenant, namespace, functionName);
                throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s cannot be admitted:- %s", ComponentType.FUNCTION, functionName, e.getMessage()));
            }

            // merge from existing metadata
            Function.FunctionMetaData.Builder functionMetaDataBuilder = Function.FunctionMetaData.newBuilder().mergeFrom(existingComponent)
                    .setFunctionDetails(functionDetails);

            Function.PackageLocationMetaData.Builder packageLocationMetaDataBuilder;
            if (isNotBlank(functionPkgUrl) || functionPackageFile != null) {
                try {
                    packageLocationMetaDataBuilder = delegate.getFunctionPackageLocation(functionDetails,
                            functionPkgUrl, fileDetail, functionPackageFile);
                } catch (Exception e) {
                    throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
                }
            } else {
                packageLocationMetaDataBuilder = Function.PackageLocationMetaData.newBuilder().mergeFrom(existingComponent.getPackageLocation());
            }

            functionMetaDataBuilder.setPackageLocation(packageLocationMetaDataBuilder);

            delegate.updateRequest(functionMetaDataBuilder.build());
        } finally {
            if (!(functionPkgUrl != null && functionPkgUrl.startsWith(Utils.FILE))
                    && functionPackageFile != null && functionPackageFile.exists()) {
                functionPackageFile.delete();
            }
        }
        return Response.ok().build();
    }

    public Response deregisterFunction(String tenant, String namespace, String functionName, String clientAppId) {
        delegate.deregisterFunction(tenant, namespace, functionName, clientAppId, null);
        return Response.ok().build();
    }

    public Response listFunctions(String tenant, String namespace) {
        Collection<String> functionStateList = delegate.listFunctions( tenant, namespace, null, null);
        return Response.status(Response.Status.OK).entity(new Gson().toJson(functionStateList.toArray())).build();
    }

    public Response triggerFunction(String tenant, String namespace, String functionName, String triggerValue,
                                    InputStream triggerStream, String topic) {
        String result = delegate.triggerFunction(tenant, namespace, functionName, triggerValue, triggerStream, topic, null, null);
        return Response.status(Response.Status.OK).entity(result).build();
    }

    public Response getFunctionState(String tenant, String namespace, String functionName, String key) {
        FunctionState functionState = delegate.getFunctionState(
                tenant, namespace, functionName, key, null, null);

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

    public Response restartFunctionInstance(String tenant, String namespace, String functionName, String instanceId, URI
            uri) {
        delegate.restartFunctionInstance(tenant, namespace, functionName, instanceId, uri, null, null);
        return Response.ok().build();
    }

    public Response restartFunctionInstances(String tenant, String namespace, String functionName) {
        delegate.restartFunctionInstances(tenant, namespace, functionName, null, null);
        return Response.ok().build();
    }

    public Response stopFunctionInstance(String tenant, String namespace, String functionName, String instanceId, URI
            uri) {
        delegate.stopFunctionInstance(tenant, namespace, functionName, instanceId, uri, null ,null);
        return Response.ok().build();
    }

    public Response stopFunctionInstances(String tenant, String namespace, String functionName) {
        delegate.stopFunctionInstances(tenant, namespace, functionName, null, null);
        return Response.ok().build();
    }

    public Response uploadFunction(InputStream uploadedInputStream, String path) {
        delegate.uploadFunction(uploadedInputStream, path);
        return Response.ok().build();
    }

    public Response downloadFunction(String path) {
        return Response.status(Response.Status.OK).entity(delegate.downloadFunction(path)).build();
    }

    public List<ConnectorDefinition> getListOfConnectors() {
        return delegate.getListOfConnectors();
    }

    private InstanceCommunication.FunctionStatus toProto(
            org.apache.pulsar.common.policies.data.FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData
                    functionInstanceStatus, String instanceId) {
        List<InstanceCommunication.FunctionStatus.ExceptionInformation> latestSysExceptions
                = functionInstanceStatus.getLatestSystemExceptions()
                .stream()
                .map(exceptionInformation -> InstanceCommunication.FunctionStatus.ExceptionInformation.newBuilder()
                        .setExceptionString(exceptionInformation.getExceptionString())
                        .setMsSinceEpoch(exceptionInformation.getTimestampMs())
                        .build())
                .collect(Collectors.toList());

        List<InstanceCommunication.FunctionStatus.ExceptionInformation> latestUserExceptions
                = functionInstanceStatus.getLatestUserExceptions()
                .stream()
                .map(exceptionInformation -> InstanceCommunication.FunctionStatus.ExceptionInformation.newBuilder()
                        .setExceptionString(exceptionInformation.getExceptionString())
                        .setMsSinceEpoch(exceptionInformation.getTimestampMs())
                        .build())
                .collect(Collectors.toList());


        InstanceCommunication.FunctionStatus functionStatus = InstanceCommunication.FunctionStatus.newBuilder()
                .setRunning(functionInstanceStatus.isRunning())
                .setFailureException(functionInstanceStatus.getError())
                .setNumRestarts(functionInstanceStatus.getNumRestarts())
                .setNumSuccessfullyProcessed(functionInstanceStatus.getNumSuccessfullyProcessed())
                .setNumUserExceptions(functionInstanceStatus.getNumUserExceptions())
                .addAllLatestUserExceptions(latestUserExceptions)
                .setNumSystemExceptions(functionInstanceStatus.getNumSystemExceptions())
                .addAllLatestSystemExceptions(latestSysExceptions)
                .setAverageLatency(functionInstanceStatus.getAverageLatency())
                .setLastInvocationTime(functionInstanceStatus.getLastInvocationTime())
                .setInstanceId(instanceId)
                .setWorkerId(delegate.worker().getWorkerConfig().getWorkerId())
                .build();

        return functionStatus;
    }

    private Function.FunctionDetails validateUpdateRequestParams(final String tenant,
                                                                 final String namespace,
                                                                 final String componentName,
                                                                 final String functionDetailsJson,
                                                                 final ComponentType componentType,
                                                                 final String functionPkgUrl,
                                                                 final File componentPackageFile) throws IOException {
        if (tenant == null) {
            throw new IllegalArgumentException("Tenant is not provided");
        }
        if (namespace == null) {
            throw new IllegalArgumentException("Namespace is not provided");
        }
        if (componentName == null) {
            throw new IllegalArgumentException(String.format("%s Name is not provided", componentType));
        }

        Function.FunctionDetails.Builder functionDetailsBuilder = Function.FunctionDetails.newBuilder();
        FunctionCommon.mergeJson(functionDetailsJson, functionDetailsBuilder);
        if (isNotBlank(functionPkgUrl)) {
            // set package-url if present
            functionDetailsBuilder.setPackageUrl(functionPkgUrl);
        }
        ClassLoader clsLoader = null;
        if (functionDetailsBuilder.getRuntime() == Function.FunctionDetails.Runtime.JAVA) {
            try {
                clsLoader = loadJar(componentPackageFile);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to load JAR file", e);
            }
        }
        ValidatorUtils.validateFunctionClassTypes(clsLoader, functionDetailsBuilder);

        Function.FunctionDetails functionDetails = functionDetailsBuilder.build();

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
            String errorMessage = join(missingFields, ",");
            throw new IllegalArgumentException(errorMessage + " is not provided");
        }
        if (functionDetails.getParallelism() <= 0) {
            throw new IllegalArgumentException("Parallelism needs to be set to a positive number");
        }

        FunctionConfigUtils.validate(FunctionConfigUtils.convertFromDetails(functionDetails), componentPackageFile);
        return functionDetails;
    }
}