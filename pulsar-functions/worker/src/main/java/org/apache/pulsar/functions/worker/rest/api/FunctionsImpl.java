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

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.functions.auth.FunctionAuthUtils.getFunctionAuthData;
import static org.apache.pulsar.functions.utils.FunctionCommon.isFunctionCodeBuiltin;
import static org.apache.pulsar.functions.worker.rest.RestUtils.throwUnavailableException;

import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.policies.data.ExceptionInformation;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.auth.FunctionAuthData;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.utils.ComponentTypeUtils;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.PulsarWorkerService;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.apache.pulsar.functions.worker.service.api.Functions;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

@Slf4j
public class FunctionsImpl extends ComponentImpl implements Functions<PulsarWorkerService> {

    public FunctionsImpl(Supplier<PulsarWorkerService> workerServiceSupplier) {
        super(workerServiceSupplier, Function.FunctionDetails.ComponentType.FUNCTION);
    }

    @Override
    public void registerFunction(final String tenant,
                                 final String namespace,
                                 final String functionName,
                                 final InputStream uploadedInputStream,
                                 final FormDataContentDisposition fileDetail,
                                 final String functionPkgUrl,
                                 final FunctionConfig functionConfig,
                                 final String clientRole,
                                 AuthenticationDataHttps clientAuthenticationDataHttps) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (tenant == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Tenant is not provided");
        }
        if (namespace == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Namespace is not provided");
        }
        if (functionName == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Function name is not provided");
        }
        if (functionConfig == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Function config is not provided");
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.error("{}/{}/{} Client [{}] is not authorized to register {}", tenant, namespace,
                        functionName, clientRole, ComponentTypeUtils.toString(componentType));
                throw new RestException(Response.Status.UNAUTHORIZED, "Client is not authorized to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, functionName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        try {
            // Check tenant exists
            worker().getBrokerAdmin().tenants().getTenantInfo(tenant);

            String qualifiedNamespace = tenant + "/" + namespace;
            List<String> namespaces = worker().getBrokerAdmin().namespaces().getNamespaces(tenant);
            if (namespaces != null && !namespaces.contains(qualifiedNamespace)) {
                String qualifiedNamespaceWithCluster = String.format("%s/%s/%s", tenant,
                        worker().getWorkerConfig().getPulsarFunctionsCluster(), namespace);
                if (namespaces != null && !namespaces.contains(qualifiedNamespaceWithCluster)) {
                    log.error("{}/{}/{} Namespace {} does not exist", tenant, namespace, functionName, namespace);
                    throw new RestException(Response.Status.BAD_REQUEST, "Namespace does not exist");
                }
            }
        } catch (PulsarAdminException.NotAuthorizedException e) {
            log.error("{}/{}/{} Client [{}] is not authorized to operate {} on tenant", tenant, namespace,
                    functionName, clientRole, ComponentTypeUtils.toString(componentType));
            throw new RestException(Response.Status.UNAUTHORIZED, "Client is not authorized to perform operation");
        } catch (PulsarAdminException.NotFoundException e) {
            log.error("{}/{}/{} Tenant {} does not exist", tenant, namespace, functionName, tenant);
            throw new RestException(Response.Status.BAD_REQUEST, "Tenant does not exist");
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Issues getting tenant data", tenant, namespace, functionName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();

        if (functionMetaDataManager.containsFunction(tenant, namespace, functionName)) {
            log.error("{} {}/{}/{} already exists", ComponentTypeUtils.toString(componentType), tenant, namespace, functionName);
            throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s already exists", ComponentTypeUtils.toString(componentType), functionName));
        }

        Function.FunctionDetails functionDetails = null;
        boolean isPkgUrlProvided = isNotBlank(functionPkgUrl);
        File componentPackageFile = null;
        try {

            // validate parameters
            try {
                if (isPkgUrlProvided) {
                    if (Utils.hasPackageTypePrefix(functionPkgUrl)) {
                        componentPackageFile = downloadPackageFile(functionPkgUrl);
                    } else {
                        if (!Utils.isFunctionPackageUrlSupported(functionPkgUrl)) {
                            throw new IllegalArgumentException("Function Package url is not valid. supported url (http/https/file)");
                        }
                        try {

                            componentPackageFile = FunctionCommon.extractFileFromPkgURL(functionPkgUrl);
                        } catch (Exception e) {
                            throw new IllegalArgumentException(String.format("Encountered error \"%s\" when getting %s package from %s", e.getMessage(), ComponentTypeUtils.toString(componentType), functionPkgUrl), e);
                        }
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, functionName,
                            functionConfig, componentPackageFile);
                } else {
                    if (uploadedInputStream != null) {
                        componentPackageFile = WorkerUtils.dumpToTmpFile(uploadedInputStream);
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, functionName,
                            functionConfig, componentPackageFile);
                    if (!isFunctionCodeBuiltin(functionDetails) && (componentPackageFile == null || fileDetail == null)) {
                        throw new IllegalArgumentException(ComponentTypeUtils.toString(componentType) + " Package is not provided");
                    }
                }
            } catch (Exception e) {
                log.error("Invalid register {} request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, functionName, e);
                throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
            }

            try {
                worker().getFunctionRuntimeManager().getRuntimeFactory().doAdmissionChecks(functionDetails);
            } catch (Exception e) {
                log.error("{} {}/{}/{} cannot be admitted by the runtime factory", ComponentTypeUtils.toString(componentType), tenant, namespace, functionName);
                throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s cannot be admitted:- %s", ComponentTypeUtils.toString(componentType), functionName, e.getMessage()));
            }

            // function state
            Function.FunctionMetaData.Builder functionMetaDataBuilder = Function.FunctionMetaData.newBuilder()
                    .setFunctionDetails(functionDetails)
                    .setCreateTime(System.currentTimeMillis())
                    .setVersion(0);

            // cache auth if need
            if (worker().getWorkerConfig().isAuthenticationEnabled()) {
                Function.FunctionDetails finalFunctionDetails = functionDetails;
                worker().getFunctionRuntimeManager()
                        .getRuntimeFactory()
                        .getAuthProvider().ifPresent(functionAuthProvider -> {
                    if (clientAuthenticationDataHttps != null) {

                        try {
                            Optional<FunctionAuthData> functionAuthData = functionAuthProvider
                                    .cacheAuthData(finalFunctionDetails, clientAuthenticationDataHttps);

                            functionAuthData.ifPresent(authData -> functionMetaDataBuilder.setFunctionAuthSpec(
                                    Function.FunctionAuthenticationSpec.newBuilder()
                                            .setData(ByteString.copyFrom(authData.getData()))
                                            .build()));
                        } catch (Exception e) {
                            log.error("Error caching authentication data for {} {}/{}/{}",
                                    ComponentTypeUtils.toString(componentType), tenant, namespace, functionName, e);


                            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, String.format("Error caching authentication data for %s %s:- %s",
                                    ComponentTypeUtils.toString(componentType), functionName, e.getMessage()));
                        }
                    }
                });
            }

            Function.PackageLocationMetaData.Builder packageLocationMetaDataBuilder;
            try {
                packageLocationMetaDataBuilder = getFunctionPackageLocation(functionMetaDataBuilder.build(),
                        functionPkgUrl, fileDetail, componentPackageFile);
            } catch (Exception e) {
                log.error("Failed process {} {}/{}/{} package: ", ComponentTypeUtils.toString(componentType), tenant, namespace, functionName, e);
                throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
            }

            functionMetaDataBuilder.setPackageLocation(packageLocationMetaDataBuilder);
            updateRequest(null, functionMetaDataBuilder.build());
        } finally {
            if (componentPackageFile != null && componentPackageFile.exists()) {
                if (functionPkgUrl == null || !functionPkgUrl.startsWith(Utils.FILE)) {
                    componentPackageFile.delete();
                }
            }
        }
    }

    @Override
    public void updateFunction(final String tenant,
                               final String namespace,
                               final String functionName,
                               final InputStream uploadedInputStream,
                               final FormDataContentDisposition fileDetail,
                               final String functionPkgUrl,
                               final FunctionConfig functionConfig,
                               final String clientRole,
                               AuthenticationDataHttps clientAuthenticationDataHttps,
                               UpdateOptionsImpl updateOptions) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (tenant == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Tenant is not provided");
        }
        if (namespace == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Namespace is not provided");
        }
        if (functionName == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Function name is not provided");
        }
        if (functionConfig == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Function config is not provided");
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.error("{}/{}/{} Client [{}] is not authorized to update {}", tenant, namespace,
                        functionName, clientRole, ComponentTypeUtils.toString(componentType));
                throw new RestException(Response.Status.UNAUTHORIZED, "Client is not authorized to perform operation");

            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, functionName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();

        if (!functionMetaDataManager.containsFunction(tenant, namespace, functionName)) {
            throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), functionName));
        }

        Function.FunctionMetaData existingComponent = functionMetaDataManager.getFunctionMetaData(tenant, namespace, functionName);

        if (!InstanceUtils.calculateSubjectType(existingComponent.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, functionName, ComponentTypeUtils.toString(componentType));
            throw new RestException(Response.Status.NOT_FOUND, String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), functionName));
        }

        FunctionConfig existingFunctionConfig = FunctionConfigUtils.convertFromDetails(existingComponent.getFunctionDetails());
        // The rest end points take precedence over whatever is there in function config
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(functionName);
        FunctionConfig mergedConfig;
        try {
            mergedConfig = FunctionConfigUtils.validateUpdate(existingFunctionConfig, functionConfig);
        } catch (Exception e) {
            throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
        }

        if (existingFunctionConfig.equals(mergedConfig) && isBlank(functionPkgUrl) && uploadedInputStream == null) {
            log.error("{}/{}/{} Update contains no changes", tenant, namespace, functionName);
            throw new RestException(Response.Status.BAD_REQUEST, "Update contains no change");
        }

        Function.FunctionDetails functionDetails = null;
        File componentPackageFile = null;
        try {

            // validate parameters
            try {
                if (isNotBlank(functionPkgUrl)) {
                    if (Utils.hasPackageTypePrefix(functionPkgUrl)) {
                        componentPackageFile = downloadPackageFile(functionName);
                    } else {
                        try {
                            componentPackageFile = FunctionCommon.extractFileFromPkgURL(functionPkgUrl);
                        } catch (Exception e) {
                            throw new IllegalArgumentException(String.format("Encountered error \"%s\" when getting %s package from %s", e.getMessage(), ComponentTypeUtils.toString(componentType), functionPkgUrl));
                        }
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, functionName,
                            mergedConfig, componentPackageFile);

                } else if (existingComponent.getPackageLocation().getPackagePath().startsWith(Utils.FILE)
                        || existingComponent.getPackageLocation().getPackagePath().startsWith(Utils.HTTP)) {
                    try {
                        componentPackageFile = FunctionCommon.extractFileFromPkgURL(existingComponent.getPackageLocation().getPackagePath());
                    } catch (Exception e) {
                        throw new IllegalArgumentException(String.format("Encountered error \"%s\" when getting %s package from %s", e.getMessage(), ComponentTypeUtils.toString(componentType), functionPkgUrl));
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, functionName,
                            mergedConfig, componentPackageFile);
                } else if (uploadedInputStream != null) {

                    componentPackageFile = WorkerUtils.dumpToTmpFile(uploadedInputStream);
                    functionDetails = validateUpdateRequestParams(tenant, namespace, functionName,
                            mergedConfig, componentPackageFile);

                } else if (existingComponent.getPackageLocation().getPackagePath().startsWith(Utils.BUILTIN)) {
                    functionDetails = validateUpdateRequestParams(tenant, namespace, functionName,
                            mergedConfig, componentPackageFile);
                    if (!isFunctionCodeBuiltin(functionDetails) && (componentPackageFile == null || fileDetail == null)) {
                        throw new IllegalArgumentException(ComponentTypeUtils.toString(componentType) + " Package is not provided");
                    }
                } else {

                    componentPackageFile = FunctionCommon.createPkgTempFile();
                    componentPackageFile.deleteOnExit();
                    WorkerUtils.downloadFromBookkeeper(worker().getDlogNamespace(), componentPackageFile, existingComponent.getPackageLocation().getPackagePath());

                    functionDetails = validateUpdateRequestParams(tenant, namespace, functionName,
                            mergedConfig, componentPackageFile);
                }
            } catch (Exception e) {
                log.error("Invalid update {} request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, functionName, e);
                throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
            }

            try {
                worker().getFunctionRuntimeManager().getRuntimeFactory().doAdmissionChecks(functionDetails);
            } catch (Exception e) {
                log.error("Updated {} {}/{}/{} cannot be submitted to runtime factory", ComponentTypeUtils.toString(componentType), tenant, namespace, functionName);
                throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s cannot be admitted:- %s",
                        ComponentTypeUtils.toString(componentType), functionName, e.getMessage()));
            }

            // merge from existing metadata
            Function.FunctionMetaData.Builder functionMetaDataBuilder = Function.FunctionMetaData.newBuilder().mergeFrom(existingComponent)
                    .setFunctionDetails(functionDetails);

            // update auth data if need
            if (worker().getWorkerConfig().isAuthenticationEnabled()) {
                Function.FunctionDetails finalFunctionDetails = functionDetails;
                worker().getFunctionRuntimeManager()
                        .getRuntimeFactory()
                        .getAuthProvider().ifPresent(functionAuthProvider -> {
                            if (clientAuthenticationDataHttps != null && updateOptions != null && updateOptions.isUpdateAuthData()) {
                                // get existing auth data if it exists
                                Optional<FunctionAuthData> existingFunctionAuthData = Optional.empty();
                                if (functionMetaDataBuilder.hasFunctionAuthSpec()) {
                                    existingFunctionAuthData = Optional.ofNullable(getFunctionAuthData(Optional.ofNullable(functionMetaDataBuilder.getFunctionAuthSpec())));
                                }

                                try {
                                    Optional<FunctionAuthData> newFunctionAuthData = functionAuthProvider
                                            .updateAuthData(finalFunctionDetails, existingFunctionAuthData,
                                                    clientAuthenticationDataHttps);

                                    if (newFunctionAuthData.isPresent()) {
                                        functionMetaDataBuilder.setFunctionAuthSpec(
                                                Function.FunctionAuthenticationSpec.newBuilder()
                                                        .setData(ByteString.copyFrom(newFunctionAuthData.get().getData()))
                                                        .build());
                                    } else {
                                        functionMetaDataBuilder.clearFunctionAuthSpec();
                                    }
                                } catch (Exception e) {
                                    log.error("Error updating authentication data for {} {}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, functionName, e);
                                    throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, String.format("Error caching authentication data for %s %s:- %s", ComponentTypeUtils.toString(componentType), functionName, e.getMessage()));
                                }
                            }
                        });
            }

            Function.PackageLocationMetaData.Builder packageLocationMetaDataBuilder;
            if (isNotBlank(functionPkgUrl) || uploadedInputStream != null) {
                try {
                    packageLocationMetaDataBuilder = getFunctionPackageLocation(functionMetaDataBuilder.build(),
                            functionPkgUrl, fileDetail, componentPackageFile);
                } catch (Exception e) {
                    log.error("Failed process {} {}/{}/{} package: ", ComponentTypeUtils.toString(componentType), tenant, namespace, functionName, e);
                    throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
                }
            } else {
                packageLocationMetaDataBuilder = Function.PackageLocationMetaData.newBuilder().mergeFrom(existingComponent.getPackageLocation());
            }

            functionMetaDataBuilder.setPackageLocation(packageLocationMetaDataBuilder);

            updateRequest(existingComponent, functionMetaDataBuilder.build());
        } finally {
            if (componentPackageFile != null && componentPackageFile.exists()) {
                if ((functionPkgUrl != null && !functionPkgUrl.startsWith(Utils.FILE)) || uploadedInputStream != null) {
                    componentPackageFile.delete();
                }
            }
        }
    }

    private class GetFunctionStatus extends GetStatus<FunctionStatus, FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData> {

        @Override
        public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData notScheduledInstance() {
            FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData
                    = new FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData();
            functionInstanceStatusData.setRunning(false);
            functionInstanceStatusData.setError("Function has not been scheduled");
            return functionInstanceStatusData;
        }

        @Override
        public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData fromFunctionStatusProto(
                InstanceCommunication.FunctionStatus status,
                String assignedWorkerId) {
            FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData
                    = new FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData();
            functionInstanceStatusData.setRunning(status.getRunning());
            functionInstanceStatusData.setError(status.getFailureException());
            functionInstanceStatusData.setNumRestarts(status.getNumRestarts());
            functionInstanceStatusData.setNumReceived(status.getNumReceived());
            functionInstanceStatusData.setNumSuccessfullyProcessed(status.getNumSuccessfullyProcessed());
            functionInstanceStatusData.setNumUserExceptions(status.getNumUserExceptions());

            List<ExceptionInformation> userExceptionInformationList = new LinkedList<>();
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestUserExceptionsList()) {
                ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
                userExceptionInformationList.add(exceptionInformation);
            }
            functionInstanceStatusData.setLatestUserExceptions(userExceptionInformationList);

            // For regular functions source/sink errors are system exceptions
            functionInstanceStatusData.setNumSystemExceptions(status.getNumSystemExceptions()
                    + status.getNumSourceExceptions() + status.getNumSinkExceptions());
            List<ExceptionInformation> systemExceptionInformationList = new LinkedList<>();
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSystemExceptionsList()) {
                ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
                systemExceptionInformationList.add(exceptionInformation);
            }
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSourceExceptionsList()) {
                ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
                systemExceptionInformationList.add(exceptionInformation);
            }
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSinkExceptionsList()) {
                ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
                systemExceptionInformationList.add(exceptionInformation);
            }
            functionInstanceStatusData.setLatestSystemExceptions(systemExceptionInformationList);

            functionInstanceStatusData.setAverageLatency(status.getAverageLatency());
            functionInstanceStatusData.setLastInvocationTime(status.getLastInvocationTime());
            functionInstanceStatusData.setWorkerId(assignedWorkerId);

            return functionInstanceStatusData;
        }

        @Override
        public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData notRunning(String assignedWorkerId, String error) {
            FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData
                    = new FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData();
            functionInstanceStatusData.setRunning(false);
            if (error != null) {
                functionInstanceStatusData.setError(error);
            }
            functionInstanceStatusData.setWorkerId(assignedWorkerId);

            return functionInstanceStatusData;
        }

        @Override
        public FunctionStatus getStatus(String tenant, String namespace, String name, Collection<Function.Assignment>
                assignments, URI uri) throws PulsarAdminException {
            FunctionStatus functionStatus = new FunctionStatus();
            for (Function.Assignment assignment : assignments) {
                boolean isOwner = worker().getWorkerConfig().getWorkerId().equals(assignment.getWorkerId());
                FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData;
                if (isOwner) {
                    functionInstanceStatusData = getComponentInstanceStatus(tenant, namespace, name, assignment
                            .getInstance().getInstanceId(), null);
                } else {
                    functionInstanceStatusData = worker().getFunctionAdmin().functions().getFunctionStatus(
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getTenant(),
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getNamespace(),
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getName(),
                            assignment.getInstance().getInstanceId());
                }

                FunctionStatus.FunctionInstanceStatus instanceStatus = new FunctionStatus.FunctionInstanceStatus();
                instanceStatus.setInstanceId(assignment.getInstance().getInstanceId());
                instanceStatus.setStatus(functionInstanceStatusData);
                functionStatus.addInstance(instanceStatus);
            }

            functionStatus.setNumInstances(functionStatus.instances.size());
            functionStatus.getInstances().forEach(functionInstanceStatus -> {
                if (functionInstanceStatus.getStatus().isRunning()) {
                    functionStatus.numRunning++;
                }
            });
            return functionStatus;
        }

        @Override
        public FunctionStatus getStatusExternal(final String tenant,
                                                final String namespace,
                                                final String name,
                                                final int parallelism) {
            FunctionStatus functionStatus = new FunctionStatus();
            for (int i = 0; i < parallelism; ++i) {
                FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData
                        = getComponentInstanceStatus(tenant, namespace, name, i, null);
                FunctionStatus.FunctionInstanceStatus functionInstanceStatus
                        = new FunctionStatus.FunctionInstanceStatus();
                functionInstanceStatus.setInstanceId(i);
                functionInstanceStatus.setStatus(functionInstanceStatusData);
                functionStatus.addInstance(functionInstanceStatus);
            }

            functionStatus.setNumInstances(functionStatus.instances.size());
            functionStatus.getInstances().forEach(functionInstanceStatus -> {
                if (functionInstanceStatus.getStatus().isRunning()) {
                    functionStatus.numRunning++;
                }
            });
            return functionStatus;
        }

        @Override
        public FunctionStatus emptyStatus(final int parallelism) {
            FunctionStatus functionStatus = new FunctionStatus();
            functionStatus.setNumInstances(parallelism);
            functionStatus.setNumRunning(0);
            for (int i = 0; i < parallelism; i++) {
                FunctionStatus.FunctionInstanceStatus functionInstanceStatus = new FunctionStatus.FunctionInstanceStatus();
                functionInstanceStatus.setInstanceId(i);
                FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData
                        = new FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData();
                functionInstanceStatusData.setRunning(false);
                functionInstanceStatusData.setError("Function has not been scheduled");
                functionInstanceStatus.setStatus(functionInstanceStatusData);

                functionStatus.addInstance(functionInstanceStatus);
            }

            return functionStatus;
        }
    }

    private ExceptionInformation getExceptionInformation(InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry) {
        ExceptionInformation exceptionInformation
                = new ExceptionInformation();
        exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
        exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
        return exceptionInformation;
    }

    /**
     * Get status of a function instance.  If this worker is not running the function instance,
     * @param tenant the tenant the function belongs to
     * @param namespace the namespace the function belongs to
     * @param componentName the function name
     * @param instanceId the function instance id
     * @return the function status
     */
    @Override
    public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData getFunctionInstanceStatus(final String tenant,
                                                                                                      final String namespace,
                                                                                                      final String componentName,
                                                                                                      final String instanceId,
                                                                                                      final URI uri,
                                                                                                      final String clientRole,
                                                                                                      final AuthenticationDataSource clientAuthenticationDataHttps) {

        // validate parameters
        componentInstanceStatusRequestValidate(tenant, namespace, componentName, Integer.parseInt(instanceId), clientRole, clientAuthenticationDataHttps);

        FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData;
        try {
            functionInstanceStatusData = new GetFunctionStatus().getComponentInstanceStatus(tenant, namespace, componentName,
                    Integer.parseInt(instanceId), uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Status", tenant, namespace, componentName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        return functionInstanceStatusData;
    }

    /**
     * Get statuses of all function instances.
     * @param tenant the tenant the function belongs to
     * @param namespace the namespace the function belongs to
     * @param componentName the function name
     * @return a list of function statuses
     * @throws PulsarAdminException
     */
    @Override
    public FunctionStatus getFunctionStatus(final String tenant,
                                            final String namespace,
                                            final String componentName,
                                            final URI uri,
                                            final String clientRole,
                                            final AuthenticationDataSource clientAuthenticationDataHttps) {

        // validate parameters
        componentStatusRequestValidate(tenant, namespace, componentName, clientRole, clientAuthenticationDataHttps);

        FunctionStatus functionStatus;
        try {
            functionStatus = new GetFunctionStatus().getComponentStatus(tenant, namespace, componentName, uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Status", tenant, namespace, componentName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        return functionStatus;
    }

    @Override
    public void updateFunctionOnWorkerLeader(final String tenant,
                               final String namespace,
                               final String functionName,
                               final InputStream uploadedInputStream,
                               final boolean delete,
                               URI uri,
                               final String clientRole,
                               AuthenticationDataSource authenticationData) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (worker().getWorkerConfig().isAuthorizationEnabled()) {
            if (!isSuperUser(clientRole, authenticationData)) {
                log.error("{}/{}/{} Client [{}] is not superuser to update on worker leader {}", tenant, namespace,
                        functionName, clientRole, ComponentTypeUtils.toString(componentType));
                throw new RestException(Response.Status.UNAUTHORIZED, "Client is not authorized to perform operation");
            }
        }

        if (tenant == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Tenant is not provided");
        }
        if (namespace == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Namespace is not provided");
        }
        if (functionName == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Function name is not provided");
        }
        Function.FunctionMetaData functionMetaData;
        try {
            functionMetaData = Function.FunctionMetaData.parseFrom(uploadedInputStream);
        } catch (IOException e) {
            throw new RestException(Response.Status.BAD_REQUEST, "Corrupt Function MetaData");
        }

        // Redirect if we are not the leader
        if (!worker().getLeaderService().isLeader()) {
            WorkerInfo workerInfo = worker().getMembershipManager().getLeader();
            if (workerInfo.getWorkerId().equals(worker().getWorkerConfig().getWorkerId())) {
                throw new RestException(Response.Status.SERVICE_UNAVAILABLE,
                        "Leader not yet ready. Please retry again");
            }
            URI redirect = UriBuilder.fromUri(uri).host(workerInfo.getWorkerHostname()).port(workerInfo.getPort()).build();
            throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
        }

        // Its possible that we are not the leader anymore. That will be taken care of by FunctionMetaDataManager
        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        try {
            functionMetaDataManager.updateFunctionOnLeader(functionMetaData, delete);
        } catch (IllegalStateException e) {
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        } catch (IllegalArgumentException e) {
            throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
        }
    }

    private Function.FunctionDetails validateUpdateRequestParams(final String tenant,
                                                                 final String namespace,
                                                                 final String componentName,
                                                                 final FunctionConfig functionConfig,
                                                                 final File componentPackageFile) throws IOException {

        // The rest end points take precedence over whatever is there in function config
        Path archivePath = null;
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(componentName);
        FunctionConfigUtils.inferMissingArguments(
            functionConfig, worker().getWorkerConfig().isForwardSourceMessageProperty());

        if (!StringUtils.isEmpty(functionConfig.getJar())) {
            String builtinArchive = functionConfig.getJar();
            if (builtinArchive.startsWith(org.apache.pulsar.common.functions.Utils.BUILTIN)) {
                builtinArchive = builtinArchive.replaceFirst("^builtin://", "");
            }
            try {
                archivePath = this.worker().getFunctionsManager().getFunctionArchive(builtinArchive);
            } catch (Exception e) {
                throw new IllegalArgumentException(String.format("No Function archive %s found", archivePath));
            }
        }
        ClassLoader clsLoader  = null;
        if(archivePath != null){
            clsLoader = FunctionConfigUtils.validate(functionConfig, archivePath.toFile());
        }
        else{
            clsLoader = FunctionConfigUtils.validate(functionConfig, componentPackageFile);
        }
        return FunctionConfigUtils.convert(functionConfig, clsLoader);

    }

    private File downloadPackageFile(String packageName) throws IOException, PulsarAdminException {
        return downloadPackageFile(worker(), packageName);
    }

    static File downloadPackageFile(PulsarWorkerService worker, String packageName) throws IOException, PulsarAdminException {
        Path tempDirectory;
        if (worker.getWorkerConfig().getDownloadDirectory() != null) {
            tempDirectory = Paths.get(worker.getWorkerConfig().getDownloadDirectory());
        } else {
            // use the Nar extraction directory as a temporary directory for downloaded files
            tempDirectory = Paths.get(worker.getWorkerConfig().getNarExtractionDirectory());
        }
        File file = Files.createTempFile(tempDirectory, "function", ".tmp").toFile();
        worker.getBrokerAdmin().packages().download(packageName, file.toString());
        return file;
    }
}
