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
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.io.ConfigFieldDefinition;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.policies.data.ExceptionInformation;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.apache.pulsar.common.util.ClassLoaderUtils;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.auth.FunctionAuthData;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.utils.ComponentTypeUtils;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.SourceConfigUtils;
import org.apache.pulsar.functions.utils.io.Connector;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.PulsarWorkerService;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.apache.pulsar.functions.worker.service.api.Sources;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

@Slf4j
public class SourcesImpl extends ComponentImpl implements Sources<PulsarWorkerService> {

    public SourcesImpl(Supplier<PulsarWorkerService> workerServiceSupplier) {
        super(workerServiceSupplier, Function.FunctionDetails.ComponentType.SOURCE);
    }

    @Override
    public void registerSource(final String tenant,
                               final String namespace,
                               final String sourceName,
                               final InputStream uploadedInputStream,
                               final FormDataContentDisposition fileDetail,
                               final String sourcePkgUrl,
                               final SourceConfig sourceConfig,
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
        if (sourceName == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Source name is not provided");
        }
        if (sourceConfig == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Source config is not provided");
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.warn("{}/{}/{} Client [{}] is not authorized to register {}", tenant, namespace,
                        sourceName, clientRole, ComponentTypeUtils.toString(componentType));
                throw new RestException(Response.Status.UNAUTHORIZED, "Client is not authorized to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, sourceName, e);
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
                    log.error("{}/{}/{} Namespace {} does not exist", tenant, namespace, sourceName, namespace);
                    throw new RestException(Response.Status.BAD_REQUEST, "Namespace does not exist");
                }
            }
        } catch (PulsarAdminException.NotAuthorizedException e) {
            log.error("{}/{}/{} Client [{}] is not authorized to operate {} on tenant", tenant, namespace,
                    sourceName, clientRole, ComponentTypeUtils.toString(componentType));
            throw new RestException(Response.Status.UNAUTHORIZED, "Client is not authorized to perform operation");
        } catch (PulsarAdminException.NotFoundException e) {
            log.error("{}/{}/{} Tenant {} does not exist", tenant, namespace, sourceName, tenant);
            throw new RestException(Response.Status.BAD_REQUEST, "Tenant does not exist");
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Issues getting tenant data", tenant, namespace, sourceName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();

        if (functionMetaDataManager.containsFunction(tenant, namespace, sourceName)) {
            log.error("{} {}/{}/{} already exists", ComponentTypeUtils.toString(componentType), tenant, namespace, sourceName);
            throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s already exists", ComponentTypeUtils.toString(componentType), sourceName));
        }

        Function.FunctionDetails functionDetails = null;
        boolean isPkgUrlProvided = isNotBlank(sourcePkgUrl);
        File componentPackageFile = null;
        try {

            // validate parameters
            try {
                if (isPkgUrlProvided) {
                    if (Utils.hasPackageTypePrefix(sourcePkgUrl)) {
                        componentPackageFile = downloadPackageFile(sourcePkgUrl);
                    } else {
                        if (!Utils.isFunctionPackageUrlSupported(sourcePkgUrl)) {
                            throw new IllegalArgumentException("Function Package url is not valid. supported url (http/https/file)");
                        }
                        try {
                            componentPackageFile = FunctionCommon.extractFileFromPkgURL(sourcePkgUrl);
                        } catch (Exception e) {
                            throw new IllegalArgumentException(String.format("Encountered error \"%s\" when getting %s package from %s", e.getMessage(), ComponentTypeUtils.toString(componentType), sourcePkgUrl));
                        }
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, sourceName,
                            sourceConfig, componentPackageFile);
                } else {
                    if (uploadedInputStream != null) {
                        componentPackageFile = WorkerUtils.dumpToTmpFile(uploadedInputStream);
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, sourceName,
                            sourceConfig, componentPackageFile);
                    if (!isFunctionCodeBuiltin(functionDetails) && (componentPackageFile == null || fileDetail == null)) {
                        throw new IllegalArgumentException(ComponentTypeUtils.toString(componentType) + " Package is not provided");
                    }
                }
            } catch (Exception e) {
                log.error("Invalid register {} request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, sourceName, e);
                throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
            }

            try {
                worker().getFunctionRuntimeManager().getRuntimeFactory().doAdmissionChecks(functionDetails);
            } catch (Exception e) {
                log.error("{} {}/{}/{} cannot be admitted by the runtime factory", ComponentTypeUtils.toString(componentType), tenant, namespace, sourceName);
                throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s cannot be admitted:- %s", ComponentTypeUtils.toString(componentType), sourceName, e.getMessage()));
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
                                    ComponentTypeUtils.toString(componentType), tenant, namespace, sourceName, e);


                            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, String.format("Error caching authentication data for %s %s:- %s",
                                    ComponentTypeUtils.toString(componentType), sourceName, e.getMessage()));
                        }
                    }
                });
            }

            Function.PackageLocationMetaData.Builder packageLocationMetaDataBuilder;
            try {
                packageLocationMetaDataBuilder = getFunctionPackageLocation(functionMetaDataBuilder.build(),
                        sourcePkgUrl, fileDetail, componentPackageFile);
            } catch (Exception e) {
                log.error("Failed process {} {}/{}/{} package: ", ComponentTypeUtils.toString(componentType), tenant, namespace, sourceName, e);
                throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
            }

            functionMetaDataBuilder.setPackageLocation(packageLocationMetaDataBuilder);
            updateRequest(null, functionMetaDataBuilder.build());
        } finally {
            if (componentPackageFile != null && componentPackageFile.exists()) {
                if (sourcePkgUrl == null || !sourcePkgUrl.startsWith(Utils.FILE)) {
                    componentPackageFile.delete();
                }
            }
        }
    }

    @Override
    public void updateSource(final String tenant,
                               final String namespace,
                               final String sourceName,
                               final InputStream uploadedInputStream,
                               final FormDataContentDisposition fileDetail,
                               final String sourcePkgUrl,
                               final SourceConfig sourceConfig,
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
        if (sourceName == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Source name is not provided");
        }
        if (sourceConfig == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Source config is not provided");
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.warn("{}/{}/{} Client [{}] is not authorized to update {}", tenant, namespace,
                        sourceName, clientRole, ComponentTypeUtils.toString(componentType));
                throw new RestException(Response.Status.UNAUTHORIZED, "Client is not authorized to perform operation");

            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, sourceName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();

        if (!functionMetaDataManager.containsFunction(tenant, namespace, sourceName)) {
            throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), sourceName));
        }

        Function.FunctionMetaData existingComponent = functionMetaDataManager.getFunctionMetaData(tenant, namespace, sourceName);

        if (!InstanceUtils.calculateSubjectType(existingComponent.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, sourceName, ComponentTypeUtils.toString(componentType));
            throw new RestException(Response.Status.NOT_FOUND, String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), sourceName));
        }

        SourceConfig existingSourceConfig = SourceConfigUtils.convertFromDetails(existingComponent.getFunctionDetails());
        // The rest end points take precedence over whatever is there in functionconfig
        sourceConfig.setTenant(tenant);
        sourceConfig.setNamespace(namespace);
        sourceConfig.setName(sourceName);
        SourceConfig mergedConfig;
        try {
            mergedConfig = SourceConfigUtils.validateUpdate(existingSourceConfig, sourceConfig);
        } catch (Exception e) {
            throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
        }

        if (existingSourceConfig.equals(mergedConfig) && isBlank(sourcePkgUrl) && uploadedInputStream == null) {
            log.error("{}/{}/{} Update contains no changes", tenant, namespace, sourceName);
            throw new RestException(Response.Status.BAD_REQUEST, "Update contains no change");
        }

        Function.FunctionDetails functionDetails = null;
        File componentPackageFile = null;
        try {

            // validate parameters
            try {
                if (isNotBlank(sourcePkgUrl)) {
                    if (Utils.hasPackageTypePrefix(sourcePkgUrl)) {
                        componentPackageFile = downloadPackageFile(sourcePkgUrl);
                    } else {
                        try {
                            componentPackageFile = FunctionCommon.extractFileFromPkgURL(sourcePkgUrl);
                        } catch (Exception e) {
                            throw new IllegalArgumentException(String.format("Encountered error \"%s\" when getting %s package from %s", e.getMessage(), ComponentTypeUtils.toString(componentType), sourcePkgUrl));
                        }
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, sourceName,
                            mergedConfig, componentPackageFile);

                } else if (existingComponent.getPackageLocation().getPackagePath().startsWith(Utils.FILE)
                        || existingComponent.getPackageLocation().getPackagePath().startsWith(Utils.HTTP)) {
                    try {
                        componentPackageFile = FunctionCommon.extractFileFromPkgURL(existingComponent.getPackageLocation().getPackagePath());
                    } catch (Exception e) {
                        throw new IllegalArgumentException(String.format("Encountered error \"%s\" when getting %s package from %s", e.getMessage(), ComponentTypeUtils.toString(componentType), sourcePkgUrl));
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, sourceName,
                            mergedConfig, componentPackageFile);
                } else if (uploadedInputStream != null) {

                    componentPackageFile = WorkerUtils.dumpToTmpFile(uploadedInputStream);
                    functionDetails = validateUpdateRequestParams(tenant, namespace, sourceName,
                            mergedConfig, componentPackageFile);

                } else if (existingComponent.getPackageLocation().getPackagePath().startsWith(Utils.BUILTIN)) {
                    functionDetails = validateUpdateRequestParams(tenant, namespace, sourceName,
                            mergedConfig, componentPackageFile);
                    if (!isFunctionCodeBuiltin(functionDetails) && (componentPackageFile == null || fileDetail == null)) {
                        throw new IllegalArgumentException(ComponentTypeUtils.toString(componentType) + " Package is not provided");
                    }
                } else {

                    componentPackageFile = FunctionCommon.createPkgTempFile();
                    componentPackageFile.deleteOnExit();
                    WorkerUtils.downloadFromBookkeeper(worker().getDlogNamespace(), componentPackageFile, existingComponent.getPackageLocation().getPackagePath());

                    functionDetails = validateUpdateRequestParams(tenant, namespace, sourceName,
                            mergedConfig, componentPackageFile);
                }
            } catch (Exception e) {
                log.error("Invalid update {} request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, sourceName, e);
                throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
            }

            try {
                worker().getFunctionRuntimeManager().getRuntimeFactory().doAdmissionChecks(functionDetails);
            } catch (Exception e) {
                log.error("Updated {} {}/{}/{} cannot be submitted to runtime factory", ComponentTypeUtils.toString(componentType), tenant, namespace, sourceName);
                throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s cannot be admitted:- %s",
                        ComponentTypeUtils.toString(componentType), sourceName, e.getMessage()));
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
                            log.error("Error updating authentication data for {} {}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, sourceName, e);
                            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, String.format("Error caching authentication data for %s %s:- %s", ComponentTypeUtils.toString(componentType), sourceName, e.getMessage()));
                        }
                    }
                });
            }

            Function.PackageLocationMetaData.Builder packageLocationMetaDataBuilder;
            if (isNotBlank(sourcePkgUrl) || uploadedInputStream != null) {
                try {
                    packageLocationMetaDataBuilder = getFunctionPackageLocation(functionMetaDataBuilder.build(),
                            sourcePkgUrl, fileDetail, componentPackageFile);
                } catch (Exception e) {
                    log.error("Failed process {} {}/{}/{} package: ", ComponentTypeUtils.toString(componentType), tenant, namespace, sourceName, e);
                    throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
                }
            } else {
                packageLocationMetaDataBuilder = Function.PackageLocationMetaData.newBuilder().mergeFrom(existingComponent.getPackageLocation());
            }

            functionMetaDataBuilder.setPackageLocation(packageLocationMetaDataBuilder);

            updateRequest(existingComponent, functionMetaDataBuilder.build());
        } finally {
            if (componentPackageFile != null && componentPackageFile.exists()) {
                if ((sourcePkgUrl != null && !sourcePkgUrl.startsWith(Utils.FILE)) || uploadedInputStream != null) {
                    componentPackageFile.delete();
                }
            }
        }
    }

    private class GetSourceStatus extends GetStatus<SourceStatus, SourceStatus.SourceInstanceStatus.SourceInstanceStatusData> {

        @Override
        public SourceStatus.SourceInstanceStatus.SourceInstanceStatusData notScheduledInstance() {
            SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData
                    = new SourceStatus.SourceInstanceStatus.SourceInstanceStatusData();
            sourceInstanceStatusData.setRunning(false);
            sourceInstanceStatusData.setError("Source has not been scheduled");
            return sourceInstanceStatusData;
        }

        @Override
        public SourceStatus.SourceInstanceStatus.SourceInstanceStatusData fromFunctionStatusProto(
                InstanceCommunication.FunctionStatus status,
                String assignedWorkerId) {
            SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData
                    = new SourceStatus.SourceInstanceStatus.SourceInstanceStatusData();
            sourceInstanceStatusData.setRunning(status.getRunning());
            sourceInstanceStatusData.setError(status.getFailureException());
            sourceInstanceStatusData.setNumRestarts(status.getNumRestarts());
            sourceInstanceStatusData.setNumReceivedFromSource(status.getNumReceived());

            sourceInstanceStatusData.setNumSourceExceptions(status.getNumSourceExceptions());
            List<ExceptionInformation> sourceExceptionInformationList = new LinkedList<>();
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSourceExceptionsList()) {
                ExceptionInformation exceptionInformation
                        = new ExceptionInformation();
                exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
                exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
                sourceExceptionInformationList.add(exceptionInformation);
            }
            sourceInstanceStatusData.setLatestSourceExceptions(sourceExceptionInformationList);

            // Source treats all system and sink exceptions as system exceptions
            sourceInstanceStatusData.setNumSystemExceptions(status.getNumSystemExceptions()
                    + status.getNumUserExceptions() + status.getNumSinkExceptions());
            List<ExceptionInformation> systemExceptionInformationList = new LinkedList<>();
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestUserExceptionsList()) {
                ExceptionInformation exceptionInformation
                        = new ExceptionInformation();
                exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
                exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
                systemExceptionInformationList.add(exceptionInformation);
            }

            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSystemExceptionsList()) {
                ExceptionInformation exceptionInformation
                        = new ExceptionInformation();
                exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
                exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
                systemExceptionInformationList.add(exceptionInformation);
            }

            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSinkExceptionsList()) {
                ExceptionInformation exceptionInformation
                        = new ExceptionInformation();
                exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
                exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
                systemExceptionInformationList.add(exceptionInformation);
            }
            sourceInstanceStatusData.setLatestSystemExceptions(systemExceptionInformationList);

            sourceInstanceStatusData.setNumWritten(status.getNumSuccessfullyProcessed());
            sourceInstanceStatusData.setLastReceivedTime(status.getLastInvocationTime());
            sourceInstanceStatusData.setWorkerId(assignedWorkerId);

            return sourceInstanceStatusData;
        }

        @Override
        public SourceStatus.SourceInstanceStatus.SourceInstanceStatusData notRunning(String assignedWorkerId, String error) {
            SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData
                    = new SourceStatus.SourceInstanceStatus.SourceInstanceStatusData();
            sourceInstanceStatusData.setRunning(false);
            if (error != null) {
                sourceInstanceStatusData.setError(error);
            }
            sourceInstanceStatusData.setWorkerId(assignedWorkerId);

            return sourceInstanceStatusData;
        }

        @Override
        public SourceStatus getStatus(final String tenant,
                                      final String namespace,
                                      final String name,
                                      final Collection<Function.Assignment> assignments,
                                      final URI uri) throws PulsarAdminException {
            SourceStatus sourceStatus = new SourceStatus();
            for (Function.Assignment assignment : assignments) {
                boolean isOwner = worker().getWorkerConfig().getWorkerId().equals(assignment.getWorkerId());
                SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData;
                if (isOwner) {
                    sourceInstanceStatusData = getComponentInstanceStatus(tenant, namespace, name, assignment.getInstance().getInstanceId(), null);
                } else {
                    sourceInstanceStatusData = worker().getFunctionAdmin().sources().getSourceStatus(
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getTenant(),
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getNamespace(),
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getName(),
                            assignment.getInstance().getInstanceId());
                }

                SourceStatus.SourceInstanceStatus instanceStatus = new SourceStatus.SourceInstanceStatus();
                instanceStatus.setInstanceId(assignment.getInstance().getInstanceId());
                instanceStatus.setStatus(sourceInstanceStatusData);
                sourceStatus.addInstance(instanceStatus);
            }

            sourceStatus.setNumInstances(sourceStatus.instances.size());
            sourceStatus.getInstances().forEach(sourceInstanceStatus -> {
                if (sourceInstanceStatus.getStatus().isRunning()) {
                    sourceStatus.numRunning++;
                }
            });
            return sourceStatus;
        }

        @Override
        public SourceStatus getStatusExternal(final String tenant,
                                              final String namespace,
                                              final String name,
                                              final int parallelism) {
            SourceStatus sinkStatus = new SourceStatus();
            for (int i = 0; i < parallelism; ++i) {
                SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData
                        = getComponentInstanceStatus(tenant, namespace, name, i, null);
                SourceStatus.SourceInstanceStatus sourceInstanceStatus
                        = new SourceStatus.SourceInstanceStatus();
                sourceInstanceStatus.setInstanceId(i);
                sourceInstanceStatus.setStatus(sourceInstanceStatusData);
                sinkStatus.addInstance(sourceInstanceStatus);
            }

            sinkStatus.setNumInstances(sinkStatus.instances.size());
            sinkStatus.getInstances().forEach(sourceInstanceStatus -> {
                if (sourceInstanceStatus.getStatus().isRunning()) {
                    sinkStatus.numRunning++;
                }
            });
            return sinkStatus;
        }

        @Override
        public SourceStatus emptyStatus(final int parallelism) {
            SourceStatus sourceStatus = new SourceStatus();
            sourceStatus.setNumInstances(parallelism);
            sourceStatus.setNumRunning(0);
            for (int i = 0; i < parallelism; i++) {
                SourceStatus.SourceInstanceStatus sourceInstanceStatus = new SourceStatus.SourceInstanceStatus();
                sourceInstanceStatus.setInstanceId(i);
                SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData
                        = new SourceStatus.SourceInstanceStatus.SourceInstanceStatusData();
                sourceInstanceStatusData.setRunning(false);
                sourceInstanceStatusData.setError("Source has not been scheduled");
                sourceInstanceStatus.setStatus(sourceInstanceStatusData);

                sourceStatus.addInstance(sourceInstanceStatus);
            }

            return sourceStatus;
        }
    }

    @Override
    public SourceStatus getSourceStatus(final String tenant,
                                        final String namespace,
                                        final String componentName,
                                        final URI uri, final String clientRole,
                                        final AuthenticationDataSource clientAuthenticationDataHttps) {
        // validate parameters
        componentStatusRequestValidate(tenant, namespace, componentName, clientRole, clientAuthenticationDataHttps);

        SourceStatus sourceStatus;
        try {
            sourceStatus = new GetSourceStatus().getComponentStatus(tenant, namespace, componentName, uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Status", tenant, namespace, componentName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        return sourceStatus;
    }

    @Override
    public SourceStatus.SourceInstanceStatus.SourceInstanceStatusData getSourceInstanceStatus(final String tenant,
                                                                                              final String namespace,
                                                                                              final String sourceName,
                                                                                              final String instanceId,
                                                                                              final URI uri,
                                                                                              final String clientRole,
                                                                                              final AuthenticationDataSource clientAuthenticationDataHttps) {
        // validate parameters
        componentInstanceStatusRequestValidate(tenant, namespace, sourceName, Integer.parseInt(instanceId), clientRole, clientAuthenticationDataHttps);

        SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData;
        try {
            sourceInstanceStatusData = new GetSourceStatus().getComponentInstanceStatus(tenant, namespace, sourceName,
                    Integer.parseInt(instanceId), uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Status", tenant, namespace, sourceName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
        return sourceInstanceStatusData;
    }

    @Override
    public SourceConfig getSourceInfo(final String tenant,
                                      final String namespace,
                                      final String componentName) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        // validate parameters
        try {
            validateGetFunctionRequestParams(tenant, namespace, componentName, componentType);
        } catch (IllegalArgumentException e) {
            log.error("Invalid get {} request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, componentName, e);
            throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.error("{} does not exist @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, componentName);
            throw new RestException(Response.Status.NOT_FOUND, String.format(ComponentTypeUtils.toString(componentType) + " %s doesn't exist", componentName));
        }
        Function.FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName, ComponentTypeUtils.toString(componentType));
            throw new RestException(Response.Status.NOT_FOUND, String.format(ComponentTypeUtils.toString(componentType) + " %s doesn't exist", componentName));
        }
        SourceConfig config = SourceConfigUtils.convertFromDetails(functionMetaData.getFunctionDetails());
        return config;
    }

    @Override
    public List<ConnectorDefinition> getSourceList() {
        List<ConnectorDefinition> connectorDefinitions = getListOfConnectors();
        List<ConnectorDefinition> retval = new ArrayList<>();
        for (ConnectorDefinition connectorDefinition : connectorDefinitions) {
            if (!org.apache.commons.lang.StringUtils.isEmpty(connectorDefinition.getSourceClass())) {
                retval.add(connectorDefinition);
            }
        }
        return retval;
    }

    @Override
    public List<ConfigFieldDefinition> getSourceConfigDefinition(String name) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }
        List<ConfigFieldDefinition> retval = this.worker().getConnectorsManager().getSourceConfigDefinition(name);
        if (retval == null) {
            throw new RestException(Response.Status.NOT_FOUND, "builtin source does not exist");
        }
        return retval;
    }

    private Function.FunctionDetails validateUpdateRequestParams(final String tenant,
                                                                 final String namespace,
                                                                 final String sourceName,
                                                                 final SourceConfig sourceConfig,
                                                                 final File sourcePackageFile) {
        // The rest end points take precedence over whatever is there in sourceconfig
        sourceConfig.setTenant(tenant);
        sourceConfig.setNamespace(namespace);
        sourceConfig.setName(sourceName);
        org.apache.pulsar.common.functions.Utils.inferMissingArguments(sourceConfig);

        ClassLoader classLoader = null;
        // check if source is builtin and extract classloader
        if (!StringUtils.isEmpty(sourceConfig.getArchive())) {
            String archive = sourceConfig.getArchive();
            if (archive.startsWith(org.apache.pulsar.common.functions.Utils.BUILTIN)) {
                archive = archive.replaceFirst("^builtin://", "");

                Connector connector = worker().getConnectorsManager().getConnector(archive);
                // check if builtin connector exists
                if (connector == null) {
                    throw new IllegalArgumentException("Built-in source is not available");
                }
                classLoader = connector.getClassLoader();
            }
        }

        boolean shouldCloseClassLoader = false;
        try {
            // if source is not builtin, attempt to extract classloader from package file if it exists
            if (classLoader == null && sourcePackageFile != null) {
                classLoader = getClassLoaderFromPackage(sourceConfig.getClassName(),
                        sourcePackageFile, worker().getWorkerConfig().getNarExtractionDirectory());
                shouldCloseClassLoader = true;
            }

            if (classLoader == null) {
                throw new IllegalArgumentException("Source package is not provided");
            }

            SourceConfigUtils.ExtractedSourceDetails sourceDetails
                    = SourceConfigUtils.validateAndExtractDetails(
                            sourceConfig, classLoader, worker().getWorkerConfig().getValidateConnectorConfig());
            return SourceConfigUtils.convert(sourceConfig, sourceDetails);
        } finally {
            if (shouldCloseClassLoader) {
                ClassLoaderUtils.closeClassLoader(classLoader);
            }
        }
    }

    private File downloadPackageFile(String packageName) throws IOException, PulsarAdminException {
        return FunctionsImpl.downloadPackageFile(worker(), packageName);
    }
}
