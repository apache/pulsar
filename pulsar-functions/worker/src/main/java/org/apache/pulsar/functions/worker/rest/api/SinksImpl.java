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
import java.util.Arrays;
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
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.policies.data.ExceptionInformation;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.auth.FunctionAuthData;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.utils.ComponentTypeUtils;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.SinkConfigUtils;
import org.apache.pulsar.functions.utils.io.Connector;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.PulsarWorkerService;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.apache.pulsar.functions.worker.service.api.Sinks;
import org.apache.pulsar.packages.management.core.common.PackageType;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

@Slf4j
public class SinksImpl extends ComponentImpl implements Sinks<PulsarWorkerService> {

    public SinksImpl(Supplier<PulsarWorkerService> workerServiceSupplier) {
        super(workerServiceSupplier, Function.FunctionDetails.ComponentType.SINK);
    }

    @Override
    public void registerSink(final String tenant,
                             final String namespace,
                             final String sinkName,
                             final InputStream uploadedInputStream,
                             final FormDataContentDisposition fileDetail,
                             final String sinkPkgUrl,
                             final SinkConfig sinkConfig,
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
        if (sinkName == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Sink name is not provided");
        }
        if (sinkConfig == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Sink config is not provided");
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.warn("{}/{}/{} Client [{}] is not authorized to register {}", tenant, namespace,
                        sinkName, clientRole, ComponentTypeUtils.toString(componentType));
                throw new RestException(Response.Status.UNAUTHORIZED, "Client is not authorized to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, sinkName, e);
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
                    log.error("{}/{}/{} Namespace {} does not exist", tenant, namespace, sinkName, namespace);
                    throw new RestException(Response.Status.BAD_REQUEST, "Namespace does not exist");
                }
            }
        } catch (PulsarAdminException.NotAuthorizedException e) {
            log.error("{}/{}/{} Client [{}] is not authorized to operate {} on tenant", tenant, namespace,
                    sinkName, clientRole, ComponentTypeUtils.toString(componentType));
            throw new RestException(Response.Status.UNAUTHORIZED, "Client is not authorized to perform operation");
        } catch (PulsarAdminException.NotFoundException e) {
            log.error("{}/{}/{} Tenant {} does not exist", tenant, namespace, sinkName, tenant);
            throw new RestException(Response.Status.BAD_REQUEST, "Tenant does not exist");
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Issues getting tenant data", tenant, namespace, sinkName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();

        if (functionMetaDataManager.containsFunction(tenant, namespace, sinkName)) {
            log.error("{} {}/{}/{} already exists", ComponentTypeUtils.toString(componentType), tenant, namespace, sinkName);
            throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s already exists", ComponentTypeUtils.toString(componentType), sinkName));
        }

        Function.FunctionDetails functionDetails = null;
        boolean isPkgUrlProvided = isNotBlank(sinkPkgUrl);
        File componentPackageFile = null;
        try {

            // validate parameters
            try {
                if (isPkgUrlProvided) {
                    if (Utils.hasPackageTypePrefix(sinkPkgUrl)) {
                        componentPackageFile = downloadPackageFile(sinkPkgUrl);
                    } else {
                        if (!Utils.isFunctionPackageUrlSupported(sinkPkgUrl)) {
                            throw new IllegalArgumentException("Function Package url is not valid. supported url (http/https/file)");
                        }
                        try {
                            componentPackageFile = FunctionCommon.extractFileFromPkgURL(sinkPkgUrl);
                        } catch (Exception e) {
                            throw new IllegalArgumentException(String.format("Encountered error \"%s\" when getting %s package from %s", e.getMessage(), ComponentTypeUtils.toString(componentType), sinkPkgUrl));
                        }
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, sinkName,
                            sinkConfig, componentPackageFile);
                } else {
                    if (uploadedInputStream != null) {
                        componentPackageFile = WorkerUtils.dumpToTmpFile(uploadedInputStream);
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, sinkName,
                            sinkConfig, componentPackageFile);
                    if (!isFunctionCodeBuiltin(functionDetails) && (componentPackageFile == null || fileDetail == null)) {
                        throw new IllegalArgumentException(ComponentTypeUtils.toString(componentType) + " Package is not provided");
                    }
                }
            } catch (Exception e) {
                log.error("Invalid register {} request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, sinkName, e);
                throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
            }

            try {
                worker().getFunctionRuntimeManager().getRuntimeFactory().doAdmissionChecks(functionDetails);
            } catch (Exception e) {
                log.error("{} {}/{}/{} cannot be admitted by the runtime factory", ComponentTypeUtils.toString(componentType), tenant, namespace, sinkName);
                throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s cannot be admitted:- %s", ComponentTypeUtils.toString(componentType), sinkName, e.getMessage()));
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
                                    ComponentTypeUtils.toString(componentType), tenant, namespace, sinkName, e);


                            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, String.format("Error caching authentication data for %s %s:- %s",
                                    ComponentTypeUtils.toString(componentType), sinkName, e.getMessage()));
                        }
                    }
                });
            }

            Function.PackageLocationMetaData.Builder packageLocationMetaDataBuilder;
            try {
                packageLocationMetaDataBuilder = getFunctionPackageLocation(functionMetaDataBuilder.build(),
                        sinkPkgUrl, fileDetail, componentPackageFile);
            } catch (Exception e) {
                log.error("Failed process {} {}/{}/{} package: ", ComponentTypeUtils.toString(componentType), tenant, namespace, sinkName, e);
                throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
            }

            functionMetaDataBuilder.setPackageLocation(packageLocationMetaDataBuilder);
            updateRequest(null, functionMetaDataBuilder.build());
        } finally {
            if (componentPackageFile != null && componentPackageFile.exists()) {
                if (sinkPkgUrl == null || !sinkPkgUrl.startsWith(Utils.FILE)) {
                    componentPackageFile.delete();
                }
            }
        }
    }

    @Override
    public void updateSink(final String tenant,
                           final String namespace,
                           final String sinkName,
                           final InputStream uploadedInputStream,
                           final FormDataContentDisposition fileDetail,
                           final String sinkPkgUrl,
                           final SinkConfig sinkConfig,
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
        if (sinkName == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Sink name is not provided");
        }
        if (sinkConfig == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Sink config is not provided");
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.warn("{}/{}/{} Client [{}] is not authorized to update {}", tenant, namespace,
                        sinkName, clientRole, ComponentTypeUtils.toString(componentType));
                throw new RestException(Response.Status.UNAUTHORIZED, "Client is not authorized to perform operation");

            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, sinkName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();

        if (!functionMetaDataManager.containsFunction(tenant, namespace, sinkName)) {
            throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), sinkName));
        }

        Function.FunctionMetaData existingComponent = functionMetaDataManager.getFunctionMetaData(tenant, namespace, sinkName);

        if (!InstanceUtils.calculateSubjectType(existingComponent.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, sinkName, ComponentTypeUtils.toString(componentType));
            throw new RestException(Response.Status.NOT_FOUND, String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), sinkName));
        }


        SinkConfig existingSinkConfig = SinkConfigUtils.convertFromDetails(existingComponent.getFunctionDetails());
        // The rest end points take precedence over whatever is there in functionconfig
        sinkConfig.setTenant(tenant);
        sinkConfig.setNamespace(namespace);
        sinkConfig.setName(sinkName);

        SinkConfig mergedConfig;
        try {
            mergedConfig = SinkConfigUtils.validateUpdate(existingSinkConfig, sinkConfig);
        } catch (Exception e) {
            throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
        }

        if (existingSinkConfig.equals(mergedConfig) && isBlank(sinkPkgUrl) && uploadedInputStream == null) {
            log.error("{}/{}/{} Update contains no changes", tenant, namespace, sinkName);
            throw new RestException(Response.Status.BAD_REQUEST, "Update contains no change");
        }

        Function.FunctionDetails functionDetails = null;
        File componentPackageFile = null;
        try {

            // validate parameters
            try {
                if (isNotBlank(sinkPkgUrl)) {
                    if (Utils.hasPackageTypePrefix(sinkPkgUrl)) {
                        componentPackageFile = downloadPackageFile(sinkPkgUrl);
                    } else {
                        try {
                            componentPackageFile = FunctionCommon.extractFileFromPkgURL(sinkPkgUrl);
                        } catch (Exception e) {
                            throw new IllegalArgumentException(String.format("Encountered error \"%s\" when getting %s package from %s", e.getMessage(), ComponentTypeUtils.toString(componentType), sinkPkgUrl));
                        }
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, sinkName,
                            mergedConfig, componentPackageFile);

                } else if (existingComponent.getPackageLocation().getPackagePath().startsWith(Utils.FILE)
                        || existingComponent.getPackageLocation().getPackagePath().startsWith(Utils.HTTP)) {
                    try {
                        componentPackageFile = FunctionCommon.extractFileFromPkgURL(existingComponent.getPackageLocation().getPackagePath());
                    } catch (Exception e) {
                        throw new IllegalArgumentException(String.format("Encountered error \"%s\" when getting %s package from %s", e.getMessage(), ComponentTypeUtils.toString(componentType), sinkPkgUrl));
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, sinkName,
                            mergedConfig, componentPackageFile);
                } else if (uploadedInputStream != null) {

                    componentPackageFile = WorkerUtils.dumpToTmpFile(uploadedInputStream);
                    functionDetails = validateUpdateRequestParams(tenant, namespace, sinkName,
                            mergedConfig, componentPackageFile);

                } else if (existingComponent.getPackageLocation().getPackagePath().startsWith(Utils.BUILTIN)) {
                    functionDetails = validateUpdateRequestParams(tenant, namespace, sinkName,
                            mergedConfig, componentPackageFile);
                    if (!isFunctionCodeBuiltin(functionDetails) && (componentPackageFile == null || fileDetail == null)) {
                        throw new IllegalArgumentException(ComponentTypeUtils.toString(componentType) + " Package is not provided");
                    }
                } else {

                    componentPackageFile = FunctionCommon.createPkgTempFile();
                    componentPackageFile.deleteOnExit();
                    WorkerUtils.downloadFromBookkeeper(worker().getDlogNamespace(), componentPackageFile, existingComponent.getPackageLocation().getPackagePath());

                    functionDetails = validateUpdateRequestParams(tenant, namespace, sinkName,
                            mergedConfig, componentPackageFile);
                }
            } catch (Exception e) {
                log.error("Invalid update {} request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, sinkName, e);
                throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
            }

            try {
                worker().getFunctionRuntimeManager().getRuntimeFactory().doAdmissionChecks(functionDetails);
            } catch (Exception e) {
                log.error("Updated {} {}/{}/{} cannot be submitted to runtime factory", ComponentTypeUtils.toString(componentType), tenant, namespace, sinkName);
                throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s cannot be admitted:- %s",
                        ComponentTypeUtils.toString(componentType), sinkName, e.getMessage()));
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
                            log.error("Error updating authentication data for {} {}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, sinkName, e);
                            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, String.format("Error caching authentication data for %s %s:- %s", ComponentTypeUtils.toString(componentType), sinkName, e.getMessage()));
                        }
                    }
                });
            }

            Function.PackageLocationMetaData.Builder packageLocationMetaDataBuilder;
            if (isNotBlank(sinkPkgUrl) || uploadedInputStream != null) {
                try {
                    packageLocationMetaDataBuilder = getFunctionPackageLocation(functionMetaDataBuilder.build(),
                            sinkPkgUrl, fileDetail, componentPackageFile);
                } catch (Exception e) {
                    log.error("Failed process {} {}/{}/{} package: ", ComponentTypeUtils.toString(componentType), tenant, namespace, sinkName, e);
                    throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
                }
            } else {
                packageLocationMetaDataBuilder = Function.PackageLocationMetaData.newBuilder().mergeFrom(existingComponent.getPackageLocation());
            }

            functionMetaDataBuilder.setPackageLocation(packageLocationMetaDataBuilder);

            updateRequest(existingComponent, functionMetaDataBuilder.build());
        } finally {
            if (componentPackageFile != null && componentPackageFile.exists()) {
                if ((sinkPkgUrl != null && !sinkPkgUrl.startsWith(Utils.FILE)) || uploadedInputStream != null) {
                    componentPackageFile.delete();
                }
            }
        }
    }

    private class GetSinkStatus extends GetStatus<SinkStatus, SinkStatus.SinkInstanceStatus.SinkInstanceStatusData> {

        @Override
        public SinkStatus.SinkInstanceStatus.SinkInstanceStatusData notScheduledInstance() {
            SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData
                    = new SinkStatus.SinkInstanceStatus.SinkInstanceStatusData();
            sinkInstanceStatusData.setRunning(false);
            sinkInstanceStatusData.setError("Sink has not been scheduled");
            return sinkInstanceStatusData;
        }

        @Override
        public SinkStatus.SinkInstanceStatus.SinkInstanceStatusData fromFunctionStatusProto(
                InstanceCommunication.FunctionStatus status,
                String assignedWorkerId) {
            SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData
                    = new SinkStatus.SinkInstanceStatus.SinkInstanceStatusData();
            sinkInstanceStatusData.setRunning(status.getRunning());
            sinkInstanceStatusData.setError(status.getFailureException());
            sinkInstanceStatusData.setNumRestarts(status.getNumRestarts());
            sinkInstanceStatusData.setNumReadFromPulsar(status.getNumReceived());

            // We treat source/user/system exceptions returned from function as system exceptions
            sinkInstanceStatusData.setNumSystemExceptions(status.getNumSystemExceptions()
                    + status.getNumUserExceptions() + status.getNumSourceExceptions());
            List<ExceptionInformation> systemExceptionInformationList = new LinkedList<>();
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestUserExceptionsList()) {
                ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
                systemExceptionInformationList.add(exceptionInformation);
            }

            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSystemExceptionsList()) {
                ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
                systemExceptionInformationList.add(exceptionInformation);
            }

            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSourceExceptionsList()) {
                ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
                systemExceptionInformationList.add(exceptionInformation);
            }
            sinkInstanceStatusData.setLatestSystemExceptions(systemExceptionInformationList);

            sinkInstanceStatusData.setNumSinkExceptions(status.getNumSinkExceptions());
            List<ExceptionInformation> sinkExceptionInformationList = new LinkedList<>();
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSinkExceptionsList()) {
                ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
                sinkExceptionInformationList.add(exceptionInformation);
            }
            sinkInstanceStatusData.setLatestSinkExceptions(sinkExceptionInformationList);

            sinkInstanceStatusData.setNumWrittenToSink(status.getNumSuccessfullyProcessed());
            sinkInstanceStatusData.setLastReceivedTime(status.getLastInvocationTime());
            sinkInstanceStatusData.setWorkerId(assignedWorkerId);

            return sinkInstanceStatusData;
        }

        @Override
        public SinkStatus.SinkInstanceStatus.SinkInstanceStatusData notRunning(String assignedWorkerId, String error) {
            SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData
                    = new SinkStatus.SinkInstanceStatus.SinkInstanceStatusData();
            sinkInstanceStatusData.setRunning(false);
            if (error != null) {
                sinkInstanceStatusData.setError(error);
            }
            sinkInstanceStatusData.setWorkerId(assignedWorkerId);

            return sinkInstanceStatusData;
        }

        @Override
        public SinkStatus getStatus(final String tenant,
                                    final String namespace,
                                    final String name,
                                    final Collection<Function.Assignment> assignments,
                                    final URI uri) throws PulsarAdminException {
            SinkStatus sinkStatus = new SinkStatus();
            for (Function.Assignment assignment : assignments) {
                boolean isOwner = worker().getWorkerConfig().getWorkerId().equals(assignment.getWorkerId());
                SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData;
                if (isOwner) {
                    sinkInstanceStatusData = getComponentInstanceStatus(tenant,
                            namespace, name, assignment.getInstance().getInstanceId(), null);
                } else {
                    sinkInstanceStatusData = worker().getFunctionAdmin().sink().getSinkStatus(
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getTenant(),
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getNamespace(),
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getName(),
                            assignment.getInstance().getInstanceId());
                }

                SinkStatus.SinkInstanceStatus instanceStatus = new SinkStatus.SinkInstanceStatus();
                instanceStatus.setInstanceId(assignment.getInstance().getInstanceId());
                instanceStatus.setStatus(sinkInstanceStatusData);
                sinkStatus.addInstance(instanceStatus);
            }

            sinkStatus.setNumInstances(sinkStatus.instances.size());
            sinkStatus.getInstances().forEach(sinkInstanceStatus -> {
                if (sinkInstanceStatus.getStatus().isRunning()) {
                    sinkStatus.numRunning++;
                }
            });
            return sinkStatus;
        }

        @Override
        public SinkStatus getStatusExternal(final String tenant,
                                            final String namespace,
                                            final String name,
                                            final int parallelism) {
            SinkStatus sinkStatus = new SinkStatus();
            for (int i = 0; i < parallelism; ++i) {
                SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData
                        = getComponentInstanceStatus(tenant, namespace, name, i, null);
                SinkStatus.SinkInstanceStatus sinkInstanceStatus
                        = new SinkStatus.SinkInstanceStatus();
                sinkInstanceStatus.setInstanceId(i);
                sinkInstanceStatus.setStatus(sinkInstanceStatusData);
                sinkStatus.addInstance(sinkInstanceStatus);
            }

            sinkStatus.setNumInstances(sinkStatus.instances.size());
            sinkStatus.getInstances().forEach(sinkInstanceStatus -> {
                if (sinkInstanceStatus.getStatus().isRunning()) {
                    sinkStatus.numRunning++;
                }
            });
            return sinkStatus;
        }

        @Override
        public SinkStatus emptyStatus(final int parallelism) {
            SinkStatus sinkStatus = new SinkStatus();
            sinkStatus.setNumInstances(parallelism);
            sinkStatus.setNumRunning(0);
            for (int i = 0; i < parallelism; i++) {
                SinkStatus.SinkInstanceStatus sinkInstanceStatus = new SinkStatus.SinkInstanceStatus();
                sinkInstanceStatus.setInstanceId(i);
                SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData
                        = new SinkStatus.SinkInstanceStatus.SinkInstanceStatusData();
                sinkInstanceStatusData.setRunning(false);
                sinkInstanceStatusData.setError("Sink has not been scheduled");
                sinkInstanceStatus.setStatus(sinkInstanceStatusData);

                sinkStatus.addInstance(sinkInstanceStatus);
            }

            return sinkStatus;
        }
    }

    private ExceptionInformation getExceptionInformation(InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry) {
        ExceptionInformation exceptionInformation
                = new ExceptionInformation();
        exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
        exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
        return exceptionInformation;
    }

    @Override
    public SinkStatus.SinkInstanceStatus.SinkInstanceStatusData getSinkInstanceStatus(final String tenant,
                                                                                      final String namespace,
                                                                                      final String sinkName,
                                                                                      final String instanceId,
                                                                                      final URI uri,
                                                                                      final String clientRole,
                                                                                      final AuthenticationDataSource clientAuthenticationDataHttps) {

        // validate parameters
        componentInstanceStatusRequestValidate(tenant, namespace, sinkName, Integer.parseInt(instanceId), clientRole, clientAuthenticationDataHttps);


        SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData;
        try {
            sinkInstanceStatusData = new GetSinkStatus().getComponentInstanceStatus(tenant, namespace, sinkName,
                    Integer.parseInt(instanceId), uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Status", tenant, namespace, sinkName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
        return sinkInstanceStatusData;
    }

    @Override
    public SinkStatus getSinkStatus(final String tenant,
                                    final String namespace,
                                    final String componentName,
                                    final URI uri,
                                    final String clientRole,
                                    final AuthenticationDataSource clientAuthenticationDataHttps) {

        // validate parameters
        componentStatusRequestValidate(tenant, namespace, componentName, clientRole, clientAuthenticationDataHttps);

        SinkStatus sinkStatus;
        try {
            sinkStatus = new GetSinkStatus().getComponentStatus(tenant, namespace, componentName, uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Status", tenant, namespace, componentName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        return sinkStatus;
    }

    @Override
    public SinkConfig getSinkInfo(final String tenant,
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
        SinkConfig config = SinkConfigUtils.convertFromDetails(functionMetaData.getFunctionDetails());
        return config;
    }

    @Override
    public List<ConnectorDefinition> getSinkList() {
        List<ConnectorDefinition> connectorDefinitions = getListOfConnectors();
        List<ConnectorDefinition> retval = new ArrayList<>();
        for (ConnectorDefinition connectorDefinition : connectorDefinitions) {
            if (!org.apache.commons.lang.StringUtils.isEmpty(connectorDefinition.getSinkClass())) {
                retval.add(connectorDefinition);
            }
        }
        return retval;
    }

    @Override
    public List<ConfigFieldDefinition> getSinkConfigDefinition(String name) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }
        List<ConfigFieldDefinition> retval = this.worker().getConnectorsManager().getSinkConfigDefinition(name);
        if (retval == null) {
            throw new RestException(Response.Status.NOT_FOUND, "builtin sink does not exist");
        }
        return retval;
    }

    private Function.FunctionDetails validateUpdateRequestParams(final String tenant,
                                                                 final String namespace,
                                                                 final String sinkName,
                                                                 final SinkConfig sinkConfig,
                                                                 final File sinkPackageFile) throws IOException {

        // The rest end points take precedence over whatever is there in sinkConfig
        sinkConfig.setTenant(tenant);
        sinkConfig.setNamespace(namespace);
        sinkConfig.setName(sinkName);
        org.apache.pulsar.common.functions.Utils.inferMissingArguments(sinkConfig);

        ClassLoader classLoader = null;
        // check if sink is builtin and extract classloader
        if (!StringUtils.isEmpty(sinkConfig.getArchive())) {
            String archive = sinkConfig.getArchive();
            if (archive.startsWith(org.apache.pulsar.common.functions.Utils.BUILTIN)) {
                archive = archive.replaceFirst("^builtin://", "");

                Connector connector = worker().getConnectorsManager().getConnector(archive);
                // check if builtin connector exists
                if (connector == null) {
                    throw new IllegalArgumentException("Built-in sink is not available");
                }
                classLoader = connector.getClassLoader();
            }
        }

        // if sink is not builtin, attempt to extract classloader from package file if it exists
        if (classLoader == null && sinkPackageFile != null) {
            classLoader = getClassLoaderFromPackage(sinkConfig.getClassName(),
                    sinkPackageFile, worker().getWorkerConfig().getNarExtractionDirectory());
        }

        if (classLoader == null) {
            throw new IllegalArgumentException("Sink package is not provided");
        }

        SinkConfigUtils.ExtractedSinkDetails sinkDetails = SinkConfigUtils.validateAndExtractDetails(
                sinkConfig, classLoader, worker().getWorkerConfig().getValidateConnectorConfig());
        return SinkConfigUtils.convert(sinkConfig, sinkDetails);
    }

    private File downloadPackageFile(String packageName) throws IOException, PulsarAdminException {
        return FunctionsImpl.downloadPackageFile(worker(), packageName);
    }
}
