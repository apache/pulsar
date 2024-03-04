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
import org.apache.pulsar.broker.authentication.AuthenticationParameters;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionDefinition;
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
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.utils.FunctionFilePackage;
import org.apache.pulsar.functions.utils.FunctionMetaDataUtils;
import org.apache.pulsar.functions.utils.ValidatableFunctionPackage;
import org.apache.pulsar.functions.utils.functions.FunctionArchive;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.FunctionsManager;
import org.apache.pulsar.functions.worker.PulsarWorkerService;
import org.apache.pulsar.functions.worker.WorkerConfig;
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
                                 final AuthenticationParameters authParams) {

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

        throwRestExceptionIfUnauthorizedForNamespace(tenant, namespace, functionName, "register", authParams);

        try {
            // Check tenant exists
            worker().getBrokerAdmin().tenants().getTenantInfo(tenant);

            String qualifiedNamespace = tenant + "/" + namespace;
            List<String> namespaces = worker().getBrokerAdmin().namespaces().getNamespaces(tenant);
            if (namespaces != null && !namespaces.contains(qualifiedNamespace)) {
                String qualifiedNamespaceWithCluster = String.format("%s/%s/%s", tenant,
                        worker().getWorkerConfig().getPulsarFunctionsCluster(), namespace);
                if (!namespaces.contains(qualifiedNamespaceWithCluster)) {
                    log.error("{}/{}/{} Namespace {} does not exist", tenant, namespace, functionName, namespace);
                    throw new RestException(Response.Status.BAD_REQUEST, "Namespace does not exist");
                }
            }
        } catch (PulsarAdminException.NotAuthorizedException e) {
            log.error("{}/{}/{} Client is not authorized to operate {} on tenant", tenant, namespace,
                    functionName, ComponentTypeUtils.toString(componentType));
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
            log.error("{} {}/{}/{} already exists",
                    ComponentTypeUtils.toString(componentType), tenant, namespace, functionName);
            throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s already exists",
                    ComponentTypeUtils.toString(componentType), functionName));
        }

        Function.FunctionDetails functionDetails;
        File componentPackageFile = null;
        try {

            // validate parameters
            try {
                if (isNotBlank(functionPkgUrl)) {
                    componentPackageFile = getPackageFile(componentType, functionPkgUrl);
                    functionDetails = validateUpdateRequestParams(tenant, namespace, functionName,
                            functionConfig, componentPackageFile);
                } else {
                    if (uploadedInputStream != null) {
                        componentPackageFile = WorkerUtils.dumpToTmpFile(uploadedInputStream);
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, functionName,
                            functionConfig, componentPackageFile);
                    if (!isFunctionCodeBuiltin(functionDetails)
                            && (componentPackageFile == null || fileDetail == null)) {
                        throw new IllegalArgumentException(ComponentTypeUtils.toString(componentType)
                                + " Package is not provided");
                    }
                }
            } catch (Exception e) {
                log.error("Invalid register {} request @ /{}/{}/{}",
                        ComponentTypeUtils.toString(componentType), tenant, namespace, functionName, e);
                throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
            }

            try {
                worker().getFunctionRuntimeManager().getRuntimeFactory().doAdmissionChecks(functionDetails);
            } catch (Exception e) {
                log.error("{} {}/{}/{} cannot be admitted by the runtime factory",
                        ComponentTypeUtils.toString(componentType), tenant, namespace, functionName);
                throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s cannot be admitted:- %s",
                        ComponentTypeUtils.toString(componentType), functionName, e.getMessage()));
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
                    if (authParams.getClientAuthenticationDataSource() != null) {

                        try {
                            Optional<FunctionAuthData> functionAuthData = functionAuthProvider
                                    .cacheAuthData(finalFunctionDetails,
                                            authParams.getClientAuthenticationDataSource());

                            functionAuthData.ifPresent(authData -> functionMetaDataBuilder.setFunctionAuthSpec(
                                    Function.FunctionAuthenticationSpec.newBuilder()
                                            .setData(ByteString.copyFrom(authData.getData()))
                                            .build()));
                        } catch (Exception e) {
                            log.error("Error caching authentication data for {} {}/{}/{}",
                                    ComponentTypeUtils.toString(componentType), tenant, namespace, functionName, e);


                            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR,
                                    String.format("Error caching authentication data for %s %s:- %s",
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
                log.error("Failed process {} {}/{}/{} package: ", ComponentTypeUtils.toString(componentType),
                        tenant, namespace, functionName, e);
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
                               final AuthenticationParameters authParams,
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

        throwRestExceptionIfUnauthorizedForNamespace(tenant, namespace, functionName, "update",
                authParams);

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();

        if (!functionMetaDataManager.containsFunction(tenant, namespace, functionName)) {
            throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s doesn't exist",
                    ComponentTypeUtils.toString(componentType), functionName));
        }

        Function.FunctionMetaData existingComponent = functionMetaDataManager
                .getFunctionMetaData(tenant, namespace, functionName);

        if (!InstanceUtils.calculateSubjectType(existingComponent.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, functionName,
                    ComponentTypeUtils.toString(componentType));
            throw new RestException(Response.Status.NOT_FOUND, String.format("%s %s doesn't exist",
                    ComponentTypeUtils.toString(componentType), functionName));
        }

        FunctionConfig existingFunctionConfig = FunctionConfigUtils
                .convertFromDetails(existingComponent.getFunctionDetails());
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

        Function.FunctionDetails functionDetails;
        File componentPackageFile = null;
        try {

            // validate parameters
            try {
                componentPackageFile = getPackageFile(
                        componentType,
                        functionPkgUrl,
                        existingComponent.getPackageLocation().getPackagePath(),
                        uploadedInputStream);
                functionDetails = validateUpdateRequestParams(tenant, namespace, functionName,
                        mergedConfig, componentPackageFile);
                if (existingComponent.getPackageLocation().getPackagePath().startsWith(Utils.BUILTIN)
                        && !isFunctionCodeBuiltin(functionDetails)
                        && (componentPackageFile == null || fileDetail == null)) {
                    throw new IllegalArgumentException(ComponentTypeUtils.toString(componentType)
                            + " Package is not provided");
                }
            } catch (Exception e) {
                log.error("Invalid update {} request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType),
                        tenant, namespace, functionName, e);
                throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
            }

            try {
                worker().getFunctionRuntimeManager().getRuntimeFactory().doAdmissionChecks(functionDetails);
            } catch (Exception e) {
                log.error("Updated {} {}/{}/{} cannot be submitted to runtime factory",
                        ComponentTypeUtils.toString(componentType), tenant, namespace, functionName);
                throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s cannot be admitted:- %s",
                        ComponentTypeUtils.toString(componentType), functionName, e.getMessage()));
            }

            // merge from existing metadata
            Function.FunctionMetaData.Builder functionMetaDataBuilder =
                    Function.FunctionMetaData.newBuilder().mergeFrom(existingComponent)
                            .setFunctionDetails(functionDetails);

            // update auth data if need
            if (worker().getWorkerConfig().isAuthenticationEnabled()) {
                Function.FunctionDetails finalFunctionDetails = functionDetails;
                worker().getFunctionRuntimeManager()
                        .getRuntimeFactory()
                        .getAuthProvider().ifPresent(functionAuthProvider -> {
                    if (authParams.getClientAuthenticationDataSource() != null && updateOptions
                            != null && updateOptions.isUpdateAuthData()) {
                        // get existing auth data if it exists
                        Optional<FunctionAuthData> existingFunctionAuthData = Optional.empty();
                        if (functionMetaDataBuilder.hasFunctionAuthSpec()) {
                            existingFunctionAuthData = Optional.ofNullable(getFunctionAuthData(Optional
                                    .ofNullable(functionMetaDataBuilder.getFunctionAuthSpec())));
                        }

                                try {
                                    Optional<FunctionAuthData> newFunctionAuthData = functionAuthProvider
                                            .updateAuthData(finalFunctionDetails, existingFunctionAuthData,
                                                    authParams.getClientAuthenticationDataSource());

                            if (newFunctionAuthData.isPresent()) {
                                functionMetaDataBuilder.setFunctionAuthSpec(
                                        Function.FunctionAuthenticationSpec.newBuilder()
                                                .setData(ByteString.copyFrom(
                                                        newFunctionAuthData.get().getData())).build());
                            } else {
                                functionMetaDataBuilder.clearFunctionAuthSpec();
                            }
                        } catch (Exception e) {
                            log.error("Error updating authentication data for {} {}/{}/{}", ComponentTypeUtils
                                    .toString(componentType), tenant, namespace, functionName, e);
                            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR,
                                    String.format("Error caching authentication data for %s %s:- %s",
                                            ComponentTypeUtils.toString(componentType), functionName,
                                            e.getMessage()));
                        }
                    }
                });
            }

            Function.PackageLocationMetaData.Builder packageLocationMetaDataBuilder;
            if (isNotBlank(functionPkgUrl) || uploadedInputStream != null) {
                Function.FunctionMetaData metaData = functionMetaDataBuilder.build();
                metaData = FunctionMetaDataUtils.incrMetadataVersion(metaData, metaData);
                try {
                    packageLocationMetaDataBuilder = getFunctionPackageLocation(metaData,
                            functionPkgUrl, fileDetail, componentPackageFile);
                } catch (Exception e) {
                    log.error("Failed process {} {}/{}/{} package: ", ComponentTypeUtils.toString(componentType),
                            tenant, namespace, functionName, e);
                    throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
                }
            } else {
                packageLocationMetaDataBuilder = Function.PackageLocationMetaData.newBuilder()
                        .mergeFrom(existingComponent.getPackageLocation());
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

    private class GetFunctionStatus
            extends GetStatus<FunctionStatus, FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData> {

        @Override
        public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData notScheduledInstance() {
            FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData =
                    new FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData();
            functionInstanceStatusData.setRunning(false);
            functionInstanceStatusData.setError("Function has not been scheduled");
            return functionInstanceStatusData;
        }

        @Override
        public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData fromFunctionStatusProto(
                InstanceCommunication.FunctionStatus status,
                String assignedWorkerId) {
            FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData =
                    new FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData();
            functionInstanceStatusData.setRunning(status.getRunning());
            functionInstanceStatusData.setError(status.getFailureException());
            functionInstanceStatusData.setNumRestarts(status.getNumRestarts());
            functionInstanceStatusData.setNumReceived(status.getNumReceived());
            functionInstanceStatusData.setNumSuccessfullyProcessed(status.getNumSuccessfullyProcessed());
            functionInstanceStatusData.setNumUserExceptions(status.getNumUserExceptions());

            List<ExceptionInformation> userExceptionInformationList = new LinkedList<>();
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status
                    .getLatestUserExceptionsList()) {
                ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
                userExceptionInformationList.add(exceptionInformation);
            }
            functionInstanceStatusData.setLatestUserExceptions(userExceptionInformationList);

            // For regular functions source/sink errors are system exceptions
            functionInstanceStatusData.setNumSystemExceptions(status.getNumSystemExceptions()
                    + status.getNumSourceExceptions() + status.getNumSinkExceptions());
            List<ExceptionInformation> systemExceptionInformationList = new LinkedList<>();
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status
                    .getLatestSystemExceptionsList()) {
                ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
                systemExceptionInformationList.add(exceptionInformation);
            }
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status
                    .getLatestSourceExceptionsList()) {
                ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
                systemExceptionInformationList.add(exceptionInformation);
            }
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status
                    .getLatestSinkExceptionsList()) {
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
        public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData notRunning(String assignedWorkerId,
                                                                                           String error) {
            FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData =
                    new FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData();
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
                FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData =
                        getComponentInstanceStatus(tenant, namespace, name, i, null);
                FunctionStatus.FunctionInstanceStatus functionInstanceStatus =
                        new FunctionStatus.FunctionInstanceStatus();
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
                FunctionStatus.FunctionInstanceStatus functionInstanceStatus =
                        new FunctionStatus.FunctionInstanceStatus();
                functionInstanceStatus.setInstanceId(i);
                FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData =
                        new FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData();
                functionInstanceStatusData.setRunning(false);
                functionInstanceStatusData.setError("Function has not been scheduled");
                functionInstanceStatus.setStatus(functionInstanceStatusData);

                functionStatus.addInstance(functionInstanceStatus);
            }

            return functionStatus;
        }
    }

    private ExceptionInformation getExceptionInformation(
            InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry) {
        ExceptionInformation exceptionInformation =
                new ExceptionInformation();
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
    public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData getFunctionInstanceStatus(
            final String tenant,
            final String namespace,
            final String componentName,
            final String instanceId,
            final URI uri,
            final AuthenticationParameters authParams) {

        // validate parameters
        componentInstanceStatusRequestValidate(tenant, namespace, componentName, Integer.parseInt(instanceId),
                authParams);

        FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData;
        try {
            functionInstanceStatusData = new GetFunctionStatus().getComponentInstanceStatus(tenant,
                    namespace, componentName,
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
                                            final AuthenticationParameters authParams) {

        // validate parameters
        componentStatusRequestValidate(tenant, namespace, componentName, authParams);

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
                                             final AuthenticationParameters authParams) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (worker().getWorkerConfig().isAuthorizationEnabled()) {
            if (!isSuperUser(authParams)) {
                log.error("{}/{}/{} Client with role [{}] and originalPrincipal [{}] is not superuser to update on"
                                + " worker leader {}", tenant, namespace, functionName, authParams.getClientRole(),
                        authParams.getClientAuthenticationDataSource(),
                        ComponentTypeUtils.toString(componentType));
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
            if (workerInfo == null || workerInfo.getWorkerId().equals(worker().getWorkerConfig().getWorkerId())) {
                throw new RestException(Response.Status.SERVICE_UNAVAILABLE,
                        "Leader not yet ready. Please retry again");
            }
            URI redirect = UriBuilder.fromUri(uri).host(workerInfo.getWorkerHostname())
                    .port(workerInfo.getPort()).build();
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
                                                                 final File componentPackageFile) {

        // The rest end points take precedence over whatever is there in function config
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(componentName);
        WorkerConfig workerConfig = worker().getWorkerConfig();
        FunctionConfigUtils.inferMissingArguments(
                functionConfig, workerConfig.isForwardSourceMessageProperty());

        String archive = functionConfig.getJar();
        ValidatableFunctionPackage functionPackage = null;
        // check if function is builtin and extract classloader
        if (!StringUtils.isEmpty(archive)) {
            if (archive.startsWith(org.apache.pulsar.common.functions.Utils.BUILTIN)) {
                archive = archive.replaceFirst("^builtin://", "");

                FunctionsManager functionsManager = worker().getFunctionsManager();
                FunctionArchive function = functionsManager.getFunction(archive);

                // check if builtin function exists
                if (function == null) {
                    throw new IllegalArgumentException(String.format("No Function %s found", archive));
                }
                functionPackage = function.getFunctionPackage();
            }
        }
        boolean shouldCloseFunctionPackage = false;
        try {
            if (functionConfig.getRuntime() == FunctionConfig.Runtime.JAVA) {
                // if function is not builtin, attempt to extract classloader from package file if it exists
                if (functionPackage == null && componentPackageFile != null) {
                    functionPackage =
                            new FunctionFilePackage(componentPackageFile, workerConfig.getNarExtractionDirectory(),
                                    workerConfig.getEnableClassloadingOfExternalFiles(), FunctionDefinition.class);
                    shouldCloseFunctionPackage = true;
                }

                if (functionPackage == null) {
                    throw new IllegalArgumentException("Function package is not provided");
                }

                FunctionConfigUtils.ExtractedFunctionDetails functionDetails = FunctionConfigUtils.validateJavaFunction(
                        functionConfig, functionPackage);
                return FunctionConfigUtils.convert(functionConfig, functionDetails);
            } else {
                FunctionConfigUtils.validateNonJavaFunction(functionConfig);
                return FunctionConfigUtils.convert(functionConfig);
            }
        } finally {
            if (shouldCloseFunctionPackage && functionPackage instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) functionPackage).close();
                } catch (Exception e) {
                    log.error("Failed to close function file", e);
                }
            }
        }
    }
}
