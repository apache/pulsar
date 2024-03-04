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
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.functions.utils.FunctionCommon.getStateNamespace;
import static org.apache.pulsar.functions.utils.FunctionCommon.getUniquePackageName;
import static org.apache.pulsar.functions.utils.FunctionCommon.isFunctionCodeBuiltin;
import static org.apache.pulsar.functions.worker.rest.RestUtils.throwUnavailableException;
import com.google.common.base.Utf8;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.api.kv.result.KeyValue;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.bookkeeper.clients.admin.StorageAdminClient;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.exceptions.NamespaceNotFoundException;
import org.apache.bookkeeper.clients.exceptions.StreamNotFoundException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationParameters;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.internal.FunctionsImpl;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsDataImpl;
import org.apache.pulsar.common.policies.data.FunctionStatsImpl;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.PackageLocationMetaData;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.apache.pulsar.functions.utils.ComponentTypeUtils;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.utils.FunctionMetaDataUtils;
import org.apache.pulsar.functions.utils.ValidatableFunctionPackage;
import org.apache.pulsar.functions.utils.functions.FunctionArchive;
import org.apache.pulsar.functions.utils.io.Connector;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.FunctionRuntimeInfo;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.PulsarWorkerService;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.apache.pulsar.functions.worker.service.api.Component;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

@Slf4j
public abstract class ComponentImpl implements Component<PulsarWorkerService> {

    private final AtomicReference<StorageClient> storageClient = new AtomicReference<>();
    protected final Supplier<PulsarWorkerService> workerServiceSupplier;
    protected final Function.FunctionDetails.ComponentType componentType;

    public ComponentImpl(Supplier<PulsarWorkerService> workerServiceSupplier,
                         Function.FunctionDetails.ComponentType componentType) {
        this.workerServiceSupplier = workerServiceSupplier;
        this.componentType = componentType;
    }

    protected abstract class GetStatus<X, T> {

        public abstract T notScheduledInstance();

        public abstract T fromFunctionStatusProto(InstanceCommunication.FunctionStatus status,
                                                  String assignedWorkerId);

        public abstract T notRunning(String assignedWorkerId, String error);

        public T getComponentInstanceStatus(final String tenant,
                                            final String namespace,
                                            final String name,
                                            final int instanceId,
                                            final URI uri) {

            Function.Assignment assignment;
            if (worker().getFunctionRuntimeManager().getRuntimeFactory().externallyManaged()) {
                assignment = worker().getFunctionRuntimeManager().findFunctionAssignment(tenant, namespace, name, -1);
            } else {
                assignment = worker().getFunctionRuntimeManager()
                        .findFunctionAssignment(tenant, namespace, name, instanceId);
            }

            if (assignment == null) {
                return notScheduledInstance();
            }

            final String assignedWorkerId = assignment.getWorkerId();
            final String workerId = worker().getWorkerConfig().getWorkerId();

            // If I am running worker
            if (assignedWorkerId.equals(workerId)) {
                FunctionRuntimeInfo functionRuntimeInfo = worker().getFunctionRuntimeManager().getFunctionRuntimeInfo(
                        FunctionCommon.getFullyQualifiedInstanceId(assignment.getInstance()));
                if (functionRuntimeInfo == null) {
                    return notRunning(assignedWorkerId, "");
                }
                RuntimeSpawner runtimeSpawner = functionRuntimeInfo.getRuntimeSpawner();

                if (runtimeSpawner != null) {
                    try {
                        return fromFunctionStatusProto(
                                functionRuntimeInfo.getRuntimeSpawner().getFunctionStatus(instanceId).get(),
                                assignedWorkerId);
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    String message = functionRuntimeInfo.getStartupException() != null
                            ? functionRuntimeInfo.getStartupException().getMessage() : "";
                    return notRunning(assignedWorkerId, message);
                }
            } else {
                // query other worker

                List<WorkerInfo> workerInfoList = worker().getMembershipManager().getCurrentMembership();
                WorkerInfo workerInfo = null;
                for (WorkerInfo entry : workerInfoList) {
                    if (assignment.getWorkerId().equals(entry.getWorkerId())) {
                        workerInfo = entry;
                    }
                }
                if (workerInfo == null) {
                    return notScheduledInstance();
                }

                if (uri == null) {
                    throw new WebApplicationException(
                            Response.serverError().status(Status.INTERNAL_SERVER_ERROR).build());
                } else {
                    URI redirect =
                            UriBuilder.fromUri(uri).host(workerInfo.getWorkerHostname()).port(workerInfo.getPort())
                                    .build();
                    throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                }
            }
        }

        public abstract X getStatus(String tenant,
                                    String namespace,
                                    String name,
                                    Collection<Function.Assignment> assignments,
                                    URI uri) throws PulsarAdminException;

        public abstract X getStatusExternal(String tenant,
                                            String namespace,
                                            String name,
                                            int parallelism);

        public abstract X emptyStatus(int parallelism);

        public X getComponentStatus(final String tenant,
                                    final String namespace,
                                    final String name,
                                    final URI uri) {

            Function.FunctionMetaData functionMetaData =
                    worker().getFunctionMetaDataManager().getFunctionMetaData(tenant, namespace, name);

            Collection<Function.Assignment> assignments =
                    worker().getFunctionRuntimeManager().findFunctionAssignments(tenant, namespace, name);

            // TODO refactor the code for externally managed.
            if (worker().getFunctionRuntimeManager().getRuntimeFactory().externallyManaged()) {
                Function.Assignment assignment = assignments.iterator().next();
                boolean isOwner = worker().getWorkerConfig().getWorkerId().equals(assignment.getWorkerId());
                if (isOwner) {
                    return getStatusExternal(tenant, namespace, name,
                            functionMetaData.getFunctionDetails().getParallelism());
                } else {

                    // find the hostname/port of the worker who is the owner

                    List<WorkerInfo> workerInfoList = worker().getMembershipManager().getCurrentMembership();
                    WorkerInfo workerInfo = null;
                    for (WorkerInfo entry : workerInfoList) {
                        if (assignment.getWorkerId().equals(entry.getWorkerId())) {
                            workerInfo = entry;
                        }
                    }
                    if (workerInfo == null) {
                        return emptyStatus(functionMetaData.getFunctionDetails().getParallelism());
                    }

                    if (uri == null) {
                        throw new WebApplicationException(
                                Response.serverError().status(Status.INTERNAL_SERVER_ERROR).build());
                    } else {
                        URI redirect =
                                UriBuilder.fromUri(uri).host(workerInfo.getWorkerHostname()).port(workerInfo.getPort())
                                        .build();
                        throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                    }
                }
            } else {
                try {
                    return getStatus(tenant, namespace, name, assignments, uri);
                } catch (PulsarAdminException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public PulsarWorkerService worker() {
        try {
            return checkNotNull(workerServiceSupplier.get());
        } catch (Throwable t) {
            log.info("Failed to get worker service", t);
            throw t;
        }
    }

    boolean isWorkerServiceAvailable() {
        WorkerService workerService = workerServiceSupplier.get();
        if (workerService == null) {
            return false;
        }
        return workerService.isInitialized();
    }

    PackageLocationMetaData.Builder getFunctionPackageLocation(final FunctionMetaData functionMetaData,
                                                               final String functionPkgUrl,
                                                               final FormDataContentDisposition fileDetail,
                                                               final File uploadedInputStreamAsFile) throws Exception {
        FunctionDetails functionDetails = functionMetaData.getFunctionDetails();
        String tenant = functionDetails.getTenant();
        String namespace = functionDetails.getNamespace();
        String componentName = functionDetails.getName();
        PackageLocationMetaData.Builder packageLocationMetaDataBuilder = PackageLocationMetaData.newBuilder();
        boolean isBuiltin = isFunctionCodeBuiltin(functionDetails);
        boolean isPkgUrlProvided = isNotBlank(functionPkgUrl);
        if (worker().getFunctionRuntimeManager().getRuntimeFactory().externallyManaged()) {
            // For externally managed schedulers, the pkgUrl/builtin stuff can be copied to bk
            // if the function worker image does not include connectors
            if (isBuiltin) {
                if (worker().getWorkerConfig().getUploadBuiltinSinksSources()) {
                    File component;
                    String archiveName;
                    switch (componentType) {
                        case SOURCE:
                            archiveName = functionDetails.getSource().getBuiltin();
                            component = worker().getConnectorsManager().getSourceArchive(archiveName).toFile();
                            break;
                        case SINK:
                            archiveName = functionDetails.getSink().getBuiltin();
                            component = worker().getConnectorsManager().getSinkArchive(archiveName).toFile();
                            break;
                        default:
                            archiveName = functionDetails.getBuiltin();
                            component = worker().getFunctionsManager().getFunctionArchive(archiveName).toFile();
                            break;
                        }
                    packageLocationMetaDataBuilder.setPackagePath(createPackagePath(tenant, namespace, componentName,
                            component.getName()));
                    packageLocationMetaDataBuilder.setOriginalFileName(component.getName());
                    WorkerUtils.uploadFileToBookkeeper(packageLocationMetaDataBuilder.getPackagePath(),
                                component, worker().getDlogNamespace());
                    log.info("Uploading {} package to {}", ComponentTypeUtils.toString(componentType),
                            packageLocationMetaDataBuilder.getPackagePath());
                } else {
                    log.info("Skipping upload for the built-in package {}", ComponentTypeUtils.toString(componentType));
                    packageLocationMetaDataBuilder
                            .setPackagePath("builtin://" + getFunctionCodeBuiltin(functionDetails));
                }
            } else if (isPkgUrlProvided) {
                packageLocationMetaDataBuilder.setOriginalFileName(uploadedInputStreamAsFile.getName());
                packageLocationMetaDataBuilder.setPackagePath(createPackagePath(tenant, namespace, componentName,
                        uploadedInputStreamAsFile.getName()));
                WorkerUtils.uploadFileToBookkeeper(packageLocationMetaDataBuilder.getPackagePath(),
                        uploadedInputStreamAsFile, worker().getDlogNamespace());
                log.info("Uploading {} package to {}", ComponentTypeUtils.toString(componentType),
                        packageLocationMetaDataBuilder.getPackagePath());
            } else if (functionMetaData.getPackageLocation().getPackagePath().startsWith(Utils.HTTP)
                    || functionMetaData.getPackageLocation().getPackagePath().startsWith(Utils.FILE)) {
                String fileName =
                        new File(new URL(functionMetaData.getPackageLocation().getPackagePath()).toURI()).getName();
                packageLocationMetaDataBuilder.setOriginalFileName(fileName);
                packageLocationMetaDataBuilder.setPackagePath(createPackagePath(tenant, namespace, componentName,
                        fileName));
                WorkerUtils.uploadFileToBookkeeper(packageLocationMetaDataBuilder.getPackagePath(),
                        uploadedInputStreamAsFile, worker().getDlogNamespace());
                log.info("Uploading {} package to {}", ComponentTypeUtils.toString(componentType),
                        packageLocationMetaDataBuilder.getPackagePath());
            } else {
                packageLocationMetaDataBuilder.setOriginalFileName(fileDetail.getFileName());
                packageLocationMetaDataBuilder.setPackagePath(createPackagePath(tenant, namespace, componentName,
                        fileDetail.getFileName()));
                WorkerUtils.uploadFileToBookkeeper(packageLocationMetaDataBuilder.getPackagePath(),
                        uploadedInputStreamAsFile, worker().getDlogNamespace());
                log.info("Uploading {} package to {}", ComponentTypeUtils.toString(componentType),
                        packageLocationMetaDataBuilder.getPackagePath());
            }
        } else {
            // For pulsar managed schedulers, the pkgUrl/builtin stuff should be copied to bk
            if (isBuiltin) {
                packageLocationMetaDataBuilder.setPackagePath("builtin://" + getFunctionCodeBuiltin(functionDetails));
            } else if (isPkgUrlProvided) {
                packageLocationMetaDataBuilder.setPackagePath(functionPkgUrl);
            } else if (functionMetaData.getPackageLocation().getPackagePath().startsWith(Utils.HTTP)
                    || functionMetaData.getPackageLocation().getPackagePath().startsWith(Utils.FILE)) {
                packageLocationMetaDataBuilder.setPackagePath(functionMetaData.getPackageLocation().getPackagePath());
            } else {
                packageLocationMetaDataBuilder.setOriginalFileName(fileDetail.getFileName());
                packageLocationMetaDataBuilder.setPackagePath(createPackagePath(tenant, namespace, componentName,
                                fileDetail.getFileName()));
                WorkerUtils.uploadFileToBookkeeper(packageLocationMetaDataBuilder.getPackagePath(),
                        uploadedInputStreamAsFile, worker().getDlogNamespace());
                log.info("Uploading {} package to {}", ComponentTypeUtils.toString(componentType),
                        packageLocationMetaDataBuilder.getPackagePath());
            }
        }
        return packageLocationMetaDataBuilder;
    }

    private void deleteStatestoreTableAsync(String namespace, String table) {
        StorageAdminClient adminClient = worker().getStateStoreAdminClient();
        if (adminClient != null) {
            adminClient.deleteStream(namespace, table).whenComplete((res, throwable) -> {
                if ((throwable == null && res)
                        || ((throwable instanceof NamespaceNotFoundException
                        || throwable instanceof StreamNotFoundException))) {
                    log.info("{}/{} table deleted successfully", namespace, table);
                } else {
                    if (throwable != null) {
                        log.error("{}/{} table deletion failed {}  but moving on", namespace, table, throwable);
                    } else {
                        log.error("{}/{} table deletion failed but moving on", namespace, table);
                    }
                }
            });
        }
    }

    @Override
    public void deregisterFunction(final String tenant,
                                   final String namespace,
                                   final String componentName,
                                   final AuthenticationParameters authParams) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        throwRestExceptionIfUnauthorizedForNamespace(tenant, namespace, componentName, "deregister",
                authParams);

        // validate parameters
        try {
            validateDeregisterRequestParams(tenant, namespace, componentName, componentType);
        } catch (IllegalArgumentException e) {
            log.error("Invalid deregister {} request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant,
                    namespace, componentName, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.error("{} to deregister does not exist @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant,
                    namespace, componentName);
            throw new RestException(Status.NOT_FOUND,
                    String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), componentName));
        }
        FunctionMetaData functionMetaData =
                functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);

        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName,
                    ComponentTypeUtils.toString(componentType));
            throw new RestException(Status.NOT_FOUND,
                    String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), componentName));
        }

        FunctionMetaData newVersionedMetaData =
                FunctionMetaDataUtils.incrMetadataVersion(functionMetaData, functionMetaData);
        internalProcessFunctionRequest(newVersionedMetaData.getFunctionDetails().getTenant(),
                newVersionedMetaData.getFunctionDetails().getNamespace(),
                newVersionedMetaData.getFunctionDetails().getName(),
                newVersionedMetaData, true,
                String.format("Error deleting %s @ /%s/%s/%s",
                        ComponentTypeUtils.toString(componentType), tenant, namespace, componentName));

        // clean up component files stored in BK
        String functionPackagePath = functionMetaData.getPackageLocation().getPackagePath();
        if (!functionPackagePath.startsWith(Utils.HTTP)
                && !functionPackagePath.startsWith(Utils.FILE)
                && !functionPackagePath.startsWith(Utils.BUILTIN)) {
            try {
                WorkerUtils.deleteFromBookkeeper(worker().getDlogNamespace(), functionPackagePath);
            } catch (IOException e) {
                log.error("{}/{}/{} Failed to cleanup package in BK with path {}", tenant, namespace, componentName,
                        functionPackagePath, e);
            }

        }

        deleteStatestoreTableAsync(getStateNamespace(tenant, namespace), componentName);
    }

    @Override
    public FunctionConfig getFunctionInfo(final String tenant,
                                          final String namespace,
                                          final String componentName,
                                          final AuthenticationParameters authParams) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        throwRestExceptionIfUnauthorizedForNamespace(tenant, namespace, componentName, "get",
                authParams);

        // validate parameters
        try {
            validateGetFunctionRequestParams(tenant, namespace, componentName, componentType);
        } catch (IllegalArgumentException e) {
            log.error("Invalid get {} request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant,
                    namespace, componentName, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.error("{} does not exist @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace,
                    componentName);
            throw new RestException(Status.NOT_FOUND,
                    String.format(ComponentTypeUtils.toString(componentType) + " %s doesn't exist", componentName));
        }
        FunctionMetaData functionMetaData =
                functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName,
                    ComponentTypeUtils.toString(componentType));
            throw new RestException(Status.NOT_FOUND,
                    String.format(ComponentTypeUtils.toString(componentType) + " %s doesn't exist", componentName));
        }
        FunctionConfig config = FunctionConfigUtils.convertFromDetails(functionMetaData.getFunctionDetails());
        return config;
    }

    @Override
    public void stopFunctionInstance(final String tenant,
                                     final String namespace,
                                     final String componentName,
                                     final String instanceId,
                                     final URI uri,
                                     final AuthenticationParameters authParams) {
        changeFunctionInstanceStatus(tenant, namespace, componentName, instanceId, false, uri, authParams);
    }

    @Override
    public void startFunctionInstance(final String tenant,
                                      final String namespace,
                                      final String componentName,
                                      final String instanceId,
                                      final URI uri,
                                      final AuthenticationParameters authParams) {
        changeFunctionInstanceStatus(tenant, namespace, componentName, instanceId, true, uri, authParams);
    }

    @Deprecated
    public void changeFunctionInstanceStatus(final String tenant,
                                             final String namespace,
                                             final String componentName,
                                             final String instanceId,
                                             final boolean start,
                                             final URI uri,
                                             final String clientRole,
                                             final AuthenticationDataSource clientAuthenticationDataHttps) {
        AuthenticationParameters authParams = AuthenticationParameters.builder()
                .clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps)
                .build();
        changeFunctionInstanceStatus(tenant, namespace, componentName, instanceId, start, uri, authParams);
    }

    public void changeFunctionInstanceStatus(final String tenant,
                                             final String namespace,
                                             final String componentName,
                                             final String instanceId,
                                             final boolean start,
                                             final URI uri,
                                             final AuthenticationParameters authParams) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        throwRestExceptionIfUnauthorizedForNamespace(tenant, namespace, componentName, "start/stop",
                authParams);

        // validate parameters
        try {
            validateGetFunctionInstanceRequestParams(tenant, namespace, componentName, componentType, instanceId);
        } catch (IllegalArgumentException e) {
            log.error("Invalid start/stop {} request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant,
                    namespace, componentName, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.error("{} does not exist @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace,
                    componentName);
            throw new RestException(Status.NOT_FOUND,
                    String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), componentName));
        }

        FunctionMetaData functionMetaData =
                functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName,
                    ComponentTypeUtils.toString(componentType));
            throw new RestException(Status.NOT_FOUND,
                    String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), componentName));
        }

        if (!FunctionMetaDataUtils.canChangeState(functionMetaData, Integer.parseInt(instanceId),
                start ? Function.FunctionState.RUNNING : Function.FunctionState.STOPPED)) {
            log.error("Operation not permitted on {}/{}/{}", tenant, namespace, componentName);
            throw new RestException(Status.BAD_REQUEST, "Operation not permitted");
        }

        FunctionMetaData newFunctionMetaData = FunctionMetaDataUtils
                .changeFunctionInstanceStatus(functionMetaData, Integer.parseInt(instanceId), start);
        internalProcessFunctionRequest(tenant, namespace, componentName, newFunctionMetaData, false,
                String.format("Failed to start/stop %s: %s/%s/%s/%s", ComponentTypeUtils.toString(componentType),
                        tenant, namespace, componentName, instanceId));
    }

    @Override
    public void restartFunctionInstance(final String tenant,
                                        final String namespace,
                                        final String componentName,
                                        final String instanceId,
                                        final URI uri,
                                        final AuthenticationParameters authParams) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        throwRestExceptionIfUnauthorizedForNamespace(tenant, namespace, componentName, "restart",
                authParams);

        // validate parameters
        try {
            validateGetFunctionInstanceRequestParams(tenant, namespace, componentName, componentType, instanceId);
        } catch (IllegalArgumentException e) {
            log.error("Invalid restart {} request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant,
                    namespace, componentName, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.error("{} does not exist @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace,
                    componentName);
            throw new RestException(Status.NOT_FOUND,
                    String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), componentName));
        }

        FunctionMetaData functionMetaData =
                functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName,
                    ComponentTypeUtils.toString(componentType));
            throw new RestException(Status.NOT_FOUND,
                    String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), componentName));
        }

        FunctionRuntimeManager functionRuntimeManager = worker().getFunctionRuntimeManager();
        try {
            functionRuntimeManager.restartFunctionInstance(tenant, namespace, componentName,
                    Integer.parseInt(instanceId), uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("Failed to restart {}: {}/{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant,
                    namespace, componentName, instanceId, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public void stopFunctionInstances(final String tenant,
                                      final String namespace,
                                      final String componentName,
                                      final AuthenticationParameters authParams) {
        changeFunctionStatusAllInstances(tenant, namespace, componentName, false, authParams);
    }

    @Override
    public void startFunctionInstances(final String tenant,
                                       final String namespace,
                                       final String componentName,
                                       final AuthenticationParameters authParams) {
        changeFunctionStatusAllInstances(tenant, namespace, componentName, true, authParams);
    }

    @Deprecated
    public void changeFunctionStatusAllInstances(final String tenant,
                                                 final String namespace,
                                                 final String componentName,
                                                 final boolean start,
                                                 final String clientRole,
                                                 final AuthenticationDataSource clientAuthenticationDataHttps) {
        AuthenticationParameters authParams = AuthenticationParameters.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        changeFunctionStatusAllInstances(tenant, namespace, componentName, start, authParams);
    }

    public void changeFunctionStatusAllInstances(final String tenant,
                                                 final String namespace,
                                                 final String componentName,
                                                 final boolean start,
                                                 final AuthenticationParameters authParams) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        throwRestExceptionIfUnauthorizedForNamespace(tenant, namespace, componentName, "start/stop",
                authParams);

        // validate parameters
        try {
            validateGetFunctionRequestParams(tenant, namespace, componentName, componentType);
        } catch (IllegalArgumentException e) {
            log.error("Invalid start/stop {} request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant,
                    namespace, componentName, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.warn("{} in stopFunctionInstances does not exist @ /{}/{}/{}",
                    ComponentTypeUtils.toString(componentType), tenant, namespace, componentName);
            throw new RestException(Status.NOT_FOUND,
                    String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), componentName));
        }

        FunctionMetaData functionMetaData =
                functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName,
                    ComponentTypeUtils.toString(componentType));
            throw new RestException(Status.NOT_FOUND,
                    String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), componentName));
        }

        if (!FunctionMetaDataUtils.canChangeState(functionMetaData, -1,
                start ? Function.FunctionState.RUNNING : Function.FunctionState.STOPPED)) {
            log.error("Operation not permitted on {}/{}/{}", tenant, namespace, componentName);
            throw new RestException(Status.BAD_REQUEST, "Operation not permitted");
        }

        FunctionMetaData newFunctionMetaData =
                FunctionMetaDataUtils.changeFunctionInstanceStatus(functionMetaData, -1, start);
        internalProcessFunctionRequest(tenant, namespace, componentName, newFunctionMetaData, false,
                String.format("Failed to start/stop %s: %s/%s/%s", ComponentTypeUtils.toString(componentType), tenant,
                        namespace, componentName));
    }

    @Override
    public void restartFunctionInstances(final String tenant,
                                         final String namespace,
                                         final String componentName,
                                         final AuthenticationParameters authParams) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        throwRestExceptionIfUnauthorizedForNamespace(tenant, namespace, componentName, "restart",
                authParams);

        // validate parameters
        try {
            validateGetFunctionRequestParams(tenant, namespace, componentName, componentType);
        } catch (IllegalArgumentException e) {
            log.error("Invalid restart {} request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant,
                    namespace, componentName, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.warn("{} in stopFunctionInstances does not exist @ /{}/{}/{}",
                    ComponentTypeUtils.toString(componentType), tenant, namespace, componentName);
            throw new RestException(Status.NOT_FOUND,
                    String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), componentName));
        }

        FunctionMetaData functionMetaData =
                functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName,
                    ComponentTypeUtils.toString(componentType));
            throw new RestException(Status.NOT_FOUND,
                    String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), componentName));
        }

        FunctionRuntimeManager functionRuntimeManager = worker().getFunctionRuntimeManager();
        try {
            functionRuntimeManager.restartFunctionInstances(tenant, namespace, componentName);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("Failed to restart {}: {}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace,
                    componentName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public FunctionStatsImpl getFunctionStats(final String tenant,
                                              final String namespace,
                                              final String componentName,
                                              final URI uri,
                                              final AuthenticationParameters authParams) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        throwRestExceptionIfUnauthorizedForNamespace(tenant, namespace, componentName, "get stats for",
                authParams);

        // validate parameters
        try {
            validateGetFunctionRequestParams(tenant, namespace, componentName, componentType);
        } catch (IllegalArgumentException e) {
            log.error("Invalid get {} Stats request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant,
                    namespace, componentName, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.warn("{} in get {} Stats does not exist @ /{}/{}/{}", ComponentTypeUtils.toString(componentType),
                    componentType, tenant, namespace, componentName);
            throw new RestException(Status.NOT_FOUND,
                    String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), componentName));
        }

        FunctionMetaData functionMetaData =
                functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName,
                    ComponentTypeUtils.toString(componentType));
            throw new RestException(Status.NOT_FOUND,
                    String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), componentName));
        }

        FunctionRuntimeManager functionRuntimeManager = worker().getFunctionRuntimeManager();
        FunctionStatsImpl functionStats;
        try {
            functionStats = functionRuntimeManager.getFunctionStats(tenant, namespace, componentName, uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Stats", tenant, namespace, componentName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        return functionStats;
    }

    @Override
    public FunctionInstanceStatsDataImpl
    getFunctionsInstanceStats(final String tenant,
                           final String namespace,
                           final String componentName,
                           final String instanceId,
                           final URI uri,
                           final AuthenticationParameters authParams) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        throwRestExceptionIfUnauthorizedForNamespace(tenant, namespace, componentName, "get stats for",
                authParams);

        // validate parameters
        try {
            validateGetFunctionInstanceRequestParams(tenant, namespace, componentName, componentType, instanceId);
        } catch (IllegalArgumentException e) {
            log.error("Invalid get {} Stats request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant,
                    namespace, componentName, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());

        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.warn("{} in get {} Stats does not exist @ /{}/{}/{}", ComponentTypeUtils.toString(componentType),
                    componentType, tenant, namespace, componentName);
            throw new RestException(Status.NOT_FOUND,
                    String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), componentName));
        }
        FunctionMetaData functionMetaData =
                functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName,
                    ComponentTypeUtils.toString(componentType));
            throw new RestException(Status.NOT_FOUND,
                    String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), componentName));

        }
        int instanceIdInt = Integer.parseInt(instanceId);
        if (instanceIdInt < 0 || instanceIdInt >= functionMetaData.getFunctionDetails().getParallelism()) {
            log.error("instanceId in get {} Stats out of bounds @ /{}/{}/{}",
                    ComponentTypeUtils.toString(componentType), tenant, namespace, componentName);
            throw new RestException(Status.BAD_REQUEST,
                    String.format("%s %s doesn't have instance with id %s", ComponentTypeUtils.toString(componentType),
                            componentName, instanceId));
        }

        FunctionRuntimeManager functionRuntimeManager = worker().getFunctionRuntimeManager();
        FunctionInstanceStatsDataImpl functionInstanceStatsData;
        try {
            functionInstanceStatsData =
                    functionRuntimeManager.getFunctionInstanceStats(tenant, namespace, componentName,
                            Integer.parseInt(instanceId), uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Stats", tenant, namespace, componentName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        return functionInstanceStatsData;
    }

    @Override
    public List<String> listFunctions(final String tenant,
                                      final String namespace,
                                      final AuthenticationParameters authParams) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        throwRestExceptionIfUnauthorizedForNamespace(tenant, namespace, null, "list", authParams);

        // validate parameters
        try {
            validateListFunctionRequestParams(tenant, namespace);
        } catch (IllegalArgumentException e) {
            log.error("Invalid list {} request @ /{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace,
                    e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();

        Collection<FunctionMetaData> functionStateList = functionMetaDataManager.listFunctions(tenant, namespace);
        List<String> retVals = new LinkedList<>();
        for (FunctionMetaData functionMetaData : functionStateList) {
            if (InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
                retVals.add(functionMetaData.getFunctionDetails().getName());
            }
        }
        return retVals;
    }

    void updateRequest(FunctionMetaData existingFunctionMetaData, final FunctionMetaData functionMetaData) {
        FunctionMetaData updatedVersionMetaData =
                FunctionMetaDataUtils.incrMetadataVersion(existingFunctionMetaData, functionMetaData);
        internalProcessFunctionRequest(updatedVersionMetaData.getFunctionDetails().getTenant(),
                updatedVersionMetaData.getFunctionDetails().getNamespace(),
                updatedVersionMetaData.getFunctionDetails().getName(),
                updatedVersionMetaData, false, "Update Failed");
    }

    @Override
    public List<ConnectorDefinition> getListOfConnectors() {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        return this.worker().getConnectorsManager().getConnectorDefinitions();
    }

    @Override
    public void reloadConnectors(AuthenticationParameters authParams) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }
        if (worker().getWorkerConfig().isAuthorizationEnabled()) {
            // Only superuser has permission to do this operation.
            if (!isSuperUser(authParams)) {
                throw new RestException(Status.UNAUTHORIZED, "This operation requires super-user access");
            }
        }
        try {
            this.worker().getConnectorsManager().reloadConnectors(worker().getWorkerConfig());
        } catch (IOException e) {
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public String triggerFunction(final String tenant,
                                  final String namespace,
                                  final String functionName,
                                  final String input,
                                  final InputStream uploadedInputStream,
                                  final String topic,
                                  final AuthenticationParameters authParams) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        throwRestExceptionIfUnauthorizedForNamespace(tenant, namespace, functionName, "trigger", authParams);

        // validate parameters
        try {
            validateTriggerRequestParams(tenant, namespace, functionName, topic, input, uploadedInputStream);
        } catch (IllegalArgumentException e) {
            log.error("Invalid trigger function request @ /{}/{}/{}", tenant, namespace, functionName, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, functionName)) {
            log.warn("Function in trigger function does not exist @ /{}/{}/{}", tenant, namespace, functionName);
            throw new RestException(Status.NOT_FOUND, String.format("Function %s doesn't exist", functionName));
        }

        FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace,
                functionName);

        String inputTopicToWrite;
        if (topic != null) {
            inputTopicToWrite = topic;
        } else if (functionMetaData.getFunctionDetails().getSource().getInputSpecsCount() == 1) {
            inputTopicToWrite = functionMetaData.getFunctionDetails().getSource().getInputSpecsMap()
                    .keySet().iterator().next();
        } else {
            log.error("Function in trigger function has more than 1 input topics @ /{}/{}/{}", tenant, namespace,
                    functionName);
            throw new RestException(Status.BAD_REQUEST, "Function in trigger function has more than 1 input topics");
        }
        if (functionMetaData.getFunctionDetails().getSource().getInputSpecsCount() == 0
                || !functionMetaData.getFunctionDetails().getSource().getInputSpecsMap()
                .containsKey(inputTopicToWrite)) {
            log.error("Function in trigger function has unidentified topic @ /{}/{}/{} {}", tenant, namespace,
                    functionName, inputTopicToWrite);
            throw new RestException(Status.BAD_REQUEST, "Function in trigger function has unidentified topic");
        }
        try {
            worker().getBrokerAdmin().topics().getSubscriptions(inputTopicToWrite);
        } catch (PulsarAdminException e) {
            log.error("Function in trigger function is not ready @ /{}/{}/{}", tenant, namespace, functionName);
            throw new RestException(Status.BAD_REQUEST, "Function in trigger function is not ready");
        }
        String outputTopic = functionMetaData.getFunctionDetails().getSink().getTopic();
        Reader<byte[]> reader = null;
        Producer<byte[]> producer = null;
        try {
            if (outputTopic != null && !outputTopic.isEmpty()) {
                reader = worker().getClient().newReader()
                        .topic(outputTopic)
                        .startMessageId(MessageId.latest)
                        .readerName(worker().getWorkerConfig().getWorkerId() + "-trigger-"
                                + FunctionCommon.getFullyQualifiedName(tenant, namespace, functionName))
                        .create();
            }
            producer = worker().getClient().newProducer(Schema.AUTO_PRODUCE_BYTES())
                    .topic(inputTopicToWrite)
                    .producerName(worker().getWorkerConfig().getWorkerId() + "-trigger-"
                            + FunctionCommon.getFullyQualifiedName(tenant, namespace, functionName))
                    .create();
            byte[] targetArray;
            if (uploadedInputStream != null) {
                targetArray = new byte[uploadedInputStream.available()];
                uploadedInputStream.read(targetArray);
            } else {
                targetArray = input.getBytes();
            }
            MessageId msgId = producer.send(targetArray);
            if (reader == null) {
                return null;
            }
            long curTime = System.currentTimeMillis();
            long maxTime = curTime + 1000;
            while (curTime < maxTime) {
                Message msg = reader.readNext(10000, TimeUnit.MILLISECONDS);
                if (msg == null) {
                    break;
                }
                if (msg.getProperties().containsKey("__pfn_input_msg_id__")
                        && msg.getProperties().containsKey("__pfn_input_topic__")) {
                    MessageId newMsgId = MessageId.fromByteArray(
                            Base64.getDecoder().decode((String) msg.getProperties().get("__pfn_input_msg_id__")));

                    if (msgId.equals(newMsgId)
                            && msg.getProperties().get("__pfn_input_topic__")
                            .equals(TopicName.get(inputTopicToWrite).toString())) {
                        return new String(msg.getData());
                    }
                }
                curTime = System.currentTimeMillis();
            }
            throw new RestException(Status.REQUEST_TIMEOUT, "Request Timed Out");
        } catch (SchemaSerializationException e) {
            throw new RestException(Status.BAD_REQUEST, String.format(
                    "Failed to serialize input with error: %s. Please check"
                            + "if input data conforms with the schema of the input topic.",
                    e.getMessage()));
        } catch (IOException e) {
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        } finally {
            if (reader != null) {
                reader.closeAsync();
            }
            if (producer != null) {
                producer.closeAsync();
            }
        }
    }

    @Override
    public FunctionState getFunctionState(final String tenant,
                                          final String namespace,
                                          final String functionName,
                                          final String key,
                                          final AuthenticationParameters authParams) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        throwRestExceptionIfUnauthorizedForNamespace(tenant, namespace, functionName, "get state for",
                authParams);

        if (null == worker().getStateStoreAdminClient()) {
            throwStateStoreUnvailableResponse();
        }

        // validate parameters
        try {
            validateFunctionStateParams(tenant, namespace, functionName, key);
        } catch (IllegalArgumentException e) {
            log.error("Invalid getFunctionState request @ /{}/{}/{}/{}",
                    tenant, namespace, functionName, key, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        String tableNs = getStateNamespace(tenant, namespace);
        String tableName = functionName;

        String stateStorageServiceUrl = worker().getWorkerConfig().getStateStorageServiceUrl();

        if (storageClient.get() == null) {
            storageClient.compareAndSet(null, StorageClientBuilder.newBuilder()
                    .withSettings(StorageClientSettings.newBuilder()
                            .serviceUri(stateStorageServiceUrl)
                            .clientName("functions-admin")
                            .build())
                    .withNamespace(tableNs)
                    .build());
        }

        FunctionState value;
        try (Table<ByteBuf, ByteBuf> table = result(storageClient.get().openTable(tableName))) {
            try (KeyValue<ByteBuf, ByteBuf> kv = result(table.getKv(Unpooled.wrappedBuffer(key.getBytes(UTF_8))))) {
                if (null == kv) {
                    throw new RestException(Status.NOT_FOUND, "key '" + key + "' doesn't exist.");
                } else {
                    if (kv.isNumber()) {
                        value = new FunctionState(key, null, null, kv.numberValue(), kv.version());
                    } else {
                        byte[] bytes = ByteBufUtil.getBytes(kv.value());
                        if (Utf8.isWellFormed(bytes)) {
                            value = new FunctionState(key, new String(bytes, UTF_8),
                                    null, null, kv.version());
                        } else {
                            value = new FunctionState(
                                    key, null, bytes, null, kv.version());
                        }
                    }
                }
            }
        } catch (RestException e) {
            throw e;
        } catch (org.apache.bookkeeper.clients.exceptions.NamespaceNotFoundException | StreamNotFoundException e) {
            log.debug("State not found while processing getFunctionState request @ /{}/{}/{}/{}",
                    tenant, namespace, functionName, key, e);
            throw new RestException(Status.NOT_FOUND, e.getMessage());
        } catch (Exception e) {
            log.error("Error while getFunctionState request @ /{}/{}/{}/{}",
                    tenant, namespace, functionName, key, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
        return value;
    }

    @Override
    public void putFunctionState(final String tenant,
                                 final String namespace,
                                 final String functionName,
                                 final String key,
                                 final FunctionState state,
                                 final AuthenticationParameters authParams) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (null == worker().getStateStoreAdminClient()) {
            throwStateStoreUnvailableResponse();
        }

        throwRestExceptionIfUnauthorizedForNamespace(tenant, namespace, functionName, "put state for",
                authParams);

        if (!key.equals(state.getKey())) {
            log.error("{}/{}/{} Bad putFunction Request, path key doesn't match key in json", tenant, namespace,
                    functionName);
            throw new RestException(Status.BAD_REQUEST, "Path key doesn't match key in json");
        }
        if (state.getStringValue() == null && state.getByteValue() == null) {
            throw new RestException(Status.BAD_REQUEST, "Setting Counter values not supported in put state");
        }

        // validate parameters
        try {
            validateFunctionStateParams(tenant, namespace, functionName, key);
        } catch (IllegalArgumentException e) {
            log.error("Invalid putFunctionState request @ /{}/{}/{}/{}",
                    tenant, namespace, functionName, key, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        String tableNs = getStateNamespace(tenant, namespace);
        String tableName = functionName;

        String stateStorageServiceUrl = worker().getWorkerConfig().getStateStorageServiceUrl();

        if (storageClient.get() == null) {
            storageClient.compareAndSet(null, StorageClientBuilder.newBuilder()
                    .withSettings(StorageClientSettings.newBuilder()
                            .serviceUri(stateStorageServiceUrl)
                            .clientName("functions-admin")
                            .build())
                    .withNamespace(tableNs)
                    .build());
        }

        ByteBuf value;
        if (!isEmpty(state.getStringValue())) {
            value = Unpooled.wrappedBuffer(state.getStringValue().getBytes());
        } else {
            value = Unpooled.wrappedBuffer(state.getByteValue());
        }
        try (Table<ByteBuf, ByteBuf> table = result(storageClient.get().openTable(tableName))) {
            result(table.put(Unpooled.wrappedBuffer(key.getBytes(UTF_8)), value));
        } catch (org.apache.bookkeeper.clients.exceptions.NamespaceNotFoundException
                | org.apache.bookkeeper.clients.exceptions.StreamNotFoundException e) {
            log.debug("State not found while processing putFunctionState request @ /{}/{}/{}/{}",
                    tenant, namespace, functionName, key, e);
            throw new RestException(Status.NOT_FOUND, e.getMessage());
        } catch (Exception e) {
            log.error("Error while putFunctionState request @ /{}/{}/{}/{}",
                    tenant, namespace, functionName, key, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public void uploadFunction(final InputStream uploadedInputStream, final String path,
                               AuthenticationParameters authParams) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (worker().getWorkerConfig().isAuthorizationEnabled() && !isSuperUser(authParams)) {
            throw new RestException(Status.UNAUTHORIZED, "Client is not authorized to perform operation");
        }

        // validate parameters
        try {
            if (uploadedInputStream == null || path == null) {
                throw new IllegalArgumentException("Function Package is not provided " + path);
            }
        } catch (IllegalArgumentException e) {
            log.error("Invalid upload function request @ /{}", path, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        // Upload to bookkeeper
        try {
            log.info("Uploading function package to {}", path);
            WorkerUtils.uploadToBookKeeper(worker().getDlogNamespace(), uploadedInputStream, path);
        } catch (IOException e) {
            log.error("Error uploading file {}", path, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public StreamingOutput downloadFunction(String tenant, String namespace, String componentName,
                                            AuthenticationParameters authParams) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        throwRestExceptionIfUnauthorizedForNamespace(tenant, namespace, componentName, "download package for",
                authParams);

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.error("{} does not exist @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace,
                    componentName);
            throw new RestException(Status.NOT_FOUND,
                    String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), componentName));
        }

        FunctionMetaData functionMetaData =
            functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        String pkgPath = functionMetaData.getPackageLocation().getPackagePath();

        FunctionDetails.ComponentType componentType =
            InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails());

        return getStreamingOutput(pkgPath, componentType);
    }

    private StreamingOutput getStreamingOutput(String pkgPath) {
        return getStreamingOutput(pkgPath, null);
    }

    private StreamingOutput getStreamingOutput(String pkgPath, FunctionDetails.ComponentType componentType) {
        return output -> {
            if (pkgPath.startsWith(Utils.HTTP)) {
                if (!worker().getPackageUrlValidator().isValidPackageUrl(componentType, pkgPath)) {
                    throw new IllegalArgumentException("Invalid package url: " + pkgPath);
                }
                URL url = URI.create(pkgPath).toURL();
                try (InputStream inputStream = url.openStream()) {
                    IOUtils.copy(inputStream, output);
                }
            } else if (pkgPath.startsWith(Utils.FILE)) {
                if (!worker().getPackageUrlValidator().isValidPackageUrl(componentType, pkgPath)) {
                    throw new IllegalArgumentException("Invalid package url: " + pkgPath);
                }
                URI url = URI.create(pkgPath);
                File file = new File(url.getPath());
                Files.copy(file.toPath(), output);
            } else if (pkgPath.startsWith(Utils.BUILTIN)
                    && !worker().getWorkerConfig().getUploadBuiltinSinksSources()) {
                Path narPath = getBuiltinArchivePath(pkgPath, componentType);
                log.info("Loading {} from {}", pkgPath, narPath);
                try (InputStream in = new FileInputStream(narPath.toString())) {
                    IOUtils.copy(in, output, 1024);
                    output.flush();
                }
            } else {
                WorkerUtils.downloadFromBookkeeper(worker().getDlogNamespace(), output, pkgPath);
            }
        };
    }

    private Path getBuiltinArchivePath(String pkgPath, FunctionDetails.ComponentType componentType) {
        String type = pkgPath.replaceFirst("^builtin://", "");
        if (!FunctionDetails.ComponentType.FUNCTION.equals(componentType)) {
            Connector connector = worker().getConnectorsManager().getConnector(type);
            if (connector != null) {
                return connector.getArchivePath();
            }
            if (componentType != null) {
                throw new IllegalStateException("Didn't find " + type + " in built-in connectors");
            }
        }
        FunctionArchive function = worker().getFunctionsManager().getFunction(type);
        if (function != null) {
            return function.getArchivePath();
        }
        if (componentType != null) {
            throw new IllegalStateException("Didn't find " + type + " in built-in functions");
        }
        throw new IllegalStateException("Didn't find " + type + " in built-in connectors or functions");
    }

    @Override
    public StreamingOutput downloadFunction(final String path, final AuthenticationParameters authParams) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (worker().getWorkerConfig().isAuthorizationEnabled()) {
            // to maintain backwards compatiblity but still have authorization
            String[] tokens = path.split("/");
            if (tokens.length == 4) {
                String tenant = tokens[0];
                String namespace = tokens[1];
                String componentName = tokens[2];

                throwRestExceptionIfUnauthorizedForNamespace(tenant, namespace, componentName, "download package for",
                        authParams);
            } else {
                if (!isSuperUser(authParams)) {
                    throw new RestException(Status.UNAUTHORIZED, "Client is not authorized to perform operation");
                }
            }
        }

        return getStreamingOutput(path);
    }

    private void validateListFunctionRequestParams(final String tenant, final String namespace)
            throws IllegalArgumentException {

        if (tenant == null) {
            throw new IllegalArgumentException("Tenant is not provided");
        }
        if (namespace == null) {
            throw new IllegalArgumentException("Namespace is not provided");
        }
    }

    protected void validateGetFunctionInstanceRequestParams(final String tenant,
                                                            final String namespace,
                                                            final String componentName,
                                                            final FunctionDetails.ComponentType componentType,
                                                            final String instanceId) throws IllegalArgumentException {
        validateGetFunctionRequestParams(tenant, namespace, componentName, componentType);
        if (instanceId == null) {
            throw new IllegalArgumentException(String.format("%s Instance Id is not provided", componentType));
        }
    }

    protected void validateGetFunctionRequestParams(String tenant, String namespace, String subject,
                                                    FunctionDetails.ComponentType componentType)
            throws IllegalArgumentException {

        if (tenant == null) {
            throw new IllegalArgumentException("Tenant is not provided");
        }
        if (namespace == null) {
            throw new IllegalArgumentException("Namespace is not provided");
        }
        if (subject == null) {
            throw new IllegalArgumentException(ComponentTypeUtils.toString(componentType) + " name is not provided");
        }
    }

    private void validateDeregisterRequestParams(String tenant, String namespace, String subject,
                                                 FunctionDetails.ComponentType componentType)
            throws IllegalArgumentException {

        if (tenant == null) {
            throw new IllegalArgumentException("Tenant is not provided");
        }
        if (namespace == null) {
            throw new IllegalArgumentException("Namespace is not provided");
        }
        if (subject == null) {
            throw new IllegalArgumentException(ComponentTypeUtils.toString(componentType) + " name is not provided");
        }
    }

    private void validateFunctionStateParams(final String tenant,
                                             final String namespace,
                                             final String functionName,
                                             final String key)
            throws IllegalArgumentException {

        if (tenant == null) {
            throw new IllegalArgumentException("Tenant is not provided");
        }
        if (namespace == null) {
            throw new IllegalArgumentException("Namespace is not provided");
        }
        if (functionName == null) {
            throw new IllegalArgumentException(ComponentTypeUtils.toString(componentType) + " name is not provided");
        }
        if (key == null) {
            throw new IllegalArgumentException("Key is not provided");
        }
    }

    private String getFunctionCodeBuiltin(FunctionDetails functionDetails) {
        if (functionDetails.hasSource()) {
            SourceSpec sourceSpec = functionDetails.getSource();
            if (!isEmpty(sourceSpec.getBuiltin())) {
                return sourceSpec.getBuiltin();
            }
        }

        if (functionDetails.hasSink()) {
            SinkSpec sinkSpec = functionDetails.getSink();
            if (!isEmpty(sinkSpec.getBuiltin())) {
                return sinkSpec.getBuiltin();
            }
        }

        if (!isEmpty(functionDetails.getBuiltin())) {
            return functionDetails.getBuiltin();
        }

        return null;
    }

    private void validateTriggerRequestParams(final String tenant,
                                              final String namespace,
                                              final String functionName,
                                              final String topic,
                                              final String input,
                                              final InputStream uploadedInputStream) {
        // Note : Checking topic is not required it can be null

        if (tenant == null) {
            throw new IllegalArgumentException("Tenant is not provided");
        }
        if (namespace == null) {
            throw new IllegalArgumentException("Namespace is not provided");
        }
        if (functionName == null) {
            throw new IllegalArgumentException("Function name is not provided");
        }
        if (uploadedInputStream == null && input == null) {
            throw new IllegalArgumentException("Trigger Data is not provided");
        }
    }

    private void throwStateStoreUnvailableResponse() {
        throw new RestException(Status.SERVICE_UNAVAILABLE,
                "State storage client is not done initializing. " + "Please try again in a little while.");
    }

    public static String createPackagePath(String tenant, String namespace, String functionName, String fileName) {
        return String.format("%s/%s/%s/%s", tenant, namespace, Codec.encode(functionName),
                getUniquePackageName(Codec.encode(fileName)));
    }

    /**
     * @deprecated use {@link #isAuthorizedRole(String, String, AuthenticationParameters)} instead.
     */
    @Deprecated
    public boolean isAuthorizedRole(String tenant, String namespace, String clientRole,
                                    AuthenticationDataSource authenticationData) throws PulsarAdminException {
        AuthenticationParameters authParams = AuthenticationParameters.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(authenticationData).build();
        return isAuthorizedRole(tenant, namespace, authParams);
    }

    public boolean isAuthorizedRole(String tenant, String namespace,
                                    AuthenticationParameters authParams) throws PulsarAdminException {
        if (worker().getWorkerConfig().isAuthorizationEnabled()) {
            return allowFunctionOps(NamespaceName.get(tenant, namespace), authParams);
        } else {
            return true;
        }
    }

    public void throwRestExceptionIfUnauthorizedForNamespace(String tenant, String namespace, String componentName,
                                                             String action, AuthenticationParameters authParams) {
        try {
            if (!isAuthorizedRole(tenant, namespace, authParams)) {
                log.warn("{}/{}/{} Client with role [{}] and originalPrincipal [{}] is not authorized to {} {}",
                        tenant, namespace, componentName, authParams.getClientRole(),
                        authParams.getOriginalPrincipal(), action, ComponentTypeUtils.toString(componentType));
                throw new RestException(Status.UNAUTHORIZED, "Client is not authorized to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, componentName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Deprecated
    protected void componentStatusRequestValidate(final String tenant, final String namespace,
                                                  final String componentName,
                                                  final String clientRole,
                                                  final AuthenticationDataSource clientAuthenticationDataHttps) {
        AuthenticationParameters authParams = AuthenticationParameters.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        componentStatusRequestValidate(tenant, namespace, componentName, authParams);
    }

    protected void componentStatusRequestValidate(final String tenant, final String namespace,
                                                  final String componentName,
                                                  final AuthenticationParameters authParams) {
        if (!isWorkerServiceAvailable()) {
            throw new RestException(Status.SERVICE_UNAVAILABLE,
                    "Function worker service is not done initializing. Please try again in a little while.");
        }

        throwRestExceptionIfUnauthorizedForNamespace(tenant, namespace, componentName, "get status for",
                authParams);

        // validate parameters
        try {
            validateGetFunctionRequestParams(tenant, namespace, componentName, componentType);
        } catch (IllegalArgumentException e) {
            log.error("Invalid get {} Status request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant,
                    namespace, componentName, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.warn("{} in get {} Status does not exist @ /{}/{}/{}", ComponentTypeUtils.toString(componentType),
                    componentType, tenant, namespace, componentName);
            throw new RestException(Status.NOT_FOUND,
                    String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), componentName));
        }

        FunctionMetaData functionMetaData =
                functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName,
                    ComponentTypeUtils.toString(componentType));
            throw new RestException(Status.NOT_FOUND,
                    String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), componentName));
        }
    }

    @Deprecated
    protected void componentInstanceStatusRequestValidate(final String tenant,
                                                      final String namespace,
                                                      final String componentName,
                                                      final int instanceId,
                                                      final String clientRole,
                                                      final AuthenticationDataSource clientAuthenticationDataHttps) {
        AuthenticationParameters authParams = AuthenticationParameters.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        componentInstanceStatusRequestValidate(tenant, namespace, componentName, instanceId, authParams);
    }

    protected void componentInstanceStatusRequestValidate(final String tenant,
                                                          final String namespace,
                                                          final String componentName,
                                                          final int instanceId,
                                                          final AuthenticationParameters authParams) {

        componentStatusRequestValidate(tenant, namespace, componentName, authParams);

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        FunctionMetaData functionMetaData =
                functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        int parallelism = functionMetaData.getFunctionDetails().getParallelism();
        if (instanceId < 0 || instanceId >= parallelism) {
            log.error("instanceId in get {} Status out of bounds @ /{}/{}/{}",
                    ComponentTypeUtils.toString(componentType), tenant, namespace, componentName);
            throw new RestException(Status.BAD_REQUEST,
                    String.format("%s %s doesn't have instance with id %s", ComponentTypeUtils.toString(componentType),
                            componentName, instanceId));
        }
    }

    public boolean isSuperUser(AuthenticationParameters authParams) {
        if (authParams.getClientRole() != null) {
            try {
                return worker().getAuthorizationService().isSuperUser(authParams)
                        .get(worker().getWorkerConfig().getMetadataStoreOperationTimeoutSeconds(), SECONDS);
            } catch (InterruptedException e) {
                log.warn("Time-out {} sec while checking the role {} originalPrincipal {} is a super user role ",
                        worker().getWorkerConfig().getMetadataStoreOperationTimeoutSeconds(),
                        authParams.getClientRole(), authParams.getOriginalPrincipal());
                throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
            } catch (Exception e) {
                log.warn("Failed verifying role {} originalPrincipal {} is a super user role",
                        authParams.getClientRole(), authParams.getOriginalPrincipal(), e);
                throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
            }
        }
        return false;
    }

    /**
     * @deprecated use {@link #isSuperUser(AuthenticationParameters)}
     */
    @Deprecated
    public boolean isSuperUser(String clientRole, AuthenticationDataSource authenticationData) {
        AuthenticationParameters authParams = AuthenticationParameters.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(authenticationData).build();
        return isSuperUser(authParams);
    }

    /**
     * @deprecated use {@link #isSuperUser(AuthenticationParameters)}
     */
    @Deprecated
    public boolean allowFunctionOps(NamespaceName namespaceName, String role,
                                    AuthenticationDataSource authenticationData) {
        AuthenticationParameters authParams = AuthenticationParameters.builder().clientRole(role)
                .clientAuthenticationDataSource(authenticationData).build();
        return allowFunctionOps(namespaceName, authParams);
    }

    public boolean allowFunctionOps(NamespaceName namespaceName, AuthenticationParameters authParams) {
        try {
            switch (componentType) {
                case SINK:
                    return worker().getAuthorizationService().allowSinkOpsAsync(namespaceName, authParams)
                            .get(worker().getWorkerConfig().getMetadataStoreOperationTimeoutSeconds(), SECONDS);
                case SOURCE:
                    return worker().getAuthorizationService().allowSourceOpsAsync(namespaceName, authParams)
                            .get(worker().getWorkerConfig().getMetadataStoreOperationTimeoutSeconds(), SECONDS);
                case FUNCTION:
                default:
                    return worker().getAuthorizationService().allowFunctionOpsAsync(namespaceName, authParams)
                            .get(worker().getWorkerConfig().getMetadataStoreOperationTimeoutSeconds(), SECONDS);
            }
        } catch (InterruptedException e) {
            log.warn("Time-out {} sec while checking function authorization on {} ",
                    worker().getWorkerConfig().getMetadataStoreOperationTimeoutSeconds(), namespaceName);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        } catch (Exception e) {
            log.warn("Admin-client with Role [{}] originalPrincipal [{}] failed to get function permissions for "
                            + "namespace - {}. {}", authParams.getClientRole(),
                    authParams.getOriginalPrincipal(), namespaceName, e.getMessage(), e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    private void internalProcessFunctionRequest(final String tenant, final String namespace, final String functionName,
                                                final FunctionMetaData functionMetadata, boolean delete,
                                                String errorMsg) {
        try {
            if (worker().getLeaderService().isLeader()) {
                worker().getFunctionMetaDataManager().updateFunctionOnLeader(functionMetadata, delete);
            } else {
                FunctionsImpl functions = (FunctionsImpl) worker().getFunctionAdmin().functions();
                functions.updateOnWorkerLeader(tenant,
                        namespace, functionName, functionMetadata.toByteArray(), delete);
            }
        } catch (PulsarAdminException e) {
            log.error(errorMsg, e);
            throw new RestException(e.getStatusCode(), e.getMessage());
        } catch (IllegalStateException e) {
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        } catch (IllegalArgumentException e) {
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }
    }

    static File downloadPackageFile(PulsarWorkerService worker, String packageName)
            throws IOException, PulsarAdminException {
        Path tempDirectory;
        if (worker.getWorkerConfig().getDownloadDirectory() != null) {
            tempDirectory = Paths.get(worker.getWorkerConfig().getDownloadDirectory());
        } else {
            // use the Nar extraction directory as a temporary directory for downloaded files
            tempDirectory = Paths.get(worker.getWorkerConfig().getNarExtractionDirectory());
        }
        Files.createDirectories(tempDirectory);
        File file = Files.createTempFile(tempDirectory, "function", ".tmp").toFile();
        worker.getBrokerAdmin().packages().download(packageName, file.toString());
        return file;
    }

    protected File getPackageFile(FunctionDetails.ComponentType componentType, String functionPkgUrl,
                                  String existingPackagePath, InputStream uploadedInputStream)
            throws IOException, PulsarAdminException {
        File componentPackageFile = null;
        if (isNotBlank(functionPkgUrl)) {
            componentPackageFile = getPackageFile(componentType, functionPkgUrl);
        } else if (existingPackagePath.startsWith(Utils.FILE) || existingPackagePath.startsWith(Utils.HTTP)) {
            if (!worker().getPackageUrlValidator().isValidPackageUrl(componentType, functionPkgUrl)) {
                throw new IllegalArgumentException("Function Package url is not valid."
                        + "supported url (http/https/file)");
            }
            try {
                componentPackageFile = FunctionCommon.extractFileFromPkgURL(existingPackagePath);
            } catch (Exception e) {
                throw new IllegalArgumentException(String.format("Encountered error \"%s\" "
                                + "when getting %s package from %s", e.getMessage(),
                        ComponentTypeUtils.toString(componentType), functionPkgUrl));
            }
        } else if (Utils.hasPackageTypePrefix(existingPackagePath)) {
            componentPackageFile = getPackageFile(componentType, existingPackagePath);
        } else if (uploadedInputStream != null) {
            componentPackageFile = WorkerUtils.dumpToTmpFile(uploadedInputStream);
        } else if (!existingPackagePath.startsWith(Utils.BUILTIN)) {
            componentPackageFile = FunctionCommon.createPkgTempFile();
            componentPackageFile.deleteOnExit();
            WorkerUtils.downloadFromBookkeeper(worker().getDlogNamespace(),
                    componentPackageFile, existingPackagePath);
        }
        return componentPackageFile;
    }

    protected File getPackageFile(FunctionDetails.ComponentType componentType, String functionPkgUrl)
            throws IOException, PulsarAdminException {
        if (Utils.hasPackageTypePrefix(functionPkgUrl)) {
            return downloadPackageFile(worker(), functionPkgUrl);
        } else {
            if (!worker().getPackageUrlValidator().isValidPackageUrl(componentType, functionPkgUrl)) {
                throw new IllegalArgumentException("Function Package url is not valid."
                        + "supported url (http/https/file)");
            }
            try {
                return FunctionCommon.extractFileFromPkgURL(functionPkgUrl);
            } catch (Exception e) {
                throw new IllegalArgumentException(String.format("Encountered error \"%s\" "
                                + "when getting %s package from %s", e.getMessage(),
                        ComponentTypeUtils.toString(componentType), functionPkgUrl), e);
            }
        }
    }

    protected ValidatableFunctionPackage getBuiltinFunctionPackage(String archive) {
        if (!StringUtils.isEmpty(archive)) {
            if (archive.startsWith(org.apache.pulsar.common.functions.Utils.BUILTIN)) {
                archive = archive.replaceFirst("^builtin://", "");

                FunctionArchive function = worker().getFunctionsManager().getFunction(archive);
                // check if builtin connector exists
                if (function == null) {
                    throw new IllegalArgumentException("Built-in " + componentType + " is not available");
                }
                return function.getFunctionPackage();
            }
        }
        return null;
    }
}
