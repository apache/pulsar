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
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.api.kv.result.KeyValue;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.exceptions.NamespaceNotFoundException;
import org.apache.bookkeeper.clients.exceptions.StreamNotFoundException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.FunctionStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.functions.auth.FunctionAuthData;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.PackageLocationMetaData;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.apache.pulsar.functions.utils.ComponentType;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.utils.SinkConfigUtils;
import org.apache.pulsar.functions.utils.SourceConfigUtils;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.FunctionRuntimeInfo;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.apache.pulsar.functions.worker.request.RequestResult;
import org.apache.pulsar.functions.worker.rest.RestException;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.functions.utils.ComponentType.FUNCTION;
import static org.apache.pulsar.functions.utils.ComponentType.SINK;
import static org.apache.pulsar.functions.utils.ComponentType.SOURCE;
import static org.apache.pulsar.functions.utils.FunctionCommon.extractFileFromPkgURL;
import static org.apache.pulsar.functions.utils.FunctionCommon.getStateNamespace;
import static org.apache.pulsar.functions.utils.FunctionCommon.getUniquePackageName;
import static org.apache.pulsar.functions.worker.WorkerUtils.isFunctionCodeBuiltin;

@Slf4j
public abstract class ComponentImpl {

    private final AtomicReference<StorageClient> storageClient = new AtomicReference<>();
    protected final Supplier<WorkerService> workerServiceSupplier;
    protected final ComponentType componentType;

    public ComponentImpl(Supplier<WorkerService> workerServiceSupplier, ComponentType componentType) {
        this.workerServiceSupplier = workerServiceSupplier;
        this.componentType = componentType;
    }

    protected abstract class GetStatus<S, T> {

        public abstract T notScheduledInstance();

        public abstract T fromFunctionStatusProto(final InstanceCommunication.FunctionStatus status,
                                                  final String assignedWorkerId);

        public abstract T notRunning(final String assignedWorkerId, final String error);

        public T getComponentInstanceStatus(final String tenant,
                                            final String namespace,
                                            final String name,
                                            final int instanceId,
                                            final URI uri) {

            Function.Assignment assignment;
            if (worker().getFunctionRuntimeManager().getRuntimeFactory().externallyManaged()) {
                assignment = worker().getFunctionRuntimeManager().findFunctionAssignment(tenant, namespace, name, -1);
            } else {
                assignment = worker().getFunctionRuntimeManager().findFunctionAssignment(tenant, namespace, name, instanceId);
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
                    String message = functionRuntimeInfo.getStartupException() != null ? functionRuntimeInfo.getStartupException().getMessage() : "";
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
                    throw new WebApplicationException(Response.serverError().status(Status.INTERNAL_SERVER_ERROR).build());
                } else {
                    URI redirect = UriBuilder.fromUri(uri).host(workerInfo.getWorkerHostname()).port(workerInfo.getPort()).build();
                    throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                }
            }
        }

        public abstract S getStatus(final String tenant,
                                    final String namespace,
                                    final String name,
                                    final Collection<Function.Assignment> assignments,
                                    final URI uri) throws PulsarAdminException;

        public abstract S getStatusExternal(final String tenant,
                                            final String namespace,
                                            final String name,
                                            final int parallelism);

        public abstract S emptyStatus(final int parallelism);

        public S getComponentStatus(final String tenant,
                                    final String namespace,
                                    final String name,
                                    final URI uri) {

            Function.FunctionMetaData functionMetaData = worker().getFunctionMetaDataManager().getFunctionMetaData(tenant, namespace, name);

            Collection<Function.Assignment> assignments = worker().getFunctionRuntimeManager().findFunctionAssignments(tenant, namespace, name);

            // TODO refactor the code for externally managed.
            if (worker().getFunctionRuntimeManager().getRuntimeFactory().externallyManaged()) {
                Function.Assignment assignment = assignments.iterator().next();
                boolean isOwner = worker().getWorkerConfig().getWorkerId().equals(assignment.getWorkerId());
                if (isOwner) {
                    return getStatusExternal(tenant, namespace, name, functionMetaData.getFunctionDetails().getParallelism());
                } else {

                    // find the hostname/port of the worker who is the owner

                    List<WorkerInfo> workerInfoList = worker().getMembershipManager().getCurrentMembership();
                    WorkerInfo workerInfo = null;
                    for (WorkerInfo entry: workerInfoList) {
                        if (assignment.getWorkerId().equals(entry.getWorkerId())) {
                            workerInfo = entry;
                        }
                    }
                    if (workerInfo == null) {
                        return emptyStatus(functionMetaData.getFunctionDetails().getParallelism());
                    }

                    if (uri == null) {
                        throw new WebApplicationException(Response.serverError().status(Status.INTERNAL_SERVER_ERROR).build());
                    } else {
                        URI redirect = UriBuilder.fromUri(uri).host(workerInfo.getWorkerHostname()).port(workerInfo.getPort()).build();
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

    protected WorkerService worker() {
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
        if (!workerService.isInitialized()) {
            return false;
        }
        return true;
    }

    public void registerFunction(final String tenant,
                                 final String namespace,
                                 final String componentName,
                                 final InputStream uploadedInputStream,
                                 final FormDataContentDisposition fileDetail,
                                 final String functionPkgUrl,
                                 final String componentConfigJson,
                                 final String clientRole,
                                 AuthenticationDataHttps clientAuthenticationDataHttps) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (tenant == null) {
            throw new RestException(Status.BAD_REQUEST, "Tenant is not provided");
        }
        if (namespace == null) {
            throw new RestException(Status.BAD_REQUEST, "Namespace is not provided");
        }
        if (componentName == null) {
            throw new RestException(Status.BAD_REQUEST, componentType + " Name is not provided");
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.error("{}/{}/{} Client [{}] is not admin and authorized to register {}", tenant, namespace,
                        componentName, clientRole, componentType);
                throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, componentName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        try {
            // Check tenant exists
            final TenantInfo tenantInfo = worker().getBrokerAdmin().tenants().getTenantInfo(tenant);

            String qualifiedNamespace = tenant + "/" + namespace;
            if (!worker().getBrokerAdmin().namespaces().getNamespaces(tenant).contains(qualifiedNamespace)) {
                log.error("{}/{}/{} Namespace {} does not exist", tenant, namespace,
                        componentName, namespace);
                throw new RestException(Status.BAD_REQUEST, "Namespace does not exist");
            }
        } catch (PulsarAdminException.NotAuthorizedException e) {
            log.error("{}/{}/{} Client [{}] is not admin and authorized to operate {} on tenant", tenant, namespace,
                    componentName, clientRole, componentType);
            throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
        } catch (PulsarAdminException.NotFoundException e) {
            log.error("{}/{}/{} Tenant {} does not exist", tenant, namespace, componentName, tenant);
            throw new RestException(Status.BAD_REQUEST, "Tenant does not exist");
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Issues getting tenant data", tenant, namespace, componentName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();

        if (functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.error("{} {}/{}/{} already exists", componentType, tenant, namespace, componentName);
            throw new RestException(Status.BAD_REQUEST, String.format("%s %s already exists", componentType, componentName));
        }

        FunctionDetails functionDetails;
        boolean isPkgUrlProvided = isNotBlank(functionPkgUrl);
        File componentPackageFile = null;
        try {

            // validate parameters
            try {
                if (isPkgUrlProvided) {

                    if (!Utils.isFunctionPackageUrlSupported(functionPkgUrl)) {
                        throw new IllegalArgumentException("Function Package url is not valid. supported url (http/https/file)");
                    }
                    try {
                        componentPackageFile = FunctionCommon.extractFileFromPkgURL(functionPkgUrl);
                    } catch (Exception e) {
                        throw new IllegalArgumentException(String.format("Encountered error \"%s\" when getting %s package from %s", e.getMessage(), componentType, functionPkgUrl));
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, componentName,
                            componentConfigJson, componentType, componentPackageFile);
                } else {
                    if (uploadedInputStream != null) {
                        componentPackageFile = WorkerUtils.dumpToTmpFile(uploadedInputStream);
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, componentName,
                            componentConfigJson, componentType, componentPackageFile);
                    if (!isFunctionCodeBuiltin(functionDetails) && (componentPackageFile == null || fileDetail == null)) {
                        throw new IllegalArgumentException(componentType + " Package is not provided");
                    }
                }
            } catch (Exception e) {
                log.error("Invalid register {} request @ /{}/{}/{}", componentType, tenant, namespace, componentName, e);
                throw new RestException(Status.BAD_REQUEST, e.getMessage());
            }

            try {
                worker().getFunctionRuntimeManager().getRuntimeFactory().doAdmissionChecks(functionDetails);
            } catch (Exception e) {
                log.error("{} {}/{}/{} cannot be admitted by the runtime factory", componentType, tenant, namespace, componentName);
                throw new RestException(Status.BAD_REQUEST, String.format("%s %s cannot be admitted:- %s", componentType, componentName, e.getMessage()));
            }

            // function state
            FunctionMetaData.Builder functionMetaDataBuilder = FunctionMetaData.newBuilder()
                    .setFunctionDetails(functionDetails)
                    .setCreateTime(System.currentTimeMillis())
                    .setVersion(0);

            // cache auth if need
            if (clientAuthenticationDataHttps != null) {
                try {
                    Optional<FunctionAuthData> functionAuthData = worker().getFunctionRuntimeManager()
                            .getRuntimeFactory()
                            .getAuthProvider()
                            .cacheAuthData(tenant, namespace, componentName, clientAuthenticationDataHttps);

                    if (functionAuthData.isPresent()) {
                        functionMetaDataBuilder.setFunctionAuthSpec(
                                Function.FunctionAuthenticationSpec.newBuilder()
                                        .setData(ByteString.copyFrom(functionAuthData.get().getData()))
                                        .build());
                    }
                } catch (Exception e) {
                    log.error("Error caching authentication data for {} {}/{}/{}", componentType, tenant, namespace, componentName, e);

                    throw new RestException(Status.INTERNAL_SERVER_ERROR, String.format("Error caching authentication data for %s %s:- %s", componentType, componentName, e.getMessage()));
                }
            }

            PackageLocationMetaData.Builder packageLocationMetaDataBuilder;
            try {
                packageLocationMetaDataBuilder = getFunctionPackageLocation(functionDetails,
                        functionPkgUrl, fileDetail, componentPackageFile);
            } catch (Exception e) {
                throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
            }

            functionMetaDataBuilder.setPackageLocation(packageLocationMetaDataBuilder);
            updateRequest(functionMetaDataBuilder.build());
        } finally {
            if (!(functionPkgUrl != null && functionPkgUrl.startsWith(Utils.FILE))
                    && componentPackageFile != null && componentPackageFile.exists()) {
                componentPackageFile.delete();
            }
        }
    }

    PackageLocationMetaData.Builder getFunctionPackageLocation(final FunctionDetails functionDetails,
                                                                       final String functionPkgUrl,
                                                                       final FormDataContentDisposition fileDetail,
                                                                       final File uploadedInputStreamAsFile) throws Exception {
        String tenant = functionDetails.getTenant();
        String namespace = functionDetails.getNamespace();
        String componentName = functionDetails.getName();
        PackageLocationMetaData.Builder packageLocationMetaDataBuilder = PackageLocationMetaData.newBuilder();
        boolean isBuiltin = isFunctionCodeBuiltin(functionDetails);
        boolean isPkgUrlProvided = isNotBlank(functionPkgUrl);
        if (worker().getFunctionRuntimeManager().getRuntimeFactory().externallyManaged()) {
            // For externally managed schedulers, the pkgUrl/builtin stuff should be copied to bk
            if (isBuiltin) {
                File sinkOrSource;
                if (componentType.equals(SOURCE)) {
                    String archiveName = functionDetails.getSource().getBuiltin();
                    sinkOrSource = worker().getConnectorsManager().getSourceArchive(archiveName).toFile();
                } else {
                    String archiveName = functionDetails.getSink().getBuiltin();
                    sinkOrSource = worker().getConnectorsManager().getSinkArchive(archiveName).toFile();
                }
                packageLocationMetaDataBuilder.setPackagePath(createPackagePath(tenant, namespace, componentName,
                        sinkOrSource.getName()));
                packageLocationMetaDataBuilder.setOriginalFileName(sinkOrSource.getName());
                log.info("Uploading {} package to {}", componentType, packageLocationMetaDataBuilder.getPackagePath());
                WorkerUtils.uploadFileToBookkeeper(packageLocationMetaDataBuilder.getPackagePath(), sinkOrSource, worker().getDlogNamespace());
            } else if (isPkgUrlProvided) {
                File file = extractFileFromPkgURL(functionPkgUrl);
                packageLocationMetaDataBuilder.setPackagePath(createPackagePath(tenant, namespace, componentName,
                        file.getName()));
                packageLocationMetaDataBuilder.setOriginalFileName(file.getName());
                log.info("Uploading {} package to {}", componentType, packageLocationMetaDataBuilder.getPackagePath());
                WorkerUtils.uploadFileToBookkeeper(packageLocationMetaDataBuilder.getPackagePath(), file, worker().getDlogNamespace());
            } else {
                packageLocationMetaDataBuilder.setPackagePath(createPackagePath(tenant, namespace, componentName,
                        fileDetail.getFileName()));
                packageLocationMetaDataBuilder.setOriginalFileName(fileDetail.getFileName());
                log.info("Uploading {} package to {}", componentType, packageLocationMetaDataBuilder.getPackagePath());
                WorkerUtils.uploadFileToBookkeeper(packageLocationMetaDataBuilder.getPackagePath(), uploadedInputStreamAsFile, worker().getDlogNamespace());
            }
        } else {
            // For pulsar managed schedulers, the pkgUrl/builtin stuff should be copied to bk
            if (isBuiltin) {
                packageLocationMetaDataBuilder.setPackagePath("builtin://" + getFunctionCodeBuiltin(functionDetails));
            } else if (isPkgUrlProvided) {
                packageLocationMetaDataBuilder.setPackagePath(functionPkgUrl);
            } else {
                packageLocationMetaDataBuilder.setPackagePath(createPackagePath(tenant, namespace, componentName, fileDetail.getFileName()));
                packageLocationMetaDataBuilder.setOriginalFileName(fileDetail.getFileName());
                log.info("Uploading {} package to {}", componentType, packageLocationMetaDataBuilder.getPackagePath());
                WorkerUtils.uploadFileToBookkeeper(packageLocationMetaDataBuilder.getPackagePath(), uploadedInputStreamAsFile, worker().getDlogNamespace());
            }
        }
        return packageLocationMetaDataBuilder;
    }


    public void updateFunction(final String tenant,
                               final String namespace,
                               final String componentName,
                               final InputStream uploadedInputStream,
                               final FormDataContentDisposition fileDetail,
                               final String functionPkgUrl,
                               final String componentConfigJson,
                               final String clientRole,
                               AuthenticationDataHttps clientAuthenticationDataHttps) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (tenant == null) {
            throw new RestException(Status.BAD_REQUEST, "Tenant is not provided");
        }
        if (namespace == null) {
            throw new RestException(Status.BAD_REQUEST, "Namespace is not provided");
        }
        if (componentName == null) {
            throw new RestException(Status.BAD_REQUEST, componentType + " Name is not provided");
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.error("{}/{}/{} Client [{}] is not admin and authorized to update {}", tenant, namespace,
                        componentName, clientRole, componentType);
                throw new RestException(Status.UNAUTHORIZED, componentType + "client is not authorize to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, componentName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();

        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            throw new RestException(Status.BAD_REQUEST, String.format("%s %s doesn't exist", componentType, componentName));
        }

        String mergedComponentConfigJson;
        String existingComponentConfigJson;

        FunctionMetaData existingComponent = functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);

        if (componentType.equals(FUNCTION)) {
            FunctionConfig existingFunctionConfig = FunctionConfigUtils.convertFromDetails(existingComponent.getFunctionDetails());
            existingComponentConfigJson = new Gson().toJson(existingFunctionConfig);
            FunctionConfig functionConfig = new Gson().fromJson(componentConfigJson, FunctionConfig.class);
            // The rest end points take precedence over whatever is there in functionconfig
            functionConfig.setTenant(tenant);
            functionConfig.setNamespace(namespace);
            functionConfig.setName(componentName);
            try {
                FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(existingFunctionConfig, functionConfig);
                mergedComponentConfigJson = new Gson().toJson(mergedConfig);
            } catch (Exception e) {
                throw new RestException(Status.BAD_REQUEST, e.getMessage());
            }
        } else if (componentType.equals(SOURCE)) {
            SourceConfig existingSourceConfig = SourceConfigUtils.convertFromDetails(existingComponent.getFunctionDetails());
            existingComponentConfigJson = new Gson().toJson(existingSourceConfig);
            SourceConfig sourceConfig = new Gson().fromJson(componentConfigJson, SourceConfig.class);
            // The rest end points take precedence over whatever is there in functionconfig
            sourceConfig.setTenant(tenant);
            sourceConfig.setNamespace(namespace);
            sourceConfig.setName(componentName);
            try {
                SourceConfig mergedConfig = SourceConfigUtils.validateUpdate(existingSourceConfig, sourceConfig);
                mergedComponentConfigJson = new Gson().toJson(mergedConfig);
            } catch (Exception e) {
                throw new RestException(Status.BAD_REQUEST, e.getMessage());
            }
        } else {
            SinkConfig existingSinkConfig = SinkConfigUtils.convertFromDetails(existingComponent.getFunctionDetails());
            existingComponentConfigJson = new Gson().toJson(existingSinkConfig);
            SinkConfig sinkConfig = new Gson().fromJson(componentConfigJson, SinkConfig.class);
            // The rest end points take precedence over whatever is there in functionconfig
            sinkConfig.setTenant(tenant);
            sinkConfig.setNamespace(namespace);
            sinkConfig.setName(componentName);
            try {
                SinkConfig mergedConfig = SinkConfigUtils.validateUpdate(existingSinkConfig, sinkConfig);
                mergedComponentConfigJson = new Gson().toJson(mergedConfig);
            } catch (Exception e) {
                throw new RestException(Status.BAD_REQUEST, e.getMessage());
            }
        }

        if (existingComponentConfigJson.equals(mergedComponentConfigJson) && isBlank(functionPkgUrl) && uploadedInputStream == null) {
            log.error("{}/{}/{} Update contains no changes", tenant, namespace, componentName);
            throw new RestException(Status.BAD_REQUEST, "Update contains no change");
        }

        FunctionDetails functionDetails;
        File componentPackageFile = null;
        try {

            // validate parameters
            try {
                if (isNotBlank(functionPkgUrl)) {
                    try {
                        componentPackageFile = FunctionCommon.extractFileFromPkgURL(functionPkgUrl);
                    } catch (Exception e) {
                        throw new IllegalArgumentException(String.format("Encountered error \"%s\" when getting %s package from %s", e.getMessage(), componentType, functionPkgUrl));
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, componentName,
                            mergedComponentConfigJson, componentType, componentPackageFile);

                } else if (uploadedInputStream != null) {

                    componentPackageFile = WorkerUtils.dumpToTmpFile(uploadedInputStream);
                    functionDetails = validateUpdateRequestParams(tenant, namespace, componentName,
                            mergedComponentConfigJson, componentType, componentPackageFile);

                } else if (existingComponent.getPackageLocation().getPackagePath().startsWith("builtin://")) {
                    functionDetails = validateUpdateRequestParams(tenant, namespace, componentName
                            , mergedComponentConfigJson, componentType, componentPackageFile);
                    if (!isFunctionCodeBuiltin(functionDetails) && (componentPackageFile == null || fileDetail == null)) {
                        throw new IllegalArgumentException(componentType + " Package is not provided");
                    }
                } else {
                    componentPackageFile = File.createTempFile("functions", null);
                    componentPackageFile.deleteOnExit();
                    WorkerUtils.downloadFromBookkeeper(worker().getDlogNamespace(), componentPackageFile, existingComponent.getPackageLocation().getPackagePath());

                    functionDetails = validateUpdateRequestParams(tenant, namespace, componentName,
                            mergedComponentConfigJson, componentType, componentPackageFile);
                }
            } catch (Exception e) {
                log.error("Invalid update {} request @ /{}/{}/{}", componentType, tenant, namespace, componentName, e);
                throw new RestException(Status.BAD_REQUEST, e.getMessage());
            }

            try {
                worker().getFunctionRuntimeManager().getRuntimeFactory().doAdmissionChecks(functionDetails);
            } catch (Exception e) {
                log.error("Updated {} {}/{}/{} cannot be submitted to runtime factory", componentType, tenant, namespace, componentName);
                throw new RestException(Status.BAD_REQUEST, String.format("%s %s cannot be admitted:- %s", componentType, componentName, e.getMessage()));
            }

            // merge from existing metadata
            FunctionMetaData.Builder functionMetaDataBuilder = FunctionMetaData.newBuilder().mergeFrom(existingComponent)
                    .setFunctionDetails(functionDetails);

            PackageLocationMetaData.Builder packageLocationMetaDataBuilder;
            if (isNotBlank(functionPkgUrl) || componentPackageFile != null) {
                try {
                    packageLocationMetaDataBuilder = getFunctionPackageLocation(functionDetails,
                            functionPkgUrl, fileDetail, componentPackageFile);
                } catch (Exception e) {
                    throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
                }
            } else {
                packageLocationMetaDataBuilder = PackageLocationMetaData.newBuilder().mergeFrom(existingComponent.getPackageLocation());
            }

            functionMetaDataBuilder.setPackageLocation(packageLocationMetaDataBuilder);

            updateRequest(functionMetaDataBuilder.build());
        } finally {
            if (!(functionPkgUrl != null && functionPkgUrl.startsWith(Utils.FILE))
                    && componentPackageFile != null && componentPackageFile.exists()) {
                componentPackageFile.delete();
            }
        }
    }

    public void deregisterFunction(final String tenant,
                                   final String namespace,
                                   final String componentName,
                                   final String clientRole,
                                   AuthenticationDataHttps clientAuthenticationDataHttps) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.error("{}/{}/{} Client [{}] is not admin and authorized to deregister {}", tenant, namespace,
                        componentName, clientRole, componentType);
                throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, componentName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        // delete state table
        if (null != worker().getStateStoreAdminClient()) {
            final String tableNs = getStateNamespace(tenant, namespace);
            final String tableName = componentName;
            try {
                FutureUtils.result(worker().getStateStoreAdminClient().deleteStream(tableNs, tableName));
            } catch (NamespaceNotFoundException | StreamNotFoundException e) {
                // ignored if the state table doesn't exist
            } catch (Exception e) {
                log.error("{}/{}/{} Failed to delete state table", e);
                throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
            }
        }

        // validate parameters
        try {
            validateDeregisterRequestParams(tenant, namespace, componentName, componentType);
        } catch (IllegalArgumentException e) {
            log.error("Invalid deregister {} request @ /{}/{}/{}", componentType, tenant, namespace, componentName, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.error("{} to deregister does not exist @ /{}/{}/{}", componentType, tenant, namespace, componentName);
            throw new RestException(Status.NOT_FOUND, String.format("%s %s doesn't exist", componentType, componentName));
        }
        FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);

        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName, componentType);
            throw new RestException(Status.NOT_FOUND, String.format("%s %s doesn't exist", componentType, componentName));
        }

        CompletableFuture<RequestResult> completableFuture = functionMetaDataManager.deregisterFunction(tenant,
                namespace, componentName);

        RequestResult requestResult = null;
        try {
            requestResult = completableFuture.get();
            if (!requestResult.isSuccess()) {
                throw new RestException(Status.BAD_REQUEST, requestResult.getMessage());
            }
        } catch (ExecutionException e) {
            log.error("Execution Exception while deregistering {} @ /{}/{}/{}",
                    componentType, tenant, namespace, componentName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getCause().getMessage());
        } catch (InterruptedException e) {
            log.error("Interrupted Exception while deregistering {} @ /{}/{}/{}",
                    componentType, tenant, namespace, componentName, e);
            throw new RestException(Status.REQUEST_TIMEOUT, e.getMessage());
        }
    }

    public FunctionConfig getFunctionInfo(final String tenant,
                                          final String namespace,
                                          final String componentName,
                                          final String clientRole,
                                          final AuthenticationDataSource clientAuthenticationDataHttps) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.error("{}/{}/{} Client [{}] is not admin and authorized to get {}", tenant, namespace,
                        componentName, clientRole, componentType);
                throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, componentName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        // validate parameters
        try {
            validateGetFunctionRequestParams(tenant, namespace, componentName, componentType);
        } catch (IllegalArgumentException e) {
            log.error("Invalid get {} request @ /{}/{}/{}", componentType, tenant, namespace, componentName, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.error("{} does not exist @ /{}/{}/{}", componentType, tenant, namespace, componentName);
            throw new RestException(Status.NOT_FOUND, String.format(componentType + " %s doesn't exist", componentName));
        }
        FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName, componentType);
            throw new RestException(Status.NOT_FOUND, String.format(componentType + " %s doesn't exist", componentName));
        }
        FunctionConfig config = FunctionConfigUtils.convertFromDetails(functionMetaData.getFunctionDetails());
        return config;
    }

    public void stopFunctionInstance(final String tenant,
                                     final String namespace,
                                     final String componentName,
                                     final String instanceId,
                                     final URI uri,
                                     final String clientRole,
                                     final AuthenticationDataSource clientAuthenticationDataHttps) {
        changeFunctionInstanceStatus(tenant, namespace, componentName, instanceId, false, uri, clientRole, clientAuthenticationDataHttps);
    }

    public void startFunctionInstance(final String tenant,
                                      final String namespace,
                                      final String componentName,
                                      final String instanceId,
                                      final URI uri,
                                      final String clientRole,
                                      final AuthenticationDataSource clientAuthenticationDataHttps) {
        changeFunctionInstanceStatus(tenant, namespace, componentName, instanceId, true, uri, clientRole, clientAuthenticationDataHttps);
    }

    public void changeFunctionInstanceStatus(final String tenant,
                                             final String namespace,
                                             final String componentName,
                                             final String instanceId,
                                             final boolean start,
                                             final URI uri,
                                             final String clientRole,
                                             final AuthenticationDataSource clientAuthenticationDataHttps) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.error("{}/{}/{} Client [{}] is not admin and authorized to start/stop {}", tenant, namespace,
                        componentName, clientRole, componentType);
                throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, componentName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        // validate parameters
        try {
            validateGetFunctionInstanceRequestParams(tenant, namespace, componentName, componentType, instanceId);
        } catch (IllegalArgumentException e) {
            log.error("Invalid start/stop {} request @ /{}/{}/{}", componentType, tenant, namespace, componentName, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.error("{} does not exist @ /{}/{}/{}", componentType, tenant, namespace, componentName);
            throw new RestException(Status.NOT_FOUND, String.format("%s %s doesn't exist", componentType, componentName));
        }

        FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName, componentType);
            throw new RestException(Status.NOT_FOUND, String.format("%s %s doesn't exist", componentType, componentName));
        }

        if (!functionMetaDataManager.canChangeState(functionMetaData, Integer.parseInt(instanceId), start ? Function.FunctionState.RUNNING : Function.FunctionState.STOPPED)) {
            log.error("Operation not permitted on {}/{}/{}", tenant, namespace, componentName);
            throw new RestException(Status.BAD_REQUEST, String.format("Operation not permitted"));
        }

        try {
            functionMetaDataManager.changeFunctionInstanceStatus(tenant, namespace, componentName,
                    Integer.parseInt(instanceId), start);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("Failed to start/stop {}: {}/{}/{}/{}", componentType, tenant, namespace, componentName, instanceId, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    public void restartFunctionInstance(final String tenant,
                                        final String namespace,
                                        final String componentName,
                                        final String instanceId,
                                        final URI uri,
                                        final String clientRole,
                                        final AuthenticationDataSource clientAuthenticationDataHttps) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.error("{}/{}/{} Client [{}] is not admin and authorized to restart {}", tenant, namespace,
                        componentName, clientRole, componentType);
                throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, componentName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        // validate parameters
        try {
            validateGetFunctionInstanceRequestParams(tenant, namespace, componentName, componentType, instanceId);
        } catch (IllegalArgumentException e) {
            log.error("Invalid restart {} request @ /{}/{}/{}", componentType, tenant, namespace, componentName, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.error("{} does not exist @ /{}/{}/{}", componentType, tenant, namespace, componentName);
            throw new RestException(Status.NOT_FOUND, String.format("%s %s doesn't exist", componentType, componentName));
        }

        FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName, componentType);
            throw new RestException(Status.NOT_FOUND, String.format("%s %s doesn't exist", componentType, componentName));
        }

        FunctionRuntimeManager functionRuntimeManager = worker().getFunctionRuntimeManager();
        try {
            functionRuntimeManager.restartFunctionInstance(tenant, namespace, componentName,
                    Integer.parseInt(instanceId), uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("Failed to restart {}: {}/{}/{}/{}", componentType, tenant, namespace, componentName, instanceId, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    public void stopFunctionInstances(final String tenant,
                                      final String namespace,
                                      final String componentName,
                                      final String clientRole,
                                      final AuthenticationDataSource clientAuthenticationDataHttps) {
        changeFunctionStatusAllInstances(tenant, namespace, componentName, false, clientRole, clientAuthenticationDataHttps);
    }

    public void startFunctionInstances(final String tenant,
                                       final String namespace,
                                       final String componentName,
                                       final String clientRole,
                                       final AuthenticationDataSource clientAuthenticationDataHttps) {
        changeFunctionStatusAllInstances(tenant, namespace, componentName, true, clientRole, clientAuthenticationDataHttps);
    }

    public void changeFunctionStatusAllInstances(final String tenant,
                                                 final String namespace,
                                                 final String componentName,
                                                 final boolean start,
                                                 final String clientRole,
                                                 final AuthenticationDataSource clientAuthenticationDataHttps) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.error("{}/{}/{} Client [{}] is not admin and authorized to start/stop {}", tenant, namespace,
                        componentName, clientRole, componentType);
                throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, componentName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        // validate parameters
        try {
            validateGetFunctionRequestParams(tenant, namespace, componentName, componentType);
        } catch (IllegalArgumentException e) {
            log.error("Invalid start/stop {} request @ /{}/{}/{}", componentType, tenant, namespace, componentName, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.error("{} in stopFunctionInstances does not exist @ /{}/{}/{}", componentType, tenant, namespace, componentName);
            throw new RestException(Status.NOT_FOUND, String.format("%s %s doesn't exist", componentType, componentName));
        }

        FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName, componentType);
            throw new RestException(Status.NOT_FOUND, String.format("%s %s doesn't exist", componentType, componentName));
        }

        if (!functionMetaDataManager.canChangeState(functionMetaData, -1, start ? Function.FunctionState.RUNNING : Function.FunctionState.STOPPED)) {
            log.error("Operation not permitted on {}/{}/{}", tenant, namespace, componentName);
            throw new RestException(Status.BAD_REQUEST, String.format("Operation not permitted"));
        }

        try {
            functionMetaDataManager.changeFunctionInstanceStatus(tenant, namespace, componentName, -1, start);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("Failed to start/stop {}: {}/{}/{}", componentType, tenant, namespace, componentName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    public void restartFunctionInstances(final String tenant,
                                         final String namespace,
                                         final String componentName,
                                         final String clientRole,
                                         final AuthenticationDataSource clientAuthenticationDataHttps) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.error("{}/{}/{} Client [{}] is not admin and authorized to restart {}", tenant, namespace,
                        componentName, clientRole, componentType);
                throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, componentName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        // validate parameters
        try {
            validateGetFunctionRequestParams(tenant, namespace, componentName, componentType);
        } catch (IllegalArgumentException e) {
            log.error("Invalid restart {} request @ /{}/{}/{}", componentType, tenant, namespace, componentName, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.error("{} in stopFunctionInstances does not exist @ /{}/{}/{}", componentType, tenant, namespace, componentName);
            throw new RestException(Status.NOT_FOUND, String.format("%s %s doesn't exist", componentType, componentName));
        }

        FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName, componentType);
            throw new RestException(Status.NOT_FOUND, String.format("%s %s doesn't exist", componentType, componentName));
        }

        FunctionRuntimeManager functionRuntimeManager = worker().getFunctionRuntimeManager();
        try {
            functionRuntimeManager.restartFunctionInstances(tenant, namespace, componentName);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("Failed to restart {}: {}/{}/{}", componentType, tenant, namespace, componentName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    public FunctionStats getFunctionStats(final String tenant,
                                          final String namespace,
                                          final String componentName,
                                          final URI uri,
                                          final String clientRole,
                                          final AuthenticationDataSource clientAuthenticationDataHttps) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.error("{}/{}/{} Client [{}] is not admin and authorized to get stats for {}", tenant, namespace,
                        componentName, clientRole, componentType);
                throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, componentName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        // validate parameters
        try {
            validateGetFunctionRequestParams(tenant, namespace, componentName, componentType);
        } catch (IllegalArgumentException e) {
            log.error("Invalid get {} Stats request @ /{}/{}/{}", componentType, tenant, namespace, componentName, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.error("{} in get {} Stats does not exist @ /{}/{}/{}", componentType, componentType, tenant, namespace, componentName);
            throw new RestException(Status.NOT_FOUND, String.format("%s %s doesn't exist", componentType, componentName));
        }

        FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName, componentType);
            throw new RestException(Status.NOT_FOUND, String.format("%s %s doesn't exist", componentType, componentName));
        }

        FunctionRuntimeManager functionRuntimeManager = worker().getFunctionRuntimeManager();
        FunctionStats functionStats;
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

    public FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData getFunctionsInstanceStats(final String tenant,
                                                                                                   final String namespace,
                                                                                                   final String componentName,
                                                                                                   final String instanceId,
                                                                                                   final URI uri,
                                                                                                   final String clientRole,
                                                                                                   final AuthenticationDataSource clientAuthenticationDataHttps) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.error("{}/{}/{} Client [{}] is not admin and authorized to get stats for {}", tenant, namespace,
                        componentName, clientRole, componentType);
                throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, componentName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        // validate parameters
        try {
            validateGetFunctionInstanceRequestParams(tenant, namespace, componentName, componentType, instanceId);
        } catch (IllegalArgumentException e) {
            log.error("Invalid get {} Stats request @ /{}/{}/{}", componentType, tenant, namespace, componentName, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());

        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.error("{} in get {} Stats does not exist @ /{}/{}/{}", componentType, componentType, tenant, namespace, componentName);
            throw new RestException(Status.NOT_FOUND, String.format("%s %s doesn't exist", componentType, componentName));
        }
        FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName, componentType);
            throw new RestException(Status.NOT_FOUND, String.format("%s %s doesn't exist", componentType, componentName));

        }
        int instanceIdInt = Integer.parseInt(instanceId);
        if (instanceIdInt < 0 || instanceIdInt >= functionMetaData.getFunctionDetails().getParallelism()) {
            log.error("instanceId in get {} Stats out of bounds @ /{}/{}/{}", componentType, tenant, namespace, componentName);
            throw new RestException(Status.BAD_REQUEST, String.format("%s %s doesn't have instance with id %s", componentType, componentName, instanceId));
        }

        FunctionRuntimeManager functionRuntimeManager = worker().getFunctionRuntimeManager();
        FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData functionInstanceStatsData;
        try {
            functionInstanceStatsData = functionRuntimeManager.getFunctionInstanceStats(tenant, namespace, componentName,
                    Integer.parseInt(instanceId), uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Stats", tenant, namespace, componentName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        return functionInstanceStatsData;
    }

    public List<String> listFunctions(final String tenant,
                                      final String namespace,
                                      final String clientRole,
                                      final AuthenticationDataSource clientAuthenticationDataHttps) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.error("{}/{} Client [{}] is not admin and authorized to list {}", tenant, namespace, clientRole, componentType);
                throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{} Failed to authorize [{}]", tenant, namespace, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        // validate parameters
        try {
            validateListFunctionRequestParams(tenant, namespace);
        } catch (IllegalArgumentException e) {
            log.error("Invalid list {} request @ /{}/{}", componentType, tenant, namespace, e);
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

    void updateRequest(final FunctionMetaData functionMetaData) {

        // Submit to FMT
        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();

        CompletableFuture<RequestResult> completableFuture = functionMetaDataManager.updateFunction(functionMetaData);

        RequestResult requestResult = null;
        try {
            requestResult = completableFuture.get();
            if (!requestResult.isSuccess()) {
                throw new RestException(Status.BAD_REQUEST, requestResult.getMessage());
            }
        } catch (ExecutionException e) {
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        } catch (InterruptedException e) {
            throw new RestException(Status.REQUEST_TIMEOUT, e.getMessage());
        }

    }

    public List<ConnectorDefinition> getListOfConnectors() {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        return this.worker().getConnectorsManager().getConnectors();
    }

    public String triggerFunction(final String tenant,
                                  final String namespace,
                                  final String functionName,
                                  final String input,
                                  final InputStream uploadedInputStream,
                                  final String topic,
                                  final String clientRole,
                                  final AuthenticationDataSource clientAuthenticationDataHttps) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.error("{}/{}/{} Client [{}] is not admin and authorized to trigger {}", tenant, namespace,
                        functionName, clientRole, componentType);
                throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, functionName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        // validate parameters
        try {
            validateTriggerRequestParams(tenant, namespace, functionName, topic, input, uploadedInputStream);
        } catch (IllegalArgumentException e) {
            log.error("Invalid trigger function request @ /{}/{}/{}", tenant, namespace, functionName, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, functionName)) {
            log.error("Function in trigger function does not exist @ /{}/{}/{}", tenant, namespace, functionName);
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
            log.error("Function in trigger function has more than 1 input topics @ /{}/{}/{}", tenant, namespace, functionName);
            throw new RestException(Status.BAD_REQUEST, "Function in trigger function has more than 1 input topics");
            }
        if (functionMetaData.getFunctionDetails().getSource().getInputSpecsCount() == 0
                || !functionMetaData.getFunctionDetails().getSource().getInputSpecsMap()
                .containsKey(inputTopicToWrite)) {
            log.error("Function in trigger function has unidentified topic @ /{}/{}/{} {}", tenant, namespace, functionName, inputTopicToWrite);
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
                reader = worker().getClient().newReader().topic(outputTopic).startMessageId(MessageId.latest).create();
            }
            producer = worker().getClient().newProducer(Schema.AUTO_PRODUCE_BYTES())
                    .topic(inputTopicToWrite)
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
                if (msg == null)
                    break;
                if (msg.getProperties().containsKey("__pfn_input_msg_id__")
                        && msg.getProperties().containsKey("__pfn_input_topic__")) {
                    MessageId newMsgId = MessageId.fromByteArray(
                            Base64.getDecoder().decode((String) msg.getProperties().get("__pfn_input_msg_id__")));

                    if (msgId.equals(newMsgId)
                            && msg.getProperties().get("__pfn_input_topic__").equals(TopicName.get(inputTopicToWrite).toString())) {
                       return new String(msg.getData());
                    }
                }
                curTime = System.currentTimeMillis();
            }
            throw new RestException(Status.REQUEST_TIMEOUT, "Request Timed Out");
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

    public FunctionState getFunctionState(final String tenant,
                                          final String namespace,
                                          final String functionName,
                                          final String key,
                                          final String clientRole,
                                          final AuthenticationDataSource clientAuthenticationDataHttps) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.error("{}/{}/{} Client [{}] is not admin and authorized to get state for {}", tenant, namespace,
                        functionName, clientRole, componentType);
                throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, functionName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        if (null == worker().getStateStoreAdminClient()) {
            throwStateStoreUnvailableResponse();
        }

        // validate parameters
        try {
            validateGetFunctionStateParams(tenant, namespace, functionName, key);
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
                        value = new FunctionState(key, null, kv.numberValue(), kv.version());
                    } else {
                        value = new FunctionState(key, new String(ByteBufUtil.getBytes(kv.value()), UTF_8), null, kv.version());
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error while getFunctionState request @ /{}/{}/{}/{}",
                    tenant, namespace, functionName, key, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
        return value;
    }

    public void uploadFunction(final InputStream uploadedInputStream, final String path) {
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
            WorkerUtils.uploadToBookeeper(worker().getDlogNamespace(), uploadedInputStream, path);
        } catch (IOException e) {
            log.error("Error uploading file {}", path, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    public StreamingOutput downloadFunction(final String path) {

        final StreamingOutput streamingOutput = new StreamingOutput() {
            @Override
            public void write(final OutputStream output) throws IOException {
                if (path.startsWith(org.apache.pulsar.common.functions.Utils.HTTP)) {
                    URL url = new URL(path);
                    IOUtils.copy(url.openStream(), output);
                } else if (path.startsWith(org.apache.pulsar.common.functions.Utils.FILE)) {
                    URL url = new URL(path);
                    File file;
                    try {
                        file = new File(url.toURI());
                        IOUtils.copy(new FileInputStream(file), output);
                    } catch (URISyntaxException e) {
                        throw new IllegalArgumentException("invalid file url path: " + path);
                    }
                } else {
                    WorkerUtils.downloadFromBookkeeper(worker().getDlogNamespace(), output, path);
                }
            }
        };

        return streamingOutput;
    }

    private void validateListFunctionRequestParams(final String tenant, final String namespace) throws IllegalArgumentException {

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
                                                            final ComponentType componentType,
                                                            final String instanceId) throws IllegalArgumentException {
        validateGetFunctionRequestParams(tenant, namespace, componentName, componentType);
        if (instanceId == null) {
            throw new IllegalArgumentException(String.format("%s Instance Id is not provided", componentType));
        }
    }

    protected void validateGetFunctionRequestParams(String tenant, String namespace, String subject, ComponentType componentType)
            throws IllegalArgumentException {

        if (tenant == null) {
            throw new IllegalArgumentException("Tenant is not provided");
        }
        if (namespace == null) {
            throw new IllegalArgumentException("Namespace is not provided");
        }
        if (subject == null) {
            throw new IllegalArgumentException(componentType + " Name is not provided");
        }
    }

    private void validateDeregisterRequestParams(String tenant, String namespace, String subject, ComponentType componentType)
            throws IllegalArgumentException {

        if (tenant == null) {
            throw new IllegalArgumentException("Tenant is not provided");
        }
        if (namespace == null) {
            throw new IllegalArgumentException("Namespace is not provided");
        }
        if (subject == null) {
            throw new IllegalArgumentException(componentType + " Name is not provided");
        }
    }

    private void validateGetFunctionStateParams(final String tenant,
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
            throw new IllegalArgumentException("Function Name is not provided");
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

        return null;
    }

private FunctionDetails validateUpdateRequestParams(final String tenant,
                                                    final String namespace,
                                                    final String componentName,
                                                    final String componentConfigJson,
                                                    final ComponentType componentType,
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

        if (componentType.equals(FUNCTION) && !isEmpty(componentConfigJson)) {
            FunctionConfig functionConfig = new Gson().fromJson(componentConfigJson, FunctionConfig.class);
            // The rest end points take precedence over whatever is there in functionconfig
            functionConfig.setTenant(tenant);
            functionConfig.setNamespace(namespace);
            functionConfig.setName(componentName);
            FunctionConfigUtils.inferMissingArguments(functionConfig);
            ClassLoader clsLoader = FunctionConfigUtils.validate(functionConfig, componentPackageFile);
            return FunctionConfigUtils.convert(functionConfig, clsLoader);
        }
        if (componentType.equals(SOURCE)) {
            Path archivePath = null;
            SourceConfig sourceConfig = new Gson().fromJson(componentConfigJson, SourceConfig.class);
            // The rest end points take precedence over whatever is there in sourceconfig
            sourceConfig.setTenant(tenant);
            sourceConfig.setNamespace(namespace);
            sourceConfig.setName(componentName);
            org.apache.pulsar.common.functions.Utils.inferMissingArguments(sourceConfig);
            if (!StringUtils.isEmpty(sourceConfig.getArchive())) {
                String builtinArchive = sourceConfig.getArchive();
                if (builtinArchive.startsWith(org.apache.pulsar.common.functions.Utils.BUILTIN)) {
                    builtinArchive = builtinArchive.replaceFirst("^builtin://", "");
                }
                try {
                    archivePath = this.worker().getConnectorsManager().getSourceArchive(builtinArchive);
                } catch (Exception e) {
                    throw new IllegalArgumentException(String.format("No Source archive %s found", archivePath));
                }
            }
            SourceConfigUtils.ExtractedSourceDetails sourceDetails = SourceConfigUtils.validate(sourceConfig, archivePath, componentPackageFile);
            return SourceConfigUtils.convert(sourceConfig, sourceDetails);
        }
        if (componentType.equals(SINK)) {
            Path archivePath = null;
            SinkConfig sinkConfig = new Gson().fromJson(componentConfigJson, SinkConfig.class);
            // The rest end points take precedence over whatever is there in sinkConfig
            sinkConfig.setTenant(tenant);
            sinkConfig.setNamespace(namespace);
            sinkConfig.setName(componentName);
            org.apache.pulsar.common.functions.Utils.inferMissingArguments(sinkConfig);
            if (!StringUtils.isEmpty(sinkConfig.getArchive())) {
                String builtinArchive = sinkConfig.getArchive();
                if (builtinArchive.startsWith(org.apache.pulsar.common.functions.Utils.BUILTIN)) {
                    builtinArchive = builtinArchive.replaceFirst("^builtin://", "");
                }
                try {
                    archivePath = this.worker().getConnectorsManager().getSinkArchive(builtinArchive);
                } catch (Exception e) {
                    throw new IllegalArgumentException(String.format("No Sink archive %s found", archivePath));
                }
            }
            SinkConfigUtils.ExtractedSinkDetails sinkDetails = SinkConfigUtils.validate(sinkConfig, archivePath, componentPackageFile);
            return SinkConfigUtils.convert(sinkConfig, sinkDetails);
        } else {
            throw new IllegalArgumentException("Unrecognized component type: " + componentType);
        }
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
            throw new IllegalArgumentException("Function Name is not provided");
        }
        if (uploadedInputStream == null && input == null) {
            throw new IllegalArgumentException("Trigger Data is not provided");
        }
    }

    protected static void throwUnavailableException() {
        throw new RestException(Status.SERVICE_UNAVAILABLE,
                "Function worker service is not done initializing. " + "Please try again in a little while.");
    }

    private void throwStateStoreUnvailableResponse() {
        throw new RestException(Status.SERVICE_UNAVAILABLE,
                "State storage client is not done initializing. " + "Please try again in a little while.");
    }

    public static String createPackagePath(String tenant, String namespace, String functionName, String fileName) {
        return String.format("%s/%s/%s/%s", tenant, namespace, Codec.encode(functionName),
                getUniquePackageName(Codec.encode(fileName)));
    }

    public boolean isAuthorizedRole(String tenant, String namespace, String clientRole,
                                    AuthenticationDataSource authenticationData) throws PulsarAdminException {
        if (worker().getWorkerConfig().isAuthorizationEnabled()) {
            // skip authorization if client role is super-user
            if (isSuperUser(clientRole)) {
                return true;
            }
            TenantInfo tenantInfo = worker().getBrokerAdmin().tenants().getTenantInfo(tenant);
            if (clientRole != null && (tenantInfo.getAdminRoles() == null || tenantInfo.getAdminRoles().isEmpty()
                    || tenantInfo.getAdminRoles().contains(clientRole))) {
                return true;
            }

            // check if role has permissions granted
            if (clientRole != null && authenticationData != null) {
                return allowFunctionOps(NamespaceName.get(tenant, namespace), clientRole, authenticationData);
            } else {
                return false;
            }
        }
        return true;
    }


    protected void componentStatusRequestValidate (final String tenant, final String namespace, final String componentName,
                                                   final String clientRole,
                                                   final AuthenticationDataSource clientAuthenticationDataHttps) {
        if (!isWorkerServiceAvailable()) {
            throw new RestException(Status.SERVICE_UNAVAILABLE, "Function worker service is not done initializing. Please try again in a little while.");
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.error("{}/{}/{} Client [{}] is not admin and authorized get status for {}", tenant, namespace,
                        componentName, clientRole, componentType);
                throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, componentName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        // validate parameters
        try {
            validateGetFunctionRequestParams(tenant, namespace, componentName, componentType);
        } catch (IllegalArgumentException e) {
            log.error("Invalid get {} Status request @ /{}/{}/{}", componentType, tenant, namespace, componentName, e);
            throw new RestException(Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.error("{} in get {} Status does not exist @ /{}/{}/{}", componentType, componentType, tenant, namespace, componentName);
            throw new RestException(Status.NOT_FOUND, String.format("%s %s doesn't exist", componentType, componentName));
        }

        FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName, componentType);
            throw new RestException(Status.NOT_FOUND, String.format("%s %s doesn't exist", componentType, componentName));
        }
    }

    protected void componentInstanceStatusRequestValidate (final String tenant,
                                                           final String namespace,
                                                           final String componentName,
                                                           final int instanceId,
                                                           final String clientRole,
                                                           final AuthenticationDataSource clientAuthenticationDataHttps) {
        componentStatusRequestValidate(tenant, namespace, componentName, clientRole, clientAuthenticationDataHttps);

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        int parallelism = functionMetaData.getFunctionDetails().getParallelism();
        if (instanceId < 0 || instanceId >= parallelism) {
            log.error("instanceId in get {} Status out of bounds @ /{}/{}/{}", componentType, tenant, namespace, componentName);
            throw new RestException(Status.BAD_REQUEST,
                    String.format("%s %s doesn't have instance with id %s", componentType, componentName, instanceId));
        }
    }

    public boolean isSuperUser(String clientRole) {
        return clientRole != null
                && worker().getWorkerConfig().getSuperUserRoles() != null
                && worker().getWorkerConfig().getSuperUserRoles().contains(clientRole);
    }

    public boolean allowFunctionOps(NamespaceName namespaceName, String role,
                                    AuthenticationDataSource authenticationData) {
        try {
            return worker().getAuthorizationService().allowFunctionOpsAsync(
                    namespaceName, role, authenticationData).get(worker().getWorkerConfig().getZooKeeperOperationTimeoutSeconds(), SECONDS);
        } catch (InterruptedException e) {
            log.warn("Time-out {} sec while checking function authorization on {} ", worker().getWorkerConfig().getZooKeeperOperationTimeoutSeconds(), namespaceName);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        } catch (Exception e) {
            log.warn("Admin-client with Role - {} failed to get function permissions for namespace - {}. {}", role, namespaceName,
                    e.getMessage(), e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }
}
