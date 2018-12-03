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
package org.apache.pulsar.functions.worker;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;

import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.ExceptionInformation;
import org.apache.pulsar.common.policies.data.FunctionStats;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.Assignment;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.runtime.*;

import com.google.common.annotations.VisibleForTesting;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider;
import org.apache.pulsar.functions.secretsproviderconfigurator.DefaultSecretsProviderConfigurator;
import org.apache.pulsar.functions.secretsproviderconfigurator.SecretsProviderConfigurator;
import org.apache.pulsar.functions.utils.Reflections;

/**
 * This class managers all aspects of functions assignments and running of function assignments for this worker
 */
@Slf4j
public class FunctionRuntimeManager implements AutoCloseable{

    // all assignments
    // WorkerId -> Function Fully Qualified InstanceId -> List<Assignments>
    @VisibleForTesting
    Map<String, Map<String, Assignment>> workerIdToAssignments = new ConcurrentHashMap<>();

    // All the runtime info related to functions executed by this worker
    // Fully Qualified InstanceId - > FunctionRuntimeInfo
    // NOTE: please use setFunctionRuntimeInfo and deleteFunctionRuntimeInfo methods to modify this data structure
    // Since during initialization phase nothing should be modified
    @VisibleForTesting
    Map<String, FunctionRuntimeInfo> functionRuntimeInfoMap = new ConcurrentHashMap<>();

    @VisibleForTesting
    @Getter
    final WorkerConfig workerConfig;

    @VisibleForTesting
    LinkedBlockingQueue<FunctionAction> actionQueue;

    private FunctionAssignmentTailer functionAssignmentTailer;

    private FunctionActioner functionActioner;

    @Getter
    private RuntimeFactory runtimeFactory;

    private MembershipManager membershipManager;
    
    private final PulsarAdmin functionAdmin;
    
    @Getter
    private WorkerService workerService;

    @Setter
    @Getter
    boolean isInitializePhase = false;

    public FunctionRuntimeManager(WorkerConfig workerConfig, WorkerService workerService, Namespace dlogNamespace,
            MembershipManager membershipManager, ConnectorsManager connectorsManager) throws Exception {
        this.workerConfig = workerConfig;
        this.workerService = workerService;
        this.functionAdmin = workerService.getFunctionAdmin();

        SecretsProviderConfigurator secretsProviderConfigurator;
        if (!StringUtils.isEmpty(workerConfig.getSecretsProviderConfiguratorClassName())) {
            secretsProviderConfigurator = (SecretsProviderConfigurator) Reflections.createInstance(workerConfig.getSecretsProviderConfiguratorClassName(), ClassLoader.getSystemClassLoader());
        } else {
            secretsProviderConfigurator = new DefaultSecretsProviderConfigurator();
        }
        secretsProviderConfigurator.init(workerConfig.getSecretsProviderConfiguratorConfig());

        AuthenticationConfig authConfig = AuthenticationConfig.builder()
                .clientAuthenticationPlugin(workerConfig.getClientAuthenticationPlugin())
                .clientAuthenticationParameters(workerConfig.getClientAuthenticationParameters())
                .tlsTrustCertsFilePath(workerConfig.getTlsTrustCertsFilePath())
                .useTls(workerConfig.isUseTls()).tlsAllowInsecureConnection(workerConfig.isTlsAllowInsecureConnection())
                .tlsHostnameVerificationEnable(workerConfig.isTlsHostnameVerificationEnable()).build();

        if (workerConfig.getThreadContainerFactory() != null) {
            this.runtimeFactory = new ThreadRuntimeFactory(
                    workerConfig.getThreadContainerFactory().getThreadGroupName(),
                    workerConfig.getPulsarServiceUrl(),
                    workerConfig.getStateStorageServiceUrl(),
                    authConfig,
                    new ClearTextSecretsProvider(),
                     null);
        } else if (workerConfig.getProcessContainerFactory() != null) {
            this.runtimeFactory = new ProcessRuntimeFactory(
                    workerConfig.getPulsarServiceUrl(),
                    workerConfig.getStateStorageServiceUrl(),
                    authConfig,
                    workerConfig.getProcessContainerFactory().getJavaInstanceJarLocation(),
                    workerConfig.getProcessContainerFactory().getPythonInstanceLocation(),
                    workerConfig.getProcessContainerFactory().getLogDirectory(),
                    workerConfig.getProcessContainerFactory().getExtraFunctionDependenciesDir(),
                    secretsProviderConfigurator);
        } else if (workerConfig.getKubernetesContainerFactory() != null){
            this.runtimeFactory = new KubernetesRuntimeFactory(
                    workerConfig.getKubernetesContainerFactory().getK8Uri(),
                    workerConfig.getKubernetesContainerFactory().getJobNamespace(),
                    workerConfig.getKubernetesContainerFactory().getPulsarDockerImageName(),
                    workerConfig.getKubernetesContainerFactory().getPulsarRootDir(),
                    workerConfig.getKubernetesContainerFactory().getSubmittingInsidePod(),
                    workerConfig.getKubernetesContainerFactory().getInstallUserCodeDependencies(),
                    workerConfig.getKubernetesContainerFactory().getPythonDependencyRepository(),
                    workerConfig.getKubernetesContainerFactory().getPythonExtraDependencyRepository(),
                    workerConfig.getKubernetesContainerFactory().getExtraFunctionDependenciesDir(),
                    workerConfig.getKubernetesContainerFactory().getCustomLabels(),
                    StringUtils.isEmpty(workerConfig.getKubernetesContainerFactory().getPulsarServiceUrl()) ? workerConfig.getPulsarServiceUrl() : workerConfig.getKubernetesContainerFactory().getPulsarServiceUrl(),
                    StringUtils.isEmpty(workerConfig.getKubernetesContainerFactory().getPulsarAdminUrl()) ? workerConfig.getPulsarWebServiceUrl() : workerConfig.getKubernetesContainerFactory().getPulsarAdminUrl(),
                    workerConfig.getStateStorageServiceUrl(),
                    authConfig,
                    workerConfig.getKubernetesContainerFactory().getExpectedMetricsCollectionInterval() == null ? -1 : workerConfig.getKubernetesContainerFactory().getExpectedMetricsCollectionInterval(),
                    workerConfig.getKubernetesContainerFactory().getChangeConfigMap(),
                    workerConfig.getKubernetesContainerFactory().getChangeConfigMapNamespace(),
                    secretsProviderConfigurator);
        } else {
            throw new RuntimeException("Either Thread, Process or Kubernetes Container Factory need to be set");
        }

        this.actionQueue = new LinkedBlockingQueue<>();

        this.functionActioner = new FunctionActioner(this.workerConfig, runtimeFactory,
                dlogNamespace, actionQueue, connectorsManager);

        this.membershipManager = membershipManager;
    }

    /**
     * Initializes the FunctionRuntimeManager.  Does the following:
     * 1. Consume all existing assignments to establish existing/latest set of assignments
     * 2. After current assignments are read, assignments belonging to this worker will be processed
     */
    public void initialize() {
        log.info("/** Initializing Runtime Manager **/");
        try {
            Reader<byte[]> reader = this.getWorkerService().getClient().newReader()
                    .topic(this.getWorkerConfig().getFunctionAssignmentTopic()).readCompacted(true)
                    .startMessageId(MessageId.earliest).create();

            this.functionAssignmentTailer = new FunctionAssignmentTailer(this, reader);
            // read all existing messages
            this.setInitializePhase(true);
            while (reader.hasMessageAvailable()) {
                this.functionAssignmentTailer.processAssignment(reader.readNext());
            }
            this.setInitializePhase(false);
            // realize existing assignments
            Map<String, Assignment> assignmentMap = workerIdToAssignments.get(this.workerConfig.getWorkerId());
            if (assignmentMap != null) {
                for (Assignment assignment : assignmentMap.values()) {
                    startFunctionInstance(assignment);
                }
            }
            // start assignment tailer
            this.functionAssignmentTailer.start();

        } catch (Exception e) {
            log.error("Failed to initialize function runtime manager: ", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Starts the function runtime manager
     */
    public void start() {
        log.info("/** Starting Function Runtime Manager **/");
        log.info("Initialize metrics sink...");
        log.info("Starting function actioner...");
        this.functionActioner.start();
        log.info("Starting function assignment tailer...");
        this.functionAssignmentTailer.start();
    }

    /**
     * Public methods
     */

    /**
     * Get current assignments
     * @return a map of current assignments in the follwing format
     * {workerId : {FullyQualifiedInstanceId : Assignment}}
     */
    public synchronized Map<String, Map<String, Assignment>> getCurrentAssignments() {
        Map<String, Map<String, Assignment>> copy = new HashMap<>();
        for (Map.Entry<String, Map<String, Assignment>> entry : this.workerIdToAssignments.entrySet()) {
            Map<String, Assignment> tmp = new HashMap<>();
            tmp.putAll(entry.getValue());
            copy.put(entry.getKey(), tmp);
        }
        return copy;
    }

    /**
     * Find a assignment of a function
     * @param tenant the tenant the function belongs to
     * @param namespace the namespace the function belongs to
     * @param functionName the function name
     * @return the assignment of the function
     */
    public synchronized Assignment findFunctionAssignment(String tenant, String namespace,
                                                          String functionName, int instanceId) {
        return this.findAssignment(tenant, namespace, functionName, instanceId);
    }

    /**
     * Find all instance assignments of function
     * @param tenant
     * @param namespace
     * @param functionName
     * @return
     */
    public synchronized Collection<Assignment> findFunctionAssignments(String tenant,
                                                                       String namespace, String functionName) {
        return findFunctionAssignments(tenant, namespace, functionName, this.workerIdToAssignments);
    }

    public static Collection<Assignment> findFunctionAssignments(String tenant,
                                                                 String namespace, String functionName,
                                                                 Map<String, Map<String, Assignment>> workerIdToAssignments) {

        Collection<Assignment> assignments = new LinkedList<>();

        for (Map<String, Assignment> entryMap : workerIdToAssignments.values()) {
            assignments.addAll(entryMap.values().stream()
                    .filter(
                            assignment ->
                                    (tenant.equals(assignment.getInstance()
                                            .getFunctionMetaData().getFunctionDetails()
                                            .getTenant())
                                            && namespace.equals((assignment.getInstance()
                                            .getFunctionMetaData().getFunctionDetails()
                                            .getNamespace()))
                                            && functionName.equals(assignment.getInstance()
                                            .getFunctionMetaData().getFunctionDetails()
                                            .getName())))
                    .collect(Collectors.toList()));
        }

        return assignments;
    }


    /**
     * Removes a collection of assignments
     * @param assignments assignments to remove
     */
    public synchronized void removeAssignments(Collection<Assignment> assignments) {
        for (Assignment assignment : assignments) {
            this.deleteAssignment(assignment);
        }
    }

    public Response stopFunctionInstance(String tenant, String namespace, String functionName, int instanceId,
            boolean restart, URI uri) throws Exception {
        if (runtimeFactory.externallyManaged()) {
            return Response.status(Status.NOT_IMPLEMENTED).type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData("Externally managed schedulers can't do per instance stop")).build();
        }
        Assignment assignment = this.findAssignment(tenant, namespace, functionName, instanceId);
        final String fullFunctionName = String.format("%s/%s/%s/%s", tenant, namespace, functionName, instanceId);
        if (assignment == null) {
            return Response.status(Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(fullFunctionName + " doesn't exist")).build();
        }

        final String assignedWorkerId = assignment.getWorkerId();
        final String workerId = this.workerConfig.getWorkerId();

        if (assignedWorkerId.equals(workerId)) {
            stopFunction(org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance()), restart);
            return Response.status(Status.OK).build();
        } else {
            // query other worker
            List<WorkerInfo> workerInfoList = this.membershipManager.getCurrentMembership();
            WorkerInfo workerInfo = null;
            for (WorkerInfo entry : workerInfoList) {
                if (assignment.getWorkerId().equals(entry.getWorkerId())) {
                    workerInfo = entry;
                }
            }
            if (workerInfo == null) {
                return Response.status(Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON)
                        .entity(new ErrorData(fullFunctionName + " has not been assigned yet")).build();
            }

            if (uri == null) {
                throw new WebApplicationException(Response.serverError().status(Status.INTERNAL_SERVER_ERROR).build());
            } else {
                URI redirect = UriBuilder.fromUri(uri).host(workerInfo.getWorkerHostname()).port(workerInfo.getPort()).build();
                throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
            }
        }
    }

    public Response stopFunctionInstances(String tenant, String namespace, String functionName, boolean restart)
            throws Exception {
        final String fullFunctionName = String.format("%s/%s/%s", tenant, namespace, functionName);
        Collection<Assignment> assignments = this.findFunctionAssignments(tenant, namespace, functionName);

        if (assignments.isEmpty()) {
            return Response.status(Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(fullFunctionName + " has not been assigned yet")).build();
        }
        if (runtimeFactory.externallyManaged()) {
            Assignment assignment = assignments.iterator().next();
            final String assignedWorkerId = assignment.getWorkerId();
            final String workerId = this.workerConfig.getWorkerId();
            String fullyQualifiedInstanceId = org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance());
            if (assignedWorkerId.equals(workerId)) {
                stopFunction(fullyQualifiedInstanceId, restart);
            } else {
                List<WorkerInfo> workerInfoList = this.membershipManager.getCurrentMembership();
                WorkerInfo workerInfo = null;
                for (WorkerInfo entry : workerInfoList) {
                    if (assignment.getWorkerId().equals(entry.getWorkerId())) {
                        workerInfo = entry;
                    }
                }
                if (workerInfo == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] has not been assigned yet", fullyQualifiedInstanceId);
                    }
                    return Response.status(Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON)
                            .entity(new ErrorData(fullFunctionName + " has not been assigned yet")).build();
                }
                if (restart) {
                    this.functionAdmin.functions().restartFunction(tenant, namespace, functionName);
                } else {
                    this.functionAdmin.functions().stopFunction(tenant, namespace, functionName);
                }
            }
        } else {
            for (Assignment assignment : assignments) {
                final String assignedWorkerId = assignment.getWorkerId();
                final String workerId = this.workerConfig.getWorkerId();
                String fullyQualifiedInstanceId = org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance());
                if (assignedWorkerId.equals(workerId)) {
                    stopFunction(fullyQualifiedInstanceId, restart);
                } else {
                    List<WorkerInfo> workerInfoList = this.membershipManager.getCurrentMembership();
                    WorkerInfo workerInfo = null;
                    for (WorkerInfo entry : workerInfoList) {
                        if (assignment.getWorkerId().equals(entry.getWorkerId())) {
                            workerInfo = entry;
                        }
                    }
                    if (workerInfo == null) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] has not been assigned yet", fullyQualifiedInstanceId);
                        }
                        continue;
                    }
                    if (restart) {
                        this.functionAdmin.functions().restartFunction(tenant, namespace, functionName,
                                assignment.getInstance().getInstanceId());
                    } else {
                        this.functionAdmin.functions().stopFunction(tenant, namespace, functionName,
                                assignment.getInstance().getInstanceId());
                    }
                }
            }
        }
        return Response.status(Status.OK).build();
    }

    /**
     * It stops all functions instances owned by current worker
     * @throws Exception
     */
    public void stopAllOwnedFunctions() {
        if (runtimeFactory.externallyManaged()) {
            log.warn("Will not stop any functions since they are externally managed");
            return;
        }
        final String workerId = this.workerConfig.getWorkerId();
        Map<String, Assignment> assignments = workerIdToAssignments.get(workerId);
        if (assignments != null) {
            assignments.values().forEach(assignment -> {
                String fullyQualifiedInstanceId = org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance());
                try {
                    stopFunction(fullyQualifiedInstanceId, false);
                } catch (Exception e) {
                    log.warn("Failed to stop function {} - {}", fullyQualifiedInstanceId, e.getMessage());
                }
            });
        }
    }

    private void stopFunction(String fullyQualifiedInstanceId, boolean restart) throws Exception {
        log.info("[{}] {}..", restart ? "restarting" : "stopping", fullyQualifiedInstanceId);
        FunctionRuntimeInfo functionRuntimeInfo = this.getFunctionRuntimeInfo(fullyQualifiedInstanceId);
        if (functionRuntimeInfo != null) {
            this.functionActioner.stopFunction(functionRuntimeInfo);
            try {
                if(restart) {
                    this.functionActioner.startFunction(functionRuntimeInfo);
                }
            } catch (Exception ex) {
                log.info("{} Error re-starting function", fullyQualifiedInstanceId, ex);
                functionRuntimeInfo.setStartupException(ex);
                throw ex;
            }
        }
    }

    /**
     * Get stats of a function instance.  If this worker is not running the function instance,
     * @param tenant the tenant the function belongs to
     * @param namespace the namespace the function belongs to
     * @param functionName the function name
     * @param instanceId the function instance id
     * @return jsonObject containing stats for instance
     */
    public FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData getFunctionInstanceStats(String tenant, String namespace,
                                                                        String functionName, int instanceId, URI uri) {
        Assignment assignment;
        if (runtimeFactory.externallyManaged()) {
            assignment = this.findAssignment(tenant, namespace, functionName, -1);
        } else {
            assignment = this.findAssignment(tenant, namespace, functionName, instanceId);
        }

        if (assignment == null) {
            return new FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData();
        }

        final String assignedWorkerId = assignment.getWorkerId();
        final String workerId = this.workerConfig.getWorkerId();

        // If I am running worker
        if (assignedWorkerId.equals(workerId)) {
            FunctionRuntimeInfo functionRuntimeInfo = this.getFunctionRuntimeInfo(
                    org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance()));
            RuntimeSpawner runtimeSpawner = functionRuntimeInfo.getRuntimeSpawner();
            if (runtimeSpawner != null) {
                return Utils.getFunctionInstanceStats(org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance()), functionRuntimeInfo).getMetrics();
            }
            return new FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData();
        } else {
            // query other worker

            List<WorkerInfo> workerInfoList = this.membershipManager.getCurrentMembership();
            WorkerInfo workerInfo = null;
            for (WorkerInfo entry: workerInfoList) {
                if (assignment.getWorkerId().equals(entry.getWorkerId())) {
                    workerInfo = entry;
                }
            }
            if (workerInfo == null) {
                return new FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData();
            }

            if (uri == null) {
                throw new WebApplicationException(Response.serverError().status(Status.INTERNAL_SERVER_ERROR).build());
            } else {
                URI redirect = UriBuilder.fromUri(uri).host(workerInfo.getWorkerHostname()).port(workerInfo.getPort()).build();
                throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
            }
        }
    }

    /**
     * Get stats of all function instances.
     * @param tenant the tenant the function belongs to
     * @param namespace the namespace the function belongs to
     * @param functionName the function name
     * @return a list of function statuses
     * @throws PulsarAdminException
     */
    public FunctionStats getFunctionStats(String tenant, String namespace, String functionName, URI uri) throws PulsarAdminException {
        Collection<Assignment> assignments = this.findFunctionAssignments(tenant, namespace, functionName);

        FunctionStats functionStats = new FunctionStats();
        if (assignments.isEmpty()) {
            return functionStats;
        }

        if (runtimeFactory.externallyManaged()) {
            Assignment assignment = assignments.iterator().next();
            boolean isOwner = this.workerConfig.getWorkerId().equals(assignment.getWorkerId());
            if (isOwner) {
                int parallelism = assignment.getInstance().getFunctionMetaData().getFunctionDetails().getParallelism();
                for (int i = 0; i < parallelism; ++i) {

                    FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData functionInstanceStatsData = getFunctionInstanceStats(tenant, namespace,
                            functionName, i, null);

                    FunctionStats.FunctionInstanceStats functionInstanceStats = new FunctionStats.FunctionInstanceStats();
                    functionInstanceStats.setInstanceId(i);
                    functionInstanceStats.setMetrics(functionInstanceStatsData);
                    functionStats.addInstance(functionInstanceStats);
                }
            } else {
                // find the hostname/port of the worker who is the owner

                List<WorkerInfo> workerInfoList = this.membershipManager.getCurrentMembership();
                WorkerInfo workerInfo = null;
                for (WorkerInfo entry: workerInfoList) {
                    if (assignment.getWorkerId().equals(entry.getWorkerId())) {
                        workerInfo = entry;
                    }
                }
                if (workerInfo == null) {
                    return functionStats;
                }

                if (uri == null) {
                    throw new WebApplicationException(Response.serverError().status(Status.INTERNAL_SERVER_ERROR).build());
                } else {
                    URI redirect = UriBuilder.fromUri(uri).host(workerInfo.getWorkerHostname()).port(workerInfo.getPort()).build();
                    throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                }
            }
        } else {
            for (Assignment assignment : assignments) {
                boolean isOwner = this.workerConfig.getWorkerId().equals(assignment.getWorkerId());

                FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData functionInstanceStatsData;
                if (isOwner) {
                    functionInstanceStatsData = getFunctionInstanceStats(tenant, namespace, functionName,
                            assignment.getInstance().getInstanceId(), null);
                } else {
                    functionInstanceStatsData = this.functionAdmin.functions().getFunctionStats(
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getTenant(),
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getNamespace(),
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getName(),
                            assignment.getInstance().getInstanceId());
                }

                FunctionStats.FunctionInstanceStats functionInstanceStats = new FunctionStats.FunctionInstanceStats();
                functionInstanceStats.setInstanceId(assignment.getInstance().getInstanceId());
                functionInstanceStats.setMetrics(functionInstanceStatsData);
                functionStats.addInstance(functionInstanceStats);
            }
        }
        return functionStats.calculateOverall();
    }

    /**
     * Get status of a function instance.  If this worker is not running the function instance,
     * @param tenant the tenant the function belongs to
     * @param namespace the namespace the function belongs to
     * @param functionName the function name
     * @param instanceId the function instance id
     * @return the function status
     */
    public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData getFunctionInstanceStatus(
            String tenant, String namespace,
            String functionName, int instanceId, URI uri) {
        Assignment assignment;
        if (runtimeFactory.externallyManaged()) {
            assignment = this.findAssignment(tenant, namespace, functionName, -1);
        } else {
            assignment = this.findAssignment(tenant, namespace, functionName, instanceId);
        }

        if (assignment == null) {
            FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData
                    = new FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData();
            functionInstanceStatusData.setRunning(false);
            functionInstanceStatusData.setError("Function has not been scheduled");
            return functionInstanceStatusData;
        }

        final String assignedWorkerId = assignment.getWorkerId();
        final String workerId = this.workerConfig.getWorkerId();

        // If I am running worker
        if (assignedWorkerId.equals(workerId)) {
            FunctionRuntimeInfo functionRuntimeInfo = this.getFunctionRuntimeInfo(
                    org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance()));
            RuntimeSpawner runtimeSpawner = functionRuntimeInfo.getRuntimeSpawner();
            FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData
                    = new FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData();
            if (runtimeSpawner != null) {
                try {
                    InstanceCommunication.FunctionStatus status = functionRuntimeInfo.getRuntimeSpawner().getFunctionStatus(instanceId).get();
                    functionInstanceStatusData.setRunning(status.getRunning());
                    functionInstanceStatusData.setError(status.getFailureException());
                    functionInstanceStatusData.setNumRestarts(status.getNumRestarts());
                    functionInstanceStatusData.setNumReceived(status.getNumReceived());
                    functionInstanceStatusData.setNumSuccessfullyProcessed(status.getNumSuccessfullyProcessed());
                    functionInstanceStatusData.setNumUserExceptions(status.getNumUserExceptions());

                    List<ExceptionInformation> userExceptionInformationList = new LinkedList<>();
                    for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestUserExceptionsList()) {
                        ExceptionInformation exceptionInformation
                                = new ExceptionInformation();
                        exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
                        exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
                        userExceptionInformationList.add(exceptionInformation);
                    }
                    functionInstanceStatusData.setLatestUserExceptions(userExceptionInformationList);

                    functionInstanceStatusData.setNumSystemExceptions(status.getNumSystemExceptions());
                    List<ExceptionInformation> systemExceptionInformationList = new LinkedList<>();
                    for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSystemExceptionsList()) {
                        ExceptionInformation exceptionInformation
                                = new ExceptionInformation();
                        exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
                        exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
                        systemExceptionInformationList.add(exceptionInformation);
                    }
                    functionInstanceStatusData.setLatestSystemExceptions(systemExceptionInformationList);

                    functionInstanceStatusData.setAverageLatency(status.getAverageLatency());
                    functionInstanceStatusData.setLastInvocationTime(status.getLastInvocationTime());
                    functionInstanceStatusData.setWorkerId(assignedWorkerId);


                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            } else {
                functionInstanceStatusData.setRunning(false);
                if (functionRuntimeInfo.getStartupException() != null) {
                    functionInstanceStatusData.setError(functionRuntimeInfo.getStartupException().getMessage());
                }
                functionInstanceStatusData.setWorkerId(assignedWorkerId);
            }
            return functionInstanceStatusData;
        } else {
            // query other worker

            List<WorkerInfo> workerInfoList = this.membershipManager.getCurrentMembership();
            WorkerInfo workerInfo = null;
            for (WorkerInfo entry: workerInfoList) {
                if (assignment.getWorkerId().equals(entry.getWorkerId())) {
                    workerInfo = entry;
                }
            }
            if (workerInfo == null) {
                FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData
                        = new FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData();
                functionInstanceStatusData.setRunning(false);
                functionInstanceStatusData.setError("Function has not been scheduled");
                return functionInstanceStatusData;
            }

            if (uri == null) {
                throw new WebApplicationException(Response.serverError().status(Status.INTERNAL_SERVER_ERROR).build());
            } else {
                URI redirect = UriBuilder.fromUri(uri).host(workerInfo.getWorkerHostname()).port(workerInfo.getPort()).build();
                throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
            }
        }
    }

    /**
     * Get statuses of all function instances.
     * @param tenant the tenant the function belongs to
     * @param namespace the namespace the function belongs to
     * @param functionName the function name
     * @return a list of function statuses
     * @throws PulsarAdminException 
     */
    public FunctionStatus getFunctionStatus(String tenant, String namespace,
                                            String functionName, URI uri)
            throws PulsarAdminException {

        Collection<Assignment> assignments = this.findFunctionAssignments(tenant, namespace, functionName);

        FunctionStatus functionStatus = new FunctionStatus();
        if (assignments.isEmpty()) {
            Function.FunctionMetaData functionMetaData = workerService.getFunctionMetaDataManager().getFunctionMetaData(tenant, namespace, functionName);
            functionStatus.setNumInstances(functionMetaData.getFunctionDetails().getParallelism());
            functionStatus.setNumRunning(0);

            return functionStatus;
        }

        // TODO refactor the code for externally managed.
        if (runtimeFactory.externallyManaged()) {
            Assignment assignment = assignments.iterator().next();
            boolean isOwner = this.workerConfig.getWorkerId().equals(assignment.getWorkerId());
            if (isOwner) {
                int parallelism = assignment.getInstance().getFunctionMetaData().getFunctionDetails().getParallelism();
                for (int i = 0; i < parallelism; ++i) {
                    FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData
                            = getFunctionInstanceStatus(tenant, namespace, functionName, i, null);
                    FunctionStatus.FunctionInstanceStatus functionInstanceStatus
                            = new FunctionStatus.FunctionInstanceStatus();
                    functionInstanceStatus.setInstanceId(i);
                    functionInstanceStatus.setStatus(functionInstanceStatusData);
                    functionStatus.addInstance(functionInstanceStatus);
                }
            } else {
                // find the hostname/port of the worker who is the owner

                List<WorkerInfo> workerInfoList = this.membershipManager.getCurrentMembership();
                WorkerInfo workerInfo = null;
                for (WorkerInfo entry: workerInfoList) {
                    if (assignment.getWorkerId().equals(entry.getWorkerId())) {
                        workerInfo = entry;
                    }
                }
                if (workerInfo == null) {
                    Function.FunctionMetaData functionMetaData = workerService.getFunctionMetaDataManager().getFunctionMetaData(tenant, namespace, functionName);
                    functionStatus.setNumInstances(functionMetaData.getFunctionDetails().getParallelism());
                    functionStatus.setNumRunning(0);
                    return functionStatus;
                }

                if (uri == null) {
                    throw new WebApplicationException(Response.serverError().status(Status.INTERNAL_SERVER_ERROR).build());
                } else {
                    URI redirect = UriBuilder.fromUri(uri).host(workerInfo.getWorkerHostname()).port(workerInfo.getPort()).build();
                    throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                }
            }
        } else {
            for (Assignment assignment : assignments) {
                boolean isOwner = this.workerConfig.getWorkerId().equals(assignment.getWorkerId());
                FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData;
                if (isOwner) {
                    functionInstanceStatusData = getFunctionInstanceStatus(tenant, namespace, functionName, assignment.getInstance().getInstanceId(), null);
                } else {
                    functionInstanceStatusData = this.functionAdmin.functions().getFunctionStatus(
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
        }
        functionStatus.setNumInstances(functionStatus.instances.size());
        functionStatus.getInstances().forEach(functionInstanceStatus -> {
            if (functionInstanceStatus.getStatus().isRunning()) {
                functionStatus.numRunning++;
            }
        });
        return functionStatus;
    }

    /**
     * Process an assignment update from the assignment topic
     * @param newAssignment the assignment
     */
    public synchronized void processAssignment(Assignment newAssignment) {

        Map<String, Assignment> existingAssignmentMap = new HashMap<>();
        for (Map<String, Assignment> entry : this.workerIdToAssignments.values()) {
            existingAssignmentMap.putAll(entry);
        }

        if (existingAssignmentMap.containsKey(org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(newAssignment.getInstance()))) {
            updateAssignment(newAssignment);
        } else {
            addAssignment(newAssignment);
        }
    }

    private void updateAssignment(Assignment assignment) {
        String fullyQualifiedInstanceId = org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance());
        Assignment existingAssignment = this.findAssignment(assignment);
        // potential updates need to happen
        if (!existingAssignment.equals(assignment)) {
            FunctionRuntimeInfo functionRuntimeInfo = this.functionRuntimeInfoMap.get(fullyQualifiedInstanceId);
            //stop function
            if (functionRuntimeInfo != null) {
                this.insertStopAction(functionRuntimeInfo);
            }
            // still assigned to me, need to restart
            if (assignment.getWorkerId().equals(this.workerConfig.getWorkerId())) {
                //start again
                FunctionRuntimeInfo newFunctionRuntimeInfo = new FunctionRuntimeInfo();
                newFunctionRuntimeInfo.setFunctionInstance(assignment.getInstance());
                this.insertStartAction(newFunctionRuntimeInfo);
                this.setFunctionRuntimeInfo(fullyQualifiedInstanceId, newFunctionRuntimeInfo);
            }

            // find existing assignment
            Assignment existing_assignment = this.findAssignment(assignment);
            if (existing_assignment != null) {
                // delete old assignment that could have old data
                this.deleteAssignment(existing_assignment);
            }
            // set to newest assignment
            this.setAssignment(assignment);
        }
    }
   
    public synchronized void deleteAssignment(String fullyQualifiedInstanceId) {
        FunctionRuntimeInfo functionRuntimeInfo = this.functionRuntimeInfoMap.get(fullyQualifiedInstanceId);
        if (functionRuntimeInfo != null) {
            this.insertStopAction(functionRuntimeInfo);
            this.deleteFunctionRuntimeInfo(fullyQualifiedInstanceId);
        }
        
        String workerId = null;
        for(Entry<String, Map<String, Assignment>> workerAssignments : workerIdToAssignments.entrySet()) {
            if(workerAssignments.getValue().remove(fullyQualifiedInstanceId)!=null) {
                workerId = workerAssignments.getKey();
                break;
            }
        }
        Map<String, Assignment> worker;
        if (workerId != null && ((worker = workerIdToAssignments.get(workerId)) != null && worker.isEmpty())) {
            this.workerIdToAssignments.remove(workerId);
        }
    }

    @VisibleForTesting
    void deleteAssignment(Assignment assignment) {
        String fullyQualifiedInstanceId = org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance());
        Map<String, Assignment> assignmentMap = this.workerIdToAssignments.get(assignment.getWorkerId());
        if (assignmentMap != null) {
            if (assignmentMap.containsKey(fullyQualifiedInstanceId)) {
                assignmentMap.remove(fullyQualifiedInstanceId);
            }
            if (assignmentMap.isEmpty()) {
                this.workerIdToAssignments.remove(assignment.getWorkerId());
            }
        }
    }

    private void addAssignment(Assignment assignment) {
        //add new function
        this.setAssignment(assignment);

        //Assigned to me
        if (assignment.getWorkerId().equals(workerConfig.getWorkerId())) {
            startFunctionInstance(assignment);
        }
    }

    private void startFunctionInstance(Assignment assignment) {
        String fullyQualifiedInstanceId = org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance());
        if (!this.functionRuntimeInfoMap.containsKey(fullyQualifiedInstanceId)) {
            this.setFunctionRuntimeInfo(fullyQualifiedInstanceId, new FunctionRuntimeInfo()
                    .setFunctionInstance(assignment.getInstance()));

        } else {
            //Somehow this function is already started
            log.warn("Function {} already running. Going to restart function.",
                    this.functionRuntimeInfoMap.get(fullyQualifiedInstanceId));
            this.insertStopAction(this.functionRuntimeInfoMap.get(fullyQualifiedInstanceId));
        }
        FunctionRuntimeInfo functionRuntimeInfo = this.functionRuntimeInfoMap.get(fullyQualifiedInstanceId);
        this.insertStartAction(functionRuntimeInfo);
    }

    public Map<String, FunctionRuntimeInfo> getFunctionRuntimeInfos() {
        return this.functionRuntimeInfoMap;
    }

    /**
     * Private methods for internal use.  Should not be used outside of this class
     */

    @VisibleForTesting
    void insertStopAction(FunctionRuntimeInfo functionRuntimeInfo) {
        if (!this.isInitializePhase) {
            FunctionAction functionAction = new FunctionAction();
            functionAction.setAction(FunctionAction.Action.STOP);
            functionAction.setFunctionRuntimeInfo(functionRuntimeInfo);
            try {
                actionQueue.put(functionAction);
            } catch (InterruptedException ex) {
                throw new RuntimeException("Interrupted while putting action");
            }
        }
    }

    @VisibleForTesting
    void insertStartAction(FunctionRuntimeInfo functionRuntimeInfo) {
        if (!this.isInitializePhase) {
            FunctionAction functionAction = new FunctionAction();
            functionAction.setAction(FunctionAction.Action.START);
            functionAction.setFunctionRuntimeInfo(functionRuntimeInfo);
            try {
                actionQueue.put(functionAction);
            } catch (InterruptedException ex) {
                throw new RuntimeException("Interrupted while putting action");
            }
        }
    }

    private Assignment findAssignment(String tenant, String namespace, String functionName, int instanceId) {
        String fullyQualifiedInstanceId
                = org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(tenant, namespace, functionName, instanceId);
        for (Map.Entry<String, Map<String, Assignment>> entry : this.workerIdToAssignments.entrySet()) {
            Map<String, Assignment> assignmentMap = entry.getValue();
            Assignment existingAssignment = assignmentMap.get(fullyQualifiedInstanceId);
            if (existingAssignment != null) {
                return existingAssignment;
            }
        }
        return null;
    }

    private Assignment findAssignment(Assignment assignment) {
        return findAssignment(
                assignment.getInstance().getFunctionMetaData().getFunctionDetails().getTenant(),
                assignment.getInstance().getFunctionMetaData().getFunctionDetails().getNamespace(),
                assignment.getInstance().getFunctionMetaData().getFunctionDetails().getName(),
                assignment.getInstance().getInstanceId()
        );
    }

    @VisibleForTesting
    void setAssignment(Assignment assignment) {
        if (!this.workerIdToAssignments.containsKey(assignment.getWorkerId())) {
            this.workerIdToAssignments.put(assignment.getWorkerId(), new HashMap<>());
        }
        this.workerIdToAssignments.get(assignment.getWorkerId()).put(
                org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance()),
                assignment);
    }

    private void deleteFunctionRuntimeInfo(String fullyQualifiedInstanceId) {
        if (!this.isInitializePhase) {
            this.functionRuntimeInfoMap.remove(fullyQualifiedInstanceId);
        }
    }

    private void setFunctionRuntimeInfo(String fullyQualifiedInstanceId, FunctionRuntimeInfo functionRuntimeInfo) {
        // Don't modify Function Runtime Infos when initializing
        if (!this.isInitializePhase) {
            this.functionRuntimeInfoMap.put(fullyQualifiedInstanceId, functionRuntimeInfo);
        }
    }

    @Override
    public void close() throws Exception {
        stopAllOwnedFunctions();
        this.functionActioner.close();
        this.functionAssignmentTailer.close();
        if (runtimeFactory != null) {
            runtimeFactory.close();
        }
    }

    private FunctionRuntimeInfo getFunctionRuntimeInfo(String fullyQualifiedInstanceId) {
        return this.functionRuntimeInfoMap.get(fullyQualifiedInstanceId);
    }

}
