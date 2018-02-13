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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderConfiguration;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.Assignment;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.Request.AssignmentsUpdate;
import org.apache.pulsar.functions.runtime.container.FunctionContainerFactory;
import org.apache.pulsar.functions.runtime.container.ProcessFunctionContainerFactory;
import org.apache.pulsar.functions.runtime.container.ThreadFunctionContainerFactory;
import org.apache.pulsar.functions.runtime.metrics.MetricsSink;
import org.apache.pulsar.functions.runtime.spawner.Spawner;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class managers all aspects of functions assignments and running of function assignments for this worker
 */
@Slf4j
public class FunctionRuntimeManager implements AutoCloseable{

    // all assignments
    // WorkerId -> Function Fully Qualified Name -> List<Assignments>
    @VisibleForTesting
    Map<String, Map<String, Assignment>> workerIdToAssignments = new ConcurrentHashMap<>();

    // All the runtime info related to functions executed by this worker
    // Fully qualified name - > FunctionRuntimeInfo
    @VisibleForTesting
    Map<String, FunctionRuntimeInfo> functionRuntimeInfoMap = new ConcurrentHashMap<>();

    @VisibleForTesting
    final WorkerConfig workerConfig;

    @VisibleForTesting
    LinkedBlockingQueue<FunctionAction> actionQueue;

    private long currentAssignmentVersion = 0;

    private final FunctionAssignmentTailer functionAssignmentTailer;

    private MetricsSink metricsSink;

    private FunctionActioner functionActioner;

    private FunctionContainerFactory functionContainerFactory;

    private MembershipManager membershipManager;


    public FunctionRuntimeManager(WorkerConfig workerConfig,
                                  PulsarClient pulsarClient,
                                  Namespace dlogNamespace,
                                  MembershipManager membershipManager) throws Exception {
        this.workerConfig = workerConfig;

        Reader reader = pulsarClient.createReader(
                this.workerConfig.getFunctionAssignmentTopic(),
                MessageId.latest,
                new ReaderConfiguration());
        this.functionAssignmentTailer = new FunctionAssignmentTailer(this, reader);

        if (workerConfig.getThreadContainerFactory() != null) {
            this.functionContainerFactory = new ThreadFunctionContainerFactory(
                    workerConfig.getThreadContainerFactory().getThreadGroupName(),
                    workerConfig.getPulsarServiceUrl(),
                    workerConfig.getStateStorageServiceUrl());
        } else if (workerConfig.getProcessContainerFactory() != null) {
            this.functionContainerFactory = new ProcessFunctionContainerFactory(
                    workerConfig.getPulsarServiceUrl(),
                    workerConfig.getProcessContainerFactory().getJavaInstanceJarLocation(),
                    workerConfig.getProcessContainerFactory().getPythonInstanceLocation(),
                    workerConfig.getProcessContainerFactory().getLogDirectory());
        } else {
            throw new RuntimeException("Either Thread or Process Container Factory need to be set");
        }

        this.metricsSink = createMetricsSink();

        this.actionQueue = new LinkedBlockingQueue<>();

        this.functionActioner = new FunctionActioner(this.workerConfig, functionContainerFactory,
                this.metricsSink, this.workerConfig.getMetricsConfig().getMetricsCollectionInterval(),
                dlogNamespace, actionQueue);

        this.membershipManager = membershipManager;
    }

    /**
     * Starts the function runtime manager
     */
    public void start() {
        log.info("/** Starting Function Runtime Manager **/");
        log.info("Initialize metrics sink...");
        this.metricsSink.init(this.workerConfig.getMetricsConfig().getMetricsSinkConfig());
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
     * {workerId : {FullyQualifiedFunctionName : Assignment}}
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
    public synchronized Assignment findFunctionAssignment(String tenant, String namespace, String functionName) {
        return this.findAssignment(tenant, namespace, functionName);
    }

    /**
     * get the current version number of assignments
     * @return assignments version number
     */
    public synchronized long getCurrentAssignmentVersion() {
        return new Long(this.currentAssignmentVersion);
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

    /**
     * Get status of a function.  If this worker is not running the function, route to worker that is to get the status
     * @param tenant the tenant the function belongs to
     * @param namespace the namespace the function belongs to
     * @param functionName the function name
     * @return the function status
     */
    public InstanceCommunication.FunctionStatus getFunctionStatus(String tenant, String namespace, String functionName) {
        String workerId = this.workerConfig.getWorkerId();

        Function.Assignment assignment = this.findAssignment(tenant, namespace, functionName);
        if (assignment == null) {
            InstanceCommunication.FunctionStatus.Builder functionStatusBuilder
                    = InstanceCommunication.FunctionStatus.newBuilder();
            functionStatusBuilder.setRunning(false);
            functionStatusBuilder.setFailureException("Function has not been scheduled");
            return functionStatusBuilder.build();
        }

        InstanceCommunication.FunctionStatus functionStatus = null;
        // If I am running worker
        if (assignment.getWorkerId().equals(workerId)) {
            FunctionRuntimeInfo functionRuntimeInfo = this.getFunctionRuntimeInfo(
                    FunctionConfigUtils.getFullyQualifiedName(tenant, namespace, functionName));
            Spawner spawner = functionRuntimeInfo.getSpawner();
            if (spawner != null) {
                try {
                    functionStatus = functionRuntimeInfo.getSpawner().getFunctionStatus().get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            } else {
                InstanceCommunication.FunctionStatus.Builder functionStatusBuilder
                        = InstanceCommunication.FunctionStatus.newBuilder();
                functionStatusBuilder.setRunning(false);
                if (functionRuntimeInfo.getStartupException() != null) {
                    functionStatusBuilder.setFailureException(functionRuntimeInfo.getStartupException().getMessage());
                }
                functionStatus = functionStatusBuilder.build();
            }
        } else {
            // query other worker

            List<MembershipManager.WorkerInfo> workerInfoList = this.membershipManager.getCurrentMembership();
            MembershipManager.WorkerInfo workerInfo = null;
            for (MembershipManager.WorkerInfo entry: workerInfoList) {
                if (assignment.getWorkerId().equals(entry.getWorkerId())) {
                    workerInfo = entry;
                }
            }
            if (workerInfo == null) {
                InstanceCommunication.FunctionStatus.Builder functionStatusBuilder
                        = InstanceCommunication.FunctionStatus.newBuilder();
                functionStatusBuilder.setRunning(false);
                functionStatusBuilder.setFailureException("Function has not been scheduled");
                return functionStatusBuilder.build();
            }

            Client client = ClientBuilder.newClient();

            // TODO: implement authentication/authorization
            String jsonResponse = client.target(String.format("http://%s:%d/admin/functions/%s/%s/%s/status",
                    workerInfo.getWorkerHostname(), workerInfo.getPort(), tenant, namespace, functionName))
                    .request(MediaType.TEXT_PLAIN)
                    .get(String.class);

            InstanceCommunication.FunctionStatus.Builder functionStatusBuilder = InstanceCommunication.FunctionStatus.newBuilder();
            try {
                JsonFormat.parser().merge(jsonResponse, functionStatusBuilder);
            } catch (InvalidProtocolBufferException e) {
                log.warn("Got invalid function status response from {}", workerInfo, e);
                throw new RuntimeException(e);
            }
            functionStatus = functionStatusBuilder.build();
        }

        return functionStatus;
    }

    /**
     * Process an assignment update from the assignment topic
     * @param messageId the message id of the update assignment
     * @param assignmentsUpdate the assignment update
     */
    public synchronized void processAssignmentUpdate(MessageId messageId, AssignmentsUpdate assignmentsUpdate) {
        if (assignmentsUpdate.getVersion() > this.currentAssignmentVersion) {

            Map<String, Assignment> assignmentMap = new HashMap<>();
            for (Assignment assignment : assignmentsUpdate.getAssignmentsList()) {
                assignmentMap.put(
                        FunctionConfigUtils.getFullyQualifiedName(assignment.getFunctionMetaData().getFunctionConfig()),
                        assignment);
            }
            Map<String, Assignment> existingAssignmentMap = new HashMap<>();
            for (Map<String, Assignment> entry : this.workerIdToAssignments.values()) {
                existingAssignmentMap.putAll(entry);
            }

            Map<String, Assignment> assignmentsToAdd = diff(assignmentMap, existingAssignmentMap);

            Map<String, Assignment> assignmentsToDelete = diff(existingAssignmentMap, assignmentMap);

            Map<String, Assignment> existingAssignments = inCommon(assignmentMap, existingAssignmentMap);

            // functions to add
            for (Map.Entry<String, Assignment> assignmentEntry : assignmentsToAdd.entrySet()) {
                String fullyQualifiedName = assignmentEntry.getKey();
                Assignment assignment = assignmentEntry.getValue();

                //add new function
                this.setAssignment(assignment);

                //Assigned to me
                if (assignment.getWorkerId().equals(workerConfig.getWorkerId())) {
                    if (!this.functionRuntimeInfoMap.containsKey(fullyQualifiedName)) {
                        this.setFunctionRuntimeInfo(fullyQualifiedName, new FunctionRuntimeInfo()
                                .setFunctionMetaData(assignment.getFunctionMetaData()));

                    } else {
                        //Somehow this function is already started
                        log.warn("Function {} already running. Going to restart function.",
                                this.functionRuntimeInfoMap.get(fullyQualifiedName));
                        this.insertStopAction(this.functionRuntimeInfoMap.get(fullyQualifiedName));
                    }
                    FunctionRuntimeInfo functionRuntimeInfo = this.functionRuntimeInfoMap.get(fullyQualifiedName);
                    this.insertStartAction(functionRuntimeInfo);
                }
            }

            // functions to delete
            for (Map.Entry<String, Assignment> assignmentEntry : assignmentsToDelete.entrySet()) {
                String fullyQualifiedName = assignmentEntry.getKey();
                Assignment assignment = assignmentEntry.getValue();

                FunctionRuntimeInfo functionRuntimeInfo = this.functionRuntimeInfoMap.get(fullyQualifiedName);
                if (functionRuntimeInfo != null) {
                    this.insertStopAction(functionRuntimeInfo);
                    this.deleteFunctionRuntimeInfo(fullyQualifiedName);
                }
                this.deleteAssignment(assignment);
            }

            // functions to update
            for (Map.Entry<String, Assignment> assignmentEntry : existingAssignments.entrySet()) {
                String fullyQualifiedName = assignmentEntry.getKey();
                Assignment assignment = assignmentEntry.getValue();
                Assignment existingAssignment = this.findAssignment(assignment);
                // potential updates need to happen
                if (!existingAssignment.equals(assignment)) {
                    FunctionRuntimeInfo functionRuntimeInfo = this.functionRuntimeInfoMap.get(fullyQualifiedName);
                    //stop function
                    if (functionRuntimeInfo != null) {
                        this.insertStopAction(functionRuntimeInfo);
                    }
                    // still assigned to me, need to restart
                    if (assignment.getWorkerId().equals(this.workerConfig.getWorkerId())) {
                        //start again
                        FunctionRuntimeInfo newFunctionRuntimeInfo = new FunctionRuntimeInfo();
                        newFunctionRuntimeInfo.setFunctionMetaData(assignment.getFunctionMetaData());
                        this.insertStartAction(newFunctionRuntimeInfo);
                        this.setFunctionRuntimeInfo(fullyQualifiedName, newFunctionRuntimeInfo);
                        this.setAssignment(assignment);
                    }
                }
            }

            // set as current assignment
            this.currentAssignmentVersion = assignmentsUpdate.getVersion();

        } else {
            log.debug("Received out of date assignment update: {}", assignmentsUpdate);
        }
    }

    /**
     * Private methods for internal use.  Should not be used outside of this class
     */

    @VisibleForTesting
    void insertStopAction(FunctionRuntimeInfo functionRuntimeInfo) {
        FunctionAction functionAction = new FunctionAction();
        functionAction.setAction(FunctionAction.Action.STOP);
        functionAction.setFunctionRuntimeInfo(functionRuntimeInfo);
        try {
            actionQueue.put(functionAction);
        } catch (InterruptedException ex) {
            throw new RuntimeException("Interrupted while putting action");
        }

    }

    @VisibleForTesting
    void insertStartAction(FunctionRuntimeInfo functionRuntimeInfo) {
        FunctionAction functionAction = new FunctionAction();
        functionAction.setAction(FunctionAction.Action.START);
        functionAction.setFunctionRuntimeInfo(functionRuntimeInfo);
        try {
            actionQueue.put(functionAction);
        } catch (InterruptedException ex) {
            throw new RuntimeException("Interrupted while putting action");
        }
    }

    private Assignment findAssignment(String tenant, String namespace, String functionName) {
        String fullyQualifiedName
                = FunctionConfigUtils.getFullyQualifiedName(tenant, namespace, functionName);
        for (Map.Entry<String, Map<String, Assignment>> entry : this.workerIdToAssignments.entrySet()) {
            Map<String, Assignment> assignmentMap = entry.getValue();
            Assignment existingAssignment = assignmentMap.get(fullyQualifiedName);
            if (existingAssignment != null) {
                return existingAssignment;
            }
        }
        return null;
    }

    private Assignment findAssignment(Assignment assignment) {
        return findAssignment(
                assignment.getFunctionMetaData().getFunctionConfig().getTenant(),
                assignment.getFunctionMetaData().getFunctionConfig().getNamespace(),
                assignment.getFunctionMetaData().getFunctionConfig().getName()
        );
    }

    @VisibleForTesting
    void setAssignment(Assignment assignment) {
        if (!this.workerIdToAssignments.containsKey(assignment.getWorkerId())) {
            this.workerIdToAssignments.put(assignment.getWorkerId(), new HashMap<>());
        }
        this.workerIdToAssignments.get(assignment.getWorkerId()).put(
                FunctionConfigUtils.getFullyQualifiedName(assignment.getFunctionMetaData().getFunctionConfig()),
                assignment);
    }

    @VisibleForTesting
    void deleteAssignment(Assignment assignment) {
        Map<String, Assignment> assignmentMap = this.workerIdToAssignments.get(assignment.getWorkerId());
        if (assignmentMap != null) {
            String fullyQualifiedName = FunctionConfigUtils.getFullyQualifiedName(
                    assignment.getFunctionMetaData().getFunctionConfig());
            if (assignmentMap.containsKey(fullyQualifiedName)) {
                assignmentMap.remove(fullyQualifiedName);
            }
            if (assignmentMap.isEmpty()) {
                this.workerIdToAssignments.remove(assignment.getWorkerId());
            }
        }
    }

    private void deleteFunctionRuntimeInfo(String fullyQualifiedName) {
        this.functionRuntimeInfoMap.remove(fullyQualifiedName);
    }

    private void setFunctionRuntimeInfo(String fullyQualifiedName, FunctionRuntimeInfo functionRuntimeInfo) {
        this.functionRuntimeInfoMap.put(fullyQualifiedName, functionRuntimeInfo);
    }

    @Override
    public void close() throws Exception {
        this.functionActioner.close();
        this.functionAssignmentTailer.close();
    }

    private MetricsSink createMetricsSink() {
        String className = workerConfig.getMetricsConfig().getMetricsSinkClassName();
        try {
            MetricsSink sink = (MetricsSink) Class.forName(className).newInstance();
            return sink;
        } catch (InstantiationException e) {
            throw new RuntimeException(e + " IMetricsSink class must have a no-arg constructor.");
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e + " IMetricsSink class must be concrete.");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e + " IMetricsSink class must be a class path.");
        }
    }

    private Map<String, Assignment> diff(Map<String, Assignment> assignmentMap1, Map<String, Assignment> assignmentMap2) {
        Map<String, Assignment> result = new HashMap<>();
        for (Map.Entry<String, Assignment> entry : assignmentMap1.entrySet()) {
            if (!assignmentMap2.containsKey(entry.getKey())) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    private Map<String, Assignment> inCommon(Map<String, Assignment> assignmentMap1, Map<String, Assignment> assignmentMap2) {

        Map<String, Assignment> result = new HashMap<>();
        for (Map.Entry<String, Assignment> entry : assignmentMap1.entrySet()) {
            if (assignmentMap2.containsKey(entry.getKey())) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    private FunctionRuntimeInfo getFunctionRuntimeInfo(String fullyQualifiedName) {
        return this.functionRuntimeInfoMap.get(fullyQualifiedName);
    }
}
