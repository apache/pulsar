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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.utils.FunctionCommon;

import static org.apache.pulsar.functions.worker.SchedulerManager.checkHeartBeatFunction;

/**
 * A simple implementation of leader election using a pulsar topic.
 */
@Slf4j
public class MembershipManager implements AutoCloseable, ConsumerEventListener {

    private final String consumerName;
    private final ConsumerImpl<byte[]> consumer;
    private final WorkerConfig workerConfig;
    private PulsarAdmin pulsarAdmin;
    private final CompletableFuture<Void> firstConsumerEventFuture;
    private final AtomicBoolean isLeader = new AtomicBoolean();

    static final String COORDINATION_TOPIC_SUBSCRIPTION = "participants";

    private static String WORKER_IDENTIFIER = "id";

    // How long functions have remained assigned or scheduled on a failed node
    // FullyQualifiedFunctionName -> time in millis
    @VisibleForTesting
    Map<Function.Instance, Long> unsignedFunctionDurations = new HashMap<>();

    MembershipManager(WorkerService service, PulsarClient client, PulsarAdmin pulsarAdmin)
            throws PulsarClientException {
        this.workerConfig = service.getWorkerConfig();
        this.pulsarAdmin = pulsarAdmin;
        consumerName = String.format(
            "%s:%s:%d",
            workerConfig.getWorkerId(),
            workerConfig.getWorkerHostname(),
            workerConfig.getWorkerPort()
        );
        firstConsumerEventFuture = new CompletableFuture<>();
        // the membership manager is using a `coordination` topic for leader election.
        // we don't produce any messages into this topic, we only use the `failover` subscription
        // to elect an active consumer as the leader worker. The leader worker will be responsible
        // for scheduling snapshots for FMT and doing task assignment.
        consumer = (ConsumerImpl<byte[]>) client.newConsumer()
                .topic(workerConfig.getClusterCoordinationTopic())
                .subscriptionName(COORDINATION_TOPIC_SUBSCRIPTION)
                .subscriptionType(SubscriptionType.Failover)
                .consumerEventListener(this)
                .property(WORKER_IDENTIFIER, consumerName)
                .subscribe();
        
        isLeader.set(checkLeader(service, consumer.getConsumerName()));
    }

    @Override
    public void becameActive(Consumer<?> consumer, int partitionId) {
        firstConsumerEventFuture.complete(null);
        if (isLeader.compareAndSet(false, true)) {
            log.info("Worker {} became the leader.", consumerName);
        }
    }

    @Override
    public void becameInactive(Consumer<?> consumer, int partitionId) {
        firstConsumerEventFuture.complete(null);
        if (isLeader.compareAndSet(true, false)) {
            log.info("Worker {} lost the leadership.", consumerName);
        }
    }

    public boolean isLeader() {
        return isLeader.get();
    }

    public List<WorkerInfo> getCurrentMembership() {

        List<WorkerInfo> workerIds = new LinkedList<>();
        TopicStats topicStats = null;
        try {
            topicStats = this.pulsarAdmin.topics().getStats(this.workerConfig.getClusterCoordinationTopic());
        } catch (PulsarAdminException e) {
            log.error("Failed to get status of coordinate topic {}",
                    this.workerConfig.getClusterCoordinationTopic(), e);
            throw new RuntimeException(e);
        }

        for (ConsumerStats consumerStats : topicStats.subscriptions
                .get(COORDINATION_TOPIC_SUBSCRIPTION).consumers) {
            WorkerInfo workerInfo = WorkerInfo.parseFrom(consumerStats.metadata.get(WORKER_IDENTIFIER));
            workerIds.add(workerInfo);
        }
        return workerIds;
    }

    public WorkerInfo getLeader() {
        TopicStats topicStats = null;
        try {
            topicStats = this.pulsarAdmin.topics().getStats(this.workerConfig.getClusterCoordinationTopic());
        } catch (PulsarAdminException e) {
            log.error("Failed to get status of coordinate topic {}",
                    this.workerConfig.getClusterCoordinationTopic(), e);
            throw new RuntimeException(e);
        }

        String activeConsumerName = topicStats.subscriptions.get(COORDINATION_TOPIC_SUBSCRIPTION).activeConsumerName;
        WorkerInfo leader = null;
        for (ConsumerStats consumerStats : topicStats.subscriptions
                .get(COORDINATION_TOPIC_SUBSCRIPTION).consumers) {
            if (consumerStats.consumerName.equals(activeConsumerName)) {
                leader = WorkerInfo.parseFrom(consumerStats.metadata.get(WORKER_IDENTIFIER));
            }
        }
        if (leader == null) {
            log.warn("Failed to determine leader in functions cluster");
        }
        return leader;
    }

    @Override
    public void close() throws PulsarClientException {
        consumer.close();
    }

    public void checkFailures(FunctionMetaDataManager functionMetaDataManager,
                              FunctionRuntimeManager functionRuntimeManager,
                              SchedulerManager schedulerManager) {

        Set<String> currentMembership = this.getCurrentMembership().stream()
                .map(entry -> entry.getWorkerId()).collect(Collectors.toSet());
        List<Function.FunctionMetaData> functionMetaDataList = functionMetaDataManager.getAllFunctionMetaData();
        Map<String, Function.FunctionMetaData> functionMetaDataMap = new HashMap<>();
        for (Function.FunctionMetaData entry : functionMetaDataList) {
            functionMetaDataMap.put(FunctionCommon.getFullyQualifiedName(entry.getFunctionDetails()), entry);
        }
        Map<String, Map<String, Function.Assignment>> currentAssignments = functionRuntimeManager.getCurrentAssignments();
        Map<String, Function.Assignment> assignmentMap = new HashMap<>();
        for (Map<String, Function.Assignment> entry : currentAssignments.values()) {
            assignmentMap.putAll(entry);
        }
        long currentTimeMs = System.currentTimeMillis();

        // remove functions
        Iterator<Map.Entry<Function.Instance, Long>> it = unsignedFunctionDurations.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Function.Instance, Long> entry = it.next();
            String fullyQualifiedFunctionName = FunctionCommon.getFullyQualifiedName(
                    entry.getKey().getFunctionMetaData().getFunctionDetails());
            String fullyQualifiedInstanceId = FunctionCommon.getFullyQualifiedInstanceId(entry.getKey());
            //remove functions that don't exist anymore
            if (!functionMetaDataMap.containsKey(fullyQualifiedFunctionName)) {
                it.remove();
            } else {
                //remove functions that have been scheduled
                Function.Assignment assignment = assignmentMap.get(fullyQualifiedInstanceId);
                if (assignment != null) {
                    String assignedWorkerId = assignment.getWorkerId();
                    // check if assigned to worker that has failed
                    if (currentMembership.contains(assignedWorkerId)) {
                        it.remove();
                    }
                }
            }
        }

        // check for function instances that haven't been assigned
        for (Function.FunctionMetaData functionMetaData : functionMetaDataList) {
            Collection<Function.Assignment> assignments
                    = FunctionRuntimeManager.findFunctionAssignments(functionMetaData.getFunctionDetails().getTenant(),
                    functionMetaData.getFunctionDetails().getNamespace(),
                    functionMetaData.getFunctionDetails().getName(),
                    currentAssignments);

            Set<Function.Instance> assignedInstances = assignments.stream()
                    .map(assignment -> assignment.getInstance())
                    .collect(Collectors.toSet());

            Set<Function.Instance> instances = new HashSet<>(SchedulerManager.computeInstances(functionMetaData, functionRuntimeManager.getRuntimeFactory().externallyManaged()));

            for (Function.Instance instance : instances) {
                if (!assignedInstances.contains(instance)) {
                    if (!this.unsignedFunctionDurations.containsKey(instance)) {
                        this.unsignedFunctionDurations.put(instance, currentTimeMs);
                    }
                }
            }
        }

        // check failed nodes
        for (Map.Entry<String, Map<String, Function.Assignment>> entry : currentAssignments.entrySet()) {
            String workerId = entry.getKey();
            Map<String, Function.Assignment> assignmentEntries = entry.getValue();
            if (!currentMembership.contains(workerId)) {
                for (Function.Assignment assignmentEntry : assignmentEntries.values()) {
                    Function.Instance instance = assignmentEntry.getInstance();
                    // avoid scheduling-trigger for heartbeat-function if owner-worker is not up
                    if (checkHeartBeatFunction(instance) != null) {
                        continue;
                    }
                    if (!this.unsignedFunctionDurations.containsKey(instance)) {
                        this.unsignedFunctionDurations.put(instance, currentTimeMs);
                    }
                }
            }
        }

        boolean triggerScheduler = false;
        // check unassigned
        Collection<Function.Instance> needSchedule = new LinkedList<>();
        Collection<Function.Assignment> needRemove = new LinkedList<>();
        for (Map.Entry<Function.Instance, Long> entry : this.unsignedFunctionDurations.entrySet()) {
            Function.Instance instance = entry.getKey();
            long unassignedDurationMs = entry.getValue();
            if (currentTimeMs - unassignedDurationMs > this.workerConfig.getRescheduleTimeoutMs()) {
                needSchedule.add(instance);
                // remove assignment from failed node
                Function.Assignment assignment = assignmentMap.get(FunctionCommon.getFullyQualifiedInstanceId(instance));
                if (assignment != null) {
                    needRemove.add(assignment);
                }
                triggerScheduler = true;
            }
        }
        if (!needRemove.isEmpty()) {
            functionRuntimeManager.removeAssignments(needRemove);
        }
        if (triggerScheduler) {
            log.info("Functions that need scheduling/rescheduling: {}", needSchedule);
            schedulerManager.schedule();
        }
    }

    /**
     * Private methods
     */

    private boolean checkLeader(WorkerService service, String consumerName) {
        try {
            TopicStats stats = service.getBrokerAdmin().topics()
                    .getStats(service.getWorkerConfig().getClusterCoordinationTopic());
            String activeConsumerName = stats != null
                    && stats.subscriptions.get(COORDINATION_TOPIC_SUBSCRIPTION) != null
                            ? stats.subscriptions.get(COORDINATION_TOPIC_SUBSCRIPTION).activeConsumerName
                            : null;
            return consumerName != null && consumerName.equalsIgnoreCase(activeConsumerName);
        } catch (Exception e) {
            log.warn("Failed to check leader {}", e.getMessage());
        }
        return false;
    }
    
}
