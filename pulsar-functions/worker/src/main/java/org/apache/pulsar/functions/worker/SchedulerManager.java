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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.Assignment;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.Instance;
import org.apache.pulsar.functions.utils.Actions;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.worker.scheduler.IScheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SchedulerManager implements AutoCloseable {

    private final WorkerConfig workerConfig;
    private final ErrorNotifier errorNotifier;
    private final ThreadPoolExecutor executorService;

    @Setter
    private FunctionMetaDataManager functionMetaDataManager;

    @Setter
    private LeaderService leaderService;

    @Setter
    private MembershipManager membershipManager;

    @Setter
    private FunctionRuntimeManager functionRuntimeManager;

    private final IScheduler scheduler;

    private final Producer<byte[]> producer;


    private final ScheduledExecutorService scheduledExecutorService;
    
    private final PulsarAdmin admin;

    @Getter
    private Lock schedulerLock = new ReentrantLock(true);
    
    AtomicBoolean isCompactionNeeded = new AtomicBoolean(false);
    private static final long DEFAULT_ADMIN_API_BACKOFF_SEC = 60; 
    public static final String HEARTBEAT_TENANT = "pulsar-function";
    public static final String HEARTBEAT_NAMESPACE = "heartbeat";

    public SchedulerManager(WorkerConfig workerConfig,
                            PulsarClient pulsarClient,
                            PulsarAdmin admin,
                            ErrorNotifier errorNotifier) {
        this.workerConfig = workerConfig;
        this.admin = admin;
        this.scheduler = Reflections.createInstance(workerConfig.getSchedulerClassName(), IScheduler.class,
                Thread.currentThread().getContextClassLoader());

        this.producer = createProducer(pulsarClient, workerConfig);
        this.executorService = new ThreadPoolExecutor(1, 5, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(5));
        this.executorService.setThreadFactory(new ThreadFactoryBuilder().setNameFormat("worker-scheduler-%d").build());
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("worker-assignment-topic-compactor"));
        this.errorNotifier = errorNotifier;
        
        scheduleCompaction(this.scheduledExecutorService, workerConfig.getTopicCompactionFrequencySec());
    }

    private static Producer<byte[]> createProducer(PulsarClient client, WorkerConfig config) {
        Actions.Action createProducerAction = Actions.Action.builder()
                .actionName(String.format("Creating producer for assignment topic %s", config.getFunctionAssignmentTopic()))
                .numRetries(5)
                .sleepBetweenInvocationsMs(10000)
                .supplier(() -> {
                    try {
                        Producer<byte[]> producer = client.newProducer().topic(config.getFunctionAssignmentTopic())
                                .enableBatching(false)
                                .blockIfQueueFull(true)
                                .compressionType(CompressionType.LZ4)
                                .sendTimeout(0, TimeUnit.MILLISECONDS)
                                .producerName(config.getWorkerId() + "-scheduler-manager")
                                .createAsync().get(10, TimeUnit.SECONDS);
                        return Actions.ActionResult.builder().success(true).result(producer).build();
                    } catch (Exception e) {
                        log.error("Exception while at creating producer to topic {}", config.getFunctionAssignmentTopic(), e);
                        return Actions.ActionResult.builder()
                                .success(false)
                                .build();
                    }
                })
                .build();
        AtomicReference<Producer<byte[]>> producer = new AtomicReference<>();
        try {
            Actions.newBuilder()
                    .addAction(createProducerAction.toBuilder()
                            .onSuccess((actionResult) -> producer.set((Producer<byte[]>) actionResult.getResult()))
                            .build())
                    .run();
        } catch (InterruptedException e) {
            log.error("Interrupted at creating producer to topic {}", config.getFunctionAssignmentTopic(), e);
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        if (producer.get() == null) {
            throw new RuntimeException("Can't create a producer on assignment topic "
                    + config.getFunctionAssignmentTopic());
        }
        return producer.get();
    }

    public Future<?> schedule() {
        try {
            return executorService.submit(() -> {
                try {
                    schedulerLock.lock();

                    boolean isLeader = leaderService.isLeader();
                    if (isLeader) {
                        try {
                            invokeScheduler();
                        } catch (Throwable th) {
                            log.error("Encountered error when invoking scheduler", th);
                            errorNotifier.triggerError(th);
                        }
                    }
                } finally {
                    schedulerLock.unlock();
                }
            });
        } catch (RejectedExecutionException e) {
            // task queue is full so just ignore
            log.debug("Rejected task to invoke scheduler since task queue is already full");
            return CompletableFuture.completedFuture(null);
        }
    }

    private void scheduleCompaction(ScheduledExecutorService executor, long scheduleFrequencySec) {
        if (executor != null) {
            executor.scheduleWithFixedDelay(() -> {
                if (leaderService.isLeader() && isCompactionNeeded.get()) {
                    compactAssignmentTopic();
                    isCompactionNeeded.set(false);
                }
            }, scheduleFrequencySec, scheduleFrequencySec, TimeUnit.SECONDS);
        }
    }
    
    @VisibleForTesting
    public void invokeScheduler() {
        
        Set<String> currentMembership = membershipManager.getCurrentMembership()
                .stream().map(workerInfo -> workerInfo.getWorkerId()).collect(Collectors.toSet());

        List<FunctionMetaData> allFunctions = functionMetaDataManager.getAllFunctionMetaData();
        Map<String, Function.Instance> allInstances = computeAllInstances(allFunctions, functionRuntimeManager.getRuntimeFactory().externallyManaged());
        Map<String, Map<String, Assignment>> workerIdToAssignments = functionRuntimeManager
                .getCurrentAssignments();

        //delete assignments of functions and instances that don't exist anymore
        Iterator<Map.Entry<String, Map<String, Assignment>>> it = workerIdToAssignments.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Map<String, Assignment>> workerIdToAssignmentEntry = it.next();
            String workerId = workerIdToAssignmentEntry.getKey();
            Map<String, Assignment> functionMap = workerIdToAssignmentEntry.getValue();

            // remove instances that don't exist anymore
            functionMap.entrySet().removeIf(entry -> {
                String fullyQualifiedInstanceId = entry.getKey();
                boolean deleted = !allInstances.containsKey(fullyQualifiedInstanceId);
                if (deleted) {
                    Assignment assignment = entry.getValue();
                    publishNewAssignment(assignment.toBuilder().build(), true);
                    
                    // Directly update in memory assignment cache since I am leader
                    log.info("Deleting assignment: {}", assignment);
                    functionRuntimeManager.deleteAssignment(fullyQualifiedInstanceId);
                }
                return deleted;
            });

            // update assignment instances in case attributes of a function gets updated
            for (Map.Entry<String, Assignment> entry : functionMap.entrySet()) {
                String fullyQualifiedInstanceId = entry.getKey();
                Assignment assignment = entry.getValue();
                Function.Instance instance = allInstances.get(fullyQualifiedInstanceId);

                if (!assignment.getInstance().equals(instance)) {
                    functionMap.put(fullyQualifiedInstanceId, assignment.toBuilder().setInstance(instance).build());
                    Assignment newAssignment = assignment.toBuilder().setInstance(instance).build().toBuilder().build();
                    publishNewAssignment(newAssignment, false);

                    // Directly update in memory assignment cache since I am leader
                    log.info("Updating assignment: {}", assignment);
                    functionRuntimeManager.processAssignment(newAssignment);
                }
                if (functionMap.isEmpty()) {
                    it.remove();
                }
            }
        }

        List<Assignment> currentAssignments = workerIdToAssignments
                .entrySet()
                .stream()
                .filter(workerIdToAssignmentEntry -> {
                    String workerId = workerIdToAssignmentEntry.getKey();
                    // remove assignments to workers that don't exist / died for now.
                    // wait for failure detector to unassign them in the future for re-scheduling
                    if (!currentMembership.contains(workerId)) {
                        return false;
                    }

                    return true;
                })
                .flatMap(stringMapEntry -> stringMapEntry.getValue().values().stream())
                .collect(Collectors.toList());

        Pair<List<Function.Instance>, List<Assignment>> unassignedInstances 
                = getUnassignedFunctionInstances(workerIdToAssignments, allInstances);

        List<Assignment> assignments = scheduler.schedule(unassignedInstances.getLeft(), currentAssignments, currentMembership);
        assignments.addAll(unassignedInstances.getRight());

        if (log.isDebugEnabled()) {
            log.debug("New assignments computed: {}", assignments);
        }

        isCompactionNeeded.set(!assignments.isEmpty());

        for(Assignment assignment : assignments) {
            publishNewAssignment(assignment, false);

            // Directly update in memory assignment cache since I am leader
            log.info("Adding assignment: {}", assignment);
            functionRuntimeManager.processAssignment(assignment);
        }
        
    }

    public void compactAssignmentTopic() {
        if (this.admin != null) {
            try {
                this.admin.topics().triggerCompaction(workerConfig.getFunctionAssignmentTopic());
            } catch (PulsarAdminException e) {
                log.error("Failed to trigger compaction", e);
                scheduledExecutorService.schedule(() -> compactAssignmentTopic(), DEFAULT_ADMIN_API_BACKOFF_SEC,
                        TimeUnit.SECONDS);
            }
        }
    }

    private void publishNewAssignment(Assignment assignment, boolean deleted) {
        try {
            String fullyQualifiedInstanceId = FunctionCommon.getFullyQualifiedInstanceId(assignment.getInstance());
            // publish empty message with instance-id key so, compactor can delete and skip delivery of this instance-id
            // message
            producer.newMessage().key(fullyQualifiedInstanceId)
                    .value(deleted ? "".getBytes() : assignment.toByteArray()).send();
        } catch (Exception e) {
            log.error("Failed to {} assignment update {}", assignment, deleted ? "send" : "deleted", e);
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Function.Instance> computeAllInstances(List<FunctionMetaData> allFunctions,
                                                                     boolean externallyManagedRuntime) {
        Map<String, Function.Instance> functionInstances = new HashMap<>();
        for (FunctionMetaData functionMetaData : allFunctions) {
            for (Function.Instance instance : computeInstances(functionMetaData, externallyManagedRuntime)) {
                functionInstances.put(FunctionCommon.getFullyQualifiedInstanceId(instance), instance);
            }
        }
        return functionInstances;
    }

    public static List<Function.Instance> computeInstances(FunctionMetaData functionMetaData,
                                                           boolean externallyManagedRuntime) {
        List<Function.Instance> functionInstances = new LinkedList<>();
        if (!externallyManagedRuntime) {
            int instances = functionMetaData.getFunctionDetails().getParallelism();
            for (int i = 0; i < instances; i++) {
                functionInstances.add(Function.Instance.newBuilder()
                        .setFunctionMetaData(functionMetaData)
                        .setInstanceId(i)
                        .build());
            }
        } else {
            functionInstances.add(Function.Instance.newBuilder()
                    .setFunctionMetaData(functionMetaData)
                    .setInstanceId(-1)
                    .build());
        }
        return functionInstances;
    }

    private Pair<List<Function.Instance>, List<Assignment>> getUnassignedFunctionInstances(
            Map<String, Map<String, Assignment>> currentAssignments, Map<String, Function.Instance> functionInstances) {

        List<Function.Instance> unassignedFunctionInstances = new LinkedList<>();
        List<Assignment> heartBeatAssignments = Lists.newArrayList();
        Map<String, Assignment> assignmentMap = new HashMap<>();
        for (Map<String, Assignment> entry : currentAssignments.values()) {
            assignmentMap.putAll(entry);
        }

        for (Map.Entry<String, Function.Instance> instanceEntry : functionInstances.entrySet()) {
            String fullyQualifiedInstanceId = instanceEntry.getKey();
            Function.Instance instance = instanceEntry.getValue();
            String heartBeatWorkerId = checkHeartBeatFunction(instance);
            if (heartBeatWorkerId != null) {
                heartBeatAssignments
                        .add(Assignment.newBuilder().setInstance(instance).setWorkerId(heartBeatWorkerId).build());
                continue;
            }
            if (!assignmentMap.containsKey(fullyQualifiedInstanceId)) {
                unassignedFunctionInstances.add(instance);
            }
        }
        return ImmutablePair.of(unassignedFunctionInstances, heartBeatAssignments);
    }

    @Override
    public void close() {
        scheduledExecutorService.shutdown();

        executorService.shutdown();

        try {
            this.producer.close();
        } catch (PulsarClientException e) {
            log.warn("Failed to shutdown scheduler manager assignment producer", e);
        }
    }
    
    public static String checkHeartBeatFunction(Instance funInstance) {
        if (funInstance.getFunctionMetaData() != null
                && funInstance.getFunctionMetaData().getFunctionDetails() != null) {
            FunctionDetails funDetails = funInstance.getFunctionMetaData().getFunctionDetails();
            return HEARTBEAT_TENANT.equals(funDetails.getTenant())
                    && HEARTBEAT_NAMESPACE.equals(funDetails.getNamespace()) ? funDetails.getName() : null;
        }
        return null;
    }
}
