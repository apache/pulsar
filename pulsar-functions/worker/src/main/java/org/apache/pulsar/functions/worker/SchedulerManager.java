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

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.Assignment;
import org.apache.pulsar.functions.proto.Request;
import org.apache.pulsar.functions.utils.Reflections;
import org.apache.pulsar.functions.worker.scheduler.IScheduler;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class SchedulerManager implements AutoCloseable {

    private final WorkerConfig workerConfig;

    @Setter
    private FunctionMetaDataManager functionMetaDataManager;

    @Setter
    private MembershipManager membershipManager;

    @Setter
    private FunctionRuntimeManager functionRuntimeManager;

    private final IScheduler scheduler;

    private final Producer producer;

    private final ExecutorService executorService;

    public SchedulerManager(WorkerConfig workerConfig, PulsarClient pulsarClient) {
        this.workerConfig = workerConfig;
        this.scheduler = Reflections.createInstance(workerConfig.getSchedulerClassName(), IScheduler.class,
                Thread.currentThread().getContextClassLoader());

        ProducerConfiguration producerConf = new ProducerConfiguration()
            .setBatchingEnabled(true)
            .setBlockIfQueueFull(true)
            .setCompressionType(CompressionType.LZ4)
            // retry until succeed
            .setSendTimeout(0, TimeUnit.MILLISECONDS);
        try {
            this.producer = pulsarClient.createProducer(this.workerConfig.getFunctionAssignmentTopic(), producerConf);
        } catch (PulsarClientException e) {
            log.error("Failed to create producer to function assignment topic "
                    + this.workerConfig.getFunctionAssignmentTopic(), e);
            throw new RuntimeException(e);
        }

        this.executorService =
                new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>());
    }

    public Future<?> schedule() {
        return executorService.submit(() -> {
            synchronized (SchedulerManager.this) {
                boolean isLeader = membershipManager.isLeader();
                if (isLeader) {
                    invokeScheduler();
                }
            }
        });
    }

    private void invokeScheduler() {
        List<String> currentMembership = this.membershipManager.getCurrentMembership()
                .stream().map(workerInfo -> workerInfo.getWorkerId()).collect(Collectors.toList());

        List<FunctionMetaData> allFunctions = this.functionMetaDataManager.getAllFunctionMetaData();
        Map<String, Function.Instance> allInstances = computeAllInstances(allFunctions);
        Map<String, Map<String, Assignment>> workerIdToAssignments = this.functionRuntimeManager
                .getCurrentAssignments();
        //delete assignments of functions and instances that don't exist anymore
        Iterator<Map.Entry<String, Map<String, Assignment>>> it = workerIdToAssignments.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Map<String, Assignment>> workerIdToAssignmentEntry = it.next();
            Map<String, Assignment> functionMap = workerIdToAssignmentEntry.getValue();
            // remove instances that don't exist anymore
            functionMap.entrySet().removeIf(
                    entry -> {
                        String fullyQualifiedInstanceId = entry.getKey();
                        return !allInstances.containsKey(fullyQualifiedInstanceId);
                    });

            // update assignment instances in case attributes of a function gets updated
            for (Map.Entry<String, Assignment> entry : functionMap.entrySet()) {
                String fullyQualifiedInstanceId = entry.getKey();
                Assignment assignment = entry.getValue();
                Function.Instance instance = allInstances.get(fullyQualifiedInstanceId);

                if (!assignment.getInstance().equals(instance)) {
                    functionMap.put(fullyQualifiedInstanceId, assignment.toBuilder().setInstance(instance).build());
                }
            }
            if (functionMap.isEmpty()) {
                it.remove();
            }
        }

        List<Assignment> currentAssignments = workerIdToAssignments
                .entrySet().stream()
                .flatMap(stringMapEntry -> stringMapEntry.getValue().values().stream()).collect(Collectors.toList());

        List<Function.Instance> needsAssignment = this.getUnassignedFunctionInstances(workerIdToAssignments,
                allInstances);

        List<Assignment> assignments = this.scheduler.schedule(
                needsAssignment, currentAssignments, currentMembership);

        log.debug("New assignments computed: {}", assignments);

        long assignmentVersion = this.functionRuntimeManager.getCurrentAssignmentVersion() + 1;
        Request.AssignmentsUpdate assignmentsUpdate = Request.AssignmentsUpdate.newBuilder()
                .setVersion(assignmentVersion)
                .addAllAssignments(assignments)
                .build();

        CompletableFuture<MessageId> messageIdCompletableFuture = producer.sendAsync(assignmentsUpdate.toByteArray());
        try {
            messageIdCompletableFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to send assignment update", e);
            throw new RuntimeException(e);
        }

        // wait for assignment update to go throw the pipeline
        int retries = 0;
        while (this.functionRuntimeManager.getCurrentAssignmentVersion() < assignmentVersion) {
            if (retries >= this.workerConfig.getAssignmentWriteMaxRetries()) {
                log.warn("Max number of retries reached for waiting for assignment to propagate. Will continue now.");
                break;
            }
            log.info("Waiting for assignments to propagate...");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            retries++;
        }
    }

    public static Map<String, Function.Instance> computeAllInstances(List<FunctionMetaData> allFunctions) {
        Map<String, Function.Instance> functionInstances = new HashMap<>();
        for (FunctionMetaData functionMetaData : allFunctions) {
            for (Function.Instance instance : computeInstances(functionMetaData)) {
                functionInstances.put(Utils.getFullyQualifiedInstanceId(instance), instance);
            }
        }
        return functionInstances;
    }

    public static List<Function.Instance> computeInstances(FunctionMetaData functionMetaData) {
        List<Function.Instance> functionInstances = new LinkedList<>();
        int instances = functionMetaData.getFunctionDetails().getParallelism();
        for (int i = 0; i < instances; i++) {
            functionInstances.add(Function.Instance.newBuilder()
                    .setFunctionMetaData(functionMetaData)
                    .setInstanceId(i)
                    .build());
        }
        return functionInstances;
    }

    private List<Function.Instance> getUnassignedFunctionInstances(
            Map<String, Map<String, Assignment>> currentAssignments, Map<String, Function.Instance> functionInstances) {

        List<Function.Instance> unassignedFunctionInstances = new LinkedList<>();
        Map<String, Assignment> assignmentMap = new HashMap<>();
        for (Map<String, Assignment> entry : currentAssignments.values()) {
            assignmentMap.putAll(entry);
        }

        for (Map.Entry<String, Function.Instance> instanceEntry : functionInstances.entrySet()) {
            String fullyQualifiedInstanceId = instanceEntry.getKey();
            Function.Instance instance = instanceEntry.getValue();
            if (!assignmentMap.containsKey(fullyQualifiedInstanceId)) {
                unassignedFunctionInstances.add(instance);
            }
        }
        return unassignedFunctionInstances;
    }

    @Override
    public void close() {
        try {
            this.producer.close();
        } catch (PulsarClientException e) {
            log.warn("Failed to shutdown scheduler manager assignment producer", e);
        }
        this.executorService.shutdown();
    }
}
