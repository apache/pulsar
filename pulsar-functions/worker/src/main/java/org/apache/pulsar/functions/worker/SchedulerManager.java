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
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.Assignment;
import org.apache.pulsar.functions.proto.Request;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.utils.Reflections;
import org.apache.pulsar.functions.worker.scheduler.IScheduler;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Slf4j
public class SchedulerManager implements AutoCloseable {

    private WorkerConfig workerConfig;

    @Setter
    private FunctionMetaDataManager functionMetaDataManager;

    @Setter
    private MembershipManager membershipManager;

    @Setter
    private FunctionRuntimeManager functionRuntimeManager;

    private IScheduler scheduler;

    private Producer producer;

    private ExecutorService executorService;

    public SchedulerManager(WorkerConfig workerConfig, PulsarClient pulsarClient) {
        this.workerConfig = workerConfig;
        this.scheduler = Reflections.createInstance(workerConfig.getSchedulerClassName(), IScheduler.class,
                Thread.currentThread().getContextClassLoader());

        try {
            this.producer = pulsarClient.createProducer(this.workerConfig.getFunctionAssignmentTopic());
        } catch (PulsarClientException e) {
            log.error("Failed to create producer to function assignment topic "
                    + this.workerConfig.getFunctionAssignmentTopic(), e);
            throw new RuntimeException(e);
        }

        this.executorService =
                new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>());
    }

    public void schedule() {
        executorService.submit(() -> {
            synchronized (SchedulerManager.this) {
                boolean isLeader = false;
                try {
                    isLeader = membershipManager.becomeLeader().get(30, TimeUnit.SECONDS);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    log.warn("Failed to attempt becoming leader", e);
                }

                if (isLeader) {
                    invokeScheduler();
                }
            }
        });
    }

    public void invokeScheduler() {
        List<String> currentMembership = this.membershipManager.getCurrentMembership();


        List<FunctionMetaData> allFunctions = this.functionMetaDataManager.getAllFunctionMetaData();

        Map<String, Map<String, Assignment>> workerIdToAssignments = this.functionRuntimeManager.getCurrentAssignments();

        List<Assignment> currentAssignments = workerIdToAssignments
                .entrySet().stream()
                .flatMap(stringMapEntry -> stringMapEntry.getValue().values().stream()).collect(Collectors.toList());

        List<FunctionMetaData> needsAssignment = this.getUnassignedFunctions(workerIdToAssignments, allFunctions);

        List<Assignment> assignments = this.scheduler.schedule(
                needsAssignment, currentAssignments, currentMembership);

        log.info("New Assignment computed: {}", assignments);
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
        while (this.functionRuntimeManager.getCurrentAssignmentVersion() < assignmentVersion) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private List<FunctionMetaData> getUnassignedFunctions(
            Map<String, Map<String, Assignment>> currentAssignments, List<FunctionMetaData> allFunctions) {

        List<FunctionMetaData> unassignedFunctions = new LinkedList<>();
        Map<String, Assignment> assignmentMap = new HashMap<>();
        for (Map<String, Assignment> entry : currentAssignments.values()) {
            assignmentMap.putAll(entry);
        }

        for (FunctionMetaData functionMetaData : allFunctions) {
            String fullyQualifiedName = FunctionConfigUtils.getFullyQualifiedName(functionMetaData.getFunctionConfig());
            if (!assignmentMap.containsKey(fullyQualifiedName)) {
                unassignedFunctions.add(functionMetaData);
            }
        }
        return unassignedFunctions;
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
