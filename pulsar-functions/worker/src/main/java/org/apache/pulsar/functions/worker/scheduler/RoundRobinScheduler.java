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
package org.apache.pulsar.functions.worker.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.proto.Function.Assignment;
import org.apache.pulsar.functions.proto.Function.Instance;

import com.google.common.collect.Lists;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

@Slf4j
public class RoundRobinScheduler implements IScheduler {

    @Override
    public List<Assignment> schedule(List<Instance> unassignedFunctionInstances,
            List<Assignment> currentAssignments, Set<String> workers) {

        Map<String, List<Assignment>> workerIdToAssignment = new HashMap<>();
        List<Assignment> newAssignments = Lists.newArrayList();

        for (String workerId : workers) {
            workerIdToAssignment.put(workerId, new LinkedList<>());
        }

        for (Assignment existingAssignment : currentAssignments) {
            workerIdToAssignment.get(existingAssignment.getWorkerId()).add(existingAssignment);
        }

        for (Instance unassignedFunctionInstance : unassignedFunctionInstances) {
            String workerId = findNextWorker(workerIdToAssignment);
            Assignment newAssignment = Assignment.newBuilder().setInstance(unassignedFunctionInstance)
                    .setWorkerId(workerId).build();
            workerIdToAssignment.get(workerId).add(newAssignment);
            newAssignments.add(newAssignment);
        }

        return newAssignments;
    }

    private String findNextWorker(Map<String, List<Assignment>> workerIdToAssignment) {
        String targetWorkerId = null;
        int least = Integer.MAX_VALUE;
        for (Map.Entry<String, List<Assignment>> entry : workerIdToAssignment.entrySet()) {
            String workerId = entry.getKey();
            List<Assignment> workerAssignments = entry.getValue();
            if (workerAssignments.size() < least) {
                targetWorkerId = workerId;
                least = workerAssignments.size();
            }
        }
        return targetWorkerId;
    }

    @Override
    public List<Assignment> rebalance(List<Assignment> currentAssignments, Set<String> workers) {

        Map<String, Queue<Instance>> workerToAssignmentMap = new HashMap<>();

        workers.forEach(workerId -> workerToAssignmentMap.put(workerId, new LinkedList<>()));

        currentAssignments.forEach(assignment -> workerToAssignmentMap.computeIfAbsent(assignment.getWorkerId(), s -> new LinkedList<>()).add(assignment.getInstance()));

        List<Assignment> newAssignments = new LinkedList<>();

         RebalanceStats rebalanceStats = new RebalanceStats(workerToAssignmentMap);
        int iterations = 0;
        while(true) {
            iterations++;

            Map.Entry<String, Queue<Instance>> mostAssignmentsWorker = findWorkerWithMostAssignments(workerToAssignmentMap);

            Map.Entry<String, Queue<Instance>> leastAssignmentsWorker = findWorkerWithLeastAssignments(workerToAssignmentMap);

            if (mostAssignmentsWorker.getValue().size() == leastAssignmentsWorker.getValue().size()
                    || mostAssignmentsWorker.getValue().size() == leastAssignmentsWorker.getValue().size() + 1) {
                break;
            }

            String mostAssignmentsWorkerId = mostAssignmentsWorker.getKey();
            String leastAssignmentsWorkerId = leastAssignmentsWorker.getKey();

            Queue<Instance> src = workerToAssignmentMap.get(mostAssignmentsWorkerId);
            Queue<Instance> dest = workerToAssignmentMap.get(leastAssignmentsWorkerId);

            // update stats
            rebalanceStats.decrementInstance(mostAssignmentsWorkerId);
            rebalanceStats.incrementInstance(leastAssignmentsWorkerId);

            Instance instance = src.poll();
            Assignment newAssignment = Assignment.newBuilder()
                    .setInstance(instance)
                    .setWorkerId(leastAssignmentsWorkerId)
                    .build();
            newAssignments.add(newAssignment);

            dest.add(instance);
        }

        log.info("Rebalance - iterations: {} stats: {}", iterations, rebalanceStats);

        return newAssignments;
    }

    private Map.Entry<String, Queue<Instance>> findWorkerWithLeastAssignments(Map<String, Queue<Instance>> workerToAssignmentMap) {
        return workerToAssignmentMap.entrySet().stream().min(Comparator.comparingInt(o -> o.getValue().size())).get();

    }

    private Map.Entry<String, Queue<Instance>> findWorkerWithMostAssignments(Map<String, Queue<Instance>> workerToAssignmentMap) {
        return workerToAssignmentMap.entrySet().stream().max(Comparator.comparingInt(o -> o.getValue().size())).get();
    }

    @Data
    private static class RebalanceStats {
        @Override
        public String toString() {
            try {
                return ObjectMapperFactory.getThreadLocal().writerWithDefaultPrettyPrinter().writeValueAsString(workerStatsMap);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        @Builder
        @Data
        private static class WorkerStats {
            private int originalNumAssignments;
            private int numAssignmentsAfterRebalance;
            private int instancesAdded;
            private int instancesRemoved;
        }

        private Map<String, WorkerStats> workerStatsMap = new HashMap<>();

        public RebalanceStats(Map<String, Queue<Instance>> workerToAssignmentMap) {
            for(Map.Entry<String, Queue<Instance>> entry : workerToAssignmentMap.entrySet()) {
                WorkerStats workerStats = WorkerStats.builder()
                        .originalNumAssignments(entry.getValue().size())
                        .numAssignmentsAfterRebalance(entry.getValue().size())
                        .build();
                workerStatsMap.put(entry.getKey(), workerStats);
            }
        }

        private void decrementInstance(String workerId) {
            WorkerStats stats = workerStatsMap.get(workerId);
            if (stats == null) {
                throw new RuntimeException("Rebalance stats for worker " + workerId + " shouldn't be null");
            }

            stats.instancesRemoved++;
            stats.numAssignmentsAfterRebalance--;
        }

        private void incrementInstance(String workerId) {
            WorkerStats stats = workerStatsMap.get(workerId);
            if (stats == null) {
                throw new RuntimeException("Rebalance stats for worker " + workerId + " shouldn't be null");
            }

            stats.instancesAdded++;
            stats.numAssignmentsAfterRebalance++;
        }
    }
}
