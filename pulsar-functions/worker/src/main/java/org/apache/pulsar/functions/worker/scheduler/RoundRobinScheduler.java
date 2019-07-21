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

import org.apache.pulsar.functions.proto.Function.Assignment;
import org.apache.pulsar.functions.proto.Function.Instance;

import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
}
