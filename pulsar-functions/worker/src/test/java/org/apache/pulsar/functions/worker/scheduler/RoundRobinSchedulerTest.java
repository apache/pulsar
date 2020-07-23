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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.proto.Function;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class RoundRobinSchedulerTest {

    @Test
    public void testRebalance() {
        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(0)
                .build();

        List<Function.Assignment> assignments = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            Function.Assignment assignment1 = Function.Assignment.newBuilder()
                    .setWorkerId("worker-1")
                    .setInstance(Function.Instance.newBuilder()
                            .setFunctionMetaData(function1).setInstanceId(i).build())
                    .build();

            assignments.add(assignment1);
        }

        Set<String> workers = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            workers.add("worker-" + i);
        }

        RoundRobinScheduler roundRobinScheduler = new RoundRobinScheduler();

        List<Function.Assignment> newAssignments = roundRobinScheduler.rebalance(assignments, workers);

        Map<String, Integer> workerAssignments = new HashMap<>();
        for (Function.Assignment assignment : newAssignments) {
            Integer count = workerAssignments.get((assignment.getWorkerId()));
            if (count == null) {
                count = 0;
            }
            count++;
            workerAssignments.put(assignment.getWorkerId(), count);
        }

        Assert.assertEquals(workerAssignments.size(), 2);
        for (Map.Entry<String, Integer> entry : workerAssignments.entrySet()) {
            Assert.assertEquals(entry.getValue().intValue(), 3);
        }
    }
}
