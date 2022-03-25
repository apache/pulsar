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
package org.apache.pulsar.tests.integration.functions.java;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.MappingIterator;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.policies.data.FunctionStatusUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.functions.PulsarFunctionsTest;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.Runtime;
import org.apache.pulsar.tests.integration.topologies.FunctionRuntimeType;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Slf4j
public class PulsarWorkerRebalanceDrainTest extends PulsarFunctionsTest {

    final String UrlProtocolPrefix = "http://";
    final String WorkerRebalanceUrlSuffix = "/admin/v2/worker/rebalance";
    final String WorkerDrainAtLeaderUrlSuffix = "/admin/v2/worker/leader/drain?workerId=";
    final int NumFunctionsAssignedOnEachWorker = 2;
    final int NumAdditionalWorkersAtSetup = 1;

    PulsarWorkerRebalanceDrainTest(FunctionRuntimeType functionRuntimeType) {
		super(functionRuntimeType);
    }

    @Override
    public void setupCluster() throws Exception {
        super.setupCluster();
        pulsarCluster.setupFunctionWorkers(randomName(5), functionRuntimeType, NumAdditionalWorkersAtSetup);
        log.debug("PulsarWorkerRebalanceDrainTest: set up a total of {} function workers, of type {}",
                pulsarCluster.getAlWorkers().size(), functionRuntimeType);
    }

    @Test(groups = {"java_function", "rebalance_drain", "rebalance"})
    public void testRebalanceWorkers() throws Exception {
        testRebalance();
        log.info("Done with testRebalance");
    }

    @Test(groups = {"java_function", "rebalance_drain", "drain"})
    public void testDrainWorkers() throws Exception {
        testDrain();
        log.info("Done with testDrain");
    }

    private List<WorkerInfo> workerInfoDecode(String json) throws IOException {
	    try (MappingIterator<WorkerInfo> it = ObjectMapperFactory.getThreadLocal().readerFor(WorkerInfo.class)
                .readValues(json)) {
            return it.readAll();
        }
    }

    // Parse a retrieved function-assignments list.
    private List<Map<String, Collection<String>>> functionAssignmentsDecode(String json) throws IOException {
        // Ad hoc parsing of the list of maps for function assignments.
        // It is ad hoc because there doesn't appear to be any class exported from the function-worker code
        // for this structure.
        // The list is expected to be of the following form (this is from a real example):
        //   String json =
        //       "pulsar-functions-worker-process-hurkp-1    "
        //       + " [public/default/testDrainFunctionality-cipscktc:0, public/default/testDrainFunctionality-mzaudmyx:0] "
        //       + "pulsar-functions-worker-process-hurkp-0    "
        //       + " [public/default/testDrainFunctionality-morjleeh:0, public/default/testDrainFunctionality-owsxgiuo:0] "
        //       + "pulsar-functions-worker-process-qmcao-0    "
        //       + "[public/default/testDrainFunctionality-nsnqbnlc:0, public/default/testDrainFunctionality-tbakebis:0] ";

        final int nextWorkerStart = 0;
        String remainingJson = json;
        String nextFunctionList;
        String nextWorker;
        boolean moreToParse = true;
        List<Map<String, Collection<String>>> retVal = new ArrayList<>();

        while (moreToParse) {
            int nextFunctionListStart = remainingJson.indexOf("[");
            int nextFunctionListEnd = remainingJson.indexOf("]");
            nextWorker = remainingJson.substring(nextWorkerStart, nextFunctionListStart);
            nextWorker = nextWorker.replaceAll("\\s+","");
            nextFunctionList = remainingJson.substring(nextFunctionListStart + 1, nextFunctionListEnd);
            String[] funcAssignments = nextFunctionList.split(",");
            Map<String, Collection<String>> curMap = new HashMap<>();
            curMap.put(nextWorker, Arrays.asList(funcAssignments));
            log.info("Found new entry: {}, {}", nextWorker, funcAssignments);
            retVal.add(curMap);
            log.info("retVal is {}", retVal);

            // If we are already at the end, or if there are no more opening brackets
            // (corresponding to function assignments), we are done.
            try {
                remainingJson = remainingJson.substring(nextFunctionListEnd + 1);
            } catch (Throwable t) {
                if (log.isDebugEnabled()) {
                    log.debug("Got exception {} while moving past function-list-end", t.getMessage());
                }
                moreToParse = false;
            }
            if (remainingJson.indexOf("[") < 0) {
                moreToParse = false;
            }
        }
        return retVal;
    }

    private List<WorkerInfo> getClusterStatus() throws Exception {
        val result = pulsarCluster.getAnyWorker().execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "functions-worker",
                "get-cluster"
        );
        log.debug("getClusterStatus result is: {}", result);
        return workerInfoDecode(result.getStdout());
    }

    private WorkerInfo getClusterLeader() throws Exception {
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "functions-worker",
                "get-cluster-leader"
        );
        List<WorkerInfo> winfos = workerInfoDecode(result.getStdout());
        assertEquals(winfos.size(), 1);
        return workerInfoDecode(result.getStdout()).get(0);
    }

    private List<Map<String, Collection<String>>> getFunctionAssignments() throws Exception
    {
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "functions-worker",
                "get-function-assignments"
        );
        log.debug("getFunctionAssignments result is: {}", result);
        return functionAssignmentsDecode(result.getStdout());
    }

    // Get the total number of functions assigned to all workers in the given function-info.
    private int getFuncAssignmentsCount(List<Map<String, Collection<String>>> finfos) {
        int funcCount = 0;
        for (val l : finfos) {
            for (val m : l.entrySet()) {
                if (log.isDebugEnabled()) {
                    log.debug("accumulating for key={}, value={} (size {})",
                            m.getKey(), m.getValue(), m.getValue().size());
                }
                funcCount += m.getValue().size();
            }
        }
        return funcCount;
    }

    // Get the min number of functions assigned to any worker in the given function-info.
    private int getMinFuncAssignmentOnAnyWorker(List<Map<String, Collection<String>>> finfos) {
        int minFuncCount = Integer.MAX_VALUE;
        for (val l : finfos) {
            for (val m : l.entrySet()) {
                if (log.isDebugEnabled()) {
                    log.debug("comparing current_min={} with key={}, value={} (size {})",
                            minFuncCount, m.getKey(), m.getValue(), m.getValue().size());
                }
                minFuncCount = Math.min(minFuncCount, m.getValue().size());
            }
        }
        return minFuncCount;
    }

    private void callRebalance() throws Exception {
        val leader = getClusterLeader();
        val worker = pulsarCluster.getWorker(leader.getWorkerId());
        assertTrue(worker != null);

        String rebalanceUrl = UrlProtocolPrefix
                + "localhost"
                + ":"
                + leader.getPort()
                + WorkerRebalanceUrlSuffix;
        ContainerExecResult result = worker.execCmd(
                PulsarCluster.CURL,
                "-X",
                "PUT",
                rebalanceUrl
        );
        if (log.isDebugEnabled()) {
            log.debug("callRebalance: leader's rebalance url is: {}", rebalanceUrl);
            log.debug("callRebalance: curl for rebalance: result is {}", result);
        }
    }

    private void callDrain(final String workerToDrain) throws Exception {
        val leader = getClusterLeader();
        val worker = pulsarCluster.getWorker(leader.getWorkerId());
        assertTrue(worker != null);

        String drainUrl = UrlProtocolPrefix
                + "localhost"
                + ":"
                + leader.getPort()
                + WorkerDrainAtLeaderUrlSuffix
                + workerToDrain;
        ContainerExecResult result = worker.execCmd(
                PulsarCluster.CURL,
                "-X",
                "PUT",
                drainUrl
        );
        if (log.isDebugEnabled()) {
            log.debug("callDrain: leader's drain url is: {}", drainUrl);
            log.debug("callDrain: curl for drain: result is {}", result);
        }
    }

    private void createFunctionWorker(String functionName, String topicPrefix) throws Exception {
	    String suffix = functionName + randomName(8);
        String inputTopicName = "persistent://public/default/" + topicPrefix + "-input-" + suffix;
        String outputTopicName = topicPrefix + "-output-" + suffix;
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build()) {
            admin.topics().createNonPartitionedTopic(inputTopicName);
            admin.topics().createNonPartitionedTopic(outputTopicName);
        }

        submitFunction(
                Runtime.JAVA, inputTopicName, outputTopicName, functionName, null, SERDE_JAVA_CLASS,
                SERDE_OUTPUT_CLASS, Collections.singletonMap(topicPrefix, outputTopicName)
        );
    }

    private void showWorkerStatus(String callerContext) throws Exception {
        List<WorkerInfo> winfos = getClusterStatus();
        assertEquals(winfos.size(), pulsarCluster.getAlWorkers().size());
        log.info("{} get-cluster retrieved info about {} workers", callerContext, winfos.size());
        winfos.forEach(w -> log.info("{} get-cluster worker-info: {}", callerContext, w));

        val leaderInfo = getClusterLeader();
        log.info("{} get-cluster-leader info: {}", callerContext, leaderInfo);

        val finfos = getFunctionAssignments();
        log.info("{} get-function-assignments retrieved info about {} workers with {} functions",
                callerContext, finfos.size(), getFuncAssignmentsCount(finfos));
        finfos.forEach(f -> log.info("{} get-function-assignments info: {}", callerContext, f));
    }

    private void allocateFunctions(String callingTest, String topicPrefix) throws  Exception {
        // Allocate functions until there are NumFunctionsAssignedOnEachWorker on each worker, on the average.
        ContainerExecResult result;
        int numFunctions = pulsarCluster.getAlWorkers().size() * NumFunctionsAssignedOnEachWorker;

        for (int ix = 0; ix < numFunctions; ix++) {
            String functionName = callingTest + "-" + randomName(8);
            createFunctionWorker(functionName, topicPrefix);

            result = pulsarCluster.getAnyWorker().execCmd(
                    PulsarCluster.ADMIN_SCRIPT,
                    "functions",
                    "status",
                    "--tenant", "public",
                    "--namespace", "default",
                    "--name", functionName
            );

            FunctionStatus functionStatus = FunctionStatusUtil.decode(result.getStdout());
            log.debug("{}}: functionStatus is {}", callingTest, functionStatus);

            assertEquals(functionStatus.getNumInstances(), 1);
            assertEquals(functionStatus.getInstances().get(0).getStatus().isRunning(), true);
        }
    }

    private void testRebalance() throws Exception {
        // Add some workers; then call rebalance, and check that functions were assigned to all of the workers.
        allocateFunctions("testRebalanceAddWorkers", "test-rebalance");

        if (log.isDebugEnabled()) {
            this.showWorkerStatus("testRebalanceAddWorkers after allocating functions");
        }

        WorkerInfo oldClusterLeaderInfo = getClusterLeader();
        log.info("Cluster leader before adding more workers is: {}", oldClusterLeaderInfo);

        List<Map<String, Collection<String>>> startFinfos = getFunctionAssignments();
        int startFuncCount = getFuncAssignmentsCount(startFinfos);
        log.info("testRebalanceAddWorkers: got info about {} workers with {} functions before creating new workers",
                startFinfos.size(), startFuncCount);
        // Check that there are NumFunctionsAssignedOnEachWorker functions assigned to each worker,
        // since the assignment is round-robin by default.
        assertEquals(getMinFuncAssignmentOnAnyWorker(startFinfos), NumFunctionsAssignedOnEachWorker);

        // Add a few more workers, to test rebalance
        int initialNumWorkers = pulsarCluster.getAlWorkers().size();
        final int numWorkersToAdd = 2;
        log.info("testRebalanceAddWorkers: cluster has {} FunctionWorkers; going to set up {} more",
                pulsarCluster.getAlWorkers().size(), numWorkersToAdd);
        pulsarCluster.setupFunctionWorkers(randomName(5), functionRuntimeType, numWorkersToAdd);
        assertEquals(pulsarCluster.getAlWorkers().size(), initialNumWorkers + numWorkersToAdd);
        log.info("testRebalanceAddWorkers: got a total of {} function workers, of type {}",
                pulsarCluster.getAlWorkers().size(), functionRuntimeType);

        this.showWorkerStatus("testRebalanceAddWorkers status after adding more workers");

        WorkerInfo newClusterLeaderInfo = getClusterLeader();
        log.info("Cluster leader after adding {} workers is: {}", numWorkersToAdd, newClusterLeaderInfo);
        // Leadership should not have changed.
        assertTrue(oldClusterLeaderInfo.getWorkerId().compareTo(newClusterLeaderInfo.getWorkerId()) == 0);

        this.showWorkerStatus("testRebalanceAddWorkers after adding more workers");

        // Rebalance.
        callRebalance();
        this.showWorkerStatus("testRebalanceAddWorkers after rebalance");

        List<Map<String, Collection<String>>> endFinfos = getFunctionAssignments();
        int endFuncCount = getFuncAssignmentsCount(endFinfos);
        log.info("testRebalanceAddWorkers: got info about {} workers with {} functions after rebalance",
                endFinfos.size(), endFuncCount);

        assertEquals(endFinfos.size() - startFinfos.size(), numWorkersToAdd);
        assertEquals(startFuncCount, endFuncCount);
        // Since scheduling is round-robin (default), we expect the minimum number of  function assignments
        // on any worker to be the floor of the average number of functions per worker.
        int minFuncsPerWorker = (int) Math.floor(endFuncCount / endFinfos.size());
        assertEquals(getMinFuncAssignmentOnAnyWorker(endFinfos),  minFuncsPerWorker);

        // Since we increased the number of workers, the minFuncsPerWorker should evaluate to some
        // value less than the erstwhile NumFunctionsAssignedOnEachWorker.
        assertTrue(minFuncsPerWorker < NumFunctionsAssignedOnEachWorker);
    }

    private void testDrain() throws Exception {
        allocateFunctions("testDrain", "test-drain");

        val startFinfos = getFunctionAssignments();
        int startFuncCount = getFuncAssignmentsCount(startFinfos);
        log.info("testDrain: got info about {} workers with {} functions before drain",
                startFinfos.size(), startFuncCount);

        if (log.isDebugEnabled()) {
            this.showWorkerStatus("testDrain after allocating functions");
        }

        WorkerInfo clusterLeaderInfo = getClusterLeader();

        // Drain
        callDrain(clusterLeaderInfo.getWorkerId());
        if (log.isDebugEnabled()) {
            this.showWorkerStatus("testDrain after drain");
        }

        val endFinfos = getFunctionAssignments();
        int endFuncCount = getFuncAssignmentsCount(endFinfos);
        log.info("testDrain: got info about {} workers with {} functions after drain",
                endFinfos.size(), endFuncCount);

        assertTrue(startFinfos.size() > endFinfos.size());
        assertEquals(startFuncCount, endFuncCount);

        // Since default scheduling is round-robin, we expect the minimum number of  function assignments
        // on any worker to be the floor of the average number of functions per worker.
        final int minFuncsPerWorker = (int) Math.floor(endFuncCount / endFinfos.size());
        assertEquals(getMinFuncAssignmentOnAnyWorker(endFinfos), minFuncsPerWorker);
    }
}
