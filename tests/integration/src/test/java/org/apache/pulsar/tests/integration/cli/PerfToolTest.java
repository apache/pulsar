/*
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
package org.apache.pulsar.tests.integration.cli;

import static org.testng.Assert.fail;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.tests.integration.containers.ChaosContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.containers.ZKContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.messaging.TopicMessagingBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PerfToolTest extends TopicMessagingBase {

    private static final int MESSAGE_COUNT = 50;

    /**
     * {@code --client-api V5} drives the V5 client against the same {@code persistent://} topic the v4
     * tests use: the V5 SDK serves a non-scalable topic through a synthetic layout, and the integration
     * cluster does not pre-create scalable topics.
     */
    private static final String[] V5_CLIENT = {"--client-api", "V5"};
    private static final String[] DEFAULT_CLIENT = {};

    @Test
    public void testProduce() throws Exception {
        String serviceUrl = "pulsar://" + pulsarCluster.getProxy().getContainerName() + ":"
                + PulsarContainer.BROKER_PORT;
        final String topicName = getNonPartitionedTopic("testProduce", true);
        // Using the ZK container as it is separate from brokers, so its environment resembles real world usage more
        ZKContainer clientToolContainer = pulsarCluster.getZooKeeper();
        ContainerExecResult produceResult =
                produceWithPerfTool(clientToolContainer, V5_CLIENT, serviceUrl, topicName, MESSAGE_COUNT);
        checkOutputForLogs(produceResult, "PerformanceProducer - Using the V5 client",
                "PerformanceProducer - Aggregated throughput stats",
                "PerformanceProducer - Aggregated latency stats");
    }

    @Test
    public void testConsume() throws Exception {
        String serviceUrl = "pulsar://" + pulsarCluster.getProxy().getContainerName() + ":"
                + PulsarContainer.BROKER_PORT;
        final String topicName = getNonPartitionedTopic("testConsume", true);
        // Using the ZK container as it is separate from brokers, so its environment resembles real world usage more
        ZKContainer clientToolContainer = pulsarCluster.getZooKeeper();
        ContainerExecResult consumeResult =
                consumeWithPerfTool(clientToolContainer, V5_CLIENT, serviceUrl, topicName);
        checkOutputForLogs(consumeResult, "PerformanceConsumer - Using the V5 client",
                "PerformanceConsumer - Aggregated throughput stats",
                "PerformanceConsumer - Aggregated latency stats");
    }

    @Test
    public void testRead() throws Exception {
        String serviceUrl = "pulsar://" + pulsarCluster.getProxy().getContainerName() + ":"
                + PulsarContainer.BROKER_PORT;
        final String topicName = getNonPartitionedTopic("testRead", true);
        // Using the ZK container as it is separate from brokers, so its environment resembles real world usage more
        ZKContainer clientToolContainer = pulsarCluster.getZooKeeper();
        ContainerExecResult readResult = readWithPerfTool(clientToolContainer, V5_CLIENT, serviceUrl, topicName);
        checkOutputForLogs(readResult, "PerformanceReader - Using the V5 client",
                "PerformanceReader - Aggregated throughput stats ",
                "PerformanceReader - Aggregated latency stats");
    }

    /** A {@code persistent://} topic selects the v4 client when {@code --client-api} is not given. */
    @Test
    public void testProduceV4() throws Exception {
        String serviceUrl = "pulsar://" + pulsarCluster.getProxy().getContainerName() + ":"
                + PulsarContainer.BROKER_PORT;
        final String topicName = getNonPartitionedTopic("testProduceV4", true);
        ZKContainer clientToolContainer = pulsarCluster.getZooKeeper();
        ContainerExecResult produceResult =
                produceWithPerfTool(clientToolContainer, DEFAULT_CLIENT, serviceUrl, topicName, MESSAGE_COUNT);
        checkOutputForLogs(produceResult, "PerformanceProducer - Using the v4 client",
                "PerformanceProducer - Aggregated throughput stats",
                "PerformanceProducer - Aggregated latency stats");
    }

    @Test
    public void testConsumeV4() throws Exception {
        String serviceUrl = "pulsar://" + pulsarCluster.getProxy().getContainerName() + ":"
                + PulsarContainer.BROKER_PORT;
        final String topicName = getNonPartitionedTopic("testConsumeV4", true);
        ZKContainer clientToolContainer = pulsarCluster.getZooKeeper();
        ContainerExecResult consumeResult =
                consumeWithPerfTool(clientToolContainer, DEFAULT_CLIENT, serviceUrl, topicName);
        checkOutputForLogs(consumeResult, "PerformanceConsumer - Using the v4 client",
                "PerformanceConsumer - Aggregated throughput stats",
                "PerformanceConsumer - Aggregated latency stats");
    }

    @Test
    public void testReadV4() throws Exception {
        String serviceUrl = "pulsar://" + pulsarCluster.getProxy().getContainerName() + ":"
                + PulsarContainer.BROKER_PORT;
        final String topicName = getNonPartitionedTopic("testReadV4", true);
        ZKContainer clientToolContainer = pulsarCluster.getZooKeeper();
        ContainerExecResult readResult =
                readWithPerfTool(clientToolContainer, DEFAULT_CLIENT, serviceUrl, topicName);
        checkOutputForLogs(readResult, "PerformanceReader - Using the v4 client",
                "PerformanceReader - Aggregated throughput stats ",
                "PerformanceReader - Aggregated latency stats");
    }

    private ContainerExecResult produceWithPerfTool(ChaosContainer<?> container, String[] clientArgs, String url,
                                                    String topic, int messageCount) throws Exception {
        ContainerExecResult result = container.execCmd(perfCommand("produce", clientArgs, "-u", url, "-m",
                String.valueOf(messageCount), topic));

        return failOnError("Performance producer", result);
    }

    private ContainerExecResult consumeWithPerfTool(ChaosContainer<?> container, String[] clientArgs, String url,
                                                    String topic) throws Exception {
        CompletableFuture<ContainerExecResult> resultFuture = container.execCmdAsync(perfCommand("consume",
                clientArgs, "-u", url, "-m", String.valueOf(MESSAGE_COUNT), topic));
        produceWithPerfTool(container, clientArgs, url, topic, MESSAGE_COUNT);

        ContainerExecResult result = resultFuture.get(5, TimeUnit.SECONDS);
        return failOnError("Performance consumer", result);
    }

    private ContainerExecResult readWithPerfTool(ChaosContainer<?> container, String[] clientArgs, String url,
                                                 String topic) throws Exception {
        CompletableFuture<ContainerExecResult> resultFuture = container.execCmdAsync(perfCommand("read",
                clientArgs, "-u", url, "-n", String.valueOf(MESSAGE_COUNT), topic));
        produceWithPerfTool(container, clientArgs, url, topic, MESSAGE_COUNT);

        ContainerExecResult result = resultFuture.get(5, TimeUnit.SECONDS);
        return failOnError("Performance consumer", result);
    }

    private static String[] perfCommand(String command, String[] clientArgs, String... args) {
        List<String> commandLine = new ArrayList<>();
        commandLine.add("bin/pulsar-perf");
        commandLine.add(command);
        commandLine.addAll(Arrays.asList(clientArgs));
        commandLine.addAll(Arrays.asList(args));
        return commandLine.toArray(new String[0]);
    }

    private static ContainerExecResult failOnError(String processDesc, ContainerExecResult result) {
        if (result.getExitCode() != 0) {
            fail(processDesc + " failed. Command output:\n" + result.getStdout()
                    + "\nError output:\n" + result.getStderr());
        }
        return result;
    }

    private static void checkOutputForLogs(ContainerExecResult result, String... logs) {
        String output = result.getStdout();
        for (String log : logs) {
            Assert.assertTrue(output.contains(log),
                    "command output did not contain log message '" + log + "'.\nFull stdout is:\n" + output);
        }
    }

}
