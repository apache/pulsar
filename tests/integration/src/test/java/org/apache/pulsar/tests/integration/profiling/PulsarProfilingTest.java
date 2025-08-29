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
package org.apache.pulsar.tests.integration.profiling;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.tests.ManualTestUtil;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.utils.DockerUtils;
import org.testcontainers.containers.GenericContainer;
import org.testng.annotations.Test;

/**
 * Test profiling.
 *
 * Example usage:
 * ./build/build_java_test_image.sh -Ddocker.install.asyncprofiler=true
 * export PULSAR_TEST_IMAGE_NAME=apachepulsar/java-test-image:latest
 * export NETTY_LEAK_DETECTION=off
 * export ENABLE_MANUAL_TEST=true
 * mvn -am -pl tests/integration -DskipTests install
 * mvn -DintegrationTests -pl tests/integration -Dtest=PulsarProfilingTest -DredirectTestOutputToFile=false test
 */
@Slf4j
public class PulsarProfilingTest extends PulsarTestSuite {
    private static final String DEFAULT_PULSAR_MEM = "-Xms512m -Xmx1g";

    static class PulsarPerfContainer extends GenericContainer<PulsarPerfContainer> {
        private final String brokerHostname;

        public PulsarPerfContainer(String clusterName,
                                   String brokerHostname,
                                   String hostname) {
            super(PulsarContainer.DEFAULT_IMAGE_NAME);
            this.brokerHostname = brokerHostname;
            withCreateContainerCmdModifier(createContainerCmd -> {
                createContainerCmd.withHostName(hostname);
                createContainerCmd.withName(clusterName + "-" + hostname);
            });
            withEnv("PULSAR_MEM", DEFAULT_PULSAR_MEM);
            setCommand("sleep 1000000");
        }

        public CompletableFuture<Long> consume(String topicName) throws Exception {
            return DockerUtils.runCommandAsyncWithLogging(getDockerClient(), getContainerId(),
                    "/pulsar/bin/pulsar-perf", "consume", topicName,
                    "-u", "pulsar://" + brokerHostname + ":6650",
                    "-st", "Shared",
                    "-m", "1000000");
        }

        public CompletableFuture<Long> produce(String topicName) throws Exception {
            return DockerUtils.runCommandAsyncWithLogging(getDockerClient(), getContainerId(),
                    "/pulsar/bin/pulsar-perf", "produce", topicName,
                    "-u", "pulsar://" + brokerHostname + ":6650",
                    "-au", "http://" + brokerHostname + ":8080",
                    "-r", "1000000", "-m", "1000100");
        }
    }

    private PulsarPerfContainer perfConsume;
    private PulsarPerfContainer perfProduce;

    @Override
    public void setupCluster() throws Exception {
        ManualTestUtil.skipManualTestIfNotEnabled();
        super.setupCluster();
    }

    @Override
    protected PulsarClusterSpec.PulsarClusterSpecBuilder beforeSetupCluster(String clusterName,
        PulsarClusterSpec.PulsarClusterSpecBuilder specBuilder) {
        specBuilder.profileBroker(true);
        String brokerHostname = clusterName + "-pulsar-broker-0";
        perfProduce = new PulsarPerfContainer(clusterName, brokerHostname, "perf-produce");
        perfConsume = new PulsarPerfContainer(clusterName, brokerHostname, "perf-consume");
        specBuilder.externalServices(Map.of("pulsar-produce", perfProduce, "pulsar-consume", perfConsume));
        specBuilder.brokerEnvs(Map.of("PULSAR_MEM", DEFAULT_PULSAR_MEM));
        specBuilder.bookkeeperEnvs(Map.of("PULSAR_MEM", DEFAULT_PULSAR_MEM));
        return specBuilder;
    }

    @Test
    public void profileTest() throws Exception {
        String topicName = generateTopicName("profiletest", true);
        CompletableFuture<Long> consumeFuture = perfConsume.consume(topicName);
        Thread.sleep(1000);
        CompletableFuture<Long> produceFuture = perfConsume.produce(topicName);
        FutureUtil.waitForAll(List.of(consumeFuture, produceFuture)).get();
    }
}
