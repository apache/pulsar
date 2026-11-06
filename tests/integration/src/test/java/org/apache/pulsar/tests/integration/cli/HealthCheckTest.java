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

import static org.awaitility.Awaitility.await;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.HealthChecker;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.tests.integration.containers.BKContainer;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test the healthcheck command.
 */
public class HealthCheckTest extends TestRetrySupport {

    private final PulsarClusterSpec spec = PulsarClusterSpec.builder()
        .clusterName("HealthCheckTest-" + UUID.randomUUID().toString().substring(0, 8))
        .numProxies(0)
        .numFunctionWorkers(0)
        // With the bookies paused, a check fails after about 30 s, and the bookies' ZooKeeper sessions can expire
        // meanwhile. A bookie registers again when it resumes, but its AutoRecovery daemon stops on the expired
        // session and shuts the bookie down.
        .bookkeeperEnvs(Map.of("autoRecoveryDaemonEnabled", "false"))
        // With ZooKeeper paused, a check fails once the broker gives up loading its health check topic: after 5 s
        // rather than 60 s, so that ZooKeeper resumes before any session in the cluster expires
        .brokerEnvs(Map.of("PULSAR_PREFIX_topicLoadTimeoutSeconds", "5"))
        .build();

    private PulsarCluster pulsarCluster = null;

    @Override
    @BeforeClass(alwaysRun = true)
    public final void setup() throws Exception {
        incrementSetupNumber();
        pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();
    }

    @Override
    @AfterClass(alwaysRun = true)
    public final void cleanup() {
        markCurrentSetupNumberCleaned();
        if (pulsarCluster != null) {
            pulsarCluster.stop();
            pulsarCluster = null;
        }
    }

    @Test
    public void testEverythingOK() throws Exception {
        for (CompletableFuture<ContainerExecResult> healthcheck : startHealthchecks()) {
            ContainerExecResult result = healthcheck.get();
            Assert.assertEquals(result.getExitCode(), 0);
            Assert.assertEquals(result.getStdout().trim(), "ok");
        }
    }

    private void assertHealthcheckFailure() throws Exception {
        for (CompletableFuture<ContainerExecResult> healthcheck : startHealthchecks()) {
            try {
                healthcheck.get();
                Assert.fail("Should always fail");
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof ContainerExecException cee)) {
                    throw e;
                }
                Assert.assertEquals(cee.getResult().getExitCode(), 1);
            }
        }
    }

    // The checks of all the brokers run at once, so that ZooKeeper or the bookies are down for the time of one
    private List<CompletableFuture<ContainerExecResult>> startHealthchecks() throws Exception {
        List<CompletableFuture<ContainerExecResult>> healthchecks = new ArrayList<>();
        for (BrokerContainer b : pulsarCluster.getBrokers()) {
            healthchecks.add(b.execCmdAsync(PulsarCluster.ADMIN_SCRIPT, "brokers", "healthcheck"));
        }
        return healthchecks;
    }

    // The cluster is shared by the test methods: a method that takes ZooKeeper or the bookies down waits until every
    // broker passes its health check again
    private void waitUntilHealthy() throws Exception {
        for (BrokerContainer b : pulsarCluster.getBrokers()) {
            try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(b.getHttpServiceUrl()).build()) {
                await().atMost(60, TimeUnit.SECONDS).ignoreExceptions()
                        .untilAsserted(() -> admin.brokers().healthcheck());
            }
        }
    }

    // Once a broker has loaded its health check topic, its check doesn't need ZooKeeper
    private void unloadHealthcheckTopics() throws Exception {
        for (BrokerContainer b : pulsarCluster.getBrokers()) {
            String brokerId = b.getHostName() + ":" + PulsarContainer.BROKER_HTTP_PORT;
            String topic = "persistent://" + String.format(NamespaceService.HEARTBEAT_NAMESPACE_FMT, brokerId) + "/"
                    + HealthChecker.HEALTH_CHECK_TOPIC_SUFFIX;
            try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(b.getHttpServiceUrl()).build()) {
                admin.topics().unload(topic);
            }
        }
    }

    @Test
    public void testZooKeeperDown() throws Exception {
        // The checks load the health check topics, which the unload then finds
        waitUntilHealthy();
        unloadHealthcheckTopics();
        pulsarCluster.getZooKeeper().execCmd("pkill", "-STOP", "java");
        try {
            assertHealthcheckFailure();
        } finally {
            pulsarCluster.getZooKeeper().execCmd("pkill", "-CONT", "java");
        }
        waitUntilHealthy();
    }

    @Test
    public void testBookKeeperDown() throws Exception {
        for (BKContainer b : pulsarCluster.getBookies()) {
            b.execCmd("pkill", "-STOP", "java");
        }
        try {
            assertHealthcheckFailure();
        } finally {
            for (BKContainer b : pulsarCluster.getBookies()) {
                b.execCmd("pkill", "-CONT", "java");
            }
        }
        waitUntilHealthy();
    }
}
