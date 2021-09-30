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
package org.apache.pulsar.tests.integration.cli;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.tests.integration.containers.BKContainer;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;

import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test the healthcheck command.
 */
public class HealthCheckTest extends TestRetrySupport {

    private static final Logger log = LoggerFactory.getLogger(HealthCheckTest.class);

    private final PulsarClusterSpec spec = PulsarClusterSpec.builder()
        .clusterName("HealthCheckTest-" + UUID.randomUUID().toString().substring(0, 8))
        .numProxies(0)
        .numFunctionWorkers(0)
        .enablePrestoWorker(false).build();

    private PulsarCluster pulsarCluster = null;

    @BeforeMethod(alwaysRun = true)
    public final void setup() throws Exception {
        incrementSetupNumber();
        pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();
    }

    @AfterMethod(alwaysRun = true)
    public final void cleanup() {
        markCurrentSetupNumberCleaned();
        if (pulsarCluster != null) {
            pulsarCluster.stop();
            pulsarCluster = null;
        }
    }

    @Test
    public void testEverythingOK() throws Exception {
        for (BrokerContainer b : pulsarCluster.getBrokers()) {
            ContainerExecResult result = b.execCmd(PulsarCluster.ADMIN_SCRIPT, "brokers", "healthcheck");
            Assert.assertEquals(result.getExitCode(), 0);
            Assert.assertEquals(result.getStdout().trim(), "ok");
        }
    }

    private void assertHealthcheckFailure() throws Exception {
        for (BrokerContainer b : pulsarCluster.getBrokers()) {
            try {
                b.execCmd(PulsarCluster.ADMIN_SCRIPT, "brokers", "healthcheck");
                Assert.fail("Should always fail");
            } catch (ContainerExecException e) {
                Assert.assertEquals(e.getResult().getExitCode(), 1);
            }
        }
    }

    @Test
    public void testZooKeeperDown() throws Exception {
        pulsarCluster.getZooKeeper().execCmd("pkill", "-STOP", "-f", "QuorumPeerMain");
        assertHealthcheckFailure();
    }

    // Disabled until PulsarAdmin can time out (#2891)
    // @Test
    // public void testBrokerDown() throws Exception {
    //     for (BrokerContainer b : pulsarCluster.getBrokers()) {
    //         b.execCmd("pkill", "-STOP", "-f", "PulsarBrokerStarter");
    //     }
    //     assertHealthcheckFailure();
    // }

    @Test
    public void testBookKeeperDown() throws Exception {
        for (BKContainer b : pulsarCluster.getBookies()) {
            b.execCmd("pkill", "-STOP", "-f", "Main");
        }
        assertHealthcheckFailure();
    }

    private static Map<String, String> parseOutput(String output) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(output, new TypeReference<HashMap<String, String>>() {});
    }
}
