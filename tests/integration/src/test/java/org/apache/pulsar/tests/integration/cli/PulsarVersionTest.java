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

import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

/**
 * Pulsar version test class.
 */
public class PulsarVersionTest extends TestRetrySupport {

    private static final String clusterNamePrefix = "pulsar-version";
    private PulsarCluster pulsarCluster;

    @Override
    @BeforeClass(alwaysRun = true)
    public final void setup() throws Exception {
        incrementSetupNumber();
        PulsarClusterSpec spec = PulsarClusterSpec.builder()
                .clusterName(String.format("%s-%s", clusterNamePrefix, RandomStringUtils.randomAlphabetic(6)))
                .build();
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
    public void getVersion() throws Exception {
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("brokers", "version");
        String version = result.getStdout();
        ContainerExecResult adminVersionShortOption = pulsarCluster.runAdminCommandOnAnyBroker("-v");
        assertTrue(adminVersionShortOption.getStdout().contains(version));
        ContainerExecResult adminVersionLongOption = pulsarCluster.runAdminCommandOnAnyBroker("--version");
        assertTrue(adminVersionLongOption.getStdout().contains(version));
        ContainerExecResult clientVersionShortOption = pulsarCluster.getAnyBroker().execCmd(
                PulsarCluster.CLIENT_SCRIPT, "-v");
        assertTrue(clientVersionShortOption.getStdout().contains(version));
        ContainerExecResult clientVersionLongOption = pulsarCluster.getAnyBroker().execCmd(
                PulsarCluster.CLIENT_SCRIPT, "--version");
        assertTrue(clientVersionLongOption.getStdout().contains(version));
    }

}
