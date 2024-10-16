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
package org.apache.pulsar.tests.integration.bookkeeper;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertEquals;

/**
 * Test bookkeeper setup with http server enabled.
 */
@Slf4j
public class BookkeeperInstallWithHttpServerEnabledTest extends PulsarClusterTestBase {

    @BeforeClass(alwaysRun = true)
    @Override
    public final void setupCluster() throws Exception {
        incrementSetupNumber();

        final String clusterName = Stream.of(this.getClass().getSimpleName(), randomName(5))
                .filter(s -> !s.isEmpty())
                .collect(joining("-"));
        bookkeeperEnvs.put("httpServerEnabled", "true");
        bookieAdditionalPorts.add(8000);
        PulsarClusterSpec spec = PulsarClusterSpec.builder()
                .numBookies(2)
                .numBrokers(1)
                .bookkeeperEnvs(bookkeeperEnvs)
                .bookieAdditionalPorts(bookieAdditionalPorts)
                .clusterName(clusterName)
                .build();

        log.info("Setting up cluster {} with {} bookies, {} brokers",
                spec.clusterName(), spec.numBookies(), spec.numBrokers());

        pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();

        log.info("Cluster {} is setup", spec.clusterName());
    }

    @AfterClass(alwaysRun = true)
    @Override
    public final void tearDownCluster() throws Exception {
        super.tearDownCluster();
    }

    @Test
    public void testBookieHttpServerIsRunning() throws Exception {
        ContainerExecResult result = pulsarCluster.getAnyBookie().execCmd(
                PulsarCluster.CURL,
                "-X",
                "GET",
                "http://localhost:8000/heartbeat");
        assertEquals(result.getExitCode(), 0);
        assertEquals(result.getStdout(), "OK\n");
    }
}
