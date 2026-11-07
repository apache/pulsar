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

import static org.testng.Assert.assertTrue;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testng.annotations.Test;

/**
 * Pulsar version test class.
 */
public class PulsarVersionTest extends PulsarTestSuite {

    @Override
    protected boolean isClusterShared() {
        return true;
    }

    @Test
    public void getVersion() throws Exception {
        // Each command starts a JVM: they run at once
        BrokerContainer broker = pulsarCluster.getAnyBroker();
        CompletableFuture<ContainerExecResult> brokerVersion =
                broker.execCmdAsync(PulsarCluster.ADMIN_SCRIPT, "brokers", "version");
        List<CompletableFuture<ContainerExecResult>> versionOptions = List.of(
                broker.execCmdAsync(PulsarCluster.ADMIN_SCRIPT, "-v"),
                broker.execCmdAsync(PulsarCluster.ADMIN_SCRIPT, "--version"),
                broker.execCmdAsync(PulsarCluster.CLIENT_SCRIPT, "-v"),
                broker.execCmdAsync(PulsarCluster.CLIENT_SCRIPT, "--version"));
        String version = brokerVersion.get().getStdout();
        for (CompletableFuture<ContainerExecResult> versionOption : versionOptions) {
            assertTrue(versionOption.get().getStdout().contains(version));
        }
    }

}
