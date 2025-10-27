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
package org.apache.pulsar.broker.service;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test
public class BrokerEventLoopShutdownTest {

    private static final String clusterName = "test";
    private LocalBookkeeperEnsemble bk;

    @BeforeClass(alwaysRun = true)
    public void setup() throws Exception {
        BrokerService.GRACEFUL_SHUTDOWN_QUIET_PERIOD_MAX_MS.setValue(3600000);
        bk = new LocalBookkeeperEnsemble(1, 0, () -> 0);
        bk.start();
    }

    @AfterClass(alwaysRun = true, timeOut = 30000)
    public void cleanup() throws Exception {
        bk.stop();
    }

    @Test(timeOut = 60000)
    public void testCloseOneBroker() throws Exception {
        @Cleanup final var broker0 = new PulsarService(brokerConfig());
        @Cleanup final var broker1 = new PulsarService(brokerConfig());
        broker0.start();
        broker1.start();

        final var admin = broker0.getAdminClient();
        if (!admin.clusters().getClusters().contains(clusterName)) {
            admin.clusters().createCluster(clusterName, ClusterData.builder().build());
            admin.tenants().createTenant("public", TenantInfo.builder()
                    .allowedClusters(Collections.singleton(clusterName)).build());
            admin.namespaces().createNamespace("public/default");
        }

        final var startNs = System.nanoTime();
        broker0.close();
        final var closeTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
        Assert.assertTrue(closeTimeMs < 5000, "close time: " + closeTimeMs + " ms");
    }

    private ServiceConfiguration brokerConfig() {
        final var config = new ServiceConfiguration();
        config.setClusterName(clusterName);
        config.setAdvertisedAddress("localhost");
        config.setBrokerServicePort(Optional.of(0));
        config.setWebServicePort(Optional.of(0));
        config.setMetadataStoreUrl("zk:127.0.0.1:" + bk.getZookeeperPort());
        config.setManagedLedgerDefaultWriteQuorum(1);
        config.setManagedLedgerDefaultAckQuorum(1);
        config.setManagedLedgerDefaultEnsembleSize(1);

        config.setBrokerShutdownTimeoutMs(40000); // the actual timeout is 1/4 (10 second) for each event loop
        return config;
    }
}
