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
package org.apache.pulsar.broker.loadbalance.extensions;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.PortManager;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "flaky")
public class BrokerRegistryIntegrationTest {

    private static final String clusterName = "test";
    private final int zkPort = PortManager.nextFreePort();
    private final LocalBookkeeperEnsemble bk = new LocalBookkeeperEnsemble(2, zkPort, PortManager::nextFreePort);
    private PulsarService pulsar;
    private BrokerRegistry brokerRegistry;
    private String brokerMetadataPath;

    @BeforeClass
    protected void setup() throws Exception {
        bk.start();
        pulsar = new PulsarService(brokerConfig());
        pulsar.start();
        final var admin = pulsar.getAdminClient();
        admin.clusters().createCluster(clusterName, ClusterData.builder().build());
        admin.tenants().createTenant("public", TenantInfo.builder()
                .allowedClusters(Collections.singleton(clusterName)).build());
        admin.namespaces().createNamespace("public/default");
        brokerRegistry = ((ExtensibleLoadManagerWrapper) pulsar.getLoadManager().get()).get().getBrokerRegistry();
        brokerMetadataPath = LoadManager.LOADBALANCE_BROKERS_ROOT + "/" + pulsar.getBrokerId();
    }

    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        final var startMs = System.currentTimeMillis();
        if (pulsar != null) {
            pulsar.close();
        }
        final var elapsedMs = System.currentTimeMillis() - startMs;
        bk.stop();
        if (elapsedMs > 5000) {
            throw new RuntimeException("Broker took " + elapsedMs + "ms to close");
        }
    }

    @Test
    public void testRecoverFromNodeDeletion() throws Exception {
        // Simulate the case that the node was somehow deleted (e.g. by session timeout)
        Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> Assert.assertEquals(
                brokerRegistry.getAvailableBrokersAsync().join(), List.of(pulsar.getBrokerId())));
        pulsar.getLocalMetadataStore().delete(brokerMetadataPath, Optional.empty());
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> Assert.assertEquals(
                brokerRegistry.getAvailableBrokersAsync().join(), List.of(pulsar.getBrokerId())));

        // If the node is deleted by unregister(), it should not recreate the path
        brokerRegistry.unregister();
        Thread.sleep(3000);
        Assert.assertTrue(brokerRegistry.getAvailableBrokersAsync().get().isEmpty());

        // Restore the normal state
        brokerRegistry.registerAsync().get();
        Assert.assertEquals(brokerRegistry.getAvailableBrokersAsync().get(), List.of(pulsar.getBrokerId()));
    }

    @Test
    public void testRegisterAgain() throws Exception {
        Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> Assert.assertEquals(
                brokerRegistry.getAvailableBrokersAsync().join(), List.of(pulsar.getBrokerId())));
        final var metadataStore = pulsar.getLocalMetadataStore();
        final var oldResult = metadataStore.get(brokerMetadataPath).get().orElseThrow();
        log.info("Old result: {} {}", new String(oldResult.getValue()), oldResult.getStat().getVersion());
        brokerRegistry.registerAsync().get();

        Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            final var newResult = metadataStore.get(brokerMetadataPath).get().orElseThrow();
            log.info("New result: {} {}", new String(newResult.getValue()), newResult.getStat().getVersion());
            Assert.assertTrue(newResult.getStat().getVersion() > oldResult.getStat().getVersion());
            Assert.assertEquals(newResult.getValue(), oldResult.getValue());
        });
    }

    protected ServiceConfiguration brokerConfig() {
        final var config = new ServiceConfiguration();
        config.setClusterName(clusterName);
        config.setAdvertisedAddress("localhost");
        config.setBrokerServicePort(Optional.of(0));
        config.setWebServicePort(Optional.of(0));
        config.setMetadataStoreUrl("zk:127.0.0.1:" + bk.getZookeeperPort());
        config.setManagedLedgerDefaultWriteQuorum(1);
        config.setManagedLedgerDefaultAckQuorum(1);
        config.setManagedLedgerDefaultEnsembleSize(1);
        config.setDefaultNumberOfNamespaceBundles(16);
        config.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        config.setLoadBalancerDebugModeEnabled(true);
        config.setBrokerShutdownTimeoutMs(100);
        return config;
    }
}
