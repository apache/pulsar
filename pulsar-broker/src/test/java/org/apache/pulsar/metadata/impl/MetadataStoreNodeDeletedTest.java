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
package org.apache.pulsar.metadata.impl;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.bookkeeper.util.PortManager;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerWrapper;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MetadataStoreNodeDeletedTest {

    private static final String clusterName = "test";
    private final int zkPort = PortManager.nextFreePort();
    private final LocalBookkeeperEnsemble bk = new LocalBookkeeperEnsemble(2, zkPort, PortManager::nextFreePort);
    private PulsarService pulsar;

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
    }

    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        if (pulsar != null) {
            pulsar.close();
        }
        bk.stop();
    }

    @Test
    public void testLookupAfterSessionTimeout() throws Exception {
        final var metadataStore = (ZKMetadataStore) pulsar.getLocalMetadataStore();
        final var children = metadataStore.getChildren(LoadManager.LOADBALANCE_BROKERS_ROOT).get();
        Assert.assertEquals(children, List.of(pulsar.getBrokerId()));

        // Simulate the case that the node was somehow deleted (e.g. by session timeout)
        final var path = LoadManager.LOADBALANCE_BROKERS_ROOT + "/" + pulsar.getBrokerId();
        metadataStore.delete(path, Optional.empty());
        metadataStore.receivedNotification(new Notification(NotificationType.Deleted, path));
        Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            final var newChildren = metadataStore.getChildren(LoadManager.LOADBALANCE_BROKERS_ROOT).join();
            Assert.assertEquals(newChildren, List.of(pulsar.getBrokerId()));
        });

        // If the node is deleted by unregister(), it should not recreate the path
        ((ExtensibleLoadManagerWrapper) pulsar.getLoadManager().get()).get().getBrokerRegistry().unregister();
        Thread.sleep(3000);
        Assert.assertTrue(metadataStore.getChildren(LoadManager.LOADBALANCE_BROKERS_ROOT).join().isEmpty());
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
        config.setDefaultNumberOfNamespaceBundles(16);
        config.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        config.setLoadBalancerDebugModeEnabled(true);
        config.setBrokerShutdownTimeoutMs(100);
        return config;
    }
}
