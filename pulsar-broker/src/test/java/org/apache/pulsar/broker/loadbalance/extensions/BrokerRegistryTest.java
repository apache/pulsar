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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit test for {@link BrokerRegistry}.
 */
public class BrokerRegistryTest {

    private ExecutorService executor;

    private LocalBookkeeperEnsemble bkEnsemble;

    private PulsarService pulsar1;

    private PulsarService pulsar2;

    // Make sure the load manager don't register itself to `/loadbalance/brokers/{lookupServiceAddress}`
    public static class MockLoadManager implements LoadManager {

        @Override
        public void start() throws PulsarServerException {
            // No-op
        }

        @Override
        public boolean isCentralized() {
            return false;
        }

        @Override
        public Optional<ResourceUnit> getLeastLoaded(ServiceUnitId su) throws Exception {
            return Optional.empty();
        }

        @Override
        public LoadManagerReport generateLoadReport() throws Exception {
            return null;
        }

        @Override
        public void setLoadReportForceUpdateFlag() {
            // No-op
        }

        @Override
        public void writeLoadReportOnZookeeper() throws Exception {
            // No-op
        }

        @Override
        public void writeResourceQuotasToZooKeeper() throws Exception {
            // No-op
        }

        @Override
        public List<Metrics> getLoadBalancingMetrics() {
            return null;
        }

        @Override
        public void doLoadShedding() {
            // No-op
        }

        @Override
        public void doNamespaceBundleSplit() throws Exception {
            // No-op
        }

        @Override
        public void disableBroker() throws Exception {
            // No-op
        }

        @Override
        public Set<String> getAvailableBrokers() throws Exception {
            return null;
        }

        @Override
        public CompletableFuture<Set<String>> getAvailableBrokersAsync() {
            return null;
        }

        @Override
        public void stop() throws PulsarServerException {
            // No-op
        }

        @Override
        public void initialize(PulsarService pulsar) {
            // No-op
        }
    }

    @BeforeMethod
    void setup() throws Exception {
        executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());
        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        // Start broker 1
        ServiceConfiguration config1 = new ServiceConfiguration();
        config1.setLoadBalancerEnabled(false);
        config1.setLoadManagerClassName(MockLoadManager.class.getName());
        config1.setClusterName("use");
        config1.setWebServicePort(Optional.of(0));
        config1.setMetadataStoreUrl("zk:127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config1.setBrokerShutdownTimeoutMs(0L);
        config1.setBrokerServicePort(Optional.of(0));
        config1.setAdvertisedAddress("localhost");
        createCluster(bkEnsemble.getZkClient(), config1);
        pulsar1 = new PulsarService(config1);
        pulsar1.start();

        // Start broker 2
        ServiceConfiguration config2 = new ServiceConfiguration();
        config2.setLoadBalancerEnabled(false);
        config2.setLoadManagerClassName(MockLoadManager.class.getName());
        config2.setClusterName("use");
        config2.setWebServicePort(Optional.of(0));
        config2.setMetadataStoreUrl("zk:127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config2.setBrokerShutdownTimeoutMs(0L);
        config2.setBrokerServicePort(Optional.of(0));
        config2.setAdvertisedAddress("localhost");
        pulsar2 = new PulsarService(config2);
        pulsar2.start();
    }

    @AfterMethod(alwaysRun = true)
    void shutdown() throws Exception {
        executor.shutdownNow();

        pulsar2.close();
        pulsar1.close();

        bkEnsemble.stop();
    }

    @Test(timeOut = 30 * 1000)
    public void testRegister() throws Exception {
        BrokerRegistryImpl brokerRegistry1 = new BrokerRegistryImpl(pulsar1);
        BrokerRegistryImpl brokerRegistry2 = new BrokerRegistryImpl(pulsar2);

        Set<String> address = new HashSet<>();
        brokerRegistry1.listen((lookupServiceAddress, type) -> {
            address.add(lookupServiceAddress);
        });

        brokerRegistry1.start();
        brokerRegistry2.start();
        brokerRegistry1.register();
        brokerRegistry2.register();

        assertEquals(brokerRegistry1.getAvailableBrokersAsync().get().size(), 2);
        assertEquals(brokerRegistry2.getAvailableBrokersAsync().get().size(), 2);
        Awaitility.await().atMost(Duration.ofSeconds(5))
                        .untilAsserted(() -> {
                            assertEquals(brokerRegistry1.brokerLookupDataMap.size(), 2);
                            assertEquals(brokerRegistry2.brokerLookupDataMap.size(), 2);
                            assertEquals(address.size(), 2);
                        });

        assertEquals(address, brokerRegistry1.brokerLookupDataMap.keySet());
        assertEquals(address, brokerRegistry2.brokerLookupDataMap.keySet());

        Optional<BrokerLookupData> lookupDataOpt =
                brokerRegistry1.lookupAsync(brokerRegistry2.getLookupServiceAddress()).get();
        assertTrue(lookupDataOpt.isPresent());
        assertEquals(lookupDataOpt.get().brokerVersion(), pulsar2.getBrokerVersion());

        brokerRegistry1.unregister();
        assertEquals(brokerRegistry2.getAvailableBrokersAsync().get().size(), 1);

        brokerRegistry1.close();
        brokerRegistry2.close();
    }


    private void createCluster(ZooKeeper zk, ServiceConfiguration config) throws Exception {
        ZkUtils.createFullPathOptimistic(zk, "/admin/clusters/" + config.getClusterName(),
                ObjectMapperFactory.getThreadLocal().writeValueAsBytes(
                        ClusterData.builder()
                                .serviceUrl("http://" + config.getAdvertisedAddress() + ":" + config.getWebServicePort()
                                        .get())
                                .build()),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
}

