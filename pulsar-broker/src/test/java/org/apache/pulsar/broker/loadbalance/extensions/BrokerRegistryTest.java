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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for {@link BrokerRegistry}.
 */
public class BrokerRegistryTest {

    private final List<PulsarService> pulsarServices = new CopyOnWriteArrayList<>();

    private ExecutorService executor;

    private LocalBookkeeperEnsemble bkEnsemble;


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

    @BeforeClass
    void setup() throws Exception {
        executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());
        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();
    }

    @SneakyThrows
    private PulsarService createPulsarService() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setLoadBalancerEnabled(false);
        config.setLoadManagerClassName(MockLoadManager.class.getName());
        config.setClusterName("use");
        config.setWebServicePort(Optional.of(0));
        config.setMetadataStoreUrl("zk:127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config.setBrokerShutdownTimeoutMs(0L);
        config.setBrokerServicePort(Optional.of(0));
        config.setAdvertisedAddress("localhost");
        PulsarService pulsar = new PulsarService(config);
        pulsarServices.add(pulsar);
        return pulsar;
    }

    @AfterClass(alwaysRun = true)
    void shutdown() throws Exception {
        executor.shutdownNow();
        bkEnsemble.stop();
    }

    @AfterMethod(alwaysRun = true)
    void cleanUp() {
        pulsarServices.forEach(pulsarService -> {
            if (pulsarService.isRunning()) {
                pulsarService.shutdownNow();
            }
        });
        pulsarServices.clear();
    }

    @Test(timeOut = 30 * 1000)
    public void testRegisterAndLookupCache() throws Exception {
        PulsarService pulsar1 = createPulsarService();
        PulsarService pulsar2 = createPulsarService();
        PulsarService pulsar3 = createPulsarService();
        pulsar1.start();
        pulsar2.start();
        pulsar3.start();
        BrokerRegistryImpl brokerRegistry1 = new BrokerRegistryImpl(pulsar1);
        BrokerRegistryImpl brokerRegistry2 = new BrokerRegistryImpl(pulsar2);
        BrokerRegistryImpl brokerRegistry3 = new BrokerRegistryImpl(pulsar3);

        Set<String> address = new HashSet<>();
        brokerRegistry1.listen((lookupServiceAddress, type) -> {
            address.add(lookupServiceAddress);
        });

        brokerRegistry1.start();
        brokerRegistry2.start();

        assertEquals(brokerRegistry1.brokerLookupDataCache.size(), 2);
        assertEquals(brokerRegistry2.brokerLookupDataCache.size(), 2);
        assertEquals(address.size(), 2);

        assertEquals(brokerRegistry1.getAvailableBrokersAsync().get().size(), 2);
        assertEquals(brokerRegistry2.getAvailableBrokersAsync().get().size(), 2);

        // Check three broker cache are flush successes.
        brokerRegistry3.start();
        assertEquals(brokerRegistry3.brokerLookupDataCache.size(), 3);
        assertEquals(brokerRegistry3.getAvailableBrokersAsync().get().size(), 3);
        Awaitility.await().atMost(Duration.ofSeconds(5))
                        .untilAsserted(() -> {
                            assertEquals(brokerRegistry1.brokerLookupDataCache.size(), 3);
                            assertEquals(brokerRegistry2.brokerLookupDataCache.size(), 3);
                            assertEquals(address.size(), 3);
                        });

        assertEquals(address, brokerRegistry1.brokerLookupDataCache.keySet());
        assertEquals(address, brokerRegistry2.brokerLookupDataCache.keySet());
        assertEquals(address, brokerRegistry3.brokerLookupDataCache.keySet());

        Optional<BrokerLookupData> lookupDataOpt =
                brokerRegistry1.lookupAsync(brokerRegistry2.getLookupServiceAddress()).get();
        assertTrue(lookupDataOpt.isPresent());
        assertEquals(lookupDataOpt.get().brokerVersion(), pulsar2.getBrokerVersion());

        brokerRegistry1.unregister();
        assertEquals(brokerRegistry2.getAvailableBrokersAsync().get().size(), 2);

        brokerRegistry1.close();
        brokerRegistry2.close();
        brokerRegistry3.close();
    }

    @Test
    public void test() {

    }
}

