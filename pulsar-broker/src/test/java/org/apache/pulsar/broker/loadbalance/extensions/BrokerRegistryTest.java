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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
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
@Slf4j
public class BrokerRegistryTest {

    private final List<PulsarService> pulsarServices = new CopyOnWriteArrayList<>();
    private final List<BrokerRegistryImpl> brokerRegistries = new CopyOnWriteArrayList<>();

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
        PulsarService pulsar = spy(new PulsarService(config));
        pulsarServices.add(pulsar);
        return pulsar;
    }

    private BrokerRegistryImpl createBrokerRegistryImpl(PulsarService pulsar) {
        BrokerRegistryImpl brokerRegistry = spy(new BrokerRegistryImpl(pulsar));
        brokerRegistries.add(brokerRegistry);
        return brokerRegistry;
    }

    @AfterClass(alwaysRun = true)
    void shutdown() throws Exception {
        executor.shutdownNow();
        bkEnsemble.stop();
    }

    @AfterMethod(alwaysRun = true)
    void cleanUp() {
        log.info("Cleaning up the broker registry...");
        brokerRegistries.forEach(brokerRegistry -> {
            if (brokerRegistry.isStarted()) {
                try {
                    brokerRegistry.close();
                } catch (PulsarServerException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        brokerRegistries.clear();
        log.info("Cleaning up the pulsar services...");
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
        BrokerRegistryImpl brokerRegistry1 = createBrokerRegistryImpl(pulsar1);
        BrokerRegistryImpl brokerRegistry2 = createBrokerRegistryImpl(pulsar2);
        BrokerRegistryImpl brokerRegistry3 = createBrokerRegistryImpl(pulsar3);

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

        // Unregister and see the available brokers.
        brokerRegistry1.unregister();
        assertEquals(brokerRegistry2.getAvailableBrokersAsync().get().size(), 2);

    }

    @Test
    public void testRegisterFail() throws Exception {
        PulsarService pulsar1 = createPulsarService();
        PulsarService pulsar2 = createPulsarService();
        pulsar1.start();
        pulsar2.start();

        doReturn(pulsar1.getLookupServiceAddress()).when(pulsar2).getLookupServiceAddress();
        BrokerRegistryImpl brokerRegistry1 = createBrokerRegistryImpl(pulsar1);
        BrokerRegistryImpl brokerRegistry2 = createBrokerRegistryImpl(pulsar2);
        brokerRegistry1.start();
        try {
            brokerRegistry2.start();
            fail();
        } catch (Exception ex) {
            log.info("Broker registry start failed.", ex);
            assertTrue(ex instanceof PulsarServerException);
            assertTrue(ex.getMessage().contains("LockBusyException"));
        }
    }

    @Test
    public void testGetAvailableBrokersAsyncAndReadFromCache() throws Exception {
        PulsarService pulsar1 = createPulsarService();
        PulsarService pulsar2 = createPulsarService();
        pulsar1.start();
        pulsar2.start();

        CoordinationService coordinationService = spy(pulsar1.getCoordinationService());

        doReturn(coordinationService).when(pulsar1).getCoordinationService();

        LockManager<BrokerLookupData> lockManager = spy(coordinationService.getLockManager(BrokerLookupData.class));

        doReturn(lockManager).when(coordinationService).getLockManager(BrokerLookupData.class);

        BrokerRegistryImpl brokerRegistry1 = createBrokerRegistryImpl(pulsar1);
        Set<String> address = new HashSet<>();
        brokerRegistry1.listen((lookupServiceAddress, type) -> {
            address.add(lookupServiceAddress);
        });
        BrokerRegistryImpl brokerRegistry2 = createBrokerRegistryImpl(pulsar2);
        brokerRegistry1.start();
        List<String> availableBrokers1 = brokerRegistry1.getAvailableBrokersAsync().get();
        assertEquals(availableBrokers1.size(), 1);

        CompletableFuture<List<String>> future = new CompletableFuture<>();
        future.completeExceptionally(new MetadataStoreException("Mock exception"));
        doReturn(future).when(lockManager).listLocks(anyString());

        brokerRegistry2.start();

        Awaitility.await().untilAsserted(() -> {
            // The lock manager list locks will get exception, it will read the list from cache.
            List<String> availableBrokers2 = brokerRegistry1.getAvailableBrokersAsync().get();
            assertEquals(address.size(), 2);
            assertEquals(availableBrokers2.size(), 2);
        });
    }

    @Test
    public void testGetAvailableBrokersAsyncAndReloadCache() throws Exception {
        PulsarService pulsar1 = createPulsarService();
        PulsarService pulsar2 = createPulsarService();
        pulsar1.start();
        pulsar2.start();

        ScheduledExecutorService spyExecutor = spy(pulsar1.getLoadManagerExecutor());

        doReturn(spyExecutor).when(pulsar1).getLoadManagerExecutor();
        MetadataStoreExtended localMetadataStore = spy(pulsar1.getLocalMetadataStore());
        doReturn(localMetadataStore).when(pulsar1).getLocalMetadataStore();

        // Disable the register listener method, so the cache will not update.
        doNothing().when(localMetadataStore).registerListener(any());

        BrokerRegistryImpl brokerRegistry1 = createBrokerRegistryImpl(pulsar1);
        brokerRegistry1.start();

        assertEquals(brokerRegistry1.brokerLookupDataCache.size(), 1);

        BrokerRegistryImpl brokerRegistry2 = createBrokerRegistryImpl(pulsar2);
        brokerRegistry2.start();
        assertEquals(brokerRegistry2.brokerLookupDataCache.size(), 2);
        assertEquals(brokerRegistry1.brokerLookupDataCache.size(), 1);

        // Sleep and check the cache not update.
        TimeUnit.SECONDS.sleep(1);
        assertEquals(brokerRegistry1.brokerLookupDataCache.size(), 1);

        // Verify the cache reload only called in broker registry start method.
        verify(brokerRegistry1, times(1)).reloadAllBrokerLookupCacheAsync();

        // Trigger cache update.
        List<String> availableBrokers = brokerRegistry1.getAvailableBrokersAsync().get();
        assertEquals(availableBrokers.size(), 2);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(brokerRegistry1.brokerLookupDataCache.size(), 2);
        });

        // Verify the cache reloaded.
        verify(brokerRegistry1, times(2)).reloadAllBrokerLookupCacheAsync();
    }

}

