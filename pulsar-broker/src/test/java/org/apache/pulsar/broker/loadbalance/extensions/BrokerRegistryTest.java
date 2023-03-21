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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
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
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.awaitility.Awaitility;
import org.testcontainers.shaded.org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for {@link BrokerRegistry}.
 */
@Slf4j
@Test(groups = "broker")
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
        public String setNamespaceBundleAffinity(String bundle, String broker) {
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
            try {
                pulsarService.close();
            } catch (PulsarServerException e) {
                throw new RuntimeException(e);
            }
        });
        pulsarServices.clear();
    }

    @Test(timeOut = 30 * 1000)
    public void testRegisterAndLookup() throws Exception {
        PulsarService pulsar1 = createPulsarService();
        PulsarService pulsar2 = createPulsarService();
        PulsarService pulsar3 = createPulsarService();
        pulsar1.start();
        pulsar2.start();
        pulsar3.start();
        BrokerRegistryImpl brokerRegistry1 = createBrokerRegistryImpl(pulsar1);
        BrokerRegistryImpl brokerRegistry2 = createBrokerRegistryImpl(pulsar2);
        BrokerRegistryImpl brokerRegistry3 = createBrokerRegistryImpl(pulsar3);

        Set<String> brokerIds = new HashSet<>();
        brokerRegistry1.addListener((brokerId, type) -> {
            brokerIds.add(brokerId);
        });

        brokerRegistry1.start();
        brokerRegistry2.start();

        Awaitility.await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertEquals(brokerIds.size(), 2));

        assertEquals(brokerRegistry1.getAvailableBrokersAsync().get().size(), 2);
        assertEquals(brokerRegistry2.getAvailableBrokersAsync().get().size(), 2);

        // Check three broker cache are flush successes.
        brokerRegistry3.start();
        assertEquals(brokerRegistry3.getAvailableBrokersAsync().get().size(), 3);
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertEquals(brokerIds.size(), 3));

        assertEquals(brokerIds, new HashSet<>(brokerRegistry1.getAvailableBrokersAsync().get()));
        assertEquals(brokerIds, new HashSet<>(brokerRegistry2.getAvailableBrokersAsync().get()));
        assertEquals(brokerIds, new HashSet<>(brokerRegistry3.getAvailableBrokersAsync().get()));
        assertEquals(brokerIds, brokerRegistry1.getAvailableBrokerLookupDataAsync().get().keySet());
        assertEquals(brokerIds, brokerRegistry2.getAvailableBrokerLookupDataAsync().get().keySet());
        assertEquals(brokerIds, brokerRegistry3.getAvailableBrokerLookupDataAsync().get().keySet());

        Optional<BrokerLookupData> lookupDataOpt =
                brokerRegistry1.lookupAsync(brokerRegistry2.getBrokerId()).get();
        assertTrue(lookupDataOpt.isPresent());
        assertEquals(lookupDataOpt.get().getWebServiceUrl(), pulsar2.getSafeWebServiceAddress());
        assertEquals(lookupDataOpt.get().getWebServiceUrlTls(), pulsar2.getWebServiceAddressTls());
        assertEquals(lookupDataOpt.get().getPulsarServiceUrl(), pulsar2.getBrokerServiceUrl());
        assertEquals(lookupDataOpt.get().getPulsarServiceUrlTls(), pulsar2.getBrokerServiceUrlTls());
        assertEquals(lookupDataOpt.get().advertisedListeners(), pulsar2.getAdvertisedListeners());
        assertEquals(lookupDataOpt.get().protocols(), pulsar2.getProtocolDataToAdvertise());
        assertEquals(lookupDataOpt.get().persistentTopicsEnabled(), pulsar2.getConfiguration()
                .isEnablePersistentTopics());
        assertEquals(lookupDataOpt.get().nonPersistentTopicsEnabled(), pulsar2.getConfiguration()
                .isEnableNonPersistentTopics());
        assertEquals(lookupDataOpt.get().brokerVersion(), pulsar2.getBrokerVersion());

        // Unregister and see the available brokers.
        brokerRegistry1.unregister();

        // After unregistering, the other broker registry lock manager's metadata cache might not be updated yet,
        // so we need to wait for it to synchronize.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(brokerRegistry2.getAvailableBrokersAsync().get().size(), 2);
        });
    }

    @Test
    public void testRegisterFailWithSameBrokerId() throws Exception {
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
    public void testCloseRegister() throws Exception {
        PulsarService pulsar1 = createPulsarService();
        pulsar1.start();
        BrokerRegistryImpl brokerRegistry = createBrokerRegistryImpl(pulsar1);
        assertEquals(getState(brokerRegistry), BrokerRegistryImpl.State.Init);

        // Check state after stated.
        brokerRegistry.start();
        assertEquals(getState(brokerRegistry), BrokerRegistryImpl.State.Registered);

        // Add a listener
        brokerRegistry.addListener((brokerId, type) -> {
            // Ignore.
        });
        assertTrue(brokerRegistry.isStarted());
        List<BiConsumer<String, NotificationType>> listeners =
                WhiteboxImpl.getInternalState(brokerRegistry, "listeners");
        assertFalse(listeners.isEmpty());

        // Check state after unregister.
        brokerRegistry.unregister();
        assertEquals(getState(brokerRegistry), BrokerRegistryImpl.State.Started);

        // Check state after re-register.
        brokerRegistry.register();
        assertEquals(getState(brokerRegistry), BrokerRegistryImpl.State.Registered);

        // Check state after close.
        brokerRegistry.close();
        assertFalse(brokerRegistry.isStarted());
        assertEquals(getState(brokerRegistry), BrokerRegistryImpl.State.Closed);
        listeners = WhiteboxImpl.getInternalState(brokerRegistry, "listeners");
        assertTrue(listeners.isEmpty());

        try {
            brokerRegistry.getAvailableBrokersAsync().get();
            fail();
        } catch (Exception ex) {
            log.info("Failed to getAvailableBrokersAsync.", ex);
            assertTrue(FutureUtil.unwrapCompletionException(ex) instanceof IllegalStateException);
        }

        try {
            brokerRegistry.getAvailableBrokerLookupDataAsync().get();
            fail();
        } catch (Exception ex) {
            log.info("Failed to getAvailableBrokerLookupDataAsync.", ex);
            assertTrue(FutureUtil.unwrapCompletionException(ex) instanceof IllegalStateException);
        }

        try {
            brokerRegistry.lookupAsync("test").get();
            fail();
        } catch (Exception ex) {
            log.info("Failed to lookupAsync.", ex);
            assertTrue(FutureUtil.unwrapCompletionException(ex) instanceof IllegalStateException);
        }

        try {
            brokerRegistry.addListener((brokerId, type) -> {
                // Ignore.
            });
            fail();
        } catch (Exception ex) {
            log.info("Failed to lookupAsync.", ex);
            assertTrue(FutureUtil.unwrapCompletionException(ex) instanceof IllegalStateException);
        }
    }

    @Test
    public void testIsVerifiedNotification() {
        assertFalse(BrokerRegistryImpl.isVerifiedNotification(new Notification(NotificationType.Created, "/")));
        assertFalse(BrokerRegistryImpl.isVerifiedNotification(new Notification(NotificationType.Created,
                BrokerRegistryImpl.LOOKUP_DATA_PATH + "xyz")));
        assertFalse(BrokerRegistryImpl.isVerifiedNotification(new Notification(NotificationType.Created,
                BrokerRegistryImpl.LOOKUP_DATA_PATH)));
        assertTrue(BrokerRegistryImpl.isVerifiedNotification(
                new Notification(NotificationType.Created, BrokerRegistryImpl.LOOKUP_DATA_PATH + "/brokerId")));
        assertTrue(BrokerRegistryImpl.isVerifiedNotification(
                new Notification(NotificationType.Created, BrokerRegistryImpl.LOOKUP_DATA_PATH + "/brokerId/xyz")));
    }

    @Test
    public void testKeyPath() {
        String keyPath = BrokerRegistryImpl.keyPath("brokerId");
        assertEquals(keyPath, BrokerRegistryImpl.LOOKUP_DATA_PATH + "/brokerId");
    }

    public BrokerRegistryImpl.State getState(BrokerRegistryImpl brokerRegistry) {
        return WhiteboxImpl.getInternalState(brokerRegistry, BrokerRegistryImpl.State.class);
    }
}

