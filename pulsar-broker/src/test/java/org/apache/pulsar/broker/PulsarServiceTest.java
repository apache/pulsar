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
package org.apache.pulsar.broker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertSame;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.logging.log4j.Level;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.namespace.OwnershipCache;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.metadata.api.MetadataCacheConfig;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore.OperationType;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.utils.TestLogAppender;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs.Ids;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
public class PulsarServiceTest extends MockedPulsarServiceBaseTest {

    private boolean useStaticPorts = false;

    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        useStaticPorts = false;
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTopicNameCacheMaxCapacity(5000);
        conf.setMaxSecondsToClearTopicNameCache(5);
        if (useStaticPorts) {
            conf.setBrokerServicePortTls(Optional.of(6651));
            conf.setBrokerServicePort(Optional.of(6660));
            conf.setWebServicePort(Optional.of(8081));
            conf.setWebServicePortTls(Optional.of(8082));
        }
        conf.setTlsTrustCertsFilePath(CA_CERT_FILE_PATH);
        conf.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
    }

    @Test
    public void testGetWorkerService() throws Exception {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setMetadataStoreUrl("zk:localhost");
        configuration.setClusterName("clusterName");
        configuration.setFunctionsWorkerEnabled(true);
        configuration.setBrokerShutdownTimeoutMs(0L);
        configuration.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        WorkerService expectedWorkerService = mock(WorkerService.class);
        @Cleanup
        PulsarService pulsarService = spy(new PulsarService(configuration, new WorkerConfig(),
                Optional.of(expectedWorkerService), (exitCode) -> {}));

        WorkerService actualWorkerService = pulsarService.getWorkerService();
        assertSame(expectedWorkerService, actualWorkerService);
    }

    /**
     * Verifies that the getWorkerService throws {@link UnsupportedOperationException}
     * when functionsWorkerEnabled is set to false .
     */
    @Test
    public void testGetWorkerServiceException() throws Exception {
        conf.setFunctionsWorkerEnabled(false);
        setup();

        String errorMessage = "Pulsar Function Worker is not enabled, probably functionsWorkerEnabled is set to false";

        int thrownCnt = 0;
        try {
            pulsar.getWorkerService();
        } catch (UnsupportedOperationException e) {
            thrownCnt++;
            assertEquals(e.getMessage(), errorMessage);
        }

        try {
            admin.sources().listSources("my", "test");
        } catch (PulsarAdminException e) {
            thrownCnt++;
            assertEquals(e.getStatusCode(), 409);
            assertEquals(e.getMessage(), errorMessage);
        }

        try {
            admin.sinks().getSinkStatus("my", "test", "test");
        } catch (PulsarAdminException e) {
            thrownCnt++;
            assertEquals(e.getStatusCode(), 409);
            assertEquals(e.getMessage(), errorMessage);
        }

        try {
            admin.functions().getFunction("my", "test", "test");
        } catch (PulsarAdminException e) {
            thrownCnt++;
            assertEquals(e.getStatusCode(), 409);
            assertEquals(e.getMessage(), errorMessage);
        }

        try {
            admin.worker().getClusterLeader();
        } catch (PulsarAdminException e) {
            thrownCnt++;
            assertEquals(e.getStatusCode(), 409);
            assertEquals(e.getMessage(), errorMessage);
        }

        try {
            admin.worker().getFunctionsStats();
        } catch (PulsarAdminException e) {
            thrownCnt++;
            assertEquals(e.getStatusCode(), 409);
            assertEquals(e.getMessage(), errorMessage);
        }

        assertEquals(thrownCnt, 6);
    }

    @Test
    public void testAdvertisedAddress() throws Exception {
        cleanup();
        useStaticPorts = true;
        setup();
        assertEquals(pulsar.getAdvertisedAddress(), "localhost");
        assertEquals(pulsar.getBrokerServiceUrlTls(), "pulsar+ssl://localhost:6651");
        assertEquals(pulsar.getBrokerServiceUrl(), "pulsar://localhost:6660");
        assertEquals(pulsar.getWebServiceAddress(), "http://localhost:8081");
        assertEquals(pulsar.getWebServiceAddressTls(), "https://localhost:8082");
        assertEquals(conf, pulsar.getConfiguration());
    }

    @Test
    public void testAdvertisedListeners() throws Exception {
        cleanup();
        // don't use dynamic ports when using advertised listeners (#12079)
        useStaticPorts = true;
        conf.setAdvertisedListeners("internal:pulsar://gateway:6650, internal:pulsar+ssl://gateway:6651");
        conf.setInternalListenerName("internal");
        setup();
        assertEquals(pulsar.getAdvertisedAddress(), "localhost");
        assertEquals(pulsar.getBrokerServiceUrlTls(), "pulsar+ssl://gateway:6651");
        assertEquals(pulsar.getBrokerServiceUrl(), "pulsar://gateway:6650");
        assertEquals(pulsar.getWebServiceAddress(), "http://localhost:8081");
        assertEquals(pulsar.getWebServiceAddressTls(), "https://localhost:8082");
        assertEquals(conf, pulsar.getConfiguration());
    }

    @Test
    public void testDynamicBrokerPort() throws Exception {
        cleanup();
        useStaticPorts = false;
        setup();
        assertEquals(pulsar.getAdvertisedAddress(), "localhost");
        assertEquals(conf, pulsar.getConfiguration());
        assertEquals(conf.getBrokerServicePortTls(), pulsar.getBrokerListenPortTls());
        assertEquals(conf.getBrokerServicePort(), pulsar.getBrokerListenPort());
        assertEquals(pulsar.getBrokerServiceUrlTls(),
                "pulsar+ssl://localhost:" + pulsar.getBrokerListenPortTls().get());
        assertEquals(pulsar.getBrokerServiceUrl(),
                "pulsar://localhost:" + pulsar.getBrokerListenPort().get());
        assertEquals(pulsar.getWebServiceAddress(),
                "http://localhost:" + pulsar.getWebService().getListenPortHTTP().get());
        assertEquals(pulsar.getWebServiceAddressTls(),
                "https://localhost:" + pulsar.getWebService().getListenPortHTTPS().get());
    }

    @Test
    public void testTopicCacheConfiguration() throws Exception {
        cleanup();
        setup();
        assertEquals(conf.getTopicNameCacheMaxCapacity(), 5000);
        assertEquals(conf.getMaxSecondsToClearTopicNameCache(), 5);

        List<TopicName> topicNameCached = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            topicNameCached.add(TopicName.get("public/default/tp_" + i));
        }

        // Verify: the cache does not clear since it is not larger than max capacity.
        Thread.sleep(10 * 1000);
        for (int i = 0; i < 20; i++) {
            assertTrue(topicNameCached.get(i) == TopicName.get("public/default/tp_" + i));
        }

        // Update max capacity.
        admin.brokers().updateDynamicConfiguration("topicNameCacheMaxCapacity", "10");

        // Verify: the cache were cleared.
        Thread.sleep(10 * 1000);
        for (int i = 0; i < 20; i++) {
            assertFalse(topicNameCached.get(i) == TopicName.get("public/default/tp_" + i));
        }
    }

    @Test
    public void testBacklogAndRetentionCheck() throws PulsarServerException {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setClusterName("test");
        config.setMetadataStoreUrl("memory:local");
        config.setMetadataStoreConfigPath("memory:local");
        PulsarService pulsarService = new PulsarService(config);

        // Check the default configuration
        try {
            pulsarService.start();
        } catch (Exception e) {
            assertFalse(e.getCause() instanceof IllegalArgumentException);
        } finally {
            pulsarService.close();
        }

        // Only set retention
        config.setDefaultRetentionSizeInMB(5);
        config.setDefaultRetentionTimeInMinutes(5);

        pulsarService = new PulsarService(config);

        try {
            pulsarService.start();
        } catch (Exception e) {
            assertFalse(e.getCause() instanceof IllegalArgumentException);
        } finally {
            pulsarService.close();
        }

        // Set both retention and backlog quota
        config.setBacklogQuotaDefaultLimitBytes(4 * 1024 * 1024);
        config.setBacklogQuotaDefaultLimitSecond(4 * 60);

        pulsarService = new PulsarService(config);

        try {
            pulsarService.start();
        } catch (Exception e) {
            assertFalse(e.getCause() instanceof IllegalArgumentException);
        } finally {
            pulsarService.close();
        }

        // Set invalidated retention and backlog quota
        config.setBacklogQuotaDefaultLimitBytes(6 * 1024 * 1024);

        pulsarService = new PulsarService(config);

        try {
            pulsarService.start();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        } finally {
            pulsarService.close();
        }

        config.setBacklogQuotaDefaultLimitBytes(4 * 1024 * 1024);
        config.setBacklogQuotaDefaultLimitSecond(6 * 60);

        pulsarService = new PulsarService(config);

        try {
            pulsarService.start();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        } finally {
            pulsarService.close();
        }

        // Only set backlog quota
        config.setDefaultRetentionSizeInMB(0);
        config.setDefaultRetentionTimeInMinutes(0);

        pulsarService = new PulsarService(config);

        try {
            pulsarService.start();
        } catch (Exception e) {
            assertFalse(e.getCause() instanceof IllegalArgumentException);
        } finally {
            pulsarService.close();
        }
    }

    @Test
    public void testShutdownViaAdminApi() throws Exception {
        super.internalSetup();
        super.setupDefaultTenantAndNamespace();
        String topic = "persistent://public/default/testShutdownViaAdminApi";
        admin.topics().createNonPartitionedTopic(topic);
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .sendTimeout(5, TimeUnit.SECONDS)
                .create();
        producer.send("message 1".getBytes());
        admin.brokers()
                .shutDownBrokerGracefully(0, false)
                .get(30, TimeUnit.SECONDS);
        try {
            producer.send("message 2".getBytes());
            fail("sending msg should timeout, because broker is down and there is only one broker");
        } catch (Exception e) {
            assertTrue(e instanceof PulsarClientException.TimeoutException);
        }
    }

    @DataProvider
    public Object[][] ownershipLockExpiryScenarios() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "ownershipLockExpiryScenarios")
    public void testOwnershipLockExpiryLogging(boolean shuttingDown) throws Exception {
        @Cleanup
        ZookeeperServerTest zk = new ZookeeperServerTest(0);
        zk.start();
        @Cleanup
        ZKMetadataStore store = new ZKMetadataStore(zk.getHostPort(),
                MetadataStoreConfig.builder().build(), true);
        FaultInjectionMetadataStore faultStore = new FaultInjectionMetadataStore(store);
        @Cleanup
        PulsarTestContext context = PulsarTestContext.builder()
                .localMetadataStore(faultStore)
                .configurationMetadataStore(store)
                .build();
        PulsarService broker = context.getPulsarService();
        OwnershipCache ownershipCache = broker.getNamespaceService().getOwnershipCache();
        var bundle = broker.getNamespaceService().getNamespaceBundleFactory()
                .getFullBundle(NamespaceName.get("public/shutdown-ownership"));
        ownershipCache.tryAcquiringOwnership(bundle).get(10, TimeUnit.SECONDS);
        var lock = ownershipCache.getLocallyAcquiredLocks().get(bundle);
        assertThat(lock).isNotNull();

        @Cleanup
        TestLogAppender logAppender = TestLogAppender.create(OwnershipCache.class);

        if (shuttingDown) {
            // Failing broker deregistration skips graceful bundle unloading. The real coordination-service
            // shutdown must then release the still-registered ownership lock directly through LockManager.
            AtomicBoolean failureInjected = new AtomicBoolean();
            String brokerPath = LoadManager.LOADBALANCE_BROKERS_ROOT + "/" + broker.getBrokerId();
            faultStore.failConditional(new MetadataStoreException("Broker deregistration failed"),
                    (operation, path) -> {
                        if (operation == OperationType.DELETE && path.equals(brokerPath)) {
                            failureInjected.set(true);
                            return true;
                        }
                        return false;
                    });
            broker.closeAsync().get(30, TimeUnit.SECONDS);

            assertThat(failureInjected).isTrue();
            assertThat(broker.isRunning()).isFalse();
            assertThat(store.get(lock.getPath()).get(10, TimeUnit.SECONDS)).isEmpty();
        } else {
            // Replace the ephemeral lock atomically so revalidation sees a persistent node and expires
            // the lock. This exercises the real metadata notification and revalidation path, not release().
            byte[] data = store.get(lock.getPath()).get(10, TimeUnit.SECONDS).orElseThrow().getValue();
            store.getZkClient().multi(List.of(Op.delete(lock.getPath(), -1),
                    Op.create(lock.getPath(), data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)));
            lock.getLockExpiredFuture().get(10, TimeUnit.SECONDS);
            Awaitility.await().untilAsserted(() -> assertThat(ownershipCache.getOwnedBundle(bundle)).isNull());
            assertThat(broker.isRunning()).isTrue();
        }
        assertThat(lock.getLockExpiredFuture()).isDone().isNotCompletedExceptionally();
        assertThat(ownershipCache.getLocallyAcquiredLocks()).doesNotContainKey(bundle);
        assertThat(ownershipCache.getOwnedBundle(bundle)).isNull();
        boolean expiryLogged = logAppender.getEvents().stream().anyMatch(event -> event.getLevel() == Level.INFO
                && event.getLoggerName().equals(OwnershipCache.class.getName())
                && event.getMessage().getFormattedMessage().contains("Resource lock has expired"));
        assertThat(expiryLogged).as("INFO expiry while shuttingDown=%s", shuttingDown).isEqualTo(!shuttingDown);
    }

    @Test
    public void testMetadataSerDesThreads() throws Exception {
        final var numSerDesThreads = 5;
        final var config = new ServiceConfiguration();
        config.setMetadataStoreSerDesThreads(numSerDesThreads);
        config.setClusterName("test");
        config.setMetadataStoreUrl("memory:local");
        config.setConfigurationMetadataStoreUrl("memory:local");

        @Cleanup final var pulsar = new PulsarService(config);
        pulsar.start();

        BiConsumer<MetadataStore, String> verifier = (store, prefix) -> {
            final var serDes = new CustomMetadataSerDes();
            final var cache = store.getMetadataCache(prefix, serDes, MetadataCacheConfig.builder().build());
            for (int i = 0; i < 100 && serDes.threadNameToSerializedPaths.size() < numSerDesThreads; i++) {
                cache.create(prefix + i, "value-" + i).join();
                final var value = cache.get(prefix + i).join();
                assertEquals(value.orElseThrow(), "value-" + i);
                final var newValue = cache.readModifyUpdate(prefix + i, s -> s + "-updated").join();
                assertEquals(newValue, "value-" + i + "-updated");
                // Verify the serialization and deserialization are handled by the same thread
                assertEquals(serDes.threadNameToSerializedPaths, serDes.threadNameToDeserializedPaths);
            }
            log.info().attr("threadMapping", serDes.threadNameToSerializedPaths)
                    .log("SerDes thread mapping");
            assertEquals(serDes.threadNameToSerializedPaths.keySet().size(), numSerDesThreads);
            // Verify a path cannot be handled by multiple threads
            final var paths = serDes.threadNameToSerializedPaths.values().stream()
                .flatMap(Set::stream).sorted().toList();
            assertEquals(paths.stream().distinct().toList(), paths);
        };

        verifier.accept(pulsar.getLocalMetadataStore(), "/test-local/");
        verifier.accept(pulsar.getConfigurationMetadataStore(), "/test-config/");
    }

    private static class CustomMetadataSerDes implements MetadataSerde<String> {

        final Map<String, Set<String>> threadNameToSerializedPaths = new ConcurrentHashMap<>();
        final Map<String, Set<String>> threadNameToDeserializedPaths = new ConcurrentHashMap<>();

        @Override
        public byte[] serialize(String path, String value) throws IOException{
            threadNameToSerializedPaths.computeIfAbsent(Thread.currentThread().getName(),
                    __ -> ConcurrentHashMap.newKeySet()).add(path);
            return value.getBytes();
        }

        @Override
        public String deserialize(String path, byte[] data, Stat stat) throws IOException {
            threadNameToDeserializedPaths.computeIfAbsent(Thread.currentThread().getName(),
                    __ -> ConcurrentHashMap.newKeySet()).add(path);
            return new String(data);
        }
    }
}
