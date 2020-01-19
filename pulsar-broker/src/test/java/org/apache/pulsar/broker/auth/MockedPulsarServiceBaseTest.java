/**
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
package org.apache.pulsar.broker.auth;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.NoOpShutdownService;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all tests that need a Pulsar instance without a ZK and BK cluster
 */
public abstract class MockedPulsarServiceBaseTest {

    protected ServiceConfiguration conf;
    protected PulsarService pulsar;
    protected PulsarAdmin admin;
    protected PulsarClient pulsarClient;
    protected URL brokerUrl;
    protected URL brokerUrlTls;

    protected URI lookupUrl;

    protected MockZooKeeper mockZooKeeper;
    protected NonClosableMockBookKeeper mockBookKeeper;
    protected boolean isTcpLookup = false;
    protected final String configClusterName = "test";

    private SameThreadOrderedSafeExecutor sameThreadOrderedSafeExecutor;
    private ExecutorService bkExecutor;

    public MockedPulsarServiceBaseTest() {
        resetConfig();
    }

    protected void resetConfig() {
        this.conf = new ServiceConfiguration();
        this.conf.setAdvertisedAddress("localhost");
        this.conf.setClusterName(configClusterName);
        this.conf.setAdvertisedAddress("localhost"); // there are TLS tests in here, they need to use localhost because of the certificate
        this.conf.setManagedLedgerCacheSizeMB(8);
        this.conf.setActiveConsumerFailoverDelayTimeMillis(0);
        this.conf.setDefaultNumberOfNamespaceBundles(1);
        this.conf.setZookeeperServers("localhost:2181");
        this.conf.setConfigurationStoreServers("localhost:3181");
        this.conf.setAllowAutoTopicCreationType("non-partitioned");
        this.conf.setBrokerServicePort(Optional.of(0));
        this.conf.setBrokerServicePortTls(Optional.of(0));
        this.conf.setWebServicePort(Optional.of(0));
        this.conf.setWebServicePortTls(Optional.of(0));
    }

    protected final void internalSetup() throws Exception {
        init();
        lookupUrl = new URI(brokerUrl.toString());
        if (isTcpLookup) {
            lookupUrl = new URI(pulsar.getBrokerServiceUrl());
        }
        pulsarClient = newPulsarClient(lookupUrl.toString(), 0);
    }

    protected PulsarClient newPulsarClient(String url, int intervalInSecs) throws PulsarClientException {
        return PulsarClient.builder().serviceUrl(url).statsInterval(intervalInSecs, TimeUnit.SECONDS).build();
    }

    protected final void internalSetupForStatsTest() throws Exception {
        init();
        String lookupUrl = brokerUrl.toString();
        if (isTcpLookup) {
            lookupUrl = new URI(pulsar.getBrokerServiceUrl()).toString();
        }
        pulsarClient = newPulsarClient(lookupUrl, 1);
    }

    protected final void init() throws Exception {
        this.conf.setBrokerServicePort(Optional.of(0));
        this.conf.setBrokerServicePortTls(Optional.of(0));
        this.conf.setAdvertisedAddress("localhost");
        this.conf.setWebServicePort(Optional.of(0));
        this.conf.setWebServicePortTls(Optional.of(0));

        sameThreadOrderedSafeExecutor = new SameThreadOrderedSafeExecutor();
        bkExecutor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("mock-pulsar-bk")
                .setUncaughtExceptionHandler((thread, ex) -> log.info("Uncaught exception", ex))
                .build());

        mockZooKeeper = createMockZooKeeper();
        mockBookKeeper = createMockBookKeeper(mockZooKeeper, bkExecutor);

        startBroker();
    }

    protected final void internalCleanup() throws Exception {
        try {
            // if init fails, some of these could be null, and if so would throw
            // an NPE in shutdown, obscuring the real error
            if (admin != null) {
                admin.close();
                admin = null;
            }
            if (pulsarClient != null) {
                pulsarClient.close();
                pulsarClient = null;
            }
            if (pulsar != null) {
                pulsar.close();
            }
            if (mockBookKeeper != null) {
                mockBookKeeper.reallyShutdown();
            }
            if (mockZooKeeper != null) {
                mockZooKeeper.shutdown();
            }
            if (sameThreadOrderedSafeExecutor != null) {
                sameThreadOrderedSafeExecutor.shutdown();
            }
            if (bkExecutor != null) {
                bkExecutor.shutdown();
            }
        } catch (Exception e) {
            log.warn("Failed to clean up mocked pulsar service:", e);
        }
    }

    protected abstract void setup() throws Exception;

    protected abstract void cleanup() throws Exception;

    protected void restartBroker() throws Exception {
        stopBroker();
        startBroker();
    }

    protected void stopBroker() throws Exception {
        pulsar.close();
        // Simulate cleanup of ephemeral nodes
        //mockZooKeeper.delete("/loadbalance/brokers/localhost:" + pulsar.getConfiguration().getWebServicePort(), -1);
    }

    protected void startBroker() throws Exception {
        this.pulsar = startBroker(conf);

        brokerUrl = new URL(pulsar.getWebServiceAddress());
        brokerUrlTls = new URL(pulsar.getWebServiceAddressTls());

        if (admin != null) {
            admin.close();
        }
        admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString()).build());
    }

    protected PulsarService startBroker(ServiceConfiguration conf) throws Exception {
        PulsarService pulsar = spy(new PulsarService(conf));
        pulsar.setShutdownService(new NoOpShutdownService());

        setupBrokerMocks(pulsar);
        boolean isAuthorizationEnabled = conf.isAuthorizationEnabled();
        // enable authorization to initialize authorization service which is used by grant-permission
        conf.setAuthorizationEnabled(true);
        pulsar.start();
        conf.setAuthorizationEnabled(isAuthorizationEnabled);

        Compactor spiedCompactor = spy(pulsar.getCompactor());
        doReturn(spiedCompactor).when(pulsar).getCompactor();

        return pulsar;
    }

    protected void setupBrokerMocks(PulsarService pulsar) throws Exception {
        // Override default providers with mocked ones
        doReturn(mockZooKeeperClientFactory).when(pulsar).getZooKeeperClientFactory();
        doReturn(mockBookKeeperClientFactory).when(pulsar).newBookKeeperClientFactory();

        Supplier<NamespaceService> namespaceServiceSupplier = () -> spy(new NamespaceService(pulsar));
        doReturn(namespaceServiceSupplier).when(pulsar).getNamespaceServiceProvider();

        doReturn(sameThreadOrderedSafeExecutor).when(pulsar).getOrderedExecutor();
    }

    public TenantInfo createDefaultTenantInfo() throws PulsarAdminException {
        // create local cluster if not exist
        if (!admin.clusters().getClusters().contains(configClusterName)) {
            admin.clusters().createCluster(configClusterName, new ClusterData());
        }
        Set<String> allowedClusters = Sets.newHashSet();
        allowedClusters.add(configClusterName);
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet(), allowedClusters);
        return tenantInfo;
    }

    public static MockZooKeeper createMockZooKeeper() throws Exception {
        MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
        List<ACL> dummyAclList = new ArrayList<>(0);

        ZkUtils.createFullPathOptimistic(zk, "/ledgers/available/192.168.1.1:" + 5000,
                "".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), dummyAclList, CreateMode.PERSISTENT);

        zk.create("/ledgers/LAYOUT", "1\nflat:1".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), dummyAclList,
                CreateMode.PERSISTENT);
        return zk;
    }

    public static NonClosableMockBookKeeper createMockBookKeeper(ZooKeeper zookeeper,
                                                                 ExecutorService executor) throws Exception {
        return spy(new NonClosableMockBookKeeper(zookeeper, executor));
    }

    // Prevent the MockBookKeeper instance from being closed when the broker is restarted within a test
    public static class NonClosableMockBookKeeper extends PulsarMockBookKeeper {

        public NonClosableMockBookKeeper(ZooKeeper zk, ExecutorService executor) throws Exception {
            super(zk, executor);
        }

        @Override
        public void close() {
            // no-op
        }

        @Override
        public void shutdown() {
            // no-op
        }

        public void reallyShutdown() {
            super.shutdown();
        }
    }

    protected ZooKeeperClientFactory mockZooKeeperClientFactory = new ZooKeeperClientFactory() {

        @Override
        public CompletableFuture<ZooKeeper> create(String serverList, SessionType sessionType,
                int zkSessionTimeoutMillis) {
            // Always return the same instance (so that we don't loose the mock ZK content on broker restart
            return CompletableFuture.completedFuture(mockZooKeeper);
        }
    };

    private BookKeeperClientFactory mockBookKeeperClientFactory = new BookKeeperClientFactory() {

        @Override
        public BookKeeper create(ServiceConfiguration conf, ZooKeeper zkClient,
                Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                Map<String, Object> properties) {
            // Always return the same instance (so that we don't loose the mock BK content on broker restart
            return mockBookKeeper;
        }

        @Override
        public void close() {
            // no-op
        }
    };

    public static boolean retryStrategically(Predicate<Void> predicate, int retryCount, long intSleepTimeInMillis)
            throws Exception {
        for (int i = 0; i < retryCount; i++) {
            if (predicate.test(null) || i == (retryCount - 1)) {
                return true;
            }
            Thread.sleep(intSleepTimeInMillis + (intSleepTimeInMillis * i));
        }
        return false;
    }

    public static void setFieldValue(Class<?> clazz, Object classObj, String fieldName, Object fieldValue) throws Exception {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(classObj, fieldValue);
    }

    private static final Logger log = LoggerFactory.getLogger(MockedPulsarServiceBaseTest.class);
}
