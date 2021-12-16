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

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.channel.EventLoopGroup;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.intercept.CounterBrokerInterceptor;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all tests that need a Pulsar instance without a ZK and BK cluster.
 */
@PowerMockIgnore(value = {"org.slf4j.*", "com.sun.org.apache.xerces.*" })
public abstract class MockedPulsarServiceBaseTest extends TestRetrySupport {

    protected final String DUMMY_VALUE = "DUMMY_VALUE";
    protected final String GLOBAL_DUMMY_VALUE = "GLOBAL_DUMMY_VALUE";

    protected ServiceConfiguration conf;
    protected PulsarService pulsar;
    protected PulsarAdmin admin;
    protected PulsarClient pulsarClient;
    protected PortForwarder brokerGateway;
    protected boolean enableBrokerGateway =  false;
    protected URL brokerUrl;
    protected URL brokerUrlTls;

    protected URI lookupUrl;

    protected MockZooKeeper mockZooKeeper;
    protected MockZooKeeper mockZooKeeperGlobal;
    protected NonClosableMockBookKeeper mockBookKeeper;
    protected boolean isTcpLookup = false;
    protected static final String configClusterName = "test";

    private SameThreadOrderedSafeExecutor sameThreadOrderedSafeExecutor;
    private OrderedExecutor bkExecutor;

    protected boolean enableBrokerInterceptor = false;

    public MockedPulsarServiceBaseTest() {
        resetConfig();
    }

    protected PulsarService getPulsar() {
        return pulsar;
    }

    protected final void resetConfig() {
        this.conf = getDefaultConf();
    }

    protected final void internalSetup() throws Exception {
        incrementSetupNumber();
        init();
        lookupUrl = new URI(brokerUrl.toString());
        if (isTcpLookup) {
            lookupUrl = new URI(pulsar.getBrokerServiceUrl());

            // setup port forwarding from the advertised port to the listen port
            if (enableBrokerGateway) {
                InetSocketAddress gatewayAddress = new InetSocketAddress(lookupUrl.getHost(), lookupUrl.getPort());
                InetSocketAddress brokerAddress = new InetSocketAddress("127.0.0.1", pulsar.getBrokerListenPort().get());
                brokerGateway = new PortForwarder(gatewayAddress, brokerAddress);
            }
        }
        pulsarClient = newPulsarClient(lookupUrl.toString(), 0);
    }

    protected final void internalSetup(ServiceConfiguration serviceConfiguration) throws Exception {
        this.conf = serviceConfiguration;
        internalSetup();
    }

    protected PulsarClient newPulsarClient(String url, int intervalInSecs) throws PulsarClientException {
        ClientBuilder clientBuilder =
                PulsarClient.builder()
                        .serviceUrl(url)
                        .statsInterval(intervalInSecs, TimeUnit.SECONDS);
        customizeNewPulsarClientBuilder(clientBuilder);
        return createNewPulsarClient(clientBuilder);
    }

    protected void customizeNewPulsarClientBuilder(ClientBuilder clientBuilder) {

    }

    protected PulsarClient createNewPulsarClient(ClientBuilder clientBuilder) throws PulsarClientException {
        return clientBuilder.build();
    }

    protected PulsarClient replacePulsarClient(ClientBuilder clientBuilder) throws PulsarClientException {
        if (pulsarClient != null) {
            pulsarClient.shutdown();
        }
        pulsarClient = createNewPulsarClient(clientBuilder);
        return pulsarClient;
    }

    protected final void internalSetupForStatsTest() throws Exception {
        init();
        String lookupUrl = brokerUrl.toString();
        if (isTcpLookup) {
            lookupUrl = new URI(pulsar.getBrokerServiceUrl()).toString();
        }
        pulsarClient = newPulsarClient(lookupUrl, 1);
    }

    protected void doInitConf() throws Exception {
        this.conf.setBrokerShutdownTimeoutMs(0L);
        this.conf.setBrokerServicePort(Optional.of(0));
        this.conf.setBrokerServicePortTls(Optional.of(0));
        this.conf.setAdvertisedAddress("localhost");
        this.conf.setWebServicePort(Optional.of(0));
        this.conf.setWebServicePortTls(Optional.of(0));
        this.conf.setNumExecutorThreadPoolSize(5);
    }

    protected final void init() throws Exception {
        doInitConf();
        sameThreadOrderedSafeExecutor = new SameThreadOrderedSafeExecutor();
        bkExecutor = OrderedExecutor.newBuilder().numThreads(1).name("mock-pulsar-bk").build();

        mockZooKeeper = createMockZooKeeper();
        mockZooKeeperGlobal = createMockZooKeeperGlobal();
        mockBookKeeper = createMockBookKeeper(bkExecutor);

        startBroker();
    }

    protected final void internalCleanup() throws Exception {
        markCurrentSetupNumberCleaned();
        // if init fails, some of these could be null, and if so would throw
        // an NPE in shutdown, obscuring the real error
        if (admin != null) {
            admin.close();
            admin = null;
        }
        if (pulsarClient != null) {
            pulsarClient.shutdown();
            pulsarClient = null;
        }
        if (brokerGateway != null) {
            brokerGateway.close();
        }
        if (pulsar != null) {
            stopBroker();
            pulsar = null;
        }
        resetConfig();
        if (mockBookKeeper != null) {
            mockBookKeeper.reallyShutdown();
            mockBookKeeper = null;
        }
        if (mockZooKeeperGlobal != null) {
            mockZooKeeperGlobal.shutdown();
            mockZooKeeperGlobal = null;
        }
        if (mockZooKeeper != null) {
            mockZooKeeper.shutdown();
            mockZooKeeper = null;
        }
        if(sameThreadOrderedSafeExecutor != null) {
            try {
                sameThreadOrderedSafeExecutor.shutdownNow();
                sameThreadOrderedSafeExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                log.error("sameThreadOrderedSafeExecutor shutdown had error", ex);
                Thread.currentThread().interrupt();
            }
            sameThreadOrderedSafeExecutor = null;
        }
        if(bkExecutor != null) {
            try {
                bkExecutor.shutdownNow();
                bkExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                log.error("bkExecutor shutdown had error", ex);
                Thread.currentThread().interrupt();
            }
            bkExecutor = null;
        }
        onCleanup();
    }

    protected void onCleanup() {

    }

    protected abstract void setup() throws Exception;

    protected abstract void cleanup() throws Exception;

    protected void beforePulsarStartMocks(PulsarService pulsar) throws Exception {
        // No-op
    }

    protected void restartBroker() throws Exception {
        stopBroker();
        startBroker();
    }

    protected void stopBroker() throws Exception {
        log.info("Stopping Pulsar broker. brokerServiceUrl: {} webServiceAddress: {}", pulsar.getBrokerServiceUrl(),
                pulsar.getWebServiceAddress());
        // set shutdown timeout to 0 for forceful shutdown
        pulsar.getConfiguration().setBrokerShutdownTimeoutMs(0L);
        pulsar.close();
        pulsar = null;
        // Simulate cleanup of ephemeral nodes
        //mockZooKeeper.delete("/loadbalance/brokers/localhost:" + pulsar.getConfiguration().getWebServicePort(), -1);
    }

    protected void startBroker() throws Exception {
        if (this.pulsar != null) {
            throw new RuntimeException("broker already started!");
        }
        this.pulsar = startBroker(conf);

        brokerUrl = pulsar.getWebServiceAddress() != null ? new URL(pulsar.getWebServiceAddress()) : null;
        brokerUrlTls = pulsar.getWebServiceAddressTls() != null ? new URL(pulsar.getWebServiceAddressTls()) : null;

        if (admin != null) {
            admin.close();
        }
        PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder().serviceHttpUrl(brokerUrl != null
                ? brokerUrl.toString()
                : brokerUrlTls.toString());
        customizeNewPulsarAdminBuilder(pulsarAdminBuilder);
        admin = spy(pulsarAdminBuilder.build());
    }

    protected void customizeNewPulsarAdminBuilder(PulsarAdminBuilder pulsarAdminBuilder) {

    }

    protected PulsarService startBroker(ServiceConfiguration conf) throws Exception {
        return startBrokerWithoutAuthorization(conf);
    }

    protected PulsarService startBrokerWithoutAuthorization(ServiceConfiguration conf) throws Exception {
        conf.setBrokerShutdownTimeoutMs(0L);
        PulsarService pulsar = spy(newPulsarService(conf));
        setupBrokerMocks(pulsar);
        beforePulsarStartMocks(pulsar);
        pulsar.start();
        log.info("Pulsar started. brokerServiceUrl: {} webServiceAddress: {}", pulsar.getBrokerServiceUrl(),
                pulsar.getWebServiceAddress());
        return pulsar;
    }

    protected PulsarService newPulsarService(ServiceConfiguration conf) throws Exception {
        return new PulsarService(conf);
    }

    protected void setupBrokerMocks(PulsarService pulsar) throws Exception {
        // Override default providers with mocked ones
        doReturn(mockBookKeeperClientFactory).when(pulsar).newBookKeeperClientFactory();
        doReturn(createLocalMetadataStore()).when(pulsar).createLocalMetadataStore();
        doReturn(createConfigurationMetadataStore()).when(pulsar).createConfigurationMetadataStore();

        Supplier<NamespaceService> namespaceServiceSupplier = () -> spy(new NamespaceService(pulsar));
        doReturn(namespaceServiceSupplier).when(pulsar).getNamespaceServiceProvider();

        doReturn(sameThreadOrderedSafeExecutor).when(pulsar).getOrderedExecutor();
        doReturn(new CounterBrokerInterceptor()).when(pulsar).getBrokerInterceptor();

        doAnswer((invocation) -> spy(invocation.callRealMethod())).when(pulsar).newCompactor();
        if (enableBrokerInterceptor) {
            mockConfigBrokerInterceptors(pulsar);
        }
    }

    protected MetadataStoreExtended createLocalMetadataStore() throws MetadataStoreException {
        return new ZKMetadataStore(mockZooKeeper);
    }

    protected MetadataStoreExtended createConfigurationMetadataStore() throws MetadataStoreException {
        return new ZKMetadataStore(mockZooKeeperGlobal);
    }

    private void mockConfigBrokerInterceptors(PulsarService pulsarService) {
        ServiceConfiguration configuration = spy(conf);
        Set<String> mockBrokerInterceptors = mock(Set.class);
        when(mockBrokerInterceptors.isEmpty()).thenReturn(false);
        when(configuration.getBrokerInterceptors()).thenReturn(mockBrokerInterceptors);
        when(pulsarService.getConfig()).thenReturn(configuration);
    }

    protected void waitForZooKeeperWatchers() {
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    protected TenantInfoImpl createDefaultTenantInfo() throws PulsarAdminException {
        // create local cluster if not exist
        if (!admin.clusters().getClusters().contains(configClusterName)) {
            admin.clusters().createCluster(configClusterName, ClusterData.builder().build());
        }
        Set<String> allowedClusters = Sets.newHashSet();
        allowedClusters.add(configClusterName);
        return new TenantInfoImpl(Sets.newHashSet(), allowedClusters);
    }

    public static MockZooKeeper createMockZooKeeper() throws Exception {
        MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
        List<ACL> dummyAclList = new ArrayList<>(0);

        ZkUtils.createFullPathOptimistic(zk, "/ledgers/available/192.168.1.1:" + 5000,
                "".getBytes(StandardCharsets.UTF_8), dummyAclList, CreateMode.PERSISTENT);

        zk.create("/ledgers/LAYOUT", "1\nflat:1".getBytes(StandardCharsets.UTF_8), dummyAclList,
                CreateMode.PERSISTENT);
        return zk;
    }

    public static MockZooKeeper createMockZooKeeperGlobal() {
        return  MockZooKeeper.newInstanceForGlobalZK(MoreExecutors.newDirectExecutorService());
    }

    public static NonClosableMockBookKeeper createMockBookKeeper(OrderedExecutor executor) throws Exception {
        return spy(new NonClosableMockBookKeeper(executor));
    }

    // Prevent the MockBookKeeper instance from being closed when the broker is restarted within a test
    public static class NonClosableMockBookKeeper extends PulsarMockBookKeeper {

        public NonClosableMockBookKeeper(OrderedExecutor executor) throws Exception {
            super(executor);
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

    private final BookKeeperClientFactory mockBookKeeperClientFactory = new BookKeeperClientFactory() {

        @Override
        public BookKeeper create(ServiceConfiguration conf, MetadataStoreExtended store,
                                 EventLoopGroup eventLoopGroup,
                                 Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                                 Map<String, Object> properties) {
            // Always return the same instance (so that we don't loose the mock BK content on broker restart
            return mockBookKeeper;
        }

        @Override
        public BookKeeper create(ServiceConfiguration conf, MetadataStoreExtended store,
                                 EventLoopGroup eventLoopGroup,
                                 Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                                 Map<String, Object> properties, StatsLogger statsLogger) {
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

    public static void setFieldValue(Class<?> clazz, Object classObj, String fieldName,
                                     Object fieldValue) throws Exception {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(classObj, fieldValue);
    }

    protected static ServiceConfiguration getDefaultConf() {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setAdvertisedAddress("localhost");
        configuration.setClusterName(configClusterName);
        // there are TLS tests in here, they need to use localhost because of the certificate
        configuration.setManagedLedgerCacheSizeMB(8);
        configuration.setActiveConsumerFailoverDelayTimeMillis(0);
        configuration.setDefaultNumberOfNamespaceBundles(1);
        configuration.setZookeeperServers("localhost:2181");
        configuration.setConfigurationStoreServers("localhost:3181");
        configuration.setAllowAutoTopicCreationType("non-partitioned");
        configuration.setBrokerShutdownTimeoutMs(0L);
        configuration.setBrokerServicePort(Optional.of(0));
        configuration.setBrokerServicePortTls(Optional.of(0));
        configuration.setWebServicePort(Optional.of(0));
        configuration.setWebServicePortTls(Optional.of(0));
        configuration.setBookkeeperClientExposeStatsToPrometheus(true);
        configuration.setNumExecutorThreadPoolSize(5);
        configuration.setBrokerMaxConnections(0);
        configuration.setBrokerMaxConnectionsPerIp(0);
        return configuration;
    }

    private static final Logger log = LoggerFactory.getLogger(MockedPulsarServiceBaseTest.class);
}
