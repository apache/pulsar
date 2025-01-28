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
package org.apache.pulsar.broker.auth;

import static org.apache.pulsar.broker.BrokerTestUtil.spyWithoutRecordingInvocations;
import static org.testng.Assert.assertEquals;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.TimeoutHandler;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.utils.ResourceUtils;
import org.apache.zookeeper.MockZooKeeper;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.mockito.Mockito;
import org.mockito.internal.util.MockUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;

/**
 * Base class for all tests that need a Pulsar instance without a ZK and BK cluster.
 */
public abstract class MockedPulsarServiceBaseTest extends TestRetrySupport {
    // All certificate-authority files are copied from the tests/certificate-authority directory and all share the same
    // root CA.
    public static String getTlsFileForClient(String name) {
        return ResourceUtils.getAbsolutePath(String.format("certificate-authority/client-keys/%s.pem", name));
    }
    public final static String CA_CERT_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/certs/ca.cert.pem");
    public final static String BROKER_CERT_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/server-keys/broker.cert.pem");
    public final static String BROKER_KEY_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/server-keys/broker.key-pk8.pem");
    public final static String PROXY_CERT_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/server-keys/proxy.cert.pem");
    public final static String PROXY_KEY_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/server-keys/proxy.key-pk8.pem");
    public final static String BROKER_KEYSTORE_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/jks/broker.keystore.jks");
    public final static String BROKER_TRUSTSTORE_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/jks/broker.truststore.jks");
    public final static String BROKER_TRUSTSTORE_NO_PASSWORD_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/jks/broker.truststore.nopassword.jks");
    public final static String BROKER_KEYSTORE_PW = "111111";
    public final static String BROKER_TRUSTSTORE_PW = "111111";

    public final static String CLIENT_KEYSTORE_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/jks/client.keystore.jks");
    public final static String CLIENT_TRUSTSTORE_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/jks/client.truststore.jks");
    public final static String CLIENT_TRUSTSTORE_NO_PASSWORD_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/jks/client.truststore.nopassword.jks");
    public final static String CLIENT_KEYSTORE_PW = "111111";
    public final static String CLIENT_TRUSTSTORE_PW = "111111";

    public final static String PROXY_KEYSTORE_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/jks/proxy.keystore.jks");
    public final static String PROXY_KEYSTORE_PW = "111111";
    public final static String PROXY_AND_CLIENT_TRUSTSTORE_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/jks/proxy-and-client.truststore.jks");
    public final static String PROXY_AND_CLIENT_TRUSTSTORE_PW = "111111";

    public final static String CLIENT_KEYSTORE_CN = "clientuser";
    public final static String KEYSTORE_TYPE = "JKS";

    protected final String DUMMY_VALUE = "DUMMY_VALUE";
    protected final String GLOBAL_DUMMY_VALUE = "GLOBAL_DUMMY_VALUE";

    protected ServiceConfiguration conf;
    protected PulsarTestContext pulsarTestContext;
    protected MockZooKeeper mockZooKeeper;
    protected MockZooKeeper mockZooKeeperGlobal;
    protected PulsarService pulsar;
    protected PulsarAdmin admin;
    protected PulsarClient pulsarClient;
    protected PortForwarder brokerGateway;
    protected boolean enableBrokerGateway =  false;
    protected URL brokerUrl;
    protected URL brokerUrlTls;

    protected URI lookupUrl;

    protected boolean isTcpLookup = false;
    protected String configClusterName = "test";

    protected boolean enableBrokerInterceptor = false;

    public MockedPulsarServiceBaseTest() {
        resetConfig();
    }

    protected void setupWithClusterName(String clusterName) throws Exception {
        this.conf.setClusterName(clusterName);
        this.configClusterName = clusterName;
        this.internalSetup();
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

    /**
     * Customize the {@link ClientBuilder} before creating a new {@link PulsarClient} instance.
     * @param clientBuilder
     */
    protected void customizeNewPulsarClientBuilder(ClientBuilder clientBuilder) {

    }

    /**
     * Customize the {@link BrokerService} just after it has been created.
     * @param brokerService the {@link BrokerService} instance
     */
    protected BrokerService customizeNewBrokerService(BrokerService brokerService) {
        return brokerService;
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
        this.conf.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        this.conf.setBrokerServicePort(Optional.of(0));
        this.conf.setAdvertisedAddress("localhost");
        this.conf.setWebServicePort(Optional.of(0));
        this.conf.setNumExecutorThreadPoolSize(5);
        this.conf.setExposeBundlesMetricsInPrometheus(true);
    }

    protected final void init() throws Exception {
        doInitConf();
        startBroker();
    }

    protected final void internalCleanup() throws Exception {
        markCurrentSetupNumberCleaned();
        // if init fails, some of these could be null, and if so would throw
        // an NPE in shutdown, obscuring the real error
        if (admin != null) {
            admin.close();
            if (MockUtil.isMock(admin)) {
                Mockito.reset(admin);
            }
            admin = null;
        }
        if (pulsarClient != null) {
            pulsarClient.shutdown();
            pulsarClient = null;
        }
        if (brokerGateway != null) {
            brokerGateway.close();
            brokerGateway = null;
        }
        if (pulsarTestContext != null) {
            pulsarTestContext.close();
            pulsarTestContext = null;
        }

        resetConfig();
        onCleanup();

        // clear fields to avoid test runtime memory leak, pulsarTestContext already handles closing of these instances
        pulsar = null;
        mockZooKeeper = null;
        mockZooKeeperGlobal = null;
    }

    protected void onCleanup() {

    }

    protected abstract void setup() throws Exception;

    protected abstract void cleanup() throws Exception;

    /**
     * Customize the PulsarService instance before it is started.
     * This can be used to add custom mock or spy configuration to PulsarService.
     *
     * @param pulsar the PulsarService instance
     * @throws Exception if an error occurs
     */
    protected void beforePulsarStart(PulsarService pulsar) throws Exception {
        // No-op
    }

    /**
     * Customize the PulsarService instance after it is started.
     * @param pulsar the PulsarService instance
     * @throws Exception if an error occurs
     */
     protected void afterPulsarStart(PulsarService pulsar) throws Exception {
        // No-op
    }

    /**
     * Restarts the test broker.
     *
     * @throws Exception if an error occurs
     */
    protected void restartBroker() throws Exception {
        stopBroker();
        startBroker();
    }

    protected void stopBroker() throws Exception {
        if (pulsar == null) {
            return;
        }
        log.info("Stopping Pulsar broker. brokerServiceUrl: {} webServiceAddress: {}", pulsar.getBrokerServiceUrl(),
                pulsar.getWebServiceAddress());
        pulsar.close();
        pulsar = null;
        // Simulate cleanup of ephemeral nodes
        //mockZooKeeper.delete("/loadbalance/brokers/localhost:" + pulsar.getConfiguration().getWebServicePort(), -1);
    }

    protected void startBroker() throws Exception {
        this.pulsarTestContext = createMainPulsarTestContext(conf);
        this.mockZooKeeper = pulsarTestContext.getMockZooKeeper();
        this.mockZooKeeperGlobal = pulsarTestContext.getMockZooKeeperGlobal();
        this.pulsar = pulsarTestContext.getPulsarService();
        afterPulsarStart(pulsar);

        brokerUrl = pulsar.getWebServiceAddress() != null ? new URL(pulsar.getWebServiceAddress()) : null;
        brokerUrlTls = pulsar.getWebServiceAddressTls() != null ? new URL(pulsar.getWebServiceAddressTls()) : null;

        if (admin != null) {
            admin.close();
            if (MockUtil.isMock(admin)) {
                Mockito.reset(admin);
            }
        }
        PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder().serviceHttpUrl(brokerUrl != null
                ? brokerUrl.toString()
                : brokerUrlTls.toString());
        customizeNewPulsarAdminBuilder(pulsarAdminBuilder);
        admin = spyWithoutRecordingInvocations(pulsarAdminBuilder.build());
    }

    /**
     * Customize the PulsarAdminBuilder instance before it is used to create a PulsarAdmin instance.
     *
     * @param pulsarAdminBuilder the PulsarAdminBuilder instance
     */
    protected void customizeNewPulsarAdminBuilder(PulsarAdminBuilder pulsarAdminBuilder) {

    }

    /**
     * Creates the PulsarTestContext instance for the main PulsarService instance.
     *
     * @see PulsarTestContext
     * @param conf the ServiceConfiguration instance to use
     * @return the PulsarTestContext instance
     * @throws Exception if an error occurs
     */
    protected PulsarTestContext createMainPulsarTestContext(ServiceConfiguration conf) throws Exception {
        PulsarTestContext.Builder pulsarTestContextBuilder = createPulsarTestContextBuilder(conf);
        if (pulsarTestContext != null) {
            pulsarTestContextBuilder.reuseMockBookkeeperAndMetadataStores(pulsarTestContext);
            pulsarTestContextBuilder.reuseSpyConfig(pulsarTestContext);
            pulsarTestContextBuilder.chainClosing(pulsarTestContext);
        }
        customizeMainPulsarTestContextBuilder(pulsarTestContextBuilder);
        return pulsarTestContextBuilder
                .build();
    }

    /**
     * Customize the PulsarTestContext.Builder instance used for creating the PulsarTestContext
     * for the main PulsarService instance.
     *
     * @param pulsarTestContextBuilder the PulsarTestContext.Builder instance to customize
     */
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {

    }

    /**
     * Creates a PulsarTestContext.Builder instance that is used for the builder of the main PulsarTestContext and also
     * for the possible additional PulsarTestContext instances.
     *
     * When overriding this method, it is recommended to call the super method and then customize the returned builder.
     *
     * @param conf the ServiceConfiguration instance to use
     * @return a PulsarTestContext.Builder instance
     */
    protected PulsarTestContext.Builder createPulsarTestContextBuilder(ServiceConfiguration conf) {
        PulsarTestContext.Builder builder = PulsarTestContext.builder()
                .spyByDefault()
                .config(conf)
                .withMockZookeeper(true)
                .pulsarServiceCustomizer(pulsarService -> {
                    try {
                        beforePulsarStart(pulsarService);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .brokerServiceCustomizer(this::customizeNewBrokerService);
        return builder;
    }

    /**
     * This method can be used in test classes for creating additional PulsarTestContext instances
     * that share the same mock ZooKeeper and BookKeeper instances as the main PulsarTestContext instance.
     *
     * @param conf the ServiceConfiguration instance to use
     * @return the PulsarTestContext instance
     * @throws Exception if an error occurs
     */
    protected PulsarTestContext createAdditionalPulsarTestContext(ServiceConfiguration conf) throws Exception {
        return createPulsarTestContextBuilder(conf)
                .reuseMockBookkeeperAndMetadataStores(pulsarTestContext)
                .reuseSpyConfig(pulsarTestContext)
                .build();
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
        Set<String> allowedClusters = new HashSet<>();
        allowedClusters.add(configClusterName);
        return new TenantInfoImpl(new HashSet<>(), allowedClusters);
    }


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

    protected ServiceConfiguration getDefaultConf() {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setAdvertisedAddress("localhost");
        configuration.setClusterName(configClusterName);
        // there are TLS tests in here, they need to use localhost because of the certificate
        configuration.setManagedLedgerCacheSizeMB(8);
        configuration.setActiveConsumerFailoverDelayTimeMillis(0);
        configuration.setDefaultNumberOfNamespaceBundles(1);
        configuration.setMetadataStoreUrl("zk:localhost:2181");
        configuration.setConfigurationMetadataStoreUrl("zk:localhost:3181");
        configuration.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
        configuration.setBrokerShutdownTimeoutMs(0L);
        configuration.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        configuration.setBrokerServicePort(Optional.of(0));
        configuration.setWebServicePort(Optional.of(0));
        configuration.setBookkeeperClientExposeStatsToPrometheus(true);
        configuration.setNumExecutorThreadPoolSize(5);
        configuration.setBrokerMaxConnections(0);
        configuration.setBrokerMaxConnectionsPerIp(0);
        return configuration;
    }

    protected void setupDefaultTenantAndNamespace() throws Exception {
        final String tenant = "public";
        final String namespace = tenant + "/default";

        if (!admin.clusters().getClusters().contains(configClusterName)) {
            admin.clusters().createCluster(configClusterName,
                    ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        }

        if (!admin.tenants().getTenants().contains(tenant)) {
            admin.tenants().createTenant(tenant, TenantInfo.builder().allowedClusters(
                    Sets.newHashSet(configClusterName)).build());
        }

        if (!admin.namespaces().getNamespaces(tenant).contains(namespace)) {
            admin.namespaces().createNamespace(namespace);
        }
    }

    protected Object asyncRequests(Consumer<TestAsyncResponse> function) throws Exception {
        TestAsyncResponse ctx = new TestAsyncResponse();
        function.accept(ctx);
        ctx.latch.await();
        if (ctx.e != null) {
            throw (Exception) ctx.e;
        }
        return ctx.response;
    }

    public static class TestAsyncResponse implements AsyncResponse {

        Object response;
        Throwable e;
        CountDownLatch latch = new CountDownLatch(1);

        @Override
        public boolean resume(Object response) {
            this.response = response;
            latch.countDown();
            return true;
        }

        @Override
        public boolean resume(Throwable response) {
            this.e = response;
            latch.countDown();
            return true;
        }

        @Override
        public boolean cancel() {
            return false;
        }

        @Override
        public boolean cancel(int retryAfter) {
            return false;
        }

        @Override
        public boolean cancel(Date retryAfter) {
            return false;
        }

        @Override
        public boolean isSuspended() {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public boolean setTimeout(long time, TimeUnit unit) {
            return false;
        }

        @Override
        public void setTimeoutHandler(TimeoutHandler handler) {

        }

        @Override
        public Collection<Class<?>> register(Class<?> callback) {
            return null;
        }

        @Override
        public Map<Class<?>, Collection<Class<?>>> register(Class<?> callback, Class<?>... callbacks) {
            return null;
        }

        @Override
        public Collection<Class<?>> register(Object callback) {
            return null;
        }

        @Override
        public Map<Class<?>, Collection<Class<?>>> register(Object callback, Object... callbacks) {
            return null;
        }

    }

    /**
     * see {@link BrokerTestBase#deleteNamespaceWithRetry(String, boolean, PulsarAdmin, Collection)}
     */
    protected void deleteNamespaceWithRetry(String ns, boolean force)
            throws Exception {
        BrokerTestBase.deleteNamespaceWithRetry(ns, force, admin, pulsar);
    }

    /**
     * see {@link BrokerTestBase#deleteNamespaceWithRetry(String, boolean, PulsarAdmin, Collection)}
     */
    protected void deleteNamespaceWithRetry(String ns, boolean force, PulsarAdmin admin)
            throws Exception {
        BrokerTestBase.deleteNamespaceWithRetry(ns, force, admin, pulsar);
    }

    /**
     * see {@link MockedPulsarServiceBaseTest#deleteNamespaceWithRetry(String, boolean, PulsarAdmin, Collection)}
     */
    public static void deleteNamespaceWithRetry(String ns, boolean force, PulsarAdmin admin, PulsarService...pulsars)
            throws Exception {
        deleteNamespaceWithRetry(ns, force, admin, Arrays.asList(pulsars));
    }

    /**
     * 1. Pause system "__change_event" topic creates.
     * 2. Do delete namespace with retry because maybe fail by race-condition with create topics.
     */
    public static void deleteNamespaceWithRetry(String ns, boolean force, PulsarAdmin admin,
                                                Collection<PulsarService> pulsars) throws Exception {
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                // Maybe fail by race-condition with create topics, just retry.
                admin.namespaces().deleteNamespace(ns, force);
                return true;
            } catch (Exception ex) {
                return false;
            }
        });
    }

    @DataProvider(name = "invalidPersistentPolicies")
    public Object[][] incorrectPersistentPolicies() {
        return new Object[][] {
                {0, 0, 0},
                {1, 0, 0},
                {0, 0, 1},
                {0, 1, 0},
                {1, 1, 0},
                {1, 0, 1}
        };
    }

    protected ServiceProducer getServiceProducer(ProducerImpl clientProducer, String topicName) {
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).join().get();
        org.apache.pulsar.broker.service.Producer serviceProducer =
                persistentTopic.getProducers().get(clientProducer.getProducerName());
        long clientProducerId = WhiteboxImpl.getInternalState(clientProducer, "producerId");
        assertEquals(serviceProducer.getProducerId(), clientProducerId);
        assertEquals(serviceProducer.getEpoch(), clientProducer.getConnectionHandler().getEpoch());
        return new ServiceProducer(serviceProducer, persistentTopic);
    }

    @Data
    @AllArgsConstructor
    public static class ServiceProducer {
        private org.apache.pulsar.broker.service.Producer serviceProducer;
        private PersistentTopic persistentTopic;
    }

    protected void sleepSeconds(int seconds){
        try {
            Thread.sleep(1000 * seconds);
        } catch (InterruptedException e) {
            log.warn("This thread has been interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

    private static void reconnectAllConnections(PulsarClientImpl c) throws Exception {
        ConnectionPool pool = c.getCnxPool();
        Method closeAllConnections = ConnectionPool.class.getDeclaredMethod("closeAllConnections", new Class[]{});
        closeAllConnections.setAccessible(true);
        closeAllConnections.invoke(pool, new Object[]{});
    }

    protected void reconnectAllConnections() throws Exception {
        reconnectAllConnections((PulsarClientImpl) pulsarClient);
    }

    protected void logTopicStats(String topic) {
        BrokerTestUtil.logTopicStats(log, admin, topic);
    }

    private static final Logger log = LoggerFactory.getLogger(MockedPulsarServiceBaseTest.class);
}
