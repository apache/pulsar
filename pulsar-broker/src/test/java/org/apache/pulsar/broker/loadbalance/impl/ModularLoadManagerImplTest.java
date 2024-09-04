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
package org.apache.pulsar.broker.loadbalance.impl;

import static java.lang.Thread.sleep;
import static org.apache.pulsar.broker.resources.LoadBalanceResources.BROKER_TIME_AVERAGE_BASE_PATH;
import static org.apache.pulsar.broker.resources.LoadBalanceResources.BUNDLE_DATA_BASE_PATH;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadBalancerTestingUtils;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared.BrokerTopicLoadingPredicate;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.ResourceQuota;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.PortManager;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.policies.data.loadbalancer.BrokerData;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageMessageData;
import org.apache.pulsar.utils.ResourceUtils;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class ModularLoadManagerImplTest {

    public final static String CA_CERT_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/certs/ca.cert.pem");
    public final static String BROKER_CERT_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/server-keys/broker.cert.pem");
    public final static String BROKER_KEY_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/server-keys/broker.key-pk8.pem");

    private LocalBookkeeperEnsemble bkEnsemble;

    private URL url1;
    private PulsarService pulsar1;
    private PulsarAdmin admin1;

    private URL url2;
    private PulsarService pulsar2;
    private PulsarAdmin admin2;

    private PulsarService pulsar3;

    private String primaryBrokerId;

    private String secondaryBrokerId;

    private NamespaceBundleFactory nsFactory;

    private ModularLoadManagerImpl primaryLoadManager;
    private ModularLoadManagerImpl secondaryLoadManager;

    private ExecutorService executor;

    // Invoke non-overloaded method.
    private Object invokeSimpleMethod(final Object instance, final String methodName, final Object... args)
            throws Exception {
        for (Method method : instance.getClass().getDeclaredMethods()) {
            if (method.getName().equals(methodName)) {
                method.setAccessible(true);
                return method.invoke(instance, args);
            }
        }
        throw new IllegalArgumentException("Method not found: " + methodName);
    }

    private static Object getField(final Object instance, final String fieldName) throws Exception {
        final Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(instance);
    }

    private static void setField(final Object instance, final String fieldName, final Object value) throws Exception {
        final Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(instance, value);
    }

    @BeforeMethod
    void setup() throws Exception {
        executor = new ThreadPoolExecutor(1, 20, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        // Start broker 1
        ServiceConfiguration config1 = new ServiceConfiguration();
        config1.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        config1.setLoadBalancerLoadSheddingStrategy("org.apache.pulsar.broker.loadbalance.impl.OverloadShedder");
        config1.setClusterName("use");
        config1.setWebServicePort(Optional.of(0));
        config1.setWebServicePortTls(Optional.of(0));
        config1.setMetadataStoreUrl("zk:127.0.0.1:" + bkEnsemble.getZookeeperPort());

        config1.setAdvertisedAddress("localhost");
        config1.setBrokerShutdownTimeoutMs(0L);
        config1.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        config1.setBrokerServicePort(Optional.of(0));
        config1.setBrokerServicePortTls(Optional.of(0));
        config1.setTlsTrustCertsFilePath(CA_CERT_FILE_PATH);
        config1.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        config1.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
        pulsar1 = new PulsarService(config1);
        pulsar1.start();

        primaryBrokerId = pulsar1.getBrokerId();
        url1 = new URL(pulsar1.getWebServiceAddress());
        admin1 = PulsarAdmin.builder().serviceHttpUrl(url1.toString()).build();

        // Start broker 2
        ServiceConfiguration config2 = new ServiceConfiguration();
        config2.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        config2.setLoadBalancerLoadSheddingStrategy("org.apache.pulsar.broker.loadbalance.impl.OverloadShedder");
        config2.setClusterName("use");
        config2.setWebServicePort(Optional.of(0));
        config2.setWebServicePortTls(Optional.of(0));
        config2.setMetadataStoreUrl("zk:127.0.0.1:" + bkEnsemble.getZookeeperPort());
        config2.setAdvertisedAddress("localhost");
        config2.setBrokerShutdownTimeoutMs(0L);
        config2.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        config2.setBrokerServicePort(Optional.of(0));
        config2.setBrokerServicePortTls(Optional.of(0));
        config2.setTlsTrustCertsFilePath(CA_CERT_FILE_PATH);
        config2.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        config2.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
        pulsar2 = new PulsarService(config2);
        pulsar2.start();

        ServiceConfiguration config = new ServiceConfiguration();
        config.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        config.setLoadBalancerLoadSheddingStrategy("org.apache.pulsar.broker.loadbalance.impl.OverloadShedder");
        config.setClusterName("use");
        config.setWebServicePort(Optional.of(0));
        config.setWebServicePortTls(Optional.of(0));
        config.setMetadataStoreUrl("zk:127.0.0.1:" + bkEnsemble.getZookeeperPort());
        config.setAdvertisedAddress("localhost");
        config.setBrokerShutdownTimeoutMs(0L);
        config.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        config.setBrokerServicePort(Optional.of(0));
        config.setBrokerServicePortTls(Optional.of(0));
        config.setTlsTrustCertsFilePath(CA_CERT_FILE_PATH);
        config.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        config.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
        pulsar3 = new PulsarService(config);

        secondaryBrokerId = pulsar2.getBrokerId();
        url2 = new URL(pulsar2.getWebServiceAddress());
        admin2 = PulsarAdmin.builder().serviceHttpUrl(url2.toString()).build();

        primaryLoadManager = (ModularLoadManagerImpl) getField(pulsar1.getLoadManager().get(), "loadManager");
        secondaryLoadManager = (ModularLoadManagerImpl) getField(pulsar2.getLoadManager().get(), "loadManager");
        nsFactory = new NamespaceBundleFactory(pulsar1, Hashing.crc32());
        sleep(100);
    }

    @AfterMethod(alwaysRun = true)
    void shutdown() throws Exception {
        log.info("--- Shutting down ---");
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }

        if (admin1 != null) {
            admin1.close();
            admin1 = null;
        }
        if (admin2 != null) {
            admin2.close();
            admin2 = null;
        }

        if (pulsar2 != null) {
            pulsar2.close();
            pulsar2 = null;
        }
        if (pulsar1 != null) {
            pulsar1.close();
            pulsar1 = null;
        }
        if (pulsar3 != null && pulsar3.isRunning()) {
            pulsar3.close();
        }
        pulsar3 = null;
        if (bkEnsemble != null) {
            bkEnsemble.stop();
            bkEnsemble = null;
        }
    }

    private NamespaceBundle makeBundle(final String property, final String cluster, final String namespace) {
        return nsFactory.getBundle(NamespaceName.get(property, cluster, namespace),
                Range.range(NamespaceBundles.FULL_LOWER_BOUND, BoundType.CLOSED, NamespaceBundles.FULL_UPPER_BOUND,
                        BoundType.CLOSED));
    }

    private NamespaceBundle makeBundle(final String all) {
        return makeBundle(all, all, all);
    }

    private String mockBundleName(final int i) {
        return String.format("%d/%d/%d/0x00000000_0xffffffff", i, i, i);
    }

    // Test disabled since it's depending on CPU usage in the machine
    @Test(enabled = false)
    public void testCandidateConsistency() throws Exception {
        boolean foundFirst = false;
        boolean foundSecond = false;
        // After 2 selections, the load balancer should select both brokers due to preallocation.
        for (int i = 0; i < 2; ++i) {
            final ServiceUnitId serviceUnit = makeBundle(Integer.toString(i));
            final String broker = primaryLoadManager.selectBrokerForAssignment(serviceUnit).get();
            if (broker.equals(primaryBrokerId)) {
                foundFirst = true;
            } else {
                foundSecond = true;
            }
        }

        assertTrue(foundFirst);
        assertTrue(foundSecond);

        // Now disable the secondary broker.
        secondaryLoadManager.disableBroker();

        LoadData loadData = (LoadData) getField(primaryLoadManager, "loadData");

        // Make sure the second broker is not in the internal map.
        Awaitility.await().untilAsserted(() -> assertFalse(loadData.getBrokerData().containsKey(secondaryBrokerId)));

        // Try 5 more selections, ensure they all go to the first broker.
        for (int i = 2; i < 7; ++i) {
            final ServiceUnitId serviceUnit = makeBundle(Integer.toString(i));
            assertEquals(primaryLoadManager.selectBrokerForAssignment(serviceUnit), primaryBrokerId);
        }
    }

    // Test that bundles belonging to the same namespace are distributed evenly among brokers.

    // Test disabled since it's depending on CPU usage in the machine
    @Test(enabled = false)
    public void testEvenBundleDistribution() throws Exception {
        final NamespaceBundle[] bundles = LoadBalancerTestingUtils.makeBundles(nsFactory, "test", "test", "test", 16);
        int numAssignedToPrimary = 0;
        int numAssignedToSecondary = 0;
        final BundleData bundleData = new BundleData(10, 1000);
        final TimeAverageMessageData longTermMessageData = new TimeAverageMessageData(1000);
        longTermMessageData.setMsgRateIn(1000);
        bundleData.setLongTermData(longTermMessageData);
        final String firstBundleDataPath = String.format("%s/%s", BUNDLE_DATA_BASE_PATH, bundles[0]);
        // Write long message rate for first bundle to ensure that even bundle distribution is not a coincidence of
        // balancing by message rate. If we were balancing by message rate, one of the brokers should only have this
        // one bundle.
        pulsar1.getLocalMetadataStore().getMetadataCache(BundleData.class).create(firstBundleDataPath, bundleData).join();
        for (final NamespaceBundle bundle : bundles) {
            if (primaryLoadManager.selectBrokerForAssignment(bundle).equals(primaryBrokerId)) {
                ++numAssignedToPrimary;
            } else {
                ++numAssignedToSecondary;
            }
            if ((numAssignedToPrimary + numAssignedToSecondary) % 2 == 0) {
                // On even number of assignments, assert that an equal number of bundles have been assigned between
                // them.
                assertEquals(numAssignedToPrimary, numAssignedToSecondary);
            }
        }
    }



    @Test
    public void testBrokerAffinity() throws Exception {
        // Start broker 3
        pulsar3.start();

        final String tenant = "test";
        final String cluster = "test";
        String namespace = tenant + "/" + cluster + "/" + "test";
        String topic = "persistent://" + namespace + "/my-topic1";
        admin1.clusters().createCluster(cluster, ClusterData.builder().serviceUrl(pulsar1.getWebServiceAddress()).build());
        admin1.tenants().createTenant(tenant,
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet(cluster)));
        admin1.namespaces().createNamespace(namespace, 16);

        String topicLookup = admin1.lookups().lookupTopic(topic);
        String bundleRange = admin1.lookups().getBundleRange(topic);

        String brokerServiceUrl = pulsar1.getBrokerServiceUrl();
        String brokerId = pulsar1.getBrokerId();
        log.debug("initial broker service url - {}", topicLookup);
        Random rand=new Random();

        if (topicLookup.equals(brokerServiceUrl)) {
            int x = rand.nextInt(2);
            if (x == 0) {
                brokerId = pulsar2.getBrokerId();
                brokerServiceUrl = pulsar2.getBrokerServiceUrl();
            }
            else {
                brokerId = pulsar3.getBrokerId();
                brokerServiceUrl = pulsar3.getBrokerServiceUrl();
            }
        }
        log.debug("destination broker service url - {}, broker url - {}", brokerServiceUrl, brokerId);
        String leaderBrokerId = admin1.brokers().getLeaderBroker().getBrokerId();
        log.debug("leader lookup address - {}, broker1 lookup address - {}", leaderBrokerId,
                pulsar1.getBrokerId());
        // Make a call to broker which is not a leader
        if (!leaderBrokerId.equals(pulsar1.getBrokerId())) {
            admin1.namespaces().unloadNamespaceBundle(namespace, bundleRange, brokerId);
        }
        else {
            admin2.namespaces().unloadNamespaceBundle(namespace, bundleRange, brokerId);
        }

        sleep(2000);
        String topicLookupAfterUnload = admin1.lookups().lookupTopic(topic);
        log.debug("final broker service url - {}", topicLookupAfterUnload);
        Assert.assertEquals(brokerServiceUrl, topicLookupAfterUnload);
    }

    /**
     * It verifies that once broker owns max-number of topics: load-manager doesn't allocates new bundles to that broker
     * unless all the brokers are in same state.
     *
     * <pre>
     * 1. Create a bundle whose bundle-resource-quota will contain max-topics
     * 2. Load-manager assigns broker to this bundle so, assigned broker is overloaded with max-topics
     * 3. For any new further bundles: broker assigns different brokers.
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testMaxTopicDistributionToBroker() throws Exception {

        final int totalBundles = 50;
        final NamespaceBundle[] bundles = LoadBalancerTestingUtils.makeBundles(nsFactory, "test", "test", "test",
                totalBundles);
        final BundleData bundleData = new BundleData(10, 1000);
        // it sets max topics under this bundle so, owner of this broker reaches max-topic threshold
        bundleData.setTopics(pulsar1.getConfiguration().getLoadBalancerBrokerMaxTopics() + 10);
        final TimeAverageMessageData longTermMessageData = new TimeAverageMessageData(1000);
        longTermMessageData.setMsgRateIn(1000);
        bundleData.setLongTermData(longTermMessageData);
        final String firstBundleDataPath = String.format("%s/%s", BUNDLE_DATA_BASE_PATH, bundles[0]);
        pulsar1.getLocalMetadataStore().getMetadataCache(BundleData.class).create(firstBundleDataPath, bundleData).join();
        String maxTopicOwnedBroker = primaryLoadManager.selectBrokerForAssignment(bundles[0]).get();

        for (int i = 1; i < totalBundles; i++) {
            assertNotEquals(primaryLoadManager.selectBrokerForAssignment(bundles[i]), maxTopicOwnedBroker);
        }
    }

    /**
     * It verifies that the load-manager of leader broker only write topK * brokerCount bundles to zk.
     */
    @Test
    public void testFilterBundlesWhileWritingToMetadataStore() throws Exception {
        Map<String, PulsarService> pulsarServices = new HashMap<>();
        pulsarServices.put(pulsar1.getWebServiceAddress(), pulsar1);
        pulsarServices.put(pulsar2.getWebServiceAddress(), pulsar2);
        MetadataCache<BundleData> metadataCache = pulsar1.getLocalMetadataStore().getMetadataCache(BundleData.class);
        String protocol = "http://";
        PulsarService leaderBroker = pulsarServices.get(protocol + pulsar1.getLeaderElectionService().getCurrentLeader().get().getBrokerId());
        ModularLoadManagerImpl loadManager = (ModularLoadManagerImpl) getField(
                leaderBroker.getLoadManager().get(), "loadManager");
        int topK = 1;
        leaderBroker.getConfiguration().setLoadBalancerMaxNumberOfBundlesInBundleLoadReport(topK);
        // there are two broker in cluster, so total bundle count will be topK * 2
        int exportBundleCount = topK * 2;

        // create and configure bundle-data
        final int totalBundles = 5;
        final NamespaceBundle[] bundles = LoadBalancerTestingUtils.makeBundles(
                nsFactory, "test", "test", "test", totalBundles);
        LoadData loadData = (LoadData) getField(loadManager, "loadData");
        for (int i = 0; i < totalBundles; i++) {
            final BundleData bundleData = new BundleData(10, 1000);
            final String bundleDataPath = String.format("%s/%s", BUNDLE_DATA_BASE_PATH, bundles[i]);
            final TimeAverageMessageData longTermMessageData = new TimeAverageMessageData(1000);
            longTermMessageData.setMsgThroughputIn(1000 * i);
            longTermMessageData.setMsgThroughputOut(1000 * i);
            longTermMessageData.setMsgRateIn(1000 * i);
            longTermMessageData.setNumSamples(1000);
            bundleData.setLongTermData(longTermMessageData);
            loadData.getBundleData().put(bundles[i].toString(), bundleData);
            loadData.getBrokerData().get(leaderBroker.getWebServiceAddress().substring(protocol.length()))
                    .getLocalData().getLastStats().put(bundles[i].toString(), new NamespaceBundleStats());
            metadataCache.create(bundleDataPath, bundleData).join();
        }
        for (int i = 0; i < totalBundles; i++) {
            final String bundleDataPath = String.format("%s/%s", BUNDLE_DATA_BASE_PATH, bundles[i]);
            assertEquals(metadataCache.getWithStats(bundleDataPath).get().get().getStat().getVersion(), 0);
        }

        // update bundle data to zk and verify
        loadManager.writeBundleDataOnZooKeeper();
        int filterBundleCount = totalBundles - exportBundleCount;
        for (int i = 0; i < filterBundleCount; i++) {
            final String bundleDataPath = String.format("%s/%s", BUNDLE_DATA_BASE_PATH, bundles[i]);
            assertEquals(metadataCache.getWithStats(bundleDataPath).get().get().getStat().getVersion(), 0);
        }
        for (int i = filterBundleCount; i < totalBundles; i++) {
            final String bundleDataPath = String.format("%s/%s", BUNDLE_DATA_BASE_PATH, bundles[i]);
            assertEquals(metadataCache.getWithStats(bundleDataPath).get().get().getStat().getVersion(), 1);
        }
    }

    // Test that load shedding works
    @Test
    public void testLoadShedding() throws Exception {
        final NamespaceBundleStats stats1 = new NamespaceBundleStats();
        final NamespaceBundleStats stats2 = new NamespaceBundleStats();
        stats1.msgRateIn = 100;
        stats2.msgRateIn = 200;
        final Map<String, NamespaceBundleStats> statsMap = new ConcurrentHashMap<>();
        statsMap.put(mockBundleName(1), stats1);
        statsMap.put(mockBundleName(2), stats2);
        final LocalBrokerData localBrokerData = new LocalBrokerData();
        localBrokerData.update(new SystemResourceUsage(), statsMap);
        final Namespaces namespacesSpy1 = spy(pulsar1.getAdminClient().namespaces());
        AtomicReference<String> bundleReference = new AtomicReference<>();
        doAnswer(invocation -> {
            bundleReference.set(invocation.getArguments()[0].toString() + '/' + invocation.getArguments()[1]);
            return null;
        }).when(namespacesSpy1).unloadNamespaceBundle(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

        AtomicReference<Optional<String>> selectedBrokerRef = new AtomicReference<>();
        ModularLoadManagerImpl primaryLoadManagerSpy = spy(primaryLoadManager);
        doAnswer(invocation -> {
            ServiceUnitId serviceUnitId = (ServiceUnitId) invocation.getArguments()[0];
            Optional<String> broker = primaryLoadManager.selectBroker(serviceUnitId);
            selectedBrokerRef.set(broker);
            return broker;
        }).when(primaryLoadManagerSpy).selectBroker(any());

        setField(pulsar1.getAdminClient(), "namespaces", namespacesSpy1);
        pulsar1.getConfiguration().setLoadBalancerEnabled(true);
        final LoadData loadData = (LoadData) getField(primaryLoadManagerSpy, "loadData");
        final Map<String, BrokerData> brokerDataMap = loadData.getBrokerData();
        final BrokerData brokerDataSpy1 = spy(brokerDataMap.get(primaryBrokerId));
        when(brokerDataSpy1.getLocalData()).thenReturn(localBrokerData);
        brokerDataMap.put(primaryBrokerId, brokerDataSpy1);
        // Need to update all the bundle data for the shredder to see the spy.
        primaryLoadManagerSpy.handleDataNotification(new Notification(NotificationType.Created, LoadManager.LOADBALANCE_BROKERS_ROOT + "/broker:8080"));

        sleep(100);
        localBrokerData.setCpu(new ResourceUsage(80, 100));
        primaryLoadManagerSpy.doLoadShedding();

        // 80% is below overload threshold: verify nothing is unloaded.
        verify(namespacesSpy1, Mockito.times(0))
                .unloadNamespaceBundle(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

        localBrokerData.setCpu(new ResourceUsage(90, 100));
        primaryLoadManagerSpy.doLoadShedding();
        // Most expensive bundle will be unloaded.
        verify(namespacesSpy1, Mockito.times(1))
                .unloadNamespaceBundle(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        assertEquals(bundleReference.get(), mockBundleName(2));
        assertEquals(selectedBrokerRef.get().get(), secondaryBrokerId);

        primaryLoadManagerSpy.doLoadShedding();
        // Now less expensive bundle will be unloaded (normally other bundle would move off and nothing would be
        // unloaded, but this is not the case due to the spy's behavior).
        verify(namespacesSpy1, Mockito.times(2))
                .unloadNamespaceBundle(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        assertEquals(bundleReference.get(), mockBundleName(1));
        assertEquals(selectedBrokerRef.get().get(), secondaryBrokerId);

        primaryLoadManagerSpy.doLoadShedding();
        // Now both are in grace period: neither should be unloaded.
        verify(namespacesSpy1, Mockito.times(2))
                .unloadNamespaceBundle(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        assertEquals(selectedBrokerRef.get().get(), secondaryBrokerId);

        // Test bundle transfer to same broker

        loadData.getRecentlyUnloadedBundles().clear();
        primaryLoadManagerSpy.doLoadShedding();
        verify(namespacesSpy1, Mockito.times(3))
                .unloadNamespaceBundle(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

        loadData.getRecentlyUnloadedBundles().clear();
        primaryLoadManagerSpy.doLoadShedding();
        // The bundle shouldn't be unloaded because the broker is the same.
        verify(namespacesSpy1, Mockito.times(4))
                .unloadNamespaceBundle(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void testUnloadBundleMetric() throws Exception {
        final NamespaceBundleStats stats1 = new NamespaceBundleStats();
        final NamespaceBundleStats stats2 = new NamespaceBundleStats();
        stats1.msgRateIn = 100;
        stats2.msgRateIn = 200;
        final Map<String, NamespaceBundleStats> statsMap = new ConcurrentHashMap<>();
        statsMap.put(mockBundleName(1), stats1);
        statsMap.put(mockBundleName(2), stats2);
        final LocalBrokerData localBrokerData = new LocalBrokerData();
        localBrokerData.update(new SystemResourceUsage(), statsMap);
        final Namespaces namespacesSpy1 = spy(pulsar1.getAdminClient().namespaces());
        doNothing().when(namespacesSpy1).unloadNamespaceBundle(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        setField(pulsar1.getAdminClient(), "namespaces", namespacesSpy1);
        ModularLoadManagerImpl primaryLoadManagerSpy = spy(primaryLoadManager);

        pulsar1.getConfiguration().setLoadBalancerEnabled(true);
        final LoadData loadData = (LoadData) getField(primaryLoadManagerSpy, "loadData");

        final Map<String, BrokerData> brokerDataMap = loadData.getBrokerData();
        final BrokerData brokerDataSpy1 = spy(brokerDataMap.get(primaryBrokerId));
        when(brokerDataSpy1.getLocalData()).thenReturn(localBrokerData);
        brokerDataMap.put(primaryBrokerId, brokerDataSpy1);
        // Need to update all the bundle data for the shredder to see the spy.
        primaryLoadManagerSpy.handleDataNotification(new Notification(NotificationType.Created, LoadManager.LOADBALANCE_BROKERS_ROOT + "/broker:8080"));

        sleep(100);

        // Most expensive bundle will be unloaded.
        localBrokerData.setCpu(new ResourceUsage(90, 100));
        primaryLoadManagerSpy.doLoadShedding();
        assertEquals(getField(primaryLoadManagerSpy, "unloadBundleCount"), 1l);
        assertEquals(getField(primaryLoadManagerSpy, "unloadBrokerCount"), 1l);

        // Now less expensive bundle will be unloaded
        primaryLoadManagerSpy.doLoadShedding();
        assertEquals(getField(primaryLoadManagerSpy, "unloadBundleCount"), 2l);
        assertEquals(getField(primaryLoadManagerSpy, "unloadBrokerCount"), 2l);

        // Now both are in grace period: neither should be unloaded.
        primaryLoadManagerSpy.doLoadShedding();
        assertEquals(getField(primaryLoadManagerSpy, "unloadBundleCount"), 2l);
        assertEquals(getField(primaryLoadManagerSpy, "unloadBrokerCount"), 2l);

        // clear the recently unloaded bundles to avoid the grace period
        loadData.getRecentlyUnloadedBundles().clear();

        // Test bundle to be unloaded is filtered.
        doAnswer(invocation -> {
            // return empty broker to avoid unloading the bundle
            return Optional.empty();
        }).when(primaryLoadManagerSpy).selectBroker(any());
        primaryLoadManagerSpy.doLoadShedding();

        assertEquals(getField(primaryLoadManagerSpy, "unloadBundleCount"), 2l);
        assertEquals(getField(primaryLoadManagerSpy, "unloadBrokerCount"), 2l);
    }

    // Test that ModularLoadManagerImpl will determine that writing local data to ZooKeeper is necessary if certain
    // metrics change by a percentage threshold.

    @Test
    public void testNeedBrokerDataUpdate() throws Exception {
        final LocalBrokerData lastData = new LocalBrokerData();
        final LocalBrokerData currentData = new LocalBrokerData();
        final ServiceConfiguration conf = pulsar1.getConfiguration();
        // Set this manually in case the default changes.
        conf.setLoadBalancerReportUpdateThresholdPercentage(5);
        // Easier to test using an uninitialized ModularLoadManagerImpl.
        final ModularLoadManagerImpl loadManager = new ModularLoadManagerImpl();
        setField(loadManager, "lastData", lastData);
        setField(loadManager, "localData", currentData);
        setField(loadManager, "conf", conf);
        Supplier<Boolean> needUpdate = () -> {
            try {
                return (Boolean) invokeSimpleMethod(loadManager, "needBrokerDataUpdate");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        lastData.setMsgRateIn(100);
        currentData.setMsgRateIn(104);
        // 4% difference: shouldn't trigger an update.
        assert (!needUpdate.get());
        currentData.setMsgRateIn(105.1);
        // 5% difference: should trigger an update (exactly 5% is flaky due to precision).
        assert (needUpdate.get());

        // Do similar tests for lower values.
        currentData.setMsgRateIn(94);
        assert (needUpdate.get());
        currentData.setMsgRateIn(95.1);
        assert (!needUpdate.get());

        // 0 to non-zero should always trigger an update.
        lastData.setMsgRateIn(0);
        currentData.setMsgRateIn(1e-8);
        assert (needUpdate.get());

        // non-zero to zero should trigger an update as long as the threshold is less than 100.
        lastData.setMsgRateIn(1e-8);
        currentData.setMsgRateIn(0);
        assert (needUpdate.get());

        // zero to zero should never trigger an update.
        currentData.setMsgRateIn(0);
        lastData.setMsgRateIn(0);
        assert (!needUpdate.get());

        // Minimally test other absolute values to ensure they are included.
        lastData.setCpu(new ResourceUsage(100, 1000));
        currentData.setCpu(new ResourceUsage(106, 1000));
        assert (!needUpdate.get());

        // Minimally test other absolute values to ensure they are included.
        lastData.setCpu(new ResourceUsage(100, 1000));
        currentData.setCpu(new ResourceUsage(206, 1000));
        assert (needUpdate.get());

        // set the resource weight of cpu to 0, so that it should not trigger an update
        conf.setLoadBalancerCPUResourceWeight(0);
        assert (!needUpdate.get());

        lastData.setCpu(new ResourceUsage());
        currentData.setCpu(new ResourceUsage());

        lastData.setMsgThroughputIn(100);
        currentData.setMsgThroughputIn(106);
        assert (needUpdate.get());
        currentData.setMsgThroughputIn(100);

        lastData.setNumBundles(100);
        currentData.setNumBundles(106);
        assert (needUpdate.get());

        currentData.setNumBundles(100);
        assert (!needUpdate.get());
    }

    /**
     * It verifies that deletion of broker-znode on broker-stop will invalidate availableBrokerCache list
     */
    @Test
    public void testBrokerStopCacheUpdate() throws Exception {
        ModularLoadManagerWrapper loadManagerWrapper = (ModularLoadManagerWrapper) pulsar1.getLoadManager().get();
        ModularLoadManagerImpl lm = (ModularLoadManagerImpl) loadManagerWrapper.getLoadManager();
        assertEquals(lm.getAvailableBrokers().size(), 2);

        pulsar2.close();

        Awaitility.await().untilAsserted(() -> assertEquals(lm.getAvailableBrokers().size(), 1));
    }

    /**
     * It verifies namespace-isolation policies with primary and secondary brokers.
     *
     * usecase:
     *
     * <pre>
     *  1. Namespace: primary=broker1, secondary=broker2, shared=broker3, min_limit = 1
     *     a. available-brokers: broker1, broker2, broker3 => result: broker1
     *     b. available-brokers: broker2, broker3          => result: broker2
     *     c. available-brokers: broker3                   => result: NULL
     *  2. Namespace: primary=broker1, secondary=broker2, shared=broker3, min_limit = 2
     *     a. available-brokers: broker1, broker2, broker3 => result: broker1, broker2
     *     b. available-brokers: broker2, broker3          => result: broker2
     *     c. available-brokers: broker3                   => result: NULL
     * </pre>
     */
    @Test
    public void testNamespaceIsolationPoliciesForPrimaryAndSecondaryBrokers() throws Exception {

        final String tenant = "my-property";
        final String cluster = "use";
        final String namespace = "my-ns";
        String broker1Host = pulsar1.getAdvertisedAddress() + "0";
        final String broker1Address = broker1Host + ":8080";
        String broker2Host = pulsar2.getAdvertisedAddress() + "1";
        final String broker2Address = broker2Host + ":8080";
        final String sharedBroker = "broker3:8080";
        admin1.clusters().createCluster(cluster, ClusterData.builder().serviceUrl(pulsar1.getWebServiceAddress()).build());
        admin1.tenants().createTenant(tenant,
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet(cluster)));
        admin1.namespaces().createNamespace(tenant + "/" + cluster + "/" + namespace);

        // set a new policy
        String newPolicyJsonTemplate = "{\"namespaces\":[\"%s/%s/%s.*\"],\"primary\":[\"%s\"],"
                + "\"secondary\":[\"%s\"],\"auto_failover_policy\":{\"policy_type\":\"min_available\",\"parameters\":{\"min_limit\":%s,\"usage_threshold\":80}}}";
        String newPolicyJson = String.format(newPolicyJsonTemplate, tenant, cluster, namespace, broker1Host,
                broker2Host, 1);
        String newPolicyName = "my-ns-isolation-policies";
        ObjectMapper jsonMapper = ObjectMapperFactory.create();
        NamespaceIsolationDataImpl nsPolicyData = jsonMapper.readValue(newPolicyJson.getBytes(),
                NamespaceIsolationDataImpl.class);
        admin1.clusters().createNamespaceIsolationPolicy("use", newPolicyName, nsPolicyData);

        SimpleResourceAllocationPolicies simpleResourceAllocationPolicies = new SimpleResourceAllocationPolicies(
                pulsar1);
        ServiceUnitId serviceUnit = LoadBalancerTestingUtils.makeBundles(nsFactory, tenant, cluster, namespace, 1)[0];
        BrokerTopicLoadingPredicate brokerTopicLoadingPredicate = new BrokerTopicLoadingPredicate() {
            @Override
            public boolean isEnablePersistentTopics(String brokerId) {
                return true;
            }

            @Override
            public boolean isEnableNonPersistentTopics(String brokerId) {
                return true;
            }
        };

        // (1) now we have isolation policy : primary=broker1, secondary=broker2, minLimit=1

        // test1: shared=1, primary=1, secondary=1 => It should return 1 primary broker only
        Set<String> brokerCandidateCache = new HashSet<>();
        Set<String> availableBrokers = Sets.newHashSet(sharedBroker, broker1Address, broker2Address);
        LoadManagerShared.applyNamespacePolicies(serviceUnit, simpleResourceAllocationPolicies, brokerCandidateCache,
                availableBrokers, brokerTopicLoadingPredicate);
        assertEquals(brokerCandidateCache.size(), 1);
        assertTrue(brokerCandidateCache.contains(broker1Address));

        // test2: shared=1, primary=0, secondary=1 => It should return 1 secondary broker only
        brokerCandidateCache = new HashSet<>();
        availableBrokers = Sets.newHashSet(sharedBroker, broker2Address);
        LoadManagerShared.applyNamespacePolicies(serviceUnit, simpleResourceAllocationPolicies, brokerCandidateCache,
                availableBrokers, brokerTopicLoadingPredicate);
        assertEquals(brokerCandidateCache.size(), 1);
        assertTrue(brokerCandidateCache.contains(broker2Address));

        // test3: shared=1, primary=0, secondary=0 => It should return 0 broker
        brokerCandidateCache = new HashSet<>();
        availableBrokers = Sets.newHashSet(sharedBroker);
        LoadManagerShared.applyNamespacePolicies(serviceUnit, simpleResourceAllocationPolicies, brokerCandidateCache,
                availableBrokers, brokerTopicLoadingPredicate);
        assertEquals(brokerCandidateCache.size(), 0);

        // (2) now we will have isolation policy : primary=broker1, secondary=broker2, minLimit=2

        newPolicyJson = String.format(newPolicyJsonTemplate, tenant, cluster, namespace, broker1Host,
                broker2Host, 2);
        nsPolicyData = jsonMapper.readValue(newPolicyJson.getBytes(), NamespaceIsolationDataImpl.class);
        admin1.clusters().createNamespaceIsolationPolicy("use", newPolicyName, nsPolicyData);

        // test1: shared=1, primary=1, secondary=1 => It should return primary + secondary
        brokerCandidateCache = new HashSet<>();
        availableBrokers = Sets.newHashSet(sharedBroker, broker1Address, broker2Address);
        LoadManagerShared.applyNamespacePolicies(serviceUnit, simpleResourceAllocationPolicies, brokerCandidateCache,
                availableBrokers, brokerTopicLoadingPredicate);
        assertEquals(brokerCandidateCache.size(), 2);
        assertTrue(brokerCandidateCache.contains(broker1Address));
        assertTrue(brokerCandidateCache.contains(broker2Address));

        // test2: shared=1, secondary=1 => It should return secondary
        brokerCandidateCache = new HashSet<>();
        availableBrokers = Sets.newHashSet(sharedBroker, broker2Address);
        LoadManagerShared.applyNamespacePolicies(serviceUnit, simpleResourceAllocationPolicies, brokerCandidateCache,
                availableBrokers, brokerTopicLoadingPredicate);
        assertEquals(brokerCandidateCache.size(), 1);
        assertTrue(brokerCandidateCache.contains(broker2Address));

        // test3: shared=1, => It should return 0 broker
        brokerCandidateCache = new HashSet<>();
        availableBrokers = Sets.newHashSet(sharedBroker);
        LoadManagerShared.applyNamespacePolicies(serviceUnit, simpleResourceAllocationPolicies, brokerCandidateCache,
                availableBrokers, brokerTopicLoadingPredicate);
        assertEquals(brokerCandidateCache.size(), 0);

    }

    @Test
    public void testLoadSheddingWithNamespaceIsolationPolicies() throws Exception {

        final String cluster = "use";
        final String tenant = "my-tenant";
        final String namespace = "my-tenant/use/my-ns";
        final String bundle = "0x00000000_0xffffffff";
        final String brokerHost = pulsar1.getAdvertisedAddress();
        final String brokerAddress = brokerHost  + ":8080";
        final String broker1Host = pulsar1.getAdvertisedAddress() + "1";
        final String broker1Address = broker1Host + ":8080";

        admin1.clusters().createCluster(cluster, ClusterData.builder().serviceUrl(pulsar1.getWebServiceAddress()).build());
        admin1.tenants().createTenant(tenant,
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet(cluster)));
        admin1.namespaces().createNamespace(namespace);

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsar1.getWebServiceAddress()).build();
        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://" + namespace + "/my-topic1")
                .create();
        ModularLoadManagerImpl loadManager = (ModularLoadManagerImpl) ((ModularLoadManagerWrapper) pulsar1
                .getLoadManager().get()).getLoadManager();
        pulsar1.getBrokerService().updateRates();
        loadManager.updateAll();

        // test1: no isolation policy
        assertTrue(loadManager.shouldNamespacePoliciesUnload(namespace, bundle, primaryBrokerId));

        // test2: as isolation policy, there are not another broker to load the bundle.
        String newPolicyJsonTemplate = "{\"namespaces\":[\"%s.*\"],\"primary\":[\"%s\"],"
                + "\"secondary\":[\"%s\"],\"auto_failover_policy\":{\"policy_type\":\"min_available\",\"parameters\":{\"min_limit\":%s,\"usage_threshold\":80}}}";

        String newPolicyJson = String.format(newPolicyJsonTemplate, namespace, broker1Host, broker1Host, 1);
        String newPolicyName = "my-ns-isolation-policies";
        ObjectMapper jsonMapper = ObjectMapperFactory.create();
        NamespaceIsolationDataImpl nsPolicyData = jsonMapper.readValue(newPolicyJson.getBytes(),
                NamespaceIsolationDataImpl.class);
        admin1.clusters().createNamespaceIsolationPolicy(cluster, newPolicyName, nsPolicyData);
        assertFalse(loadManager.shouldNamespacePoliciesUnload(namespace, bundle, broker1Address));

        // test3: as isolation policy, there are another can load the bundle.
        String newPolicyJson1 = String.format(newPolicyJsonTemplate, namespace, brokerHost, brokerHost, 1);
        NamespaceIsolationDataImpl nsPolicyData1 = jsonMapper.readValue(newPolicyJson1.getBytes(),
                NamespaceIsolationDataImpl.class);
        admin1.clusters().updateNamespaceIsolationPolicy(cluster, newPolicyName, nsPolicyData1);
        assertTrue(loadManager.shouldNamespacePoliciesUnload(namespace, bundle, primaryBrokerId));

        producer.close();
    }

    /**
     * It verifies that pulsar-service fails if load-manager tries to create ephemeral znode for broker which is already
     * created by other zk-session-id.
     *
     * @throws Exception
     */
    @Test
    public void testOwnBrokerZnodeByMultipleBroker() throws Exception {

        ServiceConfiguration config = new ServiceConfiguration();
        config.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        config.setClusterName("use");
        config.setWebServicePort(Optional.of(PortManager.nextLockedFreePort()));
        config.setMetadataStoreUrl("zk:127.0.0.1:" + bkEnsemble.getZookeeperPort());
        config.setBrokerShutdownTimeoutMs(0L);
        config.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        config.setBrokerServicePort(Optional.of(0));
        PulsarService pulsar = new PulsarService(config);
        // create znode using different zk-session
        final String brokerZnode = LoadManager.LOADBALANCE_BROKERS_ROOT + "/" + pulsar.getAdvertisedAddress() + ":"
                + config.getWebServicePort().get();
        pulsar1.getLocalMetadataStore()
                .put(brokerZnode, new byte[0], Optional.empty(), EnumSet.of(CreateOption.Ephemeral)).join();
        try {
            pulsar.start();
            fail("should have failed");
        } catch (PulsarServerException e) {
            //Ok.
        }

        pulsar.close();
    }

    @Test
    public void testRemoveDeadBrokerTimeAverageData() throws Exception {
        ModularLoadManagerWrapper loadManagerWrapper = (ModularLoadManagerWrapper) pulsar1.getLoadManager().get();
        ModularLoadManagerImpl lm = (ModularLoadManagerImpl) loadManagerWrapper.getLoadManager();
        assertEquals(lm.getAvailableBrokers().size(), 2);

        pulsar2.close();

        Awaitility.await().untilAsserted(() -> assertEquals(lm.getAvailableBrokers().size(), 1));
        lm.updateAll();

        List<String> data =  pulsar1.getLocalMetadataStore()
                .getMetadataCache(TimeAverageBrokerData.class)
                .getChildren(BROKER_TIME_AVERAGE_BASE_PATH)
                .join();

        Awaitility.await().untilAsserted(() -> assertTrue(pulsar1.getLeaderElectionService().isLeader()));
        assertEquals(data.size(), 1);
    }

    @DataProvider(name = "isV1")
    public Object[][] isV1() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "isV1")
    public void testBundleDataDefaultValue(boolean isV1) throws Exception {
        final String cluster = "use";
        final String tenant = "my-tenant";
        final String namespace = "my-ns";
        NamespaceName ns = isV1 ? NamespaceName.get(tenant, cluster, namespace) : NamespaceName.get(tenant, namespace);
        admin1.clusters().createCluster(cluster, ClusterData.builder().serviceUrl(pulsar1.getWebServiceAddress()).build());
        admin1.tenants().createTenant(tenant,
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet(cluster)));
        admin1.namespaces().createNamespace(ns.toString(), 16);

        // set resourceQuota to the first bundle range.
        BundlesData bundlesData = admin1.namespaces().getBundles(ns.toString());
        NamespaceBundle namespaceBundle = nsFactory.getBundle(ns,
                Range.range(Long.decode(bundlesData.getBoundaries().get(0)), BoundType.CLOSED, Long.decode(bundlesData.getBoundaries().get(1)),
                        BoundType.OPEN));
        ResourceQuota quota = new ResourceQuota();
        quota.setMsgRateIn(1024.1);
        quota.setMsgRateOut(1024.2);
        quota.setBandwidthIn(1024.3);
        quota.setBandwidthOut(1024.4);
        quota.setMemory(1024.0);
        admin1.resourceQuotas().setNamespaceBundleResourceQuota(ns.toString(), namespaceBundle.getBundleRange(), quota);

        ModularLoadManagerWrapper loadManagerWrapper = (ModularLoadManagerWrapper) pulsar1.getLoadManager().get();
        ModularLoadManagerImpl lm = (ModularLoadManagerImpl) loadManagerWrapper.getLoadManager();

        // get the bundleData of the first bundle range.
        // The default value of the bundleData be the same as resourceQuota because the resourceQuota is present.
        BundleData defaultBundleData = lm.getBundleDataOrDefault(namespaceBundle.toString());

        TimeAverageMessageData shortTermData = defaultBundleData.getShortTermData();
        TimeAverageMessageData longTermData = defaultBundleData.getLongTermData();
        assertEquals(shortTermData.getMsgRateIn(), 1024.1);
        assertEquals(shortTermData.getMsgRateOut(), 1024.2);
        assertEquals(shortTermData.getMsgThroughputIn(), 1024.3);
        assertEquals(shortTermData.getMsgThroughputOut(), 1024.4);

        assertEquals(longTermData.getMsgRateIn(), 1024.1);
        assertEquals(longTermData.getMsgRateOut(), 1024.2);
        assertEquals(longTermData.getMsgThroughputIn(), 1024.3);
        assertEquals(longTermData.getMsgThroughputOut(), 1024.4);
    }


    @Test
    public void testRemoveNonExistBundleData()
            throws PulsarAdminException, InterruptedException,
            PulsarClientException, PulsarServerException, NoSuchFieldException, IllegalAccessException {
        final String cluster = "use";
        final String tenant = "my-tenant";
        final String namespace = "remove-non-exist-bundle-data-ns";
        final String topicName = tenant + "/" + namespace + "/" + "topic";
        int bundleNumbers = 8;

        admin1.clusters().createCluster(cluster, ClusterData.builder().serviceUrl(pulsar1.getWebServiceAddress()).build());
        admin1.tenants().createTenant(tenant,
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet(cluster)));
        admin1.namespaces().createNamespace(tenant + "/" + namespace, bundleNumbers);

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsar1.getBrokerServiceUrl()).build();

        // create a lot of topic to fully distributed among bundles.
        for (int i = 0; i < 10; i++) {
            String topicNameI = topicName + i;
            admin1.topics().createPartitionedTopic(topicNameI, 20);
            // trigger bundle assignment

            pulsarClient.newConsumer().topic(topicNameI)
                    .subscriptionName("my-subscriber-name2").subscribe();
        }

        ModularLoadManagerWrapper loadManagerWrapper = (ModularLoadManagerWrapper) pulsar1.getLoadManager().get();
        ModularLoadManagerImpl lm1 = (ModularLoadManagerImpl) loadManagerWrapper.getLoadManager();
        ModularLoadManagerWrapper loadManager2 = (ModularLoadManagerWrapper) pulsar2.getLoadManager().get();
        ModularLoadManagerImpl lm2 = (ModularLoadManagerImpl) loadManager2.getLoadManager();

        Field executors = lm1.getClass().getDeclaredField("executors");
        executors.setAccessible(true);
        ExecutorService executorService = (ExecutorService) executors.get(lm1);

        assertEquals(lm1.getAvailableBrokers().size(), 2);

        pulsar1.getBrokerService().updateRates();
        pulsar2.getBrokerService().updateRates();

        lm1.writeBrokerDataOnZooKeeper(true);
        lm2.writeBrokerDataOnZooKeeper(true);

        // wait for metadata store notification finish
        CountDownLatch latch = new CountDownLatch(1);
        executorService.submit(latch::countDown);
        latch.await();

        loadManagerWrapper.writeResourceQuotasToZooKeeper();

        MetadataCache<BundleData> bundlesCache = pulsar1.getLocalMetadataStore().getMetadataCache(BundleData.class);

        // trigger bundle split
        String topicToFindBundle = topicName + 0;
        NamespaceBundle bundleWillBeSplit = pulsar1.getNamespaceService().getBundle(TopicName.get(topicToFindBundle));

        final Optional<ResourceUnit> leastLoaded = loadManagerWrapper.getLeastLoaded(bundleWillBeSplit);
        assertFalse(leastLoaded.isEmpty());

        String bundleDataPath = BUNDLE_DATA_BASE_PATH + "/" + tenant + "/" + namespace;
        CompletableFuture<List<String>> children = bundlesCache.getChildren(bundleDataPath);
        List<String> bundles = children.join();
        assertTrue(bundles.contains(bundleWillBeSplit.getBundleRange()));

        // after updateAll no namespace bundle data is deleted from metadata store.
        lm1.updateAll();

        children = bundlesCache.getChildren(bundleDataPath);
        bundles = children.join();
        assertFalse(bundles.isEmpty());
        assertEquals(bundleNumbers, bundles.size());

        NamespaceName namespaceName = NamespaceName.get(tenant, namespace);
        pulsar1.getAdminClient().namespaces().splitNamespaceBundle(tenant + "/" + namespace,
                bundleWillBeSplit.getBundleRange(),
                false, NamespaceBundleSplitAlgorithm.RANGE_EQUALLY_DIVIDE_NAME);

        NamespaceBundles allBundlesAfterSplit =
                pulsar1.getNamespaceService().getNamespaceBundleFactory()
                        .getBundles(namespaceName);

        assertFalse(allBundlesAfterSplit.getBundles().contains(bundleWillBeSplit));

        // the bundle data should be deleted

        pulsar1.getBrokerService().updateRates();
        pulsar2.getBrokerService().updateRates();

        lm1.writeBrokerDataOnZooKeeper(true);
        lm2.writeBrokerDataOnZooKeeper(true);

        latch = new CountDownLatch(1);
        // wait for metadata store notification finish
        CountDownLatch finalLatch = latch;
        executorService.submit(finalLatch::countDown);
        latch.await();

        loadManagerWrapper.writeResourceQuotasToZooKeeper();

        lm1.updateAll();

        log.info("update all triggered.");

        // check bundle data should be deleted from metadata store.

        CompletableFuture<List<String>> childrenAfterSplit = bundlesCache.getChildren(bundleDataPath);
        List<String> bundlesAfterSplit = childrenAfterSplit.join();

        assertFalse(bundlesAfterSplit.contains(bundleWillBeSplit.getBundleRange()));
    }

}
