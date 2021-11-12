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
package org.apache.pulsar.broker.loadbalance;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TimeAverageMessageData;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared.BrokerTopicLoadingPredicate;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerWrapper;
import org.apache.pulsar.broker.loadbalance.impl.SimpleResourceAllocationPolicies;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class ModularLoadManagerImplTest {
    private LocalBookkeeperEnsemble bkEnsemble;

    private URL url1;
    private PulsarService pulsar1;
    private PulsarAdmin admin1;

    private URL url2;
    private PulsarService pulsar2;
    private PulsarAdmin admin2;

    private String primaryHost;
    private String secondaryHost;

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
        config1.setClusterName("use");
        config1.setWebServicePort(Optional.of(0));
        config1.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());

        config1.setAdvertisedAddress("localhost");
        config1.setBrokerShutdownTimeoutMs(0L);
        config1.setBrokerServicePort(Optional.of(0));
        config1.setBrokerServicePortTls(Optional.of(0));
        config1.setWebServicePortTls(Optional.of(0));
        pulsar1 = new PulsarService(config1);
        pulsar1.start();

        primaryHost = String.format("%s:%d", "localhost", pulsar1.getListenPortHTTP().get());
        url1 = new URL(pulsar1.getWebServiceAddress());
        admin1 = PulsarAdmin.builder().serviceHttpUrl(url1.toString()).build();

        // Start broker 2
        ServiceConfiguration config2 = new ServiceConfiguration();
        config2.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        config2.setClusterName("use");
        config2.setWebServicePort(Optional.of(0));
        config2.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config2.setAdvertisedAddress("localhost");
        config2.setBrokerShutdownTimeoutMs(0L);
        config2.setBrokerServicePort(Optional.of(0));
        config2.setBrokerServicePortTls(Optional.of(0));
        config2.setWebServicePortTls(Optional.of(0));
        pulsar2 = new PulsarService(config2);
        pulsar2.start();

        secondaryHost = String.format("%s:%d", "localhost", pulsar2.getListenPortHTTP().get());
        url2 = new URL(pulsar2.getWebServiceAddress());
        admin2 = PulsarAdmin.builder().serviceHttpUrl(url2.toString()).build();

        primaryLoadManager = (ModularLoadManagerImpl) getField(pulsar1.getLoadManager().get(), "loadManager");
        secondaryLoadManager = (ModularLoadManagerImpl) getField(pulsar2.getLoadManager().get(), "loadManager");
        nsFactory = new NamespaceBundleFactory(pulsar1, Hashing.crc32());
        Thread.sleep(100);
    }

    @AfterMethod(alwaysRun = true)
    void shutdown() throws Exception {
        log.info("--- Shutting down ---");
        executor.shutdownNow();

        admin1.close();
        admin2.close();

        pulsar2.close();
        pulsar1.close();

        bkEnsemble.stop();
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
            if (broker.equals(primaryHost)) {
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
        Awaitility.await().untilAsserted(() -> assertFalse(loadData.getBrokerData().containsKey(secondaryHost)));

        // Try 5 more selections, ensure they all go to the first broker.
        for (int i = 2; i < 7; ++i) {
            final ServiceUnitId serviceUnit = makeBundle(Integer.toString(i));
            assertEquals(primaryLoadManager.selectBrokerForAssignment(serviceUnit), primaryHost);
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
        final String firstBundleDataPath = String.format("%s/%s", ModularLoadManagerImpl.BUNDLE_DATA_PATH, bundles[0]);
        // Write long message rate for first bundle to ensure that even bundle distribution is not a coincidence of
        // balancing by message rate. If we were balancing by message rate, one of the brokers should only have this
        // one bundle.
        pulsar1.getLocalMetadataStore().getMetadataCache(BundleData.class).create(firstBundleDataPath, bundleData).join();
        for (final NamespaceBundle bundle : bundles) {
            if (primaryLoadManager.selectBrokerForAssignment(bundle).equals(primaryHost)) {
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
        final String firstBundleDataPath = String.format("%s/%s", ModularLoadManagerImpl.BUNDLE_DATA_PATH, bundles[0]);
        pulsar1.getLocalMetadataStore().getMetadataCache(BundleData.class).create(firstBundleDataPath, bundleData).join();
        String maxTopicOwnedBroker = primaryLoadManager.selectBrokerForAssignment(bundles[0]).get();

        for (int i = 1; i < totalBundles; i++) {
            assertNotEquals(primaryLoadManager.selectBrokerForAssignment(bundles[i]), maxTopicOwnedBroker);
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
        }).when(namespacesSpy1).unloadNamespaceBundle(Mockito.anyString(), Mockito.anyString());
        setField(pulsar1.getAdminClient(), "namespaces", namespacesSpy1);
        pulsar1.getConfiguration().setLoadBalancerEnabled(true);
        final LoadData loadData = (LoadData) getField(primaryLoadManager, "loadData");
        final Map<String, BrokerData> brokerDataMap = loadData.getBrokerData();
        final BrokerData brokerDataSpy1 = spy(brokerDataMap.get(primaryHost));
        when(brokerDataSpy1.getLocalData()).thenReturn(localBrokerData);
        brokerDataMap.put(primaryHost, brokerDataSpy1);
        // Need to update all the bundle data for the shredder to see the spy.
        primaryLoadManager.handleDataNotification(new Notification(NotificationType.Created, LoadManager.LOADBALANCE_BROKERS_ROOT + "/broker:8080"));

        Thread.sleep(100);
        localBrokerData.setCpu(new ResourceUsage(80, 100));
        primaryLoadManager.doLoadShedding();

        // 80% is below overload threshold: verify nothing is unloaded.
        verify(namespacesSpy1, Mockito.times(0)).unloadNamespaceBundle(Mockito.anyString(), Mockito.anyString());

        localBrokerData.getCpu().usage = 90;
        primaryLoadManager.doLoadShedding();
        // Most expensive bundle will be unloaded.
        verify(namespacesSpy1, Mockito.times(1)).unloadNamespaceBundle(Mockito.anyString(), Mockito.anyString());
        assertEquals(bundleReference.get(), mockBundleName(2));

        primaryLoadManager.doLoadShedding();
        // Now less expensive bundle will be unloaded (normally other bundle would move off and nothing would be
        // unloaded, but this is not the case due to the spy's behavior).
        verify(namespacesSpy1, Mockito.times(2)).unloadNamespaceBundle(Mockito.anyString(), Mockito.anyString());
        assertEquals(bundleReference.get(), mockBundleName(1));

        primaryLoadManager.doLoadShedding();
        // Now both are in grace period: neither should be unloaded.
        verify(namespacesSpy1, Mockito.times(2)).unloadNamespaceBundle(Mockito.anyString(), Mockito.anyString());
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
        lastData.getCpu().usage = 100;
        lastData.getCpu().limit = 1000;
        currentData.getCpu().usage = 106;
        currentData.getCpu().limit = 1000;
        assert (!needUpdate.get());

        // Minimally test other absolute values to ensure they are included.
        lastData.getCpu().usage = 100;
        lastData.getCpu().limit = 1000;
        currentData.getCpu().usage = 206;
        currentData.getCpu().limit = 1000;
        assert (needUpdate.get());

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
     *
     * @throws Exception
     */
    @Test
    public void testBrokerStopCacheUpdate() throws Exception {
        ModularLoadManagerWrapper loadManagerWrapper = (ModularLoadManagerWrapper) pulsar1.getLoadManager().get();
        ModularLoadManagerImpl lm = Whitebox.getInternalState(loadManagerWrapper, "loadManager");
        assertEquals(lm.getAvailableBrokers().size(), 2);

        pulsar2.close();

        Awaitility.await().untilAsserted(() -> {
            assertEquals(lm.getAvailableBrokers().size(), 1);
        });
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
     *
     * @throws Exception
     */
    @Test
    public void testNamespaceIsolationPoliciesForPrimaryAndSecondaryBrokers() throws Exception {

        final String tenant = "my-property";
        final String cluster = "use";
        final String namespace = "my-ns";
        final String broker1Address = pulsar1.getAdvertisedAddress() + "0";
        final String broker2Address = pulsar2.getAdvertisedAddress() + "1";
        final String sharedBroker = "broker3";
        admin1.clusters().createCluster(cluster, ClusterData.builder().serviceUrl("http://" + pulsar1.getAdvertisedAddress()).build());
        admin1.tenants().createTenant(tenant,
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet(cluster)));
        admin1.namespaces().createNamespace(tenant + "/" + cluster + "/" + namespace);

        // set a new policy
        String newPolicyJsonTemplate = "{\"namespaces\":[\"%s/%s/%s.*\"],\"primary\":[\"%s\"],"
                + "\"secondary\":[\"%s\"],\"auto_failover_policy\":{\"policy_type\":\"min_available\",\"parameters\":{\"min_limit\":%s,\"usage_threshold\":80}}}";
        String newPolicyJson = String.format(newPolicyJsonTemplate, tenant, cluster, namespace, broker1Address,
                broker2Address, 1);
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
            public boolean isEnablePersistentTopics(String brokerUrl) {
                return true;
            }

            @Override
            public boolean isEnableNonPersistentTopics(String brokerUrl) {
                return true;
            }
        };

        // (1) now we have isolation policy : primary=broker1, secondary=broker2, minLimit=1

        // test1: shared=1, primary=1, secondary=1 => It should return 1 primary broker only
        Set<String> brokerCandidateCache = Sets.newHashSet();
        Set<String> availableBrokers = Sets.newHashSet(sharedBroker, broker1Address, broker2Address);
        LoadManagerShared.applyNamespacePolicies(serviceUnit, simpleResourceAllocationPolicies, brokerCandidateCache,
                availableBrokers, brokerTopicLoadingPredicate);
        assertEquals(brokerCandidateCache.size(), 1);
        assertTrue(brokerCandidateCache.contains(broker1Address));

        // test2: shared=1, primary=0, secondary=1 => It should return 1 secondary broker only
        brokerCandidateCache = Sets.newHashSet();
        availableBrokers = Sets.newHashSet(sharedBroker, broker2Address);
        LoadManagerShared.applyNamespacePolicies(serviceUnit, simpleResourceAllocationPolicies, brokerCandidateCache,
                availableBrokers, brokerTopicLoadingPredicate);
        assertEquals(brokerCandidateCache.size(), 1);
        assertTrue(brokerCandidateCache.contains(broker2Address));

        // test3: shared=1, primary=0, secondary=0 => It should return 0 broker
        brokerCandidateCache = Sets.newHashSet();
        availableBrokers = Sets.newHashSet(sharedBroker);
        LoadManagerShared.applyNamespacePolicies(serviceUnit, simpleResourceAllocationPolicies, brokerCandidateCache,
                availableBrokers, brokerTopicLoadingPredicate);
        assertEquals(brokerCandidateCache.size(), 0);

        // (2) now we will have isolation policy : primary=broker1, secondary=broker2, minLimit=2

        newPolicyJson = String.format(newPolicyJsonTemplate, tenant, cluster, namespace, broker1Address,
                broker2Address, 2);
        nsPolicyData = jsonMapper.readValue(newPolicyJson.getBytes(), NamespaceIsolationDataImpl.class);
        admin1.clusters().createNamespaceIsolationPolicy("use", newPolicyName, nsPolicyData);

        // test1: shared=1, primary=1, secondary=1 => It should return primary + secondary
        brokerCandidateCache = Sets.newHashSet();
        availableBrokers = Sets.newHashSet(sharedBroker, broker1Address, broker2Address);
        LoadManagerShared.applyNamespacePolicies(serviceUnit, simpleResourceAllocationPolicies, brokerCandidateCache,
                availableBrokers, brokerTopicLoadingPredicate);
        assertEquals(brokerCandidateCache.size(), 2);
        assertTrue(brokerCandidateCache.contains(broker1Address));
        assertTrue(brokerCandidateCache.contains(broker2Address));

        // test2: shared=1, secondary=1 => It should return secondary
        brokerCandidateCache = Sets.newHashSet();
        availableBrokers = Sets.newHashSet(sharedBroker, broker2Address);
        LoadManagerShared.applyNamespacePolicies(serviceUnit, simpleResourceAllocationPolicies, brokerCandidateCache,
                availableBrokers, brokerTopicLoadingPredicate);
        assertEquals(brokerCandidateCache.size(), 1);
        assertTrue(brokerCandidateCache.contains(broker2Address));

        // test3: shared=1, => It should return 0 broker
        brokerCandidateCache = Sets.newHashSet();
        availableBrokers = Sets.newHashSet(sharedBroker);
        LoadManagerShared.applyNamespacePolicies(serviceUnit, simpleResourceAllocationPolicies, brokerCandidateCache,
                availableBrokers, brokerTopicLoadingPredicate);
        assertEquals(brokerCandidateCache.size(), 0);

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
        config.setWebServicePort(Optional.of(0));
        config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config.setBrokerShutdownTimeoutMs(0L);
        config.setBrokerServicePort(Optional.of(0));
        PulsarService pulsar = new PulsarService(config);
        // create znode using different zk-session
        final String brokerZnode = LoadManager.LOADBALANCE_BROKERS_ROOT + "/" + pulsar.getAdvertisedAddress() + ":"
                + config.getWebServicePort();
        pulsar1.getLocalMetadataStore().put(brokerZnode, new byte[0], Optional.empty(), EnumSet.of(CreateOption.Ephemeral)).join();
        try {
            pulsar.start();
        } catch (PulsarServerException e) {
            //Ok.
        }

        pulsar.close();
    }
}
