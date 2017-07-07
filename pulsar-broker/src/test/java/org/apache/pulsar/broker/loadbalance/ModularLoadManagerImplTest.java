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
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.bookkeeper.test.PortManager;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.LocalBrokerData;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TimeAverageMessageData;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerWrapper;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.hash.Hashing;

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

    private ExecutorService executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>());

    private final int ZOOKEEPER_PORT = PortManager.nextFreePort();
    private final int PRIMARY_BROKER_WEBSERVICE_PORT = PortManager.nextFreePort();
    private final int SECONDARY_BROKER_WEBSERVICE_PORT = PortManager.nextFreePort();
    private final int PRIMARY_BROKER_PORT = PortManager.nextFreePort();
    private final int SECONDARY_BROKER_PORT = PortManager.nextFreePort();
    private static final Logger log = LoggerFactory.getLogger(ModularLoadManagerImplTest.class);

    static {
        System.setProperty("test.basePort", "16100");
    }

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

        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, ZOOKEEPER_PORT, PortManager.nextFreePort());
        bkEnsemble.start();

        // Start broker 1
        ServiceConfiguration config1 = new ServiceConfiguration();
        config1.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        config1.setClusterName("use");
        config1.setWebServicePort(PRIMARY_BROKER_WEBSERVICE_PORT);
        config1.setZookeeperServers("127.0.0.1" + ":" + ZOOKEEPER_PORT);
        config1.setBrokerServicePort(PRIMARY_BROKER_PORT);
        pulsar1 = new PulsarService(config1);

        pulsar1.start();

        primaryHost = String.format("%s:%d", InetAddress.getLocalHost().getHostName(), PRIMARY_BROKER_WEBSERVICE_PORT);
        url1 = new URL("http://127.0.0.1" + ":" + PRIMARY_BROKER_WEBSERVICE_PORT);
        admin1 = new PulsarAdmin(url1, (Authentication) null);

        // Start broker 2
        ServiceConfiguration config2 = new ServiceConfiguration();
        config2.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        config2.setClusterName("use");
        config2.setWebServicePort(SECONDARY_BROKER_WEBSERVICE_PORT);
        config2.setZookeeperServers("127.0.0.1" + ":" + ZOOKEEPER_PORT);
        config2.setBrokerServicePort(SECONDARY_BROKER_PORT);
        pulsar2 = new PulsarService(config2);
        secondaryHost = String.format("%s:%d", InetAddress.getLocalHost().getHostName(),
                SECONDARY_BROKER_WEBSERVICE_PORT);

        pulsar2.start();

        url2 = new URL("http://127.0.0.1" + ":" + SECONDARY_BROKER_WEBSERVICE_PORT);
        admin2 = new PulsarAdmin(url2, (Authentication) null);

        primaryLoadManager = (ModularLoadManagerImpl) getField(pulsar1.getLoadManager().get(), "loadManager");
        secondaryLoadManager = (ModularLoadManagerImpl) getField(pulsar2.getLoadManager().get(), "loadManager");
        nsFactory = new NamespaceBundleFactory(pulsar1, Hashing.crc32());
        Thread.sleep(100);
    }

    @AfterMethod
    void shutdown() throws Exception {
        log.info("--- Shutting down ---");
        executor.shutdown();

        admin1.close();
        admin2.close();

        pulsar2.close();
        pulsar1.close();

        bkEnsemble.stop();
    }

    private NamespaceBundle makeBundle(final String property, final String cluster, final String namespace) {
        return nsFactory.getBundle(new NamespaceName(property, cluster, namespace),
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
            final String broker = primaryLoadManager.selectBrokerForAssignment(serviceUnit);
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

        // Give some time for the watch to fire.
        Thread.sleep(500);

        // Make sure the second broker is not in the internal map.
        assertFalse(loadData.getBrokerData().containsKey(secondaryHost));

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
        final String firstBundleDataPath = String.format("%s/%s", ModularLoadManagerImpl.BUNDLE_DATA_ZPATH, bundles[0]);
        // Write long message rate for first bundle to ensure that even bundle distribution is not a coincidence of
        // balancing by message rate. If we were balancing by message rate, one of the brokers should only have this
        // one bundle.
        ZkUtils.createFullPathOptimistic(pulsar1.getZkClient(), firstBundleDataPath, bundleData.getJsonBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
        // Need to update all the bundle data for the shedder to see the spy.
        primaryLoadManager.onUpdate(null, null, null);
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
        String secondaryBroker = pulsar2.getAdvertisedAddress() + ":" + SECONDARY_BROKER_WEBSERVICE_PORT;
        pulsar2.getLocalZkCache().getZooKeeper().delete(LoadManager.LOADBALANCE_BROKERS_ROOT + "/" + secondaryBroker,
                -1);
        Thread.sleep(100);
        ModularLoadManagerWrapper loadManagerWapper = (ModularLoadManagerWrapper) pulsar1.getLoadManager().get();
        Field loadMgrField = ModularLoadManagerWrapper.class.getDeclaredField("loadManager");
        loadMgrField.setAccessible(true);
        ModularLoadManagerImpl loadManager = (ModularLoadManagerImpl) loadMgrField.get(loadManagerWapper);
        Set<String> avaialbeBrokers = loadManager.getAvailableBrokers();
        assertEquals(avaialbeBrokers.size(), 1);
    }
}
