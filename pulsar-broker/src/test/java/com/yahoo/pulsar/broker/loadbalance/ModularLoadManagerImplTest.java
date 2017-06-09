/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.loadbalance;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.bookkeeper.test.PortManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.hash.Hashing;
import com.yahoo.pulsar.broker.LocalBrokerData;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import com.yahoo.pulsar.client.admin.PulsarAdmin;
import com.yahoo.pulsar.client.api.Authentication;
import com.yahoo.pulsar.common.naming.NamespaceBundle;
import com.yahoo.pulsar.common.naming.NamespaceBundleFactory;
import com.yahoo.pulsar.common.naming.NamespaceBundles;
import com.yahoo.pulsar.common.naming.NamespaceName;
import com.yahoo.pulsar.common.naming.ServiceUnitId;
import com.yahoo.pulsar.common.policies.data.loadbalancer.ResourceUsage;
import com.yahoo.pulsar.zookeeper.LocalBookkeeperEnsemble;

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

    @Test
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
        assert (foundFirst && foundSecond);

        // Now disable the secondary broker.
        secondaryLoadManager.disableBroker();

        LoadData loadData = (LoadData) getField(primaryLoadManager, "loadData");

        // Give some time for the watch to fire.
        Thread.sleep(500);

        // Make sure the second broker is not in the internal map.
        assert (!loadData.getBrokerData().containsKey(secondaryHost));

        // Try 5 more selections, ensure they all go to the first broker.
        for (int i = 2; i < 7; ++i) {
            final ServiceUnitId serviceUnit = makeBundle(Integer.toString(i));
            assert (primaryLoadManager.selectBrokerForAssignment(serviceUnit).equals(primaryHost));
        }
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
}
