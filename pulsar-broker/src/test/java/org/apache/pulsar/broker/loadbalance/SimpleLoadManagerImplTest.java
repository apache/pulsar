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

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SystemUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.impl.GenericBrokerHostUsageImpl;
import org.apache.pulsar.broker.loadbalance.impl.LinuxBrokerHostUsageImpl;
import org.apache.pulsar.broker.loadbalance.impl.PulsarLoadReportImpl;
import org.apache.pulsar.broker.loadbalance.impl.PulsarResourceDescription;
import org.apache.pulsar.broker.loadbalance.impl.ResourceAvailabilityRanker;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadCalculatorImpl;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.SimpleResourceUnit;
import org.apache.pulsar.client.admin.BrokerStats;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.ResourceQuota;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicies;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.policies.data.loadbalancer.BrokerUsage;
import org.apache.pulsar.policies.data.loadbalancer.JvmUsage;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUnitRanking;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class SimpleLoadManagerImplTest {
    LocalBookkeeperEnsemble bkEnsemble;

    URL url1;
    PulsarService pulsar1;
    PulsarAdmin admin1;

    URL url2;
    PulsarService pulsar2;
    PulsarAdmin admin2;

    BrokerStats brokerStatsClient1;
    BrokerStats brokerStatsClient2;

    String primaryHost;
    String secondaryHost;

    ExecutorService executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    @BeforeMethod
    void setup() throws Exception {

        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        // Start broker 1
        ServiceConfiguration config1 = spy(new ServiceConfiguration());
        config1.setClusterName("use");
        config1.setWebServicePort(Optional.of(0));
        config1.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config1.setBrokerShutdownTimeoutMs(0L);
        config1.setBrokerServicePort(Optional.of(0));
        config1.setLoadManagerClassName(SimpleLoadManagerImpl.class.getName());
        config1.setBrokerServicePortTls(Optional.of(0));
        config1.setWebServicePortTls(Optional.of(0));
        config1.setAdvertisedAddress("localhost");
        pulsar1 = new PulsarService(config1);
        pulsar1.start();

        url1 = new URL(pulsar1.getWebServiceAddress());
        admin1 = PulsarAdmin.builder().serviceHttpUrl(url1.toString()).build();
        brokerStatsClient1 = admin1.brokerStats();
        primaryHost = pulsar1.getWebServiceAddress();

        // Start broker 2
        ServiceConfiguration config2 = new ServiceConfiguration();
        config2.setClusterName("use");
        config2.setWebServicePort(Optional.of(0));
        config2.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config2.setBrokerShutdownTimeoutMs(0L);
        config2.setBrokerServicePort(Optional.of(0));
        config2.setLoadManagerClassName(SimpleLoadManagerImpl.class.getName());
        config2.setBrokerServicePortTls(Optional.of(0));
        config2.setWebServicePortTls(Optional.of(0));
        config2.setAdvertisedAddress("localhost");
        pulsar2 = new PulsarService(config2);
        pulsar2.start();

        url2 = new URL(pulsar2.getWebServiceAddress());
        admin2 = PulsarAdmin.builder().serviceHttpUrl(url2.toString()).build();
        brokerStatsClient2 = admin2.brokerStats();
        secondaryHost = pulsar2.getWebServiceAddress();
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

    private void createNamespacePolicies(PulsarService pulsar) throws Exception {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("min_limit", "1");
        parameters.put("usage_threshold", "100");

        NamespaceIsolationPolicies policies = new NamespaceIsolationPolicies();
        // set up policy that use this broker as primary
        NamespaceIsolationData policyData = NamespaceIsolationData.builder()
                .namespaces(Collections.singletonList("pulsar/use/primary-ns.*"))
                .primary(Collections.singletonList(pulsar1.getAdvertisedAddress() + "*"))
                .secondary(Collections.singletonList("prod2-broker([78]).messaging.usw.example.co.*"))
                .autoFailoverPolicy(AutoFailoverPolicyData.builder()
                        .policyType(AutoFailoverPolicyType.min_available)
                        .parameters(parameters)
                        .build())
                .build();
        policies.setPolicy("primaryBrokerPolicy", policyData);

        try {
            pulsar.getPulsarResources().getNamespaceResources().getIsolationPolicies().createIsolationData("use",
                    policies.getPolicies());
        } catch (BadVersionException e) {
            // isolation policy already exist
            pulsar.getPulsarResources().getNamespaceResources().getIsolationPolicies().setIsolationData("use",
                    data -> policies.getPolicies());
        }
    }

    @Test
    public void testBasicBrokerSelection() throws Exception {
        SimpleLoadManagerImpl loadManager = new SimpleLoadManagerImpl(pulsar1);
        PulsarResourceDescription rd = new PulsarResourceDescription();
        rd.put("memory", new ResourceUsage(1024, 4096));
        rd.put("cpu", new ResourceUsage(10, 100));
        rd.put("bandwidthIn", new ResourceUsage(250 * 1024, 1024 * 1024));
        rd.put("bandwidthOut", new ResourceUsage(550 * 1024, 1024 * 1024));

        ResourceUnit ru1 = new SimpleResourceUnit("http://prod2-broker7.messaging.usw.example.com:8080", rd);
        Set<ResourceUnit> rus = new HashSet<>();
        rus.add(ru1);
        LoadRanker lr = new ResourceAvailabilityRanker();
        AtomicReference<Map<Long, Set<ResourceUnit>>> sortedRankingsInstance = new AtomicReference<>(Maps.newTreeMap());
        sortedRankingsInstance.get().put(lr.getRank(rd), rus);

        Field sortedRankings = SimpleLoadManagerImpl.class.getDeclaredField("sortedRankings");
        sortedRankings.setAccessible(true);
        sortedRankings.set(loadManager, sortedRankingsInstance);

        Optional<ResourceUnit> res = loadManager
                .getLeastLoaded(NamespaceName.get("pulsar/use/primary-ns.10"));
        // broker is not active so found should be null
        assertEquals(res, Optional.empty(), "found a broker when expected none to be found");

    }

    private void setObjectField(Class<?> objClass, Object objInstance, String fieldName, Object newValue)
            throws Exception {
        Field field = objClass.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(objInstance, newValue);
    }

    @Test
    public void testPrimary() throws Exception {
        createNamespacePolicies(pulsar1);
        SimpleLoadManagerImpl loadManager = new SimpleLoadManagerImpl(pulsar1);
        PulsarResourceDescription rd = new PulsarResourceDescription();
        rd.put("memory", new ResourceUsage(1024, 4096));
        rd.put("cpu", new ResourceUsage(10, 100));
        rd.put("bandwidthIn", new ResourceUsage(250 * 1024, 1024 * 1024));
        rd.put("bandwidthOut", new ResourceUsage(550 * 1024, 1024 * 1024));

        ResourceUnit ru1 = new SimpleResourceUnit(
                "http://" + pulsar1.getAdvertisedAddress() + ":" + pulsar1.getConfiguration().getWebServicePort().get(), rd);
        Set<ResourceUnit> rus = new HashSet<>();
        rus.add(ru1);
        LoadRanker lr = new ResourceAvailabilityRanker();

        // inject the load report and rankings
        Map<ResourceUnit, org.apache.pulsar.policies.data.loadbalancer.LoadReport> loadReports = new HashMap<>();
        org.apache.pulsar.policies.data.loadbalancer.LoadReport loadReport = new org.apache.pulsar.policies.data.loadbalancer.LoadReport();
        loadReport.setSystemResourceUsage(new SystemResourceUsage());
        loadReports.put(ru1, loadReport);
        setObjectField(SimpleLoadManagerImpl.class, loadManager, "currentLoadReports", loadReports);

        ResourceUnitRanking ranking = new ResourceUnitRanking(loadReport.getSystemResourceUsage(),
                new HashSet<String>(), new ResourceQuota(), new HashSet<String>(), new ResourceQuota());
        Map<ResourceUnit, ResourceUnitRanking> rankings = new HashMap<>();
        rankings.put(ru1, ranking);
        setObjectField(SimpleLoadManagerImpl.class, loadManager, "resourceUnitRankings", rankings);

        AtomicReference<Map<Long, Set<ResourceUnit>>> sortedRankingsInstance = new AtomicReference<>(Maps.newTreeMap());
        sortedRankingsInstance.get().put(lr.getRank(rd), rus);
        setObjectField(SimpleLoadManagerImpl.class, loadManager, "sortedRankings", sortedRankingsInstance);

        ResourceUnit found = loadManager
                .getLeastLoaded(NamespaceName.get("pulsar/use/primary-ns.10")).get();
        // broker is not active so found should be null
        assertNotEquals(found, null, "did not find a broker when expected one to be found");

    }

    @Test(enabled = false)
    public void testPrimarySecondary() throws Exception {
        createNamespacePolicies(pulsar1);
        SimpleLoadManagerImpl loadManager = new SimpleLoadManagerImpl(pulsar1);

        PulsarResourceDescription rd = new PulsarResourceDescription();
        rd.put("memory", new ResourceUsage(1024, 4096));
        rd.put("cpu", new ResourceUsage(10, 100));
        rd.put("bandwidthIn", new ResourceUsage(250 * 1024, 1024 * 1024));
        rd.put("bandwidthOut", new ResourceUsage(550 * 1024, 1024 * 1024));

        ResourceUnit ru1 = new SimpleResourceUnit("http://prod2-broker7.messaging.usw.example.com:8080", rd);
        Set<ResourceUnit> rus = new HashSet<>();
        rus.add(ru1);
        LoadRanker lr = new ResourceAvailabilityRanker();
        AtomicReference<Map<Long, Set<ResourceUnit>>> sortedRankingsInstance = new AtomicReference<>(Maps.newTreeMap());
        sortedRankingsInstance.get().put(lr.getRank(rd), rus);

        Field sortedRankings = SimpleLoadManagerImpl.class.getDeclaredField("sortedRankings");
        sortedRankings.setAccessible(true);
        sortedRankings.set(loadManager, sortedRankingsInstance);

        ResourceUnit found = loadManager
                .getLeastLoaded(NamespaceName.get("pulsar/use/primary-ns.10")).get();
        assertEquals(found.getResourceId(), ru1.getResourceId());
    }

    @Test
    public void testResourceDescription() {

        PulsarResourceDescription rd = new PulsarResourceDescription();
        rd.put("memory", new ResourceUsage(1024, 4096));
        rd.put("cpu", new ResourceUsage(10, 100));
        rd.put("bandwidthIn", new ResourceUsage(250 * 1024, 1024 * 1024));
        rd.put("bandwidthOut", new ResourceUsage(550 * 1024, 1024 * 1024));

        PulsarResourceDescription rd1 = new PulsarResourceDescription();
        rd1.put("memory", new ResourceUsage(2048, 4096));
        rd1.put("cpu", new ResourceUsage(50, 100));
        rd1.put("bandwidthIn", new ResourceUsage(550 * 1024, 1024 * 1024));
        rd1.put("bandwidthOut", new ResourceUsage(850 * 1024, 1024 * 1024));

        assertEquals(rd.compareTo(rd1), 1);
        assertTrue(rd1.calculateRank() > rd.calculateRank());

        SimpleLoadCalculatorImpl calc = new SimpleLoadCalculatorImpl();
        calc.recalibrateResourceUsagePerServiceUnit(null);
        assertNull(calc.getResourceDescription(null));

    }

    @Test
    public void testLoadReportParsing() throws Exception {

        ObjectMapper mapper = ObjectMapperFactory.create();
        org.apache.pulsar.policies.data.loadbalancer.LoadReport reportData = new org.apache.pulsar.policies.data.loadbalancer.LoadReport();
        reportData.setName("b1");
        SystemResourceUsage resource = new SystemResourceUsage();
        ResourceUsage resourceUsage = new ResourceUsage();
        resource.setBandwidthIn(resourceUsage);
        resource.setBandwidthOut(resourceUsage);
        resource.setMemory(resourceUsage);
        resource.setCpu(resourceUsage);
        reportData.setSystemResourceUsage(resource);
        String loadReportJson = mapper.writeValueAsString(reportData);
        LoadReport loadReport = PulsarLoadReportImpl.parse(loadReportJson);
        assertEquals(
                loadReport.getResourceUnitDescription().getResourceUsage().get("bandwidthIn").compareTo(resourceUsage),
                0);
    }

    @Test(enabled = true)
    public void testDoLoadShedding() throws Exception {
        SimpleLoadManagerImpl loadManager = spy(new SimpleLoadManagerImpl(pulsar1));
        PulsarResourceDescription rd = new PulsarResourceDescription();
        rd.put("memory", new ResourceUsage(1024, 4096));
        rd.put("cpu", new ResourceUsage(10, 100));
        rd.put("bandwidthIn", new ResourceUsage(250 * 1024, 1024 * 1024));
        rd.put("bandwidthOut", new ResourceUsage(550 * 1024, 1024 * 1024));

        ResourceUnit ru1 = new SimpleResourceUnit("http://pulsar-broker1.com:8080", rd);
        ResourceUnit ru2 = new SimpleResourceUnit("http://pulsar-broker2.com:8080", rd);
        Set<ResourceUnit> rus = new HashSet<>();
        rus.add(ru1);
        rus.add(ru2);
        LoadRanker lr = new ResourceAvailabilityRanker();
        AtomicReference<Map<Long, Set<ResourceUnit>>> sortedRankingsInstance = new AtomicReference<>(Maps.newTreeMap());
        sortedRankingsInstance.get().put(lr.getRank(rd), rus);

        Field sortedRankings = SimpleLoadManagerImpl.class.getDeclaredField("sortedRankings");
        sortedRankings.setAccessible(true);
        sortedRankings.set(loadManager, sortedRankingsInstance);

        // inject the load report and rankings
        SystemResourceUsage systemResource = new SystemResourceUsage();
        systemResource.setBandwidthIn(new ResourceUsage(90, 100));
        Map<String, NamespaceBundleStats> stats = Maps.newHashMap();
        NamespaceBundleStats nsb1 = new NamespaceBundleStats();
        nsb1.msgRateOut = 10000;
        NamespaceBundleStats nsb2 = new NamespaceBundleStats();
        nsb2.msgRateOut = 10000;
        stats.put("property/cluster/namespace1/0x00000000_0xFFFFFFFF", nsb1);
        stats.put("property/cluster/namespace2/0x00000000_0xFFFFFFFF", nsb2);

        Map<ResourceUnit, org.apache.pulsar.policies.data.loadbalancer.LoadReport> loadReports = new HashMap<>();
        org.apache.pulsar.policies.data.loadbalancer.LoadReport loadReport1 = new org.apache.pulsar.policies.data.loadbalancer.LoadReport();
        loadReport1.setSystemResourceUsage(systemResource);
        loadReport1.setBundleStats(stats);
        org.apache.pulsar.policies.data.loadbalancer.LoadReport loadReport2 = new org.apache.pulsar.policies.data.loadbalancer.LoadReport();
        loadReport2.setSystemResourceUsage(new SystemResourceUsage());
        loadReport2.setBundleStats(stats);
        loadReports.put(ru1, loadReport1);
        loadReports.put(ru2, loadReport2);
        setObjectField(SimpleLoadManagerImpl.class, loadManager, "currentLoadReports", loadReports);

        loadManager.doLoadShedding();
        verify(loadManager, atLeastOnce()).doLoadShedding();
    }

    // Test that bundles belonging to the same namespace are evenly distributed.
    @Test
    public void testEvenBundleDistribution() throws Exception {
        final NamespaceBundle[] bundles = LoadBalancerTestingUtils
                .makeBundles(pulsar1.getNamespaceService().getNamespaceBundleFactory(), "pulsar", "use", "test", 16);
        final ResourceQuota quota = new ResourceQuota();
        // Create high message rate quota for the first bundle to make it unlikely to be a coincidence of even
        // distribution.
        pulsar1.getBrokerService().getBundlesQuotas().setResourceQuota(bundles[0], quota).join();
        int numAssignedToPrimary = 0;
        int numAssignedToSecondary = 0;
        pulsar1.getConfiguration().setLoadBalancerPlacementStrategy(SimpleLoadManagerImpl.LOADBALANCER_STRATEGY_LLS);
        final SimpleLoadManagerImpl loadManager = (SimpleLoadManagerImpl) pulsar1.getLoadManager().get();

        for (final NamespaceBundle bundle : bundles) {
            if (loadManager.getLeastLoaded(bundle).get().getResourceId().equals(primaryHost)) {
                ++numAssignedToPrimary;
            } else {
                ++numAssignedToSecondary;
            }
            // Check that number of assigned bundles are equivalent when an even number have been assigned.
            if ((numAssignedToPrimary + numAssignedToSecondary) % 2 == 0) {
                assert (numAssignedToPrimary == numAssignedToSecondary);
            }
        }
    }

    @Test
    public void testNamespaceBundleStats() {
        NamespaceBundleStats nsb1 = new NamespaceBundleStats();
        nsb1.msgRateOut = 10000;
        nsb1.producerCount = 1;
        nsb1.consumerCount = 1;
        nsb1.cacheSize = 4;
        nsb1.msgRateIn = 500;
        nsb1.msgThroughputIn = 30;
        nsb1.msgThroughputOut = 30;
        NamespaceBundleStats nsb2 = new NamespaceBundleStats();
        nsb2.msgRateOut = 20000;
        nsb2.producerCount = 300;
        nsb2.consumerCount = 300;
        nsb2.cacheSize = 110000;
        nsb2.msgRateIn = 5000;
        nsb2.msgThroughputIn = 110000.0;
        nsb2.msgThroughputOut = 110000.0;
        assertEquals(-1, nsb1.compareTo(nsb2));
        assertEquals(-1, nsb1.compareByMsgRate(nsb2));
        assertEquals(-1, nsb1.compareByTopicConnections(nsb2));
        assertEquals(-1, nsb1.compareByCacheSize(nsb2));
        assertEquals(-1, nsb1.compareByBandwidthOut(nsb2));
        assertEquals(-1, nsb1.compareByBandwidthIn(nsb2));
    }

    @Test
    public void testBrokerHostUsage() {
        BrokerHostUsage brokerUsage;
        if (SystemUtils.IS_OS_LINUX) {
            brokerUsage = new LinuxBrokerHostUsageImpl(pulsar1);
        } else {
            brokerUsage = new GenericBrokerHostUsageImpl(pulsar1);
        }
        brokerUsage.getBrokerHostUsage();
        // Ok
    }

    @Test
    public void testTask() throws Exception {
        LoadManager loadManager = mock(LoadManager.class);
        AtomicReference<LoadManager> atomicLoadManager = new AtomicReference<>(loadManager);
        LoadResourceQuotaUpdaterTask task1 = new LoadResourceQuotaUpdaterTask(atomicLoadManager);
        task1.run();
        verify(loadManager, times(1)).writeResourceQuotasToZooKeeper();

        LoadSheddingTask task2 = new LoadSheddingTask(atomicLoadManager);
        task2.run();
        verify(loadManager, times(1)).doLoadShedding();
    }

    @Test
    public void testUsage() {
        Map<String, Object> metrics = Maps.newHashMap();
        metrics.put("brk_conn_cnt", 1L);
        metrics.put("brk_repl_conn_cnt", 1L);
        metrics.put("jvm_thread_cnt", 1L);
        BrokerUsage brokerUsage = BrokerUsage.populateFrom(metrics);
        assertEquals(brokerUsage.getConnectionCount(), 1);
        assertEquals(brokerUsage.getReplicationConnectionCount(), 1);
        JvmUsage jvmUsage = JvmUsage.populateFrom(metrics);
        assertEquals(jvmUsage.getThreadCount(), 1);
        SystemResourceUsage usage = new SystemResourceUsage();
        double usageLimit = 10.0;
        usage.setBandwidthIn(new ResourceUsage(usageLimit, usageLimit));
        assertEquals(usage.getBandwidthIn().usage, usageLimit);
        usage.reset();
        assertNotEquals(usage.getBandwidthIn().usage, usageLimit);
    }

}
