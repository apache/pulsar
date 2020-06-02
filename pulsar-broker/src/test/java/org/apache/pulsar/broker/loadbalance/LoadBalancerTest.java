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

import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.loadbalance.impl.PulsarResourceDescription;
import org.apache.pulsar.broker.loadbalance.impl.ResourceAvailabilityRanker;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.SimpleResourceUnit;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.internal.NamespacesImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.ResourceQuota;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicies;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.policies.data.loadbalancer.LoadReport;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.LocalZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Start two brokers in the same cluster and have them connect to the same zookeeper. When the PulsarService starts, it
 * will do the leader election and one of the brokers will become the leader. Then kill that broker and check if the
 * second one becomes the leader.
 *
 *
 */
public class LoadBalancerTest {
    LocalBookkeeperEnsemble bkEnsemble;

    ExecutorService executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    private static final Logger log = LoggerFactory.getLogger(LoadBalancerTest.class);

    private static final int MAX_RETRIES = 10;

    private static final int BROKER_COUNT = 5;
    private int[] brokerWebServicePorts = new int[BROKER_COUNT];
    private int[] brokerNativeBrokerPorts = new int[BROKER_COUNT];
    private URL[] brokerUrls = new URL[BROKER_COUNT];
    private String[] lookupAddresses = new String[BROKER_COUNT];
    private PulsarService[] pulsarServices = new PulsarService[BROKER_COUNT];
    private PulsarAdmin[] pulsarAdmins = new PulsarAdmin[BROKER_COUNT];

    @BeforeMethod
    void setup() throws Exception {
        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();
        ZkUtils.createFullPathOptimistic(bkEnsemble.getZkClient(),
                SimpleLoadManagerImpl.LOADBALANCER_DYNAMIC_SETTING_STRATEGY_ZPATH,
                "{\"loadBalancerStrategy\":\"leastLoadedServer\"}".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        final String localhost = "localhost";
        // start brokers
        for (int i = 0; i < BROKER_COUNT; i++) {


            ServiceConfiguration config = new ServiceConfiguration();
            config.setBrokerServicePort(Optional.ofNullable(brokerNativeBrokerPorts[i]));
            config.setClusterName("use");
            config.setAdvertisedAddress(localhost);
            config.setAdvertisedAddress("localhost");
            config.setWebServicePort(Optional.of(0));
            config.setBrokerServicePortTls(Optional.of(0));
            config.setWebServicePortTls(Optional.of(0));
            config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
            config.setBrokerServicePort(Optional.of(0));
            config.setLoadManagerClassName(SimpleLoadManagerImpl.class.getName());
            config.setAdvertisedAddress(localhost+i);
            config.setLoadBalancerEnabled(false);

            pulsarServices[i] = new PulsarService(config);
            pulsarServices[i].start();
            brokerWebServicePorts[i] = pulsarServices[i].getListenPortHTTP().get();
            brokerNativeBrokerPorts[i] = pulsarServices[i].getBrokerListenPort().get();

            brokerUrls[i] = new URL("http://127.0.0.1" + ":" + brokerWebServicePorts[i]);
            lookupAddresses[i] = pulsarServices[i].getAdvertisedAddress() + ":" + pulsarServices[i].getListenPortHTTP().get();
            pulsarAdmins[i] = PulsarAdmin.builder().serviceHttpUrl(brokerUrls[i].toString()).build();
        }

        createNamespacePolicies(pulsarServices[0]);

        Thread.sleep(100);
    }

    @AfterMethod
    void shutdown() throws Exception {
        log.info("--- Shutting down ---");
        executor.shutdown();

        for (int i = 0; i < BROKER_COUNT; i++) {
            pulsarAdmins[i].close();
            pulsarServices[i].close();
        }

        bkEnsemble.stop();
    }

    private LeaderBroker loopUntilLeaderChanges(LeaderElectionService les, LeaderBroker oldLeader,
            LeaderBroker newLeader) throws InterruptedException {
        int loopCount = 0;

        while (loopCount < MAX_RETRIES) {
            Thread.sleep(1000);
            // Check if the new leader is elected. If yes, break without incrementing the loopCount
            newLeader = les.getCurrentLeader();
            if (newLeader.equals(oldLeader) == false) {
                break;
            }
            ++loopCount;
        }

        // Check if maximum retries are already done. If yes, assert.
        Assert.assertNotEquals(loopCount, MAX_RETRIES, "Leader is not changed even after maximum retries.");
        return newLeader;
    }

    /*
     * tests that load manager creates its node and writes the initial load report a /loadbalance/brokers tests that
     * those load reports can be deserialized and are in valid format tests if the rankings are populated from the load
     * reports are not, both broker will have zero rank
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testLoadReportsWrittenOnZK() throws Exception {
        ZooKeeper zkc = bkEnsemble.getZkClient();
        try {
            for (int i = 0; i < BROKER_COUNT; i++) {
                String znodePath = String.format("%s/%s", SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT,
                        lookupAddresses[i]);
                byte[] loadReportData = zkc.getData(znodePath, false, null);
                assert (loadReportData.length > 0);
                log.info("LoadReport {}, {}", lookupAddresses[i], new String(loadReportData));

                LoadReport loadReport = ObjectMapperFactory.getThreadLocal().readValue(loadReportData, LoadReport.class);
                assert (loadReport.getName().equals(lookupAddresses[i]));

                // Check Initial Ranking is populated in both the brokers
                Field ranking = ((SimpleLoadManagerImpl) pulsarServices[i].getLoadManager().get()).getClass()
                        .getDeclaredField("sortedRankings");
                ranking.setAccessible(true);
                AtomicReference<Map<Long, Set<ResourceUnit>>> sortedRanking = (AtomicReference<Map<Long, Set<ResourceUnit>>>) ranking
                        .get(pulsarServices[i].getLoadManager().get());
                printSortedRanking(sortedRanking);

                // all brokers have same rank to it would be 0 --> set-of-all-the-brokers
                int brokerCount = 0;
                for (Map.Entry<Long, Set<ResourceUnit>> entry : sortedRanking.get().entrySet()) {
                    brokerCount += entry.getValue().size();
                }
                assertEquals(brokerCount, BROKER_COUNT);
                TopicName topicName = TopicName.get("persistent://pulsar/use/primary-ns/test-topic");
                ResourceUnit found = pulsarServices[i].getLoadManager().get()
                        .getLeastLoaded(pulsarServices[i].getNamespaceService().getBundle(topicName)).get();
                assertNotNull(found);
            }
        } catch (InterruptedException | KeeperException e) {
            fail("Unable to read the data from Zookeeper - [{}]", e);
        }
    }

    /*
     * tests rankings get updated when we write write the new load reports to the zookeeper on loadbalance root node
     * tests writing pre-configured load report on the zookeeper translates the pre-calculated rankings
     */
    @Test
    public void testUpdateLoadReportAndCheckUpdatedRanking() throws Exception {
        for (int i = 0; i < BROKER_COUNT; i++) {
            LoadReport lr = new LoadReport();
            lr.setName(lookupAddresses[i]);
            SystemResourceUsage sru = new SystemResourceUsage();
            sru.setBandwidthIn(new ResourceUsage(256, 1024000));
            sru.setBandwidthOut(new ResourceUsage(250, 1024000));
            sru.setMemory(new ResourceUsage(1024, 8192));
            sru.setCpu(new ResourceUsage(5, 400));
            lr.setSystemResourceUsage(sru);

            String znodePath = String.format("%s/%s", SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT,
                    lookupAddresses[i]);
            String loadReportJson = ObjectMapperFactory.getThreadLocal().writeValueAsString(lr);
            bkEnsemble.getZkClient().setData(znodePath, loadReportJson.getBytes(Charsets.UTF_8), -1);
        }

        // sleep to wait the load ranking be triggered
        Thread.sleep(5000);

        // do lookup for bunch of bundles
        int totalNamespaces = 200;
        Map<String, Integer> namespaceOwner = new HashMap<>();
        for (int i = 0; i < totalNamespaces; i++) {
            TopicName topicName = TopicName.get("persistent://pulsar/use/primary-ns-" + i + "/test-topic");
            ResourceUnit found = pulsarServices[0].getLoadManager().get()
                    .getLeastLoaded(pulsarServices[0].getNamespaceService().getBundle(topicName)).get();
            if (namespaceOwner.containsKey(found.getResourceId())) {
                namespaceOwner.put(found.getResourceId(), namespaceOwner.get(found.getResourceId()) + 1);
            } else {
                namespaceOwner.put(found.getResourceId(), 1);
            }
        }
        // assert that distribution variation is not more than 10%
        int averageNamespaces = totalNamespaces / BROKER_COUNT;
        int tenPercentOfAverageNamespaces = averageNamespaces / 10;
        int lowerBound = averageNamespaces - tenPercentOfAverageNamespaces;
        int upperBound = averageNamespaces + tenPercentOfAverageNamespaces;

        // assert each broker received ownership of fair amount of namespaces 90%+
        for (Map.Entry<String, Integer> broker : namespaceOwner.entrySet()) {
            log.info("Count of bundles assigned: {}, {}", broker.getKey(), broker.getValue());
            assertTrue(broker.getValue() >= lowerBound && broker.getValue() <= upperBound);
        }
    }

    private AtomicReference<Map<Long, Set<ResourceUnit>>> getSortedRanking(PulsarService pulsar)
            throws NoSuchFieldException, IllegalAccessException {
        Field ranking = ((SimpleLoadManagerImpl) pulsar.getLoadManager().get()).getClass()
                .getDeclaredField("sortedRankings");
        ranking.setAccessible(true);
        @SuppressWarnings("unchecked")
        AtomicReference<Map<Long, Set<ResourceUnit>>> sortedRanking = (AtomicReference<Map<Long, Set<ResourceUnit>>>) ranking
                .get(pulsar.getLoadManager().get());
        return sortedRanking;
    }

    private void printSortedRanking(AtomicReference<Map<Long, Set<ResourceUnit>>> sortedRanking) {
        log.info("Sorted Ranking Result:");
        sortedRanking.get().forEach((score, rus) -> {
            for (ResourceUnit ru : rus) {
                log.info("  - {}, {}", ru.getResourceId(), score);
            }
        });
    }

    /*
     * Pre-publish load report to ZK, each broker has: - Difference memory capacity, for the first 3 brokers memory is
     * bottleneck, for the 4/5th brokers CPU become bottleneck since memory is big enough - non-bundles assigned so all
     * idle resources are available for new bundle Check the broker rankings are the load percentage of each broker.
     */
    @Test
    public void testBrokerRanking() throws Exception {
        for (int i = 0; i < BROKER_COUNT; i++) {
            LoadReport lr = new LoadReport();
            lr.setName(lookupAddresses[i]);
            SystemResourceUsage sru = new SystemResourceUsage();
            sru.setBandwidthIn(new ResourceUsage(0, 1024000));
            sru.setBandwidthOut(new ResourceUsage(0, 1024000));
            sru.setMemory(new ResourceUsage(1024, 2048 * (i + 1)));
            sru.setCpu(new ResourceUsage(60, 400));
            lr.setSystemResourceUsage(sru);

            String znodePath = String.format("%s/%s", SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT,
                    lookupAddresses[i]);
            String loadReportJson = ObjectMapperFactory.getThreadLocal().writeValueAsString(lr);
            bkEnsemble.getZkClient().setData(znodePath, loadReportJson.getBytes(Charsets.UTF_8), -1);
        }

        // sleep to wait load ranking be triggered
        Thread.sleep(5000);

        // check the ranking result
        for (int i = 0; i < BROKER_COUNT; i++) {
            AtomicReference<Map<Long, Set<ResourceUnit>>> sortedRanking = getSortedRanking(pulsarServices[i]);
            printSortedRanking(sortedRanking);

            // brokers' ranking would be:
            // 50 --> broker 0 ( 1024 / 2048 )
            // 25 --> broker 1 ( 1024 / 4096 )
            // 16 --> broker 2 ( 1024 / 6144 )
            // 15 --> broker 3 ( 60 / 400 )
            // 15 --> broker 4 ( 60 / 400 )
            assertEquals(sortedRanking.get().size(), 4);
            assertEquals(sortedRanking.get().get(50L).size(), 1);
            assertEquals(sortedRanking.get().get(25L).size(), 1);
            assertEquals(sortedRanking.get().get(16L).size(), 1);
            assertEquals(sortedRanking.get().get(15L).size(), 2);
        }
    }

    /*
     * Pre-publish load report to ZK, each broker has: - Difference memory capacity, for the first 3 brokers memory is
     * bottleneck, for the 4/5th brokers CPU become bottleneck since memory is big enough - already has some bundles
     * assigned Check the distribution of new topics is roughly consistent (with <10% variation) with the ranking
     */
    @Test
    public void testTopicAssignmentWithExistingBundles() throws Exception {
        for (int i = 0; i < BROKER_COUNT; i++) {
            ResourceQuota defaultQuota = new ResourceQuota();
            defaultQuota.setMsgRateIn(20);
            defaultQuota.setMsgRateOut(60);
            defaultQuota.setBandwidthIn(20000);
            defaultQuota.setBandwidthOut(60000);
            defaultQuota.setMemory(87);
            pulsarServices[i].getLocalZkCacheService().getResourceQuotaCache().setDefaultQuota(defaultQuota);

            LoadReport lr = new LoadReport();
            lr.setName(lookupAddresses[i]);
            SystemResourceUsage sru = new SystemResourceUsage();
            sru.setBandwidthIn(new ResourceUsage(0, 1024000));
            sru.setBandwidthOut(new ResourceUsage(0, 1024000));
            sru.setMemory(new ResourceUsage(0, 2048 * (i + 1)));
            sru.setCpu(new ResourceUsage(60, 400));
            lr.setSystemResourceUsage(sru);

            Map<String, NamespaceBundleStats> bundleStats = new HashMap<String, NamespaceBundleStats>();
            for (int j = 0; j < (i + 1) * 5; j++) {
                String bundleName = String.format("pulsar/use/primary-ns-%d-%d/0x00000000_0xffffffff", i, j);
                NamespaceBundleStats stats = new NamespaceBundleStats();
                bundleStats.put(bundleName, stats);
            }
            lr.setBundleStats(bundleStats);

            String znodePath = String.format("%s/%s", SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT,
                    lookupAddresses[i]);
            String loadReportJson = ObjectMapperFactory.getThreadLocal().writeValueAsString(lr);
            bkEnsemble.getZkClient().setData(znodePath, loadReportJson.getBytes(Charsets.UTF_8), -1);
        }

        // sleep to wait load ranking be triggered
        Thread.sleep(5000);

        // print ranking
        for (int i = 0; i < BROKER_COUNT; i++) {
            AtomicReference<Map<Long, Set<ResourceUnit>>> sortedRanking = getSortedRanking(pulsarServices[i]);
            printSortedRanking(sortedRanking);
        }

        // check owner of new destinations and verify that the distribution is roughly
        // consistent (variation < 10%) with the broker capacity:
        int totalNamespaces = 250;
        int[] expectedAssignments = new int[] { 17, 34, 51, 68, 85 };
        Map<String, Integer> namespaceOwner = new HashMap<>();
        for (int i = 0; i < totalNamespaces; i++) {
            TopicName topicName = TopicName.get("persistent://pulsar/use/primary-ns-" + i + "/test-topic");
            ResourceUnit found = pulsarServices[0].getLoadManager().get()
                    .getLeastLoaded(pulsarServices[0].getNamespaceService().getBundle(topicName)).get();
            if (namespaceOwner.containsKey(found.getResourceId())) {
                namespaceOwner.put(found.getResourceId(), namespaceOwner.get(found.getResourceId()) + 1);
            } else {
                namespaceOwner.put(found.getResourceId(), 1);
            }
        }

        double expectedMaxVariation = 10.0;
        for (int i = 0; i < BROKER_COUNT; i++) {
            long actualValue = 0;
            String resourceId = "http://" + lookupAddresses[i];
            if (namespaceOwner.containsKey(resourceId)) {
                actualValue = namespaceOwner.get(resourceId);
            }

            long expectedValue = expectedAssignments[i];
            double variation = Math.abs(actualValue - expectedValue) * 100.0 / expectedValue;
            log.info("Topic assignment - {}, actual: {}, expected baseline: {}, variation: {}/%",
                    lookupAddresses[i], actualValue, expectedValue, String.format("%.2f", variation));
            assertTrue(variation < expectedMaxVariation);
        }
    }

    /**
     * Ensure that the load manager's zookeeper data cache is shutdown after invoking stop().
     */
    @Test
    public void testStop() throws Exception {
        final SimpleLoadManagerImpl loadManager = (SimpleLoadManagerImpl) pulsarServices[0].getLoadManager().get();
        loadManager.stop();
        Field loadReportCacheField = SimpleLoadManagerImpl.class.getDeclaredField("loadReportCacheZk");
        loadReportCacheField.setAccessible(true);
        ZooKeeperDataCache<LoadReport> loadReportCache = (ZooKeeperDataCache<LoadReport>) loadReportCacheField
                .get(loadManager);
        Field IS_SHUTDOWN_UPDATER = ZooKeeperDataCache.class.getDeclaredField("IS_SHUTDOWN_UPDATER");
        IS_SHUTDOWN_UPDATER.setAccessible(true);
        final int TRUE = 1;
        assert (((AtomicIntegerFieldUpdater<ZooKeeperDataCache>) (IS_SHUTDOWN_UPDATER.get(loadReportCache))).get(loadReportCache) == TRUE);
    }

    private AtomicReference<Map<String, ResourceQuota>> getRealtimeResourceQuota(PulsarService pulsar)
            throws NoSuchFieldException, IllegalAccessException {
        Field quotasField = ((SimpleLoadManagerImpl) pulsar.getLoadManager().get()).getClass()
                .getDeclaredField("realtimeResourceQuotas");
        quotasField.setAccessible(true);
        AtomicReference<Map<String, ResourceQuota>> realtimeResourceQuotas = (AtomicReference<Map<String, ResourceQuota>>) quotasField
                .get(pulsar.getLoadManager().get());
        return realtimeResourceQuotas;
    }

    private void printResourceQuotas(Map<String, ResourceQuota> resourceQuotas) throws Exception {
        log.info("Realtime Resource Quota:");
        for (Map.Entry<String, ResourceQuota> entry : resourceQuotas.entrySet()) {
            String quotaStr = ObjectMapperFactory.getThreadLocal().writeValueAsString(entry.getValue());
            log.info(" {}, {}", entry.getKey(), quotaStr);
        }
    }

    private void writeLoadReportsForDynamicQuota(long timestamp) throws Exception {
        for (int i = 0; i < BROKER_COUNT; i++) {
            LoadReport lr = new LoadReport();
            lr.setName(lookupAddresses[i]);
            lr.setTimestamp(timestamp);
            SystemResourceUsage sru = new SystemResourceUsage();
            sru.setBandwidthIn(new ResourceUsage(5000 * (10 + i * 5), 1024000));
            sru.setBandwidthOut(new ResourceUsage(15000 * (10 + i * 5), 1024000));
            sru.setMemory(new ResourceUsage(25 * (10 + i * 5), 2048 * (i + 1)));
            sru.setCpu(new ResourceUsage(200, 400));
            lr.setSystemResourceUsage(sru);

            Map<String, NamespaceBundleStats> bundleStats = new HashMap<String, NamespaceBundleStats>();
            for (int j = 0; j < 5; j++) {
                String bundleName = String.format("pulsar/use/primary-ns-%d-%d/0x00000000_0xffffffff", i, j);
                NamespaceBundleStats stats = new NamespaceBundleStats();
                stats.msgRateIn = 5 * (i + j);
                stats.msgRateOut = 15 * (i + j);
                stats.msgThroughputIn = 5000 * (i + j);
                stats.msgThroughputOut = 15000 * (i + j);
                stats.topics = 25 * (i + j);
                stats.consumerCount = 50 * (i + j);
                stats.producerCount = 50 * (i + j);
                bundleStats.put(bundleName, stats);
            }
            lr.setBundleStats(bundleStats);

            String znodePath = String.format("%s/%s", SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT,
                    lookupAddresses[i]);
            String loadReportJson = ObjectMapperFactory.getThreadLocal().writeValueAsString(lr);
            bkEnsemble.getZkClient().setData(znodePath, loadReportJson.getBytes(Charsets.UTF_8), -1);
        }
    }

    private void verifyBundleResourceQuota(ResourceQuota quota, double expMsgRateIn, double expMsgRateOut,
            double expBandwidthIn, double expBandwidthOut, double expMemory) {
        assertTrue(Math.abs(quota.getMsgRateIn() - expMsgRateIn) < 1);
        assertTrue(Math.abs(quota.getMsgRateOut() - expMsgRateOut) < 1);
        assertTrue(Math.abs(quota.getBandwidthIn() - expBandwidthIn) < 1);
        assertTrue(Math.abs(quota.getBandwidthOut() - expBandwidthOut) < 1);
        assertTrue(Math.abs(quota.getMemory() - expMemory) < 1);
    }

    /*
     * Test broker dynamically calculating resource quota for each connected namespace bundle.
     */
    @Test
    public void testDynamicNamespaceBundleQuota() throws Exception {
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < BROKER_COUNT; i++) {
            ResourceQuota defaultQuota = new ResourceQuota();
            defaultQuota.setMsgRateIn(20);
            defaultQuota.setMsgRateOut(60);
            defaultQuota.setBandwidthIn(20000);
            defaultQuota.setBandwidthOut(60000);
            defaultQuota.setMemory(75);
            pulsarServices[i].getLocalZkCacheService().getResourceQuotaCache().setDefaultQuota(defaultQuota);
        }

        // publish the initial load reports and wait for quotas be updated
        writeLoadReportsForDynamicQuota(startTime);
        Thread.sleep(5000);

        // publish test report for 15 minutes later and wait for quotas be updated
        writeLoadReportsForDynamicQuota(startTime + SimpleLoadManagerImpl.RESOURCE_QUOTA_GO_UP_TIMEWINDOW);
        Thread.sleep(5000);

        // print & verify resource quotas
        for (int i = 0; i < BROKER_COUNT; i++) {
            Map<String, ResourceQuota> quotas = getRealtimeResourceQuota(pulsarServices[i]).get();
            printResourceQuotas(quotas);
            verifyBundleResourceQuota(quotas.get("pulsar/use/primary-ns-0-0/0x00000000_0xffffffff"), 19.0, 58.0,
                    19791.0, 58958.0, 74.0);
            verifyBundleResourceQuota(quotas.get("pulsar/use/primary-ns-2-2/0x00000000_0xffffffff"), 20.0, 60.0,
                    20000.0, 60000.0, 100.0);
            verifyBundleResourceQuota(quotas.get("pulsar/use/primary-ns-4-4/0x00000000_0xffffffff"), 40.0, 120.0,
                    40000.0, 120000.0, 150.0);
        }

        // publish test report for 24 hours later and wait for quotas be updated
        writeLoadReportsForDynamicQuota(startTime + SimpleLoadManagerImpl.RESOURCE_QUOTA_GO_DOWN_TIMEWINDOW);
        Thread.sleep(5000);

        // print & verify resource quotas
        for (int i = 0; i < BROKER_COUNT; i++) {
            Map<String, ResourceQuota> quotas = getRealtimeResourceQuota(pulsarServices[i]).get();
            printResourceQuotas(quotas);
            verifyBundleResourceQuota(quotas.get("pulsar/use/primary-ns-0-0/0x00000000_0xffffffff"), 5.0, 6.0, 10203.0,
                    11019.0, 50.0);
            verifyBundleResourceQuota(quotas.get("pulsar/use/primary-ns-2-2/0x00000000_0xffffffff"), 20.0, 60.0,
                    20000.0, 60000.0, 100.0);
            verifyBundleResourceQuota(quotas.get("pulsar/use/primary-ns-4-4/0x00000000_0xffffffff"), 40.0, 120.0,
                    40000.0, 120000.0, 150.0);
        }
    }

    private void setObjectField(Class objClass, Object objInstance, String fieldName, Object newValue)
            throws Exception {
        Field field = objClass.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(objInstance, newValue);
    }

    private NamespaceBundleStats newBundleStats(long topics, int producers, int consumers, double msgRateIn,
            double msgRateOut, double throughputIn, double throughputOut) {
        NamespaceBundleStats stats = new NamespaceBundleStats();
        stats.topics = topics;
        stats.producerCount = producers;
        stats.consumerCount = consumers;
        stats.msgRateIn = msgRateIn;
        stats.msgRateOut = msgRateOut;
        stats.msgThroughputIn = throughputIn;
        stats.msgThroughputOut = throughputOut;
        return stats;
    }

    private BundlesData getBundles(int numBundles) {
        Long maxVal = ((long) 1) << 32;
        Long segSize = maxVal / numBundles;
        List<String> partitions = Lists.newArrayList();
        partitions.add(String.format("0x%08x", 0l));
        Long curPartition = segSize;
        for (int i = 0; i < numBundles; i++) {
            if (i != numBundles - 1) {
                partitions.add(String.format("0x%08x", curPartition));
            } else {
                partitions.add(String.format("0x%08x", maxVal - 1));
            }
            curPartition += segSize;
        }
        return new BundlesData(partitions);
    }

    private void createNamespace(PulsarService pulsar, String namespace, int numBundles) throws Exception {
        Policies policies = new Policies();
        policies.bundles = getBundles(numBundles);

        ObjectMapper jsonMapper = ObjectMapperFactory.create();
        ZooKeeper globalZk = pulsar.getGlobalZkCache().getZooKeeper();
        String zpath = AdminResource.path(POLICIES, namespace);
        ZkUtils.createFullPathOptimistic(globalZk, zpath, jsonMapper.writeValueAsBytes(policies),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    }

    /**
     * Test the namespace bundle auto-split
     */
    @Test
    public void testNamespaceBundleAutoSplit() throws Exception {
        int maxBundles = pulsarServices[0].getConfiguration().getLoadBalancerNamespaceMaximumBundles();
        long maxTopics = pulsarServices[0].getConfiguration().getLoadBalancerNamespaceBundleMaxTopics();
        int maxSessions = pulsarServices[0].getConfiguration().getLoadBalancerNamespaceBundleMaxSessions();
        long maxMsgRate = pulsarServices[0].getConfiguration().getLoadBalancerNamespaceBundleMaxMsgRate();
        long maxBandwidth = pulsarServices[0].getConfiguration().getLoadBalancerNamespaceBundleMaxBandwidthMbytes()
                * 1048576;
        pulsarServices[0].getConfiguration().setLoadBalancerAutoBundleSplitEnabled(true);

        // create namespaces
        for (int i = 1; i <= 10; i++) {
            int numBundles = (i == 10) ? maxBundles : 2;
            createNamespace(pulsarServices[0], String.format("pulsar/use/primary-ns-%02d", i), numBundles);
        }

        // fake Namespaces Admin
        NamespacesImpl namespaceAdmin = mock(NamespacesImpl.class);
        setObjectField(PulsarAdmin.class, pulsarServices[0].getAdminClient(), "namespaces", namespaceAdmin);

        // create load report
        // namespace 01~09 need to be split
        // namespace 08~10 don't need or cannot be split
        LoadReport lr = new LoadReport();
        lr.setName(lookupAddresses[0]);
        lr.setSystemResourceUsage(new SystemResourceUsage());

        Map<String, NamespaceBundleStats> bundleStats = new HashMap<String, NamespaceBundleStats>();
        bundleStats.put("pulsar/use/primary-ns-01/0x00000000_0x80000000",
                newBundleStats(maxTopics + 1, 0, 0, 0, 0, 0, 0));
        bundleStats.put("pulsar/use/primary-ns-02/0x00000000_0x80000000",
                newBundleStats(2, maxSessions + 1, 0, 0, 0, 0, 0));
        bundleStats.put("pulsar/use/primary-ns-03/0x00000000_0x80000000",
                newBundleStats(2, 0, maxSessions + 1, 0, 0, 0, 0));
        bundleStats.put("pulsar/use/primary-ns-04/0x00000000_0x80000000",
                newBundleStats(2, 0, 0, maxMsgRate + 1, 0, 0, 0));
        bundleStats.put("pulsar/use/primary-ns-05/0x00000000_0x80000000",
                newBundleStats(2, 0, 0, 0, maxMsgRate + 1, 0, 0));
        bundleStats.put("pulsar/use/primary-ns-06/0x00000000_0x80000000",
                newBundleStats(2, 0, 0, 0, 0, maxBandwidth + 1, 0));
        bundleStats.put("pulsar/use/primary-ns-07/0x00000000_0x80000000",
                newBundleStats(2, 0, 0, 0, 0, 0, maxBandwidth + 1));

        bundleStats.put("pulsar/use/primary-ns-08/0x00000000_0x80000000",
                newBundleStats(maxTopics - 1, maxSessions - 1, 1, maxMsgRate - 1, 1, maxBandwidth - 1, 1));
        bundleStats.put("pulsar/use/primary-ns-09/0x00000000_0x80000000",
                newBundleStats(1, 0, 0, 0, 0, 0, maxBandwidth + 1));
        bundleStats.put("pulsar/use/primary-ns-10/0x00000000_0x02000000",
                newBundleStats(maxTopics + 1, 0, 0, 0, 0, 0, 0));
        lr.setBundleStats(bundleStats);

        setObjectField(SimpleLoadManagerImpl.class, pulsarServices[0].getLoadManager().get(), "lastLoadReport", lr);
        String znodePath = String.format("%s/%s", SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT, lookupAddresses[0]);
        String loadReportJson = ObjectMapperFactory.getThreadLocal().writeValueAsString(lr);
        bkEnsemble.getZkClient().setData(znodePath, loadReportJson.getBytes(Charsets.UTF_8), -1);

        // sleep to wait load ranking be triggered and trigger bundle split
        Thread.sleep(5000);
        pulsarServices[0].getLoadManager().get().doNamespaceBundleSplit();

        boolean isAutoUnooadSplitBundleEnabled = pulsarServices[0].getConfiguration().isLoadBalancerAutoUnloadSplitBundlesEnabled();
        // verify bundles are split
        verify(namespaceAdmin, times(1)).splitNamespaceBundle("pulsar/use/primary-ns-01", "0x00000000_0x80000000",
                isAutoUnooadSplitBundleEnabled, null);
        verify(namespaceAdmin, times(1)).splitNamespaceBundle("pulsar/use/primary-ns-02", "0x00000000_0x80000000",
                isAutoUnooadSplitBundleEnabled, null);
        verify(namespaceAdmin, times(1)).splitNamespaceBundle("pulsar/use/primary-ns-03", "0x00000000_0x80000000",
                isAutoUnooadSplitBundleEnabled, null);
        verify(namespaceAdmin, times(1)).splitNamespaceBundle("pulsar/use/primary-ns-04", "0x00000000_0x80000000",
                isAutoUnooadSplitBundleEnabled, null);
        verify(namespaceAdmin, times(1)).splitNamespaceBundle("pulsar/use/primary-ns-05", "0x00000000_0x80000000",
                isAutoUnooadSplitBundleEnabled, null);
        verify(namespaceAdmin, times(1)).splitNamespaceBundle("pulsar/use/primary-ns-06", "0x00000000_0x80000000",
                isAutoUnooadSplitBundleEnabled, null);
        verify(namespaceAdmin, times(1)).splitNamespaceBundle("pulsar/use/primary-ns-07", "0x00000000_0x80000000",
                isAutoUnooadSplitBundleEnabled, null);
        verify(namespaceAdmin, never()).splitNamespaceBundle("pulsar/use/primary-ns-08", "0x00000000_0x80000000",
                isAutoUnooadSplitBundleEnabled, null);
        verify(namespaceAdmin, never()).splitNamespaceBundle("pulsar/use/primary-ns-09", "0x00000000_0x80000000",
                isAutoUnooadSplitBundleEnabled, null);
        verify(namespaceAdmin, never()).splitNamespaceBundle("pulsar/use/primary-ns-10", "0x00000000_0x02000000",
                isAutoUnooadSplitBundleEnabled, null);
    }

    /*
     * Test all brokers are consistent on current leader and close leader to trigger re-election.
     */
    @Test
    public void testLeaderElection() throws Exception {
        for (int i = 0; i < BROKER_COUNT - 1; i++) {
            Set<PulsarService> activePulsar = new HashSet<PulsarService>();
            LeaderBroker oldLeader = null;
            PulsarService leaderPulsar = null;
            PulsarService followerPulsar = null;

            for (int j = 0; j < BROKER_COUNT; j++) {
                if (pulsarServices[j].getState() != PulsarService.State.Closed) {
                    activePulsar.add(pulsarServices[j]);
                    LeaderElectionService les = pulsarServices[j].getLeaderElectionService();
                    if (les.isLeader()) {
                        oldLeader = les.getCurrentLeader();
                        leaderPulsar = pulsarServices[j];
                    } else {
                        followerPulsar = pulsarServices[j];
                    }
                }
            }

            // Make sure both brokers see the same leader
            log.info("Old leader is : {}", oldLeader.getServiceUrl());
            for (PulsarService pulsar : activePulsar) {
                assertEquals(pulsar.getLeaderElectionService().getCurrentLeader(), oldLeader);
            }

            // Do leader election by killing the leader broker
            leaderPulsar.close();
            LeaderBroker newLeader = oldLeader;
            newLeader = loopUntilLeaderChanges(followerPulsar.getLeaderElectionService(), oldLeader, newLeader);
            log.info("New leader is : {}", newLeader.getServiceUrl());
            Assert.assertNotEquals(newLeader, oldLeader);
        }
    }

    private void createNamespacePolicies(PulsarService pulsar) throws Exception {
        // // prepare three policies for the namespace isolation
        NamespaceIsolationPolicies policies = new NamespaceIsolationPolicies();

        // set up policy that use this broker as primary
        NamespaceIsolationData policyData = new NamespaceIsolationData();
        policyData.namespaces = new ArrayList<String>();
        policyData.namespaces.add("pulsar/use/primary-ns.*");
        policyData.primary = new ArrayList<String>();
        for (int i = 0; i < BROKER_COUNT; i++) {
            policyData.primary.add(pulsarServices[i].getAdvertisedAddress());
        }
        policyData.secondary = new ArrayList<String>();
        policyData.auto_failover_policy = new AutoFailoverPolicyData();
        policyData.auto_failover_policy.policy_type = AutoFailoverPolicyType.min_available;
        policyData.auto_failover_policy.parameters = new HashMap<String, String>();
        policyData.auto_failover_policy.parameters.put("min_limit", "1");
        policyData.auto_failover_policy.parameters.put("usage_threshold", "100");
        policies.setPolicy("primaryBrokerPolicy", policyData);

        // set up policy that use this broker as secondary
        policyData = new NamespaceIsolationData();
        policyData.namespaces = new ArrayList<String>();
        policyData.namespaces.add("pulsar/use/secondary-ns.*");
        policyData.primary = new ArrayList<String>();
        policyData.primary.add(pulsarServices[0].getAdvertisedAddress());
        policyData.secondary = new ArrayList<String>();
        for (int i = 1; i < BROKER_COUNT; i++) {
            policyData.secondary.add(pulsarServices[i].getAdvertisedAddress());
        }
        policyData.auto_failover_policy = new AutoFailoverPolicyData();
        policyData.auto_failover_policy.policy_type = AutoFailoverPolicyType.min_available;
        policyData.auto_failover_policy.parameters = new HashMap<String, String>();
        policyData.auto_failover_policy.parameters.put("min_limit", "1");
        policyData.auto_failover_policy.parameters.put("usage_threshold", "100");
        policies.setPolicy("secondaryBrokerPolicy", policyData);

        // set up policy that do not use this broker (neither primary nor secondary)
        policyData = new NamespaceIsolationData();
        policyData.namespaces = new ArrayList<String>();
        policyData.namespaces.add("pulsar/use/shared-ns.*");
        policyData.primary = new ArrayList<String>();
        policyData.primary.add(pulsarServices[0].getAdvertisedAddress());
        policyData.secondary = new ArrayList<String>();
        for (int i = 1; i < BROKER_COUNT; i++) {
            policyData.secondary.add(pulsarServices[i].getAdvertisedAddress());
        }
        policyData.auto_failover_policy = new AutoFailoverPolicyData();
        policyData.auto_failover_policy.policy_type = AutoFailoverPolicyType.min_available;
        policyData.auto_failover_policy.parameters = new HashMap<String, String>();
        policyData.auto_failover_policy.parameters.put("min_limit", "1");
        policyData.auto_failover_policy.parameters.put("usage_threshold", "100");
        policies.setPolicy("otherBrokerPolicy", policyData);

        ObjectMapper jsonMapper = ObjectMapperFactory.create();
        ZooKeeper globalZk = pulsar.getGlobalZkCache().getZooKeeper();
        ZkUtils.createFullPathOptimistic(globalZk, AdminResource.path("clusters", "use", "namespaceIsolationPolicies"),
                new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        byte[] content = jsonMapper.writeValueAsBytes(policies.getPolicies());
        globalZk.setData(AdminResource.path("clusters", "use", "namespaceIsolationPolicies"), content, -1);

    }

    @Test(enabled = false)
    public void testGetLeastLoadedBasic() throws Exception {
        LocalZooKeeperCache mockCache = mock(LocalZooKeeperCache.class);
        Set<String> activeBrokers = Sets.newHashSet("prod1-broker1.messaging.use.example.com:8080",
                "prod1-broker2.messaging.use.example.com:8080", "prod1-broker3.messaging.use.example.com:8080");
        when(mockCache.getChildren(SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT)).thenReturn(activeBrokers);
        Field zkCacheField = PulsarService.class.getDeclaredField("localZkCache");
        zkCacheField.setAccessible(true);

        LocalZooKeeperCache originalLZK1 = (LocalZooKeeperCache) zkCacheField.get(pulsarServices[0]);
        LocalZooKeeperCache originalLZK2 = (LocalZooKeeperCache) zkCacheField.get(pulsarServices[1]);
        zkCacheField.set(pulsarServices[0], mockCache);
        zkCacheField.set(pulsarServices[1], mockCache);
        SimpleLoadManagerImpl loadManager = new SimpleLoadManagerImpl(pulsarServices[0]);

        // TODO move to its own test
        PulsarResourceDescription rd = new PulsarResourceDescription();
        rd.put("memory", new ResourceUsage(1024, 4096));
        rd.put("cpu", new ResourceUsage(10, 100));
        rd.put("bandwidthIn", new ResourceUsage(250 * 1024, 1024 * 1024));
        rd.put("bandwidthOut", new ResourceUsage(550 * 1024, 1024 * 1024));

        ResourceUnit ru1 = new SimpleResourceUnit("http://prod1-broker1.messaging.use.example.com:8080", rd);
        Set<ResourceUnit> rus = new HashSet<ResourceUnit>();
        rus.add(ru1);
        LoadRanker lr = new ResourceAvailabilityRanker();
        AtomicReference<Map<Long, Set<ResourceUnit>>> sortedRankingsInstance = new AtomicReference<>(Maps.newTreeMap());
        sortedRankingsInstance.get().put(lr.getRank(rd), rus);

        Field sortedRankings = SimpleLoadManagerImpl.class.getDeclaredField("sortedRankings");
        sortedRankings.setAccessible(true);
        sortedRankings.set(loadManager, sortedRankingsInstance);

        ResourceUnit found = loadManager.getLeastLoaded(NamespaceName.get("pulsar/use/primary-ns.10")).get();
        assertEquals("http://prod1-broker1.messaging.use.example.com:8080", found.getResourceId());

        zkCacheField.set(pulsarServices[0], originalLZK1);
        zkCacheField.set(pulsarServices[1], originalLZK2);
        loadManager.stop();
    }

    /*
     * creates a ResourceDescription where the max limits for different parameters are as below
     *
     * Memory = 16GB cpu = 100 percent bandwidthIn = 1Gbps bandwidthOut = 1Gbps threads = 100
     */

    private PulsarResourceDescription createResourceDescription(long memoryInMB, long cpuPercentage,
            long bandwidthInMbps, long bandwidthOutInMbps, long threads) {
        long KB = 1024;
        long MB = 1024 * KB;
        long GB = 1024 * MB;
        PulsarResourceDescription rd = new PulsarResourceDescription();
        rd.put("memory", new ResourceUsage(memoryInMB, 4 * GB));
        rd.put("cpu", new ResourceUsage(cpuPercentage, 100));
        rd.put("bandwidthIn", new ResourceUsage(bandwidthInMbps * MB, GB));
        rd.put("bandwidthOut", new ResourceUsage(bandwidthOutInMbps * MB, GB));
        return rd;
    }

    /*
     * Simple Test, creates three Resource Units (brokers) with equal load and expects that out of 1005 namespaces it
     * should be divided fairly equally with about 10% of variation
     *
     */
    @Test(enabled = false)
    public void testLoadbalanceDistributionAmongEquallyLoaded() throws Exception {
        LoadManager loadManager = new SimpleLoadManagerImpl(pulsarServices[0]);
        ZooKeeperCache mockCache = mock(ZooKeeperCache.class);
        Set<String> activeBrokers = Sets.newHashSet("prod1-broker1.messaging.use.example.com:8080",
                "prod1-broker2.messaging.use.example.com:8080", "prod1-broker3.messaging.use.example.com:8080");
        when(mockCache.getChildren(SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT)).thenReturn(activeBrokers);
        when(mockCache.getChildren(SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT)).thenReturn(activeBrokers);
        Field zkCacheField = PulsarService.class.getDeclaredField("localZkCache");
        zkCacheField.setAccessible(true);
        zkCacheField.set(pulsarServices[0], mockCache);

        long memoryMB = 4096;
        long cpuPercent = 45;
        long bInMbps = 350;
        long bOutMbps = 180;
        long threads = 10;
        // TODO move to its own test
        PulsarResourceDescription rd = createResourceDescription(memoryMB, cpuPercent, bInMbps, bOutMbps, threads);

        Set<ResourceUnit> rus = new HashSet<ResourceUnit>();

        for (String broker : activeBrokers) {
            ResourceUnit ru = new SimpleResourceUnit(broker, rd);
            rus.add(ru);
        }

        TreeMap<Long, Set<ResourceUnit>> sortedRankingsInstance = new TreeMap<Long, Set<ResourceUnit>>();
        LoadRanker ranker = new ResourceAvailabilityRanker();
        sortedRankingsInstance.put(ranker.getRank(rd), rus);

        Field sortedRankings = SimpleLoadManagerImpl.class.getDeclaredField("sortedRankings");
        sortedRankings.setAccessible(true);
        AtomicReference<TreeMap<Long, Set<ResourceUnit>>> ar = new AtomicReference<TreeMap<Long, Set<ResourceUnit>>>();
        ar.set(sortedRankingsInstance);
        sortedRankings.set(loadManager, ar);

    }

    /**
     * This test puts few brokers in the ranking that load balance uses but does not put few brokers in the mock
     * zookeeper cache, if the broker is not in the zk cache, it's considered inactive.
     *
     * We should not see any of these inactive brokers assigned any namespace.
     */
    @Test(enabled = false)
    void testLoadBalanceDiscardingInactiveBrokersInSelection() throws Exception {
        long memoryMB = 2096;
        long cpuPercent = 12;
        long bInMbps = 100;
        long bOutMbps = 100;
        long threads = 3;
        LoadManager loadManager = new SimpleLoadManagerImpl(pulsarServices[0]);
        ZooKeeperCache mockCache = mock(ZooKeeperCache.class);
        Set<String> activeBrokers = Sets.newHashSet("prod1-broker1.messaging.use.example.com:8080",
                "prod1-broker2.messaging.use.example.com:8080");
        when(mockCache.getChildren(SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT)).thenReturn(activeBrokers);
        Field zkCacheField = PulsarService.class.getDeclaredField("localZkCache");
        zkCacheField.setAccessible(true);
        zkCacheField.set(pulsarServices[0], mockCache);

        TreeMap<Long, Set<ResourceUnit>> sortedRankingsInstance = new TreeMap<Long, Set<ResourceUnit>>();
        for (int i = 1; i <= 3; i++) {
            PulsarResourceDescription rd = createResourceDescription(memoryMB * i, cpuPercent * i, bInMbps * i,
                    bOutMbps * 2, threads * i);
            ResourceUnit ru1 = new SimpleResourceUnit(
                    String.format("http://prod1-broker%d.messaging.use.example.com:8080", i), rd);
            LoadRanker ranker = new ResourceAvailabilityRanker();
            long rank = ranker.getRank(rd);
            if (sortedRankingsInstance.containsKey(rank)) {
                // get the object set and put the rd in it
                sortedRankingsInstance.get(rank).add(ru1);
            } else {
                Set<ResourceUnit> rus = new HashSet<ResourceUnit>();
                rus.add(ru1);
                sortedRankingsInstance.put(rank, rus);
            }

        }
        Field sortedRankings = SimpleLoadManagerImpl.class.getDeclaredField("sortedRankings");
        sortedRankings.setAccessible(true);
        AtomicReference<TreeMap<Long, Set<ResourceUnit>>> ar = new AtomicReference<TreeMap<Long, Set<ResourceUnit>>>();
        ar.set(sortedRankingsInstance);
        sortedRankings.set(loadManager, ar);

        int totalNamespaces = 10;
        Map<String, Integer> namespaceOwner = new HashMap<String, Integer>();
        for (int i = 0; i < totalNamespaces; i++) {
            ResourceUnit found = loadManager
                    .getLeastLoaded(TopicName.get("persistent://pulsar/use/primary-ns/topic" + i)).get();
            if (namespaceOwner.containsKey(found.getResourceId())) {
                namespaceOwner.put(found.getResourceId(), namespaceOwner.get(found.getResourceId()) + 1);
            } else {
                namespaceOwner.put(found.getResourceId(), 0);
            }
        }
        String inactiveBroker = "prod1-broker3.messaging.use.example.com:8080";
        // check owner list contains only two entries, broker-3 should not be in
        assertEquals(namespaceOwner.size(), 2);
        assertFalse(namespaceOwner.containsKey(inactiveBroker));
    }

    @Test(enabled = false)
    void testLoadBalanceDistributionAmongUnequallyLoaded() throws Exception {
        long memoryMB = 4096;
        long cpuPercent = 25;
        long bInMbps = 256;
        long bOutMbps = 256;
        long threads = 25;

        LoadManager loadManager = new SimpleLoadManagerImpl(pulsarServices[0]);
        ZooKeeperCache mockCache = mock(ZooKeeperCache.class);
        Set<String> activeBrokers = Sets.newHashSet("prod1-broker1.messaging.use.example.com:8080",
                "prod1-broker2.messaging.use.example.com:8080", "prod1-broker3.messaging.use.example.com:8080");
        when(mockCache.getChildren(SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT)).thenReturn(activeBrokers);
        when(mockCache.getChildren(SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT)).thenReturn(activeBrokers);
        Field zkCacheField = PulsarService.class.getDeclaredField("localZkCache");
        zkCacheField.setAccessible(true);
        zkCacheField.set(pulsarServices[0], mockCache);

        int totalAvailabilityWeight = 0;
        TreeMap<Long, Set<ResourceUnit>> sortedRankingsInstance = new TreeMap<>();
        for (int i = 1; i <= 3; i++) {
            PulsarResourceDescription rd = createResourceDescription(memoryMB * i, cpuPercent * i, bInMbps * i,
                    bOutMbps * 2, threads * i);
            ResourceUnit ru1 = new SimpleResourceUnit(
                    String.format("http://prod1-broker%d.messaging.use.example.com:8080", i), rd);
            LoadRanker ranker = new ResourceAvailabilityRanker();
            long rank = ranker.getRank(rd);
            if (sortedRankingsInstance.containsKey(rank)) {
                // get the object set and put the rd in it
                sortedRankingsInstance.get(rank).add(ru1);
            } else {
                Set<ResourceUnit> rus = new HashSet<ResourceUnit>();
                rus.add(ru1);
                sortedRankingsInstance.put(rank, rus);
            }
            totalAvailabilityWeight += rank;
        }

        Field sortedRankings = SimpleLoadManagerImpl.class.getDeclaredField("sortedRankings");
        sortedRankings.setAccessible(true);
        AtomicReference<TreeMap<Long, Set<ResourceUnit>>> ar = new AtomicReference<TreeMap<Long, Set<ResourceUnit>>>();
        ar.set(sortedRankingsInstance);
        sortedRankings.set(loadManager, ar);

        int totalNamespaces = 1000;
        Map<String, Integer> namespaceOwner = new HashMap<String, Integer>();
        for (int i = 0; i < totalNamespaces; i++) {
            ResourceUnit found = loadManager
                    .getLeastLoaded(TopicName.get("persistent://pulsar/use/primary-ns/topic-" + i)).get();
            if (namespaceOwner.containsKey(found.getResourceId())) {
                namespaceOwner.put(found.getResourceId(), namespaceOwner.get(found.getResourceId()) + 1);
            } else {
                namespaceOwner.put(found.getResourceId(), 0);
            }
        }

        for (Map.Entry<Long, Set<ResourceUnit>> entry : sortedRankingsInstance.entrySet()) {
            int selectionProbability = (int) (((double) entry.getKey() / totalAvailabilityWeight) * 100);
            int idealExpectedOwned = selectionProbability * (int) ((double) totalNamespaces / 100);
            int expectedOwnedLowerBound = idealExpectedOwned - idealExpectedOwned / 10;
            int expectedOwnedUpperBound = idealExpectedOwned + idealExpectedOwned / 10;
            for (ResourceUnit ru : entry.getValue()) {
                assertTrue(namespaceOwner.containsKey(ru.getResourceId()));
                int ownedNamespaces = namespaceOwner.get(ru.getResourceId());
                assertTrue(ownedNamespaces > expectedOwnedLowerBound || ownedNamespaces < expectedOwnedUpperBound);
            }
        }
    }
}
