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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import com.beust.jcommander.internal.Maps;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerWrapper;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class AntiAffinityNamespaceGroupTest {
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

    private static Object getField(final Object instance, final String fieldName) throws Exception {
        final Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(instance);
    }

    @BeforeMethod
    void setup() throws Exception {
        executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        // Start broker 1
        ServiceConfiguration config1 = new ServiceConfiguration();
        config1.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        config1.setClusterName("use");
        config1.setWebServicePort(Optional.of(0));
        config1.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config1.setBrokerShutdownTimeoutMs(0L);
        config1.setBrokerServicePort(Optional.of(0));
        config1.setFailureDomainsEnabled(true);
        config1.setLoadBalancerEnabled(true);
        config1.setAdvertisedAddress("localhost");
        // Don't want overloaded threshold to affect namespace placement
        config1.setLoadBalancerBrokerOverloadedThresholdPercentage(400);
        createCluster(bkEnsemble.getZkClient(), config1);
        pulsar1 = new PulsarService(config1);
        pulsar1.start();

        primaryHost = String.format("%s:%d", "localhost", pulsar1.getListenPortHTTP().get());
        url1 = new URL("http://127.0.0.1" + ":" + pulsar1.getListenPortHTTP().get());
        admin1 = PulsarAdmin.builder().serviceHttpUrl(url1.toString()).build();

        // Start broker 2
        ServiceConfiguration config2 = new ServiceConfiguration();
        config2.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        config2.setClusterName("use");
        config2.setWebServicePort(Optional.of(0));
        config2.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config2.setBrokerShutdownTimeoutMs(0L);
        config2.setBrokerServicePort(Optional.of(0));
        config2.setFailureDomainsEnabled(true);
        config2.setAdvertisedAddress("localhost");
        // Don't want overloaded threshold to affect namespace placement
        config2.setLoadBalancerBrokerOverloadedThresholdPercentage(400);
        pulsar2 = new PulsarService(config2);
        pulsar2.start();

        secondaryHost = String.format("%s:%d", "localhost", pulsar2.getListenPortHTTP().get());

        url2 = new URL("http://127.0.0.1" + ":" + config2.getWebServicePort().get());
        admin2 = PulsarAdmin.builder().serviceHttpUrl(url2.toString()).build();

        primaryLoadManager = (ModularLoadManagerImpl) getField(pulsar1.getLoadManager().get(), "loadManager");
        secondaryLoadManager = (ModularLoadManagerImpl) getField(pulsar2.getLoadManager().get(), "loadManager");
        nsFactory = new NamespaceBundleFactory(pulsar1, Hashing.crc32());

        Awaitility.await().untilAsserted(() -> {
            assertEquals(pulsar1.getState(), PulsarService.State.Started);
            assertEquals(pulsar2.getState(), PulsarService.State.Started);
        });
    }

    @AfterMethod(alwaysRun = true)
    void shutdown() throws Exception {
        executor.shutdownNow();

        admin1.close();
        admin2.close();

        pulsar2.close();
        pulsar1.close();

        bkEnsemble.stop();
    }

    private void createCluster(ZooKeeper zk, ServiceConfiguration config) throws Exception {
        ZkUtils.createFullPathOptimistic(zk, "/admin/clusters/" + config.getClusterName(),
                ObjectMapperFactory.getThreadLocal().writeValueAsBytes(
                        ClusterData.builder().serviceUrl("http://" + config.getAdvertisedAddress() + ":" + config.getWebServicePort().get()).build()),
                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void testClusterDomain() {

    }

    /**
     *
     * It verifies anti-affinity-namespace assignment with failure-domain
     *
     * <pre>
     * Domain     Brokers-count
     * ________  ____________
     * domain-0   broker-0,broker-1
     * domain-1   broker-2,broker-3
     *
     * Anti-affinity-namespace assignment
     *
     * (1) ns0 -> candidate-brokers: b0, b1, b2, b3 => selected b0
     * (2) ns1 -> candidate-brokers: b2, b3         => selected b2
     * (3) ns2 -> candidate-brokers: b1, b3         => selected b1
     * (4) ns3 -> candidate-brokers: b3             => selected b3
     * (5) ns4 -> candidate-brokers: b0, b1, b2, b3 => selected b0
     *
     * "candidate" broker to own anti-affinity-namespace = b2 or b4
     *
     * </pre>
     *
     */
    @Test
    public void testAntiAffinityNamespaceFilteringWithDomain() throws Exception {

        final String namespace = "my-tenant/use/my-ns";
        final int totalNamespaces = 5;
        final String namespaceAntiAffinityGroup = "my-antiaffinity";
        final String bundle = "/0x00000000_0xffffffff";
        final int totalBrokers = 4;

        pulsar1.getConfiguration().setFailureDomainsEnabled(true);
        admin1.tenants().createTenant("my-tenant",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("use")));

        for (int i = 0; i < totalNamespaces; i++) {
            final String ns = namespace + i;
            admin1.namespaces().createNamespace(ns);
            admin1.namespaces().setNamespaceAntiAffinityGroup(ns, namespaceAntiAffinityGroup);
        }

        Set<String> brokers = Sets.newHashSet();
        Map<String, String> brokerToDomainMap = Maps.newHashMap();
        brokers.add("brokerName-0");
        brokerToDomainMap.put("brokerName-0", "domain-0");
        brokers.add("brokerName-1");
        brokerToDomainMap.put("brokerName-1", "domain-0");
        brokers.add("brokerName-2");
        brokerToDomainMap.put("brokerName-2", "domain-1");
        brokers.add("brokerName-3");
        brokerToDomainMap.put("brokerName-3", "domain-1");

        Set<String> candidate = Sets.newHashSet();
        ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<String>>> brokerToNamespaceToBundleRange = new ConcurrentOpenHashMap<>();

        assertEquals(brokers.size(), totalBrokers);

        String assignedNamespace = namespace + "0" + bundle;
        candidate.addAll(brokers);

        // for namespace-0 all brokers available
        LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar1, assignedNamespace, brokers,
                brokerToNamespaceToBundleRange, brokerToDomainMap);
        assertEquals(brokers.size(), totalBrokers);

        // add namespace-0 to broker-0 of domain-0 => state: n0->b0
        selectBrokerForNamespace(brokerToNamespaceToBundleRange, "brokerName-0", namespace + "0", assignedNamespace);
        candidate.addAll(brokers);
        // for namespace-1 only domain-1 brokers are available as broker-0 already owns namespace-0
        assignedNamespace = namespace + "1" + bundle;
        LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar1, assignedNamespace, candidate,
                brokerToNamespaceToBundleRange, brokerToDomainMap);
        assertEquals(candidate.size(), 2);
        candidate.forEach(broker -> assertEquals(brokerToDomainMap.get(broker), "domain-1"));

        // add namespace-1 to broker-2 of domain-1 => state: n0->b0, n1->b2
        selectBrokerForNamespace(brokerToNamespaceToBundleRange, "brokerName-2", namespace + "1", assignedNamespace);
        candidate.addAll(brokers);
        // for namespace-2 only brokers available are : broker-1 and broker-3
        assignedNamespace = namespace + "2" + bundle;
        LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar1, assignedNamespace, candidate,
                brokerToNamespaceToBundleRange, brokerToDomainMap);
        assertEquals(candidate.size(), 2);
        assertTrue(candidate.contains("brokerName-1"));
        assertTrue(candidate.contains("brokerName-3"));

        // add namespace-2 to broker-1 of domain-0 => state: n0->b0, n1->b2, n2->b1
        selectBrokerForNamespace(brokerToNamespaceToBundleRange, "brokerName-1", namespace + "2", assignedNamespace);
        candidate.addAll(brokers);
        // for namespace-3 only brokers available are : broker-3
        assignedNamespace = namespace + "3" + bundle;
        LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar1, assignedNamespace, candidate,
                brokerToNamespaceToBundleRange, brokerToDomainMap);
        assertEquals(candidate.size(), 1);
        assertTrue(candidate.contains("brokerName-3"));
        // add namespace-3 to broker-3 of domain-1 => state: n0->b0, n1->b2, n2->b1, n3->b3
        selectBrokerForNamespace(brokerToNamespaceToBundleRange, "brokerName-3", namespace + "3", assignedNamespace);
        candidate.addAll(brokers);
        // for namespace-4 only brokers available are : all 4 brokers
        assignedNamespace = namespace + "4" + bundle;
        LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar1, assignedNamespace, candidate,
                brokerToNamespaceToBundleRange, brokerToDomainMap);
        assertEquals(candidate.size(), 4);
    }

    /**
     * It verifies anti-affinity-namespace assignment without failure-domain enabled
     *
     * <pre>
     *  Anti-affinity-namespace assignment
     *
     * (1) ns0 -> candidate-brokers: b0, b1, b2     => selected b0
     * (2) ns1 -> candidate-brokers: b1, b2         => selected b1
     * (3) ns2 -> candidate-brokers: b2             => selected b2
     * (5) ns3 -> candidate-brokers: b0, b1, b2     => selected b0
     * </pre>
     *
     *
     * @throws Exception
     */
    @Test
    public void testAntiAffinityNamespaceFilteringWithoutDomain() throws Exception {

        final String namespace = "my-tenant/use/my-ns";
        final int totalNamespaces = 5;
        final String namespaceAntiAffinityGroup = "my-antiaffinity";
        final String bundle = "/0x00000000_0xffffffff";

        admin1.tenants().createTenant("my-tenant",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("use")));

        for (int i = 0; i < totalNamespaces; i++) {
            final String ns = namespace + i;
            admin1.namespaces().createNamespace(ns);
            admin1.namespaces().setNamespaceAntiAffinityGroup(ns, namespaceAntiAffinityGroup);
        }

        Set<String> brokers = Sets.newHashSet();
        Set<String> candidate = Sets.newHashSet();
        ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<String>>> brokerToNamespaceToBundleRange = new ConcurrentOpenHashMap<>();
        brokers.add("broker-0");
        brokers.add("broker-1");
        brokers.add("broker-2");

        String assignedNamespace = namespace + "0" + bundle;

        // all brokers available so, candidate will be all 3 brokers
        candidate.addAll(brokers);
        LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar1, assignedNamespace, brokers,
                brokerToNamespaceToBundleRange, null);
        assertEquals(brokers.size(), 3);

        // add ns-0 to broker-0
        selectBrokerForNamespace(brokerToNamespaceToBundleRange, "broker-0", namespace + "0", assignedNamespace);
        candidate.addAll(brokers);
        assignedNamespace = namespace + "1" + bundle;
        // available brokers for ns-1 => broker-1, broker-2
        LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar1, assignedNamespace, candidate,
                brokerToNamespaceToBundleRange, null);
        assertEquals(candidate.size(), 2);
        assertTrue(candidate.contains("broker-1"));
        assertTrue(candidate.contains("broker-2"));

        // add ns-1 to broker-1
        selectBrokerForNamespace(brokerToNamespaceToBundleRange, "broker-1", namespace + "1", assignedNamespace);
        candidate.addAll(brokers);
        // available brokers for ns-2 => broker-2
        assignedNamespace = namespace + "2" + bundle;
        LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar1, assignedNamespace, candidate,
                brokerToNamespaceToBundleRange, null);
        assertEquals(candidate.size(), 1);
        assertTrue(candidate.contains("broker-2"));

        // add ns-2 to broker-2
        selectBrokerForNamespace(brokerToNamespaceToBundleRange, "broker-2", namespace + "2", assignedNamespace);
        candidate.addAll(brokers);
        // available brokers for ns-3 => broker-0, broker-1, broker-2
        assignedNamespace = namespace + "3" + bundle;
        LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar1, assignedNamespace, candidate,
                brokerToNamespaceToBundleRange, null);
        assertEquals(candidate.size(), 3);
    }

    private void selectBrokerForNamespace(
            ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<String>>> brokerToNamespaceToBundleRange,
            String broker, String namespace, String assignedBundleName) {
        ConcurrentOpenHashSet<String> bundleSet = new ConcurrentOpenHashSet<>();
        bundleSet.add(assignedBundleName);
        ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<String>> nsToBundleMap = new ConcurrentOpenHashMap<>();
        nsToBundleMap.put(namespace, bundleSet);
        brokerToNamespaceToBundleRange.put(broker, nsToBundleMap);
    }

    /**
     * It verifies anti-affinity with failure domain enabled with 2 brokers.
     *
     * Note: in this class's setup method, the LoadBalancerBrokerOverloadedThresholdPercentage
     * is set to 400 to ensure that the overloaded logic doesn't affect the broker selection.
     * Without that configuration, two namespaces in the same anti-affinity group could
     * be placed on the same broker. The CPU usage can be over 100%, and if we run with more
     * than 4 cores, it could conceivably be above 400%. If this test becomes flaky again,
     * look at the logs to see if there is a mention of an overloaded broker.
     *
     * <pre>
     * 1. Register brokers to domain: domain-1: broker1 & domain-2: broker2
     * 2. Load-Manager receives a watch and updates brokerToDomain cache with new domain data
     * 3. Create two namespace with anti-affinity
     * 4. Load-manager selects broker for each namespace such that from different domains
     *
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testBrokerSelectionForAntiAffinityGroup() throws Exception {

        final String broker1 = primaryHost;
        final String broker2 = secondaryHost;
        final String cluster = pulsar1.getConfiguration().getClusterName();
        final String tenant = "tenant-" + UUID.randomUUID().toString();
        final String namespace1 = tenant + "/" + cluster + "/ns1";
        final String namespace2 = tenant + "/" + cluster + "/ns2";
        final String namespaceAntiAffinityGroup = "group";

        FailureDomain domain1 = FailureDomain.builder()
                .brokers(Collections.singleton(broker1))
                .build();
        admin1.clusters().createFailureDomain(cluster, "domain1", domain1);

        FailureDomain domain2 = FailureDomain.builder()
                .brokers(Collections.singleton(broker2))
                .build();
        admin1.clusters().createFailureDomain(cluster, "domain2", domain2);

        admin1.tenants().createTenant(tenant, new TenantInfoImpl(null, Sets.newHashSet(cluster)));
        admin1.namespaces().createNamespace(namespace1);
        admin1.namespaces().createNamespace(namespace2);

        admin1.namespaces().setNamespaceAntiAffinityGroup(namespace1, namespaceAntiAffinityGroup);
        admin1.namespaces().setNamespaceAntiAffinityGroup(namespace2, namespaceAntiAffinityGroup);

        // validate strategically if brokerToDomainCache updated
        Awaitility.await().untilAsserted(() -> {
            assertTrue(isLoadManagerUpdatedDomainCache(primaryLoadManager));
            assertTrue(isLoadManagerUpdatedDomainCache(secondaryLoadManager));
        });

        ServiceUnitId serviceUnit1 = makeBundle(tenant, cluster, "ns1");
        String selectedBroker1 = primaryLoadManager.selectBrokerForAssignment(serviceUnit1).get();

        ServiceUnitId serviceUnit2 = makeBundle(tenant, cluster, "ns2");
        String selectedBroker2 = primaryLoadManager.selectBrokerForAssignment(serviceUnit2).get();

        assertNotEquals(selectedBroker1, selectedBroker2);

    }

    /**
     * It verifies that load-shedding task should unload namespace only if there is a broker available which doesn't
     * cause uneven anti-affinity namespace distribution.
     *
     * <pre>
     * 1. broker1 owns ns-0 => broker1 can unload ns-0
     * 1. broker2 owns ns-1 => broker1 can unload ns-0
     * 1. broker3 owns ns-2 => broker1 can't unload ns-0 as all brokers have same no NS
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testLoadSheddingUtilWithAntiAffinityNamespace() throws Exception {

        final String namespace = "my-tenant/use/my-ns";
        final int totalNamespaces = 5;
        final String namespaceAntiAffinityGroup = "my-antiaffinity";
        final String bundle = "/0x00000000_0xffffffff";

        admin1.tenants().createTenant("my-tenant",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("use")));

        for (int i = 0; i < totalNamespaces; i++) {
            final String ns = namespace + i;
            admin1.namespaces().createNamespace(ns);
            admin1.namespaces().setNamespaceAntiAffinityGroup(ns, namespaceAntiAffinityGroup);
        }

        Set<String> brokers = Sets.newHashSet();
        Set<String> candidate = Sets.newHashSet();
        ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<String>>> brokerToNamespaceToBundleRange = new ConcurrentOpenHashMap<>();
        brokers.add("broker-0");
        brokers.add("broker-1");
        brokers.add("broker-2");

        String assignedNamespace = namespace + "0" + bundle;

        // all brokers available so, candidate will be all 3 brokers
        candidate.addAll(brokers);
        // add ns-0 to broker-0
        selectBrokerForNamespace(brokerToNamespaceToBundleRange, "broker-0", namespace + "0", assignedNamespace);
        String currentBroker = "broker-0";
        boolean shouldUnload = LoadManagerShared.shouldAntiAffinityNamespaceUnload(namespace + "0", bundle,
                currentBroker, pulsar1, brokerToNamespaceToBundleRange, candidate);
        assertTrue(shouldUnload);
        // add ns-1 to broker-1
        selectBrokerForNamespace(brokerToNamespaceToBundleRange, "broker-1", namespace + "1", assignedNamespace);
        shouldUnload = LoadManagerShared.shouldAntiAffinityNamespaceUnload(namespace + "0", bundle, currentBroker,
                pulsar1, brokerToNamespaceToBundleRange, candidate);
        assertTrue(shouldUnload);
        // add ns-2 to broker-2
        selectBrokerForNamespace(brokerToNamespaceToBundleRange, "broker-2", namespace + "2", assignedNamespace);
        shouldUnload = LoadManagerShared.shouldAntiAffinityNamespaceUnload(namespace + "0", bundle, currentBroker,
                pulsar1, brokerToNamespaceToBundleRange, candidate);
        assertFalse(shouldUnload);

    }

    /**
     * It verifies that load-manager::shouldAntiAffinityNamespaceUnload checks that unloading should only happen if all
     * brokers have same number of anti-affinity namespaces
     *
     * @throws Exception
     */
    @Test
    public void testLoadSheddingWithAntiAffinityNamespace() throws Exception {

        final String namespace = "my-tenant/use/my-ns";
        final int totalNamespaces = 5;
        final String namespaceAntiAffinityGroup = "my-antiaffinity";
        final String bundle = "0x00000000_0xffffffff";

        admin1.tenants().createTenant("my-tenant",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("use")));

        for (int i = 0; i < totalNamespaces; i++) {
            final String ns = namespace + i;
            admin1.namespaces().createNamespace(ns);
            admin1.namespaces().setNamespaceAntiAffinityGroup(ns, namespaceAntiAffinityGroup);
        }

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsar1.getSafeWebServiceAddress()).build();
        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://" + namespace + "0/my-topic1")
                .create();
        ModularLoadManagerImpl loadManager = (ModularLoadManagerImpl) ((ModularLoadManagerWrapper) pulsar1
                .getLoadManager().get()).getLoadManager();

        pulsar1.getBrokerService().updateRates();
        loadManager.updateAll();

        assertTrue(loadManager.shouldAntiAffinityNamespaceUnload(namespace + "0", bundle, primaryHost));
        producer.close();
    }

    private boolean isLoadManagerUpdatedDomainCache(ModularLoadManagerImpl loadManager) throws Exception {
        Field mapField = ModularLoadManagerImpl.class.getDeclaredField("brokerToFailureDomainMap");
        mapField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, String> map = (Map<String, String>) mapField.get(loadManager);
        return !map.isEmpty();
    }

    private NamespaceBundle makeBundle(final String property, final String cluster, final String namespace) {
        return nsFactory.getBundle(NamespaceName.get(property, cluster, namespace),
                Range.range(NamespaceBundles.FULL_LOWER_BOUND, BoundType.CLOSED, NamespaceBundles.FULL_UPPER_BOUND,
                        BoundType.CLOSED));
    }

}
