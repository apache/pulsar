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
package org.apache.pulsar.tests.integration.loadbalance;

import static org.apache.pulsar.tests.integration.containers.PulsarContainer.BROKER_HTTP_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Integration tests for Pulsar ExtensibleLoadManagerImpl.
 */
@Slf4j
public class ExtensibleLoadManagerTest extends TestRetrySupport {

    private static final int NUM_BROKERS = 3;
    private static final String DEFAULT_TENANT = "my-tenant";
    private static final String DEFAULT_NAMESPACE = DEFAULT_TENANT + "/my-namespace";
    private static final String nsSuffix = "-anti-affinity-enabled";

    private final String clusterName = "MultiLoadManagerTest-" + UUID.randomUUID();
    private final PulsarClusterSpec spec = PulsarClusterSpec.builder()
            .clusterName(clusterName)
            .numBrokers(NUM_BROKERS).build();
    private PulsarCluster pulsarCluster = null;
    private List<String> brokerUrls = null;
    private String hosts;
    private PulsarAdmin admin;

    @BeforeClass(alwaysRun = true)
    public void setup() throws Exception {
        incrementSetupNumber();
        Map<String, String> brokerEnvs = new HashMap<>();
        brokerEnvs.put("loadManagerClassName",
                "org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl");
        brokerEnvs.put("loadBalancerLoadSheddingStrategy",
                "org.apache.pulsar.broker.loadbalance.extensions.scheduler.TransferShedder");
        brokerEnvs.put("forceDeleteNamespaceAllowed", "true");
        brokerEnvs.put("loadBalancerDebugModeEnabled", "true");
        brokerEnvs.put("PULSAR_MEM", "-Xmx512M");
        spec.brokerEnvs(brokerEnvs);
        pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();
        brokerUrls = brokerUrls();

        hosts = pulsarCluster.getAllBrokersHttpServiceUrl();
        admin = PulsarAdmin.builder().serviceHttpUrl(hosts).build();
        // all brokers alive
        assertEquals(admin.brokers().getActiveBrokers(clusterName).size(), NUM_BROKERS);

        admin.tenants().createTenant(DEFAULT_TENANT,
                new TenantInfoImpl(new HashSet<>(), Set.of(pulsarCluster.getClusterName())));
        admin.namespaces().createNamespace(DEFAULT_NAMESPACE, 100);
    }

    @AfterClass(alwaysRun = true)
    public void cleanup() {
        markCurrentSetupNumberCleaned();
        if (pulsarCluster != null) {
            pulsarCluster.stop();
            pulsarCluster = null;
        }
        if (admin != null) {
            admin.close();
            admin = null;
        }
    }

    @BeforeMethod(alwaysRun = true)
    public void startBroker() {
        if (pulsarCluster != null) {
            pulsarCluster.getBrokers().forEach(brokerContainer -> {
                if (!brokerContainer.isRunning()) {
                    brokerContainer.start();
                }
            });
            String topicName = "persistent://" + DEFAULT_NAMESPACE + "/startBrokerCheck";
            Awaitility.await().atMost(120, TimeUnit.SECONDS).ignoreExceptions().until(
                    () -> {
                        for (BrokerContainer brokerContainer : pulsarCluster.getBrokers()) {
                            try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(
                                    brokerContainer.getHttpServiceUrl()).build()) {
                                if (admin.brokers().getActiveBrokers(clusterName).size() != NUM_BROKERS) {
                                    return false;
                                }
                                try {
                                    admin.topics().createPartitionedTopic(topicName, 10);
                                } catch (PulsarAdminException.ConflictException e) {
                                    // expected
                                }
                                admin.lookups().lookupPartitionedTopic(topicName);
                            }
                        }
                        return true;
                    }
            );
        }
    }

    @Test(timeOut = 40 * 1000)
    public void testConcurrentLookups() throws Exception {
        String topicName = "persistent://" + DEFAULT_NAMESPACE + "/testConcurrentLookups";
        List<PulsarAdmin> admins = new ArrayList<>();
        int numAdminForBroker = 10;
        for (String url : brokerUrls) {
            for (int i = 0; i < numAdminForBroker; i++) {
                admins.add(PulsarAdmin.builder().serviceHttpUrl(url).build());
            }
        }

        admin.topics().createPartitionedTopic(topicName, 100);

        var executor = Executors.newFixedThreadPool(admins.size());

        CountDownLatch latch = new CountDownLatch(admins.size());
        List<Map<String, String>> result = new CopyOnWriteArrayList<>();
        for(var admin : admins) {
            executor.execute(() -> {
                try {
                    result.add(admin.lookups().lookupPartitionedTopic(topicName));
                } catch (PulsarAdminException e) {
                    log.error("Lookup partitioned topic failed.", e);
                }
                latch.countDown();
            });
        }
        latch.await();

        assertEquals(result.size(), admins.size());

        for (int i = 1; i < admins.size(); i++) {
            assertEquals(result.get(i - 1), result.get(i));
        }
        admins.forEach(a -> a.close());
        executor.shutdown();
    }

    @Test(timeOut = 30 * 1000)
    public void testTransferAdminApi() throws Exception {
        String topicName = "persistent://" + DEFAULT_NAMESPACE + "/testUnloadAdminApi";
        admin.topics().createNonPartitionedTopic(topicName);
        String broker = admin.lookups().lookupTopic(topicName);

        int index = extractBrokerIndex(broker);

        String bundleRange = admin.lookups().getBundleRange(topicName);

        // Test transfer to current broker.
        try {
            admin.namespaces().unloadNamespaceBundle(DEFAULT_NAMESPACE, bundleRange, getBrokerUrl(index));
            fail();
        } catch (PulsarAdminException ex) {
            assertTrue(ex.getMessage().contains("cannot be transfer to same broker"));
        }

        int transferToIndex = generateRandomExcludingX(NUM_BROKERS, index);
        assertNotEquals(transferToIndex, index);
        String transferTo = getBrokerUrl(transferToIndex);
        admin.namespaces().unloadNamespaceBundle(DEFAULT_NAMESPACE, bundleRange, transferTo);

        broker = admin.lookups().lookupTopic(topicName);

        index = extractBrokerIndex(broker);
        assertEquals(index, transferToIndex);
    }

    @Test(timeOut = 30 * 1000)
    public void testSplitBundleAdminApi() throws Exception {
        String topicName = "persistent://" + DEFAULT_NAMESPACE + "/testSplitBundleAdminApi";
        admin.topics().createNonPartitionedTopic(topicName);
        String broker = admin.lookups().lookupTopic(topicName);
        log.info("The topic: {} owned by {}", topicName, broker);
        BundlesData bundles = admin.namespaces().getBundles(DEFAULT_NAMESPACE);
        int numBundles = bundles.getNumBundles();
        var bundleRanges = bundles.getBoundaries().stream().map(Long::decode).sorted().toList();
        String firstBundle = bundleRanges.get(0) + "_" + bundleRanges.get(1);
        admin.namespaces().splitNamespaceBundle(DEFAULT_NAMESPACE, firstBundle, true, null);
        BundlesData bundlesData = admin.namespaces().getBundles(DEFAULT_NAMESPACE);

        long mid = bundleRanges.get(0) + (bundleRanges.get(1) - bundleRanges.get(0)) / 2;

        assertEquals(bundlesData.getNumBundles(), numBundles + 1);
        String lowBundle = String.format("0x%08x", bundleRanges.get(0));
        String midBundle = String.format("0x%08x", mid);
        String highBundle = String.format("0x%08x", bundleRanges.get(1));
        assertTrue(bundlesData.getBoundaries().contains(lowBundle));
        assertTrue(bundlesData.getBoundaries().contains(midBundle));
        assertTrue(bundlesData.getBoundaries().contains(highBundle));

        // Test split bundle with invalid bundle range.
        try {
            admin.namespaces().splitNamespaceBundle(DEFAULT_NAMESPACE, "invalid", true, null);
            fail();
        } catch (PulsarAdminException ex) {
            assertTrue(ex.getMessage().contains("Invalid bundle range"));
        }
    }

    @Test(timeOut = 30 * 1000)
    public void testDeleteNamespace() throws Exception {
        String namespace = DEFAULT_TENANT + "/test-delete-namespace";
        String topicName = "persistent://" + namespace + "/test-delete-namespace-topic";
        admin.namespaces().createNamespace(namespace);
        admin.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(clusterName));
        assertTrue(admin.namespaces().getNamespaces(DEFAULT_TENANT).contains(namespace));
        admin.topics().createPartitionedTopic(topicName, 2);
        String broker = admin.lookups().lookupTopic(topicName);
        log.info("The topic: {} owned by: {}", topicName, broker);
        admin.namespaces().deleteNamespace(namespace, true);
        assertFalse(admin.namespaces().getNamespaces(DEFAULT_TENANT).contains(namespace));
    }

    @Test(timeOut = 120 * 1000)
    public void testStopBroker() throws Exception {
        String topicName = "persistent://" + DEFAULT_NAMESPACE + "/test-stop-broker-topic";

        admin.topics().createNonPartitionedTopic(topicName);
        String broker = admin.lookups().lookupTopic(topicName);
        log.info("The topic: {} owned by: {}", topicName, broker);

        int idx = extractBrokerIndex(broker);
        for (BrokerContainer container : pulsarCluster.getBrokers()) {
            String name = container.getHostName();
            if (name.contains(String.valueOf(idx))) {
                container.stop();
            }
        }

        Awaitility.waitAtMost(60, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
            String broker1 = admin.lookups().lookupTopic(topicName);
            assertNotEquals(broker1, broker);
        });

    }

    @Test(timeOut = 40 * 1000)
    public void testAntiAffinityPolicy() throws PulsarAdminException {
        final String namespaceAntiAffinityGroup = "my-anti-affinity-filter";
        final String antiAffinityEnabledNameSpace = DEFAULT_TENANT + "/my-ns-filter" + nsSuffix;
        final int numPartition = 20;

        List<String> activeBrokers = admin.brokers().getActiveBrokers();

        assertEquals(activeBrokers.size(), NUM_BROKERS);

        Set<String> antiAffinityEnabledNameSpacesReq = new HashSet<>();
        for (int i = 0; i < activeBrokers.size(); i++) {
            String namespace = antiAffinityEnabledNameSpace + "-" + i;
            antiAffinityEnabledNameSpacesReq.add(namespace);
            admin.namespaces().createNamespace(namespace, 1);
            admin.namespaces().setNamespaceAntiAffinityGroup(namespace, namespaceAntiAffinityGroup);
            admin.clusters().createFailureDomain(clusterName, namespaceAntiAffinityGroup, FailureDomain.builder()
                    .brokers(Set.of(activeBrokers.get(i))).build());
            String namespaceAntiAffinityGroupResp = admin.namespaces().getNamespaceAntiAffinityGroup(namespace);
            assertEquals(namespaceAntiAffinityGroupResp, namespaceAntiAffinityGroup);
            FailureDomain failureDomainResp =
                    admin.clusters().getFailureDomain(clusterName, namespaceAntiAffinityGroup);
            assertEquals(failureDomainResp.getBrokers(), Set.of(activeBrokers.get(i)));
        }

        List<String> antiAffinityNamespacesResp =
                admin.namespaces().getAntiAffinityNamespaces(DEFAULT_TENANT, clusterName, namespaceAntiAffinityGroup);
        assertEquals(new HashSet<>(antiAffinityNamespacesResp), antiAffinityEnabledNameSpacesReq);

        Set<String> result = new HashSet<>();
        for (int i = 0; i < activeBrokers.size(); i++) {
            final String topic = "persistent://" + antiAffinityEnabledNameSpace + "-" + i +"/topic";
            admin.topics().createPartitionedTopic(topic, numPartition);

            Map<String, String> topicToBroker = admin.lookups().lookupPartitionedTopic(topic);

            assertEquals(topicToBroker.size(), numPartition);

            HashSet<String> brokers = new HashSet<>(topicToBroker.values());

            assertEquals(brokers.size(), 1);
            result.add(brokers.iterator().next());
            log.info("Topic: {}, lookup result: {}", topic, brokers.iterator().next());
        }

        assertEquals(result.size(), NUM_BROKERS);
    }

    @Test(timeOut = 300 * 1000)
    public void testIsolationPolicy() throws Exception {
        final String namespaceIsolationPolicyName = "my-isolation-policy";
        final String isolationEnabledNameSpace = DEFAULT_TENANT + "/my-isolation-policy" + nsSuffix;
        Map<String, String> parameters1 = new HashMap<>();
        parameters1.put("min_limit", "1");
        parameters1.put("usage_threshold", "100");

        Awaitility.await().atMost(10, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(
                () -> {
                    List<String> activeBrokers = admin.brokers().getActiveBrokersAsync()
                            .get(5, TimeUnit.SECONDS);
                    assertEquals(activeBrokers.size(), NUM_BROKERS);
                }
        );
        try {
            admin.namespaces().createNamespace(isolationEnabledNameSpace);
        } catch (PulsarAdminException.ConflictException e) {
            //expected when retried
        }

        try {
            admin.clusters()
                    .createNamespaceIsolationPolicy(clusterName, namespaceIsolationPolicyName, NamespaceIsolationData
                            .builder()
                            .namespaces(List.of(isolationEnabledNameSpace))
                            .autoFailoverPolicy(AutoFailoverPolicyData.builder()
                                    .policyType(AutoFailoverPolicyType.min_available)
                                    .parameters(parameters1)
                                    .build())
                            .primary(List.of(getHostName(0)))
                            .secondary(List.of(getHostName(1)))
                            .build());
        } catch (PulsarAdminException.ConflictException e) {
            //expected when retried
        }

        final String topic = "persistent://" + isolationEnabledNameSpace + "/topic";
        try {
            admin.topics().createNonPartitionedTopic(topic);
        } catch (PulsarAdminException.ConflictException e) {
            //expected when retried
        }

        String broker = admin.lookups().lookupTopic(topic);
        assertEquals(extractBrokerIndex(broker), 0);

        for (BrokerContainer container : pulsarCluster.getBrokers()) {
            String name = container.getHostName();
            if (name.contains("0")) {
                container.stop();
            }
        }

        Awaitility.await().atMost(60, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(
                () -> {
                    List<String> activeBrokers = admin.brokers().getActiveBrokersAsync()
                            .get(5, TimeUnit.SECONDS);
                    assertEquals(activeBrokers.size(), 2);
                }
        );

        Awaitility.await().atMost(60, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
            String ownerBroker = admin.lookups().lookupTopicAsync(topic).get(5, TimeUnit.SECONDS);
            assertEquals(extractBrokerIndex(ownerBroker), 1);
        });

        for (BrokerContainer container : pulsarCluster.getBrokers()) {
            String name = container.getHostName();
            if (name.contains("1")) {
                container.stop();
            }
        }

        Awaitility.await().atMost(60, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(
            () -> {
                List<String> activeBrokers = admin.brokers().getActiveBrokersAsync().get(5, TimeUnit.SECONDS);
                assertEquals(activeBrokers.size(), 1);
            }
        );

        Awaitility.await().atMost(60, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(
                () -> {
                    try {
                        admin.lookups().lookupTopicAsync(topic).get(5, TimeUnit.SECONDS);
                        fail();
                    } catch (Exception ex) {
                        log.error("Failed to lookup topic: ", ex);
                        assertThat(ex.getMessage()).contains("Service Unavailable");
                    }
                }
        );
    }

    private String getBrokerUrl(int index) {
        return String.format("pulsar-broker-%d:%d", index, BROKER_HTTP_PORT);
    }

    private String getHostName(int index) {
        return String.format("pulsar-broker-%d", index);
    }

    private int extractBrokerIndex(String broker) {
        String pattern = "pulsar://.*-(\\d+):\\d+";
        Pattern compiledPattern = Pattern.compile(pattern);
        Matcher matcher = compiledPattern.matcher(broker);
        if (!matcher.find()){
            throw new IllegalArgumentException("Failed to extract broker index");
        }
        return Integer.parseInt(matcher.group(1));
    }

    private int generateRandomExcludingX(int n, int x) {
        Random random = new Random();
        int randomNumber;

        do {
            randomNumber = random.nextInt(n);
        } while (randomNumber == x);

        return randomNumber;
    }

    private List<String> brokerUrls() {
        Collection<BrokerContainer> brokers = pulsarCluster.getBrokers();
        List<String> brokerUrls = new ArrayList<>(NUM_BROKERS);
        brokers.forEach(broker -> {
            brokerUrls.add("http://" + broker.getHost() + ":" + broker.getMappedPort(BROKER_HTTP_PORT));
        });
        return brokerUrls;
    }
}
