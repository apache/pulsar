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
import static org.apache.pulsar.tests.integration.topologies.PulsarCluster.ADMIN_SCRIPT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.ArrayList;
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
import org.apache.pulsar.common.policies.data.impl.BundlesDataImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
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
        brokerEnvs.put("topicLevelPoliciesEnabled", "false");
        brokerEnvs.put("PULSAR_MEM", "-Xmx512M");
        spec.brokerEnvs(brokerEnvs);
        pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();

        // all brokers alive
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    List<String> activeBrokers = getActiveBrokers();
                    assertEquals(activeBrokers.size(), NUM_BROKERS);
                }
        );
        runAdminCommandOnRunningBroker("tenants", "create", DEFAULT_TENANT, "-c", pulsarCluster.getClusterName());
        createNamespace(DEFAULT_NAMESPACE, "100");
    }

    @AfterClass(alwaysRun = true)
    public void cleanup() {
        markCurrentSetupNumberCleaned();
        if (pulsarCluster != null) {
            pulsarCluster.stop();
            pulsarCluster = null;
        }
    }

    @BeforeMethod(alwaysRun = true)
    public void startBroker() throws Exception {
        if (pulsarCluster != null) {
            for (int i = 0; i < NUM_BROKERS; i++) {
                BrokerContainer broker = pulsarCluster.getBroker(i);
                if (!broker.isRunning()) {
                    // If have one broker down, restart entire cluster.
                    log.info("Restart entire cluster.");
                    this.cleanup();
                    this.setup();
                    break;
                }
            }
        }
        // Make sure all broker are available.
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    List<String> activeBrokers = getActiveBrokers();
                    assertEquals(activeBrokers.size(), NUM_BROKERS);
                }
        );
    }

    @Test(timeOut = 40 * 1000)
    public void testConcurrentLookups() throws Exception {
        String topicName = "persistent://" + DEFAULT_NAMESPACE + "/testConcurrentLookups";

        pulsarCluster.createPartitionedTopic(topicName, 100);

        var executor = Executors.newFixedThreadPool(NUM_BROKERS);

        CountDownLatch latch = new CountDownLatch(NUM_BROKERS);
        List<Map<String, String>> result = new CopyOnWriteArrayList<>();
        for(var broker : pulsarCluster.getBrokers()) {
            executor.execute(() -> {
                try {
                    result.add(lookupPartitionedTopic(broker, topicName));
                } catch (Exception e) {
                    log.error("Lookup partitioned topic failed.", e);
                }
                latch.countDown();
            });
        }
        latch.await();

        assertEquals(result.size(), NUM_BROKERS);

        for (int i = 1; i < NUM_BROKERS; i++) {
            assertEquals(result.get(i - 1), result.get(i));
        }
        executor.shutdown();
    }

    @Test(timeOut = 30 * 1000)
    public void testTransferAdminApi() throws Exception {
        String topicName = "persistent://" + DEFAULT_NAMESPACE + "/testUnloadAdminApi";
        createNonPartitionedTopicAndRetry(topicName);
        String broker = lookupTopic(topicName);

        int index = extractBrokerIndex(broker);

        String bundleRange = getBundleRange(topicName);

        // Test transfer to current broker.
        try {
            ContainerExecResult result = runAdminCommandOnRunningBroker("namespaces", "unload", "--bundle", bundleRange, "--destinationBroker", getBrokerUrl(index), DEFAULT_NAMESPACE);
            assertNotEquals(result.getExitCode(), 0);
        }catch (ContainerExecException e) {
            log.info("Transfer to current broker failed with ContainerExecException, could be ok", e);
            if (!e.getMessage().contains("with error code 1")) {
                fail("Expected different error code");
            }
        }

        int transferToIndex = generateRandomExcludingX(NUM_BROKERS, index);
        assertNotEquals(transferToIndex, index);
        String transferTo = getBrokerUrl(transferToIndex);
        runAdminCommandOnRunningBroker("namespaces", "unload", "--bundle", bundleRange, "--destinationBroker", transferTo, DEFAULT_NAMESPACE);

        broker = lookupTopic(topicName);

        index = extractBrokerIndex(broker);
        assertEquals(index, transferToIndex);
    }

    @Test
    public void testSplitBundleAdminApi() throws Exception {
        String topicName = "persistent://" + DEFAULT_NAMESPACE + "/testSplitBundleAdminApi";
        createNonPartitionedTopicAndRetry(topicName);
        String broker = lookupTopic(topicName);
        log.info("The topic: {} owned by {}", topicName, broker);
        ContainerExecResult result = runAdminCommandOnRunningBroker("namespaces", "bundles", DEFAULT_NAMESPACE);
        assertEquals(result.getExitCode(), 0);
        String stdout = result.getStdout();
        BundlesDataImpl bundles = ObjectMapperFactory
                .getMapper()
                .getObjectMapper()
                .readValue(stdout, BundlesDataImpl.class);

        int numBundles = bundles.getNumBundles();
        var bundleRanges = bundles.getBoundaries().stream().map(Long::decode).sorted().toList();
        String firstBundle = bundleRanges.get(0) + "_" + bundleRanges.get(1);
        result = runAdminCommandOnRunningBroker("namespaces", "split-bundle", DEFAULT_NAMESPACE,
                "--bundle", firstBundle,
                "--unload");
        assertEquals(result.getExitCode(), 0);
        long mid = bundleRanges.get(0) + (bundleRanges.get(1) - bundleRanges.get(0)) / 2;
        Awaitility.waitAtMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .untilAsserted(
                () -> {
                    ContainerExecResult res =
                            runAdminCommandOnRunningBroker("namespaces", "bundles", DEFAULT_NAMESPACE);
                    assertEquals(res.getExitCode(), 0);
                    String json = res.getStdout();
                    BundlesDataImpl bundlesData = ObjectMapperFactory
                            .getMapper()
                            .getObjectMapper()
                            .readValue(json, BundlesDataImpl.class);
                    assertEquals(bundlesData.getNumBundles(), numBundles + 1);
                    String lowBundle = String.format("0x%08x", bundleRanges.get(0));
                    String midBundle = String.format("0x%08x", mid);
                    String highBundle = String.format("0x%08x", bundleRanges.get(1));
                    assertTrue(bundlesData.getBoundaries().contains(lowBundle));
                    assertTrue(bundlesData.getBoundaries().contains(midBundle));
                    assertTrue(bundlesData.getBoundaries().contains(highBundle));
                }
        );

        // Test split bundle with invalid bundle range.
        try {
            result = runAdminCommandOnRunningBroker("namespaces", "split-bundle", DEFAULT_NAMESPACE,
                    "--bundle", "invalid",
                    "--unload");
            assertNotEquals(result.getExitCode(), 0);
            String stderr = result.getStderr();
            assertTrue(stderr.contains("Invalid bundle range"));
        } catch (Exception ex) {
            log.info("Split bundle with invalid bundle range failed with exception, could be ok", ex);
        }
    }

    @Test(timeOut = 30 * 1000)
    public void testDeleteNamespace() throws Exception {
        String namespace = DEFAULT_TENANT + "/test-delete-namespace";
        String topicName = "persistent://" + namespace + "/test-delete-namespace-topic";
        createNamespace(namespace, "10");
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "set-clusters", namespace, "-c", clusterName);
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "list", DEFAULT_TENANT);
        assertEquals(result.getExitCode(), 0);
        assertTrue(result.getStdout().contains(namespace));
        pulsarCluster.createPartitionedTopic(topicName, 2);
        String broker = lookupTopic(topicName);
        log.info("The topic: {} owned by: {}", topicName, broker);
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "delete", namespace, "-f");
        result = pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "list", DEFAULT_TENANT);
        assertEquals(result.getExitCode(), 0);
        assertFalse(result.getStdout().contains(namespace));
    }

    @Test(timeOut = 120 * 1000)
    public void testStopBroker() throws Exception {
        String topicName = "persistent://" + DEFAULT_NAMESPACE + "/test-stop-broker-topic";

        createNonPartitionedTopicAndRetry(topicName);
        String broker = lookupTopic(topicName);
        log.info("The topic: {} owned by: {}", topicName, broker);

        int idx = extractBrokerIndex(broker);
        for (BrokerContainer container : pulsarCluster.getBrokers()) {
            String name = container.getHostName();
            if (name.contains(String.valueOf(idx))) {
                try {
                    runCommandOnBrokerWithScript(container, ADMIN_SCRIPT, "brokers", "shutdown",
                            "-m", "10", "-f");
                } catch (Exception ignore) {
                    // ignore
                }
                container.stop();
            }
        }

        String broker1 = lookupTopic(topicName);

        assertNotEquals(broker1, broker);
    }

    @Test(timeOut = 80 * 1000)
    public void testAntiaffinityPolicy() throws Exception {
        final String namespaceAntiAffinityGroup = "my-anti-affinity-filter";
        final String antiAffinityEnabledNameSpace = DEFAULT_TENANT + "/my-ns-filter" + nsSuffix;
        final int numPartition = 20;

        List<String> activeBrokers = getActiveBrokers();

        assertEquals(activeBrokers.size(), NUM_BROKERS);

        for (int i = 0; i < activeBrokers.size(); i++) {
            String namespace = antiAffinityEnabledNameSpace + "-" + i;
            createNamespace(namespace, "10");
            runAdminCommandOnRunningBroker("namespaces", "set-anti-affinity-group", namespace, "-g", namespaceAntiAffinityGroup);
            runAdminCommandOnRunningBroker("clusters", "create-failure-domain", clusterName,
                    "--domain-name", namespaceAntiAffinityGroup,
                    "--broker-list", activeBrokers.get(i));
        }

        log.info("Active brokers: {}", activeBrokers);
        Set<String> result = new HashSet<>();
        for (int i = 0; i < activeBrokers.size(); i++) {
            final String topic = "persistent://" + antiAffinityEnabledNameSpace + "-" + i +"/topic";
            pulsarCluster.createPartitionedTopic(topic, numPartition);

            long startTime = System.currentTimeMillis();
            log.info("Start lookup partitioned topic: {}, {}", topic, startTime);
            Map<String, String> topicToBroker = lookupPartitionedTopic(topic);
            log.info("Finish {} lookup {} ms", topic, System.currentTimeMillis() - startTime);

            assertEquals(topicToBroker.size(), numPartition);

            HashSet<String> brokers = new HashSet<>(topicToBroker.values());

            assertEquals(brokers.size(), 1);
            result.add(brokers.iterator().next());
            log.info("Topic: {}, lookup result: {}", topic, brokers.iterator().next());
        }

        assertEquals(result.size(), NUM_BROKERS);
    }

    @Test(timeOut = 240 * 1000)
    public void testIsolationPolicy() throws Exception {
        final String namespaceIsolationPolicyName = "my-isolation-policy";
        final String isolationEnabledNameSpace = DEFAULT_TENANT + "/my-isolation-policy" + nsSuffix;
        Map<String, String> parameters1 = new HashMap<>();
        parameters1.put("min_limit", "1");
        parameters1.put("usage_threshold", "100");

        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    List<String> activeBrokers = getActiveBrokers();
                    assertEquals(activeBrokers.size(), NUM_BROKERS);
                }
        );
        createNamespace(isolationEnabledNameSpace, "10");

        ContainerExecResult result = runAdminCommandOnRunningBroker("ns-isolation-policy", "set",
                "--auto-failover-policy-type", "min_available",
                "--auto-failover-policy-params", "min_limit=1,usage_threshold=100",
                "--primary", getHostName(0),
                "--secondary", getHostName(1),
                "--namespaces", isolationEnabledNameSpace,
                clusterName, namespaceIsolationPolicyName);
        assertEquals(result.getExitCode(), 0);

        final String topic = "persistent://" + isolationEnabledNameSpace + "/topic";
        createNonPartitionedTopicAndRetry(topic);

        Awaitility.await().atMost(30, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
            String broker = lookupTopic(topic);
            // This isolated topic should be assigned to the primary broker, broker-0
            assertEquals(extractBrokerIndex(broker), 0);
        });

        for (BrokerContainer container : pulsarCluster.getBrokers()) {
            String name = container.getHostName();
            if (name.contains("0")) {
                try {
                    runCommandOnBrokerWithScript(container, ADMIN_SCRIPT, "brokers", "shutdown",
                            "-m", "10", "-f");
                } catch (Exception ignore) {
                    // ignore
                }
                container.stop();
            }
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    List<String> activeBrokers = getActiveBrokers();
                    assertEquals(activeBrokers.size(), 2);
                }
        );

        Awaitility.await().atMost(30, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
            String broker = lookupTopic(topic);
            // This isolated topic should be assigned to the secondary broker, broker-1
            assertEquals(extractBrokerIndex(broker), 1);
        });

        for (BrokerContainer container : pulsarCluster.getBrokers()) {
            String name = container.getHostName();
            if (name.contains("1")) {
                try {
                    runCommandOnBrokerWithScript(container, ADMIN_SCRIPT, "brokers", "shutdown",
                            "-m", "10", "-f");
                } catch (Exception ignore) {
                    // ignore
                }
                container.stop();
            }
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    List<String> activeBrokers = getActiveBrokers();
                    assertEquals(activeBrokers.size(), 1);
                }
        );

        try {
            lookupTopic(topic);
            fail();
        } catch (Exception ex) {
            log.error("Failed to lookup topic: ", ex);
        }
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

    public ContainerExecResult createNamespace(String nsName, String bundles) throws Exception {
        return runAdminCommandOnRunningBroker("namespaces", "create", nsName, "-b", bundles, "-c",
                pulsarCluster.getClusterName());
    }

    private String getBundleRange(String topicName) throws Exception {
        ContainerExecResult result = runAdminCommandOnRunningBroker("topics", "bundle-range", topicName);
        if (result.getExitCode() != 0) {
            log.info(result.getStderr());
            throw new Exception("Failed to get bundle range: " + topicName);
        }
        String stdout = result.getStdout();
        return stdout.trim();
    }

    private Map<String, String> lookupPartitionedTopic(String topicName) throws Exception {
        return lookupPartitionedTopic(getRunningBroker(), topicName);
    }

    private Map<String, String> lookupPartitionedTopic(BrokerContainer container,
                                                       String topicName) throws Exception {
        ContainerExecResult result =
                runCommandOnBrokerWithScript(container, ADMIN_SCRIPT, "topics", "partitioned-lookup", topicName);
        if (result.getExitCode() != 0) {
            log.info(result.getStderr());
            throw new Exception(result.getStderr());
        }
        String stdout = result.getStdout();

        Map<String, String> dataMap = new HashMap<>();
        String[] lines = stdout.split("\\n");
        for (String line : lines) {
            String[] parts = line.split("\\s+");
            if (parts.length == 2) {
                dataMap.put(parts[0], parts[1]);
            }
        }
        return dataMap;
    }

    private String lookupTopic(String topicName) throws Exception {
        ContainerExecResult result = runAdminCommandOnRunningBroker("topics", "lookup", topicName);
        if (result.getExitCode() != 0) {
            log.info(result.getStderr());
            throw new Exception(result.getStderr());
        }
        String stdout = result.getStdout();
        return stdout.trim();
    }

    private void createNonPartitionedTopicAndRetry(String topicName) throws Exception {
        runAdminCommandOnRunningBroker("topics", "create", topicName);
    }

    private List<String> getActiveBrokers() throws Exception {
        List<String> brokers = new ArrayList<>();
        ContainerExecResult result = runAdminCommandOnRunningBroker("brokers", "list", pulsarCluster.getClusterName());
        String stdout = result.getStdout();
        String[] lines = stdout.split("\n");
        for (String line : lines) {
            brokers.add(line.trim());
        }
        brokers.sort(String::compareTo);
        return brokers;
    }

    public ContainerExecResult runAdminCommandOnRunningBroker(String...commands) throws Exception {
        return runCommandOnRunningBrokerWithScript(ADMIN_SCRIPT, commands);
    }

    private ContainerExecResult runCommandOnRunningBrokerWithScript(String scriptType, String...commands)
            throws Exception {
        return runCommandOnBrokerWithScript(getRunningBroker(), scriptType, commands);
    }

    private ContainerExecResult runCommandOnBrokerWithScript(BrokerContainer container,
                                                             String scriptType,
                                                             String...commands)
            throws Exception {
        String[] cmds = new String[commands.length + 1];
        cmds[0] = scriptType;
        System.arraycopy(commands, 0, cmds, 1, commands.length);
        return container.execCmd(cmds);
    }

    private BrokerContainer getRunningBroker() {
        for (int i = 0; i < NUM_BROKERS; i++) {
            BrokerContainer broker = pulsarCluster.getBroker(i);
            if (broker.isRunning()) {
                return broker;
            }
        }
        throw new IllegalArgumentException("No broker available");
    }

}
