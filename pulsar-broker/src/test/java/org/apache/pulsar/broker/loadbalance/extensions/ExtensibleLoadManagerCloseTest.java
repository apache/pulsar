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
package org.apache.pulsar.broker.loadbalance.extensions;

import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Free;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Owned;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannel;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateMetadataStoreTableViewImpl;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateTableViewImpl;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker")
public class ExtensibleLoadManagerCloseTest {

    private static final String clusterName = "test";
    private static final Map<String, AssignmentRace> assignmentRaces = new ConcurrentHashMap<>();
    private final List<PulsarService> brokers = new ArrayList<>();
    private LocalBookkeeperEnsemble bk;

    @BeforeClass(alwaysRun = true)
    public void setup() throws Exception {
        bk = new LocalBookkeeperEnsemble(1, 0);
        bk.start();
    }

    private void setupBrokers(int numBrokers, boolean topicPoliciesEnabled) throws Exception {
        setupBrokers(numBrokers, topicPoliciesEnabled, ServiceUnitStateTableViewImpl.class.getName());
    }

    private void setupBrokers(int numBrokers, boolean topicPoliciesEnabled, String tableViewClass) throws Exception {
        for (int i = 0; i < numBrokers; i++) {
            var config = brokerConfig(topicPoliciesEnabled);
            config.setLoadManagerServiceUnitStateTableViewClassName(tableViewClass);
            final var broker = new PulsarService(config);
            brokers.add(broker);
            broker.start();
        }
        final var admin = brokers.get(0).getAdminClient();
        if (!admin.clusters().getClusters().contains(clusterName)) {
            admin.clusters().createCluster(clusterName, ClusterData.builder().build());
            admin.tenants().createTenant("public", TenantInfo.builder()
                    .allowedClusters(Collections.singleton(clusterName)).build());
            admin.namespaces().createNamespace("public/default");
        }
    }


    @AfterMethod(alwaysRun = true, timeOut = 30000)
    public void cleanupBrokers() throws Exception {
        try {
            FutureUtil.waitForAll(brokers.stream().map(PulsarService::closeAsync).toList()).get();
        } finally {
            brokers.clear();
        }
    }

    @AfterClass(alwaysRun = true, timeOut = 30000)
    public void cleanup() throws Exception {
        bk.stop();
    }

    private ServiceConfiguration brokerConfig(boolean topicPoliciesEnabled) {
        final var config = new ServiceConfiguration();
        config.setClusterName(clusterName);
        config.setAdvertisedAddress("localhost");
        config.setBrokerServicePort(Optional.of(0));
        config.setWebServicePort(Optional.of(0));
        config.setMetadataStoreUrl("zk:127.0.0.1:" + bk.getZookeeperPort());
        config.setManagedLedgerDefaultWriteQuorum(1);
        config.setManagedLedgerDefaultAckQuorum(1);
        config.setManagedLedgerDefaultEnsembleSize(1);
        config.setDefaultNumberOfNamespaceBundles(16);
        config.setLoadBalancerAutoBundleSplitEnabled(false);
        config.setTopicLevelPoliciesEnabled(topicPoliciesEnabled);
        config.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        config.setLoadBalancerDebugModeEnabled(true);
        config.setBrokerShutdownTimeoutMs(100);

        // Reduce these timeout configs to avoid failed tests being blocked too long
        config.setMetadataStoreOperationTimeoutSeconds(5);
        config.setNamespaceBundleUnloadingTimeoutMs(5000);
        return config;
    }


    @DataProvider
    public Object[][] topicPoliciesEnabled() {
        return new Object[][]{{false}, {true}};
    }

    @Test(invocationCount = 10, dataProvider = "topicPoliciesEnabled")
    public void testCloseAfterLoadingBundles(boolean topicPoliciesEnabled) throws Exception {
        setupBrokers(3, topicPoliciesEnabled);
        final var topic = "test-" + System.currentTimeMillis();
        final var admin = brokers.get(0).getAdminClient();
        admin.topics().createPartitionedTopic(topic, 20);
        admin.lookups().lookupPartitionedTopic(topic);
        final var client = PulsarClient.builder().serviceUrl(brokers.get(0).getBrokerServiceUrl()).build();
        final var producer = client.newProducer().topic(topic).create();
        producer.close();
        client.close();

        final var closeTimeMsList = new ArrayList<Long>();
        for (var broker : brokers) {
            final var startTimeMs = System.currentTimeMillis();
            broker.close();
            closeTimeMsList.add(System.currentTimeMillis() - startTimeMs);
        }
        log.info().attr("closeTimeMsList", closeTimeMsList).log("Brokers close time");
        for (var closeTimeMs : closeTimeMsList) {
            Assert.assertTrue(closeTimeMs < 5000L);
        }
    }

    @Test(dataProvider = "topicPoliciesEnabled")
    public void testLookup(boolean topicPoliciesEnabled) throws Exception {
        setupBrokers(1, topicPoliciesEnabled);
        final var topic = "test-lookup-" + UUID.randomUUID();
        final var numPartitions = 16;
        final var admin = brokers.get(0).getAdminClient();
        admin.topics().createPartitionedTopic(topic, numPartitions);

        final var futures = new ArrayList<CompletableFuture<String>>();
        for (int i = 0; i < numPartitions; i++) {
            futures.add(admin.lookups().lookupTopicAsync(topic + TopicName.PARTITIONED_TOPIC_SUFFIX + i));
        }
        FutureUtil.waitForAll(futures).get();

        final var start = System.currentTimeMillis();
        brokers.get(0).close();
        final var closeTimeMs = System.currentTimeMillis() - start;
        log.info().attr("closeTimeMs", closeTimeMs).log("Broker close time");
        Assert.assertTrue(closeTimeMs < 5000L);
    }

    @DataProvider
    public Object[][] systemBundleCleanup() {
        return new Object[][]{
                {DelayedSystemTopicTableView.class.getName(), false},
                {DelayedSystemTopicTableView.class.getName(), true},
                {DelayedMetadataStoreTableView.class.getName(), false},
                {DelayedMetadataStoreTableView.class.getName(), true}
        };
    }

    @Test(dataProvider = "systemBundleCleanup", timeOut = 60000)
    public void testCleanupRetriesConcurrentSystemAssignment(String tableViewClass, boolean hasDestination)
            throws Exception {
        setupBrokers(hasDestination ? 2 : 1, false, tableViewClass);
        var broker = brokers.get(0);
        var channel = ServiceUnitStateChannelImpl.get(broker);
        // Do not unload the ownership channel itself while checking the result of system-bundle cleanup.
        var internalBundles = new ArrayList<String>();
        for (var internalTopic : ExtensibleLoadManagerImpl.INTERNAL_TOPICS) {
            internalBundles.add(broker.getNamespaceService().getBundleAsync(TopicName.get(internalTopic))
                    .get(10, TimeUnit.SECONDS).toString());
        }
        String topic;
        String bundle;
        do {
            topic = "persistent://pulsar/system/cleanup-" + UUID.randomUUID();
            bundle = broker.getNamespaceService().getBundleAsync(TopicName.get(topic))
                    .get(10, TimeUnit.SECONDS).toString();
        } while (internalBundles.contains(bundle) || ownership(channel, bundle) != null);

        var race = new AssignmentRace();
        assignmentRaces.put(bundle, race);
        CompletableFuture<String> lookup = null;
        try {
            // Exercise real lookup, broker selection, state handlers and persistent conflict resolution.
            // The table views below only delay the normal Owned write until cleanup publishes its override.
            lookup = broker.getAdminClient().lookups().lookupTopicAsync(topic);
            var owned = race.pendingOwned.get(10, TimeUnit.SECONDS);
            var owner = brokers.stream().filter(b -> b.getBrokerId().equals(owned.dstBroker()))
                    .findFirst().orElseThrow();
            var replacement = brokers.stream().filter(b -> b != owner).findFirst();
            var ownerChannel = ServiceUnitStateChannelImpl.get(owner);
            ownerChannel.cleanOwnerships();
            assertThat(race.conflictingOverride.get(10, TimeUnit.SECONDS).versionId())
                    .as("Cleanup must encounter the same-version conflict")
                    .isEqualTo(owned.versionId());
            String serviceUnit = bundle;
            Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                var remaining = ownership(ownerChannel, serviceUnit);
                if (hasDestination) {
                    assertThat(remaining).isNotNull();
                    assertThat(remaining.state()).isEqualTo(Owned);
                    assertThat(remaining.dstBroker()).isEqualTo(replacement.orElseThrow().getBrokerId());
                } else {
                    assertThat(remaining == null || remaining.state() == Free)
                            .as("System bundle must not remain owned after cleanup: %s", remaining).isTrue();
                }
            });
        } finally {
            race.releaseOwned.complete(null);
            assignmentRaces.remove(bundle, race);
            if (lookup != null) {
                // Disabling the original owner can cancel the lookup that triggered the race.
                lookup.handle((result, error) -> null).get(10, TimeUnit.SECONDS);
            }
        }
    }

    private static ServiceUnitStateData ownership(ServiceUnitStateChannel channel, String bundle) {
        return channel.getOwnershipEntrySet().stream().filter(entry -> entry.getKey().equals(bundle))
                .map(Map.Entry::getValue).findFirst().orElse(null);
    }

    private static class AssignmentRace {
        private final AtomicBoolean held = new AtomicBoolean();
        private final CompletableFuture<ServiceUnitStateData> pendingOwned = new CompletableFuture<>();
        private final CompletableFuture<ServiceUnitStateData> conflictingOverride = new CompletableFuture<>();
        private final CompletableFuture<Void> releaseOwned = new CompletableFuture<>();
        private volatile CompletableFuture<Void> ownedPublished;
    }

    private static CompletableFuture<Void> delayOwnedWrite(String key, ServiceUnitStateData data,
                                                           Supplier<CompletableFuture<Void>> write) {
        var race = assignmentRaces.get(key);
        if (race != null && data != null) {
            if (data.state() == Owned && !data.force() && race.held.compareAndSet(false, true)) {
                race.ownedPublished = race.releaseOwned.thenCompose(__ -> write.get());
                race.pendingOwned.complete(data);
                return race.ownedPublished;
            }
            var owned = race.pendingOwned.getNow(null);
            if (data.force() && owned != null && data.versionId() == owned.versionId()) {
                race.conflictingOverride.complete(data);
                race.releaseOwned.complete(null);
                return race.ownedPublished.thenCompose(__ -> write.get());
            }
        }
        return write.get();
    }

    public static class DelayedSystemTopicTableView extends ServiceUnitStateTableViewImpl {
        @Override
        public CompletableFuture<Void> put(String key, ServiceUnitStateData value) {
            return delayOwnedWrite(key, value, () -> super.put(key, value));
        }
    }

    public static class DelayedMetadataStoreTableView extends ServiceUnitStateMetadataStoreTableViewImpl {
        @Override
        public CompletableFuture<Void> put(String key, ServiceUnitStateData value) {
            return delayOwnedWrite(key, value, () -> super.put(key, value));
        }
    }
}
