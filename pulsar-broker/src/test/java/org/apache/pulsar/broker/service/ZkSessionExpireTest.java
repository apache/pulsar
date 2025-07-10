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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannel;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateTableView;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.metadata.api.MetadataStoreTableView;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class ZkSessionExpireTest extends NetworkErrorTestBase {

    private java.util.function.Consumer<ServiceConfiguration> settings;

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.cleanup();
    }

    private void setupWithSettings(java.util.function.Consumer<ServiceConfiguration> settings) throws Exception {
        this.settings = settings;
        super.setup();
    }

    protected void setConfigDefaults(ServiceConfiguration config, String clusterName, int zkPort) {
        super.setConfigDefaults(config, clusterName, zkPort);
        settings.accept(config);
    }

    @DataProvider(name = "settings")
    public Object[][] settings() {
        return new Object[][]{
//            {false, NetworkErrorTestBase.PreferBrokerModularLoadManager.class, null},
//            {true, NetworkErrorTestBase.PreferBrokerModularLoadManager.class, null},
            {true, NetworkErrorTestBase.PreferExtensibleLoadManager.class, TableViewType.MetadataStore},
            // Create a separate PR to add this test case.
            // {true, NetworkErrorTestBase.PreferExtensibleLoadManager.class, TableViewType.SystemTopic}
        };
    }

    private enum TableViewType {
        SystemTopic,
        MetadataStore;
    }

    @Test(timeOut = 180 * 1000, dataProvider = "settings")
    public void testTopicUnloadAfterSessionRebuild(boolean enableSystemTopic, Class loadManager,
                                                   TableViewType tableViewType) throws Exception {
        // Setup.
        setupWithSettings(config -> {
            config.setManagedLedgerMaxEntriesPerLedger(1);
            config.setSystemTopicEnabled(enableSystemTopic);
            config.setTopicLevelPoliciesEnabled(enableSystemTopic);
            // Set load manager.
            if (loadManager == null) {
                return;
            }
            config.setLoadManagerClassName(loadManager.getName());
            if (loadManager.getSimpleName().equals("ModularLoadManagerImpl")) {
                return;
            }
            // Set params of metadata store table view.
            if (loadManager.getSimpleName().equals("PreferExtensibleLoadManager")) {
                if (tableViewType == TableViewType.MetadataStore) {
                    config.setLoadManagerServiceUnitStateTableViewClassName("org.apache.pulsar.broker.loadbalance"
                            + ".extensions.channel.ServiceUnitStateMetadataStoreTableViewImpl");
                    config.setLoadBalancerLoadSheddingStrategy("org.apache.pulsar.broker.loadbalance.extensions"
                            + ".scheduler.TransferShedder");
                    config.setLoadBalancerSheddingEnabled(true);
                    config.setLoadBalancerTransferEnabled(true);
                }
            }
        });

        // Init topic.
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + defaultNamespace + "/tp");
        admin1.topics().createNonPartitionedTopic(topicName);
        admin1.topics().createSubscription(topicName, "s1", MessageId.earliest);

        // Inject a prefer mechanism, so that all topics will be assigned to broker1, which can be injected a ZK
        // session expire error.
        setPreferBroker(pulsar1);
        admin1.namespaces().unload(defaultNamespace);
        admin2.namespaces().unload(defaultNamespace);

        // Confirm all brokers registered.
        Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertEquals(getAvailableBrokers(pulsar1).size(), 2);
            assertEquals(getAvailableBrokers(pulsar2).size(), 2);
        });

        // Load up a topic, and it will be assigned to broker1.
        ProducerImpl<String> p1 = (ProducerImpl<String>) client1.newProducer(Schema.STRING).topic(topicName)
                .sendTimeout(10, TimeUnit.SECONDS).create();
        Topic broker1Topic1 = pulsar1.getBrokerService().getTopic(topicName, false).join().get();
        final String brokerAddr1 = admin1.lookups().lookupTopic(topicName);
        log.info("the addr of broker 1: {}", brokerAddr1);
        assertNotNull(broker1Topic1);
        clearPreferBroker();

        // Inject a ZK session expire error, and wait for broker1 to offline.
        metadataZKProxy.rejectAllConnections();
        metadataZKProxy.disconnectFrontChannels();
        Awaitility.await().untilAsserted(() -> {
            assertEquals(getAvailableBrokers(pulsar2).size(), 1);
        });

        // Send messages continuously.
        // Verify: the topic was transferred to broker2.
        CompletableFuture<MessageId> broker1Send1 = p1.sendAsync("broker1_msg1");
        Producer<String> p2 = client2.newProducer(Schema.STRING).topic(topicName)
                .sendTimeout(10, TimeUnit.SECONDS).create();
        CompletableFuture<MessageId> broker2Send1 = p2.sendAsync("broker2_msg1");
        Awaitility.await().untilAsserted(() -> {
            CompletableFuture<Optional<Topic>> future = pulsar2.getBrokerService().getTopic(topicName, false);
            assertNotNull(future);
            assertTrue(future.isDone() && !future.isCompletedExceptionally());
            Optional<Topic> optional = future.join();
            assertTrue(optional != null && !optional.isEmpty());
        });

        // Both two brokers assumed they are the owner of the topic.
        Topic broker1Topic2 = pulsar1.getBrokerService().getTopic(topicName, false).join().get();
        Topic broker2Topic2 = pulsar2.getBrokerService().getTopic(topicName, false).join().get();
        assertNotNull(broker1Topic2);
        assertNotNull(broker2Topic2);

        // Send messages continuously.
        // Publishing to broker-1 will fail.
        // Publishing to broker-2 will success.
        CompletableFuture<MessageId> broker1Send2 = p1.sendAsync("broker1_msg2");
        CompletableFuture<MessageId> broker2Send2 = p2.sendAsync("broker2_msg2");
        admin1.brokers().getActiveBrokers();
        try {
            broker1Send1.join();
            broker1Send2.join();
            p1.getClientCnx();
            // Since system topic load balancer does not rely on ZK, the publishing can succeed.
            if (loadManager.getSimpleName().equals("ModularLoadManagerImpl")
                    || TableViewType.MetadataStore.equals(tableViewType)) {
                fail("Since the client can not get a correct response of lookup from the broker who can not connect to"
                    + " metadata store, it should failed to publish");
            }
        } catch (Exception ex) {
            // Since topic transferring of system topic base load balancer does not rely on ZK, the both publish can
            // succeed.
            if (TableViewType.SystemTopic.equals(tableViewType)) {
                fail("Since topic transferring of system topic base load balancer does not rely on ZK, the both publish"
                    + " should succeed");
            }
        }
        broker2Send1.join();
        broker2Send2.join();

        // Broker rebuild ZK session.
        metadataZKProxy.unRejectAllConnections();
        Awaitility.await().untilAsserted(() -> {
            Set<String> availableBrokers1 = getAvailableBrokers(pulsar1);
            Set<String> availableBrokers2 = getAvailableBrokers(pulsar1);
            log.info("Available brokers 1: {}", availableBrokers1);
            log.info("Available brokers 2: {}", availableBrokers2);
            assertEquals(availableBrokers1.size(), 2);
            assertEquals(availableBrokers1.size(), 2);
        });

        // Verify: the topic on broker-1 will be unloaded.
        // Verify: the topic on broker-2 is fine.
        Awaitility.await().atMost(Duration.ofSeconds(180)).untilAsserted(() -> {
            CompletableFuture<Optional<Topic>> future1 = pulsar1.getBrokerService().getTopic(topicName, false);
            log.info("broker 1 topics {}", pulsar1.getBrokerService().getTopics().keySet());
            log.info("broker 2 topics {}", pulsar2.getBrokerService().getTopics().keySet());
            log.info("broker 1 bundles {}", getOwnedBundles(pulsar1).stream()
                    .filter(s -> s.contains(defaultNamespace)).collect(Collectors.toList()));
            log.info("broker 2 bundles {}", getOwnedBundles(pulsar1).stream()
                    .filter(s -> s.contains(defaultNamespace)).collect(Collectors.toList()));
            String lookup1 = admin1.lookups().lookupTopic(topicName);
            String lookup2 = admin2.lookups().lookupTopic(topicName);
            log.info("lookup 1: {}", lookup1);
            log.info("lookup 2: {}", lookup2);
            // Both responses from different brokers should be the same.
            assertEquals(lookup1, lookup2, "both lookup result should be the same");
            // Except the system topic based load balancer, the topic should be loaded up on the broker-2.
            log.info("future 1: {}, isDone: {}, isCompletedExceptionally: {}",
                    future1, future1 == null ? "null" : future1.isDone(),
                    future1 == null ? "null" : future1.isCompletedExceptionally());
            boolean topicDoesNotExists1 = future1 == null
                    || !pulsar1.getBrokerService().getTopics().containsKey(topicName)
                    || (future1.isDone() && !future1.isCompletedExceptionally() && future1.get().isEmpty())
                    || future1.isCompletedExceptionally();
            if (loadManager.getSimpleName().equals("ModularLoadManagerImpl")
                    || TableViewType.MetadataStore.equals(tableViewType)) {
                assertNotEquals(lookup1, brokerAddr1, "lookup result should be broker-2");
                assertNotEquals(lookup2, brokerAddr1, "lookup result should be broker-2");
                assertTrue(topicDoesNotExists1, "topic should be unloaded from the broker-1");
                List<String> bundle1 = getOwnedBundles(pulsar1).stream()
                        .filter(s -> s.contains(defaultNamespace)).collect(Collectors.toList());
                List<String> bundle2 = getOwnedBundles(pulsar2).stream()
                        .filter(s -> s.contains(defaultNamespace)).collect(Collectors.toList());
                log.info("broker 1 bundles the second time {}", bundle1);
                log.info("broker 2 bundles the second time {}", bundle2);
                assertEquals(bundle1.size(), 0);
                assertEquals(bundle2.size(), 1);
                if (TableViewType.MetadataStore.equals(tableViewType)) {
                    assertEquals(getMetadataStoreBasedInternalOwnedBundles(pulsar1, brokerAddr1).stream()
                            .filter(s -> s.contains(defaultNamespace)).collect(Collectors.toList()).size(), 0);
                    assertEquals(getMetadataStoreBasedInternalOwnedBundles(pulsar2, lookup2).stream()
                            .filter(s -> s.contains(defaultNamespace)).collect(Collectors.toList()).size(), 0);
                }
            }
            // Regarding the system topic based load balancer, which does not rely on ZK, the topic may be owned by any
            // broker.
            if (TableViewType.SystemTopic.equals(tableViewType)) {
                // This implementation will be finished by a seperated PR.
            }
        });
        Topic broker2Topic3 = pulsar2.getBrokerService().getTopic(topicName, false).join().get();
        assertNotNull(broker2Topic3);

        // Send messages continuously.
        // Verify: p1.send will success(it will connect to broker-2).
        // Verify: p2.send will success.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(broker2Topic3.getProducers().size(), 2);
        });
        CompletableFuture<MessageId> broker1Send3 = p1.sendAsync("broker1_msg3");
        CompletableFuture<MessageId> broker2Send3 = p2.sendAsync("broker2_msg3");
        broker1Send3.join();
        broker2Send3.join();

        long msgBacklog = admin2.topics().getStats(topicName).getSubscriptions().get("s1").getMsgBacklog();
        log.info("msgBacklog: {}", msgBacklog);

        // cleanup.
        p1.close();
        p2.close();
        admin2.topics().delete(topicName, false);
    }

    private Collection<String> getMetadataStoreBasedInternalOwnedBundles(PulsarService pulsar, String brokerUrl) {
        Object loadManagerWrapper = pulsar.getLoadManager().get();
        ExtensibleLoadManagerImpl loadManager = WhiteboxImpl.getInternalState(loadManagerWrapper, "loadManager");
        ServiceUnitStateChannel serviceUnitStateChannel = loadManager.getServiceUnitStateChannel();
        ServiceUnitStateTableView tableview = WhiteboxImpl.getInternalState(serviceUnitStateChannel, "tableview");
        MetadataStoreTableView<ServiceUnitStateData> tableviewInternal = WhiteboxImpl.getInternalState(tableview, "tableview");
        ConcurrentMap<String, ServiceUnitStateData> data = WhiteboxImpl.getInternalState(tableviewInternal, "data");
        return data.entrySet().stream().filter(e -> {
            if (brokerUrl.equals(e.getValue().dstBroker()) && e.getValue().state().toString().equals("Owned")) {
                return true;
            }
            return false;
        }).map(e -> e.getKey()).collect(Collectors.toList());
    }
}
