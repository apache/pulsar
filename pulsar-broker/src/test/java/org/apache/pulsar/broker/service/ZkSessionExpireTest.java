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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.awaitility.Awaitility;
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
            {false, NetworkErrorTestBase.PreferBrokerModularLoadManager.class},
            {true, NetworkErrorTestBase.PreferBrokerModularLoadManager.class}
            // Create a separate PR to add this test case.
            // {true, NetworkErrorTestBase.PreferExtensibleLoadManager.class}.
        };
    }

    @Test(timeOut = 600 * 1000, dataProvider = "settings")
    public void testTopicUnloadAfterSessionRebuild(boolean enableSystemTopic, Class loadManager) throws Exception {
        // Setup.
        setupWithSettings(config -> {
            config.setManagedLedgerMaxEntriesPerLedger(1);
            config.setSystemTopicEnabled(enableSystemTopic);
            config.setTopicLevelPoliciesEnabled(enableSystemTopic);
            if (loadManager != null) {
                config.setLoadManagerClassName(loadManager.getName());
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
        try {
            broker1Send1.join();
            broker1Send2.join();
            p1.getClientCnx();
            fail("expected a publish error");
        } catch (Exception ex) {
            // Expected.
        }
        broker2Send1.join();
        broker2Send2.join();

        // Broker rebuild ZK session.
        metadataZKProxy.unRejectAllConnections();
        Awaitility.await().untilAsserted(() -> {
            assertEquals(getAvailableBrokers(pulsar1).size(), 2);
            assertEquals(getAvailableBrokers(pulsar2).size(), 2);
        });

        // Verify: the topic on broker-1 will be unloaded.
        // Verify: the topic on broker-2 is fine.
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            CompletableFuture<Optional<Topic>> future = pulsar1.getBrokerService().getTopic(topicName, false);
            log.info("broker 1 topics {}", pulsar1.getBrokerService().getTopics().keys());
            log.info("broker 2 topics {}", pulsar2.getBrokerService().getTopics().keys());
            log.info("broker 1 bundles {}", pulsar1.getNamespaceService().getOwnershipCache().getOwnedBundles()
                    .keySet().stream().map(k -> k.getNamespaceObject().toString() + "/" + k.getBundleRange())
                    .filter(s -> s.contains(defaultNamespace)).collect(Collectors.toList()));
            log.info("broker 2 bundles {}", pulsar2.getNamespaceService().getOwnershipCache().getOwnedBundles()
                    .keySet().stream().map(k -> k.getNamespaceObject().toString() + "/" + k.getBundleRange())
                    .filter(s -> s.contains(defaultNamespace)).collect(Collectors.toList()));
            log.info("future: {}, isDone: {}, isCompletedExceptionally: {}",
                    future, future == null ? "null" : future.isDone(),
                    future, future == null ? "null" : future.isCompletedExceptionally());
            assertTrue(future == null
                    || !pulsar1.getBrokerService().getTopics().containsKey(topicName)
                    || (future.isDone() && !future.isCompletedExceptionally() && future.get().isEmpty())
                    || future.isCompletedExceptionally());
        });
        Topic broker2Topic3 = pulsar2.getBrokerService().getTopic(topicName, false).join().get();
        assertNotNull(broker2Topic3);

        // Send messages continuously.
        // Verify: p1.send will success(it will connect to broker-2).
        // Verify: p2.send will success.
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
}
