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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class ZkSessionExpireTest extends NetworkErrorTestBase {

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.setup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.cleanup();
    }

    protected void setConfigDefaults(ServiceConfiguration config, String clusterName, int zkPort) {
        super.setConfigDefaults(config, clusterName, zkPort);
        config.setSystemTopicEnabled(false);
        config.setTopicLevelPoliciesEnabled(false);
        config.setManagedLedgerMaxEntriesPerLedger(1);
    }

    @Test(timeOut = 60 * 1000)
    public void testTopicUnloadAfterSessionRebuild() throws Exception {
        final String topicName = "persistent://" + defaultNamespace + "/testPartitionKey";
        admin1.topics().createNonPartitionedTopic(topicName);
        admin1.topics().createSubscription(topicName, "s1", MessageId.earliest);

        // Inject a prefer mechanism, so that all topics will be assigned to broker1, which can be injected a ZK
        // session expire error.
        setPreferBroker(pulsar1);
        admin1.namespaces().unload(defaultNamespace);
        admin2.namespaces().unload(defaultNamespace);

        // Confirm all brokers registered.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(getAvailableBrokers(pulsar1).size(), 2);
            assertEquals(getAvailableBrokers(pulsar2).size(), 2);
        });

        // Load up a topic, and it will be assigned to broker1.
        Producer<String> p1 = client2.newProducer(Schema.STRING).topic(topicName)
                .sendTimeout(10, TimeUnit.SECONDS).create();
        Topic broker1Topic1 = pulsar1.getBrokerService().getTopic(topicName, false).join().get();
        assertNotNull(broker1Topic1);

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
        Awaitility.await().untilAsserted(() -> {
            CompletableFuture<Optional<Topic>> future = pulsar1.getBrokerService().getTopic(topicName, false);
            assertTrue(future == null || future.isCompletedExceptionally());
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
