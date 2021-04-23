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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import com.google.common.collect.Sets;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.persistent.ReplicatedSubscriptionsController;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests replicated subscriptions (PIP-33)
 */
@Test(groups = "broker")
public class ReplicatorSubscriptionTest extends ReplicatorTestBase {
    private static final Logger log = LoggerFactory.getLogger(ReplicatorSubscriptionTest.class);

    @Override
    @BeforeClass(timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    /**
     * Tests replicated subscriptions across two regions
     */
    @Test
    public void testReplicatedSubscriptionAcrossTwoRegions() throws Exception {
        String namespace = BrokerTestUtil.newUniqueName("pulsar/replicatedsubscription");
        String topicName = "persistent://" + namespace + "/mytopic";
        String subscriptionName = "cluster-subscription";
        // Subscription replication produces duplicates, https://github.com/apache/pulsar/issues/10054
        // TODO: duplications shouldn't be allowed, change to "false" when fixing the issue
        boolean allowDuplicates = true;
        // this setting can be used to manually run the test with subscription replication disabled
        // it shows that subscription replication has no impact in behavior for this test case
        boolean replicateSubscriptionState = true;

        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        // create subscription in r1
        createReplicatedSubscription(client1, topicName, subscriptionName, replicateSubscriptionState);

        @Cleanup
        PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        // create subscription in r2
        createReplicatedSubscription(client2, topicName, subscriptionName, replicateSubscriptionState);

        Set<String> sentMessages = new LinkedHashSet<>();

        // send messages in r1
        {
            @Cleanup
            Producer<byte[]> producer = client1.newProducer().topic(topicName)
                    .enableBatching(false)
                    .messageRoutingMode(MessageRoutingMode.SinglePartition)
                    .create();
            int numMessages = 6;
            for (int i = 0; i < numMessages; i++) {
                String body = "message" + i;
                producer.send(body.getBytes(StandardCharsets.UTF_8));
                sentMessages.add(body);
            }
            producer.close();
        }

        Set<String> receivedMessages = new LinkedHashSet<>();

        // consume 3 messages in r1
        try (Consumer<byte[]> consumer1 = client1.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .replicateSubscriptionState(replicateSubscriptionState)
                .subscribe()) {
            readMessages(consumer1, receivedMessages, 3, allowDuplicates);
        }

        // wait for subscription to be replicated
        Thread.sleep(2 * config1.getReplicatedSubscriptionsSnapshotFrequencyMillis());

        // consume remaining messages in r2
        try (Consumer<byte[]> consumer2 = client2.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .replicateSubscriptionState(replicateSubscriptionState)
                .subscribe()) {
            readMessages(consumer2, receivedMessages, -1, allowDuplicates);
        }

        // assert that all messages have been received
        assertEquals(new ArrayList<>(sentMessages), new ArrayList<>(receivedMessages), "Sent and received " +
                "messages don't match.");
    }

    /**
     * If there's no traffic, the snapshot creation should stop and then resume when traffic comes back
     */
    @Test
    public void testReplicationSnapshotStopWhenNoTraffic() throws Exception {
        String namespace = BrokerTestUtil.newUniqueName("pulsar/replicatedsubscription");
        String topicName = "persistent://" + namespace + "/mytopic";
        String subscriptionName = "cluster-subscription";

        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));

        @Cleanup
        PulsarClient client1 = PulsarClient.builder()
                .serviceUrl(url1.toString())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        // create subscription in r1
        createReplicatedSubscription(client1, topicName, subscriptionName, true);

        @Cleanup
        PulsarClient client2 = PulsarClient.builder()
                .serviceUrl(url2.toString())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        Set<String> sentMessages = new LinkedHashSet<>();

        // send messages in r1
        {
            @Cleanup
            Producer<String> producer = client1.newProducer(Schema.STRING)
                    .topic(topicName)
                    .create();
            for (int i = 0; i < 10; i++) {
                producer.send("hello-" + i);
            }
        }

        // Wait for last snapshots to be created
        Thread.sleep(2 * config1.getReplicatedSubscriptionsSnapshotFrequencyMillis());

        // In R1
        PersistentTopic t1 = (PersistentTopic) pulsar1.getBrokerService()
                .getTopic(topicName, false).get().get();
        ReplicatedSubscriptionsController rsc1 = t1.getReplicatedSubscriptionController().get();
        Position p1 = t1.getLastPosition();
        String snapshot1 = rsc1.getLastCompletedSnapshotId().get();

        // In R2

        PersistentTopic t2 = (PersistentTopic) pulsar1.getBrokerService()
                .getTopic(topicName, false).get().get();
        ReplicatedSubscriptionsController rsc2 = t2.getReplicatedSubscriptionController().get();
        Position p2 = t2.getLastPosition();
        String snapshot2 = rsc2.getLastCompletedSnapshotId().get();

        // There shouldn't be anymore snapshots
        Thread.sleep(2 * config1.getReplicatedSubscriptionsSnapshotFrequencyMillis());
        assertEquals(t1.getLastPosition(), p1);
        assertEquals(rsc1.getLastCompletedSnapshotId().get(), snapshot1);

        assertEquals(t2.getLastPosition(), p2);
        assertEquals(rsc2.getLastCompletedSnapshotId().get(), snapshot2);


        @Cleanup
        Producer<String> producer2 = client2.newProducer(Schema.STRING)
                .topic(topicName)
                .create();
        for (int i = 0; i < 10; i++) {
            producer2.send("hello-" + i);
        }

        Thread.sleep(2 * config1.getReplicatedSubscriptionsSnapshotFrequencyMillis());

        // Now we should have one or more snapshots
        assertNotEquals(t1.getLastPosition(), p1);
        assertNotEquals(rsc1.getLastCompletedSnapshotId().get(), snapshot1);

        assertNotEquals(t2.getLastPosition(), p2);
        assertNotEquals(rsc2.getLastCompletedSnapshotId().get(), snapshot2);
    }

    void readMessages(Consumer<byte[]> consumer, Set<String> messages, int maxMessages, boolean allowDuplicates)
            throws PulsarClientException {
        int count = 0;
        while (count < maxMessages || maxMessages == -1) {
            Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
            if (message != null) {
                count++;
                String body = new String(message.getValue(), StandardCharsets.UTF_8);
                if (!allowDuplicates) {
                    assertFalse(messages.contains(body), "Duplicate message '" + body + "' detected.");
                }
                messages.add(body);
            } else {
                break;
            }
        }
    }

    void createReplicatedSubscription(PulsarClient pulsarClient, String topicName, String subscriptionName,
                                      boolean replicateSubscriptionState)
            throws PulsarClientException {
        pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName)
                .replicateSubscriptionState(replicateSubscriptionState)
                .subscribe()
                .close();
    }

}
