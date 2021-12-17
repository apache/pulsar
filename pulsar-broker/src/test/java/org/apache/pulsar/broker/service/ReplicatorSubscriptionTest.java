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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.awaitility.Awaitility;
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

    @Test(timeOut = 30000)
    public void testReplicatedSubscriptionRestApi1() throws Exception {
        final String namespace = BrokerTestUtil.newUniqueName("pulsar/replicatedsubscription");
        final String topicName = "persistent://" + namespace + "/topic-rest-api1";
        final String subName = "sub";
        // Subscription replication produces duplicates, https://github.com/apache/pulsar/issues/10054
        // TODO: duplications shouldn't be allowed, change to "false" when fixing the issue
        final boolean allowDuplicates = true;

        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));

        @Cleanup
        final PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();

        // Create subscription in r1
        createReplicatedSubscription(client1, topicName, subName, true);

        @Cleanup
        final PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();

        // Create subscription in r2
        createReplicatedSubscription(client2, topicName, subName, true);

        TopicStats stats = admin1.topics().getStats(topicName);
        assertTrue(stats.getSubscriptions().get(subName).isReplicated());

        // Disable replicated subscription in r1
        admin1.topics().setReplicatedSubscriptionStatus(topicName, subName, false);
        stats = admin1.topics().getStats(topicName);
        assertFalse(stats.getSubscriptions().get(subName).isReplicated());
        stats = admin2.topics().getStats(topicName);
        assertTrue(stats.getSubscriptions().get(subName).isReplicated());

        // Disable replicated subscription in r2
        admin2.topics().setReplicatedSubscriptionStatus(topicName, subName, false);
        stats = admin2.topics().getStats(topicName);
        assertFalse(stats.getSubscriptions().get(subName).isReplicated());

        // Unload topic in r1
        admin1.topics().unload(topicName);
        Awaitility.await().untilAsserted(() -> {
            TopicStats stats2 = admin1.topics().getStats(topicName);
            assertFalse(stats2.getSubscriptions().get(subName).isReplicated());
        });

        // Make sure the replicated subscription is actually disabled
        final int numMessages = 20;
        final Set<String> sentMessages = new LinkedHashSet<>();
        final Set<String> receivedMessages = new LinkedHashSet<>();

        Producer<byte[]> producer = client1.newProducer().topic(topicName).enableBatching(false).create();
        sentMessages.clear();
        publishMessages(producer, 0, numMessages, sentMessages);
        producer.close();

        Consumer<byte[]> consumer1 = client1.newConsumer().topic(topicName).subscriptionName(subName).subscribe();
        receivedMessages.clear();
        readMessages(consumer1, receivedMessages, numMessages, false);
        assertEquals(receivedMessages, sentMessages);
        consumer1.close();

        Consumer<byte[]> consumer2 = client2.newConsumer().topic(topicName).subscriptionName(subName).subscribe();
        receivedMessages.clear();
        readMessages(consumer2, receivedMessages, numMessages, false);
        assertEquals(receivedMessages, sentMessages);
        consumer2.close();

        // Enable replicated subscription in r1
        admin1.topics().setReplicatedSubscriptionStatus(topicName, subName, true);
        stats = admin1.topics().getStats(topicName);
        assertTrue(stats.getSubscriptions().get(subName).isReplicated());
        stats = admin2.topics().getStats(topicName);
        assertFalse(stats.getSubscriptions().get(subName).isReplicated());

        // Enable replicated subscription in r2
        admin2.topics().setReplicatedSubscriptionStatus(topicName, subName, true);
        stats = admin2.topics().getStats(topicName);
        assertTrue(stats.getSubscriptions().get(subName).isReplicated());

        // Make sure the replicated subscription is actually enabled
        sentMessages.clear();
        receivedMessages.clear();

        producer = client1.newProducer().topic(topicName).enableBatching(false).create();
        publishMessages(producer, 0, numMessages / 2, sentMessages);
        producer.close();
        Thread.sleep(2 * config1.getReplicatedSubscriptionsSnapshotFrequencyMillis());

        consumer1 = client1.newConsumer().topic(topicName).subscriptionName(subName).subscribe();
        final int numReceivedMessages1 = readMessages(consumer1, receivedMessages, numMessages / 2, allowDuplicates);
        consumer1.close();

        producer = client1.newProducer().topic(topicName).enableBatching(false).create();
        publishMessages(producer, numMessages / 2, numMessages / 2, sentMessages);
        producer.close();
        Thread.sleep(2 * config1.getReplicatedSubscriptionsSnapshotFrequencyMillis());

        consumer2 = client2.newConsumer().topic(topicName).subscriptionName(subName).subscribe();
        final int numReceivedMessages2 = readMessages(consumer2, receivedMessages, -1, allowDuplicates);
        consumer2.close();

        assertEquals(receivedMessages, sentMessages);
        assertTrue(numReceivedMessages1 < numMessages,
                String.format("numReceivedMessages1 (%d) should be less than %d", numReceivedMessages1, numMessages));
        assertTrue(numReceivedMessages2 < numMessages,
                String.format("numReceivedMessages2 (%d) should be less than %d", numReceivedMessages2, numMessages));
    }

    @Test
    public void testGetReplicatedSubscriptionStatus() throws Exception {
        final String namespace = BrokerTestUtil.newUniqueName("pulsar/replicatedsubscription");
        final String topicName1 = "persistent://" + namespace + "/tp-no-part";
        final String topicName2 = "persistent://" + namespace + "/tp-with-part";
        final String subName1 = "sub1";
        final String subName2 = "sub2";

        admin1.namespaces().createNamespace(namespace);
        admin1.topics().createNonPartitionedTopic(topicName1);
        admin1.topics().createPartitionedTopic(topicName2, 3);

        @Cleanup final PulsarClient client = PulsarClient.builder().serviceUrl(url1.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();

        // Create subscription on non-partitioned topic
        createReplicatedSubscription(client, topicName1, subName1, true);
        Awaitility.await().untilAsserted(() -> {
            Map<String, Boolean> status = admin1.topics().getReplicatedSubscriptionStatus(topicName1, subName1);
            assertTrue(status.get(topicName1));
        });
        // Disable replicated subscription on non-partitioned topic
        admin1.topics().setReplicatedSubscriptionStatus(topicName1, subName1, false);
        Awaitility.await().untilAsserted(() -> {
            Map<String, Boolean> status = admin1.topics().getReplicatedSubscriptionStatus(topicName1, subName1);
            assertFalse(status.get(topicName1));
        });

        // Create subscription on partitioned topic
        createReplicatedSubscription(client, topicName2, subName2, true);
        Awaitility.await().untilAsserted(() -> {
            Map<String, Boolean> status = admin1.topics().getReplicatedSubscriptionStatus(topicName2, subName2);
            assertEquals(status.size(), 3);
            for (int i = 0; i < 3; i++) {
                assertTrue(status.get(topicName2 + "-partition-" + i));
            }
        });
        // Disable replicated subscription on partitioned topic
        admin1.topics().setReplicatedSubscriptionStatus(topicName2, subName2, false);
        Awaitility.await().untilAsserted(() -> {
            Map<String, Boolean> status = admin1.topics().getReplicatedSubscriptionStatus(topicName2, subName2);
            assertEquals(status.size(), 3);
            for (int i = 0; i < 3; i++) {
                assertFalse(status.get(topicName2 + "-partition-" + i));
            }
        });
        // Enable replicated subscription on partition-2
        admin1.topics().setReplicatedSubscriptionStatus(topicName2 + "-partition-2", subName2, true);
        Awaitility.await().untilAsserted(() -> {
            Map<String, Boolean> status = admin1.topics().getReplicatedSubscriptionStatus(topicName2, subName2);
            assertEquals(status.size(), 3);
            for (int i = 0; i < 3; i++) {
                if (i == 2) {
                    assertTrue(status.get(topicName2 + "-partition-" + i));
                } else {
                    assertFalse(status.get(topicName2 + "-partition-" + i));
                }
            }
        });
    }

    @Test(timeOut = 30000)
    public void testReplicatedSubscriptionRestApi2() throws Exception {
        final String namespace = BrokerTestUtil.newUniqueName("pulsar/replicatedsubscription");
        final String topicName = "persistent://" + namespace + "/topic-rest-api2";
        final String subName = "sub";
        // Subscription replication produces duplicates, https://github.com/apache/pulsar/issues/10054
        // TODO: duplications shouldn't be allowed, change to "false" when fixing the issue
        final boolean allowDuplicates = true;

        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));
        admin1.topics().createPartitionedTopic(topicName, 2);

        @Cleanup
        final PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();

        // Create subscription in r1
        createReplicatedSubscription(client1, topicName, subName, true);

        @Cleanup
        final PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();

        // Create subscription in r2
        createReplicatedSubscription(client2, topicName, subName, true);

        PartitionedTopicStats partitionedStats = admin1.topics().getPartitionedStats(topicName, true);
        for (TopicStats stats : partitionedStats.getPartitions().values()) {
            assertTrue(stats.getSubscriptions().get(subName).isReplicated());
        }

        // Disable replicated subscription in r1
        admin1.topics().setReplicatedSubscriptionStatus(topicName, subName, false);
        partitionedStats = admin1.topics().getPartitionedStats(topicName, true);
        for (TopicStats stats : partitionedStats.getPartitions().values()) {
            assertFalse(stats.getSubscriptions().get(subName).isReplicated());
        }

        // Disable replicated subscription in r2
        admin2.topics().setReplicatedSubscriptionStatus(topicName, subName, false);
        partitionedStats = admin2.topics().getPartitionedStats(topicName, true);
        for (TopicStats stats : partitionedStats.getPartitions().values()) {
            assertFalse(stats.getSubscriptions().get(subName).isReplicated());
        }

        // Make sure the replicated subscription is actually disabled
        final int numMessages = 20;
        final Set<String> sentMessages = new LinkedHashSet<>();
        final Set<String> receivedMessages = new LinkedHashSet<>();

        Producer<byte[]> producer = client1.newProducer().topic(topicName).enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        sentMessages.clear();
        publishMessages(producer, 0, numMessages, sentMessages);
        producer.close();

        Consumer<byte[]> consumer1 = client1.newConsumer().topic(topicName).subscriptionName(subName).subscribe();
        receivedMessages.clear();
        readMessages(consumer1, receivedMessages, numMessages, false);
        assertEquals(receivedMessages, sentMessages);
        consumer1.close();

        Consumer<byte[]> consumer2 = client2.newConsumer().topic(topicName).subscriptionName(subName).subscribe();
        receivedMessages.clear();
        readMessages(consumer2, receivedMessages, numMessages, false);
        assertEquals(receivedMessages, sentMessages);
        consumer2.close();

        // Enable replicated subscription in r1
        admin1.topics().setReplicatedSubscriptionStatus(topicName, subName, true);
        partitionedStats = admin1.topics().getPartitionedStats(topicName, true);
        for (TopicStats stats : partitionedStats.getPartitions().values()) {
            assertTrue(stats.getSubscriptions().get(subName).isReplicated());
        }

        // Enable replicated subscription in r2
        admin2.topics().setReplicatedSubscriptionStatus(topicName, subName, true);
        partitionedStats = admin2.topics().getPartitionedStats(topicName, true);
        for (TopicStats stats : partitionedStats.getPartitions().values()) {
            assertTrue(stats.getSubscriptions().get(subName).isReplicated());
        }

        // Make sure the replicated subscription is actually enabled
        sentMessages.clear();
        receivedMessages.clear();

        producer = client1.newProducer().topic(topicName).enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        publishMessages(producer, 0, numMessages / 2, sentMessages);
        producer.close();
        Thread.sleep(2 * config1.getReplicatedSubscriptionsSnapshotFrequencyMillis());

        consumer1 = client1.newConsumer().topic(topicName).subscriptionName(subName).subscribe();
        final int numReceivedMessages1 = readMessages(consumer1, receivedMessages, numMessages / 2, allowDuplicates);
        consumer1.close();

        producer = client1.newProducer().topic(topicName).enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        publishMessages(producer, numMessages / 2, numMessages / 2, sentMessages);
        producer.close();
        Thread.sleep(2 * config1.getReplicatedSubscriptionsSnapshotFrequencyMillis());

        consumer2 = client2.newConsumer().topic(topicName).subscriptionName(subName).subscribe();
        final int numReceivedMessages2 = readMessages(consumer2, receivedMessages, -1, allowDuplicates);
        consumer2.close();

        assertEquals(receivedMessages, sentMessages);
        assertTrue(numReceivedMessages1 < numMessages,
                String.format("numReceivedMessages1 (%d) should be less than %d", numReceivedMessages1, numMessages));
        assertTrue(numReceivedMessages2 < numMessages,
                String.format("numReceivedMessages2 (%d) should be less than %d", numReceivedMessages2, numMessages));
    }

    /**
     * Tests replicated subscriptions when replicator producer is closed
     */
    @Test
    public void testReplicatedSubscriptionWhenReplicatorProducerIsClosed() throws Exception {
        String namespace = BrokerTestUtil.newUniqueName("pulsar/replicatedsubscription");
        String topicName = "persistent://" + namespace + "/when-replicator-producer-is-closed";
        String subscriptionName = "sub";

        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        // create consumer in r1
        @Cleanup
        Consumer<byte[]> consumer1 = client1.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .replicateSubscriptionState(true)
                .subscribe();

        // waiting to replicate topic/subscription to r1->r2
        Awaitility.await().until(() -> pulsar2.getBrokerService().getTopics().containsKey(topicName));
        final PersistentTopic topic2 = (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
        Awaitility.await().untilAsserted(() -> assertTrue(topic2.getReplicators().get("r1").isConnected()));
        Awaitility.await().untilAsserted(() -> assertNotNull(topic2.getSubscription(subscriptionName)));

        // unsubscribe replicated subscription in r2
        admin2.topics().deleteSubscription(topicName, subscriptionName);
        assertNull(topic2.getSubscription(subscriptionName));

        // close replicator producer in r2
        final Method closeReplProducersIfNoBacklog = PersistentTopic.class.getDeclaredMethod("closeReplProducersIfNoBacklog", null);
        closeReplProducersIfNoBacklog.setAccessible(true);
        ((CompletableFuture<Void>) closeReplProducersIfNoBacklog.invoke(topic2, null)).join();
        assertFalse(topic2.getReplicators().get("r1").isConnected());

        // send messages in r1
        int numMessages = 6;
        {
            @Cleanup
            Producer<byte[]> producer = client1.newProducer().topic(topicName)
                    .enableBatching(false)
                    .messageRoutingMode(MessageRoutingMode.SinglePartition)
                    .create();
            for (int i = 0; i < numMessages; i++) {
                String body = "message" + i;
                producer.send(body.getBytes(StandardCharsets.UTF_8));
            }
        }

        // consume 6 messages in r1
        Set<String> receivedMessages = new LinkedHashSet<>();
        assertEquals(readMessages(consumer1, receivedMessages, numMessages, false), numMessages);

        // wait for subscription to be replicated
        Awaitility.await().untilAsserted(() -> assertTrue(topic2.getReplicators().get("r1").isConnected()));
        Awaitility.await().untilAsserted(() -> assertNotNull(topic2.getSubscription(subscriptionName)));
    }

    void publishMessages(Producer<byte[]> producer, int startIndex, int numMessages, Set<String> sentMessages)
            throws PulsarClientException {
        for (int i = startIndex; i < startIndex + numMessages; i++) {
            final String msg = "msg" + i;
            producer.send(msg.getBytes(StandardCharsets.UTF_8));
            sentMessages.add(msg);
        }
    }

    int readMessages(Consumer<byte[]> consumer, Set<String> messages, int maxMessages, boolean allowDuplicates)
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
                consumer.acknowledge(message);
            } else {
                break;
            }
        }
        return count;
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
