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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.persistent.ReplicatedSubscriptionsController;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBufferState;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TransactionIsolationLevel;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
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
     * Tests replicated subscriptions across two regions and can read successful.
     */
    @Test
    public void testReplicatedSubscriptionAcrossTwoRegionsGetLastMessage() throws Exception {
        String namespace = BrokerTestUtil.newUniqueName("pulsar/replicatedsubscriptionlastmessage");
        String topicName = "persistent://" + namespace + "/mytopic";
        String subscriptionName = "cluster-subscription";
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


        // consume 3 messages in r1
        Set<String> receivedMessages = new LinkedHashSet<>();
        try (Consumer<byte[]> consumer1 = client1.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .replicateSubscriptionState(replicateSubscriptionState)
                .subscribe()) {
            readMessages(consumer1, receivedMessages, 3, false);
        }

        // wait for subscription to be replicated
        Thread.sleep(2 * config1.getReplicatedSubscriptionsSnapshotFrequencyMillis());

        // create a reader in r2
        Reader<byte[]> reader = client2.newReader().topic(topicName)
                .subscriptionName("new-sub")
                .startMessageId(MessageId.earliest)
                .create();
        int readNum = 0;
        while (reader.hasMessageAvailable()) {
            Message<byte[]> message = reader.readNext(10, TimeUnit.SECONDS);
            assertNotNull(message);
            log.info("Receive message: " + new String(message.getValue()) + " msgId: " + message.getMessageId());
            readNum++;
        }
        assertEquals(readNum, numMessages);
    }

    @Test
    public void testReplicatedSubscribeAndSwitchToStandbyCluster() throws Exception {
        final String namespace = BrokerTestUtil.newUniqueName("pulsar/ns_");
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + namespace + "/tp_");
        final String subscriptionName = "s1";
        final boolean isReplicatedSubscription = true;
        final int messagesCount = 20;
        final LinkedHashSet<String> sentMessages = new LinkedHashSet<>();
        final Set<String> receivedMessages = Collections.synchronizedSet(new LinkedHashSet<>());
        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));
        admin1.topics().createNonPartitionedTopic(topicName);
        admin1.topics().createSubscription(topicName, subscriptionName, MessageId.earliest, isReplicatedSubscription);
        final PersistentTopic topic1 =
                (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();

        // Send messages
        // Wait for the topic created on the cluster2.
        // Wait for the snapshot created.
        final PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).build();
        Producer<String> producer1 = client1.newProducer(Schema.STRING).topic(topicName).enableBatching(false).create();
        Consumer<String> consumer1 = client1.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionName(subscriptionName).replicateSubscriptionState(isReplicatedSubscription).subscribe();
        for (int i = 0; i < messagesCount / 2; i++) {
            String msg = i + "";
            producer1.send(msg);
            sentMessages.add(msg);
        }
        Awaitility.await().untilAsserted(() -> {
            ConcurrentOpenHashMap<String, ? extends Replicator> replicators = topic1.getReplicators();
            assertTrue(replicators != null && replicators.size() == 1, "Replicator should started");
            assertTrue(replicators.values().iterator().next().isConnected(), "Replicator should be connected");
            assertTrue(topic1.getReplicatedSubscriptionController().get().getLastCompletedSnapshotId().isPresent(),
                    "One snapshot should be finished");
        });
        final PersistentTopic topic2 =
                (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
        Awaitility.await().untilAsserted(() -> {
            assertTrue(topic2.getReplicatedSubscriptionController().isPresent(),
                    "Replicated subscription controller should created");
        });
        for (int i = messagesCount / 2; i < messagesCount; i++) {
            String msg = i + "";
            producer1.send(msg);
            sentMessages.add(msg);
        }

        // Consume half messages and wait the subscription created on the cluster2.
        for (int i = 0; i < messagesCount / 2; i++){
            Message<String> message = consumer1.receive(2, TimeUnit.SECONDS);
            if (message == null) {
                fail("Should not receive null.");
            }
            receivedMessages.add(message.getValue());
            consumer1.acknowledge(message);
        }
        Awaitility.await().untilAsserted(() -> {
            assertNotNull(topic2.getSubscriptions().get(subscriptionName), "Subscription should created");
        });

        // Switch client to cluster2.
        // Since the cluster1 was not crash, all messages will be replicated to the cluster2.
        consumer1.close();
        final PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString()).build();
        final Consumer consumer2 = client2.newConsumer(Schema.AUTO_CONSUME()).topic(topicName)
                .subscriptionName(subscriptionName).replicateSubscriptionState(isReplicatedSubscription).subscribe();

        // Verify all messages will be consumed.
        Awaitility.await().untilAsserted(() -> {
            while (true) {
                Message message = consumer2.receive(2, TimeUnit.SECONDS);
                if (message != null) {
                    receivedMessages.add(message.getValue().toString());
                    consumer2.acknowledge(message);
                } else {
                    break;
                }
            }
            assertEquals(receivedMessages.size(), sentMessages.size());
        });

        consumer2.close();
        producer1.close();
        client1.close();
        client2.close();
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

        // Validate that no snapshots are created before messages are published
        Thread.sleep(2 * config1.getReplicatedSubscriptionsSnapshotFrequencyMillis());
        PersistentTopic t1 = (PersistentTopic) pulsar1.getBrokerService()
                .getTopic(topicName, false).get().get();
        ReplicatedSubscriptionsController rsc1 = t1.getReplicatedSubscriptionController().get();
        // no snapshot should have been created before any messages are published
        assertTrue(rsc1.getLastCompletedSnapshotId().isEmpty());

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

    @Test(timeOut = 30000)
    public void testReplicatedSubscriptionRestApi3() throws Exception {
        final String namespace = BrokerTestUtil.newUniqueName("geo/replicatedsubscription");
        final String topicName = "persistent://" + namespace + "/topic-rest-api3";
        final String subName = "sub";
        admin4.tenants().createTenant("geo",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid4"), Sets.newHashSet(cluster1, cluster4)));
        admin4.namespaces().createNamespace(namespace);
        admin4.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(cluster1, cluster4));
        admin4.topics().createPartitionedTopic(topicName, 2);

        @Cleanup
        final PulsarClient client4 = PulsarClient.builder().serviceUrl(url4.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();

        Consumer<byte[]> consumer4 = client4.newConsumer().topic(topicName).subscriptionName(subName).subscribe();
        Assert.expectThrows(PulsarAdminException.class, () ->
                admin4.topics().setReplicatedSubscriptionStatus(topicName, subName, true));
        consumer4.close();
    }

    /**
     * before sending message, we should wait for transaction buffer recover complete,
     * or the MaxReadPosition will not move forward when the message is sent, and the
     * MaxReadPositionMovedForwardTimestamp will not be updated, then the replication will not be triggered.
     * @param topicName
     * @throws Exception
     */
    private void waitTBRecoverComplete(PulsarService pulsarService, String topicName) throws Exception {
        TopicTransactionBufferState buffer = (TopicTransactionBufferState) ((PersistentTopic) pulsarService.getBrokerService()
                .getTopic(topicName, false).get().get()).getTransactionBuffer();
        Field stateField = TopicTransactionBufferState.class.getDeclaredField("state");
        stateField.setAccessible(true);
        Awaitility.await().until(() -> !stateField.get(buffer).toString().equals("Initializing"));
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

        {
            // create consumer in r1
            @Cleanup
            Consumer<byte[]> consumer = client1.newConsumer()
                    .topic(topicName)
                    .subscriptionName(subscriptionName)
                    .replicateSubscriptionState(true)
                    .subscribe();

            // send one message to trigger replication
            if (config1.isTransactionCoordinatorEnabled()) {
                waitTBRecoverComplete(pulsar1, topicName);
            }
            @Cleanup
            Producer<byte[]> producer = client1.newProducer().topic(topicName)
                    .enableBatching(false)
                    .messageRoutingMode(MessageRoutingMode.SinglePartition)
                    .create();
            producer.send("message".getBytes(StandardCharsets.UTF_8));

            assertEquals(readMessages(consumer, new HashSet<>(), 1, false), 1);

            // waiting to replicate topic/subscription to r1->r2
            Awaitility.await().until(() -> pulsar2.getBrokerService().getTopics().containsKey(topicName));
            final PersistentTopic topic2 = (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
            Awaitility.await().untilAsserted(() -> assertTrue(topic2.getReplicators().get("r1").isConnected()));
            Awaitility.await().untilAsserted(() -> assertNotNull(topic2.getSubscription(subscriptionName)));
        }

        // unsubscribe replicated subscription in r2
        admin2.topics().deleteSubscription(topicName, subscriptionName);
        final PersistentTopic topic2 = (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
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
        @Cleanup
        Consumer<byte[]> consumer1 = client1.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .replicateSubscriptionState(true)
                .subscribe();
        assertEquals(readMessages(consumer1, receivedMessages, numMessages, false), numMessages);

        // wait for subscription to be replicated
        Awaitility.await().untilAsserted(() -> assertTrue(topic2.getReplicators().get("r1").isConnected()));
        Awaitility.await().untilAsserted(() -> assertNotNull(topic2.getSubscription(subscriptionName)));
    }

    @DataProvider(name = "isTopicPolicyEnabled")
    private Object[][] isTopicPolicyEnabled() {
        // Todo: fix replication can not be enabled at topic level.
        return new Object[][] { { Boolean.FALSE } };
    }

    /**
     * Test the replication subscription can work normal in the following cases:
     *  <p>
     *      1. Do not write data into the original topic when the topic does not configure a remote cluster. {topic1}
     *          1. Publish message to the topic and then wait a moment,
     *          the backlog will not increase after publishing completely.
     *          2. Acknowledge the messages, the last confirm entry does not change.
     *      2. Snapshot and mark will be written after topic configure a remote cluster. {topic2}
     *          1. publish message to topic. After publishing completely, the backlog of the topic keep increase.
     *          2. Wait the snapshot complete, the backlog stop changing.
     *          3. Publish messages to wait another snapshot complete.
     *          4. Ack messages to move the mark delete position after the position record in the first snapshot.
     *          5. Check new entry (a mark) appending to the original topic.
     *       3. Stopping writing snapshot and mark after remove the remote cluster of the topic. {topic2}
     *          similar to step 1.
     *  </p>
     */
    @Test(dataProvider = "isTopicPolicyEnabled")
    public void testWriteMarkerTaskOfReplicateSubscriptions(boolean isTopicPolicyEnabled) throws Exception {
        // 1. Prepare resource and use proper configuration.
        String namespace = BrokerTestUtil.newUniqueName("pulsar/testReplicateSubBackLog");
        String topic1 = "persistent://" + namespace + "/replication-enable";
        String topic2 = "persistent://" + namespace + "/replication-disable";
        String subName = "sub";

        admin1.namespaces().createNamespace(namespace);
        pulsar1.getConfiguration().setTopicLevelPoliciesEnabled(isTopicPolicyEnabled);
        pulsar1.getConfiguration().setReplicationPolicyCheckDurationSeconds(1);
        pulsar1.getConfiguration().setReplicatedSubscriptionsSnapshotFrequencyMillis(1000);
        // 2. Build Producer and Consumer.
        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();
        @Cleanup
        Consumer<byte[]> consumer1 = client1.newConsumer()
                .topic(topic1)
                .subscriptionName(subName)
                .ackTimeout(5, TimeUnit.SECONDS)
                .subscriptionType(SubscriptionType.Shared)
                .replicateSubscriptionState(true)
                .subscribe();
        @Cleanup
        Producer<byte[]> producer1 = client1.newProducer()
                .topic(topic1)
                .create();
        // 3. Test replication subscription work as expected.
        // Test case 1: disable replication, backlog will not increase.
        testReplicatedSubscriptionWhenDisableReplication(producer1, consumer1, topic1);

        // Test case 2: enable replication, mark and snapshot work as expected.
        if (isTopicPolicyEnabled) {
            admin1.topics().createNonPartitionedTopic(topic2);
            admin1.topics().setReplicationClusters(topic2, List.of("r1", "r2"));
        } else {
            admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));
        }
        @Cleanup
        Consumer<byte[]> consumer2 = client1.newConsumer()
                .topic(topic2)
                .subscriptionName(subName)
                .ackTimeout(5, TimeUnit.SECONDS)
                .subscriptionType(SubscriptionType.Shared)
                .replicateSubscriptionState(true)
                .subscribe();
        @Cleanup
        Producer<byte[]> producer2 = client1.newProducer()
                .topic(topic2)
                .create();
        testReplicatedSubscriptionWhenEnableReplication(producer2, consumer2, topic2);

        // Test case 3: enable replication, mark and snapshot work as expected.
        if (isTopicPolicyEnabled) {
            admin1.topics().setReplicationClusters(topic2, List.of("r1"));
        } else {
            admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1"));
        }
        testReplicatedSubscriptionWhenDisableReplication(producer2, consumer2, topic2);
        // 4. Clear resource.
        pulsar1.getConfiguration().setForceDeleteNamespaceAllowed(true);
        admin1.namespaces().deleteNamespace(namespace, true);
        pulsar1.getConfiguration().setForceDeleteNamespaceAllowed(false);
    }

    @Test
    public void testReplicatedSubscriptionWithCompaction() throws Exception {
        final String namespace = BrokerTestUtil.newUniqueName("pulsar/replicatedsubscription");
        final String topicName = "persistent://" + namespace + "/testReplicatedSubscriptionWithCompaction";
        final String subName = "sub";

        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));
        admin1.topics().createNonPartitionedTopic(topicName);
        admin1.topicPolicies().setCompactionThreshold(topicName, 100 * 1024 * 1024L);

        @Cleanup final PulsarClient client = PulsarClient.builder().serviceUrl(url1.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();

        Producer<String> producer = client.newProducer(Schema.STRING).topic(topicName).create();
        if (config1.isTransactionCoordinatorEnabled()) {
            waitTBRecoverComplete(pulsar1, topicName);
        }
        producer.newMessage().key("K1").value("V1").send();
        producer.newMessage().key("K1").value("V2").send();
        producer.close();

        createReplicatedSubscription(client, topicName, subName, true);
        Awaitility.await().untilAsserted(() -> {
            Map<String, Boolean> status = admin1.topics().getReplicatedSubscriptionStatus(topicName, subName);
            assertTrue(status.get(topicName));
        });

        Awaitility.await().untilAsserted(() -> {
            PersistentTopic t1 = (PersistentTopic) pulsar1.getBrokerService()
                .getTopic(topicName, false).get().get();
        ReplicatedSubscriptionsController rsc1 = t1.getReplicatedSubscriptionController().get();
        Assert.assertTrue(rsc1.getLastCompletedSnapshotId().isPresent());
        assertEquals(t1.getPendingWriteOps().get(), 0L);
        });

        admin1.topics().triggerCompaction(topicName);

        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin1.topics().compactionStatus(topicName).status,
                    LongRunningProcessStatus.Status.SUCCESS);
        });

        @Cleanup
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("sub2")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .readCompacted(true)
                .subscribe();
        List<String> result = new ArrayList<>();
        while (true) {
            Message<String> receive = consumer.receive(2, TimeUnit.SECONDS);
            if (receive == null) {
                break;
            }

            result.add(receive.getValue());
        }

        Assert.assertEquals(result, List.of("V2"));
    }

    @Test
    public void testReplicatedSubscriptionOneWay() throws Exception {
        final String namespace = BrokerTestUtil.newUniqueName("pulsar-r4/replicatedsubscription");
        final String topicName = "persistent://" + namespace + "/one-way";
        int defaultSubscriptionsSnapshotFrequency = config1.getReplicatedSubscriptionsSnapshotFrequencyMillis();
        int defaultSubscriptionsSnapshotTimeout = config1.getReplicatedSubscriptionsSnapshotTimeoutSeconds();
        config1.setReplicatedSubscriptionsSnapshotTimeoutSeconds(2);
        config1.setReplicatedSubscriptionsSnapshotFrequencyMillis(100);
        
        // cluster4 disabled ReplicatedSubscriptions
        admin1.tenants().createTenant("pulsar-r4",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid4"), Sets.newHashSet(cluster1, cluster4)));
        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(cluster1, cluster4));
        
        String subscriptionName = "cluster-subscription";
        boolean replicateSubscriptionState = true;

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        @Cleanup
        final PulsarClient client4 = PulsarClient.builder().serviceUrl(url4.toString())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        // create subscription in cluster4
        createReplicatedSubscription(client1, topicName, subscriptionName, replicateSubscriptionState);
        // create subscription in cluster4
        createReplicatedSubscription(client4, topicName, subscriptionName, replicateSubscriptionState);

        // send messages in cluster1
        @Cleanup
        Producer<byte[]> producer = client1.newProducer().topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        int numMessages = 6;
        for (int i = 0; i < numMessages; i++) {
            String body = "message" + i;
            producer.send(body.getBytes(StandardCharsets.UTF_8));
        }
        producer.close();

        // wait for snapshot marker request to be replicated
        Thread.sleep(3 * config1.getReplicatedSubscriptionsSnapshotFrequencyMillis());

        // Assert just have 1 pending snapshot in cluster1
        final PersistentTopic topic1 =
                (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
        ReplicatedSubscriptionsController r1Controller =
                topic1.getReplicatedSubscriptionController().get();
        assertEquals(r1Controller.pendingSnapshots().size(), 1);
        
        // Assert cluster4 just receive 1 snapshot request msg
        int numSnapshotRequest = 0;
        List<Message<byte[]>> r4Messages = admin4.topics()
                .peekMessages(topicName, subscriptionName, 100, true, TransactionIsolationLevel.READ_UNCOMMITTED);
        for (Message<byte[]> r4Message : r4Messages) {
            MessageMetadata msgMetadata = ((MessageImpl<byte[]>) r4Message).getMessageBuilder();
            if (msgMetadata.hasMarkerType() && msgMetadata.getMarkerType() == MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT_REQUEST_VALUE) {
                numSnapshotRequest++;
            }
        }
        Assert.assertEquals(numSnapshotRequest, 1);

        // Wait pending snapshot timeout
        Thread.sleep(config1.getReplicatedSubscriptionsSnapshotTimeoutSeconds() * 1000);
        numSnapshotRequest = 0;
        r4Messages = admin4.topics()
                .peekMessages(topicName, subscriptionName, 100, true, TransactionIsolationLevel.READ_UNCOMMITTED);
        for (Message<byte[]> r4Message : r4Messages) {
            MessageMetadata msgMetadata = ((MessageImpl<byte[]>) r4Message).getMessageBuilder();
            if (msgMetadata.hasMarkerType() && msgMetadata.getMarkerType() == MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT_REQUEST_VALUE) {
                numSnapshotRequest++;
            }
        }
        Assert.assertEquals(numSnapshotRequest, 2);

        // Set back to default config.
        config1.setReplicatedSubscriptionsSnapshotTimeoutSeconds(defaultSubscriptionsSnapshotTimeout);
        config1.setReplicatedSubscriptionsSnapshotFrequencyMillis(defaultSubscriptionsSnapshotFrequency);
    }

    /**
     * Disable replication subscription.
     *    Test scheduled task case.
     *    1. Send three messages |1:0|1:1|1:2|.
     *    2. Get topic backlog, as backlog1.
     *    3. Wait a moment.
     *    4. Get the topic backlog again, the backlog will not increase.
     *    Test acknowledge messages case.
     *    1. Get the last confirm entry, as LAC1.
     *    2. Acknowledge these messages |1:0|1:1|.
     *    3. wait a moment.
     *    4. Get the last confirm entry, as LAC2. LAC1 is equal to LAC2.
     *    Clear environment.
     *    1. Ack all the retained messages. |1:2|
     *    2. Wait for the backlog to return to zero.
     */
    private void testReplicatedSubscriptionWhenDisableReplication(Producer<byte[]> producer, Consumer<byte[]> consumer,
                                                                  String topic) throws Exception {
        final int messageSum = 3;
        // Test scheduled task case.
        for (int i = 0; i < messageSum; i++) {
            producer.newMessage().send();
        }
        long backlog1 = admin1.topics().getStats(topic, false).getBacklogSize();
        Thread.sleep(3000);
        long backlog2 = admin1.topics().getStats(topic, false).getBacklogSize();
        assertEquals(backlog1, backlog2);
        // Test acknowledge messages case.
        String lastConfirmEntry1 = admin1.topics().getInternalStats(topic).lastConfirmedEntry;
        for (int i = 0; i < messageSum - 1; i++) {
            consumer.acknowledge(consumer.receive(5, TimeUnit.SECONDS));
        }
        Awaitility.await().untilAsserted(() -> {
            String lastConfirmEntry2 = admin1.topics().getInternalStats(topic).lastConfirmedEntry;
            assertEquals(lastConfirmEntry1, lastConfirmEntry2);
        });
        // Clear environment.
        consumer.acknowledge(consumer.receive(5, TimeUnit.SECONDS));
        Awaitility.await().untilAsserted(() -> {
            long backlog4 = admin1.topics().getStats(topic, false).getBacklogSize();
            assertEquals(backlog4, 0);
        });
    }

    /**
     * Enable replication subscription.
     *    Test scheduled task case.
     *    1. Wait replicator connected.
     *    2. Send three messages |1:0|1:1|1:2|.
     *    3. Get topic backlog, as backlog1.
     *    4. Wait a moment.
     *    5. Get the topic backlog again, as backlog2. The backlog2 is bigger than backlog1. |1:0|1:1|1:2|mark|.
     *    6. Wait the snapshot complete.
     *    Test acknowledge messages case.
     *    1. Write messages and wait another snapshot complete. |1:0|1:1|1:2|mark|1:3|1:4|1:5|mark|
     *    2. Ack message |1:0|1:1|1:2|1:3|1:4|.
     *    3. Get last confirm entry, as LAC1.
     *    2. Wait a moment.
     *    3. Get Last confirm entry, as LAC2. LAC2 different to LAC1. |1:5|mark|mark|
     *    Clear environment.
     *    1. Ack all the retained message |1:5|.
     *    2. Wait for the backlog to return to zero.
     */
    private void testReplicatedSubscriptionWhenEnableReplication(Producer<byte[]> producer, Consumer<byte[]> consumer,
                                                                String topic) throws Exception {
        final int messageSum = 3;
        Awaitility.await().untilAsserted(() -> {
            List<String> keys = pulsar1.getBrokerService()
                    .getTopic(topic, false).get().get()
                    .getReplicators().keys();
            assertEquals(keys.size(), 1);
            assertTrue(pulsar1.getBrokerService()
                    .getTopic(topic, false).get().get()
                    .getReplicators().get(keys.get(0)).isConnected());
        });
        // Test scheduled task case.
        sendMessageAndWaitSnapshotComplete(producer, topic, messageSum);
        // Test acknowledge messages case.
        // After snapshot write completely, acknowledging message to move the mark delete position
        // after the position recorded in the snapshot will trigger to write a new marker.
        sendMessageAndWaitSnapshotComplete(producer, topic, messageSum);
        String lastConfirmedEntry3 = admin1.topics().getInternalStats(topic, false).lastConfirmedEntry;
        for (int i = 0; i < messageSum * 2 - 1; i++) {
            consumer.acknowledge(consumer.receive(5, TimeUnit.SECONDS));
        }
        Awaitility.await().untilAsserted(() -> {
            String lastConfirmedEntry4 = admin1.topics().getInternalStats(topic, false).lastConfirmedEntry;
            assertNotEquals(lastConfirmedEntry3, lastConfirmedEntry4);
        });
        // Clear environment.
        consumer.acknowledge(consumer.receive(5, TimeUnit.SECONDS));
        Awaitility.await().untilAsserted(() -> {
            long backlog4 = admin1.topics().getStats(topic, false).getBacklogSize();
            assertEquals(backlog4, 0);
        });
    }

    private void sendMessageAndWaitSnapshotComplete(Producer<byte[]> producer, String topic,
                                                    int messageSum) throws Exception {
        for (int i = 0; i < messageSum; i++) {
            producer.newMessage().send();
        }
        long backlog1 = admin1.topics().getStats(topic, false).getBacklogSize();
        Awaitility.await().untilAsserted(() -> {
            long backlog2 = admin1.topics().getStats(topic, false).getBacklogSize();
            assertTrue(backlog2 > backlog1);
        });
        // Wait snapshot write completely, stop writing marker into topic.
        Awaitility.await().untilAsserted(() -> {
            String lastConfirmedEntry1 = admin1.topics().getInternalStats(topic, false).lastConfirmedEntry;
            PersistentTopicInternalStats persistentTopicInternalStats =  admin1.topics().getInternalStats(topic, false);
            Thread.sleep(1000);
            String lastConfirmedEntry2 = admin1.topics().getInternalStats(topic, false).lastConfirmedEntry;
            assertEquals(lastConfirmedEntry1, lastConfirmedEntry2);
        });
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
