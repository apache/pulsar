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
package org.apache.pulsar.client.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import java.net.URL;
import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentReplicator;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.client.impl.PartitionedProducerImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.NonPersistentPublisherStats;
import org.apache.pulsar.common.policies.data.NonPersistentSubscriptionStats;
import org.apache.pulsar.common.policies.data.NonPersistentTopicStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class NonPersistentTopicTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(NonPersistentTopicTest.class);
    private final String configClusterName = "r1";

    @DataProvider(name = "subscriptionType")
    public Object[][] getSubscriptionType() {
        return new Object[][] { { SubscriptionType.Shared }, { SubscriptionType.Exclusive } };
    }

    @DataProvider(name = "loadManager")
    public Object[][] getLoadManager() {
        return new Object[][] { { SimpleLoadManagerImpl.class.getCanonicalName() },
                { ModularLoadManagerImpl.class.getCanonicalName() } };
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 90000 /* 1.5mn */)
    public void testNonPersistentPartitionsAreNotAutoCreatedWhenThePartitionedTopicDoesNotExist() throws Exception {
        final boolean defaultAllowAutoTopicCreation = conf.isAllowAutoTopicCreation();
        try {
            // Given the auto topic creation is disabled
            cleanup();
            conf.setAllowAutoTopicCreation(false);
            setup();

            final String topicPartitionName = "non-persistent://public/default/issue-9173-partition-0";

            // Then error when subscribe to a partition of a non-persistent topic that does not exist
            assertThrows(PulsarClientException.NotFoundException.class,
                    () -> pulsarClient.newConsumer().topic(topicPartitionName).subscriptionName("sub-issue-9173").subscribe());

            // Then error when produce to a partition of a non-persistent topic that does not exist
            try {
                pulsarClient.newProducer().topic(topicPartitionName).create();
                Assert.fail("Should failed due to topic not exist");
            } catch (Exception e) {
                assertTrue(e instanceof PulsarClientException.NotFoundException);
            }
        } finally {
            conf.setAllowAutoTopicCreation(defaultAllowAutoTopicCreation);
        }
    }

    @Test(timeOut = 90000 /* 1.5mn */)
    public void testAutoCreateNonPersistentPartitionsWhenThePartitionedTopicExists() throws Exception {
        final boolean defaultAllowAutoTopicCreation = conf.isAllowAutoTopicCreation();
        try {
            // Given the auto topic creation is disabled
            cleanup();
            conf.setAllowAutoTopicCreation(false);
            setup();

            // Given the non-persistent partitioned topic exists
            final String topic = "non-persistent://public/default/issue-9173";
            admin.topics().createPartitionedTopic(topic, 3);

            // When subscribe, then a sub-consumer is created for each partition which means the partitions are created
            final MultiTopicsConsumerImpl<byte[]> consumer = (MultiTopicsConsumerImpl<byte[]>) pulsarClient.newConsumer()
                    .topic(topic).subscriptionName("sub-issue-9173").subscribe();
            assertEquals(consumer.getConsumers().size(), 3);

            // When produce, a sub-producer is created for each partition which means the partitions are created
            PartitionedProducerImpl<byte[]> producer = (PartitionedProducerImpl<byte[]>) pulsarClient.newProducer().topic(topic).create();
            assertEquals(producer.getProducers().size(), 3);

            consumer.close();
            producer.close();
        } finally {
            conf.setAllowAutoTopicCreation(defaultAllowAutoTopicCreation);
        }
    }

    @Test(dataProvider = "subscriptionType")
    public void testNonPersistentTopic(SubscriptionType type) throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String topic = "non-persistent://my-property/my-ns/unacked-topic";
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topic)
                .subscriptionName("subscriber-1").subscriptionType(type).subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();

        int totalProduceMsg = 500;
        for (int i = 0; i < totalProduceMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        producer.flush();

        Message<?> msg = null;
        Set<String> messageSet = new HashSet<>();
        for (int i = 0; i < totalProduceMsg; i++) {
            msg = consumer.receive(1, TimeUnit.SECONDS);
            if (msg != null) {
                consumer.acknowledge(msg);
                String receivedMessage = new String(msg.getData());
                log.debug("Received message: [{}]", receivedMessage);
                String expectedMessage = "my-message-" + i;
                testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            } else {
                break;
            }
        }
        assertEquals(messageSet.size(), totalProduceMsg);

        producer.close();
        consumer.close();
        log.info("-- Exiting {} test --", methodName);

    }

    @Test(dataProvider = "subscriptionType")
    public void testPartitionedNonPersistentTopic(SubscriptionType type) throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String topic = "non-persistent://my-property/my-ns/partitioned-topic";
        admin.topics().createPartitionedTopic(topic, 5);
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("subscriber-1")
                .subscriptionType(type).subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        int totalProduceMsg = 500;
        for (int i = 0; i < totalProduceMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        producer.flush();

        Message<?> msg = null;
        Set<String> messageSet = new HashSet<>();
        for (int i = 0; i < totalProduceMsg; i++) {
            msg = consumer.receive(1, TimeUnit.SECONDS);
            if (msg != null) {
                consumer.acknowledge(msg);
                String receivedMessage = new String(msg.getData());
                log.debug("Received message: [{}]", receivedMessage);
                String expectedMessage = "my-message-" + i;
                testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            } else {
                break;
            }
        }
        assertEquals(messageSet.size(), totalProduceMsg);

        producer.close();
        consumer.close();
        log.info("-- Exiting {} test --", methodName);

    }

    @Test(dataProvider = "subscriptionType")
    public void testPartitionedNonPersistentTopicWithTcpLookup(SubscriptionType type) throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int numPartitions = 5;
        final String topic = "non-persistent://my-property/my-ns/partitioned-topic";
        admin.topics().createPartitionedTopic(topic, numPartitions);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();
        Consumer<byte[]> consumer = client.newConsumer().topic(topic).subscriptionName("subscriber-1")
                .subscriptionType(type).subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // Ensure all partitions exist
        for (int i = 0; i < numPartitions; i++) {
            TopicName partition = TopicName.get(topic).getPartition(i);
            assertNotNull(pulsar.getBrokerService().getTopicReference(partition.toString()));
        }

        int totalProduceMsg = 500;
        for (int i = 0; i < totalProduceMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        producer.flush();

        Message<?> msg = null;
        Set<String> messageSet = new HashSet<>();
        for (int i = 0; i < totalProduceMsg; i++) {
            msg = consumer.receive(1, TimeUnit.SECONDS);
            if (msg != null) {
                consumer.acknowledge(msg);
                String receivedMessage = new String(msg.getData());
                log.debug("Received message: [{}]", receivedMessage);
                String expectedMessage = "my-message-" + i;
                testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            } else {
                break;
            }
        }
        assertEquals(messageSet.size(), totalProduceMsg);

        producer.close();
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * It verifies that broker doesn't dispatch messages if consumer runs out of permits filled out with messages
     */
    @Test(dataProvider = "subscriptionType")
    public void testConsumerInternalQueueMaxOut(SubscriptionType type) throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String topic = "non-persistent://my-property/my-ns/unacked-topic";
        final int queueSize = 10;
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topic)
                .receiverQueueSize(queueSize).subscriptionName("subscriber-1").subscriptionType(type).subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();

        int totalProduceMsg = 50;
        for (int i = 0; i < totalProduceMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        producer.flush();

        Message<?> msg = null;
        Set<String> messageSet = new HashSet<>();
        for (int i = 0; i < totalProduceMsg; i++) {
            msg = consumer.receive(1, TimeUnit.SECONDS);
            if (msg != null) {
                consumer.acknowledge(msg);
                String receivedMessage = new String(msg.getData());
                log.debug("Received message: [{}]", receivedMessage);
                String expectedMessage = "my-message-" + i;
                testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            } else {
                break;
            }
        }
        assertEquals(messageSet.size(), queueSize);

        producer.close();
        consumer.close();
        log.info("-- Exiting {} test --", methodName);

    }

    /**
     * Verifies that broker should failed to publish message if producer publishes messages more than rate limit
     */
    @Test
    public void testProducerRateLimit() throws Exception {
        int defaultNonPersistentMessageRate = conf.getMaxConcurrentNonPersistentMessagePerConnection();
        try {
            final String topic = "non-persistent://my-property/my-ns/unacked-topic";
            // restart broker with lower publish rate limit
            conf.setMaxConcurrentNonPersistentMessagePerConnection(1);
            stopBroker();
            startBroker();
            // produce message concurrently
            @Cleanup("shutdownNow")
            ExecutorService executor = Executors.newFixedThreadPool(5);
            AtomicBoolean failed = new AtomicBoolean(false);
            Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("subscriber-1")
                    .subscribe();
            Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
            byte[] msgData = "testData".getBytes();
            final int totalProduceMessages = 10;
            CountDownLatch latch = new CountDownLatch(totalProduceMessages);
            for (int i = 0; i < totalProduceMessages; i++) {
                executor.submit(() -> {
                    try {
                        producer.send(msgData);
                    } catch (Exception e) {
                        log.error("Failed to send message", e);
                        failed.set(true);
                    }
                    latch.countDown();
                });
            }
            latch.await();

            Message<?> msg = null;
            Set<String> messageSet = new HashSet<>();
            for (int i = 0; i < totalProduceMessages; i++) {
                msg = consumer.receive(500, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    messageSet.add(new String(msg.getData()));
                } else {
                    break;
                }
            }

            // publish should not be failed
            assertFalse(failed.get());
            // but as message should be dropped at broker: broker should not receive the message
            assertNotEquals(messageSet.size(), totalProduceMessages);

            producer.close();
        } finally {
            conf.setMaxConcurrentNonPersistentMessagePerConnection(defaultNonPersistentMessageRate);
        }
    }

    /**
     * verifies message delivery with multiple consumers on shared and failover subscriptions
     *
     * @throws Exception
     */
    @Test
    public void testMultipleSubscription() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String topic = "non-persistent://my-property/my-ns/unacked-topic";
        ConsumerImpl<byte[]> consumer1Shared = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topic)
                .subscriptionName("subscriber-shared").subscriptionType(SubscriptionType.Shared).subscribe();

        ConsumerImpl<byte[]> consumer2Shared = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topic)
                .subscriptionName("subscriber-shared").subscriptionType(SubscriptionType.Shared).subscribe();

        ConsumerImpl<byte[]> consumer1FailOver = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topic)
                .subscriptionName("subscriber-fo").subscriptionType(SubscriptionType.Failover).subscribe();

        ConsumerImpl<byte[]> consumer2FailOver = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topic)
                .subscriptionName("subscriber-fo").subscriptionType(SubscriptionType.Failover).subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();

        int totalProduceMsg = 500;
        for (int i = 0; i < totalProduceMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        producer.flush();

        // consume from shared-subscriptions
        Message<?> msg = null;
        Set<String> messageSet = new HashSet<>();
        for (int i = 0; i < totalProduceMsg; i++) {
            msg = consumer1Shared.receive(500, TimeUnit.MILLISECONDS);
            if (msg != null) {
                messageSet.add(new String(msg.getData()));
            } else {
                break;
            }
        }
        for (int i = 0; i < totalProduceMsg; i++) {
            msg = consumer2Shared.receive(500, TimeUnit.MILLISECONDS);
            if (msg != null) {
                messageSet.add(new String(msg.getData()));
            } else {
                break;
            }
        }
        assertEquals(messageSet.size(), totalProduceMsg);

        // consume from failover-subscriptions
        messageSet.clear();
        for (int i = 0; i < totalProduceMsg; i++) {
            msg = consumer1FailOver.receive(500, TimeUnit.MILLISECONDS);
            if (msg != null) {
                messageSet.add(new String(msg.getData()));
            } else {
                break;
            }
        }
        for (int i = 0; i < totalProduceMsg; i++) {
            msg = consumer2FailOver.receive(500, TimeUnit.MILLISECONDS);
            if (msg != null) {
                messageSet.add(new String(msg.getData()));
            } else {
                break;
            }
        }
        assertEquals(messageSet.size(), totalProduceMsg);

        producer.close();
        consumer1Shared.close();
        consumer2Shared.close();
        consumer1FailOver.close();
        consumer2FailOver.close();
        log.info("-- Exiting {} test --", methodName);

    }

    /**
     * verifies that broker is capturing topic stats correctly
     */
    @Test
    public void testTopicStats() throws Exception {

        final String topicName = "non-persistent://my-property/my-ns/unacked-topic";
        final String subName = "non-persistent";
        final int timeWaitToSync = 100;

        NonPersistentTopicStats stats;
        SubscriptionStats subStats;

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionType(SubscriptionType.Shared).subscriptionName(subName).subscribe();
        Thread.sleep(timeWaitToSync);

        NonPersistentTopic topicRef = (NonPersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);

        rolloverPerIntervalStats(pulsar);
        stats = topicRef.getStats(false, false, false);
        subStats = stats.getSubscriptions().values().iterator().next();

        // subscription stats
        assertEquals(stats.getSubscriptions().keySet().size(), 1);
        assertEquals(subStats.getConsumers().size(), 1);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        Thread.sleep(timeWaitToSync);

        int totalProducedMessages = 100;
        for (int i = 0; i < totalProducedMessages; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        Thread.sleep(timeWaitToSync);

        rolloverPerIntervalStats(pulsar);
        stats = topicRef.getStats(false, false, false);
        subStats = stats.getSubscriptions().values().iterator().next();

        assertTrue(subStats.getMsgRateOut() > 0);
        assertEquals(subStats.getConsumers().size(), 1);
        assertTrue(subStats.getMsgThroughputOut() > 0);

        // consumer stats
        assertTrue(subStats.getConsumers().get(0).getMsgRateOut() > 0.0);
        assertTrue(subStats.getConsumers().get(0).getMsgThroughputOut() > 0.0);
        assertEquals(subStats.getMsgRateRedeliver(), 0.0);
        producer.close();
        consumer.close();

    }

    /**
     * verifies that non-persistent topic replicates using replicator
     */
    @Test
    public void testReplicator() throws Exception {

        ReplicationClusterManager replication = new ReplicationClusterManager();
        replication.setupReplicationCluster();
        try {
            final String globalTopicName = "non-persistent://pulsar/global/ns/nonPersistentTopic";
            final int timeWaitToSync = 100;

            NonPersistentTopicStats stats;
            SubscriptionStats subStats;

            @Cleanup
            PulsarClient client1 = PulsarClient.builder().serviceUrl(replication.url1.toString()).build();
            @Cleanup
            PulsarClient client2 = PulsarClient.builder().serviceUrl(replication.url2.toString()).build();
            @Cleanup
            PulsarClient client3 = PulsarClient.builder().serviceUrl(replication.url3.toString()).build();

            ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) client1.newConsumer().topic(globalTopicName)
                    .subscriptionName("subscriber-1").subscribe();
            ConsumerImpl<byte[]> consumer2 = (ConsumerImpl<byte[]>) client1.newConsumer().topic(globalTopicName)
                    .subscriptionName("subscriber-2").subscribe();

            ConsumerImpl<byte[]> repl2Consumer = (ConsumerImpl<byte[]>) client2.newConsumer().topic(globalTopicName)
                    .subscriptionName("subscriber-1").subscribe();
            ConsumerImpl<byte[]> repl3Consumer = (ConsumerImpl<byte[]>) client3.newConsumer().topic(globalTopicName)
                    .subscriptionName("subscriber-1").subscribe();

            Producer<byte[]> producer = client1.newProducer().topic(globalTopicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

            Thread.sleep(timeWaitToSync);

            PulsarService replicationPulasr = replication.pulsar1;

            // Replicator for r1 -> r2,r3
            NonPersistentTopic topicRef = (NonPersistentTopic) replication.pulsar1.getBrokerService()
                    .getTopicReference(globalTopicName).get();
            NonPersistentReplicator replicatorR2 = (NonPersistentReplicator) topicRef.getPersistentReplicator("r2");
            NonPersistentReplicator replicatorR3 = (NonPersistentReplicator) topicRef.getPersistentReplicator("r3");
            assertNotNull(topicRef);
            assertNotNull(replicatorR2);
            assertNotNull(replicatorR3);

            rolloverPerIntervalStats(replicationPulasr);
            stats = topicRef.getStats(false, false, false);
            subStats = stats.getSubscriptions().values().iterator().next();

            // subscription stats
            assertEquals(stats.getSubscriptions().keySet().size(), 2);
            assertEquals(subStats.getConsumers().size(), 1);

            Thread.sleep(timeWaitToSync);

            int totalProducedMessages = 100;
            for (int i = 0; i < totalProducedMessages; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (1) consume by consumer1
            Message<?> msg = null;
            Set<String> messageSet = new HashSet<>();
            for (int i = 0; i < totalProducedMessages; i++) {
                msg = consumer1.receive(300, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    String receivedMessage = new String(msg.getData());
                    testMessageOrderAndDuplicates(messageSet, receivedMessage, "my-message-" + i);
                } else {
                    break;
                }
            }
            assertEquals(messageSet.size(), totalProducedMessages);

            // (2) consume by consumer2
            messageSet.clear();
            for (int i = 0; i < totalProducedMessages; i++) {
                msg = consumer2.receive(300, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    String receivedMessage = new String(msg.getData());
                    testMessageOrderAndDuplicates(messageSet, receivedMessage, "my-message-" + i);
                } else {
                    break;
                }
            }
            assertEquals(messageSet.size(), totalProducedMessages);

            // (3) consume by repl2consumer
            messageSet.clear();
            for (int i = 0; i < totalProducedMessages; i++) {
                msg = repl2Consumer.receive(300, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    String receivedMessage = new String(msg.getData());
                    testMessageOrderAndDuplicates(messageSet, receivedMessage, "my-message-" + i);
                } else {
                    break;
                }
            }
            assertEquals(messageSet.size(), totalProducedMessages);

            // (4) consume by repl3consumer
            messageSet.clear();
            for (int i = 0; i < totalProducedMessages; i++) {
                msg = repl3Consumer.receive(300, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    String receivedMessage = new String(msg.getData());
                    testMessageOrderAndDuplicates(messageSet, receivedMessage, "my-message-" + i);
                } else {
                    break;
                }
            }
            assertEquals(messageSet.size(), totalProducedMessages);

            Thread.sleep(timeWaitToSync);

            rolloverPerIntervalStats(replicationPulasr);
            stats = topicRef.getStats(false, false, false);
            subStats = stats.getSubscriptions().values().iterator().next();

            assertTrue(subStats.getMsgRateOut() > 0);
            assertEquals(subStats.getConsumers().size(), 1);
            assertTrue(subStats.getMsgThroughputOut() > 0);

            // consumer stats
            assertTrue(subStats.getConsumers().get(0).getMsgRateOut() > 0.0);
            assertTrue(subStats.getConsumers().get(0).getMsgThroughputOut() > 0.0);
            assertEquals(subStats.getMsgRateRedeliver(), 0.0);

            producer.close();
            consumer1.close();
            repl2Consumer.close();
            repl3Consumer.close();
        } finally {
            replication.shutdownReplicationCluster();
        }

    }

    /**
     * verifies load manager assigns topic only if broker started in non-persistent mode
     *
     * <pre>
     * 1. Start broker with disable non-persistent topic mode
     * 2. Create namespace with non-persistency set
     * 3. Create non-persistent topic
     * 4. Load-manager should not be able to find broker
     * 5. Create producer on that topic should fail
     * </pre>
     */
    @Test(dataProvider = "loadManager")
    public void testLoadManagerAssignmentForNonPersistentTestAssignment(String loadManagerName) throws Exception {

        final String namespace = "my-property/my-ns";
        final String topicName = "non-persistent://" + namespace + "/loadManager";
        final String defaultLoadManagerName = conf.getLoadManagerClassName();
        final boolean defaultENableNonPersistentTopic = conf.isEnableNonPersistentTopics();
        try {
            // start broker to not own non-persistent namespace and create non-persistent namespace
            stopBroker();
            conf.setEnableNonPersistentTopics(false);
            conf.setLoadManagerClassName(loadManagerName);
            startBroker();

            NamespaceBundle fdqn = pulsar.getNamespaceService().getBundle(TopicName.get(topicName));
            LoadManager loadManager = pulsar.getLoadManager().get();
            ResourceUnit broker = null;
            try {
                broker = loadManager.getLeastLoaded(fdqn).get();
            } catch (Exception e) {
                // Ok. (ModulearLoadManagerImpl throws RuntimeException incase don't find broker)
            }
            assertNull(broker);

            try {
                Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).createAsync().get(1,
                        TimeUnit.SECONDS);
                producer.close();
                fail("topic loading should have failed");
            } catch (Exception e) {
                // Ok
            }
            assertFalse(pulsar.getBrokerService().getTopicReference(topicName).isPresent());

        } finally {
            conf.setEnableNonPersistentTopics(defaultENableNonPersistentTopic);
            conf.setLoadManagerClassName(defaultLoadManagerName);
        }

    }

    /**
     * verifies: broker should reject non-persistent topic loading if broker is not enable for non-persistent topic
     *
     * @throws Exception
     */
    @Test
    public void testNonPersistentTopicUnderPersistentNamespace() throws Exception {

        final String namespace = "my-property/my-ns";
        final String topicName = "non-persistent://" + namespace + "/persitentNamespace";

        final boolean defaultENableNonPersistentTopic = conf.isEnableNonPersistentTopics();
        try {
            conf.setEnableNonPersistentTopics(false);
            stopBroker();
            startBroker();
            try {
                Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).createAsync().get(1,
                        TimeUnit.SECONDS);
                producer.close();
                fail("topic loading should have failed");
            } catch (Exception e) {
                // Ok
            }

            assertFalse(pulsar.getBrokerService().getTopicReference(topicName).isPresent());
        } finally {
            conf.setEnableNonPersistentTopics(defaultENableNonPersistentTopic);
        }
    }

    /**
     * verifies that broker started with onlyNonPersistent mode doesn't own persistent-topic
     *
     * @param loadManagerName
     * @throws Exception
     */
    @Test(dataProvider = "loadManager")
    public void testNonPersistentBrokerModeRejectPersistentTopic(String loadManagerName) throws Exception {

        final String namespace = "my-property/my-ns";
        final String topicName = "persistent://" + namespace + "/loadManager";
        final String defaultLoadManagerName = conf.getLoadManagerClassName();
        final boolean defaultEnablePersistentTopic = conf.isEnablePersistentTopics();
        final boolean defaultEnableNonPersistentTopic = conf.isEnableNonPersistentTopics();
        try {
            // start broker to not own non-persistent namespace and create non-persistent namespace
            stopBroker();
            conf.setEnableNonPersistentTopics(true);
            conf.setEnablePersistentTopics(false);
            conf.setLoadManagerClassName(loadManagerName);
            startBroker();

            NamespaceBundle fdqn = pulsar.getNamespaceService().getBundle(TopicName.get(topicName));
            LoadManager loadManager = pulsar.getLoadManager().get();
            ResourceUnit broker = null;
            try {
                broker = loadManager.getLeastLoaded(fdqn).get();
            } catch (Exception e) {
                // Ok. (ModulearLoadManagerImpl throws RuntimeException incase don't find broker)
            }
            assertNull(broker);

            try {
                Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).createAsync().get(1,
                        TimeUnit.SECONDS);
                producer.close();
                fail("topic loading should have failed");
            } catch (Exception e) {
                // Ok
            }

            assertFalse(pulsar.getBrokerService().getTopicReference(topicName).isPresent());

        } finally {
            conf.setEnablePersistentTopics(defaultEnablePersistentTopic);
            conf.setEnableNonPersistentTopics(defaultEnableNonPersistentTopic);
            conf.setLoadManagerClassName(defaultLoadManagerName);
        }

    }

    /**
     * Verifies msg-drop stats
     *
     * @throws Exception
     */
    @Test(timeOut = 60000)
    public void testMsgDropStat() throws Exception {

        int defaultNonPersistentMessageRate = conf.getMaxConcurrentNonPersistentMessagePerConnection();
        try {
            final String topicName = BrokerTestUtil.newUniqueName("non-persistent://my-property/my-ns/stats-topic");

            // For non-persistent topics, set the per-connection in-flight limit to 0.
            // Since ServerCnx drops when inFlight > max; with max=0, any second overlapping send on the
            // same connection is dropped (entryId == -1) and recorded. This makes observing a publisher drop
            // reliable in this test.
            conf.setMaxConcurrentNonPersistentMessagePerConnection(0);
            stopBroker();
            startBroker();
            Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("subscriber-1")
                    .receiverQueueSize(1).subscribe();

            Consumer<byte[]> consumer2 = pulsarClient.newConsumer().topic(topicName).subscriptionName("subscriber-2")
                    .receiverQueueSize(1).subscriptionType(SubscriptionType.Shared).subscribe();

            ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer().topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

            final int threads = 10;
            @Cleanup("shutdownNow")
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            byte[] msgData = "testData".getBytes();

            /*
             * Trigger at least one publisher drop through concurrent send() calls.
             *
             * Uses CyclicBarrier to ensure all threads send simultaneously, creating overlap.
             * With maxConcurrentNonPersistentMessagePerConnection = 0, ServerCnx#handleSend
             * drops any send while another is in-flight, returning MessageId with entryId = -1.
             * Awaitility repeats whole bursts (bounded to 20s) until a drop is observed.
             */
            AtomicBoolean publisherDropSeen = new AtomicBoolean(false);
            Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> {
                CyclicBarrier barrier = new CyclicBarrier(threads);
                CountDownLatch completionLatch = new CountDownLatch(threads);
                AtomicReference<Throwable> error = new AtomicReference<>();
                publisherDropSeen.set(false);

                for (int i = 0; i < threads; i++) {
                    executor.submit(() -> {
                        try {
                            barrier.await();
                            MessageId msgId = producer.send(msgData);
                            // Publisher drop is signaled by MessageIdImpl.entryId == -1
                            if (msgId instanceof MessageIdImpl && ((MessageIdImpl) msgId).getEntryId() == -1) {
                                publisherDropSeen.set(true);
                            }
                        } catch (Throwable t) {
                            if (t instanceof InterruptedException) {
                                Thread.currentThread().interrupt();
                            }
                            error.compareAndSet(null, t);
                        } finally {
                            completionLatch.countDown();
                        }
                    });
                }

                // Wait for all sends to complete.
                assertTrue(completionLatch.await(20, TimeUnit.SECONDS));

                assertNull(error.get(), "Concurrent send encountered an exception");
                return publisherDropSeen.get();
            });

            assertTrue(publisherDropSeen.get(), "Expected at least one publisher drop (entryId == -1)");

            NonPersistentTopic topic =
                    (NonPersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();

            Awaitility.await().ignoreExceptions().untilAsserted(() -> {
                pulsar.getBrokerService().updateRates();
                NonPersistentTopicStats stats = topic.getStats(false, false, false);
                NonPersistentPublisherStats npStats = stats.getPublishers().get(0);
                NonPersistentSubscriptionStats sub1Stats = stats.getSubscriptions().get("subscriber-1");
                NonPersistentSubscriptionStats sub2Stats = stats.getSubscriptions().get("subscriber-2");
                assertTrue(npStats.getMsgDropRate() > 0);
                assertTrue(sub1Stats.getMsgDropRate() > 0);
                assertTrue(sub2Stats.getMsgDropRate() > 0);
            });

            producer.close();
            consumer.close();
            consumer2.close();
        } finally {
            conf.setMaxConcurrentNonPersistentMessagePerConnection(defaultNonPersistentMessageRate);
        }
    }

    class ReplicationClusterManager {
        URL url1;
        PulsarService pulsar1;
        BrokerService ns1;

        PulsarAdmin admin1;
        LocalBookkeeperEnsemble bkEnsemble1;

        URL url2;
        ServiceConfiguration config2;
        PulsarService pulsar2;
        BrokerService ns2;
        PulsarAdmin admin2;
        LocalBookkeeperEnsemble bkEnsemble2;

        URL url3;
        ServiceConfiguration config3;
        PulsarService pulsar3;
        BrokerService ns3;
        PulsarAdmin admin3;
        LocalBookkeeperEnsemble bkEnsemble3;

        ZookeeperServerTest globalZkS;

        ExecutorService executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());

        static final int TIME_TO_CHECK_BACKLOG_QUOTA = 5;

        // Default frequency
        public int getBrokerServicePurgeInactiveFrequency() {
            return 60;
        }

        public boolean isBrokerServicePurgeInactiveTopic() {
            return false;
        }

        void setupReplicationCluster() throws Exception {
            log.info("--- Starting ReplicatorTestBase::setup ---");
            globalZkS = new ZookeeperServerTest(0);
            globalZkS.start();

            // Start region 1
            bkEnsemble1 = new LocalBookkeeperEnsemble(3, 0, () -> 0);
            bkEnsemble1.start();

            // NOTE: we have to instantiate a new copy of System.getProperties() to make sure pulsar1 and pulsar2 have
            // completely
            // independent config objects instead of referring to the same properties object
            ServiceConfiguration config1 = new ServiceConfiguration();
            config1.setClusterName(configClusterName);
            config1.setAdvertisedAddress("localhost");
            config1.setWebServicePort(Optional.of(0));
            config1.setMetadataStoreUrl("zk:127.0.0.1:" + bkEnsemble1.getZookeeperPort());
            config1.setConfigurationMetadataStoreUrl("zk:127.0.0.1:" + globalZkS.getZookeeperPort() + "/foo");
            config1.setBrokerDeleteInactiveTopicsEnabled(isBrokerServicePurgeInactiveTopic());
            config1.setBrokerDeleteInactiveTopicsFrequencySeconds(
                    inSec(getBrokerServicePurgeInactiveFrequency(), TimeUnit.SECONDS));
            config1.setBrokerShutdownTimeoutMs(0L);
            config1.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
            config1.setBrokerServicePort(Optional.of(0));
            config1.setBacklogQuotaCheckIntervalInSeconds(TIME_TO_CHECK_BACKLOG_QUOTA);
            config1.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
            pulsar1 = new PulsarService(config1);
            pulsar1.start();
            ns1 = pulsar1.getBrokerService();

            url1 = new URL(pulsar1.getWebServiceAddress());
            admin1 = PulsarAdmin.builder().serviceHttpUrl(url1.toString()).build();

            // Start region 2

            // Start zk & bks
            bkEnsemble2 = new LocalBookkeeperEnsemble(3, 0, () -> 0);
            bkEnsemble2.start();

            config2 = new ServiceConfiguration();
            config2.setClusterName("r2");
            config2.setWebServicePort(Optional.of(0));
            config2.setAdvertisedAddress("localhost");
            config2.setMetadataStoreUrl("zk:127.0.0.1:" + bkEnsemble2.getZookeeperPort());
            config2.setConfigurationMetadataStoreUrl("zk:127.0.0.1:" + globalZkS.getZookeeperPort() + "/foo");
            config2.setBrokerDeleteInactiveTopicsEnabled(isBrokerServicePurgeInactiveTopic());
            config2.setBrokerDeleteInactiveTopicsFrequencySeconds(
                    inSec(getBrokerServicePurgeInactiveFrequency(), TimeUnit.SECONDS));
            config2.setBrokerShutdownTimeoutMs(0L);
            config2.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
            config2.setBrokerServicePort(Optional.of(0));
            config2.setBacklogQuotaCheckIntervalInSeconds(TIME_TO_CHECK_BACKLOG_QUOTA);
            config2.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
            pulsar2 = new PulsarService(config2);
            pulsar2.start();
            ns2 = pulsar2.getBrokerService();

            url2 = new URL(pulsar2.getWebServiceAddress());
            admin2 = PulsarAdmin.builder().serviceHttpUrl(url2.toString()).build();

            // Start region 3

            // Start zk & bks
            bkEnsemble3 = new LocalBookkeeperEnsemble(3, 0, () -> 0);
            bkEnsemble3.start();

            config3 = new ServiceConfiguration();
            config3.setClusterName("r3");
            config3.setWebServicePort(Optional.of(0));
            config3.setAdvertisedAddress("localhost");
            config3.setMetadataStoreUrl("zk:127.0.0.1:" + bkEnsemble3.getZookeeperPort());
            config3.setConfigurationMetadataStoreUrl("zk:127.0.0.1:" + globalZkS.getZookeeperPort() + "/foo");
            config3.setBrokerDeleteInactiveTopicsEnabled(isBrokerServicePurgeInactiveTopic());
            config3.setBrokerDeleteInactiveTopicsFrequencySeconds(
                    inSec(getBrokerServicePurgeInactiveFrequency(), TimeUnit.SECONDS));
            config3.setBrokerShutdownTimeoutMs(0L);
            config3.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
            config3.setBrokerServicePort(Optional.of(0));
            config3.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
            pulsar3 = new PulsarService(config3);
            pulsar3.start();
            ns3 = pulsar3.getBrokerService();

            url3 = new URL(pulsar3.getWebServiceAddress());
            admin3 = PulsarAdmin.builder().serviceHttpUrl(url3.toString()).build();

            // Provision the global namespace
            admin1.clusters().createCluster("r1", ClusterData.builder()
                    .serviceUrl(url1.toString())
                    .brokerServiceUrl(pulsar1.getBrokerServiceUrl())
                    .brokerServiceUrlTls(pulsar1.getBrokerServiceUrlTls())
                    .build());
            admin1.clusters().createCluster("r2", ClusterData.builder()
                    .serviceUrl(url2.toString())
                    .brokerServiceUrl(pulsar2.getBrokerServiceUrl())
                    .brokerServiceUrlTls(pulsar1.getBrokerServiceUrlTls())
                    .build());
            admin1.clusters().createCluster("r3", ClusterData.builder()
                    .serviceUrl(url3.toString())
                    .brokerServiceUrl(pulsar3.getBrokerServiceUrl())
                    .brokerServiceUrlTls(pulsar1.getBrokerServiceUrlTls())
                    .build());

            admin1.clusters().createCluster("global", ClusterData.builder().serviceUrl("http://global:8080").build());
            admin1.tenants().createTenant("pulsar", new TenantInfoImpl(
                    Sets.newHashSet("appid1", "appid2", "appid3"), Sets.newHashSet("r1", "r2", "r3")));
            admin1.namespaces().createNamespace("pulsar/global/ns");
            admin1.namespaces().setNamespaceReplicationClusters("pulsar/global/ns",
                    Sets.newHashSet("r1", "r2", "r3"));

            assertEquals(admin2.clusters().getCluster("r1").getServiceUrl(), url1.toString());
            assertEquals(admin2.clusters().getCluster("r2").getServiceUrl(), url2.toString());
            assertEquals(admin2.clusters().getCluster("r3").getServiceUrl(), url3.toString());
            assertEquals(admin2.clusters().getCluster("r1").getBrokerServiceUrl(), pulsar1.getBrokerServiceUrl());
            assertEquals(admin2.clusters().getCluster("r2").getBrokerServiceUrl(), pulsar2.getBrokerServiceUrl());
            assertEquals(admin2.clusters().getCluster("r3").getBrokerServiceUrl(), pulsar3.getBrokerServiceUrl());
            Thread.sleep(100);
            log.info("--- ReplicatorTestBase::setup completed ---");

        }

        private int inSec(int time, TimeUnit unit) {
            return (int) TimeUnit.SECONDS.convert(time, unit);
        }

        void shutdownReplicationCluster() throws Exception {
            log.info("--- Shutting down ---");
            executor.shutdownNow();

            admin1.close();
            admin2.close();
            admin3.close();

            pulsar3.close();
            ns3.close();

            pulsar2.close();
            ns2.close();

            pulsar1.close();
            ns1.close();

            bkEnsemble1.stop();
            bkEnsemble2.stop();
            bkEnsemble3.stop();
            globalZkS.stop();
        }
    }

    private void rolloverPerIntervalStats(PulsarService pulsar) {
        try {
            pulsar.getExecutor().submit(() -> pulsar.getBrokerService().updateRates()).get();
        } catch (Exception e) {
            log.error("Stats executor error", e);
        }
    }
}
