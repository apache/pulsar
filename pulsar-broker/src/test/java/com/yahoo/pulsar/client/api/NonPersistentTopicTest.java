/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.net.URL;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.test.PortManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.service.BrokerService;
import com.yahoo.pulsar.broker.service.nonpersistent.NonPersistentReplicator;
import com.yahoo.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import com.yahoo.pulsar.client.admin.PulsarAdmin;
import com.yahoo.pulsar.client.impl.ConsumerImpl;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.policies.data.ClusterData;
import com.yahoo.pulsar.common.policies.data.PersistentSubscriptionStats;
import com.yahoo.pulsar.common.policies.data.PersistentTopicStats;
import com.yahoo.pulsar.common.policies.data.PropertyAdmin;
import com.yahoo.pulsar.zookeeper.LocalBookkeeperEnsemble;
import com.yahoo.pulsar.zookeeper.ZookeeperServerTest;

public class NonPersistentTopicTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(NonPersistentTopicTest.class);

    @DataProvider(name = "subscriptionType")
    public Object[][] getSubscriptionType() {
        return new Object[][] { { SubscriptionType.Shared }, { SubscriptionType.Exclusive } };
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(dataProvider = "subscriptionType")
    public void testNonPersistentTopic(SubscriptionType type) throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String topic = "non-persistent://my-property/use/my-ns/unacked-topic";
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(type);

        ProducerConfiguration producerConf = new ProducerConfiguration();

        ConsumerImpl consumer = (ConsumerImpl) pulsarClient.subscribe(topic, "subscriber-1", conf);

        Producer producer = pulsarClient.createProducer(topic, producerConf);

        int totalProduceMsg = 500;
        for (int i = 0; i < totalProduceMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            Thread.sleep(10);
        }

        Message msg = null;
        Set<String> messageSet = Sets.newHashSet();
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

        final String topic = "non-persistent://my-property/use/my-ns/unacked-topic";
        admin.nonPersistentTopics().createPartitionedTopic(topic, 5);
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(type);

        ProducerConfiguration producerConf = new ProducerConfiguration();

        Consumer consumer = pulsarClient.subscribe(topic, "subscriber-1", conf);

        Producer producer = pulsarClient.createProducer(topic, producerConf);

        int totalProduceMsg = 500;
        for (int i = 0; i < totalProduceMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            Thread.sleep(10);
        }

        Message msg = null;
        Set<String> messageSet = Sets.newHashSet();
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
     * It verifies that consumer drops out messages if consumer is slow to consume message and once internal queue
     * filled out with messages
     */
    @Test(dataProvider = "subscriptionType")
    public void testConsumerInternalQueueMaxOut(SubscriptionType type) throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String topic = "non-persistent://my-property/use/my-ns/unacked-topic";
        final int queueSize = 10;
        ConsumerConfiguration conf = new ConsumerConfiguration();
        // conf.setSubscriptionType(type);
        conf.setSubscriptionType(SubscriptionType.Shared);
        conf.setReceiverQueueSize(queueSize);

        ProducerConfiguration producerConf = new ProducerConfiguration();

        ConsumerImpl consumer = (ConsumerImpl) pulsarClient.subscribe(topic, "subscriber-1", conf);

        Producer producer = pulsarClient.createProducer(topic, producerConf);

        int totalProduceMsg = 50;
        for (int i = 0; i < totalProduceMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            Thread.sleep(10);
        }

        Message msg = null;
        Set<String> messageSet = Sets.newHashSet();
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
            final String topic = "non-persistent://my-property/use/my-ns/unacked-topic";
            // restart broker with lower publish rate limit
            conf.setMaxConcurrentNonPersistentMessagePerConnection(1);
            stopBroker();
            startBroker();
            // produce message concurrently
            ExecutorService executor = Executors.newFixedThreadPool(10);
            AtomicBoolean failed = new AtomicBoolean(false);
            ProducerConfiguration producerConf = new ProducerConfiguration();
            Producer producer = pulsarClient.createProducer(topic, producerConf);
            byte[] msgData = "testData".getBytes();
            final int totalProduceMessages = 10;
            CountDownLatch latch = new CountDownLatch(totalProduceMessages);
            for (int i = 0; i < totalProduceMessages; i++) {
                executor.submit(() -> {
                    try {
                        producer.sendAsync(msgData).get(1, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        failed.set(true);
                    }
                    latch.countDown();
                });
            }
            latch.await();
            executor.shutdown();
            assertTrue(failed.get());
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

        final String topic = "non-persistent://my-property/use/my-ns/unacked-topic";
        ConsumerConfiguration sharedConf = new ConsumerConfiguration();
        sharedConf.setSubscriptionType(SubscriptionType.Shared);
        ConsumerConfiguration excConf = new ConsumerConfiguration();
        excConf.setSubscriptionType(SubscriptionType.Failover);

        ProducerConfiguration producerConf = new ProducerConfiguration();

        ConsumerImpl consumer1Shared = (ConsumerImpl) pulsarClient.subscribe(topic, "subscriber-shared", sharedConf);
        ConsumerImpl consumer2Shared = (ConsumerImpl) pulsarClient.subscribe(topic, "subscriber-shared", sharedConf);
        ConsumerImpl consumer1FailOver = (ConsumerImpl) pulsarClient.subscribe(topic, "subscriber-fo", excConf);
        ConsumerImpl consumer2FailOver = (ConsumerImpl) pulsarClient.subscribe(topic, "subscriber-fo", excConf);

        Producer producer = pulsarClient.createProducer(topic, producerConf);

        int totalProduceMsg = 500;
        for (int i = 0; i < totalProduceMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            Thread.sleep(10);
        }

        // consume from shared-subscriptions
        Message msg = null;
        Set<String> messageSet = Sets.newHashSet();
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

        final String topicName = "non-persistent://my-property/use/my-ns/unacked-topic";
        final String subName = "non-persistent";
        final int timeWaitToSync = 100;

        PersistentTopicStats stats;
        PersistentSubscriptionStats subStats;

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Shared);
        Consumer consumer = pulsarClient.subscribe(topicName, subName, conf);
        Thread.sleep(timeWaitToSync);

        NonPersistentTopic topicRef = (NonPersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);
        assertNotNull(topicRef);

        rolloverPerIntervalStats(pulsar);
        stats = topicRef.getStats();
        subStats = stats.subscriptions.values().iterator().next();

        // subscription stats
        assertEquals(stats.subscriptions.keySet().size(), 1);
        assertEquals(subStats.consumers.size(), 1);

        Producer producer = pulsarClient.createProducer(topicName);
        Thread.sleep(timeWaitToSync);

        int totalProducedMessages = 100;
        for (int i = 0; i < totalProducedMessages; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        Thread.sleep(timeWaitToSync);

        rolloverPerIntervalStats(pulsar);
        stats = topicRef.getStats();
        subStats = stats.subscriptions.values().iterator().next();

        assertTrue(subStats.msgRateOut > 0);
        assertEquals(subStats.consumers.size(), 1);
        assertTrue(subStats.msgThroughputOut > 0);

        // consumer stats
        assertTrue(subStats.consumers.get(0).msgRateOut > 0.0);
        assertTrue(subStats.consumers.get(0).msgThroughputOut > 0.0);
        assertEquals(subStats.msgRateRedeliver, 0.0);
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

            PersistentTopicStats stats;
            PersistentSubscriptionStats subStats;

            DestinationName dest = DestinationName.get(globalTopicName);

            PulsarClient client1 = PulsarClient.create(replication.url1.toString(), new ClientConfiguration());
            PulsarClient client2 = PulsarClient.create(replication.url2.toString(), new ClientConfiguration());
            PulsarClient client3 = PulsarClient.create(replication.url3.toString(), new ClientConfiguration());

            ProducerConfiguration producerConf = new ProducerConfiguration();
            ConsumerConfiguration consumerConf = new ConsumerConfiguration();
            ConsumerImpl consumer1 = (ConsumerImpl) client1.subscribe(globalTopicName, "subscriber-1", consumerConf);
            ConsumerImpl consumer2 = (ConsumerImpl) client1.subscribe(globalTopicName, "subscriber-2", consumerConf);
            ConsumerImpl repl2Consumer = (ConsumerImpl) client2.subscribe(globalTopicName, "subscriber-1",
                    consumerConf);
            ConsumerImpl repl3Consumer = (ConsumerImpl) client3.subscribe(globalTopicName, "subscriber-1",
                    consumerConf);
            Producer producer = client1.createProducer(globalTopicName, producerConf);

            Thread.sleep(timeWaitToSync);

            PulsarService replicationPulasr = replication.pulsar1;

            // Replicator for r1 -> r2,r3
            NonPersistentTopic topicRef = (NonPersistentTopic) replication.pulsar1.getBrokerService()
                    .getTopicReference(dest.toString());
            NonPersistentReplicator replicatorR2 = (NonPersistentReplicator) topicRef.getPersistentReplicator("r2");
            NonPersistentReplicator replicatorR3 = (NonPersistentReplicator) topicRef.getPersistentReplicator("r3");
            assertNotNull(topicRef);
            assertNotNull(replicatorR2);
            assertNotNull(replicatorR3);

            rolloverPerIntervalStats(replicationPulasr);
            stats = topicRef.getStats();
            subStats = stats.subscriptions.values().iterator().next();

            // subscription stats
            assertEquals(stats.subscriptions.keySet().size(), 2);
            assertEquals(subStats.consumers.size(), 1);

            Thread.sleep(timeWaitToSync);

            int totalProducedMessages = 100;
            for (int i = 0; i < totalProducedMessages; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (1) consume by consumer1
            Message msg = null;
            Set<String> messageSet = Sets.newHashSet();
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

            System.out.println("done consuming");

            rolloverPerIntervalStats(replicationPulasr);
            stats = topicRef.getStats();
            subStats = stats.subscriptions.values().iterator().next();

            assertTrue(subStats.msgRateOut > 0);
            assertEquals(subStats.consumers.size(), 1);
            assertTrue(subStats.msgThroughputOut > 0);

            // consumer stats
            assertTrue(subStats.consumers.get(0).msgRateOut > 0.0);
            assertTrue(subStats.consumers.get(0).msgThroughputOut > 0.0);
            assertEquals(subStats.msgRateRedeliver, 0.0);

            producer.close();
            consumer1.close();
            repl2Consumer.close();
            repl3Consumer.close();
            client1.close();
            client2.close();
            client3.close();

        } finally {
            replication.shutdownReplicationCluster();
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

        public boolean isBrokerServicePurgeInactiveDestination() {
            return false;
        }

        void setupReplicationCluster() throws Exception {
            log.info("--- Starting ReplicatorTestBase::setup ---");
            int globalZKPort = PortManager.nextFreePort();
            globalZkS = new ZookeeperServerTest(globalZKPort);
            globalZkS.start();

            // Start region 1
            int zkPort1 = PortManager.nextFreePort();
            bkEnsemble1 = new LocalBookkeeperEnsemble(3, zkPort1, PortManager.nextFreePort());
            bkEnsemble1.start();

            int webServicePort1 = PortManager.nextFreePort();

            // NOTE: we have to instantiate a new copy of System.getProperties() to make sure pulsar1 and pulsar2 have
            // completely
            // independent config objects instead of referring to the same properties object
            ServiceConfiguration config1 = new ServiceConfiguration();
            config1.setClusterName("r1");
            config1.setWebServicePort(webServicePort1);
            config1.setZookeeperServers("127.0.0.1:" + zkPort1);
            config1.setGlobalZookeeperServers("127.0.0.1:" + globalZKPort + "/foo");
            config1.setBrokerDeleteInactiveTopicsEnabled(isBrokerServicePurgeInactiveDestination());
            config1.setBrokerServicePurgeInactiveFrequencyInSeconds(
                    inSec(getBrokerServicePurgeInactiveFrequency(), TimeUnit.SECONDS));
            config1.setBrokerServicePort(PortManager.nextFreePort());
            config1.setBacklogQuotaCheckIntervalInSeconds(TIME_TO_CHECK_BACKLOG_QUOTA);
            pulsar1 = new PulsarService(config1);
            pulsar1.start();
            ns1 = pulsar1.getBrokerService();

            url1 = new URL("http://127.0.0.1:" + webServicePort1);
            admin1 = new PulsarAdmin(url1, (Authentication) null);

            // Start region 2

            // Start zk & bks
            int zkPort2 = PortManager.nextFreePort();
            bkEnsemble2 = new LocalBookkeeperEnsemble(3, zkPort2, PortManager.nextFreePort());
            bkEnsemble2.start();

            int webServicePort2 = PortManager.nextFreePort();
            config2 = new ServiceConfiguration();
            config2.setClusterName("r2");
            config2.setWebServicePort(webServicePort2);
            config2.setZookeeperServers("127.0.0.1:" + zkPort2);
            config2.setGlobalZookeeperServers("127.0.0.1:" + globalZKPort + "/foo");
            config2.setBrokerDeleteInactiveTopicsEnabled(isBrokerServicePurgeInactiveDestination());
            config2.setBrokerServicePurgeInactiveFrequencyInSeconds(
                    inSec(getBrokerServicePurgeInactiveFrequency(), TimeUnit.SECONDS));
            config2.setBrokerServicePort(PortManager.nextFreePort());
            config2.setBacklogQuotaCheckIntervalInSeconds(TIME_TO_CHECK_BACKLOG_QUOTA);
            pulsar2 = new PulsarService(config2);
            pulsar2.start();
            ns2 = pulsar2.getBrokerService();

            url2 = new URL("http://127.0.0.1:" + webServicePort2);
            admin2 = new PulsarAdmin(url2, (Authentication) null);

            // Start region 3

            // Start zk & bks
            int zkPort3 = PortManager.nextFreePort();
            bkEnsemble3 = new LocalBookkeeperEnsemble(3, zkPort3, PortManager.nextFreePort());
            bkEnsemble3.start();

            int webServicePort3 = PortManager.nextFreePort();
            config3 = new ServiceConfiguration();
            config3.setClusterName("r3");
            config3.setWebServicePort(webServicePort3);
            config3.setZookeeperServers("127.0.0.1:" + zkPort3);
            config3.setGlobalZookeeperServers("127.0.0.1:" + globalZKPort + "/foo");
            config3.setBrokerDeleteInactiveTopicsEnabled(isBrokerServicePurgeInactiveDestination());
            config3.setBrokerServicePurgeInactiveFrequencyInSeconds(
                    inSec(getBrokerServicePurgeInactiveFrequency(), TimeUnit.SECONDS));
            config3.setBrokerServicePort(PortManager.nextFreePort());
            pulsar3 = new PulsarService(config3);
            pulsar3.start();
            ns3 = pulsar3.getBrokerService();

            url3 = new URL("http://127.0.0.1:" + webServicePort3);
            admin3 = new PulsarAdmin(url3, (Authentication) null);

            // Provision the global namespace
            admin1.clusters().createCluster("r1", new ClusterData(url1.toString(), null, pulsar1.getBrokerServiceUrl(),
                    pulsar1.getBrokerServiceUrlTls()));
            admin1.clusters().createCluster("r2", new ClusterData(url2.toString(), null, pulsar2.getBrokerServiceUrl(),
                    pulsar1.getBrokerServiceUrlTls()));
            admin1.clusters().createCluster("r3", new ClusterData(url3.toString(), null, pulsar3.getBrokerServiceUrl(),
                    pulsar1.getBrokerServiceUrlTls()));

            admin1.clusters().createCluster("global", new ClusterData("http://global:8080"));
            admin1.properties().createProperty("pulsar", new PropertyAdmin(
                    Lists.newArrayList("appid1", "appid2", "appid3"), Sets.newHashSet("r1", "r2", "r3")));
            admin1.namespaces().createNamespace("pulsar/global/ns");
            admin1.namespaces().setNamespaceReplicationClusters("pulsar/global/ns",
                    Lists.newArrayList("r1", "r2", "r3"));

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
            executor.shutdown();

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