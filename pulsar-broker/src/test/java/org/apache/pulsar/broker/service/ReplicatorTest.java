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

import static org.apache.pulsar.broker.BrokerTestUtil.newUniqueName;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.scurrilous.circe.checksum.Crc32cIntChecksum;
import io.netty.buffer.ByteBuf;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.CursorAlreadyClosedException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.BrokerServiceException.NamingException;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.api.RawReader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.RetentionPolicy;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.ReplicatorStats;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.schema.Schemas;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Starts 3 brokers that are in 3 different clusters
 */
@Test(groups = "broker")
public class ReplicatorTest extends ReplicatorTestBase {

    protected String methodName;

    @BeforeMethod(alwaysRun = true)
    public void beforeMethod(Method m) throws Exception {
        methodName = m.getName();
        if (admin1 != null) {
            admin1.namespaces().removeBacklogQuota("pulsar/ns");
            admin1.namespaces().removeBacklogQuota("pulsar/ns1");
            admin1.namespaces().removeBacklogQuota("pulsar/global/ns");
        }
    }

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @DataProvider(name = "partitionedTopic")
    public Object[][] partitionedTopicProvider() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

    @Test(timeOut = 10000)
    public void activeBrokerParse() throws Exception {
        pulsar1.getConfiguration().setAuthorizationEnabled(true);
        //init clusterData

        ClusterData cluster2Data = ClusterData.builder().serviceUrl(pulsar2.getWebServiceAddress()).build();
        String cluster2 = "activeCLuster2";
        admin2.clusters().createCluster(cluster2, cluster2Data);
        Awaitility.await().until(()
                -> admin2.clusters().getCluster(cluster2) != null);

        List<String> list = admin1.brokers().getActiveBrokers(cluster2);
        assertEquals(list.get(0), pulsar2.getBrokerId());
        //restore configuration
        pulsar1.getConfiguration().setAuthorizationEnabled(false);
    }

    @Test
    public void testForcefullyTopicDeletion() throws Exception {
        log.info("--- Starting ReplicatorTest::testForcefullyTopicDeletion ---");

        final String namespace = BrokerTestUtil.newUniqueName("pulsar/removeClusterTest");
        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1"));

        final String topicName = "persistent://" + namespace + "/topic";

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        ProducerImpl<byte[]> producer1 = (ProducerImpl<byte[]>) client1.newProducer().topic(topicName)
                .enableBatching(false).messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        producer1.close();

        admin1.topics().delete(topicName, true);

        MockedPulsarServiceBaseTest
                .retryStrategically((test) -> !pulsar1.getBrokerService().getTopics().containsKey(topicName), 50, 150);

        Assert.assertFalse(pulsar1.getBrokerService().getTopics().containsKey(topicName));
    }

    @SuppressWarnings("unchecked")
    @Test(timeOut = 30000)
    public void testConcurrentReplicator() throws Exception {

        log.info("--- Starting ReplicatorTest::testConcurrentReplicator ---");

        final String namespace = BrokerTestUtil.newUniqueName("pulsar/concurrent");
        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));
        final TopicName topicName = TopicName
                .get(BrokerTestUtil.newUniqueName("persistent://" + namespace + "/topic"));

        @Cleanup
        PulsarClient client1 = PulsarClient.builder()
                .serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        Producer<byte[]> producer = client1.newProducer().topic(topicName.toString())
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        producer.close();

        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService()
                .getOrCreateTopic(topicName.toString()).get();

        PulsarClientImpl pulsarClient = spy((PulsarClientImpl) pulsar1.getBrokerService()
                .getReplicationClient("r3",
                        pulsar1.getBrokerService().pulsar().getPulsarResources().getClusterResources()
                        .getCluster("r3")));
        final Method startRepl = PersistentTopic.class.getDeclaredMethod("startReplicator", String.class);
        startRepl.setAccessible(true);

        Field replClientField = BrokerService.class.getDeclaredField("replicationClients");
        replClientField.setAccessible(true);
        ConcurrentOpenHashMap<String, PulsarClient> replicationClients =
                (ConcurrentOpenHashMap<String, PulsarClient>) replClientField
                .get(pulsar1.getBrokerService());
        replicationClients.put("r3", pulsarClient);

        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2", "r3"));
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 5; i++) {
            executor.submit(() -> {
                try {
                    startRepl.invoke(topic, "r3");
                } catch (Exception e) {
                    fail("setting replicator failed", e);
                }
            });
        }
        Thread.sleep(3000);
        // One time is to create a Replicator for user topics,
        // and the other time is to create a Replicator for Topic Policies
        Mockito.verify(pulsarClient, Mockito.times(2))
                .createProducerAsync(
                        Mockito.any(ProducerConfigurationData.class),
                        Mockito.any(Schema.class), eq(null));
    }

    @DataProvider(name = "namespace")
    public Object[][] namespaceNameProvider() {
        return new Object[][] { { "pulsar/ns" }, { "pulsar/global/ns" } };
    }

    @Test(dataProvider = "namespace")
    public void testReplication(String namespace) throws Exception {
        log.info("--- Starting ReplicatorTest::testReplication ---");

        // This test is to verify that the config change on global namespace is successfully applied in broker during
        // runtime.
        // Run a set of producer tasks to create the topics
        final TopicName dest = TopicName
                .get(BrokerTestUtil.newUniqueName("persistent://" + namespace + "/repltopic"));

        @Cleanup
        MessageProducer producer1 = new MessageProducer(url1, dest);
        log.info("--- Starting producer --- " + url1);

        @Cleanup
        MessageProducer producer2 = new MessageProducer(url2, dest);
        log.info("--- Starting producer --- " + url2);

        @Cleanup
        MessageProducer producer3 = new MessageProducer(url3, dest);
        log.info("--- Starting producer --- " + url3);

        @Cleanup
        MessageConsumer consumer1 = new MessageConsumer(url1, dest);
        log.info("--- Starting Consumer --- " + url1);

        @Cleanup
        MessageConsumer consumer2 = new MessageConsumer(url2, dest);
        log.info("--- Starting Consumer --- " + url2);

        @Cleanup
        MessageConsumer consumer3 = new MessageConsumer(url3, dest);
        log.info("--- Starting Consumer --- " + url3);

        // Produce from cluster1 and consume from the rest
        producer1.produce(2);

        consumer1.receive(2);

        consumer2.receive(2);

        consumer3.receive(2);

        // Produce from cluster2 and consume from the rest
        producer2.produce(2);

        consumer1.receive(2);

        consumer2.receive(2);

        consumer3.receive(2);

        // Produce from cluster3 and consume from the rest
        producer3.produce(2);

        consumer1.receive(2);

        consumer2.receive(2);

        consumer3.receive(2);

        // Produce from cluster1&2 and consume from cluster3
        producer1.produce(1);
        producer2.produce(1);

        consumer1.receive(1);

        consumer2.receive(1);

        consumer3.receive(1);

        consumer1.receive(1);

        consumer2.receive(1);

        consumer3.receive(1);
    }

    @Test(invocationCount = 5)
    public void testReplicationWithSchema() throws Exception {
        config1.setBrokerDeduplicationEnabled(true);
        config2.setBrokerDeduplicationEnabled(true);
        config3.setBrokerDeduplicationEnabled(true);
        PulsarClient client1 = pulsar1.getClient();
        PulsarClient client2 = pulsar2.getClient();
        PulsarClient client3 = pulsar3.getClient();
        final TopicName topic = TopicName
                .get(BrokerTestUtil.newUniqueName("persistent://pulsar/ns/testReplicationWithSchema"));

        final String subName = "my-sub";

        @Cleanup
        Producer<Schemas.PersonOne> producer1 = client1.newProducer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(topic.toString())
                .enableBatching(false)
                .create();

        @Cleanup
        Producer<Schemas.PersonThree> producer2 = client1.newProducer(Schema.AVRO(Schemas.PersonThree.class))
                .topic(topic.toString())
                .enableBatching(false)
                .create();

        admin1.topics().createSubscription(topic.toString(), subName, MessageId.earliest);
        admin2.topics().createSubscription(topic.toString(), subName, MessageId.earliest);
        admin3.topics().createSubscription(topic.toString(), subName, MessageId.earliest);

        final int totalMessages = 1000;

        for (int i = 0; i < totalMessages / 2; i++) {
            producer1.sendAsync(new Schemas.PersonOne(i));
        }

        for (int i = 500; i < totalMessages; i++) {
            producer2.sendAsync(new Schemas.PersonThree(i, "name-" + i));
        }

        Awaitility.await().untilAsserted(() -> {
            assertTrue(admin1.topics().getInternalStats(topic.toString()).schemaLedgers.size() > 0);
            assertTrue(admin2.topics().getInternalStats(topic.toString()).schemaLedgers.size() > 0);
            assertTrue(admin3.topics().getInternalStats(topic.toString()).schemaLedgers.size() > 0);
        });

        @Cleanup
        Consumer<GenericRecord> consumer1 = client1.newConsumer(Schema.AUTO_CONSUME())
                .topic(topic.toString())
                .subscriptionName(subName)
                .subscribe();

        @Cleanup
        Consumer<GenericRecord> consumer2 = client2.newConsumer(Schema.AUTO_CONSUME())
                .topic(topic.toString())
                .subscriptionName(subName)
                .subscribe();

        @Cleanup
        Consumer<GenericRecord> consumer3 = client3.newConsumer(Schema.AUTO_CONSUME())
                .topic(topic.toString())
                .subscriptionName(subName)
                .subscribe();

        int lastId = -1;
        for (int i = 0; i < totalMessages; i++) {
            Message<GenericRecord> msg1 = consumer1.receive();
            Message<GenericRecord> msg2 = consumer2.receive();
            Message<GenericRecord> msg3 = consumer3.receive();
            assertTrue(msg1 != null && msg2 != null && msg3 != null);
            GenericRecord record1 = msg1.getValue();
            GenericRecord record2 = msg2.getValue();
            GenericRecord record3 = msg3.getValue();
            int id1 = (int) record1.getField("id");
            int id2 = (int) record2.getField("id");
            int id3 = (int) record3.getField("id");
            log.info("Received ids, id1: {}, id2: {}, id3: {}, lastId: {}", id1, id2, id3, lastId);
            assertTrue(id1 == id2 && id2 == id3);
            assertTrue(id1 > lastId);
            lastId = id1;
            consumer1.acknowledge(msg1);
            consumer2.acknowledge(msg2);
            consumer3.acknowledge(msg3);
        }

        @Cleanup
        Producer<byte[]> producerBytes = client1.newProducer()
                .topic(topic.toString())
                .enableBatching(false)
                .create();

        byte[] data = "Bytes".getBytes();
        producerBytes.send(data);

        assertEquals(consumer1.receive().getValue().getNativeObject(), data);
        assertEquals(consumer2.receive().getValue().getNativeObject(), data);
        assertEquals(consumer3.receive().getValue().getNativeObject(), data);
    }

    @Test
    public void testCounterOfPendingMessagesCorrect() throws Exception {
        // Init replicator and send many messages.
        PulsarClient client1 = pulsar1.getClient();
        final TopicName topic = TopicName
                .get(BrokerTestUtil.newUniqueName("persistent://pulsar/ns1/testReplicationWithGetSchemaError"));
        final String subName = "my-sub";
        @Cleanup
        Consumer<GenericRecord> consumer = client1.newConsumer(Schema.AUTO_CONSUME())
                .topic(topic.toString())
                .subscriptionName(subName)
                .receiverQueueSize(10)
                .subscribe();
        @Cleanup
        Producer<Schemas.PersonOne> producer = client1.newProducer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(topic.toString())
                .enableBatching(false)
                .create();
        for (int i = 0; i < 20; i++) {
            producer.send(new Schemas.PersonOne(i));
        }

        // Verify "pendingMessages" still is correct even if error occurs.
        PersistentReplicator replicator = ensureReplicatorCreated(topic, pulsar1);
        waitReplicateFinish(topic, admin1);
        Awaitility.await().untilAsserted(() -> {
            assertEquals((int) WhiteboxImpl.getInternalState(replicator, "pendingMessages"), 0);
        });
    }

    @Test
    public void testReplicationWillNotStuckByIncompleteSchemaFuture() throws Exception {
        int originalReplicationProducerQueueSize = pulsar1.getConfiguration().getReplicationProducerQueueSize();
        pulsar1.getConfiguration().setReplicationProducerQueueSize(5);
        // Init replicator and send many messages.
        PulsarClient client1 = pulsar1.getClient();
        final TopicName topic = TopicName
                .get(BrokerTestUtil.newUniqueName("persistent://pulsar/ns1/testReplicationWithGetSchemaError"));
        admin1.namespaces().setSchemaCompatibilityStrategy("pulsar/ns1", SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
        final String subName = "sub";
        @Cleanup
        Consumer<GenericRecord> consumer = client1.newConsumer(Schema.AUTO_CONSUME())
                .topic(topic.toString())
                .subscriptionName(subName)
                .receiverQueueSize(10)
                .subscribe();
        Thread sendTask = new Thread(() -> {
            for (int i = 0; i < 50; i++) {
                try {
                    Schema schema = Schema.JSON(SchemaDefinition.builder().withJsonDef(String.format("""
                        {
                            "type": "record",
                            "name": "Test_Pojo",
                            "namespace": "org.apache.pulsar.schema.compatibility",
                            "fields": [{
                                "name": "prop_%s",
                                "type": ["null", "string"],
                                "default": null
                            }]
                        }
                        """, i)).build());
                    Producer producer = client1
                            .newProducer(schema)
                            .topic(topic.toString())
                            .create();
                    producer.send(String.valueOf(i).toString().getBytes());
                    pulsar1.getBrokerService().checkReplicationPolicies();
                    producer.close();
                    Thread.sleep(100);
                } catch (Exception e){
                }
            }
        });
        sendTask.start();

        // Verify the replicate task can finish.
        ensureReplicatorCreated(topic, pulsar1);
        sendTask.join();
        waitReplicateFinish(topic, admin1);

        // cleanup
        pulsar1.getConfiguration().setReplicationProducerQueueSize(originalReplicationProducerQueueSize);
    }

    private static void waitReplicateFinish(TopicName topicName, PulsarAdmin admin){
        Awaitility.await().untilAsserted(() -> {
            for (Map.Entry<String, ? extends ReplicatorStats> subStats :
                    admin.topics().getStats(topicName.toString(), true, false, false).getReplication().entrySet()){
                assertTrue(subStats.getValue().getReplicationBacklog() == 0, "replication task finished");
            }
        });
    }

    private static PersistentReplicator ensureReplicatorCreated(TopicName topicName, PulsarService pulsar) {
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName.toString(), false).join().get();
        Awaitility.await().until(() -> !persistentTopic.getReplicators().isEmpty());
        return (PersistentReplicator) persistentTopic.getReplicators().values().iterator().next();
    }

    @Test
    public void testReplicationOverrides() throws Exception {
        log.info("--- Starting ReplicatorTest::testReplicationOverrides ---");

        // This test is to verify that the config change on global namespace is successfully applied in broker during
        // runtime.
        // Run a set of producer tasks to create the topics
        for (int i = 0; i < 10; i++) {
            final TopicName dest = TopicName
                    .get(BrokerTestUtil.newUniqueName("persistent://pulsar/ns/repltopic"));

            @Cleanup
            MessageProducer producer1 = new MessageProducer(url1, dest);
            log.info("--- Starting producer --- " + url1);

            @Cleanup
            MessageProducer producer2 = new MessageProducer(url2, dest);
            log.info("--- Starting producer --- " + url2);

            @Cleanup
            MessageProducer producer3 = new MessageProducer(url3, dest);
            log.info("--- Starting producer --- " + url3);

            @Cleanup
            MessageConsumer consumer1 = new MessageConsumer(url1, dest);
            log.info("--- Starting Consumer --- " + url1);

            @Cleanup
            MessageConsumer consumer2 = new MessageConsumer(url2, dest);
            log.info("--- Starting Consumer --- " + url2);

            @Cleanup
            MessageConsumer consumer3 = new MessageConsumer(url3, dest);
            log.info("--- Starting Consumer --- " + url3);

            // Produce a message that isn't replicated
            producer1.produce(1, producer1.newMessage().disableReplication());

            consumer1.receive(1);
            assertTrue(consumer2.drained());
            assertTrue(consumer3.drained());

            // Produce a message not replicated to r2
            producer1.produce(1, producer1.newMessage().replicationClusters(List.of("r1", "r3")));
            consumer1.receive(1);
            assertTrue(consumer2.drained());
            consumer3.receive(1);

            // Produce a default replicated message
            producer1.produce(1);

            consumer1.receive(1);
            consumer2.receive(1);
            consumer3.receive(1);

            assertTrue(consumer1.drained());
            assertTrue(consumer2.drained());
            assertTrue(consumer3.drained());
        }
    }

    @Test
    public void testFailures() {

        log.info("--- Starting ReplicatorTest::testFailures ---");

        try {
            // 1. Create a consumer using the reserved consumer id prefix "pulsar.repl."

            final TopicName dest = TopicName
                    .get(BrokerTestUtil.newUniqueName("persistent://pulsar/ns/res-cons-id-"));

            // Create another consumer using replication prefix as sub id
            MessageConsumer consumer = new MessageConsumer(url2, dest, "pulsar.repl.");
            consumer.close();

        } catch (Exception e) {
            // SUCCESS
        }

    }

    @Test(timeOut = 30000)
    public void testReplicatePeekAndSkip() throws Exception {

        final TopicName dest = TopicName.get(
                BrokerTestUtil.newUniqueName("persistent://pulsar/ns/peekAndSeekTopic"));

        @Cleanup
        MessageProducer producer1 = new MessageProducer(url1, dest);

        @Cleanup
        MessageConsumer consumer1 = new MessageConsumer(url3, dest);

        // Produce from cluster1 and consume from the rest
        producer1.produce(2);
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopicReference(dest.toString()).get();
        PersistentReplicator replicator = (PersistentReplicator) topic.getReplicators()
                .get(topic.getReplicators().keys().get(0));
        replicator.skipMessages(2);
        CompletableFuture<Entry> result = replicator.peekNthMessage(1);
        Entry entry = result.get(50, TimeUnit.MILLISECONDS);
        assertNull(entry);
    }

    @Test(timeOut = 30000)
    public void testReplicatorClearBacklog() throws Exception {

        // This test is to verify that reset cursor fails on global topic
        SortedSet<String> testDests = new TreeSet<>();

        final TopicName dest = TopicName
                .get(BrokerTestUtil.newUniqueName("persistent://pulsar/ns/clearBacklogTopic"));
        testDests.add(dest.toString());

        @Cleanup
        MessageProducer producer1 = new MessageProducer(url1, dest);

        @Cleanup
        MessageConsumer consumer1 = new MessageConsumer(url3, dest);

        // Produce from cluster1 and consume from the rest
        producer1.produce(2);
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopicReference(dest.toString()).get();
        PersistentReplicator replicator = (PersistentReplicator) spy(
                topic.getReplicators().get(topic.getReplicators().keys().get(0)));
        replicator.readEntriesFailed(new ManagedLedgerException.InvalidCursorPositionException("failed"), null);
        replicator.clearBacklog().get();
        Thread.sleep(100);
        replicator.updateRates(); // for code-coverage
        replicator.expireMessages(1); // for code-coverage
        ReplicatorStats status = replicator.getStats();
        assertEquals(status.getReplicationBacklog(), 0);
    }

    @Test(timeOut = 30000)
    public void testResetCursorNotFail() throws Exception {

        log.info("--- Starting ReplicatorTest::testResetCursorNotFail ---");

        // This test is to verify that reset cursor fails on global topic
        final TopicName dest = TopicName
                .get(BrokerTestUtil.newUniqueName("persistent://pulsar/ns/resetrepltopic"));

        @Cleanup
        MessageProducer producer1 = new MessageProducer(url1, dest);
        log.info("--- Starting producer --- " + url1);

        @Cleanup
        MessageConsumer consumer1 = new MessageConsumer(url1, dest);
        log.info("--- Starting Consumer --- " + url1);

        // Produce from cluster1 and consume from the rest
        producer1.produce(2);

        consumer1.receive(2);

        admin1.topics().resetCursor(dest.toString(), "sub-id", System.currentTimeMillis());
    }

    @Test
    public void testReplicationForBatchMessages() throws Exception {
        log.info("--- Starting ReplicatorTest::testReplicationForBatchMessages ---");

        // Run a set of producer tasks to create the topics
        final TopicName dest = TopicName
                .get(BrokerTestUtil.newUniqueName("persistent://pulsar/ns/repltopicbatch"));

        @Cleanup
        MessageProducer producer1 = new MessageProducer(url1, dest, true);
        log.info("--- Starting producer --- " + url1);

        @Cleanup
        MessageProducer producer2 = new MessageProducer(url2, dest, true);
        log.info("--- Starting producer --- " + url2);

        @Cleanup
        MessageProducer producer3 = new MessageProducer(url3, dest, true);
        log.info("--- Starting producer --- " + url3);

        @Cleanup
        MessageConsumer consumer1 = new MessageConsumer(url1, dest);
        log.info("--- Starting Consumer --- " + url1);

        @Cleanup
        MessageConsumer consumer2 = new MessageConsumer(url2, dest);
        log.info("--- Starting Consumer --- " + url2);

        @Cleanup
        MessageConsumer consumer3 = new MessageConsumer(url3, dest);
        log.info("--- Starting Consumer --- " + url3);

        // Produce from cluster1 and consume from the rest
        producer1.produceBatch(10);

        consumer1.receive(10);

        consumer2.receive(10);

        consumer3.receive(10);

        // Produce from cluster2 and consume from the rest
        producer2.produceBatch(10);

        consumer1.receive(10);

        consumer2.receive(10);

        consumer3.receive(10);
    }

    /**
     * It verifies that: if it fails while removing replicator-cluster-cursor: it should not restart the replicator and
     * it should have cleaned up from the list
     *
     * @throws Exception
     */
    @Test(timeOut = 30000)
    public void testDeleteReplicatorFailure() throws Exception {
        log.info("--- Starting ReplicatorTest::testDeleteReplicatorFailure ---");
        final String topicName = BrokerTestUtil.newUniqueName("persistent://pulsar/ns/repltopicbatch");
        final TopicName dest = TopicName.get(topicName);

        @Cleanup
        MessageProducer producer1 = new MessageProducer(url1, dest);
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopicReference(topicName).get();
        final String replicatorClusterName = topic.getReplicators().keys().get(0);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) topic.getManagedLedger();
        CountDownLatch latch = new CountDownLatch(1);
        // delete cursor already : so next time if topic.removeReplicator will get exception but then it should
        // remove-replicator from the list even with failure
        ledger.asyncDeleteCursor("pulsar.repl." + replicatorClusterName, new DeleteCursorCallback() {
            @Override
            public void deleteCursorComplete(Object ctx) {
                latch.countDown();
            }

            @Override
            public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                latch.countDown();
            }
        }, null);
        latch.await();

        Method removeReplicator = PersistentTopic.class.getDeclaredMethod("removeReplicator", String.class);
        removeReplicator.setAccessible(true);
        // invoke removeReplicator : it fails as cursor is not present: but still it should remove the replicator from
        // list without restarting it
        @SuppressWarnings("unchecked")
        CompletableFuture<Void> result = (CompletableFuture<Void>) removeReplicator.invoke(topic,
                replicatorClusterName);
        result.thenApply((v) -> {
            assertNull(topic.getPersistentReplicator(replicatorClusterName));
            return null;
        });
    }

    @SuppressWarnings("unchecked")
    @Test(priority = 5, timeOut = 30000)
    public void testReplicatorProducerClosing() throws Exception {
        log.info("--- Starting ReplicatorTest::testDeleteReplicatorFailure ---");
        final String topicName = BrokerTestUtil.newUniqueName("persistent://pulsar/ns/repltopicbatch");
        final TopicName dest = TopicName.get(topicName);

        @Cleanup
        MessageProducer producer1 = new MessageProducer(url1, dest);
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopicReference(topicName).get();
        final String replicatorClusterName = topic.getReplicators().keys().get(0);
        Replicator replicator = topic.getPersistentReplicator(replicatorClusterName);
        pulsar2.close();
        pulsar2 = null;
        pulsar3.close();
        pulsar3 = null;
        replicator.terminate();
        Thread.sleep(100);
        Field field = AbstractReplicator.class.getDeclaredField("producer");
        field.setAccessible(true);
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) field.get(replicator);
        assertNull(producer);
    }

    @Test(priority = 5, timeOut = 30000)
    public void testReplicatorConnected() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://pulsar/ns/tp_" + UUID.randomUUID());
        final TopicName dest = TopicName.get(topicName);
        admin1.topics().createPartitionedTopic(topicName, 1);

        @Cleanup
        MessageProducer producer1 = new MessageProducer(url1, dest);

        Awaitility.await().until(() -> {
            TopicStats topicStats = admin1.topics().getStats(topicName + "-partition-0");
            return topicStats.getReplication().values().stream()
                    .map(ReplicatorStats::isConnected).reduce((a, b) -> a & b).get();
        });

        PartitionedTopicStats
                partitionedTopicStats = admin1.topics().getPartitionedStats(topicName, true);

        for (ReplicatorStats replicatorStats : partitionedTopicStats.getReplication().values()){
            assertTrue(replicatorStats.isConnected());
        }
    }

    @Test
    public void testDeleteTopicFailure() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://pulsar/ns/tp_" + UUID.randomUUID());
        admin1.topics().createNonPartitionedTopic(topicName);
        try {
            admin1.topics().delete(topicName);
            fail("Delete topic should fail if enabled replicator");
        } catch (Exception ex) {
            assertTrue(ex instanceof PulsarAdminException);
            assertEquals(((PulsarAdminException) ex).getStatusCode(), 422/* Unprocessable entity*/);
        }
    }

    @Test
    public void testDeletePartitionedTopicFailure() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://pulsar/ns/tp_" + UUID.randomUUID());
        admin1.topics().createPartitionedTopic(topicName, 2);
        admin1.topics().createSubscription(topicName, "sub1", MessageId.earliest);
        try {
            admin1.topics().deletePartitionedTopic(topicName);
            fail("Delete topic should fail if enabled replicator");
        } catch (Exception ex) {
            assertTrue(ex instanceof PulsarAdminException);
            assertEquals(((PulsarAdminException) ex).getStatusCode(), 422/* Unprocessable entity*/);
        }
    }

    @Test(priority = 4, timeOut = 30000)
    public void testReplicatorProducerName() throws Exception {
        log.info("--- Starting ReplicatorTest::testReplicatorProducerName ---");
        final String topicName = BrokerTestUtil.newUniqueName("persistent://pulsar/ns/testReplicatorProducerName");
        final TopicName dest = TopicName.get(topicName);

        @Cleanup
        MessageProducer producer1 = new MessageProducer(url1, dest);

        Awaitility.await().untilAsserted(() -> {
            assertTrue(pulsar2.getBrokerService().getTopicReference(topicName).isPresent());
        });
        Optional<Topic> topic = pulsar2.getBrokerService().getTopicReference(topicName);
        assertTrue(topic.isPresent());
        Awaitility.await().untilAsserted(() -> {
            Set<String> remoteClusters = topic.get().getProducers().values().stream()
                    .map(org.apache.pulsar.broker.service.Producer::getRemoteCluster)
                    .collect(Collectors.toSet());
            assertTrue(remoteClusters.contains("r1"));
        });
    }

    @Test(priority = 4, timeOut = 30000)
    public void testReplicatorProducerNameWithUserDefinedReplicatorPrefix() throws Exception {
        log.info("--- Starting ReplicatorTest::testReplicatorProducerNameWithUserDefinedReplicatorPrefix ---");
        final String topicName = BrokerTestUtil.newUniqueName(
                "persistent://pulsar/ns/testReplicatorProducerNameWithUserDefinedReplicatorPrefix");
        final TopicName dest = TopicName.get(topicName);

        pulsar1.getConfiguration().setReplicatorPrefix("user-defined-prefix");
        pulsar2.getConfiguration().setReplicatorPrefix("user-defined-prefix");
        pulsar3.getConfiguration().setReplicatorPrefix("user-defined-prefix");

        @Cleanup
        MessageProducer producer1 = new MessageProducer(url1, dest);

        Awaitility.await().untilAsserted(()->{
            assertTrue(pulsar2.getBrokerService().getTopicReference(topicName).isPresent());
        });
        Optional<Topic> topic = pulsar2.getBrokerService().getTopicReference(topicName);
        assertTrue(topic.isPresent());
        Set<String> remoteClusters = topic.get().getProducers().values().stream()
                .map(org.apache.pulsar.broker.service.Producer::getRemoteCluster)
                .collect(Collectors.toSet());
        assertTrue(remoteClusters.contains("r1"));
    }


    /**
     * Issue #199
     *
     * It verifies that: if the remote cluster reaches backlog quota limit, replicator temporarily stops and once the
     * backlog drains it should resume replication.
     *
     * @throws Exception
     */

    @Test(timeOut = 60000, priority = -1)
    public void testResumptionAfterBacklogRelaxed() throws Exception {
        List<RetentionPolicy> policies = new ArrayList<>();
        policies.add(RetentionPolicy.producer_exception);
        policies.add(RetentionPolicy.producer_request_hold);

        for (RetentionPolicy policy : policies) {
            // Use 1Mb quota by default
            admin1.namespaces().setBacklogQuota("pulsar/ns1", BacklogQuota.builder()
                    .limitSize(1 * 1024 * 1024)
                    .retentionPolicy(policy)
                    .build());
            Thread.sleep(200);

            TopicName dest = TopicName
                    .get(BrokerTestUtil.newUniqueName("persistent://pulsar/ns1/%s-" + policy));

            // Producer on r1
            @Cleanup
            MessageProducer producer1 = new MessageProducer(url1, dest);

            // Consumer on r2
            @Cleanup
            MessageConsumer consumer2 = new MessageConsumer(url2, dest);

            // Replicator for r1 -> r2
            PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopicReference(dest.toString()).get();
            Replicator replicator = topic.getPersistentReplicator("r2");

            // Produce 1 message in r1. This message will be replicated immediately into r2 and it will become part of
            // local backlog
            producer1.produce(1);

            Thread.sleep(500);

            // Restrict backlog quota limit to 1 byte to stop replication
            admin1.namespaces().setBacklogQuota("pulsar/ns1", BacklogQuota.builder()
                    .limitSize(1)
                    .retentionPolicy(policy)
                    .build());

            Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);

            assertEquals(replicator.getStats().replicationBacklog, 0);

            // Next message will not be replicated, because r2 has reached the quota
            producer1.produce(1);

            Thread.sleep(500);

            assertEquals(replicator.getStats().replicationBacklog, 1);

            // Consumer will now drain 1 message and the replication backlog will be cleared
            consumer2.receive(1);

            // Wait until the 2nd message got delivered to consumer
            consumer2.receive(1);

            int retry = 10;
            for (int i = 0; i < retry && replicator.getStats().replicationBacklog > 0; i++) {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }

            assertEquals(replicator.getStats().replicationBacklog, 0);
        }
    }

    /**
     * It verifies that PersistentReplicator considers CursorAlreadyClosedException as non-retriable-read exception and
     * it should closed the producer as cursor is already closed because replicator is already deleted.
     *
     * @throws Exception
     */
    @Test(timeOut = 15000)
    public void testCloseReplicatorStartProducer() throws Exception {
        TopicName dest = TopicName.get(BrokerTestUtil.newUniqueName("persistent://pulsar/ns1/closeCursor"));
        // Producer on r1
        @Cleanup
        MessageProducer producer1 = new MessageProducer(url1, dest);
        // Consumer on r1
        @Cleanup
        MessageConsumer consumer1 = new MessageConsumer(url1, dest);
        // Consumer on r2
        @Cleanup
        MessageConsumer consumer2 = new MessageConsumer(url2, dest);

        // Replicator for r1 -> r2
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopicReference(dest.toString()).get();
        PersistentReplicator replicator = (PersistentReplicator) topic.getPersistentReplicator("r2");

        // close the cursor
        Field cursorField = PersistentReplicator.class.getDeclaredField("cursor");
        cursorField.setAccessible(true);
        ManagedCursor cursor = (ManagedCursor) cursorField.get(replicator);
        cursor.close();
        // try to read entries
        producer1.produce(10);

        try {
            cursor.readEntriesOrWait(10);
            fail("It should have failed");
        } catch (Exception e) {
            assertEquals(e.getClass(), CursorAlreadyClosedException.class);
        }

        // replicator-readException: cursorAlreadyClosed
        replicator.readEntriesFailed(new CursorAlreadyClosedException("Cursor already closed exception"), null);

        // wait replicator producer to be closed
        Thread.sleep(100);

        // Replicator producer must be closed
        Field producerField = AbstractReplicator.class.getDeclaredField("producer");
        producerField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ProducerImpl<byte[]> replicatorProducer = (ProducerImpl<byte[]>) producerField.get(replicator);
        assertNull(replicatorProducer);
    }

    @Test(timeOut = 30000)
    public void verifyChecksumAfterReplication() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://pulsar/ns/checksumAfterReplication");

        @Cleanup
        PulsarClient c1 = PulsarClient.builder().serviceUrl(url1.toString()).build();
        Producer<byte[]> p1 = c1.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        @Cleanup
        PulsarClient c2 = PulsarClient.builder().serviceUrl(url2.toString()).build();
        RawReader reader2 = RawReader.create(c2, topicName, "sub").get();

        p1.send("Hello".getBytes());

        RawMessage msg = reader2.readNextAsync().get();

        ByteBuf b = msg.getHeadersAndPayload();

        assertTrue(Commands.hasChecksum(b));
        int parsedChecksum = Commands.readChecksum(b);
        int computedChecksum = Crc32cIntChecksum.computeChecksum(b);

        assertEquals(parsedChecksum, computedChecksum);

        p1.close();
        reader2.closeAsync().get();
    }

    @Test
    public void testReplicatorWithPartitionedTopic() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String persistentTopicName = "persistent://" + namespace + "/partTopic" + UUID.randomUUID();

        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2", "r3"));
        // Create partitioned-topic from R1
        admin1.topics().createPartitionedTopic(persistentTopicName, 3);
        // List partitioned topics from R2
        Awaitility.await().untilAsserted(() -> assertNotNull(admin2.topics().getPartitionedTopicList(namespace)));
        Awaitility.await().untilAsserted(() -> assertEquals(
                admin2.topics().getPartitionedTopicList(namespace).get(0), persistentTopicName));
        assertEquals(admin1.topics().getList(namespace).size(), 3);
        // List partitioned topics from R3
        Awaitility.await().untilAsserted(() -> assertNotNull(admin3.topics().getPartitionedTopicList(namespace)));
        Awaitility.await().untilAsserted(() -> assertEquals(
                admin3.topics().getPartitionedTopicList(namespace).get(0), persistentTopicName));
        // Update partitioned topic from R2
        admin2.topics().updatePartitionedTopic(persistentTopicName, 5);
        assertEquals(admin2.topics().getPartitionedTopicMetadata(persistentTopicName).partitions, 5);
        assertEquals((int) admin2.topics().getList(namespace).stream().filter(topic ->
                !topic.contains(SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME)).count(), 5);
        // Update partitioned topic from R3
        admin3.topics().updatePartitionedTopic(persistentTopicName, 6);
        assertEquals(admin3.topics().getPartitionedTopicMetadata(persistentTopicName).partitions, 6);
        assertEquals(admin3.topics().getList(namespace).stream().filter(topic ->
                !topic.contains(SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME)).count(), 6);
        // Update partitioned topic from R1
        admin1.topics().updatePartitionedTopic(persistentTopicName, 7);
        assertEquals(admin1.topics().getPartitionedTopicMetadata(persistentTopicName).partitions, 7);
        assertEquals(admin2.topics().getPartitionedTopicMetadata(persistentTopicName).partitions, 7);
        assertEquals(admin3.topics().getPartitionedTopicMetadata(persistentTopicName).partitions, 7);
        assertEquals(admin1.topics().getList(namespace).stream().filter(topic ->
                !topic.contains(SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME)).count(), 7);
        assertEquals(admin2.topics().getList(namespace).stream().filter(topic ->
                !topic.contains(SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME)).count(), 7);
        assertEquals(admin3.topics().getList(namespace).stream().filter(topic ->
                !topic.contains(SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME)).count(), 7);
    }

    /**
     * It verifies that broker should not start replicator for partitioned-topic (topic without -partition postfix)
     *
     * @param isPartitionedTopic
     * @throws Exception
     */
    @Test(dataProvider = "partitionedTopic")
    public void testReplicatorOnPartitionedTopic(boolean isPartitionedTopic) throws Exception {

        log.info("--- Starting ReplicatorTest::{} --- ", methodName);

        final String namespace = BrokerTestUtil.newUniqueName("pulsar/partitionedNs-" + isPartitionedTopic);
        final String persistentTopicName = BrokerTestUtil.newUniqueName("persistent://" + namespace + "/partTopic-" + isPartitionedTopic);
        final String nonPersistentTopicName = BrokerTestUtil.newUniqueName("non-persistent://" + namespace + "/partTopic-" + isPartitionedTopic);
        BrokerService brokerService = pulsar1.getBrokerService();

        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2", "r3"));

        if (isPartitionedTopic) {
            admin1.topics().createPartitionedTopic(persistentTopicName, 5);
            admin1.topics().createPartitionedTopic(nonPersistentTopicName, 5);
        }

        // load namespace with dummy topic on ns
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(url1.toString()).build();
        client.newProducer().topic("persistent://" + namespace + "/dummyTopic")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // persistent topic test
        try {
            brokerService.getOrCreateTopic(persistentTopicName).get();
            if (isPartitionedTopic) {
                fail("Topic creation fails with partitioned topic as replicator init fails");
            }
        } catch (Exception e) {
            if (!isPartitionedTopic) {
                fail("Topic creation should not fail without any partitioned topic");
            }
            assertTrue(e.getCause() instanceof NamingException);
        }

        // non-persistent topic test
        try {
            brokerService.getOrCreateTopic(nonPersistentTopicName).get();
            if (isPartitionedTopic) {
                fail("Topic creation fails with partitioned topic as replicator init fails");
            }
        } catch (Exception e) {
            if (!isPartitionedTopic) {
                fail("Topic creation should not fail without any partitioned topic");
            }
            assertTrue(e.getCause() instanceof NamingException);
        }

    }

    @Test
    public void testReplicatedCluster() throws Exception {

        log.info("--- Starting ReplicatorTest::testReplicatedCluster ---");

        final String namespace = BrokerTestUtil.newUniqueName("pulsar/global/repl");
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + namespace + "/topic1");
        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2", "r3"));
        admin1.topics().createPartitionedTopic(topicName, 4);

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        @Cleanup
        PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        Producer<byte[]> producer1 = client1.newProducer().topic(topicName).create();
        org.apache.pulsar.client.api.Consumer<byte[]> consumer1 = client1.newConsumer().topic(topicName).subscriptionName("s1").subscribe();
        org.apache.pulsar.client.api.Consumer<byte[]> consumer2 = client2.newConsumer().topic(topicName).subscriptionName("s1").subscribe();
        byte[] value = "test".getBytes();

        // publish message local only
        TypedMessageBuilder<byte[]> msg = producer1.newMessage().replicationClusters(List.of("r1")).value(value);
        msg.send();
        assertEquals(consumer1.receive().getValue(), value);

        Message<byte[]> msg2 = consumer2.receive(1, TimeUnit.SECONDS);
        if (msg2 != null) {
            fail("msg should have not been replicated to remote cluster");
        }

        consumer1.close();
        consumer2.close();
        producer1.close();

    }

    /**
     * This validates that broker supports update-partition api for global topics.
     * <pre>
     *  1. Create global topic with 4 partitions
     *  2. Update partition with 8 partitions
     *  3. Create producer on the partition topic which loads all new partitions
     *  4. Check subscriptions are created on all new partitions.
     * </pre>
     * @throws Exception
     */
    @Test
    public void testUpdateGlobalTopicPartition() throws Exception {
        log.info("--- Starting ReplicatorTest::testUpdateGlobalTopicPartition ---");

        final String cluster1 = pulsar1.getConfig().getClusterName();
        final String cluster2 = pulsar2.getConfig().getClusterName();
        final String namespace = BrokerTestUtil.newUniqueName("pulsar/ns");
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + namespace + "/topic1");
        int startPartitions = 4;
        int newPartitions = 8;
        final String subscriberName = "sub1";
        admin1.namespaces().createNamespace(namespace, Sets.newHashSet(cluster1, cluster2));
        admin1.topics().createPartitionedTopic(topicName, startPartitions);

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        @Cleanup
        PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        Consumer<byte[]> consumer1 = client1.newConsumer().topic(topicName).subscriptionName(subscriberName)
                .subscribe();
        Consumer<byte[]> consumer2 = client2.newConsumer().topic(topicName).subscriptionName(subscriberName)
                .subscribe();

        admin1.topics().updatePartitionedTopic(topicName, newPartitions);

        assertEquals(admin1.topics().getPartitionedTopicMetadata(topicName).partitions, newPartitions);

        // create producers to load all the partition topics
        Producer<byte[]> producer1 = client1.newProducer().topic(topicName).create();
        Producer<byte[]> producer2 = client2.newProducer().topic(topicName).create();

        for (int i = startPartitions; i < newPartitions; i++) {
            String partitionedTopic = topicName + TopicName.PARTITIONED_TOPIC_SUFFIX + i;
            assertEquals(admin1.topics().getSubscriptions(partitionedTopic).size(), 1);
            assertEquals(admin2.topics().getSubscriptions(partitionedTopic).size(), 1);
        }

        producer1.close();
        producer2.close();
        consumer1.close();
        consumer2.close();
    }

    @Test
    public void testIncrementPartitionsOfTopicWithReplicatedSubscription() throws Exception {
        final String cluster1 = pulsar1.getConfig().getClusterName();
        final String cluster2 = pulsar2.getConfig().getClusterName();
        final String namespace = BrokerTestUtil.newUniqueName("pulsar/ns");
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + namespace + "/topic1");
        int startPartitions = 4;
        int newPartitions = 8;
        final String subscriberName = "sub1";
        admin1.namespaces().createNamespace(namespace, Sets.newHashSet(cluster1, cluster2));
        admin1.topics().createPartitionedTopic(topicName, startPartitions);

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        Consumer<byte[]> consumer1 = client1.newConsumer().topic(topicName).subscriptionName(subscriberName)
                .replicateSubscriptionState(true)
                .subscribe();

        admin1.topics().updatePartitionedTopic(topicName, newPartitions);

        assertEquals(admin1.topics().getPartitionedTopicMetadata(topicName).partitions, newPartitions);

        Map<String, Boolean> replicatedSubscriptionStatus =
                admin1.topics().getReplicatedSubscriptionStatus(topicName, subscriberName);
        assertEquals(replicatedSubscriptionStatus.size(), newPartitions);
        for (Map.Entry<String, Boolean> replicatedStatusForPartition : replicatedSubscriptionStatus.entrySet()) {
            assertTrue(replicatedStatusForPartition.getValue(),
                    "Replicated status is invalid for " + replicatedStatusForPartition.getKey());
        }
        consumer1.close();
    }

    @DataProvider(name = "topicPrefix")
    public static Object[][] topicPrefix() {
        return new Object[][] { { "persistent://", "/persistent" }, { "non-persistent://", "/non-persistent" } };
    }

    @Test(dataProvider = "topicPrefix")
    public void testTopicReplicatedAndProducerCreate(String topicPrefix, String topicName) throws Exception {
        log.info("--- Starting ReplicatorTest::testTopicReplicatedAndProducerCreate ---");

        final String cluster1 = pulsar1.getConfig().getClusterName();
        final String cluster2 = pulsar2.getConfig().getClusterName();
        final String namespace = BrokerTestUtil.newUniqueName("pulsar/ns");
        final String partitionedTopicName = BrokerTestUtil.newUniqueName(topicPrefix + namespace + topicName + "-partitioned");
        final String nonPartitionedTopicName = BrokerTestUtil.newUniqueName(topicPrefix + namespace + topicName + "-non-partitioned");
        final int startPartitions = 4;
        admin1.namespaces().createNamespace(namespace, Sets.newHashSet(cluster1, cluster2));
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2", "r3"));
        admin1.topics().createPartitionedTopic(partitionedTopicName, startPartitions);
        admin1.topics().createNonPartitionedTopic(nonPartitionedTopicName);

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        @Cleanup
        PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        //persistent topic
        Producer<byte[]> persistentProducer1 = client1.newProducer().topic(partitionedTopicName).create();
        Producer<byte[]> persistentProducer2 = client2.newProducer().topic(partitionedTopicName).create();
        assertNotNull(persistentProducer1.send("test".getBytes()));
        assertNotNull(persistentProducer2.send("test".getBytes()));
        //non-persistent topic
        Producer<byte[]> nonPersistentProducer1 = client1.newProducer().topic(nonPartitionedTopicName).create();
        Producer<byte[]> nonPersistentProducer2 = client2.newProducer().topic(nonPartitionedTopicName).create();

        assertNotNull(nonPersistentProducer1.send("test".getBytes()));
        assertNotNull(nonPersistentProducer2.send("test".getBytes()));

        persistentProducer1.close();
        persistentProducer2.close();

        nonPersistentProducer1.close();
        nonPersistentProducer2.close();
    }

    @Test
    public void testCleanupTopic() throws Exception {

        final String cluster1 = pulsar1.getConfig().getClusterName();
        final String cluster2 = pulsar2.getConfig().getClusterName();
        final String namespace = "pulsar/ns-" + System.nanoTime();
        final String topicName = "persistent://" + namespace + "/cleanTopic";
        final String topicMlName = namespace + "/persistent/cleanTopic";
        admin1.namespaces().createNamespace(namespace, Sets.newHashSet(cluster1, cluster2));

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        long topicLoadTimeoutSeconds = 3;
        config1.setTopicLoadTimeoutSeconds(topicLoadTimeoutSeconds);
        config2.setTopicLoadTimeoutSeconds(topicLoadTimeoutSeconds);

        ManagedLedgerFactoryImpl mlFactory = (ManagedLedgerFactoryImpl) pulsar1.getManagedLedgerClientFactory()
                .getManagedLedgerFactory();
        Field ledgersField = ManagedLedgerFactoryImpl.class.getDeclaredField("ledgers");
        ledgersField.setAccessible(true);
        ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>> ledgers = (ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>>) ledgersField
                .get(mlFactory);
        CompletableFuture<ManagedLedgerImpl> mlFuture = new CompletableFuture<>();
        ledgers.put(topicMlName, mlFuture);

        try {
            Consumer<byte[]> consumer = client1.newConsumer().topic(topicName).subscriptionType(SubscriptionType.Shared)
                    .subscriptionName("my-subscriber-name").subscribeAsync().get(100, TimeUnit.MILLISECONDS);
            fail("consumer should fail due to topic loading failure");
        } catch (Exception e) {
            // Ok
        }

        CompletableFuture<Optional<Topic>> topicFuture = null;
        for (int i = 0; i < 5; i++) {
            topicFuture = pulsar1.getBrokerService().getTopics().get(topicName);
            if (topicFuture != null) {
                break;
            }
            Thread.sleep(i * 1000);
        }

        try {
            topicFuture.get();
            fail("topic creation should fail");
        } catch (Exception e) {
            // Ok
        }

        try {
            Consumer<byte[]> consumer = client1.newConsumer().topic(topicName).subscriptionType(SubscriptionType.Shared)
                    .subscriptionName("my-subscriber-name").subscribeAsync().get(100, TimeUnit.MILLISECONDS);
            fail("consumer should fail due to topic loading failure");
        } catch (Exception e) {
            // Ok
        }

        ManagedLedgerImpl ml = (ManagedLedgerImpl) mlFactory.open(topicMlName + "-2");
        mlFuture.complete(ml);

        // Re-create topic will success.
        Consumer<byte[]> consumer = client1.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Shared).subscribeAsync()
                .get(2 * topicLoadTimeoutSeconds, TimeUnit.SECONDS);

        consumer.close();
    }


    @Test
    public void createPartitionedTopicTest() throws Exception {
        final String cluster1 = pulsar1.getConfig().getClusterName();
        final String cluster2 = pulsar2.getConfig().getClusterName();
        final String cluster3 = pulsar3.getConfig().getClusterName();
        final String namespace = newUniqueName("pulsar/ns");

        final String persistentPartitionedTopic =
                newUniqueName("persistent://" + namespace + "/partitioned");
        final String persistentNonPartitionedTopic =
                newUniqueName("persistent://" + namespace + "/non-partitioned");
        final String nonPersistentPartitionedTopic =
                newUniqueName("non-persistent://" + namespace + "/partitioned");
        final String nonPersistentNonPartitionedTopic =
                newUniqueName("non-persistent://" + namespace + "/non-partitioned");
        final int numPartitions = 3;

        admin1.namespaces().createNamespace(namespace, Sets.newHashSet(cluster1, cluster2, cluster3));
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2", "r3"));

        admin1.topics().createPartitionedTopic(persistentPartitionedTopic, numPartitions);
        admin1.topics().createPartitionedTopic(nonPersistentPartitionedTopic, numPartitions);
        admin1.topics().createNonPartitionedTopic(persistentNonPartitionedTopic);
        admin1.topics().createNonPartitionedTopic(nonPersistentNonPartitionedTopic);

        List<String> partitionedTopicList = admin1.topics().getPartitionedTopicList(namespace);
        Assert.assertTrue(partitionedTopicList.contains(persistentPartitionedTopic));
        Assert.assertTrue(partitionedTopicList.contains(nonPersistentPartitionedTopic));

        // expected topic list didn't contain non-persistent-non-partitioned topic,
        // because this model topic didn't create path in local metadata store.
        List<String> expectedTopicList = Lists.newArrayList(
                persistentNonPartitionedTopic, nonPersistentNonPartitionedTopic);
        TopicName pt = TopicName.get(persistentPartitionedTopic);
        for (int i = 0; i < numPartitions; i++) {
            expectedTopicList.add(pt.getPartition(i).toString());
        }

        checkListContainExpectedTopic(admin1, namespace, expectedTopicList);
        checkListContainExpectedTopic(admin2, namespace, expectedTopicList);
        checkListContainExpectedTopic(admin3, namespace, expectedTopicList);
    }

    @Test
    public void testDoNotReplicateSystemTopic() throws Exception {
        cleanup();
        config1.setTransactionCoordinatorEnabled(true);
        config2.setTransactionCoordinatorEnabled(true);
        config3.setTransactionCoordinatorEnabled(true);
        setup();
        final String namespace = newUniqueName("pulsar/ns");
        admin1.namespaces().createNamespace(namespace, Sets.newHashSet("r1", "r2", "r3"));
        String topic = TopicName.get("persistent", NamespaceName.get(namespace),
                "testDoesNotReplicateSystemTopic").toString();
        admin1.topics().createNonPartitionedTopic(topic);
        String systemTopic = TopicName.get("persistent", NamespaceName.get(namespace),
                SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT).toString();

        @Cleanup
        PulsarClient client2 = PulsarClient.builder().serviceUrl(pulsar2.getBrokerServiceUrl()).build();

        @Cleanup
        Consumer<byte[]> consumerFromR2 = client2.newConsumer()
                .topic(topic.toString())
                .subscriptionName("sub-rep")
                .subscribe();

        // Replicator will not replicate System Topic other than topic policies
        initTransaction(2, admin1, pulsar1.getBrokerServiceUrl(), pulsar1);
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar1.getBrokerServiceUrl())
                .enableTransaction(true).build();
        TransactionImpl abortTransaction = (TransactionImpl) client.newTransaction()
                .withTransactionTimeout(60, TimeUnit.SECONDS).build().get();

        @Cleanup
        Producer<byte[]> producer = client.newProducer().topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false).create();
        producer.newMessage(abortTransaction).value("aborted".getBytes(StandardCharsets.UTF_8)).send();
        abortTransaction.abort().get();
        assertNull(consumerFromR2.receive(5, TimeUnit.SECONDS));

        TransactionImpl transaction = (TransactionImpl) client.newTransaction()
                .withTransactionTimeout(60, TimeUnit.SECONDS).build().get();

        producer.newMessage(transaction).value("1".getBytes(StandardCharsets.UTF_8)).send();
        assertNull(consumerFromR2.receive(5, TimeUnit.SECONDS));
        transaction.commit();

        Assert.assertEquals(consumerFromR2.receive(5, TimeUnit.SECONDS).getValue(),
                "1".getBytes(StandardCharsets.UTF_8));

        // wait extra 500ms before evaluating stats for the system topics
        Thread.sleep(500L);

        Assert.assertEquals(admin1.topics().getStats(systemTopic).getReplication().size(), 0);
        Assert.assertEquals(admin2.topics().getStats(systemTopic).getReplication().size(), 0);
        Assert.assertEquals(admin3.topics().getStats(systemTopic).getReplication().size(), 0);

        cleanup();
        setup();
    }

    private void initTransaction(int coordinatorSize, PulsarAdmin admin, String ServiceUrl,
                                 PulsarService pulsarService) throws Exception {
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString(), coordinatorSize);
        pulsarService.getPulsarResources()
                .getNamespaceResources()
                .getPartitionedTopicResources()
                .createPartitionedTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN,
                        new PartitionedTopicMetadata(coordinatorSize));
        admin.lookups().lookupTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN.toString());
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(ServiceUrl).enableTransaction(true).build();
        pulsarClient.close();
        Awaitility.await().until(() ->
                pulsarService.getTransactionMetadataStoreService().getStores().size() == coordinatorSize);
    }

    @Test
    public void testLookupAnotherCluster() throws Exception {
        log.info("--- Starting ReplicatorTest::testLookupAnotherCluster ---");

        String namespace = "pulsar/r2/cross-cluster-ns";
        admin1.namespaces().createNamespace(namespace);
        final TopicName topicName = TopicName
                .get("persistent://" + namespace + "/topic");

        @Cleanup
        PulsarClient client1 = PulsarClient.builder()
                .serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        Producer<byte[]> producer = client1.newProducer().topic(topicName.toString())
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        producer.close();
    }

    private void checkListContainExpectedTopic(PulsarAdmin admin, String namespace, List<String> expectedTopicList) {
        // wait non-partitioned topics replicators created finished
        final List<String> list = new ArrayList<>();
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> {
            list.clear();
            list.addAll(admin.topics().getList(namespace).stream()
                    .filter(topic -> !topic.contains(SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME))
                    .collect(Collectors.toList()));
            return list.size() == expectedTopicList.size();
        });
        for (String expectTopic : expectedTopicList) {
            Assert.assertTrue(list.contains(expectTopic));
        }
    }

    @Test
    public void testReplicatorWithFailedAck() throws Exception {

        log.info("--- Starting ReplicatorTest::testReplication ---");

        String namespace = BrokerTestUtil.newUniqueName("pulsar/global/ns");
        admin1.namespaces().createNamespace(namespace, Sets.newHashSet("r1"));
        final TopicName dest = TopicName
                .get(BrokerTestUtil.newUniqueName("persistent://" + namespace + "/ackFailedTopic"));

        @Cleanup
        MessageProducer producer1 = new MessageProducer(url1, dest);

        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopic(dest.toString(), false)
                .getNow(null).get();
        final ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) topic.getManagedLedger();
        final ManagedCursorImpl cursor = (ManagedCursorImpl) managedLedger.openCursor("pulsar.repl.r2");
        final ManagedCursorImpl spyCursor = spy(cursor);
        managedLedger.getCursors().removeCursor(cursor.getName());
        managedLedger.getCursors().add(spyCursor, PositionImpl.EARLIEST);
        AtomicBoolean isMakeAckFail = new AtomicBoolean(false);
        doAnswer(invocation -> {
            Position pos = (Position) invocation.getArguments()[0];
            AsyncCallbacks.DeleteCallback cb = (AsyncCallbacks.DeleteCallback) invocation.getArguments()[1];
            Object ctx = invocation.getArguments()[2];
            if (isMakeAckFail.get()) {
                log.info("async-delete {} will be failed", pos);
                cb.deleteFailed(new ManagedLedgerException("mocked error"), ctx);
            } else {
                log.info("async-delete {} will success", pos);
                cursor.asyncDelete(pos, cb, ctx);
            }
            return null;
        }).when(spyCursor).asyncDelete(Mockito.any(Position.class), Mockito.any(AsyncCallbacks.DeleteCallback.class),
                Mockito.any());

        log.info("--- Starting producer --- " + url1);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));
        // Produce from cluster1 and consume from the rest
        producer1.produce(2);

        MessageIdImpl lastMessageId = (MessageIdImpl) topic.getLastMessageId().get();
        Position lastPosition = PositionImpl.get(lastMessageId.getLedgerId(), lastMessageId.getEntryId());

        Awaitility.await().pollInterval(1, TimeUnit.SECONDS).timeout(30, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(() -> {
                    ConcurrentOpenHashMap<String, Replicator> replicators = topic.getReplicators();
                    PersistentReplicator replicator = (PersistentReplicator) replicators.get("r2");
                    assertEquals(org.apache.pulsar.broker.service.AbstractReplicator.State.Started,
                            replicator.getState());
                });

        // Make sure all the data has replicated to the remote cluster before close the cursor.
        Awaitility.await().untilAsserted(() -> assertEquals(cursor.getMarkDeletedPosition(), lastPosition));

        isMakeAckFail.set(true);

        producer1.produce(10);

        // The cursor is closed, so the mark delete position will not move forward.
        assertEquals(cursor.getMarkDeletedPosition(), lastPosition);

        isMakeAckFail.set(false);

        Awaitility.await().timeout(30, TimeUnit.SECONDS).until(
                () -> {
                    log.info("++++++++++++ {}, {}", cursor.getMarkDeletedPosition(), cursor.getReadPosition());
                    return cursor.getMarkDeletedPosition().getEntryId() == (cursor.getReadPosition().getEntryId() - 1);
                });
    }

    private static final Logger log = LoggerFactory.getLogger(ReplicatorTest.class);

    @Test
    public void testWhenUpdateReplicationCluster() throws Exception {
        log.info("--- testWhenUpdateReplicationCluster ---");
        String namespace = BrokerTestUtil.newUniqueName("pulsar/ns");;
        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));
        final TopicName dest = TopicName.get(
                BrokerTestUtil.newUniqueName("persistent://" + namespace + "/testWhenUpdateReplicationCluster"));
        @Cleanup
        MessageProducer producer1 = new MessageProducer(url1, dest);
        log.info("--- Starting producer --- " + url1);

        producer1.produce(2);

        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopic(dest.toString(), false)
                .getNow(null).get();
        Awaitility.await().untilAsserted(() -> {
            assertTrue(topic.getReplicators().containsKey("r2"));
        });

        admin1.topics().setReplicationClusters(dest.toString(), List.of("r1"));

        Awaitility.await().untilAsserted(() -> {
            Set<String> replicationClusters = admin1.topics().getReplicationClusters(dest.toString(), false);
            assertTrue(replicationClusters != null && replicationClusters.size() == 1);
            assertTrue(topic.getReplicators().isEmpty());
        });
    }

    @Test
    public void testReplicatorProducerNotExceed() throws Exception {
        log.info("--- testReplicatorProducerNotExceed ---");
        String namespace1 = BrokerTestUtil.newUniqueName("pulsar/ns1");
        admin1.namespaces().createNamespace(namespace1);
        admin1.namespaces().setNamespaceReplicationClusters(namespace1, Sets.newHashSet("r1", "r2"));
        final TopicName dest1 = TopicName.get(
                BrokerTestUtil.newUniqueName("persistent://" + namespace1 + "/testReplicatorProducerNotExceed1"));
        String namespace2 = BrokerTestUtil.newUniqueName("pulsar/ns2");
        admin2.namespaces().createNamespace(namespace2);
        admin2.namespaces().setNamespaceReplicationClusters(namespace2, Sets.newHashSet("r1", "r2"));
        final TopicName dest2 = TopicName.get(
                BrokerTestUtil.newUniqueName("persistent://" + namespace1 + "/testReplicatorProducerNotExceed2"));
        admin1.topics().createPartitionedTopic(dest1.toString(), 1);
        admin1.topicPolicies().setMaxProducers(dest1.toString(), 1);
        admin2.topics().createPartitionedTopic(dest2.toString(), 1);
        admin2.topicPolicies().setMaxProducers(dest2.toString(), 1);
        @Cleanup
        MessageProducer producer1 = new MessageProducer(url1, dest1);
        log.info("--- Starting producer1 --- " + url1);

        producer1.produce(1);

        @Cleanup
        MessageProducer producer2 = new MessageProducer(url2, dest2);
        log.info("--- Starting producer2 --- " + url2);

        producer2.produce(1);

        Assert.assertThrows(PulsarClientException.ProducerBusyException.class, () -> new MessageProducer(url2, dest2));
    }

    @Test
    public void testReplicatorWithTTL() throws Exception {
        log.info("--- Starting ReplicatorTest::testReplicatorWithTTL ---");

        final String cluster1 = pulsar1.getConfig().getClusterName();
        final String cluster2 = pulsar2.getConfig().getClusterName();
        final String namespace = BrokerTestUtil.newUniqueName("pulsar/ns");
        final TopicName topic = TopicName
                .get(BrokerTestUtil.newUniqueName("persistent://" + namespace + "/testReplicatorWithTTL"));
        admin1.namespaces().createNamespace(namespace, Sets.newHashSet(cluster1, cluster2));
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(cluster1, cluster2));
        admin1.topics().createNonPartitionedTopic(topic.toString());
        admin1.topicPolicies().setMessageTTL(topic.toString(), 1);

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        @Cleanup
        Producer<byte[]> persistentProducer1 = client1.newProducer().topic(topic.toString()).create();
        persistentProducer1.send("V1".getBytes());

        waitReplicateFinish(topic, admin1);

        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar1.getBrokerService().getTopicReference(topic.toString()).get();
        persistentTopic.getReplicators().forEach((cluster, replicator) -> {
            PersistentReplicator persistentReplicator = (PersistentReplicator) replicator;
            // Pause replicator
            pauseReplicator(persistentReplicator);
        });

        persistentProducer1.send("V2".getBytes());
        persistentProducer1.send("V3".getBytes());

        Thread.sleep(1000);

        admin1.topics().expireMessagesForAllSubscriptions(topic.toString(), 1);

        persistentTopic.getReplicators().forEach((cluster, replicator) -> {
            PersistentReplicator persistentReplicator = (PersistentReplicator) replicator;
            persistentReplicator.startProducer();
        });

        waitReplicateFinish(topic, admin1);

        persistentProducer1.send("V4".getBytes());

        waitReplicateFinish(topic, admin1);

        @Cleanup
        PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        @Cleanup
        Consumer<byte[]> consumer = client2.newConsumer().topic(topic.toString())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscriptionName("sub").subscribe();

        List<String> result = new ArrayList<>();
        while (true) {
            Message<byte[]> receive = consumer.receive(2, TimeUnit.SECONDS);
            if (receive == null) {
                break;
            }
            result.add(new String(receive.getValue()));
        }

        assertEquals(result, Lists.newArrayList("V1", "V4"));
    }

    @Test
    public void testEnableReplicationWithNamespaceAllowedClustersPolices() throws Exception {
        log.info("--- testEnableReplicationWithNamespaceAllowedClustersPolices ---");
        String namespace1 = "pulsar/ns" + RandomUtils.nextLong();
        admin1.namespaces().createNamespace(namespace1);
        admin2.namespaces().createNamespace(namespace1 + "init_cluster_node");
        admin1.namespaces().setNamespaceAllowedClusters(namespace1, Sets.newHashSet("r1", "r2"));
        final TopicName topicName = TopicName.get(
                BrokerTestUtil.newUniqueName("persistent://" + namespace1 + "/testReplicatorProducerNotExceed1"));

        @Cleanup PulsarClient client1 = PulsarClient
                .builder()
                .serviceUrl(pulsar1.getBrokerServiceUrl())
                .build();
        @Cleanup Producer<byte[]> producer = client1
                .newProducer()
                .topic(topicName.toString())
                .create();
        producer.newMessage().send();
        // Enable replication at the topic level in the cluster1.
        admin1.topics().setReplicationClusters(topicName.toString(), List.of("r1", "r2"));

        PersistentTopic persistentTopic1 = (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName.toString(),
                false)
                .get()
                .get();
        // Verify the replication from cluster1 to cluster2 is ready, but the replication form the cluster2 to cluster1
        // is not ready.
        Awaitility.await().untilAsserted(() -> {
            ConcurrentOpenHashMap<String, Replicator> replicatorMap = persistentTopic1.getReplicators();
            assertEquals(replicatorMap.size(), 1);
            Replicator replicator = replicatorMap.get(replicatorMap.keys().get(0));
            assertTrue(replicator.isConnected());
        });

        PersistentTopic persistentTopic2 = (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName.toString(),
                        false)
                .get()
                .get();

        Awaitility.await().untilAsserted(() -> {
            ConcurrentOpenHashMap<String, Replicator> replicatorMap = persistentTopic2.getReplicators();
            assertEquals(replicatorMap.size(), 0);
        });
        // Enable replication at the topic level in the cluster2.
        admin2.topics().setReplicationClusters(topicName.toString(), List.of("r1", "r2"));
        //  Verify the replication between cluster1 and cluster2  is ready.
        Awaitility.await().untilAsserted(() -> {
            ConcurrentOpenHashMap<String, Replicator> replicatorMap = persistentTopic2.getReplicators();
            assertEquals(replicatorMap.size(), 1);
            Replicator replicator = replicatorMap.get(replicatorMap.keys().get(0));
            assertTrue(replicator.isConnected());
        });
    }

    private void pauseReplicator(PersistentReplicator replicator) {
        Awaitility.await().untilAsserted(() -> {
            assertTrue(replicator.isConnected());
        });
        replicator.closeProducerAsync(true);
    }
}
