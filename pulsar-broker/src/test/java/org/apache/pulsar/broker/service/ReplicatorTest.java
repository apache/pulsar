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

import static org.apache.pulsar.broker.BrokerTestUtil.newUniqueName;
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import com.scurrilous.circe.checksum.Crc32cIntChecksum;
import io.netty.buffer.ByteBuf;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.CursorAlreadyClosedException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerServiceException.NamingException;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.api.RawReader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.events.EventsTopicNames;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.RetentionPolicy;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ReplicatorStats;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.schema.Schemas;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

/**
 * Starts 3 brokers that are in 3 different clusters
 */
@Test(groups = "broker")
public class ReplicatorTest extends ReplicatorTestBase {

    protected String methodName;

    @BeforeMethod(alwaysRun = true)
    public void beforeMethod(Method m) throws Exception {
        methodName = m.getName();
        admin1.namespaces().removeBacklogQuota("pulsar/ns");
        admin1.namespaces().removeBacklogQuota("pulsar/ns1");
        admin1.namespaces().removeBacklogQuota("pulsar/global/ns");
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

    @Test
    public void testConfigChange() throws Exception {
        log.info("--- Starting ReplicatorTest::testConfigChange ---");
        // This test is to verify that the config change on global namespace is successfully applied in broker during
        // runtime.
        // Run a set of producer tasks to create the topics
        List<Future<Void>> results = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            final TopicName dest = TopicName.get(BrokerTestUtil.newUniqueName("persistent://pulsar/ns/topic-" + i));

            results.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {

                    @Cleanup
                    MessageProducer producer = new MessageProducer(url1, dest);
                    log.info("--- Starting producer --- " + url1);

                    @Cleanup
                    MessageConsumer consumer = new MessageConsumer(url1, dest);
                    log.info("--- Starting Consumer --- " + url1);

                    producer.produce(2);
                    consumer.receive(2);
                    return null;
                }
            }));
        }

        for (Future<Void> result : results) {
            try {
                result.get();
            } catch (Exception e) {
                log.error("exception in getting future result ", e);
                fail(String.format("replication test failed with %s exception", e.getMessage()));
            }
        }

        Thread.sleep(1000L);
        // Make sure that the internal replicators map contains remote cluster info
        ConcurrentOpenHashMap<String, PulsarClient> replicationClients1 = ns1.getReplicationClients();
        ConcurrentOpenHashMap<String, PulsarClient> replicationClients2 = ns2.getReplicationClients();
        ConcurrentOpenHashMap<String, PulsarClient> replicationClients3 = ns3.getReplicationClients();

        Assert.assertNotNull(replicationClients1.get("r2"));
        Assert.assertNotNull(replicationClients1.get("r3"));
        Assert.assertNotNull(replicationClients2.get("r1"));
        Assert.assertNotNull(replicationClients2.get("r3"));
        Assert.assertNotNull(replicationClients3.get("r1"));
        Assert.assertNotNull(replicationClients3.get("r2"));

        // Case 1: Update the global namespace replication configuration to only contains the local cluster itself
        admin1.namespaces().setNamespaceReplicationClusters("pulsar/ns", Sets.newHashSet("r1"));

        // Wait for config changes to be updated.
        Thread.sleep(1000L);

        // Make sure that the internal replicators map still contains remote cluster info
        Assert.assertNotNull(replicationClients1.get("r2"));
        Assert.assertNotNull(replicationClients1.get("r3"));
        Assert.assertNotNull(replicationClients2.get("r1"));
        Assert.assertNotNull(replicationClients2.get("r3"));
        Assert.assertNotNull(replicationClients3.get("r1"));
        Assert.assertNotNull(replicationClients3.get("r2"));

        // Case 2: Update the configuration back
        admin1.namespaces().setNamespaceReplicationClusters("pulsar/ns", Sets.newHashSet("r1", "r2", "r3"));

        // Wait for config changes to be updated.
        Thread.sleep(1000L);

        // Make sure that the internal replicators map still contains remote cluster info
        Assert.assertNotNull(replicationClients1.get("r2"));
        Assert.assertNotNull(replicationClients1.get("r3"));
        Assert.assertNotNull(replicationClients2.get("r1"));
        Assert.assertNotNull(replicationClients2.get("r3"));
        Assert.assertNotNull(replicationClients3.get("r1"));
        Assert.assertNotNull(replicationClients3.get("r2"));

        // Case 3: TODO: Once automatic cleanup is implemented, add tests case to verify auto removal of clusters
    }

    @Test(timeOut = 10000)
    public void activeBrokerParse() throws Exception {
        pulsar1.getConfiguration().setAuthorizationEnabled(true);
        //init clusterData

        String cluster2ServiceUrls = String.format("%s,localhost:1234,localhost:5678", pulsar2.getWebServiceAddress());
        ClusterData cluster2Data = ClusterData.builder().serviceUrl(cluster2ServiceUrls).build();
        String cluster2 = "activeCLuster2";
        admin2.clusters().createCluster(cluster2, cluster2Data);
        Awaitility.await().until(()
                -> admin2.clusters().getCluster(cluster2) != null);

        List<String> list = admin1.brokers().getActiveBrokers(cluster2);
        assertEquals(list.get(0), url2.toString().replace("http://", ""));
        //restore configuration
        pulsar1.getConfiguration().setAuthorizationEnabled(false);
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

    @Test
    public void testReplicationWithSchema() throws Exception {
        PulsarClient client1 = pulsar1.getClient();
        PulsarClient client2 = pulsar2.getClient();
        PulsarClient client3 = pulsar3.getClient();
        final TopicName topic = TopicName
                .get(BrokerTestUtil.newUniqueName("persistent://pulsar/ns/testReplicationWithSchema"));

        final String subName = "my-sub";

        @Cleanup
        Producer<Schemas.PersonOne> producer1 = client1.newProducer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(topic.toString())
                .create();
        @Cleanup
        Producer<Schemas.PersonOne> producer2 = client2.newProducer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(topic.toString())
                .create();
        @Cleanup
        Producer<Schemas.PersonOne> producer3 = client3.newProducer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(topic.toString())
                .create();

        List<Producer<Schemas.PersonOne>> producers = Lists.newArrayList(producer1, producer2, producer3);

        @Cleanup
        Consumer<Schemas.PersonOne> consumer1 = client1.newConsumer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(topic.toString())
                .subscriptionName(subName)
                .subscribe();

        @Cleanup
        Consumer<Schemas.PersonOne> consumer2 = client2.newConsumer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(topic.toString())
                .subscriptionName(subName)
                .subscribe();

        @Cleanup
        Consumer<Schemas.PersonOne> consumer3 = client3.newConsumer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(topic.toString())
                .subscriptionName(subName)
                .subscribe();

        for (int i = 0; i < 3; i++) {
            producers.get(i).send(new Schemas.PersonOne(i));
            Message<Schemas.PersonOne> msg1 = consumer1.receive();
            Message<Schemas.PersonOne> msg2 = consumer2.receive();
            Message<Schemas.PersonOne> msg3 = consumer3.receive();
            assertTrue(msg1 != null && msg2 != null && msg3 != null);
            assertTrue(msg1.getValue().equals(msg2.getValue()) && msg2.getValue().equals(msg3.getValue()));
            consumer1.acknowledge(msg1);
            consumer2.acknowledge(msg2);
            consumer3.acknowledge(msg3);
        }
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
            producer1.produce(1, producer1.newMessage().replicationClusters(Lists.newArrayList("r1", "r3")));
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
        replicator.disconnect(false);
        Thread.sleep(100);
        Field field = AbstractReplicator.class.getDeclaredField("producer");
        field.setAccessible(true);
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) field.get(replicator);
        assertNull(producer);
    }

    @Test(priority = 5, timeOut = 30000)
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

    @Test(priority = 5, timeOut = 30000)
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
        List<RetentionPolicy> policies = Lists.newArrayList();
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
                !topic.contains(EventsTopicNames.NAMESPACE_EVENTS_LOCAL_NAME)).count(), 5);
        // Update partitioned topic from R3
        admin3.topics().updatePartitionedTopic(persistentTopicName, 6);
        assertEquals(admin3.topics().getPartitionedTopicMetadata(persistentTopicName).partitions, 6);
        assertEquals(admin3.topics().getList(namespace).stream().filter(topic ->
                !topic.contains(EventsTopicNames.NAMESPACE_EVENTS_LOCAL_NAME)).count(), 6);
        // Update partitioned topic from R1
        admin1.topics().updatePartitionedTopic(persistentTopicName, 7);
        assertEquals(admin1.topics().getPartitionedTopicMetadata(persistentTopicName).partitions, 7);
        assertEquals(admin2.topics().getPartitionedTopicMetadata(persistentTopicName).partitions, 7);
        assertEquals(admin3.topics().getPartitionedTopicMetadata(persistentTopicName).partitions, 7);
        assertEquals(admin1.topics().getList(namespace).stream().filter(topic ->
                !topic.contains(EventsTopicNames.NAMESPACE_EVENTS_LOCAL_NAME)).count(), 7);
        assertEquals(admin2.topics().getList(namespace).stream().filter(topic ->
                !topic.contains(EventsTopicNames.NAMESPACE_EVENTS_LOCAL_NAME)).count(), 7);
        assertEquals(admin3.topics().getList(namespace).stream().filter(topic ->
                !topic.contains(EventsTopicNames.NAMESPACE_EVENTS_LOCAL_NAME)).count(), 7);
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

        final String namespace = "pulsar/global/repl";
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
        TypedMessageBuilder<byte[]> msg = producer1.newMessage().replicationClusters(Lists.newArrayList("r1")).value(value);
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

    @DataProvider(name = "topicPrefix")
    public static Object[][] topicPrefix() {
        return new Object[][] { { "persistent://" , "/persistent" }, { "non-persistent://" , "/non-persistent" } };
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

        final CompletableFuture<Optional<Topic>> timedOutTopicFuture = topicFuture;
        // timeout topic future should be removed from cache
        retryStrategically((test) -> pulsar1.getBrokerService().getTopic(topicName, false) != timedOutTopicFuture, 5,
                1000);

        assertNotEquals(timedOutTopicFuture, pulsar1.getBrokerService().getTopics().get(topicName));

        try {
            Consumer<byte[]> consumer = client1.newConsumer().topic(topicName).subscriptionType(SubscriptionType.Shared)
                    .subscriptionName("my-subscriber-name").subscribeAsync().get(100, TimeUnit.MILLISECONDS);
            fail("consumer should fail due to topic loading failure");
        } catch (Exception e) {
            // Ok
        }

        ManagedLedgerImpl ml = (ManagedLedgerImpl) mlFactory.open(topicMlName + "-2");
        mlFuture.complete(ml);

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
        String systemTopic = TopicName.get("persistent", NamespaceName.get(namespace),
                EventsTopicNames.TRANSACTION_BUFFER_SNAPSHOT).toString();
        admin1.topics().createNonPartitionedTopic(topic);
        // Replicator will not replicate System Topic other than topic policies
        initTransaction(2, admin1, pulsar1.getBrokerServiceUrl(), pulsar1);
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar1.getBrokerServiceUrl())
                .enableTransaction(true).build();
        TransactionImpl transaction = (TransactionImpl) client.newTransaction()
                .withTransactionTimeout(60, TimeUnit.SECONDS).build().get();
        @Cleanup
        Producer<byte[]> producer = client.newProducer().topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false).create();
        producer.newMessage(transaction).value("1".getBytes(StandardCharsets.UTF_8)).send();
        transaction.commit();

        Awaitility.await().untilAsserted(() -> {
            Assert.assertEquals(admin1.topics().getStats(systemTopic).getReplication().size(), 0);
            Assert.assertEquals(admin2.topics().getStats(systemTopic).getReplication().size(), 0);
            Assert.assertEquals(admin3.topics().getStats(systemTopic).getReplication().size(), 0);
        });
        cleanup();
        setup();

    }

    private void initTransaction(int coordinatorSize, PulsarAdmin admin, String ServiceUrl,
                                 PulsarService pulsarService) throws Exception {
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString(), coordinatorSize);
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), coordinatorSize);
        admin.lookups().lookupTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString());
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
                    .filter(topic -> !topic.contains(EventsTopicNames.NAMESPACE_EVENTS_LOCAL_NAME))
                    .collect(Collectors.toList()));
            return list.size() == expectedTopicList.size();
        });
        for (String expectTopic : expectedTopicList) {
            Assert.assertTrue(list.contains(expectTopic));
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ReplicatorTest.class);

}
