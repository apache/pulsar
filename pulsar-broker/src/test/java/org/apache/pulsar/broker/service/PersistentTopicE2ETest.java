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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.EntryCacheImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.service.persistent.MessageRedeliveryController;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.schema.SchemaRegistry;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.ProducerBusyException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.schema.Schemas;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "flaky")
public class PersistentTopicE2ETest extends BrokerTestBase {
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        for (AutoCloseable closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception e) {
                // ignore exception
            }
        }
        closeables.clear();
        super.internalCleanup();
    }

    @Test
    public void testSimpleProducerEvents() throws Exception {
        final String topicName = "persistent://prop/ns-abc/topic0";

        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        // 2. producer publish messages
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        rolloverPerIntervalStats();
        assertTrue(topicRef.getProducers().values().iterator().next().getStats().msgRateIn > 0.0);

        // 3. producer disconnect
        producer.close();

        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        assertEquals(topicRef.getProducers().size(), 0);
    }

    @Test
    public void testSimpleConsumerEvents() throws Exception {
        final String topicName = "persistent://prop/ns-abc/topic1";
        final String subName = "sub1";
        final int numMsgs = 10;

        // 1. client connect
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        PersistentSubscription subRef = topicRef.getSubscription(subName);

        assertNotNull(topicRef);
        assertNotNull(subRef);
        assertTrue(subRef.getDispatcher().isConsumerConnected());

        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        assertEquals(getAvailablePermits(subRef), 1000 /* default */);

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        for (int i = 0; i < numMsgs * 2; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        assertTrue(subRef.getDispatcher().isConsumerConnected());
        rolloverPerIntervalStats();
        assertEquals(subRef.getNumberOfEntriesInBacklog(false), numMsgs * 2);

        // 2. messages pushed before client receive
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        assertEquals(getAvailablePermits(subRef), 1000 - numMsgs * 2);

        Message<byte[]> msg = null;
        for (int i = 0; i < numMsgs; i++) {
            msg = consumer.receive();
            // 3. in-order message delivery
            assertEquals(new String(msg.getData()), "my-message-" + i);
            consumer.acknowledge(msg);
        }

        rolloverPerIntervalStats();

        // 4. messages deleted on individual acks
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        assertEquals(subRef.getNumberOfEntriesInBacklog(false), numMsgs);

        for (int i = 0; i < numMsgs; i++) {
            msg = consumer.receive();
            if (i == numMsgs - 1) {
                consumer.acknowledgeCumulative(msg);
            }
        }

        rolloverPerIntervalStats();

        // 5. messages deleted on cumulative acks
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        assertEquals(subRef.getNumberOfEntriesInBacklog(false), 0);

        // 6. consumer unsubscribe
        consumer.unsubscribe();

        // 6. consumer graceful close
        consumer.close();

        // 7. consumer unsubscribe
        try {
            consumer.unsubscribe();
            fail("Should have failed");
        } catch (PulsarClientException.AlreadyClosedException e) {
            // ok
        }

        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        subRef = topicRef.getSubscription(subName);
        assertNull(subRef);

        producer.close();

        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
    }

    @Test
    public void testConsumerFlowControl() throws Exception {
        final String topicName = "persistent://prop/ns-abc/topic2";
        final String subName = "sub2";

        Message<byte[]> msg;
        int recvQueueSize = 4;

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .receiverQueueSize(recvQueueSize).subscribe();
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);
        PersistentSubscription subRef = topicRef.getSubscription(subName);
        assertNotNull(subRef);

        // 1. initial receive queue size recorded
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        assertEquals(getAvailablePermits(subRef), recvQueueSize);

        for (int i = 0; i < recvQueueSize / 2; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            msg = consumer.receive();
            consumer.acknowledge(msg);
        }

        // 2. queue size re-adjusted after successful receive of half of window size
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        assertEquals(getAvailablePermits(subRef), recvQueueSize);

        consumer.close();
        assertFalse(subRef.getDispatcher().isConsumerConnected());
    }

    /**
     * Validation: 1. validates active-cursor after active subscription 2. validate active-cursor with subscription 3.
     * unconsumed messages should be present into cache 4. cache and active-cursor should be empty once subscription is
     * closed
     *
     * @throws Exception
     */
    @Test
    public void testActiveSubscriptionWithCache() throws Exception {
        final String topicName = "persistent://prop/ns-abc/topic2";
        final String subName = "sub2";

        Message<byte[]> msg;
        int recvQueueSize = 4;

        // (1) Create subscription
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .receiverQueueSize(recvQueueSize).subscribe();
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // (2) Produce Messages
        for (int i = 0; i < recvQueueSize / 2; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            msg = consumer.receive();
            consumer.acknowledge(msg);
        }

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        // (3) Get Entry cache
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) topicRef.getManagedLedger();
        Field cacheField = ManagedLedgerImpl.class.getDeclaredField("entryCache");
        cacheField.setAccessible(true);
        EntryCacheImpl entryCache = (EntryCacheImpl) cacheField.get(ledger);

        /************* Validation on non-empty active-cursor **************/
        // (4) Get ActiveCursor : which is list of active subscription
        Iterable<ManagedCursor> activeCursors = ledger.getActiveCursors();
        ManagedCursor curosr = activeCursors.iterator().next();
        // (4.1) Validate: active Cursor must be non-empty
        assertNotNull(curosr);
        // (4.2) Validate: validate cursor name
        assertEquals(subName, curosr.getName());

        /************* Validation on empty active-cursor **************/
        // (5) Close consumer: which (1)removes activeConsumer and (2)clears the entry-cache
        consumer.close();
        Thread.sleep(1000);
        // (5.1) Validate: active-consumer must be empty
        assertFalse(ledger.getActiveCursors().iterator().hasNext());
        // (5.2) Validate: Entry-cache must be cleared
        assertEquals(entryCache.getSize(), 0);

    }

    // some race conditions needs to be handled
    // disabling the test for now to not block commit jobs
    @Test(enabled = false)
    public void testConcurrentConsumerThreads() throws Exception {
        // test concurrent consumer threads on same consumerId
        final String topicName = "persistent://prop/ns-abc/topic3";
        final String subName = "sub3";

        final int recvQueueSize = 100;
        final int numConsumersThreads = 10;

        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();

        final CyclicBarrier barrier = new CyclicBarrier(numConsumersThreads + 1);
        for (int i = 0; i < numConsumersThreads; i++) {
            executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    barrier.await();

                    Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                            .receiverQueueSize(recvQueueSize).subscribe();
                    for (int i = 0; i < recvQueueSize / numConsumersThreads; i++) {
                        Message<byte[]> msg = consumer.receive();
                        consumer.acknowledge(msg);
                    }
                    return null;
                }
            });
        }

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        for (int i = 0; i < recvQueueSize * numConsumersThreads; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        barrier.await();

        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        PersistentSubscription subRef = topicRef.getSubscription(subName);

        // 1. cumulatively all threads drain the backlog
        assertEquals(subRef.getNumberOfEntriesInBacklog(false), 0);

        // 2. flow control works the same as single consumer single thread
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        assertEquals(getAvailablePermits(subRef), recvQueueSize);
    }

    @Test(enabled = false)
    // TODO: enable this after java client supports graceful close
    public void testGracefulClose() throws Exception {
        final String topicName = "persistent://prop/ns-abc/topic4";
        final String subName = "sub4";

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);

        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();
        CountDownLatch latch = new CountDownLatch(1);
        executor.submit(() -> {
            for (int i = 0; i < 10; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }
            latch.countDown();
            return null;
        });

        producer.close();

        // 1. verify there are no pending publish acks once the producer close
        // is completed on client
        assertEquals(topicRef.getProducers().values().iterator().next().getPendingPublishAcks(), 0);

        // safety latch in case of failure,
        // wait for the spawned thread to complete
        latch.await();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();

        PersistentSubscription subRef = topicRef.getSubscription(subName);
        assertNotNull(subRef);

        Message<byte[]> msg = null;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive();
        }

        // 2. verify consumer close fails when there are outstanding
        // message acks
        try {
            consumer.close();
            fail("should have failed");
        } catch (IllegalStateException e) {
            // Expected - messages not acked
        }

        consumer.acknowledgeCumulative(msg);
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        // 3. verify consumer close succeeds once all messages are ack'ed
        consumer.close();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        assertTrue(subRef.getDispatcher().isConsumerConnected());
    }

    @Test
    public void testSimpleCloseTopic() throws Exception {
        final String topicName = "persistent://prop/ns-abc/topic5";
        final String subName = "sub5";

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);
        PersistentSubscription subRef = topicRef.getSubscription(subName);
        assertNotNull(subRef);

        Message<byte[]> msg;
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            msg = consumer.receive();
            consumer.acknowledge(msg);
        }

        producer.close();
        consumer.close();

        topicRef.close().get();
        assertFalse(pulsar.getBrokerService().getTopicReference(topicName).isPresent());
    }

    @Test
    public void testSingleClientMultipleSubscriptions() throws Exception {
        final String topicName = "persistent://prop/ns-abc/topic6";
        final String subName = "sub6";

        pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();
        pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        try {
            pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();
            fail("Should have thrown an exception since one consumer is already connected");
        } catch (PulsarClientException cce) {
            Assert.assertTrue(cce.getMessage().contains("Exclusive consumer is already connected"));
        }
    }

    @Test
    public void testMultipleClientsMultipleSubscriptions() throws Exception {
        final String topicName = "persistent://prop/ns-abc/topic7";
        final String subName = "sub7";

        @Cleanup("shutdown")
        PulsarClient client1 = PulsarClient.builder().serviceUrl(brokerUrl.toString()).build();
        @Cleanup("shutdown")
        PulsarClient client2 = PulsarClient.builder().serviceUrl(brokerUrl.toString()).build();

        try {
            client1.newConsumer().topic(topicName).subscriptionName(subName).subscribe();
            client1.newProducer().topic(topicName).create();

            client2.newProducer().topic(topicName).create();

            client2.newConsumer().topic(topicName).subscriptionName(subName).subscribe();
            fail("Should have thrown an exception since one consumer is already connected");
        } catch (PulsarClientException cce) {
            Assert.assertTrue(cce.getMessage().contains("Exclusive consumer is already connected"));
        }
    }

    @Test
    public void testTopicDeleteWithDisconnectedSubscription() throws Exception {
        final String topicName = "persistent://prop/ns-abc/topic8";
        final String subName = "sub1";

        // 1. client connect
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        PersistentSubscription subRef = topicRef.getSubscription(subName);

        assertNotNull(topicRef);
        assertNotNull(subRef);
        assertTrue(subRef.getDispatcher().isConsumerConnected());

        // 2. client disconnect
        consumer.close();
        assertFalse(subRef.getDispatcher().isConsumerConnected());

        // 3. delete topic
        admin.topics().delete(topicName);
        try {
            admin.topics().getStats(topicName);
        } catch (PulsarAdminException e) {
            // ok
        }
    }

    int getAvailablePermits(PersistentSubscription sub) {
        return sub.getDispatcher().getConsumers().get(0).getAvailablePermits();
    }

    @Test(enabled = false)
    public void testUnloadNamespace() throws Exception {
        String topic = "persistent://prop/ns-abc/topic-9";
        TopicName topicName = TopicName.get(topic);
        pulsarClient.newProducer().topic(topic).create();
        pulsarClient.close();

        assertNotNull(pulsar.getBrokerService().getTopicReference(topic));
        assertTrue(((ManagedLedgerFactoryImpl) pulsar.getManagedLedgerFactory()).getManagedLedgers()
                .containsKey(topicName.getPersistenceNamingEncoding()));

        admin.namespaces().unload("prop/ns-abc");

        int i = 0;
        for (i = 0; i < 30; i++) {
            if (!pulsar.getBrokerService().getTopicReference(topic).isPresent()) {
                break;
            }
            Thread.sleep(1000);
        }
        if (i == 30) {
            fail("The topic reference should be null");
        }

        // ML should have been closed as well
        assertFalse(((ManagedLedgerFactoryImpl) pulsar.getManagedLedgerFactory()).getManagedLedgers()
                .containsKey(topicName.getPersistenceNamingEncoding()));
    }

    @Test
    public void testGC() throws Exception {
        // 1. Simple successful GC
        String topicName = "persistent://prop/ns-abc/topic-10";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        producer.close();

        assertTrue(pulsar.getBrokerService().getTopicReference(topicName).isPresent());
        runGC();
        assertFalse(pulsar.getBrokerService().getTopicReference(topicName).isPresent());

        // 2. Topic is not GCed with live connection
        String subName = "sub1";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();

        runGC();
        assertTrue(pulsar.getBrokerService().getTopicReference(topicName).isPresent());

        // 3. Topic with subscription is not GCed even with no connections
        consumer.close();

        runGC();
        assertTrue(pulsar.getBrokerService().getTopicReference(topicName).isPresent());

        // 4. Topic can be GCed after unsubscribe
        admin.topics().deleteSubscription(topicName, subName);

        runGC();
        assertFalse(pulsar.getBrokerService().getTopicReference(topicName).isPresent());
    }

    @Data
    @ToString
    @EqualsAndHashCode
    private static class Foo {
        private String field1;
        private String field2;
        private int field3;
    }

    private Optional<Topic> getTopic(String topicName) {
        return pulsar.getBrokerService().getTopicReference(topicName);
    }

    private boolean topicHasSchema(String topicName) {
        String base = TopicName.get(topicName).getPartitionedTopicName();
        String schemaName = TopicName.get(base).getSchemaName();
        SchemaRegistry.SchemaAndMetadata result = pulsar.getSchemaRegistryService().getSchema(schemaName).join();
        return result != null && !result.schema.isDeleted();
    }

    @Test
    public void testGCWillDeleteSchema() throws Exception {
        // 1. Simple successful GC
        String topicName = "persistent://prop/ns-abc/topic-1";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        producer.close();

        Optional<Topic> topic = getTopic(topicName);
        assertTrue(topic.isPresent());

        byte[] data = JSONSchema.of(SchemaDefinition.builder()
                .withPojo(Foo.class).build()).getSchemaInfo().getSchema();
        SchemaData schemaData = SchemaData.builder()
                .data(data)
                .type(SchemaType.BYTES)
                .user("foo").build();
        topic.get().addSchema(schemaData).join();
        assertTrue(topicHasSchema(topicName));
        runGC();

        topic = getTopic(topicName);
        assertFalse(topic.isPresent());
        assertFalse(topicHasSchema(topicName));

        // 2. Topic is not GCed with live connection
        topicName = "persistent://prop/ns-abc/topic-2";
        String subName = "sub1";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();
        topic = getTopic(topicName);
        assertTrue(topic.isPresent());
        topic.get().addSchema(schemaData).join();
        assertTrue(topicHasSchema(topicName));

        runGC();
        topic = getTopic(topicName);
        assertTrue(topic.isPresent());
        assertTrue(topicHasSchema(topicName));

        // 3. Topic with subscription is not GCed even with no connections
        consumer.close();

        runGC();
        topic = getTopic(topicName);
        assertTrue(topic.isPresent());
        assertTrue(topicHasSchema(topicName));

        // 4. Topic can be GCed after unsubscribe
        admin.topics().deleteSubscription(topicName, subName);

        runGC();
        topic = getTopic(topicName);
        assertFalse(topic.isPresent());
        assertFalse(topicHasSchema(topicName));
    }

    @Test
    public void testDeleteSchema() throws Exception {
        @Cleanup
        PulsarClientImpl httpProtocolClient = (PulsarClientImpl) PulsarClient.builder().serviceUrl(brokerUrl.toString()).build();
        PulsarClientImpl binaryProtocolClient = (PulsarClientImpl) pulsarClient;
        LookupService binaryLookupService = binaryProtocolClient.getLookup();
        LookupService httpLookupService = httpProtocolClient.getLookup();

        String topicName = "persistent://prop/ns-abc/topic-1";
        //Topic is not GCed with live connection
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();

        Optional<Topic> topic = getTopic(topicName);
        assertTrue(topic.isPresent());

        byte[] data = JSONSchema.of(SchemaDefinition.builder()
                .withPojo(Foo.class).build()).getSchemaInfo().getSchema();
        SchemaData schemaData = SchemaData.builder()
                .data(data)
                .type(SchemaType.BYTES)
                .user("foo").build();

        topic.get().addSchema(schemaData).join();
        assertTrue(topicHasSchema(topicName));

        Assert.assertEquals(admin.schemas().getAllSchemas(topicName).size(), 1);
        assertTrue(httpLookupService.getSchema(TopicName.get(topicName), ByteBuffer.allocate(8).putLong(0).array()).get().isPresent());
        assertTrue(binaryLookupService.getSchema(TopicName.get(topicName), ByteBuffer.allocate(8).putLong(0).array()).get().isPresent());

        topic.get().deleteSchema().join();
        Assert.assertEquals(admin.schemas().getAllSchemas(topicName).size(), 0);
        assertFalse(httpLookupService.getSchema(TopicName.get(topicName), ByteBuffer.allocate(8).putLong(0).array()).get().isPresent());
        assertFalse(binaryLookupService.getSchema(TopicName.get(topicName), ByteBuffer.allocate(8).putLong(0).array()).get().isPresent());

        assertFalse(topicHasSchema(topicName));
    }

    @Test
    public void testConcurrentlyDeleteSchema() throws Exception {
        String topic = "persistent://prop/ns-delete-schema/concurrently-delete-schema-test";
        int partitions = 50;
        admin.namespaces().createNamespace("prop/ns-delete-schema", 3);
        admin.topics().createPartitionedTopic(topic, partitions);

        Producer producer = pulsarClient
                .newProducer(Schema.JSON(Schemas.BytesRecord.class))
                .topic(topic)
                .create();
        producer.close();

        CompletableFuture[] asyncFutures = new CompletableFuture[partitions];
        for (int i = 0; i < partitions; i++) {
            asyncFutures[i] = getTopic(TopicName.get(topic).getPartition(i).toString()).get().deleteSchema();
        }

        try {
            // delete the schema concurrently, and wait for the end of all operations
            CompletableFuture.allOf(asyncFutures).join();
        } catch (Exception e) {
            fail("Should not fail");
        }
    }

    /**
     * A topic that has retention policy set to non-0, should not be GCed until it has been inactive for at least the
     * retention time.
     */
    @Test
    public void testGcAndRetentionPolicy() throws Exception {

        // Retain data for at-least 10min
        admin.namespaces().setRetention("prop/ns-abc", new RetentionPolicies(10, 10));

        // 1. Simple successful GC
        String topicName = "persistent://prop/ns-abc/topic-10";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        producer.close();

        assertNotNull(pulsar.getBrokerService().getTopicReference(topicName));
        runGC();
        // Should not have been deleted, since we have retention
        assertNotNull(pulsar.getBrokerService().getTopicReference(topicName));

        // Remove retention
        admin.namespaces().setRetention("prop/ns-abc", new RetentionPolicies());
        Thread.sleep(300);

        // 2. Topic is not GCed with live connection
        String subName = "sub1";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();

        runGC();
        assertNotNull(pulsar.getBrokerService().getTopicReference(topicName));

        // 3. Topic with subscription is not GCed even with no connections
        consumer.close();

        runGC();
        assertNotNull(pulsar.getBrokerService().getTopicReference(topicName));

        // 4. Topic can be GCed after unsubscribe
        admin.topics().deleteSubscription(topicName, subName);

        runGC();
        assertFalse(pulsar.getBrokerService().getTopicReference(topicName).isPresent());
    }

    /**
     * A topic that has retention policy set to -1, should not be GCed until it has been inactive for at least the
     * retention time and the data should never be deleted
     */
    @Test
    public void testInfiniteRetentionPolicy() throws Exception {
        // Retain data forever
        admin.namespaces().setRetention("prop/ns-abc", new RetentionPolicies(-1, -1));

        // 1. Simple successful GC
        String topicName = "persistent://prop/ns-abc/topic-10";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        producer.close();

        assertNotNull(pulsar.getBrokerService().getTopicReference(topicName));
        runGC();
        // Should not have been deleted, since we have retention
        assertNotNull(pulsar.getBrokerService().getTopicReference(topicName));

        // Remove retention
        admin.namespaces().setRetention("prop/ns-abc", new RetentionPolicies());
        Thread.sleep(300);

        // 2. Topic is not GCed with live connection
        String subName = "sub1";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();

        runGC();
        assertNotNull(pulsar.getBrokerService().getTopicReference(topicName));

        // 3. Topic with subscription is not GCed even with no connections
        consumer.close();

        runGC();
        assertNotNull(pulsar.getBrokerService().getTopicReference(topicName));

        // 4. Topic can be GCed after unsubscribe
        admin.topics().deleteSubscription(topicName, subName);

        runGC();
        assertFalse(pulsar.getBrokerService().getTopicReference(topicName).isPresent());
    }

    /**
     * Set retention policy in default configuration.
     * It should be effective.
     */
    @Test
    public void testServiceConfigurationRetentionPolicy() throws Exception {
        // set retention policy in service configuration
        pulsar.getConfiguration().setDefaultRetentionSizeInMB(-1);
        pulsar.getConfiguration().setDefaultRetentionTimeInMinutes(-1);

        String namespaceName = "prop/ns-default-retention-policy";
        admin.namespaces().createNamespace(namespaceName);

        // 1. Simple successful GC
        String topicName = "persistent://prop/ns-abc/topic-10";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        producer.close();

        assertTrue(pulsar.getBrokerService().getTopicReference(topicName).isPresent());
        runGC();
        // Should not have been deleted, since we have retention
        assertTrue(pulsar.getBrokerService().getTopicReference(topicName).isPresent());

        // Remove retention
        admin.namespaces().setRetention("prop/ns-abc", new RetentionPolicies());
        Thread.sleep(300);

        // 2. Topic is not GCed with live connection
        String subName = "sub1";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();

        runGC();
        assertTrue(pulsar.getBrokerService().getTopicReference(topicName).isPresent());

        // 3. Topic with subscription is not GCed even with no connections
        consumer.close();

        runGC();
        assertTrue(pulsar.getBrokerService().getTopicReference(topicName).isPresent());

        // 4. Topic can be GCed after unsubscribe
        admin.topics().deleteSubscription(topicName, subName);

        runGC();
        assertFalse(pulsar.getBrokerService().getTopicReference(topicName).isPresent());
    }

    @Test
    public void testMessageExpiry() throws Exception {
        int messageTTLSecs = 1;
        String namespaceName = "prop/expiry-check";

        admin.namespaces().createNamespace(namespaceName);
        admin.namespaces().setNamespaceReplicationClusters(namespaceName, Sets.newHashSet("test"));
        admin.namespaces().setNamespaceMessageTTL(namespaceName, messageTTLSecs);

        final String topicName = "persistent://prop/expiry-check/topic1";
        final String subName = "sub1";
        final int numMsgs = 10;

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        PersistentSubscription subRef = topicRef.getSubscription(subName);

        consumer.close();
        assertFalse(subRef.getDispatcher().isConsumerConnected());

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        for (int i = 0; i < numMsgs; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        rolloverPerIntervalStats();
        assertEquals(subRef.getNumberOfEntriesInBacklog(false), numMsgs);

        Thread.sleep(TimeUnit.SECONDS.toMillis(messageTTLSecs));
        runMessageExpiryCheck();

        // 1. check all messages expired for this unconnected subscription
        assertEquals(subRef.getNumberOfEntriesInBacklog(false), 0);

        // clean-up
        producer.close();
        consumer.close();
        admin.topics().deleteSubscription(topicName, subName);
        admin.topics().delete(topicName);
        admin.namespaces().deleteNamespace(namespaceName);
    }

    @Test
    public void testMessageExpiryWithTopicMessageTTL() throws Exception {
        int namespaceMessageTTLSecs = 10;
        int topicMessageTTLSecs = 2;
        String namespaceName = "prop/expiry-check-2";

        cleanup();
        this.conf.setSystemTopicEnabled(true);
        this.conf.setTopicLevelPoliciesEnabled(true);
        this.conf.setForceDeleteNamespaceAllowed(true);
        setup();

        admin.namespaces().createNamespace(namespaceName);
        admin.namespaces().setNamespaceReplicationClusters(namespaceName, Sets.newHashSet("test"));
        admin.namespaces().setNamespaceMessageTTL(namespaceName, namespaceMessageTTLSecs);

        final String topicName = "persistent://prop/expiry-check-2/topic2";
        final String subName = "sub1";
        final int numMsgs = 10;

        admin.topics().createNonPartitionedTopic(topicName);
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();

        // Set topic level message ttl.
        Thread.sleep(3000);
        admin.topics().setMessageTTL(topicName, topicMessageTTLSecs);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        PersistentSubscription subRef = topicRef.getSubscription(subName);

        consumer.close();
        assertFalse(subRef.getDispatcher().isConsumerConnected());

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        for (int i = 0; i < numMsgs; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        rolloverPerIntervalStats();
        assertEquals(subRef.getNumberOfEntriesInBacklog(false), numMsgs);

        Thread.sleep(TimeUnit.SECONDS.toMillis(topicMessageTTLSecs));
        runMessageExpiryCheck();

        // 1. check all messages expired for this unconnected subscription
        assertEquals(subRef.getNumberOfEntriesInBacklog(false), 0);
        producer.close();

        // Set topic level message ttl.
        Thread.sleep(3000);
        admin.topics().removeMessageTTL(topicName);

        consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();

        topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        subRef = topicRef.getSubscription(subName);

        consumer.close();
        assertFalse(subRef.getDispatcher().isConsumerConnected());

        producer = pulsarClient.newProducer()
                .topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        for (int i = 0; i < numMsgs; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Thread.sleep(TimeUnit.SECONDS.toMillis(topicMessageTTLSecs));
        rolloverPerIntervalStats();
        assertEquals(subRef.getNumberOfEntriesInBacklog(false), numMsgs);

        Thread.sleep(TimeUnit.SECONDS.toMillis(namespaceMessageTTLSecs - topicMessageTTLSecs));
        runMessageExpiryCheck();

        // 1. check all messages expired for this unconnected subscription
        assertEquals(subRef.getNumberOfEntriesInBacklog(false), 0);

        // clean-up
        try {
            producer.close();
            consumer.close();
            admin.topics().deleteSubscription(topicName, subName);
            admin.topics().delete(topicName);
            admin.namespaces().deleteNamespace(namespaceName, true);
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 500);
        }
    }

    @Test
    public void testMessageExpiryWithFewExpiredBacklog() throws Exception {
        int messageTTLSecs = 10;
        String namespaceName = "prop/expiry-check-1";

        admin.namespaces().createNamespace(namespaceName);
        admin.namespaces().setNamespaceReplicationClusters(namespaceName, Sets.newHashSet("test"));
        admin.namespaces().setNamespaceMessageTTL(namespaceName, messageTTLSecs);

        final String topicName = "persistent://prop/expiry-check-1/topic1";
        final String subName = "sub1";
        final int numMsgs = 10;

        pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        PersistentSubscription subRef = topicRef.getSubscription(subName);

        assertTrue(subRef.getDispatcher().isConsumerConnected());

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        for (int i = 0; i < numMsgs; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        rolloverPerIntervalStats();
        assertEquals(subRef.getNumberOfEntriesInBacklog(false), numMsgs);

        Thread.sleep(TimeUnit.SECONDS.toMillis(messageTTLSecs));
        runMessageExpiryCheck();

        assertEquals(subRef.getNumberOfEntriesInBacklog(false), numMsgs);

        Thread.sleep(TimeUnit.SECONDS.toMillis(messageTTLSecs / 2));
        runMessageExpiryCheck();

        assertEquals(subRef.getNumberOfEntriesInBacklog(false), 0);
    }

    @Test
    public void testSubscriptionTypeTransitions() throws Exception {
        final String topicName = "persistent://prop/ns-abc/shared-topic2";
        final String subName = "sub2";

        Consumer<byte[]> consumer1 = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .subscriptionType(SubscriptionType.Exclusive).subscribe();
        Consumer<byte[]> consumer2 = null;
        Consumer<byte[]> consumer3 = null;

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        PersistentSubscription subRef = topicRef.getSubscription(subName);

        // 1. shared consumer on an exclusive sub fails
        try {
            PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
            closeables.add(pulsarClient);
            consumer2 = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscriptionType(SubscriptionType.Shared).subscribe();
            fail("should have failed");
        } catch (PulsarClientException e) {
            assertTrue(e.getMessage().contains("Subscription is of different type"));
        }

        // 2. failover consumer on an exclusive sub fails
        try {
            PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
            closeables.add(pulsarClient);
            consumer3 = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscriptionType(SubscriptionType.Failover).subscribe();
            fail("should have failed");
        } catch (PulsarClientException e) {
            assertTrue(e.getMessage().contains("Subscription is of different type"));
        }

        // 3. disconnected sub can be converted in shared
        consumer1.close();
        try {
            PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
            closeables.add(pulsarClient);
            consumer2 = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscriptionType(SubscriptionType.Shared).subscribe();
            assertEquals(subRef.getDispatcher().getType(), SubType.Shared);
        } catch (PulsarClientException e) {
            fail("should not fail");
        }

        // 4. exclusive fails on shared sub
        try {
            PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
            closeables.add(pulsarClient);
            consumer1 = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscriptionType(SubscriptionType.Exclusive).subscribe();
            fail("should have failed");
        } catch (PulsarClientException e) {
            assertTrue(e.getMessage().contains("Subscription is of different type"));
        }

        // 5. disconnected sub can be converted in failover
        consumer2.close();
        try {
            PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
            closeables.add(pulsarClient);
            consumer3 = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscriptionType(SubscriptionType.Failover).subscribe();
            assertEquals(subRef.getDispatcher().getType(), SubType.Failover);
        } catch (PulsarClientException e) {
            fail("should not fail");
        }

        // 5. exclusive consumer can connect after failover disconnects
        consumer3.close();
        try {
            PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
            closeables.add(pulsarClient);
            consumer1 = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscriptionType(SubscriptionType.Exclusive).subscribe();
            assertEquals(subRef.getDispatcher().getType(), SubType.Exclusive);
        } catch (PulsarClientException e) {
            fail("should not fail");
        }

        consumer1.close();
        admin.topics().delete(topicName);
    }

    @Test
    public void testReceiveWithTimeout() throws Exception {
        final String topicName = "persistent://prop/ns-abc/topic-receive-timeout";
        final String subName = "sub";

        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subName).receiverQueueSize(1000).subscribe();
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        assertEquals(consumer.getAvailablePermits(), 0);

        Message<byte[]> msg = consumer.receive(10, TimeUnit.MILLISECONDS);
        assertNull(msg);
        assertEquals(consumer.getAvailablePermits(), 0);

        producer.send("test".getBytes());
        Thread.sleep(100);

        assertEquals(consumer.getAvailablePermits(), 0);

        msg = consumer.receive(10, TimeUnit.MILLISECONDS);
        assertNotNull(msg);
        assertEquals(consumer.getAvailablePermits(), 1);

        msg = consumer.receive(10, TimeUnit.MILLISECONDS);
        assertNull(msg);
        assertEquals(consumer.getAvailablePermits(), 1);
    }

    @Test
    public void testProducerReturnedMessageId() throws Exception {
        final String topicName = "persistent://prop/ns-abc/topic-xyz";

        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) topicRef.getManagedLedger();
        long ledgerId = managedLedger.getLedgersInfoAsList().get(0).getLedgerId();

        // 2. producer publish messages
        final int SyncMessages = 10;
        for (int i = 0; i < SyncMessages; i++) {
            String message = "my-message-" + i;
            MessageId receivedMessageId = producer.send(message.getBytes());

            assertEquals(receivedMessageId, new MessageIdImpl(ledgerId, i, -1));
        }

        // 3. producer publish messages async
        final int AsyncMessages = 10;
        final CountDownLatch counter = new CountDownLatch(AsyncMessages);

        for (int i = SyncMessages; i < (SyncMessages + AsyncMessages); i++) {
            String content = "my-message-" + i;
            final int index = i;

            producer.sendAsync(content.getBytes()).thenAccept((msgId) -> {
                assertEquals(msgId, new MessageIdImpl(ledgerId, index, -1));
                counter.countDown();
            }).exceptionally((ex) -> {
                return null;
            });
        }

        counter.await();

        // 4. producer disconnect
        producer.close();
    }

    @Test
    public void testProducerQueueFullBlocking() throws Exception {
        final String topicName = "persistent://prop/ns-abc/topic-xyzx";
        final int messages = 10;

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(brokerUrl.toString()).build();

        // 1. Producer connect
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) client.newProducer()
            .topic(topicName)
            .maxPendingMessages(messages)
            .blockIfQueueFull(true)
            .sendTimeout(1, TimeUnit.SECONDS)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // 2. Stop broker
        super.internalCleanup();

        // 2. producer publish messages
        long startTime = System.nanoTime();
        for (int i = 0; i < messages; i++) {
            // Should never block
            producer.sendAsync("msg".getBytes());
        }

        // Verify thread was not blocked
        long delayNs = System.nanoTime() - startTime;
        assertTrue(delayNs < TimeUnit.SECONDS.toNanos(1));
        assertEquals(producer.getPendingQueueSize(), messages);

        // Next send operation must block, until all the messages in the queue expire
        startTime = System.nanoTime();
        producer.sendAsync("msg".getBytes());
        delayNs = System.nanoTime() - startTime;
        assertTrue(delayNs > TimeUnit.MILLISECONDS.toNanos(500));
        assertTrue(delayNs < TimeUnit.MILLISECONDS.toNanos(1500));
        assertEquals(producer.getPendingQueueSize(), 1);

        // 4. producer disconnect
        producer.close();

        // 5. Restart broker
        setup();
    }

    @Test
    public void testProducerQueueFullNonBlocking() throws Exception {
        final String topicName = "persistent://prop/ns-abc/topic-xyzx";
        final int messages = 10;

        // 1. Producer connect
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(brokerUrl.toString()).build();
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) client.newProducer()
            .topic(topicName)
            .maxPendingMessages(messages)
            .blockIfQueueFull(false)
            .sendTimeout(1, TimeUnit.SECONDS)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // 2. Stop broker
        super.internalCleanup();

        // 2. producer publish messages
        long startTime = System.nanoTime();
        for (int i = 0; i < messages; i++) {
            // Should never block
            producer.sendAsync("msg".getBytes());
        }

        // Verify thread was not blocked
        long delayNs = System.nanoTime() - startTime;
        assertTrue(delayNs < TimeUnit.SECONDS.toNanos(1));
        assertEquals(producer.getPendingQueueSize(), messages);

        // Next send operation must fail and not block
        startTime = System.nanoTime();
        try {
            producer.send("msg".getBytes());
            fail("Send should have failed");
        } catch (PulsarClientException.ProducerQueueIsFullError e) {
            // Expected
        }
        delayNs = System.nanoTime() - startTime;
        assertTrue(delayNs < TimeUnit.SECONDS.toNanos(1));
        assertEquals(producer.getPendingQueueSize(), messages);

        // 4. producer disconnect
        producer.close();

        // 5. Restart broker
        setup();
    }

    @Test
    public void testDeleteTopics() throws Exception {
        BrokerService brokerService = pulsar.getBrokerService();

        // 1. producers connect
        Producer<byte[]> producer1 = pulsarClient.newProducer()
            .topic("persistent://prop/ns-abc/topic-1")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        /* Producer<byte[]> producer2 = */ pulsarClient.newProducer()
            .topic("persistent://prop/ns-abc/topic-2")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        brokerService.updateRates();

        Map<String, NamespaceBundleStats> bundleStatsMap = brokerService.getBundleStats();
        assertEquals(bundleStatsMap.size(), 1);
        NamespaceBundleStats bundleStats = bundleStatsMap.get("prop/ns-abc/0x00000000_0xffffffff");
        assertNotNull(bundleStats);

        producer1.close();
        admin.topics().delete("persistent://prop/ns-abc/topic-1");

        brokerService.updateRates();

        bundleStatsMap = brokerService.getBundleStats();
        assertEquals(bundleStatsMap.size(), 1);
        bundleStats = bundleStatsMap.get("prop/ns-abc/0x00000000_0xffffffff");
        assertNotNull(bundleStats);

        // // Delete 2nd topic as well
        // producer2.close();
        // admin.topics().delete("persistent://prop/ns-abc/topic-2");
        //
        // brokerService.updateRates();
        //
        // bundleStatsMap = brokerService.getBundleStats();
        // assertEquals(bundleStatsMap.size(), 0);
    }

    @DataProvider(name = "codec")
    public Object[][] codecProvider() {
        return new Object[][] { { CompressionType.NONE }, { CompressionType.LZ4 }, { CompressionType.ZLIB }, };
    }

    @Test(dataProvider = "codec")
    public void testCompression(CompressionType compressionType) throws Exception {
        final String topicName = "persistent://prop/ns-abc/topic0" + compressionType;

        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .compressionType(compressionType)
            .create();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-sub").subscribe();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        // 2. producer publish messages
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        for (int i = 0; i < 10; i++) {
            Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            assertEquals(msg.getData(), ("my-message-" + i).getBytes());
        }

        // 3. producer disconnect
        producer.close();
        consumer.close();
    }

    @Test
    public void testBrokerTopicStats() throws Exception {

        BrokerService brokerService = this.pulsar.getBrokerService();
        Field field = BrokerService.class.getDeclaredField("statsUpdater");
        field.setAccessible(true);
        ScheduledExecutorService statsUpdater = (ScheduledExecutorService) field.get(brokerService);
        // disable statsUpdate to calculate rates explicitly
        statsUpdater.shutdownNow();

        final String namespace = "prop/ns-abc";
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic("persistent://" + namespace + "/topic0")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        // 1. producer publish messages
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        Metrics metric = null;

        // sleep 1 sec to caclulate metrics per second
        Thread.sleep(1000);
        brokerService.updateRates();
        List<Metrics> metrics = brokerService.getTopicMetrics();
        for (Metrics value : metrics) {
            if (value.getDimension("namespace").equalsIgnoreCase(namespace)) {
                metric = value;
                break;
            }
        }
        assertNotNull(metric);
        double msgInRate = (double) metrics.get(0).getMetrics().get("brk_in_rate");
        // rate should be calculated and no must be > 0 as we have produced 10 msgs so far
        assertTrue(msgInRate > 0);
    }

    @Test(groups = "quarantine")
    public void testBrokerConnectionStats() throws Exception {

        BrokerService brokerService = this.pulsar.getBrokerService();

        final String namespace = "prop/ns-abc";
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://" + namespace + "/topic0")
                .create();
        Map<String, Object> map = null;

        brokerService.updateRates();
        List<Metrics> metrics = brokerService.getTopicMetrics();
        for (int i = 0; i < metrics.size(); i++) {
            if (metrics.get(i).getDimensions().containsValue("broker_connection")) {
                map = metrics.get(i).getMetrics();
                break;
            }
        }
        assertNotNull(map);
        assertEquals((long) map.get("brk_connection_created_total_count"), 1);
        assertEquals((long) map.get("brk_active_connections"), 1);
        assertEquals((long) map.get("brk_connection_closed_total_count"), 0);
        assertEquals((long) map.get("brk_connection_create_success_count"), 1);
        assertEquals((long) map.get("brk_connection_create_fail_count"), 0);

        producer.close();
        pulsarClient.close();

        Awaitility.await().until(() -> {
            brokerService.updateRates();
            List<Metrics> closeMetrics = brokerService.getTopicMetrics();
            Map<String, Object> closeMap = null;
            for (int i = 0; i < closeMetrics.size(); i++) {
                if (closeMetrics.get(i).getDimensions().containsValue("broker_connection")) {
                    closeMap = closeMetrics.get(i).getMetrics();
                    break;
                }
            }

            if (closeMap != null && (long) closeMap.get("brk_connection_created_total_count") == 1
                    && (long) closeMap.get("brk_active_connections") == 0
                    && (long) closeMap.get("brk_connection_closed_total_count") == 1
                    && (long) closeMap.get("brk_connection_create_fail_count") == 0
                    && (long) closeMap.get("brk_connection_create_success_count") == 1) {
                return true;
            } else {
                return false;
            }
        });

        pulsar.getConfiguration().setAuthenticationEnabled(true);
        if (pulsarClient != null) {
            pulsarClient.shutdown();
        }
        pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl.toString())
                .operationTimeout(1, TimeUnit.MILLISECONDS).build();

        try {
            pulsarClient.newProducer()
                    .topic("persistent://" + namespace + "/topic0")
                    .create();
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof PulsarClientException.AuthenticationException);
        }

        brokerService.updateRates();
        metrics = brokerService.getTopicMetrics();
        for (int i = 0; i < metrics.size(); i++) {
            if (metrics.get(i).getDimensions().containsValue("broker_connection")) {
                map = metrics.get(i).getMetrics();
                break;
            }
        }
        assertNotNull(map);
        assertEquals((long) map.get("brk_connection_created_total_count"), 2);
        assertEquals((long) map.get("brk_active_connections"), 0);
        assertEquals((long) map.get("brk_connection_closed_total_count") , 2);
        assertEquals((long) map.get("brk_connection_create_success_count"), 1);
        assertEquals((long) map.get("brk_connection_create_fail_count") , 1);
    }

    @Test
    public void testPayloadCorruptionDetection() throws Exception {
        final String topicName = "persistent://prop/ns-abc/topic1";

        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-sub").subscribe();

        CompletableFuture<MessageId> future1 = producer.newMessage().value("message-1".getBytes()).sendAsync();

        // Stop the broker, and publishes messages. Messages are accumulated in the producer queue and they're checksums
        // would have already been computed. If we change the message content at that point, it should result in a
        // checksum validation error
        stopBroker();


        byte[] a2 = "message-2".getBytes();
        TypedMessageBuilder<byte[]> msg2 = producer.newMessage().value(a2);


        CompletableFuture<MessageId> future2 = msg2.sendAsync();

        // corrupt the message, new content would be 'message-3'
        ((TypedMessageBuilderImpl<byte[]>) msg2).getContent().put(a2.length - 1, (byte) '3');

        // Restart the broker to have the messages published
        startBroker();

        future1.get();

        try {
            future2.get();
            fail("since we corrupted the message, it should be rejected by the broker");
        } catch (Exception e) {
            // ok
        }

        // We should only receive msg1
        Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(new String(msg.getData()), "message-1");

        while ((msg = consumer.receive(1, TimeUnit.SECONDS)) != null) {
            assertEquals(new String(msg.getData()), "message-1");
        }
    }

    /**
     * Verify: Broker should not replay already acknowledged messages again and should clear them from messageReplay
     * bucket
     *
     * 1. produce messages 2. consume messages and ack all except 1 msg 3. Verification: should replay only 1 unacked
     * message
     */
    @Test
    public void testMessageRedelivery() throws Exception {
        final String topicName = "persistent://prop/ns-abc/topic2";
        final String subName = "sub2";

        Message<String> msg;
        List<Message<String>> unackedMessages = new ArrayList<>();
        int totalMessages = 20;

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        // (1) Produce messages
        for (int i = 0; i < totalMessages; i++) {
            producer.send("my-message-" + i);
        }

        // (2) Consume and only ack last 10 messages
        for (int i = 0; i < totalMessages; i++) {
            msg = consumer.receive();
            if (i >= 10) {
                unackedMessages.add(msg);
            } else {
                consumer.acknowledge(msg);
            }
        }

        consumer.redeliverUnacknowledgedMessages();

        for (int i = 0; i < 10; i++) {
            // Verify: msg [L:0] must be redelivered
            try {
                final Message<String> redeliveredMsg = consumer.receive(1, TimeUnit.SECONDS);
                unackedMessages.removeIf(unackedMessage -> unackedMessage.getValue().equals(redeliveredMsg.getValue()));
            } catch (Exception e) {
                fail("msg should be redelivered ", e);
            }
        }
        // Make sure that first 10 messages that we didn't acknowledge get redelivered.
        assertEquals(unackedMessages.size(), 0);

        // Verify no other messages are redelivered
        msg = consumer.receive(100, TimeUnit.MILLISECONDS);
        assertNull(msg);

        consumer.close();
        producer.close();
    }

    /**
     * Verify: 1. Broker should not replay already acknowledged messages 2. Dispatcher should not stuck while
     * dispatching new messages due to previous-replay of invalid/already-acked messages
     *
     * @throws Exception
     */
    @Test
    public void testMessageReplay() throws Exception {

        final String topicName = "persistent://prop/ns-abc/topic2";
        final String subName = "sub2";

        Message<byte[]> msg;
        int totalMessages = 10;
        int replayIndex = totalMessages / 2;

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared).receiverQueueSize(1).subscribe();
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);
        PersistentSubscription subRef = topicRef.getSubscription(subName);
        PersistentDispatcherMultipleConsumers dispatcher = (PersistentDispatcherMultipleConsumers) subRef
                .getDispatcher();
        Field redeliveryMessagesField = PersistentDispatcherMultipleConsumers.class
                .getDeclaredField("redeliveryMessages");
        redeliveryMessagesField.setAccessible(true);
        MessageRedeliveryController redeliveryMessages = new MessageRedeliveryController(true);

        assertNotNull(subRef);

        // (1) Produce messages
        for (int i = 0; i < totalMessages; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        MessageIdImpl firstAckedMsg = null;
        // (2) Consume and ack messages except first message
        for (int i = 0; i < totalMessages; i++) {
            msg = consumer.receive();
            consumer.acknowledge(msg);
            MessageIdImpl msgId = (MessageIdImpl) msg.getMessageId();
            if (i == 0) {
                firstAckedMsg = msgId;
            }
            if (i < replayIndex) {
                // (3) accumulate acked messages for replay
                redeliveryMessages.add(msgId.getLedgerId(), msgId.getEntryId());
            }
        }

        // (4) redelivery : should redeliver only unacked messages
        Thread.sleep(1000);

        redeliveryMessagesField.set(dispatcher, redeliveryMessages);
        // (a) redelivery with all acked-message should clear messageReply bucket
        dispatcher.redeliverUnacknowledgedMessages(dispatcher.getConsumers().get(0));
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
            return redeliveryMessages.isEmpty();
        });
        assertTrue(redeliveryMessages.isEmpty());

        // (b) fill messageReplyBucket with already acked entry again: and try to publish new msg and read it
        redeliveryMessages.add(firstAckedMsg.getLedgerId(), firstAckedMsg.getEntryId());
        redeliveryMessagesField.set(dispatcher, redeliveryMessages);
        // send new message
        final String testMsg = "testMsg";
        producer.send(testMsg.getBytes());
        // consumer should be able to receive only new message and not the
        dispatcher.consumerFlow(dispatcher.getConsumers().get(0), 1);
        msg = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(msg);
        assertEquals(msg.getData(), testMsg.getBytes());

        consumer.close();
        producer.close();
    }

    @Test
    public void testCreateProducerWithSameName() throws Exception {
        String topic = "persistent://prop/ns-abc/testCreateProducerWithSameName";

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
            .topic(topic)
            .producerName("test-producer-a")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition);
        Producer<byte[]> p1 = producerBuilder.create();

        try {
            producerBuilder.create();
            fail("Should have thrown ProducerBusyException");
        } catch (ProducerBusyException e) {
            // Expected
        }

        p1.close();

        // Now p2 should succeed
        Producer<byte[]> p2 = producerBuilder.create();

        p2.close();
    }

    @Test
    public void testGetOrCreateTopic() throws Exception {
        String topicName = "persistent://prop/ns-abc/testGetOrCreateTopic";

        admin.lookups().lookupTopic(topicName);
        Topic topic = pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        assertNotNull(topic);

        Optional<Topic> t = pulsar.getBrokerService().getTopicReference(topicName);
        assertTrue(t.isPresent());
    }

    @Test
    public void testGetTopicIfExists() throws Exception {
        String topicName = "persistent://prop/ns-abc/testGetTopicIfExists";
        admin.lookups().lookupTopic(topicName);
        Optional<Topic> topic = pulsar.getBrokerService().getTopicIfExists(topicName).join();
        assertFalse(topic.isPresent());

        Optional<Topic> t = pulsar.getBrokerService().getTopicReference(topicName);
        assertFalse(t.isPresent());
    }

    @Test
    public void testWithEventTime() throws Exception {
        final String topicName = "prop/ns-abc/topic-event-time";
        final String subName = "sub";

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionName(subName)
                .subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();

        producer.newMessage().value("test").eventTime(5).send();
        Message<String> msg = consumer.receive();
        assertNotNull(msg);
        assertEquals(msg.getValue(), "test");
        assertEquals(msg.getEventTime(), 5);
    }

    @Test
    public void testProducerBusy() throws Exception {
        final String topicName = "prop/ns-abc/producer-busy-" + System.nanoTime();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .producerName("xxx")
                .create();

        assertEquals(admin.topics().getStats(topicName).getPublishers().size(), 1);

        for (int i =0; i < 5; i++) {
            try {
                pulsarClient.newProducer(Schema.STRING)
                        .topic(topicName)
                        .producerName("xxx")
                        .create();
                fail("Should have failed");
            } catch (ProducerBusyException e) {
                // Expected
            }

            assertEquals(admin.topics().getStats(topicName).getPublishers().size(), 1);
        }

        // Try from different connection
        @Cleanup
        PulsarClient client2 = PulsarClient.builder()
                .serviceUrl(getPulsar().getBrokerServiceUrl())
                .build();

        try {
            client2.newProducer(Schema.STRING)
                    .topic(topicName)
                    .producerName("xxx")
                    .create();
            fail("Should have failed");
        } catch (ProducerBusyException e) {
            // Expected
        }

        assertEquals(admin.topics().getStats(topicName).getPublishers().size(), 1);
    }
}
