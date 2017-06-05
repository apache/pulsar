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

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.EntryCacheImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.collections.ConcurrentLongPairSet;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

/**
 */
@Test
public class PersistentTopicE2ETest extends BrokerTestBase {
    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testSimpleProducerEvents() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/topic0";

        // 1. producer connect
        Producer producer = pulsarClient.createProducer(topicName);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);
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
        final String topicName = "persistent://prop/use/ns-abc/topic1";
        final String subName = "sub1";
        final int numMsgs = 10;

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);

        // 1. client connect
        Consumer consumer = pulsarClient.subscribe(topicName, subName, conf);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);
        PersistentSubscription subRef = topicRef.getSubscription(subName);

        assertNotNull(topicRef);
        assertNotNull(subRef);
        assertTrue(subRef.getDispatcher().isConsumerConnected());

        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        assertEquals(getAvailablePermits(subRef), 1000 /* default */);

        Producer producer = pulsarClient.createProducer(topicName);
        for (int i = 0; i < numMsgs * 2; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        assertTrue(subRef.getDispatcher().isConsumerConnected());
        rolloverPerIntervalStats();
        assertEquals(subRef.getNumberOfEntriesInBacklog(), numMsgs * 2);

        // 2. messages pushed before client receive
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        assertEquals(getAvailablePermits(subRef), 1000 - numMsgs * 2);

        Message msg = null;
        for (int i = 0; i < numMsgs; i++) {
            msg = consumer.receive();
            // 3. in-order message delivery
            assertEquals(new String(msg.getData()), "my-message-" + i);
            consumer.acknowledge(msg);
        }

        rolloverPerIntervalStats();

        // 4. messages deleted on individual acks
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        assertEquals(subRef.getNumberOfEntriesInBacklog(), numMsgs);

        for (int i = 0; i < numMsgs; i++) {
            msg = consumer.receive();
            if (i == numMsgs - 1) {
                consumer.acknowledgeCumulative(msg);
            }
        }

        rolloverPerIntervalStats();

        // 5. messages deleted on cumulative acks
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        assertEquals(subRef.getNumberOfEntriesInBacklog(), 0);

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
        final String topicName = "persistent://prop/use/ns-abc/topic2";
        final String subName = "sub2";

        Message msg;
        int recvQueueSize = 4;

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        conf.setReceiverQueueSize(recvQueueSize);

        Consumer consumer = pulsarClient.subscribe(topicName, subName, conf);
        Producer producer = pulsarClient.createProducer(topicName);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);
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
        final String topicName = "persistent://prop/use/ns-abc/topic2";
        final String subName = "sub2";

        Message msg;
        int recvQueueSize = 4;

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        conf.setReceiverQueueSize(recvQueueSize);

        // (1) Create subscription
        Consumer consumer = pulsarClient.subscribe(topicName, subName, conf);
        Producer producer = pulsarClient.createProducer(topicName);

        // (2) Produce Messages
        for (int i = 0; i < recvQueueSize / 2; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            msg = consumer.receive();
            consumer.acknowledge(msg);
        }

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);

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
        // (4.3) Validate: entryCache should have cached messages
        assertTrue(entryCache.getSize() != 0);

        /************* Validation on empty active-cursor **************/
        // (5) Close consumer: which (1)removes activeConsumer and (2)clears the entry-cache
        consumer.close();
        Thread.sleep(1000);
        // (5.1) Validate: active-consumer must be empty
        assertFalse(ledger.getActiveCursors().iterator().hasNext());
        // (5.2) Validate: Entry-cache must be cleared
        assertTrue(entryCache.getSize() == 0);

    }

    // some race conditions needs to be handled
    // disabling the test for now to not block commit jobs
    @Test(enabled = false)
    public void testConcurrentConsumerThreads() throws Exception {
        // test concurrent consumer threads on same consumerId
        final String topicName = "persistent://prop/use/ns-abc/topic3";
        final String subName = "sub3";

        final int recvQueueSize = 100;
        final int numConsumersThreads = 10;

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        conf.setReceiverQueueSize(recvQueueSize);

        ExecutorService executor = Executors.newCachedThreadPool();

        final CyclicBarrier barrier = new CyclicBarrier(numConsumersThreads + 1);
        for (int i = 0; i < numConsumersThreads; i++) {
            executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    barrier.await();

                    Consumer consumer = pulsarClient.subscribe(topicName, subName, conf);
                    for (int i = 0; i < recvQueueSize / numConsumersThreads; i++) {
                        Message msg = consumer.receive();
                        consumer.acknowledge(msg);
                    }
                    return null;
                }
            });
        }

        Producer producer = pulsarClient.createProducer(topicName);
        for (int i = 0; i < recvQueueSize * numConsumersThreads; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        barrier.await();

        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);
        PersistentSubscription subRef = topicRef.getSubscription(subName);

        // 1. cumulatively all threads drain the backlog
        assertEquals(subRef.getNumberOfEntriesInBacklog(), 0);

        // 2. flow control works the same as single consumer single thread
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        assertEquals(getAvailablePermits(subRef), recvQueueSize);
    }

    @Test(enabled = false)
    // TODO: enable this after java client supports graceful close
    public void testGracefulClose() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/topic4";
        final String subName = "sub4";

        Producer producer = pulsarClient.createProducer(topicName);
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);
        assertNotNull(topicRef);

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

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = pulsarClient.subscribe(topicName, subName, conf);

        PersistentSubscription subRef = topicRef.getSubscription(subName);
        assertNotNull(subRef);

        Message msg = null;
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
        final String topicName = "persistent://prop/use/ns-abc/topic5";
        final String subName = "sub5";

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);

        Consumer consumer = pulsarClient.subscribe(topicName, subName, conf);
        Producer producer = pulsarClient.createProducer(topicName);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);
        assertNotNull(topicRef);
        PersistentSubscription subRef = topicRef.getSubscription(subName);
        assertNotNull(subRef);

        Message msg;
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            msg = consumer.receive();
            consumer.acknowledge(msg);
        }

        producer.close();
        consumer.close();

        topicRef.close().get();
        assertNull(pulsar.getBrokerService().getTopicReference(topicName));
    }

    @Test
    public void testSingleClientMultipleSubscriptions() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/topic6";
        final String subName = "sub6";

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);

        pulsarClient.subscribe(topicName, subName, conf);
        pulsarClient.createProducer(topicName);
        try {
            pulsarClient.subscribe(topicName, subName, conf);
            fail("Should have thrown an exception since one consumer is already connected");
        } catch (PulsarClientException cce) {
            Assert.assertTrue(cce.getMessage().contains("Exclusive consumer is already connected"));
        }
    }

    @Test
    public void testMultipleClientsMultipleSubscriptions() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/topic7";
        final String subName = "sub7";

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);

        PulsarClient client1 = PulsarClient.create(brokerUrl.toString());
        PulsarClient client2 = PulsarClient.create(brokerUrl.toString());

        try {
            client1.subscribe(topicName, subName, conf);
            client1.createProducer(topicName);

            client2.createProducer(topicName);

            client2.subscribe(topicName, subName, conf);
            fail("Should have thrown an exception since one consumer is already connected");
        } catch (PulsarClientException cce) {
            Assert.assertTrue(cce.getMessage().contains("Exclusive consumer is already connected"));
        } finally {
            client2.shutdown();
            client1.shutdown();
        }
    }

    @Test
    public void testTopicDeleteWithDisconnectedSubscription() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/topic8";
        final String subName = "sub1";

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);

        // 1. client connect
        Consumer consumer = pulsarClient.subscribe(topicName, subName, conf);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);
        PersistentSubscription subRef = topicRef.getSubscription(subName);

        assertNotNull(topicRef);
        assertNotNull(subRef);
        assertTrue(subRef.getDispatcher().isConsumerConnected());

        // 2. client disconnect
        consumer.close();
        assertFalse(subRef.getDispatcher().isConsumerConnected());

        // 3. delete topic
        admin.persistentTopics().delete(topicName);
        try {
            admin.persistentTopics().getStats(topicName);
        } catch (PulsarAdminException e) {
            // ok
        }
    }

    int getAvailablePermits(PersistentSubscription sub) {
        return sub.getDispatcher().getConsumers().get(0).getAvailablePermits();
    }

    @Test(enabled = false)
    public void testUnloadNamespace() throws Exception {
        String topicName = "persistent://prop/use/ns-abc/topic-9";
        DestinationName destinationName = DestinationName.get(topicName);
        pulsarClient.createProducer(topicName);
        pulsarClient.close();

        assertTrue(pulsar.getBrokerService().getTopicReference(topicName) != null);
        assertTrue(((ManagedLedgerFactoryImpl) pulsar.getManagedLedgerFactory()).getManagedLedgers()
                .containsKey(destinationName.getPersistenceNamingEncoding()));

        admin.namespaces().unload("prop/use/ns-abc");

        int i = 0;
        for (i = 0; i < 30; i++) {
            if (pulsar.getBrokerService().getTopicReference(topicName) == null) {
                break;
            }
            Thread.sleep(1000);
        }
        if (i == 30) {
            fail("The topic reference should be null");
        }

        // ML should have been closed as well
        assertFalse(((ManagedLedgerFactoryImpl) pulsar.getManagedLedgerFactory()).getManagedLedgers()
                .containsKey(destinationName.getPersistenceNamingEncoding()));
    }

    @Test
    public void testGC() throws Exception {
        // 1. Simple successful GC
        String topicName = "persistent://prop/use/ns-abc/topic-10";
        Producer producer = pulsarClient.createProducer(topicName);
        producer.close();

        assertNotNull(pulsar.getBrokerService().getTopicReference(topicName));
        runGC();
        assertNull(pulsar.getBrokerService().getTopicReference(topicName));

        // 2. Topic is not GCed with live connection
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        String subName = "sub1";
        Consumer consumer = pulsarClient.subscribe(topicName, subName, conf);

        runGC();
        assertNotNull(pulsar.getBrokerService().getTopicReference(topicName));

        // 3. Topic with subscription is not GCed even with no connections
        consumer.close();

        runGC();
        assertNotNull(pulsar.getBrokerService().getTopicReference(topicName));

        // 4. Topic can be GCed after unsubscribe
        admin.persistentTopics().deleteSubscription(topicName, subName);

        runGC();
        assertNull(pulsar.getBrokerService().getTopicReference(topicName));
    }

    @Test
    public void testMessageExpiry() throws Exception {
        int messageTTLSecs = 1;
        String namespaceName = "prop/use/expiry-check";

        admin.namespaces().createNamespace(namespaceName);
        admin.namespaces().setNamespaceMessageTTL(namespaceName, messageTTLSecs);

        final String topicName = "persistent://prop/use/expiry-check/topic1";
        final String subName = "sub1";
        final int numMsgs = 10;

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);

        Consumer consumer = pulsarClient.subscribe(topicName, subName, conf);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);
        PersistentSubscription subRef = topicRef.getSubscription(subName);

        consumer.close();
        assertFalse(subRef.getDispatcher().isConsumerConnected());

        Producer producer = pulsarClient.createProducer(topicName);
        for (int i = 0; i < numMsgs; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        rolloverPerIntervalStats();
        assertEquals(subRef.getNumberOfEntriesInBacklog(), numMsgs);

        Thread.sleep(TimeUnit.SECONDS.toMillis(messageTTLSecs));
        runMessageExpiryCheck();

        // 1. check all messages expired for this unconnected subscription
        assertEquals(subRef.getNumberOfEntriesInBacklog(), 0);

        // clean-up
        producer.close();
        consumer.close();
        admin.persistentTopics().deleteSubscription(topicName, subName);
        admin.persistentTopics().delete(topicName);
        admin.namespaces().deleteNamespace(namespaceName);
    }

    @Test
    public void testMessageExpiryWithFewExpiredBacklog() throws Exception {
        int messageTTLSecs = 10;
        String namespaceName = "prop/use/expiry-check-1";

        admin.namespaces().createNamespace(namespaceName);
        admin.namespaces().setNamespaceMessageTTL(namespaceName, messageTTLSecs);

        final String topicName = "persistent://prop/use/expiry-check-1/topic1";
        final String subName = "sub1";
        final int numMsgs = 10;

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);

        pulsarClient.subscribe(topicName, subName, conf);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);
        PersistentSubscription subRef = topicRef.getSubscription(subName);

        assertTrue(subRef.getDispatcher().isConsumerConnected());

        Producer producer = pulsarClient.createProducer(topicName);
        for (int i = 0; i < numMsgs; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        rolloverPerIntervalStats();
        assertEquals(subRef.getNumberOfEntriesInBacklog(), numMsgs);

        Thread.sleep(TimeUnit.SECONDS.toMillis(messageTTLSecs));
        runMessageExpiryCheck();

        assertEquals(subRef.getNumberOfEntriesInBacklog(), numMsgs);

        Thread.sleep(TimeUnit.SECONDS.toMillis(messageTTLSecs / 2));
        runMessageExpiryCheck();

        assertEquals(subRef.getNumberOfEntriesInBacklog(), 0);
    }

    @Test
    public void testSubscriptionTypeTransitions() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/shared-topic2";
        final String subName = "sub2";

        ConsumerConfiguration conf1 = new ConsumerConfiguration();
        conf1.setSubscriptionType(SubscriptionType.Exclusive);

        ConsumerConfiguration conf2 = new ConsumerConfiguration();
        conf2.setSubscriptionType(SubscriptionType.Shared);

        ConsumerConfiguration conf3 = new ConsumerConfiguration();
        conf3.setSubscriptionType(SubscriptionType.Failover);

        Consumer consumer1 = pulsarClient.subscribe(topicName, subName, conf1);
        Consumer consumer2 = null;
        Consumer consumer3 = null;

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);
        PersistentSubscription subRef = topicRef.getSubscription(subName);

        // 1. shared consumer on an exclusive sub fails
        try {
            consumer2 = pulsarClient.subscribe(topicName, subName, conf2);
            fail("should have failed");
        } catch (PulsarClientException e) {
            assertTrue(e.getMessage().contains("Subscription is of different type"));
        }

        // 2. failover consumer on an exclusive sub fails
        try {
            consumer3 = pulsarClient.subscribe(topicName, subName, conf3);
            fail("should have failed");
        } catch (PulsarClientException e) {
            assertTrue(e.getMessage().contains("Subscription is of different type"));
        }

        // 3. disconnected sub can be converted in shared
        consumer1.close();
        try {
            consumer2 = pulsarClient.subscribe(topicName, subName, conf2);
            assertEquals(subRef.getDispatcher().getType(), SubType.Shared);
        } catch (PulsarClientException e) {
            fail("should not fail");
        }

        // 4. exclusive fails on shared sub
        try {
            consumer1 = pulsarClient.subscribe(topicName, subName, conf1);
            fail("should have failed");
        } catch (PulsarClientException e) {
            assertTrue(e.getMessage().contains("Subscription is of different type"));
        }

        // 5. disconnected sub can be converted in failover
        consumer2.close();
        try {
            consumer3 = pulsarClient.subscribe(topicName, subName, conf3);
            assertEquals(subRef.getDispatcher().getType(), SubType.Failover);
        } catch (PulsarClientException e) {
            fail("should not fail");
        }

        // 5. exclusive consumer can connect after failover disconnects
        consumer3.close();
        try {
            consumer1 = pulsarClient.subscribe(topicName, subName, conf1);
            assertEquals(subRef.getDispatcher().getType(), SubType.Exclusive);
        } catch (PulsarClientException e) {
            fail("should not fail");
        }

        consumer1.close();
        admin.persistentTopics().delete(topicName);
    }

    @Test
    public void testReceiveWithTimeout() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/topic-receive-timeout";
        final String subName = "sub";

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        conf.setReceiverQueueSize(1000);

        ConsumerImpl consumer = (ConsumerImpl) pulsarClient.subscribe(topicName, subName, conf);
        Producer producer = pulsarClient.createProducer(topicName);

        assertEquals(consumer.getAvailablePermits(), 0);

        Message msg = consumer.receive(10, TimeUnit.MILLISECONDS);
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
        final String topicName = "persistent://prop/use/ns-abc/topic-xyz";

        // 1. producer connect
        Producer producer = pulsarClient.createProducer(topicName);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);
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
            Message msg = MessageBuilder.create().setContent(content.getBytes()).build();
            final int index = i;

            producer.sendAsync(msg).thenRun(() -> {
                assertEquals(msg.getMessageId(), new MessageIdImpl(ledgerId, index, -1));
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
        final String topicName = "persistent://prop/use/ns-abc/topic-xyzx";
        final int messages = 10;

        PulsarClient client = PulsarClient.create(brokerUrl.toString());

        // 1. Producer connect
        ProducerConfiguration producerConfiguration = new ProducerConfiguration().setMaxPendingMessages(messages)
                .setBlockIfQueueFull(true).setSendTimeout(1, TimeUnit.SECONDS);
        ProducerImpl producer = (ProducerImpl) client.createProducer(topicName, producerConfiguration);

        // 2. Stop broker
        cleanup();

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
        client.close();

        // 5. Restart broker
        setup();
    }

    @Test
    public void testProducerQueueFullNonBlocking() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/topic-xyzx";
        final int messages = 10;

        // 1. Producer connect
        PulsarClient client = PulsarClient.create(brokerUrl.toString());
        ProducerConfiguration producerConfiguration = new ProducerConfiguration().setMaxPendingMessages(messages)
                .setBlockIfQueueFull(false).setSendTimeout(1, TimeUnit.SECONDS);
        ProducerImpl producer = (ProducerImpl) client.createProducer(topicName, producerConfiguration);

        // 2. Stop broker
        cleanup();

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
        client.close();

        // 5. Restart broker
        setup();
    }

    @Test
    public void testDeleteTopics() throws Exception {
        BrokerService brokerService = pulsar.getBrokerService();

        // 1. producers connect
        Producer producer1 = pulsarClient.createProducer("persistent://prop/use/ns-abc/topic-1");
        Producer producer2 = pulsarClient.createProducer("persistent://prop/use/ns-abc/topic-2");

        brokerService.updateRates();

        Map<String, NamespaceBundleStats> bundleStatsMap = brokerService.getBundleStats();
        assertEquals(bundleStatsMap.size(), 1);
        NamespaceBundleStats bundleStats = bundleStatsMap.get("prop/use/ns-abc/0x00000000_0xffffffff");
        assertNotNull(bundleStats);

        producer1.close();
        admin.persistentTopics().delete("persistent://prop/use/ns-abc/topic-1");

        brokerService.updateRates();

        bundleStatsMap = brokerService.getBundleStats();
        assertEquals(bundleStatsMap.size(), 1);
        bundleStats = bundleStatsMap.get("prop/use/ns-abc/0x00000000_0xffffffff");
        assertNotNull(bundleStats);

        // // Delete 2nd topic as well
        // producer2.close();
        // admin.persistentTopics().delete("persistent://prop/use/ns-abc/topic-2");
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
        final String topicName = "persistent://prop/use/ns-abc/topic0" + compressionType;

        // 1. producer connect
        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setCompressionType(compressionType);
        Producer producer = pulsarClient.createProducer(topicName, producerConf);

        Consumer consumer = pulsarClient.subscribe(topicName, "my-sub");

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        // 2. producer publish messages
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        for (int i = 0; i < 10; i++) {
            Message msg = consumer.receive(5, TimeUnit.SECONDS);
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
        statsUpdater.shutdown();

        final String namespace = "prop/use/ns-abc";
        ProducerConfiguration producerConf = new ProducerConfiguration();
        Producer producer = pulsarClient.createProducer("persistent://" + namespace + "/topic0", producerConf);
        // 1. producer publish messages
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        Metrics metric = null;

        // sleep 1 sec to caclulate metrics per second
        Thread.sleep(1000);
        brokerService.updateRates();
        List<Metrics> metrics = brokerService.getDestinationMetrics();
        for (int i = 0; i < metrics.size(); i++) {
            if (metrics.get(i).getDimension("namespace").equalsIgnoreCase(namespace)) {
                metric = metrics.get(i);
                break;
            }
        }
        assertNotNull(metric);
        double msgInRate = (double) metrics.get(0).getMetrics().get("brk_in_rate");
        // rate should be calculated and no must be > 0 as we have produced 10 msgs so far
        assertTrue(msgInRate > 0);
    }

    @Test
    public void testPayloadCorruptionDetection() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/topic1";

        // 1. producer connect
        Producer producer = pulsarClient.createProducer(topicName);

        Consumer consumer = pulsarClient.subscribe(topicName, "my-sub");

        Message msg1 = MessageBuilder.create().setContent("message-1".getBytes()).build();
        CompletableFuture<MessageId> future1 = producer.sendAsync(msg1);

        // Stop the broker, and publishes messages. Messages are accumulated in the producer queue and they're checksums
        // would have already been computed. If we change the message content at that point, it should result in a
        // checksum validation error
        stopBroker();


        Message msg2 = MessageBuilder.create().setContent("message-2".getBytes()).build();
        CompletableFuture<MessageId> future2 = producer.sendAsync(msg2);

        // Taint msg2
        msg2.getData()[msg2.getData().length - 1] = '3'; // new content would be 'message-3'

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
        Message msg = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(new String(msg.getData()), "message-1");

        while ((msg = consumer.receive(1, TimeUnit.SECONDS)) != null) {
            assertEquals(new String(msg.getData()), "message-1");
        }
    }

    /**
     * Verify: Broker should not replay already acknowledged messages again and should clear them from messageReplay bucket
     *
     * 1. produce messages
     * 2. consume messages and ack all except 1 msg
     * 3. Verification: should replay only 1 unacked message
     */
    @Test()
    public void testMessageRedelivery() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/topic2";
        final String subName = "sub2";

        Message msg;
        int totalMessages = 10;

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Shared);

        Consumer consumer = pulsarClient.subscribe(topicName, subName, conf);
        Producer producer = pulsarClient.createProducer(topicName);

        // (1) Produce messages
        for (int i = 0; i < totalMessages; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        //(2) Consume and ack messages except first message
        Message unAckedMsg = null;
        for (int i = 0; i < totalMessages; i++) {
            msg = consumer.receive();
            if (i == 0) {
                unAckedMsg = msg;
            } else {
                consumer.acknowledge(msg);
            }
        }

        consumer.redeliverUnacknowledgedMessages();

        // Verify: msg [L:0] must be redelivered
        try {
            msg = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(new String(msg.getData()), new String(unAckedMsg.getData()));
        } catch (Exception e) {
            fail("msg should be redelivered ", e);
        }

        // Verify no other messages are redelivered
        msg = consumer.receive(100, TimeUnit.MILLISECONDS);
        assertNull(msg);

        consumer.close();
        producer.close();
    }

    /**
     * Verify: 
     * 1. Broker should not replay already acknowledged messages 
     * 2. Dispatcher should not stuck while dispatching new messages due to previous-replay 
     * of invalid/already-acked messages
     * 
     * @throws Exception
     */
    @Test
    public void testMessageReplay() throws Exception {

        final String topicName = "persistent://prop/use/ns-abc/topic2";
        final String subName = "sub2";

        Message msg;
        int totalMessages = 10;
        int replayIndex = totalMessages / 2;

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Shared);
        conf.setReceiverQueueSize(1);

        Consumer consumer = pulsarClient.subscribe(topicName, subName, conf);
        Producer producer = pulsarClient.createProducer(topicName);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);
        assertNotNull(topicRef);
        PersistentSubscription subRef = topicRef.getSubscription(subName);
        PersistentDispatcherMultipleConsumers dispatcher = (PersistentDispatcherMultipleConsumers) subRef
                .getDispatcher();
        Field replayMap = PersistentDispatcherMultipleConsumers.class.getDeclaredField("messagesToReplay");
        replayMap.setAccessible(true);
        ConcurrentLongPairSet messagesToReplay = new ConcurrentLongPairSet(64, 1);

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
                messagesToReplay.add(msgId.getLedgerId(), msgId.getEntryId());
            }
        }

        // (4) redelivery : should redeliver only unacked messages
        Thread.sleep(1000);

        replayMap.set(dispatcher, messagesToReplay);
        // (a) redelivery with all acked-message should clear messageReply bucket
        dispatcher.redeliverUnacknowledgedMessages(dispatcher.getConsumers().get(0));
        assertEquals(messagesToReplay.size(), 0);

        // (b) fill messageReplyBucket with already acked entry again: and try to publish new msg and read it
        messagesToReplay.add(firstAckedMsg.getLedgerId(), firstAckedMsg.getEntryId());
        replayMap.set(dispatcher, messagesToReplay);
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
    
}
