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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAllowedException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderListener;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TopicTerminationTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private final String topicName = "persistent://prop/ns-abc/topic0";

    @Test
    public void testSimpleTermination() throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        /* MessageId msgId1 = */producer.send("test-msg-1".getBytes());
        /* MessageId msgId2 = */producer.send("test-msg-2".getBytes());
        MessageId msgId3 = producer.send("test-msg-3".getBytes());

        MessageId lastMessageId = admin.topics().terminateTopicAsync(topicName).get();
        assertEquals(lastMessageId, msgId3);

        try {
            producer.send("test-msg-4".getBytes());
            fail("Should have thrown exception");
        } catch (PulsarClientException.TopicTerminatedException e) {
            // Expected
        }
    }

    @Test(groups = "broker")
    public void testCreateProducerOnTerminatedTopic() throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        /* MessageId msgId1 = */producer.send("test-msg-1".getBytes());
        /* MessageId msgId2 = */producer.send("test-msg-2".getBytes());
        MessageId msgId3 = producer.send("test-msg-3".getBytes());

        MessageId lastMessageId = admin.topics().terminateTopicAsync(topicName).get();
        assertEquals(lastMessageId, msgId3);

        try {
            pulsarClient.newProducer().topic(topicName).create();
            fail("Should have thrown exception");
        } catch (PulsarClientException.TopicTerminatedException e) {
            // Expected
        }
    }

    @Test(timeOut = 20000)
    public void testTerminateWhilePublishing() throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        CyclicBarrier barrier = new CyclicBarrier(2);
        List<CompletableFuture<MessageId>> futures = new ArrayList<>();
        Thread t = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                ///
            }

            for (int i = 0; i < 1000; i++) {
                futures.add(producer.sendAsync("test".getBytes()));
            }
        });
        t.start();

        barrier.await();

        admin.topics().terminateTopicAsync(topicName).get();

        t.join();

        // Ensure all futures are done. Also, once a future is failed, everything
        // else after that should have failed
        boolean alreadyFailed = false;

        try {
            FutureUtil.waitForAll(futures).get();
        } catch (Exception e) {
            // Ignore for now, check is below
        }

        for (int i = 0; i < 1000; i++) {
            assertTrue(futures.get(i).isDone());
            if (alreadyFailed) {
                assertTrue(futures.get(i).isCompletedExceptionally());
            }

            alreadyFailed = futures.get(i).isCompletedExceptionally();
        }
    }

    @Test(groups = "broker")
    public void testDoubleTerminate() throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        /* MessageId msgId1 = */producer.send("test-msg-1".getBytes());
        /* MessageId msgId2 = */producer.send("test-msg-2".getBytes());
        MessageId msgId3 = producer.send("test-msg-3".getBytes());

        MessageId lastMessageId = admin.topics().terminateTopicAsync(topicName).get();
        assertEquals(lastMessageId, msgId3);

        // Terminate it again
        lastMessageId = admin.topics().terminateTopicAsync(topicName).get();
        assertEquals(lastMessageId, msgId3);
    }

    @Test(groups = "broker")
    public void testTerminatePartitionedTopic() throws Exception {
        admin.topics().createPartitionedTopic(topicName, 4);

        try {
            admin.topics().terminateTopicAsync(topicName).get();
            fail("Should have failed");
        } catch (ExecutionException ee) {
            assertEquals(ee.getCause().getClass(), NotAllowedException.class);
        }
    }

    @Test(timeOut = 20000)
    public void testSimpleTerminationConsumer() throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        org.apache.pulsar.client.api.Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-sub").subscribe();

        MessageId msgId1 = producer.send("test-msg-1".getBytes());
        MessageId msgId2 = producer.send("test-msg-2".getBytes());

        Message<byte[]> msg1 = consumer.receive();
        assertEquals(msg1.getMessageId(), msgId1);
        consumer.acknowledge(msg1);

        MessageId msgId3 = producer.send("test-msg-3".getBytes());

        assertFalse(consumer.hasReachedEndOfTopic());

        MessageId lastMessageId = admin.topics().terminateTopicAsync(topicName).get();
        assertEquals(lastMessageId, msgId3);

        Message<byte[]> msg2 = consumer.receive();
        assertEquals(msg2.getMessageId(), msgId2);
        consumer.acknowledge(msg2);

        Message<byte[]> msg3 = consumer.receive();
        assertEquals(msg3.getMessageId(), msgId3);
        consumer.acknowledge(msg3);

        Message<byte[]> msg4 = consumer.receive(100, TimeUnit.MILLISECONDS);
        assertNull(msg4);

        Awaitility.await().untilAsserted(() -> assertTrue(consumer.hasReachedEndOfTopic()));
    }

    @Test(timeOut = 20000)
    public void testSimpleTerminationMessageListener() throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        CountDownLatch latch = new CountDownLatch(1);

        org.apache.pulsar.client.api.Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-sub").messageListener(new MessageListener<byte[]>() {

                    @Override
                    public void received(Consumer<byte[]> consumer, Message<byte[]> msg) {
                        // do nothing
                    }

                    @Override
                    public void reachedEndOfTopic(Consumer<byte[]> consumer) {
                        latch.countDown();
                        assertTrue(consumer.hasReachedEndOfTopic());
                    }
                }).subscribe();

        /* MessageId msgId1 = */ producer.send("test-msg-1".getBytes());
        /* MessageId msgId2 = */ producer.send("test-msg-2".getBytes());
        MessageId msgId3 = producer.send("test-msg-3".getBytes());

        consumer.acknowledgeCumulative(msgId3);

        Awaitility.await().untilAsserted(() -> assertFalse(consumer.hasReachedEndOfTopic()));

        MessageId lastMessageId = admin.topics().terminateTopicAsync(topicName).get();
        assertEquals(lastMessageId, msgId3);

        assertTrue(latch.await(3, TimeUnit.SECONDS));
        assertTrue(consumer.hasReachedEndOfTopic());
    }

    @Test(timeOut = 20000)
    public void testSimpleTerminationReader() throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        MessageId msgId1 = producer.send("test-msg-1".getBytes());
        MessageId msgId2 = producer.send("test-msg-2".getBytes());
        MessageId msgId3 = producer.send("test-msg-3".getBytes());

        MessageId lastMessageId = admin.topics().terminateTopicAsync(topicName).get();
        assertEquals(lastMessageId, msgId3);

        Reader<byte[]> reader = pulsarClient.newReader().topic(topicName).startMessageId(MessageId.earliest).create();

        Message<byte[]> msg1 = reader.readNext();
        assertEquals(msg1.getMessageId(), msgId1);

        Message<byte[]> msg2 = reader.readNext();
        assertEquals(msg2.getMessageId(), msgId2);

        Message<byte[]> msg3 = reader.readNext();
        assertEquals(msg3.getMessageId(), msgId3);

        Message<byte[]> msg4 = reader.readNext(100, TimeUnit.MILLISECONDS);
        assertNull(msg4);

        Awaitility.await().untilAsserted(() -> assertTrue(reader.hasReachedEndOfTopic()));
    }

    @Test(timeOut = 20000)
    public void testSimpleTerminationReaderListener() throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        CountDownLatch latch = new CountDownLatch(1);

        Reader<byte[]> reader = pulsarClient.newReader().topic(topicName).startMessageId(MessageId.latest)
                .readerListener(new ReaderListener<byte[]>() {
                    @Override
                    public void received(Reader<byte[]> reader, Message<byte[]> msg) {
                        // do nothing
                    }

                    @Override
                    public void reachedEndOfTopic(Reader<byte[]> reader) {
                        latch.countDown();
                        assertTrue(reader.hasReachedEndOfTopic());
                    }
                }).create();

        /* MessageId msgId1 = */ producer.send("test-msg-1".getBytes());
        /* MessageId msgId2 = */ producer.send("test-msg-2".getBytes());
        MessageId msgId3 = producer.send("test-msg-3".getBytes());

        Awaitility.await().untilAsserted(() -> assertFalse(reader.hasReachedEndOfTopic()));

        MessageId lastMessageId = admin.topics().terminateTopicAsync(topicName).get();
        assertEquals(lastMessageId, msgId3);

        assertTrue(latch.await(3, TimeUnit.SECONDS));
        assertTrue(reader.hasReachedEndOfTopic());
    }

    @Test(timeOut = 20000)
    public void testSubscribeOnTerminatedTopic() throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        /* MessageId msgId1 = */ producer.send("test-msg-1".getBytes());
        MessageId msgId2 = producer.send("test-msg-2".getBytes());

        MessageId lastMessageId = admin.topics().terminateTopicAsync(topicName).get();
        assertEquals(lastMessageId, msgId2);

        org.apache.pulsar.client.api.Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-sub").subscribe();

        Awaitility.await().untilAsserted(() -> assertTrue(consumer.hasReachedEndOfTopic()));
    }

    @Test(timeOut = 20000)
    public void testSubscribeOnTerminatedTopicWithNoMessages() throws Exception {
        pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        admin.topics().terminateTopicAsync(topicName).get();

        org.apache.pulsar.client.api.Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-sub").subscribe();

        Awaitility.await().untilAsserted(() -> assertTrue(consumer.hasReachedEndOfTopic()));
    }
}
