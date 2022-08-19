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
package org.apache.pulsar.client.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import io.netty.util.Timeout;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.client.impl.PartitionedProducerImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

import io.netty.util.concurrent.DefaultThreadFactory;

@Test(groups = "broker-api")
public class PartitionedProducerConsumerTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(PartitionedProducerConsumerTest.class);

    private ExecutorService executor;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

        executor = Executors.newFixedThreadPool(1, new DefaultThreadFactory("PartitionedProducerConsumerTest"));
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        executor.shutdownNow();
    }

    @Test(timeOut = 30000)
    public void testRoundRobinProducer() throws Exception {
        log.info("-- Starting {} test --", methodName);
        PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection

        int numPartitions = 4;
        TopicName topicName = TopicName.get("persistent://my-property/my-ns/my-partitionedtopic1-" + System.currentTimeMillis());

        admin.topics().createPartitionedTopic(topicName.toString(), numPartitions);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName.toString())
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition).create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName.toString())
                .subscriptionName("my-partitioned-subscriber").subscribe();
        assertEquals(consumer.getTopic(), topicName.toString());

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            Assert.assertNotNull(msg, "Message should not be null");
            consumer.acknowledge(msg);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            Assert.assertTrue(messageSet.add(receivedMessage), "Message " + receivedMessage + " already received");
        }

        producer.close();
        consumer.unsubscribe();
        consumer.close();
        pulsarClient.close();
        admin.topics().deletePartitionedTopic(topicName.toString());

        log.info("-- Exiting {} test --", methodName);
    }

    @Test(timeOut = 30000)
    public void testPartitionedTopicNameWithSpecialCharacter() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int numPartitions = 4;
        final String specialCharacter = "! * ' ( ) ; : @ & = + $ , \\ ? % # [ ]";
        TopicName topicName = TopicName
                .get("persistent://my-property/my-ns/my-partitionedtopic1-" + System.currentTimeMillis() + specialCharacter);
        admin.topics().createPartitionedTopic(topicName.toString(), numPartitions);

        // Try to create producer which does lookup and create connection with broker
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName.toString())
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition).create();
        producer.close();
        admin.topics().deletePartitionedTopic(topicName.toString());
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(timeOut = 30000)
    public void testCustomPartitionProducer() throws Exception {
        PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        TopicName topicName = null;
        Producer<byte[]> producer = null;
        Consumer<byte[]> consumer = null;
        final int MESSAGE_COUNT = 16;
        try {
            log.info("-- Starting {} test --", methodName);

            int numPartitions = 4;
            topicName = TopicName
                    .get("persistent://my-property/my-ns/my-partitionedtopic1-" + System.currentTimeMillis());

            admin.topics().createPartitionedTopic(topicName.toString(), numPartitions);

            producer = pulsarClient.newProducer().topic(topicName.toString())
                    .messageRouter(new AlwaysTwoMessageRouter())
                    .create();

            consumer = pulsarClient.newConsumer().topic(topicName.toString())
                    .subscriptionName("my-partitioned-subscriber").subscribe();

            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String message = "my-message-" + i;
                producer.newMessage().key(String.valueOf(i)).value(message.getBytes()).send();
            }

            Message<byte[]> msg;
            Set<String> messageSet = Sets.newHashSet();

            for (int i = 0; i < MESSAGE_COUNT; i++) {
                msg = consumer.receive(5, TimeUnit.SECONDS);
                Assert.assertNotNull(msg, "Message should not be null");
                consumer.acknowledge(msg);
                String receivedMessage = new String(msg.getData());
                log.debug("Received message: [{}]", receivedMessage);
                String expectedMessage = "my-message-" + i;
                testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            }
        } finally {
            producer.close();
            consumer.unsubscribe();
            consumer.close();
            pulsarClient.close();
            admin.topics().deletePartitionedTopic(topicName.toString());

            log.info("-- Exiting {} test --", methodName);
        }
    }

    @Test(timeOut = 30000)
    public void testSinglePartitionProducer() throws Exception {
        log.info("-- Starting {} test --", methodName);
        PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection

        int numPartitions = 4;
        TopicName topicName = TopicName
                .get("persistent://my-property/my-ns/my-partitionedtopic2-" + System.currentTimeMillis());

        admin.topics().createPartitionedTopic(topicName.toString(), numPartitions);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName.toString())
                .messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName.toString())
                .subscriptionName("my-partitioned-subscriber").subscribe();

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg;
        Set<String> messageSet = Sets.newHashSet();

        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            Assert.assertNotNull(msg, "Message should not be null");
            consumer.acknowledge(msg);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        producer.close();
        consumer.unsubscribe();
        consumer.close();
        pulsarClient.close();
        admin.topics().deletePartitionedTopic(topicName.toString());

        log.info("-- Exiting {} test --", methodName);
    }

    @Test(timeOut = 30000)
    public void testKeyBasedProducer() throws Exception {
        log.info("-- Starting {} test --", methodName);
        PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection

        int numPartitions = 4;
        TopicName topicName = TopicName
                .get("persistent://my-property/my-ns/my-partitionedtopic3-" + System.currentTimeMillis());
        String dummyKey1 = "dummykey1";
        String dummyKey2 = "dummykey2";

        admin.topics().createPartitionedTopic(topicName.toString(), numPartitions);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName.toString()).create();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName.toString())
                .subscriptionName("my-partitioned-subscriber").subscribe();

        for (int i = 0; i < 5; i++) {
            String message = "my-message-" + i;
            producer.newMessage().key(dummyKey1).value(message.getBytes()).send();
        }
        for (int i = 5; i < 10; i++) {
            String message = "my-message-" + i;
            producer.newMessage().key(dummyKey2).value(message.getBytes()).send();
        }

        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
            Assert.assertNotNull(msg, "Message should not be null");
            consumer.acknowledge(msg);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            testKeyBasedOrder(messageSet, receivedMessage);

        }

        producer.close();
        consumer.unsubscribe();
        consumer.close();
        pulsarClient.close();
        admin.topics().deletePartitionedTopic(topicName.toString());

        log.info("-- Exiting {} test --", methodName);
    }

    private void testKeyBasedOrder(Set<String> messageSet, String message) {
        int index = Integer.parseInt(message.substring(message.lastIndexOf('-') + 1));
        if (index != 0 && index != 5) {
            Assert.assertTrue(messageSet.contains("my-message-" + (index - 1)),
                    "Message my-message-" + (index - 1) + " should come before my-message-" + index);
        }
        Assert.assertTrue(messageSet.add(message), "Received duplicate message " + message);
    }

    @Test(timeOut = 100000)
    public void testPauseAndResume() throws Exception {
        log.info("-- Starting {} test --", methodName);
        PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection

        int numPartitions = 2;
        String topicName = TopicName
                .get("persistent://my-property/my-ns/my-partitionedtopic-pr-" + System.currentTimeMillis()).toString();

        admin.topics().createPartitionedTopic(topicName, numPartitions);

        int receiverQueueSize = 20; // number of permits broker has per partition when consumer initially subscribes
        int numMessages = receiverQueueSize * numPartitions;

        AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(numMessages));
        AtomicInteger received = new AtomicInteger();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().receiverQueueSize(receiverQueueSize)
                .topic(topicName)
                .subscriptionName("my-partitioned-subscriber").messageListener((c1, msg) -> {
                    Assert.assertNotNull(msg, "Message cannot be null");
                    String receivedMessage = new String(msg.getData());
                    log.debug("Received message [{}] in the listener", receivedMessage);
                    c1.acknowledgeAsync(msg);
                    received.incrementAndGet();
                    latch.get().countDown();
                }).subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();

        consumer.pause();

        for (int i = 0; i < numMessages * 2; i++) producer.send(("my-message-" + i).getBytes());

        log.info("Waiting for message listener to ack " + numMessages + " messages");
        assertTrue(latch.get().await(numMessages, TimeUnit.SECONDS), "Timed out waiting for message listener acks");

        log.info("Giving message listener an opportunity to receive messages while paused");
        Thread.sleep(2000);     // hopefully this is long enough
        assertEquals(received.intValue(), numMessages, "Consumer received messages while paused");

        latch.set(new CountDownLatch(numMessages));

        consumer.resume();

        log.info("Waiting for message listener to ack all messages");
        assertTrue(latch.get().await(numMessages, TimeUnit.SECONDS), "Timed out waiting for message listener acks");

        consumer.close();
        producer.close();
        pulsarClient.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(timeOut = 30000)
    public void testPauseAndResumeWithUnloading() throws Exception {
        final String topicName = "persistent://my-property/my-ns/pause-and-resume-with-unloading"
                + System.currentTimeMillis();
        final String subName = "sub";
        final int receiverQueueSize = 20;
        final int numPartitions = 2;
        final int numMessages = receiverQueueSize * numPartitions;

        PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 0);
        admin.topics().createPartitionedTopic(topicName, numPartitions);

        AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(numMessages));
        AtomicInteger received = new AtomicInteger();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .receiverQueueSize(receiverQueueSize).messageListener((c1, msg) -> {
                    assertNotNull(msg, "Message cannot be null");
                    c1.acknowledgeAsync(msg);
                    received.incrementAndGet();
                    latch.get().countDown();
                }).subscribe();
        consumer.pause();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).enableBatching(false).create();

        for (int i = 0; i < numMessages * 2; i++) {
            producer.send(("my-message-" + i).getBytes());
        }

        // Paused consumer receives only `numMessages` messages
        assertTrue(latch.get().await(numMessages, TimeUnit.SECONDS), "Timed out waiting for message listener acks");

        // Make sure no flow permits are sent when the consumer reconnects to the topic
        for (int i = 0; i < numPartitions; i++) {
            String partition = TopicName.get(topicName).getPartition(i).toString();
            admin.topics().unload(partition);
        }

        Thread.sleep(2000);
        assertEquals(received.intValue(), numMessages, "Consumer received messages while paused");

        latch.set(new CountDownLatch(numMessages));
        consumer.resume();
        assertTrue(latch.get().await(numMessages, TimeUnit.SECONDS), "Timed out waiting for message listener acks");

        consumer.unsubscribe();
        producer.close();
        pulsarClient.close();
        admin.topics().deletePartitionedTopic(topicName, true);
    }

    @Test(timeOut = 30000)
    public void testInvalidSequence() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int numPartitions = 4;
        TopicName topicName = TopicName
                .get("persistent://my-property/my-ns/my-partitionedtopic4-" + System.currentTimeMillis());
        admin.topics().createPartitionedTopic(topicName.toString(), numPartitions);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName.toString())
                .subscriptionName("my-subscriber-name").subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName.toString())
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        try {
            TypedMessageBuilderImpl<byte[]> mb = (TypedMessageBuilderImpl<byte[]>) producer.newMessage()
                    .value("InvalidMessage".getBytes());
            consumer.acknowledge(mb.getMessage());
        } catch (PulsarClientException.InvalidMessageException e) {
            // ok
        }

        consumer.close();

        try {
            consumer.receive();
            Assert.fail("Should fail");
        } catch (PulsarClientException.AlreadyClosedException e) {
            // ok
        }

        try {
            consumer.unsubscribe();
            Assert.fail("Should fail");
        } catch (PulsarClientException.AlreadyClosedException e) {
            // ok
        }


        producer.close();

        try {
            producer.send("message".getBytes());
            Assert.fail("Should fail");
        } catch (PulsarClientException.AlreadyClosedException e) {
            // ok
        }

        admin.topics().deletePartitionedTopic(topicName.toString());

    }

    @Test(timeOut = 30000)
    public void testSillyUser() throws Exception {

        int numPartitions = 4;
        TopicName topicName = TopicName
                .get("persistent://my-property/my-ns/my-partitionedtopic5-" + System.currentTimeMillis());
        admin.topics().createPartitionedTopic(topicName.toString(), numPartitions);

        Producer<byte[]> producer = null;
        Consumer<byte[]> consumer = null;

        try {
            pulsarClient.newProducer().messageRouter(null);
            Assert.fail("should fail");
        } catch (NullPointerException e) {
            // ok
        }

        try {
            pulsarClient.newProducer().messageRoutingMode(null);
            Assert.fail("should fail");
        } catch (NullPointerException e) {
            // ok
        }

        try {
            producer = pulsarClient.newProducer().topic(topicName.toString()).enableBatching(false)
                    .messageRoutingMode(MessageRoutingMode.SinglePartition).create();
            consumer = pulsarClient.newConsumer().topic(topicName.toString()).subscriptionName("my-sub").subscribe();
            producer.send("message1".getBytes());
            producer.send("message2".getBytes());
            /* Message<byte[]> msg1 = */ consumer.receive();
            Message<byte[]> msg2 = consumer.receive();
            consumer.acknowledgeCumulative(msg2);
        } finally {
            producer.close();
            consumer.unsubscribe();
            consumer.close();
        }

        admin.topics().deletePartitionedTopic(topicName.toString());

    }

    @Test(timeOut = 30000)
    public void testDeletePartitionedTopic() throws Exception {
        int numPartitions = 4;
        TopicName topicName = TopicName
                .get("persistent://my-property/my-ns/my-partitionedtopic6-" + System.currentTimeMillis());
        admin.topics().createPartitionedTopic(topicName.toString(), numPartitions);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName.toString()).create();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName.toString()).subscriptionName("my-sub")
                .subscribe();
        consumer.unsubscribe();
        consumer.close();
        producer.close();

        admin.topics().deletePartitionedTopic(topicName.toString());

        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topicName.toString()).create();
        if (producer1 instanceof PartitionedProducerImpl) {
            Assert.fail("should fail since partitioned topic was deleted");
        }
    }

    @Test(timeOut = 30000)
    public void testAsyncPartitionedProducerConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int totalMsg = 100;
        final Set<String> produceMsgs = Sets.newHashSet();
        final Set<String> consumeMsgs = Sets.newHashSet();

        int numPartitions = 4;
        TopicName topicName = TopicName
                .get("persistent://my-property/my-ns/my-partitionedtopic1-" + System.currentTimeMillis());

        admin.topics().createPartitionedTopic(topicName.toString(), numPartitions);
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName.toString())
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition).create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName.toString())
                .subscriptionName("my-partitioned-subscriber").subscriptionType(SubscriptionType.Shared).subscribe();

        // produce messages
        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            produceMsgs.add(message);
            producer.send(message.getBytes());
        }

        log.info(" start receiving messages :");

        // receive messages
        CountDownLatch latch = new CountDownLatch(totalMsg);
        receiveAsync(consumer, totalMsg, 0, latch, consumeMsgs, executor);

        latch.await();

        // verify message produced correctly
        assertEquals(produceMsgs.size(), totalMsg);
        // verify produced and consumed messages must be exactly same
        produceMsgs.removeAll(consumeMsgs);
        assertTrue(produceMsgs.isEmpty());

        producer.close();
        consumer.unsubscribe();
        consumer.close();
        admin.topics().deletePartitionedTopic(topicName.toString());

        log.info("-- Exiting {} test --", methodName);
    }

    @Test(timeOut = 30000)
    public void testAsyncPartitionedProducerConsumerQueueSizeOne() throws Exception {
        log.info("-- Starting {} test --", methodName);
        PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection

        final int totalMsg = 100;
        final Set<String> produceMsgs = Sets.newHashSet();
        final Set<String> consumeMsgs = Sets.newHashSet();

        int numPartitions = 4;
        TopicName topicName = TopicName
                .get("persistent://my-property/my-ns/my-partitionedtopic1-" + System.currentTimeMillis());

        admin.topics().createPartitionedTopic(topicName.toString(), numPartitions);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName.toString())
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition).create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName.toString())
                .subscriptionName("my-partitioned-subscriber").receiverQueueSize(1).subscribe();

        // produce messages
        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            produceMsgs.add(message);
            producer.send(message.getBytes());
        }

        log.info(" start receiving messages :");

        // receive messages
        CountDownLatch latch = new CountDownLatch(totalMsg);
        receiveAsync(consumer, totalMsg, 0, latch, consumeMsgs, executor);

        latch.await();

        // verify message produced correctly
        assertEquals(produceMsgs.size(), totalMsg);
        // verify produced and consumed messages must be exactly same
        produceMsgs.removeAll(consumeMsgs);
        assertTrue(produceMsgs.isEmpty());

        producer.close();
        consumer.unsubscribe();
        consumer.close();
        pulsarClient.close();
        admin.topics().deletePartitionedTopic(topicName.toString());

        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * It verifies that consumer consumes from all the partitions fairly.
     *
     * @throws Exception
     */
    @Test(timeOut = 30000)
    public void testFairDistributionForPartitionConsumers() throws Exception {
        log.info("-- Starting {} test --", methodName);
        PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection

        final int numPartitions = 2;
        final String topicName = "persistent://my-property/my-ns/my-topic-" + System.currentTimeMillis();
        final String producer1Msg = "producer1";
        final String producer2Msg = "producer2";
        final int queueSize = 10;

        admin.topics().createPartitionedTopic(topicName, numPartitions);

        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topicName + "-partition-0")
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition).create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topicName + "-partition-1")
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition).create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-partitioned-subscriber").receiverQueueSize(queueSize).subscribe();

        int partition2Msgs = 0;

        // produce messages on Partition-1: which will makes partitioned-consumer's queue full
        for (int i = 0; i < queueSize - 1; i++) {
            producer1.send((producer1Msg + "-" + i).getBytes());
        }

        Thread.sleep(1000);

        // now queue is full : so, partition-2 consumer will be pushed to paused-consumer list
        for (int i = 0; i < 5; i++) {
            producer2.send((producer2Msg + "-" + i).getBytes());
        }

        // now, Queue should take both partition's messages
        // also: we will keep producing messages to partition-1
        int produceMsgInPartition1AfterNumberOfConsumeMessages = 2;
        for (int i = 0; i < 3 * queueSize; i++) {
            Message<byte[]> msg = consumer.receive();
            partition2Msgs += (new String(msg.getData())).startsWith(producer2Msg) ? 1 : 0;
            if (i >= produceMsgInPartition1AfterNumberOfConsumeMessages) {
                producer1.send(producer1Msg.getBytes());
                Thread.sleep(100);
            }

        }

        assertTrue(partition2Msgs >= 4);
        producer1.close();
        producer2.close();
        consumer.unsubscribe();
        consumer.close();
        pulsarClient.close();
        admin.topics().deletePartitionedTopic(topicName);

        log.info("-- Exiting {} test --", methodName);
    }

    private void receiveAsync(Consumer<byte[]> consumer, int totalMessage, int currentMessage, CountDownLatch latch,
            final Set<String> consumeMsg, ExecutorService executor) throws PulsarClientException {
        if (currentMessage < totalMessage) {
            CompletableFuture<Message<byte[]>> future = consumer.receiveAsync();
            future.handle((msg, exception) -> {
                if (exception == null) {
                    // add message to consumer-queue to verify with produced messages
                    consumeMsg.add(new String(msg.getData()));
                    try {
                        consumer.acknowledge(msg);
                    } catch (PulsarClientException e1) {
                        fail("message acknowledge failed", e1);
                    }
                    // consume next message
                    executor.execute(() -> {
                        try {
                            receiveAsync(consumer, totalMessage, currentMessage + 1, latch, consumeMsg, executor);
                        } catch (PulsarClientException e) {
                            fail("message receive failed", e);
                        }
                    });
                    latch.countDown();
                }
                return null;
            });
        }
    }

    @Test
    public void testGetPartitionsForTopic() throws Exception {
        int numPartitions = 4;
        String topic = "persistent://my-property/my-ns/my-partitionedtopic1-" + System.currentTimeMillis();

        admin.topics().createPartitionedTopic(topic, numPartitions);

        List<String> expectedPartitions = new ArrayList<>();
        for (int i =0; i < numPartitions; i++) {
            expectedPartitions.add(topic + "-partition-" + i);
        }

        assertEquals(pulsarClient.getPartitionsForTopic(topic).join(), expectedPartitions);

        String nonPartitionedTopic = "persistent://my-property/my-ns/my-non-partitionedtopic1";

        assertEquals(pulsarClient.getPartitionsForTopic(nonPartitionedTopic).join(),
                Collections.singletonList(nonPartitionedTopic));
    }

    @Test
    public void testMessageIdForSubscribeToSinglePartition() throws Exception {
        PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        TopicName topicName = null;
        TopicName partition2TopicName = null;
        Producer<byte[]> producer = null;
        Consumer<byte[]> consumer1 = null;
        Consumer<byte[]> consumer2 = null;
        final int numPartitions = 4;
        final int totalMessages = 30;

        try {
            log.info("-- Starting {} test --", methodName);

            topicName = TopicName.get("persistent://my-property/my-ns/my-topic-" + System.currentTimeMillis());
            partition2TopicName = topicName.getPartition(2);

            admin.topics().createPartitionedTopic(topicName.toString(), numPartitions);

            producer = pulsarClient.newProducer().topic(topicName.toString())
                .messageRouter(new AlwaysTwoMessageRouter())
                .create();

            consumer1 = pulsarClient.newConsumer().topic(topicName.toString())
                .subscriptionName("subscriber-partitioned")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

            consumer2 = pulsarClient.newConsumer().topic(partition2TopicName.toString())
                .subscriptionName("subscriber-single")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

            for (int i = 0; i < totalMessages; i++) {
                String message = "my-message-" + i;
                producer.newMessage().key(String.valueOf(i)).value(message.getBytes()).send();
            }
            producer.flush();

            Message<byte[]> msg;

            for (int i = 0; i < totalMessages; i ++) {
                msg = consumer1.receive(5, TimeUnit.SECONDS);
                Assert.assertEquals(((MessageIdImpl)((TopicMessageIdImpl)msg.getMessageId()).getInnerMessageId()).getPartitionIndex(), 2);
                consumer1.acknowledge(msg);
            }

            for (int i = 0; i < totalMessages; i ++) {
                msg = consumer2.receive(5, TimeUnit.SECONDS);
                Assert.assertEquals(((MessageIdImpl)msg.getMessageId()).getPartitionIndex(), 2);
                consumer2.acknowledge(msg);
            }

        } finally {
            producer.close();
            consumer1.unsubscribe();
            consumer1.close();
            consumer2.unsubscribe();
            consumer2.close();
            pulsarClient.close();
            admin.topics().deletePartitionedTopic(topicName.toString());

            log.info("-- Exiting {} test --", methodName);
        }
    }


    /**
     * It verifies that consumer producer auto update for partitions extend.
     *
     * Steps:
     * 1. create topic with 2 partitions, and producer consumer
     * 2. update partition from 2 to 3.
     * 3. trigger auto update in producer, after produce, consumer will only get messages from 2 partitions.
     * 4. trigger auto update in consumer, after produce, consumer will get all messages from 3 partitions.
     *
     * @throws Exception
     */
    @Test(timeOut = 30000)
    public void testAutoUpdatePartitionsForProducerConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);
        PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection

        final int numPartitions = 2;
        final String topicName = "persistent://my-property/my-ns/my-topic-" + System.currentTimeMillis();
        final String producerMsg = "producerMsg";
        final int totalMessages = 30;

        admin.topics().createPartitionedTopic(topicName, numPartitions);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
            .enableBatching(false)
            .autoUpdatePartitions(true)
            .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
            .subscriptionName("my-partitioned-subscriber")
            .subscriptionType(SubscriptionType.Shared)
            .autoUpdatePartitions(true)
            .subscribe();

        // 1. produce and consume 2 partitions
        for (int i = 0; i < totalMessages; i++) {
            producer.send((producerMsg + " first round " + "message index: " + i).getBytes());
        }
        int messageSet = 0;
        Message<byte[]> message = consumer.receive();
        do {
            messageSet ++;
            consumer.acknowledge(message);
            log.info("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(200, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages);

        // 2. update partition from 2 to 3.
        admin.topics().updatePartitionedTopic(topicName,3);

        // 3. trigger auto update in producer, after produce, consumer will get 2/3 messages.
        log.info("trigger partitionsAutoUpdateTimerTask for producer");
        Timeout timeout = ((PartitionedProducerImpl<byte[]>)producer).getPartitionsAutoUpdateTimeout();
        timeout.task().run(timeout);
        Thread.sleep(200);

        for (int i = 0; i < totalMessages; i++) {
            producer.send((producerMsg + " second round " + "message index: " + i).getBytes());
        }
        messageSet = 0;
        message = consumer.receive();
        do {
            messageSet ++;
            consumer.acknowledge(message);
            log.info("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(200, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages * 2 / 3);

        // 4. trigger auto update in consumer, after produce, consumer will get all messages.
        log.info("trigger partitionsAutoUpdateTimerTask for consumer");
        timeout = ((MultiTopicsConsumerImpl<byte[]>)consumer).getPartitionsAutoUpdateTimeout();
        timeout.task().run(timeout);
        Thread.sleep(200);

        // former produced messages
        messageSet = 0;
        message = consumer.receive();
        do {
            messageSet ++;
            consumer.acknowledge(message);
            log.info("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(200, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages / 3);

        // former produced messages
        for (int i = 0; i < totalMessages; i++) {
            producer.send((producerMsg + " third round " + "message index: " + i).getBytes());
        }
        messageSet = 0;
        message = consumer.receive();
        do {
            messageSet ++;
            consumer.acknowledge(message);
            log.info("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(200, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages);

        pulsarClient.close();
        admin.topics().deletePartitionedTopic(topicName);

        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testCustomPartitionedProducer() throws Exception {
        PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        TopicName topicName = null;
        Producer<byte[]> producer = null;
        try {
            log.info("-- Starting {} test --", methodName);

            int numPartitions = 4;
            topicName = TopicName
                    .get("persistent://my-property/my-ns/my-partitionedtopic1-" + System.currentTimeMillis());

            admin.topics().createPartitionedTopic(topicName.toString(), numPartitions);

            RouterWithTopicName router = new RouterWithTopicName();
            producer = pulsarClient.newProducer().topic(topicName.toString())
                    .messageRouter(router)
                    .create();
            for (int i = 0; i < 1; i++) {
                String message = "my-message-" + i;
                producer.newMessage().key(String.valueOf(i)).value(message.getBytes()).send();
            }
            assertEquals(router.topicName, topicName.toString());
        } finally {
            producer.close();
            pulsarClient.close();
            admin.topics().deletePartitionedTopic(topicName.toString());
            log.info("-- Exiting {} test --", methodName);
        }
    }

    /**
     * Test producer and consumer interceptor to validate onPartitions change api invocation when partitions of the
     * topic changes.
     * 
     * @throws Exception
     */
    @Test
    public void testPartitionedTopicInterceptor() throws Exception {
        log.info("-- Starting {} test --", methodName);
        PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection

        int numPartitions = 4;
        TopicName topicName = TopicName
                .get("persistent://my-property/my-ns/interceptor-partitionedtopic1-" + System.currentTimeMillis());

        admin.topics().createPartitionedTopic(topicName.toString(), numPartitions);

        AtomicInteger newProducerPartitions = new AtomicInteger(0);
        AtomicInteger newConsumerPartitions = new AtomicInteger(0);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName.toString()).enableBatching(false)
                .autoUpdatePartitions(true).autoUpdatePartitionsInterval(1, TimeUnit.SECONDS)
                .intercept(new ProducerInterceptor<byte[]>() {
                    @Override
                    public void close() {
                    }

                    @Override
                    public Message<byte[]> beforeSend(Producer<byte[]> producer, Message<byte[]> message) {
                        return message;
                    }

                    @Override
                    public void onSendAcknowledgement(Producer<byte[]> producer, Message<byte[]> message,
                            MessageId msgId, Throwable exception) {
                    }

                    @Override
                    public void onPartitionsChange(String topicName, int partitions) {
                        newProducerPartitions.addAndGet(partitions);
                    }
                }).messageRoutingMode(MessageRoutingMode.RoundRobinPartition).create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName.toString())
                .subscriptionName("my-partitioned-subscriber").autoUpdatePartitionsInterval(1, TimeUnit.SECONDS)
                .intercept(new ConsumerInterceptor<byte[]>() {

                    @Override
                    public void close() {

                    }

                    @Override
                    public Message<byte[]> beforeConsume(Consumer<byte[]> consumer, Message<byte[]> message) {
                        return message;
                    }

                    @Override
                    public void onAcknowledge(Consumer<byte[]> consumer, MessageId messageId, Throwable exception) {

                    }

                    @Override
                    public void onAcknowledgeCumulative(Consumer<byte[]> consumer, MessageId messageId,
                            Throwable exception) {
                    }

                    @Override
                    public void onNegativeAcksSend(Consumer<byte[]> c, Set<MessageId> messageIds) {
                    }

                    @Override
                    public void onAckTimeoutSend(Consumer<byte[]> c2, Set<MessageId> messageIds) {
                    }

                    @Override
                    public void onPartitionsChange(String topic, int partitions) {
                        newConsumerPartitions.addAndGet(partitions);
                    }
                }).subscribe();

        int newPartitions = numPartitions + 5;
        admin.topics().updatePartitionedTopic(topicName.toString(), newPartitions);

        Awaitility.await().atMost(10000, TimeUnit.SECONDS).until(
                () -> newProducerPartitions.get() == newPartitions && newConsumerPartitions.get() == newPartitions);

        assertEquals(newProducerPartitions.get(), newPartitions);
        assertEquals(newConsumerPartitions.get(), newPartitions);

        Awaitility.await().untilAsserted(() -> consumer.isConnected());

        producer.close();
        consumer.unsubscribe();
        consumer.close();
        pulsarClient.close();
        admin.topics().deletePartitionedTopic(topicName.toString());

        log.info("-- Exiting {} test --", methodName);
    }

    private static class RouterWithTopicName implements MessageRouter {
        static String topicName = null;

        @Override
        public int choosePartition(Message<?> msg, TopicMetadata metadata) {
            topicName = msg.getTopicName();
            return 2;
        }
    }

    private static class AlwaysTwoMessageRouter implements MessageRouter {
        @Override
        public int choosePartition(Message<?> msg, TopicMetadata metadata) {
            return 2;
        }
    }
}
