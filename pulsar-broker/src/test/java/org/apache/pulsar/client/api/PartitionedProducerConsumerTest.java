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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.impl.PartitionedProducerImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

import io.netty.util.concurrent.DefaultThreadFactory;

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

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        executor.shutdown();
    }

    @Test(timeOut = 30000)
    public void testRoundRobinProducer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int numPartitions = 4;
        TopicName topicName = TopicName.get("persistent://my-property/my-ns/my-partitionedtopic1");

        admin.topics().createPartitionedTopic(topicName.toString(), numPartitions);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName.toString())
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition).create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName.toString())
                .subscriptionName("my-partitioned-subscriber").subscribe();

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
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
        admin.topics().deletePartitionedTopic(topicName.toString());

        log.info("-- Exiting {} test --", methodName);
    }

    @Test(timeOut = 30000)
    public void testPartitionedTopicNameWithSpecialCharacter() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int numPartitions = 4;
        final String specialCharacter = "! * ' ( ) ; : @ & = + $ , \\ ? % # [ ]";
        TopicName topicName = TopicName
                .get("persistent://my-property/my-ns/my-partitionedtopic1" + specialCharacter);
        admin.topics().createPartitionedTopic(topicName.toString(), numPartitions);

        // Try to create producer which does lookup and create connection with broker
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName.toString())
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition).create();
        producer.close();
        admin.topics().deletePartitionedTopic(topicName.toString());
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(timeOut = 30000)
    public void testSinglePartitionProducer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int numPartitions = 4;
        TopicName topicName = TopicName.get("persistent://my-property/my-ns/my-partitionedtopic2");

        admin.topics().createPartitionedTopic(topicName.toString(), numPartitions);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName.toString())
                .messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName.toString())
                .subscriptionName("my-partitioned-subscriber").subscribe();

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
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
        admin.topics().deletePartitionedTopic(topicName.toString());

        log.info("-- Exiting {} test --", methodName);
    }

    @Test(timeOut = 30000)
    public void testKeyBasedProducer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int numPartitions = 4;
        TopicName topicName = TopicName.get("persistent://my-property/my-ns/my-partitionedtopic3");
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

    @Test(timeOut = 30000)
    public void testInvalidSequence() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int numPartitions = 4;
        TopicName topicName = TopicName.get("persistent://my-property/my-ns/my-partitionedtopic4");
        admin.topics().createPartitionedTopic(topicName.toString(), numPartitions);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName.toString())
                .subscriptionName("my-subscriber-name").subscribe();

        try {
            Message<byte[]> msg = MessageBuilder.create().setContent("InvalidMessage".getBytes()).build();
            consumer.acknowledge(msg);
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

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName.toString())
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
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
        TopicName topicName = TopicName.get("persistent://my-property/my-ns/my-partitionedtopic5");
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
        TopicName topicName = TopicName.get("persistent://my-property/my-ns/my-partitionedtopic6");
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
        TopicName topicName = TopicName.get("persistent://my-property/my-ns/my-partitionedtopic1");

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

        final int totalMsg = 100;
        final Set<String> produceMsgs = Sets.newHashSet();
        final Set<String> consumeMsgs = Sets.newHashSet();

        int numPartitions = 4;
        TopicName topicName = TopicName.get("persistent://my-property/my-ns/my-partitionedtopic1");

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

        final int numPartitions = 2;
        final String topicName = "persistent://my-property/my-ns/my-topic";
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

}
