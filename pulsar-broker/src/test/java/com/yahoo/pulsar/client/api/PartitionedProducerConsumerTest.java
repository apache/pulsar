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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.yahoo.pulsar.client.api.ProducerConfiguration.MessageRoutingMode;
import com.yahoo.pulsar.client.impl.PartitionedProducerImpl;
import com.yahoo.pulsar.common.naming.DestinationName;

public class PartitionedProducerConsumerTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(PartitionedProducerConsumerTest.class);

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testRoundRobinProducer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int numPartitions = 4;
        DestinationName dn = DestinationName.get("persistent://my-property/use/my-ns/my-partitionedtopic1");

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);

        admin.persistentTopics().createPartitionedTopic(dn.toString(), numPartitions);

        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        Producer producer = pulsarClient.createProducer(dn.toString(), producerConf);

        Consumer consumer = pulsarClient.subscribe(dn.toString(), "my-partitioned-subscriber", conf);

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message msg = null;
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
        admin.persistentTopics().deletePartitionedTopic(dn.toString());

        log.info("-- Exiting {} test --", methodName);
    }
    
    @Test
    public void testPartitionedTopicNameWithSpecialCharacter() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int numPartitions = 4;
        final String specialCharacter = "! * ' ( ) ; : @ & = + $ , /\\ ? % # [ ]";
        final String topicName = "my-partitionedtopic1" + specialCharacter;
        DestinationName dn = DestinationName.get("persistent://my-property/use/my-ns/" + topicName);
        admin.persistentTopics().createPartitionedTopic(dn.toString(), numPartitions);

        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        // Try to create producer which does lookup and create connection with broker
        Producer producer = pulsarClient.createProducer(dn.toString(), producerConf);
        producer.close();
        admin.persistentTopics().deletePartitionedTopic(dn.toString());
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testSinglePartitionProducer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int numPartitions = 4;
        DestinationName dn = DestinationName.get("persistent://my-property/use/my-ns/my-partitionedtopic2");

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);

        admin.persistentTopics().createPartitionedTopic(dn.toString(), numPartitions);

        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setMessageRoutingMode(MessageRoutingMode.SinglePartition);
        Producer producer = pulsarClient.createProducer(dn.toString(), producerConf);

        Consumer consumer = pulsarClient.subscribe(dn.toString(), "my-partitioned-subscriber", conf);

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message msg = null;
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
        admin.persistentTopics().deletePartitionedTopic(dn.toString());

        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testKeyBasedProducer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int numPartitions = 4;
        DestinationName dn = DestinationName.get("persistent://my-property/use/my-ns/my-partitionedtopic3");
        String dummyKey1 = "dummykey1";
        String dummyKey2 = "dummykey2";

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);

        admin.persistentTopics().createPartitionedTopic(dn.toString(), numPartitions);

        Producer producer = pulsarClient.createProducer(dn.toString());
        Consumer consumer = pulsarClient.subscribe(dn.toString(), "my-partitioned-subscriber", conf);

        Message msg = null;
        for (int i = 0; i < 5; i++) {
            String message = "my-message-" + i;
            msg = MessageBuilder.create().setContent(message.getBytes()).setKey(dummyKey1).build();
            producer.send(msg);
        }
        for (int i = 5; i < 10; i++) {
            String message = "my-message-" + i;
            msg = MessageBuilder.create().setContent(message.getBytes()).setKey(dummyKey2).build();
            producer.send(msg);
        }

        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            Assert.assertNotNull(msg, "Message should not be null");
            consumer.acknowledge(msg);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            testKeyBasedOrder(messageSet, receivedMessage);

        }

        producer.close();
        consumer.unsubscribe();
        consumer.close();
        admin.persistentTopics().deletePartitionedTopic(dn.toString());

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

    @Test
    public void testInvalidSequence() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int numPartitions = 4;
        DestinationName dn = DestinationName.get("persistent://my-property/use/my-ns/my-partitionedtopic4");
        admin.persistentTopics().createPartitionedTopic(dn.toString(), numPartitions);

        ConsumerConfiguration consumerConf = new ConsumerConfiguration();
        consumerConf.setSubscriptionType(SubscriptionType.Exclusive);

        Consumer consumer = pulsarClient.subscribe(dn.toString(), "my-subscriber-name", consumerConf);

        try {
            Message msg = MessageBuilder.create().setContent("InvalidMessage".getBytes()).build();
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

        Producer producer = pulsarClient.createProducer(dn.toString());
        producer.close();

        try {
            producer.send("message".getBytes());
            Assert.fail("Should fail");
        } catch (PulsarClientException.AlreadyClosedException e) {
            // ok
        }

        admin.persistentTopics().deletePartitionedTopic(dn.toString());

    }

    @Test
    public void testSillyUser() throws Exception {

        int numPartitions = 4;
        DestinationName dn = DestinationName.get("persistent://my-property/use/my-ns/my-partitionedtopic5");
        admin.persistentTopics().createPartitionedTopic(dn.toString(), numPartitions);

        ProducerConfiguration producerConf = new ProducerConfiguration();
        Producer producer = null;
        Consumer consumer = null;

        try {
            producerConf.setMessageRouter(null);
            Assert.fail("should fail");
        } catch (NullPointerException e) {
            // ok
        }

        try {
            producerConf.setMessageRoutingMode(null);
            Assert.fail("should fail");
        } catch (NullPointerException e) {
            // ok
        }

        try {
            producer = pulsarClient.createProducer(dn.toString(), null);
            Assert.fail("should fail");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e instanceof PulsarClientException.InvalidConfigurationException);
        }

        ConsumerConfiguration consumerConf = new ConsumerConfiguration();

        try {
            consumer = pulsarClient.subscribe(dn.toString(), "my-subscriber-name", null);
            Assert.fail("Should fail");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e instanceof PulsarClientException.InvalidConfigurationException);
        }

        try {
            producer = pulsarClient.createProducer(dn.toString());
            consumer = pulsarClient.subscribe(dn.toString(), "my-sub");
            producer.send("message1".getBytes());
            producer.send("message2".getBytes());
            Message msg1 = consumer.receive();
            Message msg2 = consumer.receive();
            consumer.acknowledgeCumulative(msg2);
            Assert.fail("should fail since ack cumulative is not supported for partitioned topic");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e instanceof PulsarClientException.NotSupportedException);
        } finally {
            producer.close();
            consumer.unsubscribe();
            consumer.close();
        }

        admin.persistentTopics().deletePartitionedTopic(dn.toString());

    }

    @Test
    public void testDeletePartitionedTopic() throws Exception {
        int numPartitions = 4;
        DestinationName dn = DestinationName.get("persistent://my-property/use/my-ns/my-partitionedtopic6");
        admin.persistentTopics().createPartitionedTopic(dn.toString(), numPartitions);

        Producer producer = pulsarClient.createProducer(dn.toString());
        Consumer consumer = pulsarClient.subscribe(dn.toString(), "my-sub");
        consumer.unsubscribe();
        consumer.close();
        producer.close();

        admin.persistentTopics().deletePartitionedTopic(dn.toString());

        Producer producer1 = pulsarClient.createProducer(dn.toString());
        if (producer1 instanceof PartitionedProducerImpl) {
            Assert.fail("should fail since partitioned topic was deleted");
        }
    }

    @Test(timeOut = 4000)
    public void testAsyncPartitionedProducerConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int totalMsg = 100;
        final Set<String> produceMsgs = Sets.newHashSet();
        final Set<String> consumeMsgs = Sets.newHashSet();

        int numPartitions = 4;
        DestinationName dn = DestinationName.get("persistent://my-property/use/my-ns/my-partitionedtopic1");

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Shared);

        admin.persistentTopics().createPartitionedTopic(dn.toString(), numPartitions);

        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        Producer producer = pulsarClient.createProducer(dn.toString(), producerConf);

        Consumer consumer = pulsarClient.subscribe(dn.toString(), "my-partitioned-subscriber", conf);

        // produce messages
        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            produceMsgs.add(message);
            producer.send(message.getBytes());
        }

        log.info(" start receiving messages :");

        // receive messages
        CountDownLatch latch = new CountDownLatch(totalMsg);
        receiveAsync(consumer, totalMsg, 0, latch, consumeMsgs, Executors.newFixedThreadPool(1));

        latch.await();

        // verify message produced correctly
        assertEquals(produceMsgs.size(), totalMsg);
        // verify produced and consumed messages must be exactly same
        produceMsgs.removeAll(consumeMsgs);
        assertTrue(produceMsgs.isEmpty());

        producer.close();
        consumer.unsubscribe();
        consumer.close();
        admin.persistentTopics().deletePartitionedTopic(dn.toString());

        log.info("-- Exiting {} test --", methodName);
    }

    @Test(timeOut = 4000)
    public void testAsyncPartitionedProducerConsumerQueueSizeOne() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int totalMsg = 100;
        final Set<String> produceMsgs = Sets.newHashSet();
        final Set<String> consumeMsgs = Sets.newHashSet();

        int numPartitions = 4;
        DestinationName dn = DestinationName.get("persistent://my-property/use/my-ns/my-partitionedtopic1");

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setReceiverQueueSize(1);

        admin.persistentTopics().createPartitionedTopic(dn.toString(), numPartitions);

        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        Producer producer = pulsarClient.createProducer(dn.toString(), producerConf);

        Consumer consumer = pulsarClient.subscribe(dn.toString(), "my-partitioned-subscriber", conf);

        // produce messages
        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            produceMsgs.add(message);
            producer.send(message.getBytes());
        }

        log.info(" start receiving messages :");

        // receive messages
        CountDownLatch latch = new CountDownLatch(totalMsg);
        receiveAsync(consumer, totalMsg, 0, latch, consumeMsgs, Executors.newFixedThreadPool(1));

        latch.await();

        // verify message produced correctly
        assertEquals(produceMsgs.size(), totalMsg);
        // verify produced and consumed messages must be exactly same
        produceMsgs.removeAll(consumeMsgs);
        assertTrue(produceMsgs.isEmpty());

        producer.close();
        consumer.unsubscribe();
        consumer.close();
        admin.persistentTopics().deletePartitionedTopic(dn.toString());

        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * It verifies that consumer consumes from all the partitions fairly.
     * 
     * @throws Exception
     */
    @Test
    public void testFairDistributionForPartitionConsumers() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int numPartitions = 2;
        final String topicName = "persistent://my-property/use/my-ns/my-topic";
        final String producer1Msg = "producer1";
        final String producer2Msg = "producer2";
        final int queueSize = 10;

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setReceiverQueueSize(queueSize);

        admin.persistentTopics().createPartitionedTopic(topicName, numPartitions);

        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        Producer producer1 = pulsarClient.createProducer(topicName + "-partition-0", producerConf);
        Producer producer2 = pulsarClient.createProducer(topicName + "-partition-1", producerConf);

        Consumer consumer = pulsarClient.subscribe(topicName, "my-partitioned-subscriber", conf);

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
            Message msg = consumer.receive();
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
        admin.persistentTopics().deletePartitionedTopic(topicName);

        log.info("-- Exiting {} test --", methodName);
    }
    
    private void receiveAsync(Consumer consumer, int totalMessage, int currentMessage, CountDownLatch latch,
            final Set<String> consumeMsg, ExecutorService executor) throws PulsarClientException {
        if (currentMessage < totalMessage) {
            CompletableFuture<Message> future = consumer.receiveAsync();
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
