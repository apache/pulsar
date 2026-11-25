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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.client.impl.PartitionedProducerImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
@CustomLog
public class NonPersistentTopicTest extends SharedPulsarBaseTest {

    private String methodName;

    @DataProvider(name = "subscriptionType")
    public Object[][] getSubscriptionType() {
        return new Object[][] { { SubscriptionType.Shared }, { SubscriptionType.Exclusive } };
    }

    @Override
    protected boolean checkClientHandlesClosed() {
        return true;
    }

    @BeforeMethod
    public void setTestMethodName(Method method) {
        methodName = method.getName();
    }

    @Test(timeOut = 90000 /* 1.5mn */)
    public void testNonPersistentPartitionsAreNotAutoCreatedWhenThePartitionedTopicDoesNotExist() throws Exception {
        final boolean defaultAllowAutoTopicCreation = getConfig().isAllowAutoTopicCreation();
        try {
            // Given the auto topic creation is disabled
            getConfig().setAllowAutoTopicCreation(false);
            // a client with HTTP lookups, as the class's client had on a broker of its own
            @Cleanup
            PulsarClient client = PulsarClient.builder().serviceUrl(getWebServiceUrl()).build();

            final String topicPartitionName = "non-persistent://" + getNamespace() + "/issue-9173-partition-0";

            // Then error when subscribe to a partition of a non-persistent topic that does not exist
            assertThrows(PulsarClientException.NotFoundException.class,
                    () -> client.newConsumer().topic(topicPartitionName)
                            .subscriptionName("sub-issue-9173").subscribe());

            // Then error when produce to a partition of a non-persistent topic that does not exist
            try {
                client.newProducer().topic(topicPartitionName).create();
                Assert.fail("Should failed due to topic not exist");
            } catch (Exception e) {
                assertTrue(e instanceof PulsarClientException.NotFoundException);
            }
        } finally {
            getConfig().setAllowAutoTopicCreation(defaultAllowAutoTopicCreation);
        }
    }

    @Test(timeOut = 90000 /* 1.5mn */)
    public void testAutoCreateNonPersistentPartitionsWhenThePartitionedTopicExists() throws Exception {
        final boolean defaultAllowAutoTopicCreation = getConfig().isAllowAutoTopicCreation();
        try {
            // Given the auto topic creation is disabled
            getConfig().setAllowAutoTopicCreation(false);
            // a client with HTTP lookups, as the class's client had on a broker of its own
            @Cleanup
            PulsarClient client = PulsarClient.builder().serviceUrl(getWebServiceUrl()).build();

            // Given the non-persistent partitioned topic exists
            final String topic = "non-persistent://" + getNamespace() + "/issue-9173";
            admin.topics().createPartitionedTopic(topic, 3);

            // When subscribe, then a sub-consumer is created for each partition which means the partitions are created
            final MultiTopicsConsumerImpl<byte[]> consumer =
                    (MultiTopicsConsumerImpl<byte[]>) client.newConsumer()
                    .topic(topic).subscriptionName("sub-issue-9173").subscribe();
            assertEquals(consumer.getConsumers().size(), 3);

            // When produce, a sub-producer is created for each partition which means the partitions are created
            PartitionedProducerImpl<byte[]> producer =
                    (PartitionedProducerImpl<byte[]>) client.newProducer().topic(topic).create();
            assertEquals(producer.getProducers().size(), 3);

            consumer.close();
            producer.close();
        } finally {
            getConfig().setAllowAutoTopicCreation(defaultAllowAutoTopicCreation);
        }
    }

    @Test(dataProvider = "subscriptionType")
    public void testNonPersistentTopic(SubscriptionType type) throws Exception {
        log.info().attr("starting", methodName).log("-- Starting test");

        final String topic = "non-persistent://" + getNamespace() + "/unacked-topic";
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
                log.debug().attr("receivedMessage", receivedMessage).log("Received message: []");
                String expectedMessage = "my-message-" + i;
                testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            } else {
                break;
            }
        }
        assertEquals(messageSet.size(), totalProduceMsg);

        producer.close();
        consumer.close();
        log.info().attr("exiting", methodName).log("-- Exiting test");

    }

    @Test(dataProvider = "subscriptionType")
    public void testPartitionedNonPersistentTopic(SubscriptionType type) throws Exception {
        log.info().attr("starting", methodName).log("-- Starting test");

        final String topic = "non-persistent://" + getNamespace() + "/partitioned-topic";
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
                log.debug().attr("receivedMessage", receivedMessage).log("Received message: []");
                String expectedMessage = "my-message-" + i;
                testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            } else {
                break;
            }
        }
        assertEquals(messageSet.size(), totalProduceMsg);

        producer.close();
        consumer.close();
        log.info().attr("exiting", methodName).log("-- Exiting test");

    }
    @SuppressWarnings("deprecation")

    @Test(dataProvider = "subscriptionType")
    public void testPartitionedNonPersistentTopicWithTcpLookup(SubscriptionType type) throws Exception {
        log.info().attr("starting", methodName).log("-- Starting test");

        final int numPartitions = 5;
        final String topic = "non-persistent://" + getNamespace() + "/partitioned-topic";
        admin.topics().createPartitionedTopic(topic, numPartitions);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
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
            assertNotNull(getPulsar().getBrokerService().getTopicReference(partition.toString()));
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
                log.debug().attr("receivedMessage", receivedMessage).log("Received message: []");
                String expectedMessage = "my-message-" + i;
                testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            } else {
                break;
            }
        }
        assertEquals(messageSet.size(), totalProduceMsg);

        producer.close();
        consumer.close();
        log.info().attr("exiting", methodName).log("-- Exiting test");
    }

    /**
     * It verifies that broker doesn't dispatch messages if consumer runs out of permits filled out with messages.
     */
    @Test(dataProvider = "subscriptionType")
    public void testConsumerInternalQueueMaxOut(SubscriptionType type) throws Exception {
        log.info().attr("starting", methodName).log("-- Starting test");

        final String topic = "non-persistent://" + getNamespace() + "/unacked-topic";
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
                log.debug().attr("receivedMessage", receivedMessage).log("Received message: []");
                String expectedMessage = "my-message-" + i;
                testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            } else {
                break;
            }
        }
        assertEquals(messageSet.size(), queueSize);

        producer.close();
        consumer.close();
        log.info().attr("exiting", methodName).log("-- Exiting test");

    }

    /**
     * verifies message delivery with multiple consumers on shared and failover subscriptions.
     *
     * @throws Exception
     */
    @Test
    public void testMultipleSubscription() throws Exception {
        log.info().attr("starting", methodName).log("-- Starting test");

        final String topic = "non-persistent://" + getNamespace() + "/unacked-topic";
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
        log.info().attr("exiting", methodName).log("-- Exiting test");

    }

    private <T> void testMessageOrderAndDuplicates(Set<T> messagesReceived, T receivedMessage,
            T expectedMessage) {
        // Make sure that messages are received in order
        Assert.assertEquals(receivedMessage, expectedMessage,
                "Received message " + receivedMessage + " did not match the expected message " + expectedMessage);

        // Make sure that there are no duplicates
        Assert.assertTrue(messagesReceived.add(receivedMessage), "Received duplicate message " + receivedMessage);
    }
}
