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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Sets;
import lombok.Cleanup;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.schema.SchemaType;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class InterceptorsTest extends ProducerConsumerBase {

    private static final Logger log = LoggerFactory.getLogger(InterceptorsTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "receiverQueueSize")
    public Object[][] getReceiverQueueSize() {
        return new Object[][] { { 0 }, { 1000 } };
    }

    /**
     * 0 indicate the topic is non-partition topic
     */
    @DataProvider(name = "topicPartition")
    public Object[][] getTopicPartition() {
        return new Object[][] {{ 0 }, { 3 }};
    }

    @Test
    public void testProducerInterceptor() throws Exception {
        Map<MessageId, List<String>> ackCallback = new HashMap<>();

        String ns = "my-property/my-ns" + RandomUtils.nextInt(999, 1999);
        admin.namespaces().createNamespace(ns, Sets.newHashSet("test"));
        admin.namespaces().setSchemaCompatibilityStrategy(ns, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);

        abstract class BaseInterceptor implements
                org.apache.pulsar.client.api.interceptor.ProducerInterceptor {
            private static final String set = "set";
            private String tag;
            private BaseInterceptor(String tag) {
                this.tag = tag;
            }

            @Override
            public void close() {}

            @Override
            public Message beforeSend(Producer producer, Message message) {
                MessageImpl msg = (MessageImpl) message;
                msg.getMessageBuilder()
                   .addProperty().setKey(tag).setValue(set);
                return message;
            }

            @Override
            public void onSendAcknowledgement(Producer producer, Message message,
                                                     MessageId msgId, Throwable exception) {
                if (!set.equals(message.getProperties().get(tag))) {
                    return;
                }
                ackCallback.computeIfAbsent(msgId, k -> new ArrayList<>()).add(tag);
            }
        }

        BaseInterceptor interceptor1 = new BaseInterceptor("int1") {
            @Override
            public boolean eligible(Message message) {
                return true;
            }
        };
        BaseInterceptor interceptor2 = new BaseInterceptor("int2") {
            @Override
            public boolean eligible(Message message) {
                return SchemaType.STRING.equals(
                        ((MessageImpl)message).getSchemaInternal().getSchemaInfo().getType());
            }
        };
        BaseInterceptor interceptor3 = new BaseInterceptor("int3") {
            @Override
            public boolean eligible(Message message) {
                return SchemaType.INT32.equals(
                        ((MessageImpl)message).getSchemaInternal().getSchemaInfo().getType());
            }
        };

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://" + ns + "/my-topic")
                .intercept(interceptor1, interceptor2, interceptor3)
                .create();
        MessageId messageId = producer.newMessage().property("STR", "Y")
                                      .value("Hello Pulsar!").send();
        Assert.assertEquals(ackCallback.get(messageId),
                            Arrays.asList(interceptor1.tag, interceptor2.tag));
        log.info("Send result messageId: {}", messageId);
        MessageId messageId2 = producer.newMessage(Schema.INT32).property("INT", "Y")
                                       .value(18).send();
        Assert.assertEquals(ackCallback.get(messageId2),
                            Arrays.asList(interceptor1.tag, interceptor3.tag));
        log.info("Send result messageId: {}", messageId2);
        producer.close();
    }

    @Test
    public void testProducerInterceptorsWithExceptions() throws PulsarClientException {
        ProducerInterceptor<String> interceptor = new ProducerInterceptor<String>() {
            @Override
            public void close() {

            }

            @Override
            public Message<String> beforeSend(Producer<String> producer, Message<String> message) {
                throw new IllegalArgumentException();
            }

            @Override
            public void onSendAcknowledgement(Producer<String> producer, Message<String> message, MessageId msgId, Throwable exception) {
                throw new IllegalArgumentException();
            }
        };
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
            .topic("persistent://my-property/my-ns/my-topic")
            .intercept(interceptor)
            .create();

        MessageId messageId = producer.newMessage().value("Hello Pulsar!").send();
        Assert.assertNotNull(messageId);
        producer.close();
    }

    @Test
    public void testProducerInterceptorsWithErrors() throws PulsarClientException {
        ProducerInterceptor<String> interceptor = new ProducerInterceptor<String>() {
            @Override
            public void close() {

            }

            @Override
            public Message<String> beforeSend(Producer<String> producer, Message<String> message) {
                throw new AbstractMethodError();
            }

            @Override
            public void onSendAcknowledgement(Producer<String> producer, Message<String> message, MessageId msgId, Throwable exception) {
                throw new AbstractMethodError();
            }
        };
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic")
                .intercept(interceptor)
                .create();

        MessageId messageId = producer.newMessage().value("Hello Pulsar!").send();
        Assert.assertNotNull(messageId);
        producer.close();
    }

    @Test
    public void testConsumerInterceptorWithErrors() throws PulsarClientException {
        ConsumerInterceptor<String> interceptor = new ConsumerInterceptor<String>() {
            @Override
            public void close() {
                throw new AbstractMethodError();
            }

            @Override
            public Message<String> beforeConsume(Consumer<String> consumer, Message<String> message) {
                throw new AbstractMethodError();
            }

            @Override
            public void onAcknowledge(Consumer<String> consumer, MessageId messageId, Throwable exception) {
                throw new AbstractMethodError();
            }

            @Override
            public void onAcknowledgeCumulative(Consumer<String> consumer, MessageId messageId, Throwable exception) {
                throw new AbstractMethodError();
            }

            @Override
            public void onNegativeAcksSend(Consumer<String> consumer, Set<MessageId> messageIds) {
                throw new AbstractMethodError();
            }

            @Override
            public void onAckTimeoutSend(Consumer<String> consumer, Set<MessageId> messageIds) {
                throw new AbstractMethodError();
            }
        };
        Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic-exception")
                .subscriptionType(SubscriptionType.Shared)
                .intercept(interceptor)
                .subscriptionName("my-subscription-ack-timeout")
                .ackTimeout(3, TimeUnit.SECONDS)
                .subscribe();

        Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic-exception")
                .subscriptionType(SubscriptionType.Shared)
                .intercept(interceptor)
                .subscriptionName("my-subscription-negative")
                .subscribe();

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic-exception")
                .create();

        producer.newMessage().value("Hello Pulsar!").send();

        Message<String> received = consumer1.receive();
        Assert.assertEquals(received.getValue(), "Hello Pulsar!");
        // wait ack timeout
        Message<String> receivedAgain = consumer1.receive();
        Assert.assertEquals(receivedAgain.getValue(), "Hello Pulsar!");
        consumer1.acknowledge(receivedAgain);

        received = consumer2.receive();
        Assert.assertEquals(received.getValue(), "Hello Pulsar!");
        consumer2.negativeAcknowledge(received);
        receivedAgain = consumer2.receive();
        Assert.assertEquals(receivedAgain.getValue(), "Hello Pulsar!");
        consumer2.acknowledge(receivedAgain);

        producer.close();
        consumer1.close();
        consumer2.close();
    }

    @Test(dataProvider = "receiverQueueSize")
    public void testConsumerInterceptorWithSingleTopicSubscribe(Integer receiverQueueSize) throws Exception {
        ConsumerInterceptor<String> interceptor = new ConsumerInterceptor<String>() {
            @Override
            public void close() {

            }

            @Override
            public Message<String> beforeConsume(Consumer<String> consumer, Message<String> message) {
                MessageImpl<String> msg = (MessageImpl<String>) message;
                msg.getMessageBuilder().addProperty().setKey("beforeConsumer").setValue("1");
                return msg;
            }

            @Override
            public void onAcknowledge(Consumer<String> consumer, MessageId messageId, Throwable cause) {
                log.info("onAcknowledge messageId: {}", messageId, cause);
            }

            @Override
            public void onAcknowledgeCumulative(Consumer<String> consumer, MessageId messageId, Throwable cause) {
                log.info("onAcknowledgeCumulative messageIds: {}", messageId, cause);
            }

            @Override
            public void onNegativeAcksSend(Consumer<String> consumer, Set<MessageId> messageIds) {

            }

            @Override
            public void onAckTimeoutSend(Consumer<String> consumer, Set<MessageId> messageIds) {

            }
        };

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic")
                .subscriptionType(SubscriptionType.Shared)
                .intercept(interceptor)
                .subscriptionName("my-subscription")
                .receiverQueueSize(receiverQueueSize)
                .subscribe();

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic")
                .enableBatching(false)
                .create();

        // Receive a message synchronously
        producer.newMessage().value("Hello Pulsar!").send();
        Message<String> received = consumer.receive();
        MessageImpl<String> msg = (MessageImpl<String>) received;
        boolean haveKey = false;
        for (KeyValue keyValue : msg.getMessageBuilder().getPropertiesList()) {
            if ("beforeConsumer".equals(keyValue.getKey())) {
                haveKey = true;
            }
        }
        Assert.assertTrue(haveKey);
        consumer.acknowledge(received);

        // Receive a message asynchronously
        producer.newMessage().value("Hello Pulsar!").send();
        received = consumer.receiveAsync().get();
        msg = (MessageImpl<String>) received;
        haveKey = false;
        for (KeyValue keyValue : msg.getMessageBuilder().getPropertiesList()) {
            if ("beforeConsumer".equals(keyValue.getKey())) {
                haveKey = true;
            }
        }
        Assert.assertTrue(haveKey);
        consumer.acknowledge(received);
        consumer.close();

        final CompletableFuture<Message<String>> future = new CompletableFuture<>();
        consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic")
                .subscriptionType(SubscriptionType.Shared)
                .intercept(interceptor)
                .subscriptionName("my-subscription")
                .receiverQueueSize(receiverQueueSize)
                .messageListener((c, m) -> {
                    try {
                        c.acknowledge(m);
                    } catch (Exception e) {
                        Assert.fail("Failed to acknowledge", e);
                    }
                    future.complete(m);
                })
                .subscribe();

        // Receive a message using the message listener
        producer.newMessage().value("Hello Pulsar!").send();
        received = future.get();
        msg = (MessageImpl<String>) received;
        haveKey = false;
        for (KeyValue keyValue : msg.getMessageBuilder().getPropertiesList()) {
            if ("beforeConsumer".equals(keyValue.getKey())) {
                haveKey = true;
            }
        }
        Assert.assertTrue(haveKey);

        producer.close();
        consumer.close();
    }

    @Test
    public void testConsumerInterceptorWithMultiTopicSubscribe() throws PulsarClientException {

        ConsumerInterceptor<String> interceptor = new ConsumerInterceptor<String>() {
            @Override
            public void close() {

            }

            @Override
            public Message<String> beforeConsume(Consumer<String> consumer, Message<String> message) {
                MessageImpl<String> msg = (MessageImpl<String>) message;
                msg.getMessageBuilder().addProperty().setKey("beforeConsumer").setValue("1");
                return msg;
            }

            @Override
            public void onAcknowledge(Consumer<String> consumer, MessageId messageId, Throwable cause) {
                log.info("onAcknowledge messageId: {}", messageId, cause);
            }

            @Override
            public void onAcknowledgeCumulative(Consumer<String> consumer, MessageId messageId, Throwable cause) {
                log.info("onAcknowledgeCumulative messageIds: {}", messageId, cause);
            }

            @Override
            public void onNegativeAcksSend(Consumer<String> consumer, Set<MessageId> messageIds) {

            }

            @Override
            public void onAckTimeoutSend(Consumer<String> consumer, Set<MessageId> messageIds) {

            }
        };

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic")
                .create();

        Producer<String> producer1 = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic1")
                .create();

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic", "persistent://my-property/my-ns/my-topic1")
                .subscriptionType(SubscriptionType.Shared)
                .intercept(interceptor)
                .subscriptionName("my-subscription")
                .subscribe();

        producer.newMessage().value("Hello Pulsar!").send();
        producer1.newMessage().value("Hello Pulsar!").send();

        int keyCount = 0;
        for (int i = 0; i < 2; i++) {
            Message<String> received = consumer.receive();
            MessageImpl<String> msg = (MessageImpl<String>) ((TopicMessageImpl<String>) received).getMessage();
            for (KeyValue keyValue : msg.getMessageBuilder().getPropertiesList()) {
                if ("beforeConsumer".equals(keyValue.getKey())) {
                    keyCount++;
                }
            }
            consumer.acknowledge(received);
        }
        Assert.assertEquals(2, keyCount);
        producer.close();
        producer1.close();
        consumer.close();
    }

    @Test
    public void testConsumerInterceptorWithPatternTopicSubscribe() throws PulsarClientException {

        ConsumerInterceptor<String> interceptor = new ConsumerInterceptor<String>() {
            @Override
            public void close() {

            }

            @Override
            public Message<String> beforeConsume(Consumer<String> consumer, Message<String> message) {
                MessageImpl<String> msg = (MessageImpl<String>) message;
                msg.getMessageBuilder().addProperty().setKey("beforeConsumer").setValue("1");
                return msg;
            }

            @Override
            public void onAcknowledge(Consumer<String> consumer, MessageId messageId, Throwable cause) {
                log.info("onAcknowledge messageId: {}", messageId, cause);
            }

            @Override
            public void onAcknowledgeCumulative(Consumer<String> consumer, MessageId messageId, Throwable cause) {
                log.info("onAcknowledgeCumulative messageIds: {}", messageId, cause);
            }

            @Override
            public void onNegativeAcksSend(Consumer<String> consumer, Set<MessageId> messageIds) {

            }

            @Override
            public void onAckTimeoutSend(Consumer<String> consumer, Set<MessageId> messageIds) {

            }
        };

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic")
                .create();

        Producer<String> producer1 = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic1")
                .create();

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topicsPattern("persistent://my-property/my-ns/my-.*")
                .subscriptionType(SubscriptionType.Shared)
                .intercept(interceptor)
                .subscriptionName("my-subscription")
                .subscribe();

        producer.newMessage().value("Hello Pulsar!").send();
        producer1.newMessage().value("Hello Pulsar!").send();

        int keyCount = 0;
        for (int i = 0; i < 2; i++) {
            Message<String> received = consumer.receive();
            MessageImpl<String> msg = (MessageImpl<String>) ((TopicMessageImpl<String>) received).getMessage();
            for (KeyValue keyValue : msg.getMessageBuilder().getPropertiesList()) {
                if ("beforeConsumer".equals(keyValue.getKey())) {
                    keyCount++;
                }
            }
            consumer.acknowledge(received);
        }
        Assert.assertEquals(2, keyCount);
        producer.close();
        producer1.close();
        consumer.close();
    }

    @Test
    public void testConsumerInterceptorForAcknowledgeCumulative() throws PulsarClientException {

        List<MessageId> ackHolder = new ArrayList<>();

        ConsumerInterceptor<String> interceptor = new ConsumerInterceptor<String>() {
            @Override
            public void close() {

            }

            @Override
            public Message<String> beforeConsume(Consumer<String> consumer, Message<String> message) {
                MessageImpl<String> msg = (MessageImpl<String>) message;
                msg.getMessageBuilder().addProperty().setKey("beforeConsumer").setValue("1");
                return msg;
            }

            @Override
            public void onAcknowledge(Consumer<String> consumer, MessageId messageId, Throwable cause) {
                log.info("onAcknowledge messageId: {}", messageId, cause);
            }

            @Override
            public void onAcknowledgeCumulative(Consumer<String> consumer, MessageId messageId, Throwable cause) {
                long acknowledged = ackHolder.stream().filter(m -> (m.compareTo(messageId) <= 0)).count();
                Assert.assertEquals(acknowledged, 100);
                ackHolder.clear();
                log.info("onAcknowledgeCumulative messageIds: {}", messageId, cause);
            }

            @Override
            public void onNegativeAcksSend(Consumer<String> consumer, Set<MessageId> messageIds) {

            }

            @Override
            public void onAckTimeoutSend(Consumer<String> consumer, Set<MessageId> messageIds) {

            }
        };

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic")
                .subscriptionType(SubscriptionType.Failover)
                .intercept(interceptor)
                .subscriptionName("my-subscription")
                .subscribe();

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic")
                .create();

        for (int i = 0; i < 100; i++) {
            producer.newMessage().value("Hello Pulsar!").send();
        }

        int keyCount = 0;
        for (int i = 0; i < 100; i++) {
            Message<String> received = consumer.receive();
            MessageImpl<String> msg = (MessageImpl<String>) received;
            for (KeyValue keyValue : msg.getMessageBuilder().getPropertiesList()) {
                if ("beforeConsumer".equals(keyValue.getKey())) {
                    keyCount++;
                }
            }
            ackHolder.add(received.getMessageId());
            if (i == 99) {
                consumer.acknowledgeCumulative(received);
            }
        }
        Assert.assertEquals(100, keyCount);
        producer.close();
        consumer.close();
    }

    @Test
    public void testConsumerInterceptorForNegativeAcksSend() throws PulsarClientException, InterruptedException {
        final int totalNumOfMessages = 100;
        CountDownLatch latch = new CountDownLatch(totalNumOfMessages / 2);

        ConsumerInterceptor<String> interceptor = new ConsumerInterceptor<String>() {
            @Override
            public void close() {

            }

            @Override
            public Message<String> beforeConsume(Consumer<String> consumer, Message<String> message) {
                return message;
            }

            @Override
            public void onAcknowledge(Consumer<String> consumer, MessageId messageId, Throwable cause) {

            }

            @Override
            public void onAcknowledgeCumulative(Consumer<String> consumer, MessageId messageId, Throwable cause) {

            }

            @Override
            public void onNegativeAcksSend(Consumer<String> consumer, Set<MessageId> messageIds) {
                messageIds.forEach(messageId -> latch.countDown());
            }

            @Override
            public void onAckTimeoutSend(Consumer<String> consumer, Set<MessageId> messageIds) {

            }
        };

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic")
                .subscriptionType(SubscriptionType.Failover)
                .intercept(interceptor)
                .negativeAckRedeliveryDelay(100, TimeUnit.MILLISECONDS)
                .subscriptionName("my-subscription")
                .subscribe();

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic")
                .create();

        for (int i = 0; i < totalNumOfMessages; i++) {
            producer.send("Mock message");
        }

        for (int i = 0; i < totalNumOfMessages; i++) {
            Message<String> message = consumer.receive();

            if (i % 2 == 0) {
                consumer.negativeAcknowledge(message);
            } else {
                consumer.acknowledge(message);
            }
        }

        latch.await();
        Assert.assertEquals(latch.getCount(), 0);

        producer.close();
        consumer.close();
    }

    @Test
    public void testConsumerInterceptorForAckTimeoutSend() throws PulsarClientException, InterruptedException {
        final int totalNumOfMessages = 100;
        CountDownLatch latch = new CountDownLatch(totalNumOfMessages / 2);

        ConsumerInterceptor<String> interceptor = new ConsumerInterceptor<String>() {
            @Override
            public void close() {

            }

            @Override
            public Message<String> beforeConsume(Consumer<String> consumer, Message<String> message) {
                return message;
            }

            @Override
            public void onAcknowledge(Consumer<String> consumer, MessageId messageId, Throwable cause) {

            }

            @Override
            public void onAcknowledgeCumulative(Consumer<String> consumer, MessageId messageId, Throwable cause) {

            }

            @Override
            public void onNegativeAcksSend(Consumer<String> consumer, Set<MessageId> messageIds) {
            }

            @Override
            public void onAckTimeoutSend(Consumer<String> consumer, Set<MessageId> messageIds) {
                messageIds.forEach(messageId -> latch.countDown());
            }
        };

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic")
                .create();

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic")
                .subscriptionName("foo")
                .intercept(interceptor)
                .ackTimeout(2, TimeUnit.SECONDS)
                .subscribe();

        for (int i = 0; i < totalNumOfMessages; i++) {
            producer.send("Mock message");
        }

        for (int i = 0; i < totalNumOfMessages; i++) {
            Message<String> message = consumer.receive();

            if (i % 2 == 0) {
                consumer.acknowledge(message);
            }
        }

        latch.await();
        Assert.assertEquals(latch.getCount(), 0);

        producer.close();
        consumer.close();
    }

    @Test(timeOut = 1000 * 30, dataProvider = "topicPartition")
    public void testReaderInterceptor(int topicPartition) throws Exception {
        String topic = "reader-interceptor-" + topicPartition + "-" + RandomStringUtils.randomAlphabetic(5);
        if (topicPartition > 0) {
            admin.topics().createPartitionedTopic(topic, topicPartition);
        }

        class ReaderInterceptorImpl implements ReaderInterceptor<byte[]> {
            final AtomicInteger beforeReadCount = new AtomicInteger();
            final AtomicInteger newPartition = new AtomicInteger(0);
            final int maxReadCount;
            final AtomicBoolean encounterException = new AtomicBoolean(false);

            ReaderInterceptorImpl(int maxReadCount) {
                this.maxReadCount = maxReadCount;
            }

            @Override
            public void close() {
                // nothing to do
            }

            @Override
            public Message<byte[]> beforeRead(Reader<byte[]> reader, Message<byte[]> message) {
                if (beforeReadCount.get() > maxReadCount) {
                    encounterException.set(true);
                    throw new RuntimeException("The read count exceed maxReadCount - " + maxReadCount);
                }
                beforeReadCount.incrementAndGet();
                return message;
            }

            @Override
            public void onPartitionsChange(String topicName, int partitions) {
                newPartition.set(partitions);
            }
        }

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        int msgCount = 10;

        ReaderInterceptorImpl interceptor1 = new ReaderInterceptorImpl(Integer.MAX_VALUE);
        ReaderInterceptorImpl interceptor2 = new ReaderInterceptorImpl(msgCount * 2);
        @Cleanup
        Reader<byte[]> reader = pulsarClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .intercept(interceptor1, interceptor2)
                .autoUpdatePartitionsInterval(1, TimeUnit.SECONDS)
                .create();

        ReaderInterceptorImpl interceptor3 = new ReaderInterceptorImpl(msgCount * 2);
        AtomicInteger listenerReceiveCount = new AtomicInteger();
        @Cleanup
        Reader<byte[]> reader2 = pulsarClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .intercept(interceptor3)
                .autoUpdatePartitionsInterval(1, TimeUnit.SECONDS)
                .readerListener((ReaderListener<byte[]>) (r, msg) -> {
                    listenerReceiveCount.incrementAndGet();
                })
                .create();

        produceAndConsume(msgCount, producer, reader);
        Assert.assertEquals(interceptor1.beforeReadCount.get(), msgCount);
        Assert.assertEquals(interceptor2.beforeReadCount.get(), msgCount);
        Awaitility.await().pollInterval(1, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    Assert.assertEquals(listenerReceiveCount.get(), msgCount);
                }
        );
        Assert.assertEquals(interceptor3.beforeReadCount.get(), msgCount);

        if (topicPartition > 0) {
            // Make sure all interceptors still work well after update the topic partition.
            final int newPartition = topicPartition + 3;
            admin.topics().updatePartitionedTopic(topic, newPartition);
            Awaitility.await().pollInterval(1, TimeUnit.SECONDS).untilAsserted(
                    () -> {
                        Assert.assertEquals(interceptor1.newPartition.get(), newPartition);
                        Assert.assertEquals(interceptor2.newPartition.get(), newPartition);
                        Assert.assertEquals(interceptor3.newPartition.get(), newPartition);
                    });

            produceAndConsume(msgCount, producer, reader);
            Assert.assertEquals(interceptor1.beforeReadCount.get(), msgCount * 2);
            Assert.assertEquals(interceptor2.beforeReadCount.get(), msgCount * 2);
            Awaitility.await().pollInterval(1, TimeUnit.SECONDS).untilAsserted(
                    () -> {
                        Assert.assertEquals(listenerReceiveCount.get(), msgCount * 2);
                    }
            );
            Assert.assertEquals(interceptor3.beforeReadCount.get(), msgCount * 2);
        }

        // Even the ReaderInterceptor encounter errors, just log error logs, users receive messages in finally.
        produceAndConsume(msgCount * 2, producer, reader);
        Assert.assertFalse(interceptor1.encounterException.get());
        Assert.assertTrue(interceptor2.encounterException.get());
        Assert.assertTrue(interceptor3.encounterException.get());
        Assert.assertNull(reader.readNext(3, TimeUnit.SECONDS));
    }

    private void produceAndConsume(int msgCount, Producer<byte[]> producer, Reader<byte[]> reader)
            throws PulsarClientException {
        for (int i = 0; i < msgCount; i++) {
            producer.newMessage().value(("msg - " + i).getBytes()).send();
        }

        for (int i = 0; i < msgCount; i++) {
            Message<byte[]> message = reader.readNext();
            Assert.assertNotNull(message);
        }
    }

}
