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

import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class InterceptorsTest extends ProducerConsumerBase {

    private static final Logger log = LoggerFactory.getLogger(InterceptorsTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testProducerInterceptor() throws PulsarClientException {
        ProducerInterceptor<String> interceptor1 = new ProducerInterceptor<String>() {
            @Override
            public void close() {

            }

            @Override
            public Message<String> beforeSend(Producer<String> producer, Message<String> message) {
                MessageImpl<String> msg = (MessageImpl<String>) message;
                log.info("Before send message: {}", new String(msg.getData()));
                java.util.List<org.apache.pulsar.common.api.proto.PulsarApi.KeyValue> properties = msg.getMessageBuilder().getPropertiesList();
                for (int i = 0; i < properties.size(); i++) {
                    if ("key".equals(properties.get(i).getKey())) {
                        msg.getMessageBuilder().setProperties(i, PulsarApi.KeyValue.newBuilder().setKey("key").setValue("after").build());
                    }
                }
                return msg;
            }

            @Override
            public void onSendAcknowledgement(Producer<String> producer, Message<String> message, MessageId msgId, Throwable cause) {
                message.getProperties();
                Assert.assertEquals("complete", message.getProperty("key"));
                log.info("Send acknowledgement message: {}, msgId: {}", new String(message.getData()), msgId, cause);
            }
        };

        ProducerInterceptor<String> interceptor2 = new ProducerInterceptor<String>() {
            @Override
            public void close() {

            }

            @Override
            public Message<String> beforeSend(Producer<String> producer, Message<String> message) {
                MessageImpl<String> msg = (MessageImpl<String>) message;
                log.info("Before send message: {}", new String(msg.getData()));
                java.util.List<org.apache.pulsar.common.api.proto.PulsarApi.KeyValue> properties = msg.getMessageBuilder().getPropertiesList();
                for (int i = 0; i < properties.size(); i++) {
                    if ("key".equals(properties.get(i).getKey())) {
                        msg.getMessageBuilder().setProperties(i, PulsarApi.KeyValue.newBuilder().setKey("key").setValue("complete").build());
                    }
                }
                return msg;
            }

            @Override
            public void onSendAcknowledgement(Producer<String> producer, Message<String> message, MessageId msgId, Throwable cause) {
                message.getProperties();
                Assert.assertEquals("complete", message.getProperty("key"));
                log.info("Send acknowledgement message: {}, msgId: {}", new String(message.getData()), msgId, cause);
            }
        };

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic")
                .intercept(interceptor1, interceptor2)
                .create();

        MessageId messageId = producer.newMessage().property("key", "before").value("Hello Pulsar!").send();
        log.info("Send result messageId: {}", messageId);
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
                throw new NullPointerException();
            }

            @Override
            public void onSendAcknowledgement(Producer<String> producer, Message<String> message, MessageId msgId, Throwable exception) {
                throw new NullPointerException();
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
    public void testConsumerInterceptorWithSingleTopicSubscribe() throws PulsarClientException {
        ConsumerInterceptor<String> interceptor = new ConsumerInterceptor<String>() {
            @Override
            public void close() {

            }

            @Override
            public Message<String> beforeConsume(Consumer<String> consumer, Message<String> message) {
                MessageImpl<String> msg = (MessageImpl<String>) message;
                msg.getMessageBuilder().addProperties(PulsarApi.KeyValue.newBuilder().setKey("beforeConsumer").setValue("1").build());
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
                .subscribe();

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic")
                .create();

        producer.newMessage().value("Hello Pulsar!").send();

        Message<String> received = consumer.receive();
        MessageImpl<String> msg = (MessageImpl<String>) received;
        boolean haveKey = false;
        for (PulsarApi.KeyValue keyValue : msg.getMessageBuilder().getPropertiesList()) {
            if ("beforeConsumer".equals(keyValue.getKey())) {
                haveKey = true;
            }
        }
        Assert.assertTrue(haveKey);
        consumer.acknowledge(received);
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
                msg.getMessageBuilder().addProperties(PulsarApi.KeyValue.newBuilder().setKey("beforeConsumer").setValue("1").build());
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
            for (PulsarApi.KeyValue keyValue : msg.getMessageBuilder().getPropertiesList()) {
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
                msg.getMessageBuilder().addProperties(PulsarApi.KeyValue.newBuilder().setKey("beforeConsumer").setValue("1").build());
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
            for (PulsarApi.KeyValue keyValue : msg.getMessageBuilder().getPropertiesList()) {
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
                msg.getMessageBuilder().addProperties(PulsarApi.KeyValue.newBuilder().setKey("beforeConsumer").setValue("1").build());
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
            for (PulsarApi.KeyValue keyValue : msg.getMessageBuilder().getPropertiesList()) {
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
}
