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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.Data;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.reflect.Nullable;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.client.util.RetryMessageUtil;
import org.reflections.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class RetryTopicTest extends ProducerConsumerBase {

    private static final Logger log = LoggerFactory.getLogger(RetryTopicTest.class);

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

    @Test
    public void testRetryTopic() throws Exception {
        final String topic = "persistent://my-property/my-ns/retry-topic";

        final int maxRedeliveryCount = 2;

        final int sendMessages = 100;

        Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .enableRetry(true)
                .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(maxRedeliveryCount).build())
                .receiverQueueSize(100)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        @Cleanup
        PulsarClient newPulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> deadLetterConsumer = newPulsarClient.newConsumer(Schema.BYTES)
                .topic("persistent://my-property/my-ns/retry-topic-my-subscription-DLQ")
                .subscriptionName("my-subscription")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic(topic)
                .create();

        for (int i = 0; i < sendMessages; i++) {
            producer.send(String.format("Hello Pulsar [%d]", i).getBytes());
        }

        producer.close();

        int totalReceived = 0;
        do {
            Message<byte[]> message = consumer.receive();
            log.info("consumer received message : {} {}", message.getMessageId(), new String(message.getData()));
            consumer.reconsumeLater(message, 1, TimeUnit.SECONDS);
            totalReceived++;
        } while (totalReceived < sendMessages * (maxRedeliveryCount + 1));

        int totalInDeadLetter = 0;
        do {
            Message<byte[]> message = deadLetterConsumer.receive();
            log.info("dead letter consumer received message : {} {}", message.getMessageId(), new String(message.getData()));
            deadLetterConsumer.acknowledge(message);
            totalInDeadLetter++;
        } while (totalInDeadLetter < sendMessages);

        deadLetterConsumer.close();
        consumer.close();

        Consumer<byte[]> checkConsumer = this.pulsarClient.newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Message<byte[]> checkMessage = checkConsumer.receive(3, TimeUnit.SECONDS);
        if (checkMessage != null) {
            log.info("check consumer received message : {} {}", checkMessage.getMessageId(), new String(checkMessage.getData()));
        }
        assertNull(checkMessage);

        checkConsumer.close();
    }

    @Data
    public static class Foo {
        @Nullable
        private String field1;
        @Nullable
        private String field2;
    }

    @Data
    public static class FooV2 {
        @Nullable
        private String field1;
        @Nullable
        private String field2;
        @Nullable
        private String field3;
    }

    @Test(timeOut = 20000)
    public void testAutoConsumeSchemaRetryLetter() throws Exception {
        final String topic = "persistent://my-property/my-ns/retry-letter-topic";
        final String subName = "my-subscription";
        final String retrySubName = "my-subscription" + "-RETRY";
        final int sendMessages = 10;
        final String retryTopic = topic + "-RETRY";

        admin.topics().createNonPartitionedTopic(topic);

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.AUTO_PRODUCE_BYTES())
                .topic(topic)
                .enableBatching(false)
                .create();
        @Cleanup
        Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .topic(topic)
                .subscriptionName(subName)
                .isAckReceiptEnabled(true)
                .subscriptionType(SubscriptionType.Shared)
                .enableRetry(true)
                .deadLetterPolicy(DeadLetterPolicy.builder().retryLetterTopic(retryTopic)
                        .maxRedeliverCount(Integer.MAX_VALUE).build())
                .subscribe();
        @Cleanup
        Consumer<GenericRecord> retryTopicConsumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .topic(retryTopic)
                .subscriptionName(retrySubName)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();
        Set<MessageId> messageIds = new HashSet<>();
        for (int i = 0; i < sendMessages; i++) {
            if (i % 2 == 0) {
                Foo foo = new Foo();
                foo.field1 = i + "";
                foo.field2 = i + "";
                messageIds.add(producer.newMessage(Schema.AVRO(Foo.class)).value(foo).send());
            } else {
                FooV2 foo = new FooV2();
                foo.field1 = i + "";
                foo.field2 = i + "";
                foo.field3 = i + "";
                messageIds.add(producer.newMessage(Schema.AVRO(FooV2.class)).value(foo).send());
            }
        }
        producer.close();

        int totalReceived = 0;
        do {
            Message<GenericRecord> message = consumer.receive();
            log.info(
                    "consumer received message (schema={}) : {} {}",
                    message.getReaderSchema().get(), message.getMessageId(), new String(message.getData()));
            consumer.reconsumeLater(message, 1, TimeUnit.SECONDS);
            assertTrue(messageIds.contains(message.getMessageId()));
            totalReceived++;
        } while (totalReceived < sendMessages);

        // consume receive retry message
        Set<MessageId> retryTopicMessageIds = new HashSet<>();
        do {
            Message<GenericRecord> message = consumer.receive();
            log.info(
                    "consumer received retry message (schema={}) : {} {}",
                    message.getReaderSchema().get(), message.getMessageId(), new String(message.getData()));
            consumer.acknowledge(message);
            retryTopicMessageIds.add(message.getMessageId());
            assertFalse(messageIds.contains(message.getMessageId()));
            totalReceived++;
        } while (totalReceived - sendMessages < sendMessages);

        Message<GenericRecord> message = consumer.receive(2, TimeUnit.SECONDS);
        assertNull(message);

        totalReceived = 0;

        // retryTopicConsumer receive retry messages
        do {
            Message<GenericRecord> retryTopicMessage = retryTopicConsumer.receive();
            assertTrue(retryTopicMessageIds.contains(retryTopicMessage.getMessageId()));
            assertEquals(retryTopicMessage.getValue().getField("field1"), totalReceived + "");
            assertEquals(retryTopicMessage.getValue().getField("field2"), totalReceived + "");
            if (totalReceived % 2 == 0) {
                try {
                    retryTopicMessage.getValue().getField("field3");
                } catch (Exception e) {
                    assertTrue(e instanceof AvroRuntimeException);
                    assertEquals(e.getMessage(), "Not a valid schema field: field3");
                }
            } else {
                assertEquals(retryTopicMessage.getValue().getField("field3"), totalReceived + "");
            }
            totalReceived++;
        } while (totalReceived < sendMessages);

        message = retryTopicConsumer.receive(2, TimeUnit.SECONDS);
        assertNull(message);
    }

    @Test(timeOut = 60000)
    public void testRetryTopicProperties() throws Exception {
        final String topic = "persistent://my-property/my-ns/retry-topic";

        final int maxRedeliveryCount = 3;

        final int sendMessages = 10;

        Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .enableRetry(true)
                .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(maxRedeliveryCount).build())
                .receiverQueueSize(100)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        @Cleanup
        PulsarClient newPulsarClient = newPulsarClient(lookupUrl.toString(), 0);
        Consumer<byte[]> deadLetterConsumer = newPulsarClient.newConsumer(Schema.BYTES)
                .topic("persistent://my-property/my-ns/retry-topic-my-subscription-DLQ")
                .subscriptionName("my-subscription")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic(topic)
                .create();

        Set<String> originMessageIds = new HashSet<>();
        for (int i = 0; i < sendMessages; i++) {
            MessageId msgId = producer.send(String.format("Hello Pulsar [%d]", i).getBytes());
            originMessageIds.add(msgId.toString());
        }

        int totalReceived = 0;
        Set<String> retryMessageIds = new HashSet<>();
        do {
            Message<byte[]> message = consumer.receive();
            log.info("consumer received message : {} {}", message.getMessageId(), new String(message.getData()));
            // retry message
            if (message.hasProperty(RetryMessageUtil.SYSTEM_PROPERTY_RECONSUMETIMES)) {
                // check the REAL_TOPIC property
                assertEquals(message.getProperty(RetryMessageUtil.SYSTEM_PROPERTY_REAL_TOPIC), topic);
                retryMessageIds.add(message.getProperty(RetryMessageUtil.SYSTEM_PROPERTY_ORIGIN_MESSAGE_ID));
            }
            consumer.reconsumeLater(message, 1, TimeUnit.SECONDS);
            totalReceived++;
        } while (totalReceived < sendMessages * (maxRedeliveryCount + 1));

        // check the REAL_TOPIC property
        assertEquals(retryMessageIds, originMessageIds);

        int totalInDeadLetter = 0;
        Set<String> deadLetterMessageIds = new HashSet<>();
        do {
            Message message = deadLetterConsumer.receive();
            log.info("dead letter consumer received message : {} {}", message.getMessageId(),
                    new String(message.getData()));
            // dead letter message
            if (message.hasProperty(RetryMessageUtil.SYSTEM_PROPERTY_RECONSUMETIMES)) {
                // check the REAL_TOPIC property
                assertEquals(message.getProperty(RetryMessageUtil.SYSTEM_PROPERTY_REAL_TOPIC), topic);
                deadLetterMessageIds.add(message.getProperty(RetryMessageUtil.SYSTEM_PROPERTY_ORIGIN_MESSAGE_ID));
            }
            deadLetterConsumer.acknowledge(message);
            totalInDeadLetter++;
        } while (totalInDeadLetter < sendMessages);

        assertEquals(deadLetterMessageIds, originMessageIds);

        Consumer<byte[]> checkConsumer = this.pulsarClient.newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Message<byte[]> checkMessage = checkConsumer.receive(3, TimeUnit.SECONDS);
        if (checkMessage != null) {
            log.info("check consumer received message : {} {}", checkMessage.getMessageId(),
                    new String(checkMessage.getData()));
        }
        assertNull(checkMessage);

        checkConsumer.close();

        // check the custom properties
        producer.send(String.format("Hello Pulsar [%d]", 1).getBytes());
        for (int i = 0; i < maxRedeliveryCount + 1; i++) {
            Map<String, String> customProperties = new HashMap<String, String>();
            customProperties.put("custom_key", "custom_value" + i);
            Message<byte[]> message = consumer.receive();
            log.info("Received message: {}", new String(message.getValue()));
            consumer.reconsumeLater(message, customProperties, 1, TimeUnit.SECONDS);
            if (i > 0) {
                String value = message.getProperty("custom_key");
                assertEquals(value, "custom_value" + (i - 1));
                assertEquals(message.getProperty(RetryMessageUtil.SYSTEM_PROPERTY_ORIGIN_MESSAGE_ID),
                        message.getProperty(RetryMessageUtil.PROPERTY_ORIGIN_MESSAGE_ID));
            }
        }
        assertNull(consumer.receive(3, TimeUnit.SECONDS));
        Message<byte[]> message = deadLetterConsumer.receive();
        String value = message.getProperty("custom_key");
        assertEquals(value, "custom_value" + maxRedeliveryCount);
        assertEquals(message.getProperty(RetryMessageUtil.SYSTEM_PROPERTY_ORIGIN_MESSAGE_ID),
                message.getProperty(RetryMessageUtil.PROPERTY_ORIGIN_MESSAGE_ID));

        producer.close();
        consumer.close();
        deadLetterConsumer.close();
    }

    //Issue 9327: do compatibility check in case of the default retry and dead letter topic name changed
    @Test
    public void testRetryTopicNameForCompatibility () throws Exception {
        final String topic = "persistent://my-property/my-ns/retry-topic";

        final String oldRetryTopic = "persistent://my-property/my-ns/my-subscription-RETRY";

        final String oldDeadLetterTopic = "persistent://my-property/my-ns/my-subscription-DLQ";

        final int maxRedeliveryCount = 2;

        final int sendMessages = 100;

        admin.topics().createPartitionedTopic(oldRetryTopic, 2);
        admin.topics().createPartitionedTopic(oldDeadLetterTopic, 2);

        Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .enableRetry(true)
                .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(maxRedeliveryCount).build())
                .receiverQueueSize(100)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        PulsarClient newPulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> deadLetterConsumer = newPulsarClient.newConsumer(Schema.BYTES)
                .topic(oldDeadLetterTopic)
                .subscriptionName("my-subscription")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic(topic)
                .create();

        for (int i = 0; i < sendMessages; i++) {
            producer.send(String.format("Hello Pulsar [%d]", i).getBytes());
        }

        producer.close();

        int totalReceived = 0;
        do {
            Message<byte[]> message = consumer.receive();
            log.info("consumer received message : {} {}", message.getMessageId(), new String(message.getData()));
            consumer.reconsumeLater(message, 1, TimeUnit.SECONDS);
            totalReceived++;
        } while (totalReceived < sendMessages * (maxRedeliveryCount + 1));

        int totalInDeadLetter = 0;
        do {
            Message message = deadLetterConsumer.receive();
            log.info("dead letter consumer received message : {} {}", message.getMessageId(), new String(message.getData()));
            deadLetterConsumer.acknowledge(message);
            totalInDeadLetter++;
        } while (totalInDeadLetter < sendMessages);

        deadLetterConsumer.close();
        consumer.close();

        Consumer<byte[]> checkConsumer = this.pulsarClient.newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Message<byte[]> checkMessage = checkConsumer.receive(3, TimeUnit.SECONDS);
        if (checkMessage != null) {
            log.info("check consumer received message : {} {}", checkMessage.getMessageId(), new String(checkMessage.getData()));
        }
        assertNull(checkMessage);

        checkConsumer.close();
        newPulsarClient.close();
    }

    /**
     * The test is disabled {@link https://github.com/apache/pulsar/issues/2647}.
     * @throws Exception
     */
    @Test
    public void testRetryTopicWithMultiTopic() throws Exception {
        final String topic1 = "persistent://my-property/my-ns/retry-topic-1";
        final String topic2 = "persistent://my-property/my-ns/retry-topic-2";

        final int maxRedeliveryCount = 2;

        int sendMessages = 100;

        // subscribe to the original topics before publish
        Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic(topic1, topic2)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .enableRetry(true)
                .ackTimeout(1, TimeUnit.SECONDS)
                .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(maxRedeliveryCount).build())
                .receiverQueueSize(100)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        // subscribe to the DLQ topics before consuming original topics
        Consumer<byte[]> deadLetterConsumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic("persistent://my-property/my-ns/retry-topic-1-my-subscription-DLQ")
                .subscriptionName("my-subscription")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Producer<byte[]> producer1 = pulsarClient.newProducer(Schema.BYTES)
                .topic(topic1)
                .create();

        Producer<byte[]> producer2 = pulsarClient.newProducer(Schema.BYTES)
                .topic(topic2)
                .create();

        for (int i = 0; i < sendMessages; i++) {
            producer1.send(String.format("Hello Pulsar [%d]", i).getBytes());
            producer2.send(String.format("Hello Pulsar [%d]", i).getBytes());
        }

        sendMessages = sendMessages * 2;

        producer1.close();
        producer2.close();

        int totalReceived = 0;
        do {
            Message<byte[]> message = consumer.receive();
            log.info("consumer received message : {} {} - total = {}",
                message.getMessageId(), new String(message.getData()), ++totalReceived);
        } while (totalReceived < sendMessages * (maxRedeliveryCount + 1));

        int totalInDeadLetter = 0;
        do {
            Message message = deadLetterConsumer.receive();
            log.info("dead letter consumer received message : {} {}", message.getMessageId(), new String(message.getData()));
            deadLetterConsumer.acknowledge(message);
            totalInDeadLetter++;
        } while (totalInDeadLetter < sendMessages);

        deadLetterConsumer.close();
        consumer.close();

        Consumer<byte[]> checkConsumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic(topic1, topic2)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Message<byte[]> checkMessage = checkConsumer.receive(3, TimeUnit.SECONDS);
        if (checkMessage != null) {
            log.info("check consumer received message : {} {}", checkMessage.getMessageId(), new String(checkMessage.getData()));
        }
        assertNull(checkMessage);

        checkConsumer.close();
    }

    @Test
    public void testRetryTopicByCustomTopicName() throws Exception {
        final String topic = "persistent://my-property/my-ns/retry-topic";
        final int maxRedeliveryCount = 2;
        final int sendMessages = 100;

        // subscribe before publish
        Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .enableRetry(true)
                .receiverQueueSize(100)
                .deadLetterPolicy(DeadLetterPolicy.builder()
                        .maxRedeliverCount(maxRedeliveryCount)
                        .retryLetterTopic("persistent://my-property/my-ns/my-subscription-custom-Retry")
                        .build())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        @Cleanup
        PulsarClient newPulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> deadLetterConsumer = newPulsarClient.newConsumer(Schema.BYTES)
                .topic("persistent://my-property/my-ns/retry-topic-my-subscription-DLQ")
                .subscriptionName("my-subscription")
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic(topic)
                .create();
        for (int i = 0; i < sendMessages; i++) {
            producer.send(String.format("Hello Pulsar [%d]", i).getBytes());
        }
        producer.close();

        int totalReceived = 0;
        do {
            Message<byte[]> message = consumer.receive();
            log.info("consumer received message : {} {}", message.getMessageId(), new String(message.getData()));
            consumer.reconsumeLater(message, 1, TimeUnit.SECONDS);
            totalReceived++;
        } while (totalReceived < sendMessages * (maxRedeliveryCount + 1));
        int totalInDeadLetter = 0;
        do {
            Message message = deadLetterConsumer.receive();
            log.info("dead letter consumer received message : {} {}", message.getMessageId(), new String(message.getData()));
            deadLetterConsumer.acknowledge(message);
            totalInDeadLetter++;
        } while (totalInDeadLetter < sendMessages);
        deadLetterConsumer.close();
        consumer.close();
        @Cleanup
        PulsarClient newPulsarClient1 = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> checkConsumer = newPulsarClient1.newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        Message<byte[]> checkMessage = checkConsumer.receive(3, TimeUnit.SECONDS);
        if (checkMessage != null) {
            log.info("check consumer received message : {} {}", checkMessage.getMessageId(), new String(checkMessage.getData()));
        }
        assertNull(checkMessage);
        checkConsumer.close();
    }


    @Test(timeOut = 30000L)
    public void testRetryTopicException() throws Exception {
        final String topic = "persistent://my-property/my-ns/retry-topic";
        final int maxRedeliveryCount = 2;
        final int sendMessages = 1;
        // subscribe before publish
        Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .enableRetry(true)
                .receiverQueueSize(100)
                .deadLetterPolicy(DeadLetterPolicy.builder()
                        .maxRedeliverCount(maxRedeliveryCount)
                        .retryLetterTopic("persistent://my-property/my-ns/my-subscription-custom-Retry")
                        .build())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic(topic)
                .create();
        for (int i = 0; i < sendMessages; i++) {
            producer.send(String.format("Hello Pulsar [%d]", i).getBytes());
        }
        producer.close();

        // mock a retry producer exception when reconsumelater is called
        MultiTopicsConsumerImpl<byte[]> multiTopicsConsumer = (MultiTopicsConsumerImpl<byte[]>) consumer;
        List<ConsumerImpl<byte[]>> consumers = multiTopicsConsumer.getConsumers();
        for (ConsumerImpl<byte[]> c : consumers) {
            Set<Field> deadLetterPolicyField =
                    ReflectionUtils.getAllFields(c.getClass(), ReflectionUtils.withName("deadLetterPolicy"));

            if (deadLetterPolicyField.size() != 0) {
                Field field = deadLetterPolicyField.iterator().next();
                field.setAccessible(true);
                DeadLetterPolicy deadLetterPolicy = (DeadLetterPolicy) field.get(c);
                deadLetterPolicy.setRetryLetterTopic("#persistent://invlaid-topic#");
            }
        }
        Message<byte[]> message = consumer.receive();
        log.info("consumer received message : {} {}", message.getMessageId(), new String(message.getData()));
        try {
            consumer.reconsumeLater(message, 1, TimeUnit.SECONDS);
        } catch (PulsarClientException.InvalidTopicNameException e) {
            assertEquals(e.getClass(), PulsarClientException.InvalidTopicNameException.class);
        } catch (Exception e) {
            fail("exception should be PulsarClientException.InvalidTopicNameException");
        }
        consumer.close();
    }

}
