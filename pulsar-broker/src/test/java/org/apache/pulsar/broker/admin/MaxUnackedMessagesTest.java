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
package org.apache.pulsar.broker.admin;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

@Test(groups = "broker-admin")
public class MaxUnackedMessagesTest extends ProducerConsumerBase {
    private final String testTenant = "my-property";
    private final String testNamespace = "my-ns";
    private final String myNamespace = testTenant + "/" + testNamespace;
    private final String testTopic = "persistent://" + myNamespace + "/max-unacked-";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        this.conf.setSystemTopicEnabled(true);
        this.conf.setTopicLevelPoliciesEnabled(true);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        resetConfig();
    }

    @Test(timeOut = 10000)
    public void testMaxUnackedMessagesOnSubscriptionApi() throws Exception {
        final String topicName = testTopic + UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(topicName, 3);
        waitCacheInit(topicName);
        Integer max = admin.topics().getMaxUnackedMessagesOnSubscription(topicName);
        assertNull(max);

        admin.topics().setMaxUnackedMessagesOnSubscription(topicName, 2048);
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.topics().getMaxUnackedMessagesOnSubscription(topicName)));
        assertEquals(admin.topics().getMaxUnackedMessagesOnSubscription(topicName).intValue(), 2048);
        admin.topics().removeMaxUnackedMessagesOnSubscription(topicName);
        Awaitility.await().untilAsserted(()
                -> assertNull(admin.topics().getMaxUnackedMessagesOnSubscription(topicName)));
        assertNull(admin.topics().getMaxUnackedMessagesOnSubscription(topicName));
    }

    // See https://github.com/apache/pulsar/issues/5438
    @Test(timeOut = 20000)
    public void testMaxUnackedMessagesOnSubscription() throws Exception {
        final String topicName = testTopic + System.currentTimeMillis();
        final String subscriberName = "test-sub" + System.currentTimeMillis();
        final int unackMsgAllowed = 100;
        final int receiverQueueSize = 10;
        final int totalProducedMsgs = 200;

        pulsar.getConfiguration().setMaxUnackedMessagesPerSubscription(unackMsgAllowed);
        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriberName).receiverQueueSize(receiverQueueSize)
                .subscriptionType(SubscriptionType.Shared);
        Consumer<byte[]> consumer1 = consumerBuilder.subscribe();
        Consumer<byte[]> consumer2 = consumerBuilder.subscribe();
        Consumer<byte[]> consumer3 = consumerBuilder.subscribe();
        List<Consumer<?>> consumers = Lists.newArrayList(consumer1, consumer2, consumer3);
        waitCacheInit(topicName);
        admin.topics().setMaxUnackedMessagesOnSubscription(topicName, unackMsgAllowed);
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.topics().getMaxUnackedMessagesOnSubscription(topicName)));
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();

        // (1) Produced Messages
        for (int i = 0; i < totalProducedMsgs; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        // (2) try to consume messages: but will be able to consume number of messages = unackMsgAllowed
        Message<?> msg = null;
        Map<Message<?>, Consumer<?>> messages = Maps.newHashMap();
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < totalProducedMsgs; j++) {
                msg = consumers.get(i).receive(500, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    messages.put(msg, consumers.get(i));
                } else {
                    break;
                }
            }
        }

        // client must receive number of messages = unAckedMessagesBufferSize rather all produced messages: check
        // delta as 3 consumers with receiverQueueSize = 10
        assertEquals(unackMsgAllowed, messages.size(), receiverQueueSize * 3);

        // start acknowledging messages
        messages.forEach((m, c) -> {
            try {
                c.acknowledge(m);
            } catch (PulsarClientException e) {
                fail("ack failed", e);
            }
        });

        // try to consume remaining messages: broker may take time to deliver so, retry multiple time to consume
        // all messages
        Set<MessageId> result = ConcurrentHashMap.newKeySet();
        // expecting messages which are not received
        int expectedRemainingMessages = totalProducedMsgs - messages.size();
        CountDownLatch latch = new CountDownLatch(expectedRemainingMessages);
        for (int i = 0; i < consumers.size(); i++) {
            final int consumerCount = i;
            for (int j = 0; j < totalProducedMsgs; j++) {
                consumers.get(i).receiveAsync().thenAccept(m -> {
                    result.add(m.getMessageId());
                    try {
                        consumers.get(consumerCount).acknowledge(m);
                    } catch (PulsarClientException e) {
                        fail("failed to ack msg", e);
                    }
                    latch.countDown();
                });
            }
        }

        latch.await(10, TimeUnit.SECONDS);

        // total received-messages should match to produced messages (it may have duplicate messages)
        assertEquals(result.size(), expectedRemainingMessages);

        producer.close();
        consumers.forEach(c -> {
            try {
                c.close();
            } catch (PulsarClientException e) {
            }
        });
    }

    @Test(timeOut = 20000)
    public void testMaxUnackedMessagesOnConsumerApi() throws Exception {
        final String topicName = testTopic + UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(topicName, 3);
        waitCacheInit(topicName);
        Integer max = admin.topics().getMaxUnackedMessagesOnConsumer(topicName);
        assertNull(max);

        admin.topics().setMaxUnackedMessagesOnConsumer(topicName, 2048);
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.topics().getMaxUnackedMessagesOnConsumer(topicName)));
        assertEquals(admin.topics().getMaxUnackedMessagesOnConsumer(topicName).intValue(), 2048);
        admin.topics().removeMaxUnackedMessagesOnConsumer(topicName);
        Awaitility.await().untilAsserted(()
                -> assertNull(admin.topics().getMaxUnackedMessagesOnConsumer(topicName)));
        assertNull(admin.topics().getMaxUnackedMessagesOnConsumer(topicName));
    }

    @Test(timeOut = 20000)
    public void testMaxUnackedMessagesOnConsumerAppliedApi() throws Exception {
        final String topicName = testTopic + UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(topicName, 3);
        waitCacheInit(topicName);
        Integer max = admin.topics().getMaxUnackedMessagesOnConsumer(topicName, true);
        assertEquals(max.intValue(), pulsar.getConfiguration().getMaxUnackedMessagesPerConsumer());

        admin.namespaces().setMaxUnackedMessagesPerConsumer(myNamespace, 15);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.namespaces().getMaxUnackedMessagesPerConsumer(myNamespace).intValue(), 15));
        admin.namespaces().removeMaxUnackedMessagesPerConsumer(myNamespace);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.namespaces().getMaxUnackedMessagesPerConsumer(myNamespace), null));

        admin.namespaces().setMaxUnackedMessagesPerConsumer(myNamespace, 10);
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.namespaces().getMaxUnackedMessagesPerConsumer(myNamespace)));
        max = admin.topics().getMaxUnackedMessagesOnConsumer(topicName, true);
        assertEquals(max.intValue(), 10);

        admin.topics().setMaxUnackedMessagesOnConsumer(topicName, 20);
        Awaitility.await().untilAsserted(() ->
                assertNotNull(admin.topics().getMaxUnackedMessagesOnConsumer(topicName)));
        max = admin.topics().getMaxUnackedMessagesOnConsumer(topicName, true);
        assertEquals(max.intValue(), 20);
    }

    @Test
    public void testMaxUnackedMessagesOnSubApplied() throws Exception {
        final String topicName = testTopic + UUID.randomUUID().toString();
        waitCacheInit(topicName);
        assertNull(admin.namespaces().getMaxUnackedMessagesPerSubscription(myNamespace));
        assertNull(admin.topics().getMaxUnackedMessagesOnSubscription(topicName));
        assertEquals(admin.topics().getMaxUnackedMessagesOnSubscription(topicName, true),
                Integer.valueOf(conf.getMaxUnackedMessagesPerSubscription()));

        admin.namespaces().setMaxUnackedMessagesPerSubscription(myNamespace, 10);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.namespaces().getMaxUnackedMessagesPerSubscription(myNamespace),
                Integer.valueOf(10)));

        admin.topics().setMaxUnackedMessagesOnSubscription(topicName, 20);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topics().getMaxUnackedMessagesOnSubscription(topicName), Integer.valueOf(20)));

        admin.topics().removeMaxUnackedMessagesOnSubscription(topicName);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.namespaces().getMaxUnackedMessagesPerSubscription(myNamespace),
                Integer.valueOf(10)));

        admin.namespaces().removeMaxUnackedMessagesPerSubscription(myNamespace);
        assertEquals(admin.topics().getMaxUnackedMessagesOnSubscription(topicName, true),
                Integer.valueOf(conf.getMaxUnackedMessagesPerSubscription()));
    }

    @Test(timeOut = 30000)
    public void testMaxUnackedMessagesOnConsumer() throws Exception {
        final String topicName = testTopic + System.currentTimeMillis();
        final String subscriberName = "test-sub" + System.currentTimeMillis();
        final int unackMsgAllowed = 100;
        final int receiverQueueSize = 10;
        final int totalProducedMsgs = 300;

        ConsumerBuilder<String> consumerBuilder = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionName(subscriberName).receiverQueueSize(receiverQueueSize)
                .ackTimeout(1, TimeUnit.MINUTES)
                .subscriptionType(SubscriptionType.Shared);
        @Cleanup
        Consumer<String> consumer1 = consumerBuilder.subscribe();
        // 1) Produced Messages
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();
        for (int i = 0; i < totalProducedMsgs; i++) {
            String message = "my-message-" + i;
            producer.send(message);
        }
        // 2) Unlimited, so all messages can be consumed
        int count = 0;
        List<Message<String>> list = new ArrayList<>(totalProducedMsgs);
        for (int i = 0; i < totalProducedMsgs; i++) {
            Message<String> message = consumer1.receive(1, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            count++;
            list.add(message);
        }
        assertEquals(count, totalProducedMsgs);
        list.forEach(message -> {
            try {
                consumer1.acknowledge(message);
            } catch (PulsarClientException e) {
            }
        });
        // 3) Set restrictions, so only part of the data can be consumed
        waitCacheInit(topicName);
        admin.topics().setMaxUnackedMessagesOnConsumer(topicName, unackMsgAllowed);
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.topics().getMaxUnackedMessagesOnConsumer(topicName)));
        assertEquals(admin.topics().getMaxUnackedMessagesOnConsumer(topicName).intValue(), unackMsgAllowed);
        // 4) Start 2 consumer, each consumer can only consume 100 messages
        @Cleanup
        Consumer<String> consumer2 = consumerBuilder.subscribe();
        @Cleanup
        Consumer<String> consumer3 = consumerBuilder.subscribe();
        for (int i = 0; i < totalProducedMsgs; i++) {
            String message = "my-message-" + i;
            producer.send(message);
        }
        AtomicInteger consumer2Counter = new AtomicInteger(0);
        AtomicInteger consumer3Counter = new AtomicInteger(0);
        CountDownLatch countDownLatch = new CountDownLatch(2);
        startConsumer(consumer2, consumer2Counter, countDownLatch);
        startConsumer(consumer3, consumer3Counter, countDownLatch);
        countDownLatch.await(10, TimeUnit.SECONDS);
        assertEquals(consumer2Counter.get(), unackMsgAllowed);
        assertEquals(consumer3Counter.get(), unackMsgAllowed);
    }

    private void startConsumer(Consumer<String> consumer, AtomicInteger consumerCounter,
                               CountDownLatch countDownLatch) {
        new Thread(() -> {
            while (true) {
                try {
                    Message<String> message = consumer.receive(500, TimeUnit.MILLISECONDS);
                    if (message == null) {
                        countDownLatch.countDown();
                        break;
                    }
                    consumerCounter.incrementAndGet();
                } catch (PulsarClientException e) {
                    break;
                }
            }
        }).start();
    }

    private void waitCacheInit(String topicName) throws Exception {
        pulsarClient.newConsumer().topic(topicName).subscriptionName("my-sub").subscribe().close();
        TopicName topic = TopicName.get(topicName);
    }
}
