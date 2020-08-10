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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.testng.Assert.fail;

public class MaxUnackedMessagesTest extends ProducerConsumerBase {
    private final String testTenant = "public";
    private final String testNamespace = "default";
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

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 20000)
    public void testMaxUnackedMessagesOnSubscriptionApi() throws Exception {
        final String topicName = testTopic + UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(topicName, 3);
        Integer max = admin.topics().getMaxUnackedMessagesOnSubscriptionPolicy(topicName);
        assertNull(max);

        admin.topics().setMaxUnackedMessagesOnSubscriptionPolicy(topicName, 2048);
        for (int i = 0; i < 50; i++) {
            if (admin.topics().getMaxUnackedMessagesOnSubscriptionPolicy(topicName) != null) {
                break;
            }
            Thread.sleep(100);
        }
        assertEquals(admin.topics().getMaxUnackedMessagesOnSubscriptionPolicy(topicName).intValue(), 2048);
        admin.topics().removeMaxUnackedMessagesOnSubscriptionPolicy(topicName);
        for (int i = 0; i < 50; i++) {
            if (admin.topics().getMaxUnackedMessagesOnSubscriptionPolicy(topicName) == null) {
                break;
            }
            Thread.sleep(100);
        }
        assertNull(admin.topics().getMaxUnackedMessagesOnSubscriptionPolicy(topicName));
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
        admin.topics().setMaxUnackedMessagesOnSubscriptionPolicy(topicName, unackMsgAllowed);
        for (int i = 0; i < 50; i++) {
            if (admin.topics().getMaxUnackedMessagesOnSubscriptionPolicy(topicName) != null) {
                break;
            }
            Thread.sleep(100);
        }

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
        Assert.assertEquals(messages.size(), unackMsgAllowed, receiverQueueSize * 3);

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
        Assert.assertEquals(result.size(), expectedRemainingMessages);

        producer.close();
        consumers.forEach(c -> {
            try {
                c.close();
            } catch (PulsarClientException e) {
            }
        });
    }
}
