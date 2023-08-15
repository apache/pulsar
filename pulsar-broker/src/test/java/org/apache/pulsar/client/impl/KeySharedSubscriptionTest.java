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
package org.apache.pulsar.client.impl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Cleanup;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Test(groups = "broker-impl")
public class KeySharedSubscriptionTest extends ProducerConsumerBase {

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        conf.setMaxUnackedMessagesPerConsumer(10);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider
    public Object[][] subType() {
        return new Object[][] { { SubscriptionType.Shared }, { SubscriptionType.Key_Shared } };
    }

    @Test(dataProvider = "subType")
    public void testCanRecoverConsumptionWhenLiftMaxUnAckedMessagesRestriction(SubscriptionType subscriptionType)
            throws PulsarClientException {
        PulsarClient pulsarClient = PulsarClient.builder().
                serviceUrl(lookupUrl.toString())
                .build();
        final int totalMsg = 1000;
        String topic = "broker-close-test-" + RandomStringUtils.randomAlphabetic(5);
        Map<Consumer<?>, List<MessageId>> nameToId = Maps.newConcurrentMap();
        Set<MessageId> pubMessages = Sets.newConcurrentHashSet();
        Set<MessageId> recMessages = Sets.newConcurrentHashSet();
        AtomicLong lastActiveTime = new AtomicLong();
        AtomicBoolean canAcknowledgement = new AtomicBoolean(false);

        @Cleanup
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("sub-1")
                .subscriptionType(subscriptionType)
                .consumerName("con-1")
                .messageListener((cons1, msg) -> {
                    lastActiveTime.set(System.currentTimeMillis());
                    nameToId.computeIfAbsent(cons1,(k) -> new ArrayList<>())
                            .add(msg.getMessageId());
                    recMessages.add(msg.getMessageId());
                    if (canAcknowledgement.get()) {
                        try {
                            cons1.acknowledge(msg);
                        } catch (PulsarClientException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .subscribe();
        @Cleanup
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("sub-1")
                .subscriptionType(subscriptionType)
                .messageListener((cons2, msg) -> {
                    lastActiveTime.set(System.currentTimeMillis());
                    nameToId.computeIfAbsent(cons2,(k) -> new ArrayList<>())
                            .add(msg.getMessageId());
                    recMessages.add(msg.getMessageId());
                    if (canAcknowledgement.get()) {
                        try {
                            cons2.acknowledge(msg);
                        } catch (PulsarClientException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .consumerName("con-2")
                .subscribe();
        @Cleanup
        Consumer<byte[]> consumer3 = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("sub-1")
                .subscriptionType(subscriptionType)
                .messageListener((cons3, msg) -> {
                    lastActiveTime.set(System.currentTimeMillis());
                    nameToId.computeIfAbsent(cons3,(k) -> new ArrayList<>())
                            .add(msg.getMessageId());
                    recMessages.add(msg.getMessageId());
                    if (canAcknowledgement.get()) {
                        try {
                            cons3.acknowledge(msg);
                        } catch (PulsarClientException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .consumerName("con-3")
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .enableBatching(true)
                .batchingMaxPublishDelay(1, TimeUnit.MILLISECONDS)
                // We chose 9 because the maximum unacked message is 10
                .batchingMaxMessages(9)
                .create();

        for (int i = 0; i < totalMsg; i++) {
            producer.sendAsync(UUID.randomUUID().toString()
                            .getBytes(StandardCharsets.UTF_8))
                    .thenAccept(pubMessages::add);
        }

        // Wait for all consumers can not read more messages. the consumers are stuck by max unacked messages.
        Awaitility.await()
                .pollDelay(5, TimeUnit.SECONDS)
                .until(() ->
                        (System.currentTimeMillis() - lastActiveTime.get()) > TimeUnit.SECONDS.toMillis(5));

        // All consumers can acknowledge messages as they continue to receive messages.
        canAcknowledgement.set(true);

        // Acknowledgment of currently received messages to get out of stuck state due to unack message
        for (Map.Entry<Consumer<?>, List<MessageId>> entry : nameToId.entrySet()) {
            Consumer<?> consumer = entry.getKey();
            consumer.acknowledge(entry.getValue());
        }
        // refresh active time
        lastActiveTime.set(System.currentTimeMillis());

        // Wait for all consumers to continue receiving messages.
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollDelay(5, TimeUnit.SECONDS)
                .until(() ->
                        (System.currentTimeMillis() - lastActiveTime.get()) > TimeUnit.SECONDS.toMillis(5));

        //Determine if all messages have been received.
        //If the dispatcher is stuck, we can not receive enough messages.
        Assert.assertEquals(pubMessages.size(), totalMsg);
        Assert.assertEquals(pubMessages.size(), recMessages.size());
        Assert.assertTrue(recMessages.containsAll(pubMessages));
    }
}
