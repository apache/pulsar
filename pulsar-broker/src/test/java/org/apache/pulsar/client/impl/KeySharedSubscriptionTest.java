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
import lombok.Cleanup;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pulsar.client.api.*;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Test(groups = "broker-impl")
public class KeySharedSubscriptionTest extends ProducerConsumerBase {

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        conf.setMaxUnackedMessagesPerConsumer(1000);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testKeyShareSubscriptionWillNotStuck() throws PulsarClientException {
        PulsarClient pulsarClient = PulsarClient.builder().
                serviceUrl(lookupUrl.toString())
                .build();
        String topic = "broker-close-test-" + RandomStringUtils.randomAlphabetic(5);
        Map<Consumer<?>, List<MessageId>> nameToId = Maps.newConcurrentMap();
        AtomicLong lastActiveTime = new AtomicLong();
        AtomicInteger msgCount = new AtomicInteger();
        AtomicBoolean canAcknowledgement = new AtomicBoolean(false);

        @Cleanup
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("sub-1")
                .subscriptionType(SubscriptionType.Key_Shared)
                .consumerName("con-1")
                .messageListener((cons1, msg) -> {
                    lastActiveTime.set(System.currentTimeMillis());
                    nameToId.computeIfAbsent(cons1,(k) -> new ArrayList<>())
                            .add(msg.getMessageId());
                    msgCount.incrementAndGet();
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
                .subscriptionType(SubscriptionType.Key_Shared)
                .messageListener((cons2, msg) -> {
                    lastActiveTime.set(System.currentTimeMillis());
                    nameToId.computeIfAbsent(cons2,(k) -> new ArrayList<>())
                            .add(msg.getMessageId());
                    msgCount.incrementAndGet();
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
                .subscriptionType(SubscriptionType.Key_Shared)
                .messageListener((cons3, msg) -> {
                    lastActiveTime.set(System.currentTimeMillis());
                    nameToId.computeIfAbsent(cons3,(k) -> new ArrayList<>())
                            .add(msg.getMessageId());
                    msgCount.incrementAndGet();
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
                .create();
        for (int i = 0; i < 50000; i++) {
            producer.sendAsync(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        }

        while (true) {
            long differentTime = System.currentTimeMillis() - lastActiveTime.get();
            if (differentTime > TimeUnit.SECONDS.toMillis(20)) {
                break;
            }
        }

        canAcknowledgement.set(true);

        for (Map.Entry<Consumer<?>, List<MessageId>> entry : nameToId.entrySet()) {
            Consumer<?> consumer = entry.getKey();
            consumer.acknowledge(entry.getValue());
        }

        lastActiveTime.set(System.currentTimeMillis());

        while (true) {
            long differentTime = System.currentTimeMillis() - lastActiveTime.get();
            if (differentTime > TimeUnit.SECONDS.toMillis(20)) {
                break;
            }
        }

        Assert.assertTrue(msgCount.get() >= 50000);
    }
}
