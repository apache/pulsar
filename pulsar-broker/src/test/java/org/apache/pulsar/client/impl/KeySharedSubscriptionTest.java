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
package org.apache.pulsar.client.impl;

import static org.apache.pulsar.broker.BrokerTestUtil.newUniqueName;
import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.collect.Sets;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.SneakyThrows;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentStickyKeyDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.SubscriptionType;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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
        final int totalMsg = 1000;
        String topic = newUniqueName("broker-close-test");
        String subscriptionName = "sub-1";
        Map<Consumer<?>, List<MessageId>> unackedMessages = new ConcurrentHashMap<>();
        Set<MessageId> pubMessages = Sets.newConcurrentHashSet();
        Set<MessageId> recMessages = Sets.newConcurrentHashSet();
        AtomicLong lastActiveTime = new AtomicLong();
        AtomicBoolean canAcknowledgement = new AtomicBoolean(false);

        if (subscriptionType == SubscriptionType.Key_Shared) {
            // create and close consumer to create the dispatcher so that the selector can be used
            pulsarClient.newConsumer()
                    .topic(topic)
                    .subscriptionName(subscriptionName)
                    .subscriptionType(subscriptionType)
                    .subscribe()
                    .close();
        }

        List<Consumer<?>> consumerList = new ArrayList<>();
        // create 3 consumers
        for (int i = 0; i < 3; i++) {
            ConsumerBuilder<byte[]> builder = pulsarClient.newConsumer()
                    .topic(topic)
                    .consumerName("consumer-" + i)
                    .subscriptionName(subscriptionName)
                    .subscriptionType(subscriptionType)
                    .messageListener((consumer, msg) -> {
                        lastActiveTime.set(System.currentTimeMillis());
                        recMessages.add(msg.getMessageId());
                        if (canAcknowledgement.get()) {
                            try {
                                consumer.acknowledge(msg);
                            } catch (PulsarClientException e) {
                                throw new RuntimeException(e);
                            }
                        } else {
                            unackedMessages.computeIfAbsent(consumer,
                                            (k) -> Collections.synchronizedList(new ArrayList<>()))
                                    .add(msg.getMessageId());
                        }
                    });

            if (subscriptionType == SubscriptionType.Key_Shared) {
                // ensure every consumer can be distributed messages
                int stickyKeyHash = getSelector(topic, subscriptionName).makeStickyKeyHash(("key-" + i).getBytes());
                builder.keySharedPolicy(KeySharedPolicy.stickyHashRange()
                        .ranges(Range.of(stickyKeyHash, stickyKeyHash)));
            }

            consumerList.add(builder.subscribe());
        }

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .enableBatching(true)
                .batchingMaxPublishDelay(1, TimeUnit.MILLISECONDS)
                // We chose 9 because the maximum unacked message is 10
                .batchingMaxMessages(9)
                .batcherBuilder(BatcherBuilder.KEY_BASED)
                .create();

        for (int i = 0; i < totalMsg; i++) {
            producer.newMessage()
                    .key("key-" + (i % 3))
                    .value(("message-" + i).getBytes(StandardCharsets.UTF_8))
                    .sendAsync().thenAccept(pubMessages::add);
        }

        // Wait for all consumers can not read more messages. the consumers are stuck by max unacked messages.
        waitUntilLastActiveTimeNoLongerGetsUpdated(lastActiveTime);

        // All consumers can acknowledge messages as they continue to receive messages.
        canAcknowledgement.set(true);

        // Acknowledgment of currently received messages to get out of stuck state due to unack message
        for (Map.Entry<Consumer<?>, List<MessageId>> entry : unackedMessages.entrySet()) {
            Consumer<?> consumer = entry.getKey();
            List<MessageId> messageIdList = entry.getValue();
            consumer.acknowledge(messageIdList);
        }

        // refresh active time
        lastActiveTime.set(System.currentTimeMillis());

        // Wait for all consumers to continue receiving messages.
        waitUntilLastActiveTimeNoLongerGetsUpdated(lastActiveTime);

        logTopicStats(topic);

        //Determine if all messages have been received.
        //If the dispatcher is stuck, we can not receive enough messages.
        Assert.assertEquals(totalMsg, pubMessages.size());
        assertThat(recMessages).containsExactlyInAnyOrderElementsOf(pubMessages);

        // cleanup
        producer.close();
        for (Consumer<?> consumer : consumerList) {
            consumer.close();
        }
    }

    private static void waitUntilLastActiveTimeNoLongerGetsUpdated(AtomicLong lastActiveTime) {
        Awaitility.await()
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(() -> System.currentTimeMillis() - lastActiveTime.get() > TimeUnit.SECONDS.toMillis(1));
    }

    @SneakyThrows
    private StickyKeyConsumerSelector getSelector(String topic, String subscription) {
        Topic t = pulsar.getBrokerService().getTopicIfExists(topic).get().get();
        PersistentSubscription sub = (PersistentSubscription) t.getSubscription(subscription);
        PersistentStickyKeyDispatcherMultipleConsumers dispatcher =
                (PersistentStickyKeyDispatcherMultipleConsumers) sub.getDispatcher();
        return dispatcher.getSelector();
    }
}
