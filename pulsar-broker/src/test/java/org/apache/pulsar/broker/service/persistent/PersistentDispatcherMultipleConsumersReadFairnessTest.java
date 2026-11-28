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
package org.apache.pulsar.broker.service.persistent;

import static org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers.BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Replays (redeliveries, due delayed messages) go before normal reads; a redelivery storm must not
 * starve the entries waiting to be read for longer than {@code dispatcherMaxReplayPriorityDurationInMillis}.
 *
 * <p>This runs the real broker, producer and consumer with the bound enabled. The consumer nacks
 * every storm delivery right away, the real redelivery path, so the replay queue stays busy through
 * the real dispatch path, and the normal messages must be dispatched while the storm goes on.
 * Whether the bound yields, and the strict priority of the disabled bound, are covered
 * deterministically at the dispatcher level in
 * {@link PersistentDispatcherMultipleConsumersReplayYieldTest}.
 */
@Test(groups = "broker-api")
public class PersistentDispatcherMultipleConsumersReadFairnessTest extends SharedPulsarBaseTest {
    private static final String SUBSCRIPTION = "sub";
    private static final String STORM_KEY = "storm-key";
    private static final String NORMAL_KEY = "normal-key";
    // More than two default read batches, so one replay round cannot drain the storm between refills.
    private static final int STORM_COUNT = 1000;
    private static final int BOUND_MILLIS = 10;

    @DataProvider(name = "subscriptionTypes")
    public static Object[][] subscriptionTypes() {
        return new Object[][]{{SubscriptionType.Shared}, {SubscriptionType.Key_Shared}};
    }

    // Whether the bound really yields is covered deterministically at the dispatcher level, where a pass can be
    // driven with a controlled clock (PersistentDispatcherMultipleConsumersReplayYieldTest). A live storm cannot
    // keep the replay queue continuously non-empty, so the strict priority of the disabled bound cannot be asserted
    // end to end; this test checks that a bounded dispatcher keeps delivering through a real nack storm.
    @Test(dataProvider = "subscriptionTypes", timeOut = 60000)
    public void testNormalReadsAreNotStarvedByRedeliveryStorm(SubscriptionType subscriptionType) throws Exception {
        String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        Set<String> received = ConcurrentHashMap.newKeySet();
        List<String> normalReceived = new CopyOnWriteArrayList<>();
        Set<Position> stormPositions = ConcurrentHashMap.newKeySet();
        AtomicLong stormDelivered = new AtomicLong();

        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .create();
             Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                     .topic(topicName)
                     .subscriptionName(SUBSCRIPTION)
                     .subscriptionType(subscriptionType)
                     .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                     .receiverQueueSize(10)
                     // A consumer that nacks every storm delivery right away: the real redelivery path
                     // removes the pending ack and decrements the unacked count, unlike a direct refill
                     // of the replay queue.
                     .negativeAckRedeliveryDelay(0, TimeUnit.MILLISECONDS)
                     .messageListener((c, msg) -> {
                         received.add(msg.getValue());
                         if (msg.getValue().startsWith("storm")) {
                             stormDelivered.incrementAndGet();
                             c.negativeAcknowledge(msg);
                             MessageIdImpl messageId = (MessageIdImpl) msg.getMessageId();
                             stormPositions.add(PositionFactory.create(
                                     messageId.getLedgerId(), messageId.getEntryId()));
                         } else {
                             normalReceived.add(msg.getValue());
                         }
                     })
                     .subscribe()) {
            // Enable the bound before the storm starts: the nacked replays of the first storm
            // messages would otherwise starve the normal read that delivers the remaining ones.
            PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).get(5, TimeUnit.SECONDS)
                    .orElseThrow();
            PersistentSubscription subscription = (PersistentSubscription) topic.getSubscription(SUBSCRIPTION);
            PersistentDispatcherMultipleConsumers dispatcher =
                    (PersistentDispatcherMultipleConsumers) subscription.getDispatcher();
            dispatcher.setMaxReplayPriorityDurationInMillisForTesting(BOUND_MILLIS);
            org.apache.pulsar.broker.service.Consumer brokerConsumer = subscription.getConsumers().get(0);

            for (int i = 0; i < STORM_COUNT; i++) {
                producer.newMessage().key(STORM_KEY).value("storm-" + i).send();
            }
            Awaitility.await("the consumer receives the storm")
                    .atMost(Duration.ofSeconds(20))
                    .untilAsserted(() -> assertEquals(stormPositions.size(), STORM_COUNT));

            Awaitility.await("the replay queue is busy with the nack storm")
                    .atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> assertTrue(dispatcher.getNumberOfMessagesInReplay() > 0));
            long stormDeliveredBefore = stormDelivered.get();

            List<Position> normalPositions = new ArrayList<>();
            for (String value : List.of("normal-1", "normal-2", "normal-3")) {
                MessageIdImpl messageId = (MessageIdImpl) producer.newMessage()
                        .key(NORMAL_KEY).value(value).send();
                normalPositions.add(PositionFactory.create(
                        messageId.getLedgerId(), messageId.getEntryId()));
            }

            Awaitility.await("the normal read gets through the replay storm")
                    .atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> {
                        for (Position position : normalPositions) {
                            assertTrue(brokerConsumer.getPendingAcks().contains(
                                    position.getLedgerId(), position.getEntryId()));
                        }
                        assertTrue(received.contains("normal-1"));
                        assertTrue(received.contains("normal-2"));
                        assertTrue(received.contains("normal-3"));
                        assertEquals(normalReceived, List.of("normal-1", "normal-2", "normal-3"),
                                "the normal messages arrive in order");
                    });
            // The queue is momentarily empty between a delivery and its scheduled redelivery, so
            // liveness is shown by deliveries still increasing, not by the queue's size.
            assertTrue(stormDelivered.get() > stormDeliveredBefore,
                    "replays kept being delivered throughout the check");
            assertEquals(BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.get(dispatcher), 0,
                    "the dispatcher is not blocked on unacked messages");
        } finally {
            admin.topics().delete(topicName, true);
        }
    }
}
