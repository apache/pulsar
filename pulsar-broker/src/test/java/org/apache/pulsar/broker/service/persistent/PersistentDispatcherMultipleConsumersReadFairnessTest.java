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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * <p>This runs the real broker, producer and consumer. The consumer does not acknowledge the first
 * messages; the test re-adds their positions through the dispatcher, the broker-side effect of a
 * client that redelivers every message, so the replay queue stays busy through the real dispatch
 * path. With the bound disabled (0), strict replay priority is kept and the normal messages are not
 * dispatched; with a positive bound, they are dispatched while the storm goes on.
 */
@Test(groups = "broker-api")
public class PersistentDispatcherMultipleConsumersReadFairnessTest extends SharedPulsarBaseTest {
    private static final String SUBSCRIPTION = "sub";
    private static final String STORM_KEY = "storm-key";
    private static final String NORMAL_KEY = "normal-key";
    // More than two default read batches, so one replay round cannot drain the storm between refills.
    private static final int STORM_COUNT = 1000;
    private static final int BOUND_MILLIS = 10;

    @DataProvider(name = "subscriptionTypesAndBounds")
    public static Object[][] subscriptionTypesAndBounds() {
        return new Object[][]{
                {SubscriptionType.Shared, 0}, {SubscriptionType.Shared, BOUND_MILLIS},
                {SubscriptionType.Key_Shared, 0}, {SubscriptionType.Key_Shared, BOUND_MILLIS}};
    }

    @Test(dataProvider = "subscriptionTypesAndBounds", timeOut = 60000)
    public void testNormalReadsAreNotStarvedByRedeliveryStorm(SubscriptionType subscriptionType, int boundMillis)
            throws Exception {
        String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        Set<String> received = ConcurrentHashMap.newKeySet();
        List<String> normalReceived = new CopyOnWriteArrayList<>();
        Set<Position> stormPositions = ConcurrentHashMap.newKeySet();
        AtomicBoolean stormActive = new AtomicBoolean(true);

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
                     .messageListener((ignored, msg) -> {
                         received.add(msg.getValue());
                         if (msg.getValue().startsWith("storm")) {
                             MessageIdImpl messageId = (MessageIdImpl) msg.getMessageId();
                             stormPositions.add(PositionFactory.create(
                                     messageId.getLedgerId(), messageId.getEntryId()));
                         } else {
                             normalReceived.add(msg.getValue());
                         }
                     })
                     .subscribe()) {
            for (int i = 0; i < STORM_COUNT; i++) {
                producer.newMessage().key(STORM_KEY).value("storm-" + i).send();
            }
            Awaitility.await("the consumer receives the unacknowledged storm")
                    .atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> assertEquals(stormPositions.size(), STORM_COUNT));

            PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).get(5, TimeUnit.SECONDS)
                    .orElseThrow();
            PersistentSubscription subscription = (PersistentSubscription) topic.getSubscription(SUBSCRIPTION);
            PersistentDispatcherMultipleConsumers dispatcher =
                    (PersistentDispatcherMultipleConsumers) subscription.getDispatcher();
            dispatcher.setMaxReplayPriorityDurationInMillisForTesting(boundMillis);
            int stormStickyKeyHash = subscriptionType == SubscriptionType.Key_Shared
                    ? ((PersistentStickyKeyDispatcherMultipleConsumers) dispatcher).getSelector()
                            .makeStickyKeyHash(STORM_KEY.getBytes(UTF_8))
                    : -1;
            org.apache.pulsar.broker.service.Consumer brokerConsumer = subscription.getConsumers().get(0);

            Thread storm = new Thread(() -> {
                while (stormActive.get()) {
                    for (Position position : stormPositions) {
                        if (stormStickyKeyHash == -1) {
                            dispatcher.addMessageToReplay(position.getLedgerId(), position.getEntryId());
                        } else {
                            dispatcher.addMessageToReplay(position.getLedgerId(), position.getEntryId(),
                                    stormStickyKeyHash);
                        }
                    }
                    Thread.onSpinWait();
                }
            }, "replay-storm");
            storm.start();
            try {
                // Refill the replay set directly so the storm cannot drain while the refill waits for the dispatcher
                // monitor. The real consumer still receives every replayed duplicate through the normal send path.
                Awaitility.await("the replay queue is busy")
                        .atMost(Duration.ofSeconds(5))
                        .untilAsserted(() -> assertTrue(dispatcher.getNumberOfMessagesInReplay() > 0));
                dispatcher.readMoreEntriesAsync();

                List<Position> normalPositions = new ArrayList<>();
                for (String value : List.of("normal-1", "normal-2", "normal-3")) {
                    MessageIdImpl messageId = (MessageIdImpl) producer.newMessage()
                            .key(NORMAL_KEY).value(value).send();
                    normalPositions.add(PositionFactory.create(
                            messageId.getLedgerId(), messageId.getEntryId()));
                }

                if (boundMillis == 0) {
                    Awaitility.await("replays keep the strict priority when the bound is disabled")
                            .during(Duration.ofSeconds(1))
                            .atMost(Duration.ofSeconds(3))
                            .untilAsserted(() -> {
                                for (Position position : normalPositions) {
                                    assertFalse(brokerConsumer.getPendingAcks().contains(
                                            position.getLedgerId(), position.getEntryId()));
                                }
                                assertFalse(received.contains("normal-1"));
                                assertFalse(received.contains("normal-2"));
                                assertFalse(received.contains("normal-3"));
                            });
                } else {
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
                }
                assertTrue(dispatcher.getNumberOfMessagesInReplay() > 0,
                        "the replay storm is still active after the normal read");
            } finally {
                stormActive.set(false);
                storm.join(TimeUnit.SECONDS.toMillis(5));
            }
        } finally {
            admin.topics().delete(topicName, true);
        }
    }
}
