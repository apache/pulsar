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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Guards the un-acknowledged message accounting of a Shared subscription on the consumer-removal
 * path, for both the current ({@link PersistentDispatcherMultipleConsumers}) and the classic
 * ({@link PersistentDispatcherMultipleConsumersClassic}) dispatcher implementations.
 *
 * <p>{@code removeConsumer} must debit the subscription's un-acknowledged message count exactly
 * once per consumer, on the removal that actually unregisters it. That counter is what
 * {@code maxUnackedMessagesOnSubscription} throttles on, it feeds the broker-wide counter through
 * {@code addUnAckedMessages}, and nothing resets it while the dispatcher lives —
 * {@code clearComponentsAfterRemovedAllConsumers()} resets the available-permits aggregate but
 * deliberately leaves it alone — so a double debit silently raises the effective limit for the
 * lifetime of the dispatcher.
 */
@Test(groups = "broker-api")
public class SharedSubscriptionUnackedMessagesAccountingTest extends SharedPulsarBaseTest {

    private static final String SUBSCRIPTION = "shared-churn-sub";
    private static final int RECEIVER_QUEUE_SIZE = 5;
    private static final int UNACKED_MESSAGES = 10;

    private static final String CLASSIC_DISPATCHER_FLAG = "subscriptionSharedUseClassicPersistentImplementation";

    /**
     * Deterministic probe for the double-debit of the subscription's un-acknowledged message count
     * on the consumer-removal path.
     *
     * <p>{@code PersistentDispatcherMultipleConsumers#removeConsumer(Consumer)} debits the
     * subscription by the departing consumer's un-acknowledged message count before it establishes
     * whether that consumer was still registered at all. Removing the same consumer twice — which
     * the defensive path of <a href="https://github.com/apache/pulsar/pull/22270">apache/pulsar#22270</a>
     * exists precisely to tolerate — therefore debits the same deliveries twice and drives the
     * subscription counter negative. That counter is what
     * {@code maxUnackedMessagesOnSubscription} throttles on, so a negative value silently disables
     * the throttle for the lifetime of the dispatcher.
     *
     * <p>A single consumer is attached on purpose: with a second consumer connected, the first
     * removal replays the departing consumer's pending acknowledgements to the survivor, which
     * credits the counter again on a timing the test cannot observe. Removing the only consumer
     * takes {@code clearComponentsAfterRemovedAllConsumers()}, which resets the available-permits
     * aggregate but deliberately leaves the un-acknowledged count alone, so the double debit stays
     * observable.
     */
    @Test(timeOut = 60_000)
    public void testRemovingSameConsumerTwiceDebitsUnackedMessagesOnce() throws Exception {
        final String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);

        try (PulsarClient departingClient = newPulsarClient();
             Producer<byte[]> producer = pulsarClient.newProducer()
                     .topic(topicName)
                     .enableBatching(false)
                     .create()) {
            Consumer<byte[]> departing = departingClient.newConsumer(Schema.BYTES)
                    .topic(topicName)
                    .subscriptionName(SUBSCRIPTION)
                    .subscriptionType(SubscriptionType.Shared)
                    .consumerName("departing")
                    .receiverQueueSize(RECEIVER_QUEUE_SIZE)
                    .subscribe();

            for (int i = 0; i < UNACKED_MESSAGES; i++) {
                producer.send(("unacked-" + i).getBytes(StandardCharsets.UTF_8));
            }
            for (int i = 0; i < UNACKED_MESSAGES; i++) {
                assertNotNull(departing.receive(30, TimeUnit.SECONDS),
                        "the consumer did not receive the delivery it has to leave un-acknowledged");
            }

            PersistentDispatcherMultipleConsumers dispatcher = sharedDispatcher(topicName);
            org.apache.pulsar.broker.service.Consumer brokerConsumer =
                    brokerConsumer(dispatcher, "departing");
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertEquals(brokerConsumer.getUnackedMessages(), UNACKED_MESSAGES);
                assertEquals(dispatcher.totalUnackedMessages, UNACKED_MESSAGES);
            });

            dispatcher.removeConsumer(brokerConsumer);
            // The consumer is no longer registered, so this removal must not debit its
            // un-acknowledged messages a second time.
            dispatcher.removeConsumer(brokerConsumer);

            assertEquals(dispatcher.totalUnackedMessages, 0,
                    "removing an already-removed consumer debited its " + UNACKED_MESSAGES
                            + " un-acknowledged messages from the subscription a second time");

            departing.close();
        }
    }

    /**
     * The classic dispatcher ({@code subscriptionSharedUseClassicPersistentImplementation=true},
     * the documented PIP-379 rollback path) carries the identical unguarded debit in its own
     * {@code removeConsumer}, so the same probe is run against it. The flag is dynamic and the
     * dispatcher implementation is chosen when the first consumer attaches, so it is flipped for
     * the duration of this test only and restored afterwards.
     */
    @Test(timeOut = 60_000)
    public void testRemovingSameConsumerTwiceDebitsUnackedMessagesOnceOnClassicDispatcher() throws Exception {
        admin.brokers().updateDynamicConfiguration(CLASSIC_DISPATCHER_FLAG, "true");
        try {
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertTrue(
                    getPulsar().getConfiguration().isSubscriptionSharedUseClassicPersistentImplementation(),
                    "the classic-dispatcher flag did not propagate to the broker"));

            final String topicName = newTopicName();
            admin.topics().createNonPartitionedTopic(topicName);

            try (PulsarClient departingClient = newPulsarClient();
                 Producer<byte[]> producer = pulsarClient.newProducer()
                         .topic(topicName)
                         .enableBatching(false)
                         .create()) {
                Consumer<byte[]> departing = departingClient.newConsumer(Schema.BYTES)
                        .topic(topicName)
                        .subscriptionName(SUBSCRIPTION)
                        .subscriptionType(SubscriptionType.Shared)
                        .consumerName("departing")
                        .receiverQueueSize(RECEIVER_QUEUE_SIZE)
                        .subscribe();

                for (int i = 0; i < UNACKED_MESSAGES; i++) {
                    producer.send(("unacked-" + i).getBytes(StandardCharsets.UTF_8));
                }
                for (int i = 0; i < UNACKED_MESSAGES; i++) {
                    assertNotNull(departing.receive(30, TimeUnit.SECONDS),
                            "the consumer did not receive the delivery it has to leave un-acknowledged");
                }

                PersistentDispatcherMultipleConsumersClassic dispatcher = classicDispatcher(topicName);
                org.apache.pulsar.broker.service.Consumer brokerConsumer =
                        brokerConsumer(dispatcher, "departing");
                Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                    assertEquals(brokerConsumer.getUnackedMessages(), UNACKED_MESSAGES);
                    assertEquals(dispatcher.totalUnackedMessages, UNACKED_MESSAGES);
                });

                dispatcher.removeConsumer(brokerConsumer);
                // The consumer is no longer registered, so this removal must not debit its
                // un-acknowledged messages a second time.
                dispatcher.removeConsumer(brokerConsumer);

                assertEquals(dispatcher.totalUnackedMessages, 0,
                        "removing an already-removed consumer debited its " + UNACKED_MESSAGES
                                + " un-acknowledged messages from the subscription a second time");

                departing.close();
            }
        } finally {
            admin.brokers().updateDynamicConfiguration(CLASSIC_DISPATCHER_FLAG, "false");
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertFalse(
                    getPulsar().getConfiguration().isSubscriptionSharedUseClassicPersistentImplementation(),
                    "the classic-dispatcher flag was not restored"));
        }
    }

    private PersistentDispatcherMultipleConsumers sharedDispatcher(String topicName) {
        AbstractPersistentDispatcherMultipleConsumers dispatcher = dispatcher(topicName);
        assertTrue(dispatcher instanceof PersistentDispatcherMultipleConsumers,
                "expected the current dispatcher implementation, got " + dispatcher.getClass().getSimpleName());
        return (PersistentDispatcherMultipleConsumers) dispatcher;
    }

    private PersistentDispatcherMultipleConsumersClassic classicDispatcher(String topicName) {
        AbstractPersistentDispatcherMultipleConsumers dispatcher = dispatcher(topicName);
        assertTrue(dispatcher instanceof PersistentDispatcherMultipleConsumersClassic,
                "expected the classic dispatcher implementation, got " + dispatcher.getClass().getSimpleName());
        return (PersistentDispatcherMultipleConsumersClassic) dispatcher;
    }

    private AbstractPersistentDispatcherMultipleConsumers dispatcher(String topicName) {
        PersistentTopic topic = (PersistentTopic) getTopicIfExists(topicName).join()
                .orElseThrow(() -> new IllegalStateException("topic is not loaded: " + topicName));
        PersistentSubscription subscription = topic.getSubscription(SUBSCRIPTION);
        assertNotNull(subscription, "subscription is missing: " + SUBSCRIPTION);
        return (AbstractPersistentDispatcherMultipleConsumers) subscription.getDispatcher();
    }

    private org.apache.pulsar.broker.service.Consumer brokerConsumer(
            AbstractPersistentDispatcherMultipleConsumers dispatcher, String consumerName) {
        return dispatcher.getConsumers().stream()
                .filter(consumer -> consumerName.equals(consumer.consumerName()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("consumer is not connected: " + consumerName));
    }
}
