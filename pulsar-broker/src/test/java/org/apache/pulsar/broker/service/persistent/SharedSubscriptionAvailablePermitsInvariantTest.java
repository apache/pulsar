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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Guards the available-permits accounting of a Shared subscription against consumer churn.
 *
 * <p>Observed production signature this test is derived from: after a window mixing a cursor reset
 * performed while consumers were still attached, a scale-down from many consumers to one, and
 * consumer processes dying while holding un-acknowledged messages, a persistent topic's Shared
 * subscription permanently stopped dispatching. The broker reported the subscription's available
 * permits as a large negative number (order of -10^5) and no amount of client-side reconnecting or
 * re-subscribing recovered it, because every re-subscribe only credits a single receiver-queue
 * window while the deficit stays. Only {@code pulsar-admin topics unload} restored dispatching, as
 * unloading rebuilds the dispatcher and its consumers from scratch.
 *
 * <p>The Pulsar binary protocol only carries monotonically increasing client-to-broker permit
 * increments ({@code CommandFlow}), so a negative permit count can only originate from broker-side
 * bookkeeping: permits debited more than once, or debited without ever having been credited.
 *
 * <p>Two invariants are asserted here:
 * <ul>
 *   <li>the dispatcher's per-subscription aggregate {@code totalAvailablePermits} never settles on a
 *       negative value;</li>
 *   <li>no connected consumer settles on a negative {@code availablePermits} — a consumer whose
 *       permits are negative is permanently skipped by
 *       {@code PersistentDispatcherMultipleConsumers#getFirstAvailableConsumerPermits()}, which is
 *       what turns a bookkeeping deficit into a wedged subscription.</li>
 * </ul>
 *
 * <p>Both are asserted after the subscription has quiesced, so that the transient skew the
 * dispatcher tolerates by design (see the {@code Math.max(totalAvailablePermits,
 * firstAvailableConsumerPermits)} guard in {@code readMoreEntries()}) does not fail the test; only a
 * persistent deficit does.
 */
@CustomLog
@Test(groups = "broker-api")
public class SharedSubscriptionAvailablePermitsInvariantTest extends SharedPulsarBaseTest {

    private static final String SUBSCRIPTION = "shared-churn-sub";
    private static final int INITIAL_CONSUMERS = 6;
    private static final int RECEIVER_QUEUE_SIZE = 5;
    private static final int BACKLOG_SIZE = 400;
    private static final int CHURN_ROUNDS = 4;
    private static final int FINAL_BATCH_SIZE = 30;
    private static final int UNACKED_MESSAGES = 10;
    /** Fixed seed so the churn sequence is reproducible across runs. */
    private static final long CHURN_SEED = 20260101L;

    /**
     * Drives a Shared subscription through repeated rounds of consumer churn that mix the three
     * triggers of the production incident — consumers dying abruptly while holding un-acknowledged
     * messages, a cursor reset performed with consumers still attached, and a scale-down followed by
     * re-subscribes — and asserts after every round that neither the subscription aggregate nor any
     * connected consumer has settled on a negative available-permits value. The subscription must
     * still be able to drain a freshly published batch at the end.
     */
    @Test(timeOut = 180_000)
    public void testSubscriptionAvailablePermitsNeverNegativeUnderConsumerChurn() throws Exception {
        final String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, SUBSCRIPTION, MessageId.earliest);

        final List<ChurnConsumer> allConsumers = new ArrayList<>();
        final List<ChurnConsumer> liveConsumers = new ArrayList<>();
        try (Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .enableBatching(false)
                .create()) {

            for (int i = 0; i < BACKLOG_SIZE; i++) {
                producer.send(("backlog-" + i).getBytes(StandardCharsets.UTF_8));
            }

            for (int i = 0; i < INITIAL_CONSUMERS; i++) {
                ChurnConsumer churnConsumer = newChurnConsumer(topicName, "initial-" + i);
                allConsumers.add(churnConsumer);
                liveConsumers.add(churnConsumer);
            }
            awaitConnectedConsumers(topicName, INITIAL_CONSUMERS);

            final Random random = new Random(CHURN_SEED);
            for (int round = 0; round < CHURN_ROUNDS; round++) {
                // (a) Pull messages into flight and deliberately leave them un-acknowledged, so that
                // every consumer removed below still holds outstanding deliveries.
                for (ChurnConsumer churnConsumer : liveConsumers) {
                    int toReceive = 1 + random.nextInt(RECEIVER_QUEUE_SIZE);
                    for (int i = 0; i < toReceive; i++) {
                        Message<byte[]> message = churnConsumer.consumer.receive(200, TimeUnit.MILLISECONDS);
                        if (message == null) {
                            break;
                        }
                    }
                }

                // (b) Abrupt process death: the whole dedicated client is shut down, so the broker
                // sees the connection drop without ever receiving a CloseConsumer command.
                ChurnConsumer killed = liveConsumers.remove(random.nextInt(liveConsumers.size()));
                killed.client.shutdown();
                killed.closed = true;

                // (c) Connection drop with automatic re-subscribe of the same client consumer.
                ChurnConsumer reconnected = liveConsumers.get(random.nextInt(liveConsumers.size()));
                ConsumerImpl<byte[]> reconnectedImpl = (ConsumerImpl<byte[]>) reconnected.consumer;
                if (reconnectedImpl.getClientCnx() != null) {
                    reconnectedImpl.getClientCnx().close();
                }

                // (d) Graceful close, which does send a CloseConsumer command.
                ChurnConsumer gracefullyClosed = liveConsumers.remove(random.nextInt(liveConsumers.size()));
                gracefullyClosed.consumer.close();
                gracefullyClosed.client.close();
                gracefullyClosed.closed = true;

                // (e) Cursor reset while the surviving consumers stay attached.
                try {
                    admin.topics().resetCursor(topicName, SUBSCRIPTION, MessageId.earliest);
                } catch (PulsarAdminException e) {
                    log.warn().attr("round", round).exceptionMessage(e)
                            .log("resetCursor with attached consumers was rejected");
                }

                // (f) Re-subscribe, restoring the consumer count.
                for (int i = 0; i < 2; i++) {
                    ChurnConsumer churnConsumer = newChurnConsumer(topicName, "round" + round + "-" + i);
                    allConsumers.add(churnConsumer);
                    liveConsumers.add(churnConsumer);
                }

                assertAvailablePermitsNotNegative(topicName, round);
            }

            // The subscription must not be wedged: skip the accumulated backlog, publish a fresh
            // batch and require the surviving consumers to drain all of it.
            admin.topics().resetCursor(topicName, SUBSCRIPTION, MessageId.latest);
            awaitConnectedConsumers(topicName, liveConsumers.size());
            for (int i = 0; i < FINAL_BATCH_SIZE; i++) {
                producer.send(("final-" + i).getBytes(StandardCharsets.UTF_8));
            }

            Set<String> drained = new HashSet<>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (drained.size() < FINAL_BATCH_SIZE && System.nanoTime() < deadline) {
                for (ChurnConsumer churnConsumer : liveConsumers) {
                    Message<byte[]> message = churnConsumer.consumer.receive(100, TimeUnit.MILLISECONDS);
                    if (message != null) {
                        String payload = new String(message.getData(), StandardCharsets.UTF_8);
                        if (payload.startsWith("final-")) {
                            drained.add(payload);
                        }
                        churnConsumer.consumer.acknowledge(message);
                    }
                }
            }
            assertEquals(drained.size(), FINAL_BATCH_SIZE,
                    "Shared subscription is wedged after consumer churn: only " + drained.size() + " of "
                            + FINAL_BATCH_SIZE + " freshly published messages were dispatched, "
                            + describePermits(topicName));
        } finally {
            for (ChurnConsumer churnConsumer : allConsumers) {
                churnConsumer.closeQuietly();
            }
        }
    }

    /**
     * Deterministic probe for the debit-without-credit window on the consumer-removal path.
     *
     * <p>{@code Consumer#flowPermits(int)} credits the consumer's own permit counter synchronously
     * on the connection thread and only then hands the increment to the dispatcher, which applies it
     * to the subscription aggregate on the broker executor and drops it when the consumer is no
     * longer registered. {@code PersistentDispatcherMultipleConsumers#removeConsumer(Consumer)}
     * meanwhile debits the aggregate by the consumer's full permit counter, including an increment
     * that has not been applied to the aggregate yet.
     *
     * <p>The interleaving is forced deterministically by holding the dispatcher monitor — the same
     * monitor that both {@code removeConsumer} and the deferred flow handler synchronize on — for
     * the duration of the flow and the removal. That is the interleaving that occurs naturally
     * whenever the broker executor is busy when a consumer leaves.
     */
    @Test(timeOut = 60_000)
    public void testRemovingConsumerDoesNotDebitPermitsThatWereNeverCredited() throws Exception {
        final String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);

        try (PulsarClient survivorClient = newPulsarClient();
             PulsarClient departingClient = newPulsarClient()) {
            Consumer<byte[]> survivor = survivorClient.newConsumer(Schema.BYTES)
                    .topic(topicName)
                    .subscriptionName(SUBSCRIPTION)
                    .subscriptionType(SubscriptionType.Shared)
                    .consumerName("survivor")
                    .receiverQueueSize(RECEIVER_QUEUE_SIZE)
                    .subscribe();
            Consumer<byte[]> departing = departingClient.newConsumer(Schema.BYTES)
                    .topic(topicName)
                    .subscriptionName(SUBSCRIPTION)
                    .subscriptionType(SubscriptionType.Shared)
                    .consumerName("departing")
                    .receiverQueueSize(RECEIVER_QUEUE_SIZE)
                    .subscribe();

            awaitConnectedConsumers(topicName, 2);
            PersistentDispatcherMultipleConsumers dispatcher = sharedDispatcher(topicName);
            org.apache.pulsar.broker.service.Consumer brokerConsumer =
                    brokerConsumer(dispatcher, "departing");

            int aggregateBefore = dispatcher.totalAvailablePermits;
            // Large enough that the aggregate cannot stay non-negative if this credit is dropped
            // while the matching debit is applied.
            int flowPermits = Math.max(aggregateBefore, 0) + 1000;

            synchronized (dispatcher) {
                // Credits the consumer's own counter now; the aggregate credit is deferred to the
                // broker executor, which blocks on this monitor.
                brokerConsumer.flowPermits(flowPermits);
                // Debits the aggregate by the consumer's full counter, deferred increment included,
                // and unregisters the consumer so the deferred credit is discarded.
                dispatcher.removeConsumer(brokerConsumer);
            }

            Awaitility.await()
                    .pollDelay(Duration.ofSeconds(1))
                    .atMost(Duration.ofSeconds(15))
                    .untilAsserted(() -> {
                        int aggregateAfter = dispatcher.totalAvailablePermits;
                        assertTrue(aggregateAfter >= 0,
                                "subscription aggregate availablePermits went negative after removing a consumer"
                                        + " whose in-flight flow credit was discarded: before=" + aggregateBefore
                                        + ", flow=" + flowPermits + ", after=" + aggregateAfter);
                    });

            survivor.close();
            departing.close();
        }
    }

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

    private ChurnConsumer newChurnConsumer(String topicName, String consumerName) throws Exception {
        PulsarClient client = newPulsarClient();
        Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
                .topic(topicName)
                .subscriptionName(SUBSCRIPTION)
                .subscriptionType(SubscriptionType.Shared)
                .consumerName(consumerName)
                .receiverQueueSize(RECEIVER_QUEUE_SIZE)
                .subscribe();
        return new ChurnConsumer(client, consumer);
    }

    private PersistentDispatcherMultipleConsumers sharedDispatcher(String topicName) {
        PersistentTopic topic = (PersistentTopic) getTopicIfExists(topicName).join()
                .orElseThrow(() -> new IllegalStateException("topic is not loaded: " + topicName));
        PersistentSubscription subscription = topic.getSubscription(SUBSCRIPTION);
        assertNotNull(subscription, "subscription is missing: " + SUBSCRIPTION);
        return (PersistentDispatcherMultipleConsumers) subscription.getDispatcher();
    }

    private org.apache.pulsar.broker.service.Consumer brokerConsumer(
            PersistentDispatcherMultipleConsumers dispatcher, String consumerName) {
        return dispatcher.getConsumers().stream()
                .filter(consumer -> consumerName.equals(consumer.consumerName()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("consumer is not connected: " + consumerName));
    }

    private void awaitConnectedConsumers(String topicName, int expected) {
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            PersistentDispatcherMultipleConsumers dispatcher = sharedDispatcher(topicName);
            assertNotNull(dispatcher, "dispatcher is missing");
            assertEquals(dispatcher.getConsumers().size(), expected);
        });
    }

    private void assertAvailablePermitsNotNegative(String topicName, int round) {
        Awaitility.await()
                .pollDelay(Duration.ofSeconds(1))
                .atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> {
                    PersistentDispatcherMultipleConsumers dispatcher = sharedDispatcher(topicName);
                    assertNotNull(dispatcher, "dispatcher is missing after churn round " + round);

                    int aggregate = dispatcher.totalAvailablePermits;
                    assertTrue(aggregate >= 0, "churn round " + round
                            + ": subscription aggregate availablePermits is negative: " + aggregate);

                    for (org.apache.pulsar.broker.service.Consumer consumer : dispatcher.getConsumers()) {
                        assertTrue(consumer.getAvailablePermits() >= 0, "churn round " + round + ": consumer "
                                + consumer.consumerName() + " has negative availablePermits: "
                                + consumer.getAvailablePermits());
                    }

                    SubscriptionStats stats =
                            admin.topics().getStats(topicName).getSubscriptions().get(SUBSCRIPTION);
                    assertNotNull(stats, "subscription stats are missing after churn round " + round);
                    int reported = 0;
                    for (ConsumerStats consumerStats : stats.getConsumers()) {
                        assertTrue(consumerStats.getAvailablePermits() >= 0, "churn round " + round + ": consumer "
                                + consumerStats.getConsumerName() + " reports negative availablePermits: "
                                + consumerStats.getAvailablePermits());
                        reported += consumerStats.getAvailablePermits();
                    }
                    assertTrue(reported >= 0, "churn round " + round
                            + ": reported available permits sum is negative: " + reported);
                });
    }

    private String describePermits(String topicName) {
        PersistentDispatcherMultipleConsumers dispatcher = sharedDispatcher(topicName);
        if (dispatcher == null) {
            return "no dispatcher";
        }
        StringBuilder builder = new StringBuilder("aggregate availablePermits=")
                .append(dispatcher.totalAvailablePermits);
        for (org.apache.pulsar.broker.service.Consumer consumer : dispatcher.getConsumers()) {
            builder.append(", ").append(consumer.consumerName()).append("=").append(consumer.getAvailablePermits());
        }
        return builder.toString();
    }

    private static final class ChurnConsumer {
        private final PulsarClient client;
        private final Consumer<byte[]> consumer;
        private boolean closed;

        private ChurnConsumer(PulsarClient client, Consumer<byte[]> consumer) {
            this.client = client;
            this.consumer = consumer;
        }

        private void closeQuietly() {
            if (closed) {
                return;
            }
            closed = true;
            try {
                client.shutdown();
            } catch (Exception e) {
                log.warn().exceptionMessage(e).log("Failed to shut down churn client");
            }
        }
    }
}
