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
package org.apache.pulsar.client.api.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Coverage for multiple consumers attached to the same subscription on a multi-segment
 * scalable topic. Each consumer type has different multiplicity semantics:
 * <ul>
 *   <li><b>QueueConsumer</b> — Shared subscription per segment. Multiple consumers share
 *       the load: messages distribute across consumers, every message is delivered
 *       exactly once across the consumer set.</li>
 *   <li><b>StreamConsumer</b> — Exclusive subscription per segment. A second consumer
 *       on the same subscription collides on segment attach (broker enforces the
 *       exclusive lock); per-consumer segment assignment via a subscription coordinator
 *       is a separate, not-yet-implemented feature.</li>
 *   <li><b>CheckpointConsumer</b> — uses readers, no broker-side cursor. Multiple
 *       checkpoint consumers each independently read the full stream — no load
 *       balancing, full duplication.</li>
 * </ul>
 */
public class V5MultipleConsumersTest extends V5ClientBaseTest {

    @Test
    public void testQueueConsumersShareLoad() throws Exception {
        String topic = newScalableTopic(4);
        String subscription = "shared-load-sub";

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        // Three consumers on the same subscription. Subscribe before producing so the
        // load distributes from the start.
        @Cleanup
        QueueConsumer<String> c1 = v5Client.newQueueConsumer(Schema.string())
                .topic(topic).subscriptionName(subscription)
                .subscribe();
        @Cleanup
        QueueConsumer<String> c2 = v5Client.newQueueConsumer(Schema.string())
                .topic(topic).subscriptionName(subscription)
                .subscribe();
        @Cleanup
        QueueConsumer<String> c3 = v5Client.newQueueConsumer(Schema.string())
                .topic(topic).subscriptionName(subscription)
                .subscribe();

        int n = 150;
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < n; i++) {
            String v = "v-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }

        // Drain in parallel: each consumer pulls until it sees no message for a short
        // window. The set of values seen across all three must equal `sent` exactly.
        Set<String> received = ConcurrentHashMap.newKeySet();
        Set<String> c1Got = ConcurrentHashMap.newKeySet();
        Set<String> c2Got = ConcurrentHashMap.newKeySet();
        Set<String> c3Got = ConcurrentHashMap.newKeySet();
        Thread t1 = drainTo(c1, received, c1Got);
        Thread t2 = drainTo(c2, received, c2Got);
        Thread t3 = drainTo(c3, received, c3Got);
        t1.join();
        t2.join();
        t3.join();

        assertEquals(received, sent, "every message must be delivered exactly once");

        // No duplicates: the three per-consumer sets must be pairwise disjoint.
        Set<String> overlap12 = new HashSet<>(c1Got);
        overlap12.retainAll(c2Got);
        assertTrue(overlap12.isEmpty(), "c1/c2 overlap: " + overlap12);
        Set<String> overlap13 = new HashSet<>(c1Got);
        overlap13.retainAll(c3Got);
        assertTrue(overlap13.isEmpty(), "c1/c3 overlap: " + overlap13);
        Set<String> overlap23 = new HashSet<>(c2Got);
        overlap23.retainAll(c3Got);
        assertTrue(overlap23.isEmpty(), "c2/c3 overlap: " + overlap23);

        // Load distribution: each consumer must have received at least one message —
        // otherwise the "shared" semantics are broken.
        assertTrue(!c1Got.isEmpty() && !c2Got.isEmpty() && !c3Got.isEmpty(),
                "each consumer must get at least one message"
                        + " (c1=" + c1Got.size() + " c2=" + c2Got.size() + " c3=" + c3Got.size() + ")");
    }

    private Thread drainTo(QueueConsumer<String> consumer, Set<String> all, Set<String> mine) {
        Thread t = new Thread(() -> {
            try {
                while (true) {
                    Message<String> msg = consumer.receive(Duration.ofMillis(500));
                    if (msg == null) {
                        return;
                    }
                    all.add(msg.value());
                    mine.add(msg.value());
                    consumer.acknowledge(msg.id());
                }
            } catch (Exception ignored) {
            }
        }, "queue-consumer-drainer");
        t.start();
        return t;
    }

    @Test
    public void testCheckpointConsumersEachSeeFullStream() throws Exception {
        String topic = newScalableTopic(2);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        int n = 50;
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < n; i++) {
            String v = "v-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }

        // CheckpointConsumer uses Readers: there's no broker-side subscription cursor,
        // so multiple consumers don't load-balance — each independently reads the full
        // stream from the requested start position.
        @Cleanup
        CheckpointConsumer<String> a = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(Checkpoint.earliest())
                .create();
        @Cleanup
        CheckpointConsumer<String> b = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(Checkpoint.earliest())
                .create();

        Set<String> aGot = new HashSet<>();
        for (int i = 0; i < n; i++) {
            Message<String> msg = a.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "consumer A missed message #" + i);
            aGot.add(msg.value());
        }
        Set<String> bGot = new HashSet<>();
        for (int i = 0; i < n; i++) {
            Message<String> msg = b.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "consumer B missed message #" + i);
            bGot.add(msg.value());
        }
        assertEquals(aGot, sent, "consumer A must see every produced message");
        assertEquals(bGot, sent, "consumer B must see every produced message");
    }

    @Test
    public void testStreamConsumersSplitSegmentsAcrossConsumers() throws Exception {
        String topic = newScalableTopic(4);
        String subscription = "stream-split-sub";

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        // Two stream consumers on the same subscription. The broker's
        // SubscriptionCoordinator rebalances on the second attach, splitting the four
        // segments across the two consumers — each ends up with a disjoint subset.
        @Cleanup
        StreamConsumer<String> a = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        @Cleanup
        StreamConsumer<String> b = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        int n = 100;
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < n; i++) {
            String v = "v-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }

        // Drain in parallel: each consumer pulls until idle.
        Set<String> received = ConcurrentHashMap.newKeySet();
        Set<String> aGot = ConcurrentHashMap.newKeySet();
        Set<String> bGot = ConcurrentHashMap.newKeySet();
        Thread ta = drainStreamTo(a, received, aGot);
        Thread tb = drainStreamTo(b, received, bGot);
        ta.join();
        tb.join();

        assertEquals(received, sent, "every message must be delivered exactly once across consumers");

        Set<String> overlap = new HashSet<>(aGot);
        overlap.retainAll(bGot);
        assertTrue(overlap.isEmpty(),
                "no message should be delivered to both stream consumers, overlap=" + overlap);

        assertTrue(!aGot.isEmpty() && !bGot.isEmpty(),
                "controller must split segments across both consumers"
                        + " (a=" + aGot.size() + " b=" + bGot.size() + ")");
    }

    private Thread drainStreamTo(StreamConsumer<String> consumer, Set<String> all, Set<String> mine) {
        Thread t = new Thread(() -> {
            try {
                MessageId last = null;
                while (true) {
                    Message<String> msg = consumer.receive(Duration.ofSeconds(1));
                    if (msg == null) {
                        if (last != null) {
                            consumer.acknowledgeCumulative(last);
                        }
                        return;
                    }
                    all.add(msg.value());
                    mine.add(msg.value());
                    last = msg.id();
                }
            } catch (Exception ignored) {
            }
        }, "stream-consumer-drainer");
        t.start();
        return t;
    }

    /**
     * A consumer that closes and re-subscribes with the same consumer name (within the
     * controller grace period, before its session is evicted) must keep its segment
     * assignment — the controller re-attaches the new subscribe to the existing
     * session without rebalancing.
     */
    @Test
    public void testStreamConsumerReconnectWithinGraceKeepsAssignment() throws Exception {
        String topic = newScalableTopic(4);
        String subscription = "stream-reconnect-sub";

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        StreamConsumer<String> alice = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .consumerName("alice")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        @Cleanup
        StreamConsumer<String> bob = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .consumerName("bob")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // First batch: drain across both, capture each consumer's per-key share.
        int firstN = 80;
        Set<String> firstSent = new HashSet<>();
        for (int i = 0; i < firstN; i++) {
            String v = "first-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            firstSent.add(v);
        }

        Set<String> aliceFirst = ConcurrentHashMap.newKeySet();
        Set<String> bobFirst = ConcurrentHashMap.newKeySet();
        Set<String> received1 = ConcurrentHashMap.newKeySet();
        Thread t1 = drainStreamTo(alice, received1, aliceFirst);
        Thread t2 = drainStreamTo(bob, received1, bobFirst);
        t1.join();
        t2.join();
        assertEquals(received1, firstSent, "first batch must be delivered exactly once");
        assertTrue(!aliceFirst.isEmpty() && !bobFirst.isEmpty(),
                "controller must split the first batch across alice + bob");

        // Close alice's consumer and re-subscribe under the same name. The controller
        // sees the existing session for "alice", attaches the new subscribe to it, and
        // pushes back the same assignment without touching bob.
        alice.close();
        @Cleanup
        StreamConsumer<String> aliceRejoined = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .consumerName("alice")
                .subscribe();

        // Second batch: pin the same key set so each generation routes to the same
        // segments — alice's per-key share must match across batches (modulo prefix).
        int secondN = 80;
        Set<String> secondSent = new HashSet<>();
        for (int i = 0; i < secondN; i++) {
            String v = "second-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            secondSent.add(v);
        }

        Set<String> aliceSecond = ConcurrentHashMap.newKeySet();
        Set<String> bobSecond = ConcurrentHashMap.newKeySet();
        Set<String> received2 = ConcurrentHashMap.newKeySet();
        Thread r1 = drainStreamTo(aliceRejoined, received2, aliceSecond);
        Thread r2 = drainStreamTo(bob, received2, bobSecond);
        r1.join();
        r2.join();
        assertEquals(received2, secondSent, "second batch must be delivered exactly once");

        Set<String> aliceFirstKeys = stripPrefix(aliceFirst, "first-");
        Set<String> aliceSecondKeys = stripPrefix(aliceSecond, "second-");
        assertEquals(aliceSecondKeys, aliceFirstKeys,
                "alice must keep her segments after re-subscribe within grace");
        Set<String> bobFirstKeys = stripPrefix(bobFirst, "first-");
        Set<String> bobSecondKeys = stripPrefix(bobSecond, "second-");
        assertEquals(bobSecondKeys, bobFirstKeys,
                "bob must keep his segments unchanged when alice re-subscribes");
    }

    /**
     * If a consumer disconnects and stays away past the controller grace period, its
     * session is evicted and segments rebalanced to remaining peers. We force a
     * disconnect by closing alice's dedicated client (consumer.close() alone doesn't
     * sever the underlying connection), then trust the rebalance to land while bob
     * drains the second batch.
     */
    @Test
    public void testStreamConsumerDisconnectPastGraceTriggersReassignment() throws Exception {
        String topic = newScalableTopic(4);
        String subscription = "stream-evict-sub";

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        // alice on her own client so we can sever the connection independently.
        PulsarClient aliceClient = newV5Client();
        StreamConsumer<String> alice = aliceClient.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .consumerName("alice")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        @Cleanup
        StreamConsumer<String> bob = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .consumerName("bob")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        int firstN = 60;
        for (int i = 0; i < firstN; i++) {
            producer.newMessage().key("k-" + i).value("first-" + i).send();
        }

        Set<String> received1 = ConcurrentHashMap.newKeySet();
        Set<String> aliceFirst = ConcurrentHashMap.newKeySet();
        Set<String> bobFirst = ConcurrentHashMap.newKeySet();
        Thread t1 = drainStreamTo(alice, received1, aliceFirst);
        Thread t2 = drainStreamTo(bob, received1, bobFirst);
        t1.join();
        t2.join();
        assertTrue(!aliceFirst.isEmpty() && !bobFirst.isEmpty(),
                "controller must split first batch across alice + bob");

        // Sever alice's connection. The broker arms the grace timer; once it fires the
        // rebalance pushes a new assignment to bob covering every active segment.
        aliceClient.close();

        // Produce immediately and drain bob until we've received all second-batch
        // messages — the rebalance will land mid-drain and bob picks up alice's
        // segments. Cap with a generous total deadline so a regression doesn't hang.
        int secondN = 60;
        Set<String> secondSent = new HashSet<>();
        for (int i = 0; i < secondN; i++) {
            String v = "second-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            secondSent.add(v);
        }

        Set<String> bobSecond = new HashSet<>();
        long deadline = System.currentTimeMillis() + 30_000L;
        while (bobSecond.size() < secondN && System.currentTimeMillis() < deadline) {
            Message<String> msg = bob.receive(Duration.ofSeconds(1));
            if (msg != null) {
                bobSecond.add(msg.value());
            }
        }
        assertEquals(bobSecond, secondSent,
                "after past-grace eviction bob must end up serving every segment");
    }

    private static Set<String> stripPrefix(Set<String> values, String prefix) {
        Set<String> out = new HashSet<>(values.size());
        for (String v : values) {
            if (v.startsWith(prefix)) {
                out.add(v.substring(prefix.length()));
            }
        }
        return out;
    }
}
