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
}
