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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;
import org.testng.annotations.Test;

/**
 * PIP-486 end-to-end: stream consumers on an entry-bucketed segment.
 *
 * <p>A one-segment scalable topic with the default entry-bucket budget (4) gives that segment
 * {@code N = 4} entry-buckets, so the producer batches per-bucket and stamps each entry's
 * {@code entry_hash} range. A lone stream consumer owns the whole segment and subscribes
 * {@code Exclusive} (single-active dispatch); a second consumer makes the controller fan the segment
 * out — each owner takes half the buckets and subscribes {@code Key_Shared} STICKY with those ranges,
 * and the broker dispatches each whole entry by its stamped range to the bucket's owner.
 */
public class V5EntryBucketDispatchTest extends V5ClientBaseTest {

    @Test
    public void testBucketedSegmentPreservesPerKeyOrderAndDeliversAll() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("bucket-dispatch")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // 8 keys × 25 messages, interleaved. With per-bucket batching on, same-key messages must
        // still arrive in send order (only holds if every entry routes to the one consumer that owns
        // the key's bucket), and every message must be delivered exactly once.
        List<String> keys = List.of("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel");
        int perKey = 25;
        Map<String, List<String>> sent = new HashMap<>();
        for (String k : keys) {
            sent.put(k, new ArrayList<>());
        }
        for (int i = 0; i < perKey; i++) {
            for (String k : keys) {
                String value = k + "-" + i;
                producer.newMessage().key(k).value(value).send();
                sent.get(k).add(value);
            }
        }

        Map<String, List<String>> received = new HashMap<>();
        for (String k : keys) {
            received.put(k, new ArrayList<>());
        }
        int total = keys.size() * perKey;
        MessageId last = null;
        for (int i = 0; i < total; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "missed message #" + i);
            String key = msg.key().orElseThrow(() -> new AssertionError("missing key"));
            received.get(key).add(msg.value());
            last = msg.id();
        }
        consumer.acknowledgeCumulative(last);

        for (String k : keys) {
            assertEquals(received.get(k), sent.get(k), "per-key order must be preserved for key=" + k);
        }
    }

    @Test
    public void testTwoConsumersShareBucketedSegmentByEntryBucket() throws Exception {
        String topic = newScalableTopic(1);
        // Pin the layout: with more consumers than segments, PIP-483 would otherwise split the
        // segment ("segments first"). Disabling auto split/merge forces the controller to serve the
        // second consumer by fanning the segment out by entry-bucket — the path under test.
        admin.scalableTopics().setAutoScalePolicy(topic,
                AutoScalePolicyOverride.builder().enabled(false).build());
        String subscription = "bucket-share";

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        // Two stream consumers on the one-segment (N=4) topic: the controller fans the segment out,
        // giving each consumer two of the four buckets (Key_Shared STICKY under the hood).
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

        // 16 keys × 20 messages, interleaved — enough keys to populate all four buckets with
        // overwhelming probability, so both owners receive traffic.
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            keys.add("key-" + i);
        }
        int perKey = 20;
        Map<String, List<String>> sent = new HashMap<>();
        for (String k : keys) {
            sent.put(k, new ArrayList<>());
        }
        for (int i = 0; i < perKey; i++) {
            for (String k : keys) {
                String value = k + "-" + i;
                producer.newMessage().key(k).value(value).send();
                sent.get(k).add(value);
            }
        }

        Map<String, List<String>> aGot = new ConcurrentHashMap<>();
        Map<String, List<String>> bGot = new ConcurrentHashMap<>();
        Thread ta = drainOrdered(a, aGot);
        Thread tb = drainOrdered(b, bGot);
        ta.join();
        tb.join();

        assertFalse(aGot.isEmpty(), "consumer A received nothing — the segment did not fan out");
        assertFalse(bGot.isEmpty(), "consumer B received nothing — the segment did not fan out");

        // Whole-entry bucket dispatch: every key lands wholly on exactly one consumer, in send order.
        for (String k : keys) {
            List<String> fromA = aGot.get(k);
            List<String> fromB = bGot.get(k);
            assertTrue(fromA == null || fromB == null, "key " + k + " was split across both consumers");
            List<String> got = fromA != null ? fromA : fromB;
            assertEquals(got, sent.get(k), "per-key order/content for key=" + k);
        }

        // The drainers acked everything they received — for a bucket-shared segment that goes through
        // the individual-ack translation (Key_Shared forbids cumulative acks). A fresh consumer on the
        // same subscription must therefore find nothing to redeliver.
        a.close();
        b.close();
        @Cleanup
        StreamConsumer<String> c = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        assertNull(c.receive(Duration.ofSeconds(3)), "acked messages were redelivered");
    }

    @Test
    public void testConsumerJoiningMidTrafficPreservesPerKeyOrder() throws Exception {
        String topic = newScalableTopic(1);
        // Pin the layout so the second consumer is served by entry-bucket fan-out (see above).
        admin.scalableTopics().setAutoScalePolicy(topic,
                AutoScalePolicyOverride.builder().enabled(false).build());
        String subscription = "bucket-handoff";

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        StreamConsumer<String> a = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // Continuous keyed traffic; consumer B joins mid-stream, forcing a live two-phase handoff:
        // A pauses, drains what it already handed to the application, shrinks from the whole
        // segment to half the buckets and confirms; only then is B granted the other half.
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            keys.add("key-" + i);
        }
        int perKey = 40;
        Map<String, List<String>> sent = new HashMap<>();
        for (String k : keys) {
            sent.put(k, new ArrayList<>());
        }

        Map<String, List<String>> aGot = new ConcurrentHashMap<>();
        Map<String, List<String>> bGot = new ConcurrentHashMap<>();
        Thread ta = drainOrdered(a, aGot);

        StreamConsumer<String> b = null;
        try {
            for (int i = 0; i < perKey; i++) {
                if (i == perKey / 3) {
                    // Mid-traffic join: triggers the handoff while messages flow.
                    b = v5Client.newStreamConsumer(Schema.string())
                            .topic(topic)
                            .subscriptionName(subscription)
                            .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                            .subscribe();
                }
                for (String k : keys) {
                    String value = k + "-" + i;
                    producer.newMessage().key(k).value(value).send();
                    sent.get(k).add(value);
                }
            }

            Thread tb = drainOrdered(b, bGot);
            ta.join();
            tb.join();

            assertFalse(aGot.isEmpty(), "consumer A received nothing");
            assertFalse(bGot.isEmpty(), "consumer B received nothing — the handoff did not happen");

            // The handoff contract: for every key, what A processed followed by what B processed
            // is exactly the sent sequence — no loss, no duplicates (a clean drain leaves nothing
            // to redeliver), and never a message on B before A finished everything older.
            for (String k : keys) {
                List<String> combined = new ArrayList<>(aGot.getOrDefault(k, List.of()));
                combined.addAll(bGot.getOrDefault(k, List.of()));
                assertEquals(combined, sent.get(k), "per-key order across the handoff for key=" + k);
            }
        } finally {
            if (b != null) {
                b.close();
            }
        }
    }

    @Test
    public void testSegmentReleaseWithPendingAcksHandsOffCleanly() throws Exception {
        // The release (drop) path: a consumer loses a whole segment in a rebalance while the
        // application still holds delivered-but-unacked messages. The release must wait for those
        // acks — and the acks, arriving while the segment is already vacated from the consumer's
        // active set, must still reach the draining consumer, or the release never completes and the
        // new owner never gets the segment.
        String topic = newScalableTopic(2);
        admin.scalableTopics().setAutoScalePolicy(topic,
                AutoScalePolicyOverride.builder().enabled(false).build());
        String subscription = "segment-release";

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        StreamConsumer<String> a = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // Phase 1: A owns both segments; deliver keyed traffic and hold every ack.
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            keys.add("key-" + i);
        }
        int perKey = 10;
        Map<String, List<String>> sent = new HashMap<>();
        for (String k : keys) {
            sent.put(k, new ArrayList<>());
        }
        for (int i = 0; i < perKey; i++) {
            for (String k : keys) {
                String value = k + "-" + i;
                producer.newMessage().key(k).value(value).send();
                sent.get(k).add(value);
            }
        }
        Map<String, List<String>> aGot = new ConcurrentHashMap<>();
        Message<String> last = null;
        for (int i = 0; i < keys.size() * perKey; i++) {
            Message<String> msg = a.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "missed message #" + i + " in phase 1");
            String key = msg.key().orElseThrow(() -> new AssertionError("missing key"));
            aGot.computeIfAbsent(key, __ -> new ArrayList<>()).add(msg.value());
            last = msg;
        }

        // B joins: the controller rebalances to one segment each, so A must release one whole
        // segment — but it cannot until the application acks what it already received. Subscribe
        // asynchronously: B's attach only completes once A releases, which needs the ack below.
        CompletableFuture<StreamConsumer<String>> bFuture = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribeAsync();
        // Let the assignment update land and the release drain start waiting, so the ack below
        // genuinely arrives mid-drain.
        Thread.sleep(1500);
        a.acknowledgeCumulative(last.id());
        // Bounded: if the release/flip wedges, fail here instead of hanging the suite.
        @Cleanup
        StreamConsumer<String> b = bFuture.get(30, TimeUnit.SECONDS);

        // Phase 2: more traffic; the moved segment's share must now flow to B.
        for (int i = perKey; i < perKey * 2; i++) {
            for (String k : keys) {
                String value = k + "-" + i;
                producer.newMessage().key(k).value(value).send();
                sent.get(k).add(value);
            }
        }
        Map<String, List<String>> bGot = new ConcurrentHashMap<>();
        Thread ta = drainOrdered(a, aGot, Duration.ofSeconds(5));
        Thread tb = drainOrdered(b, bGot, Duration.ofSeconds(5));
        ta.join();
        tb.join();

        assertFalse(bGot.isEmpty(), "consumer B received nothing — the segment was never released");
        // Phase 1 was fully acked before the release completed, so nothing is redelivered: for every
        // key, A's deliveries followed by B's are exactly the sent sequence.
        for (String k : keys) {
            List<String> combined = new ArrayList<>(aGot.getOrDefault(k, List.of()));
            combined.addAll(bGot.getOrDefault(k, List.of()));
            assertEquals(combined, sent.get(k), "per-key order across the release for key=" + k);
        }
    }

    @Test
    public void testConsumerJoiningWithSlowAcksPreservesPerKeyOrder() throws Exception {
        // The mode-flip path with a non-empty drain window: when B joins, A flips the segment from
        // Exclusive to Key_Shared. Acks issued while the flip's drain is waiting must reach the old
        // consumer being drained — routed anywhere else they are lost (a cumulative ack is invalid on
        // the new Key_Shared consumer), the broker redelivers, and keys see duplicates.
        String topic = newScalableTopic(1);
        admin.scalableTopics().setAutoScalePolicy(topic,
                AutoScalePolicyOverride.builder().enabled(false).build());
        String subscription = "slow-ack-join";

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        StreamConsumer<String> a = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // Phase 1: deliver to A and hold every ack.
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            keys.add("key-" + i);
        }
        int perKey = 5;
        Map<String, List<String>> sent = new HashMap<>();
        for (String k : keys) {
            sent.put(k, new ArrayList<>());
        }
        for (int i = 0; i < perKey; i++) {
            for (String k : keys) {
                String value = k + "-" + i;
                producer.newMessage().key(k).value(value).send();
                sent.get(k).add(value);
            }
        }
        Map<String, List<String>> aGot = new ConcurrentHashMap<>();
        Message<String> last = null;
        for (int i = 0; i < keys.size() * perKey; i++) {
            Message<String> msg = a.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "missed message #" + i + " in phase 1");
            String key = msg.key().orElseThrow(() -> new AssertionError("missing key"));
            aGot.computeIfAbsent(key, __ -> new ArrayList<>()).add(msg.value());
            last = msg;
        }

        // B joins → A must flip the segment to Key_Shared, which drains first. Subscribe
        // asynchronously (B's attach completes only after A's flip) and ack mid-drain.
        CompletableFuture<StreamConsumer<String>> bFuture = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribeAsync();
        Thread.sleep(1500);
        a.acknowledgeCumulative(last.id());
        // Bounded: if the release/flip wedges, fail here instead of hanging the suite.
        @Cleanup
        StreamConsumer<String> b = bFuture.get(30, TimeUnit.SECONDS);

        // Phase 2: post-flip traffic, spread across both owners by bucket.
        for (int i = perKey; i < perKey * 2; i++) {
            for (String k : keys) {
                String value = k + "-" + i;
                producer.newMessage().key(k).value(value).send();
                sent.get(k).add(value);
            }
        }
        Map<String, List<String>> bGot = new ConcurrentHashMap<>();
        Thread ta = drainOrdered(a, aGot, Duration.ofSeconds(5));
        Thread tb = drainOrdered(b, bGot, Duration.ofSeconds(5));
        ta.join();
        tb.join();

        assertFalse(bGot.isEmpty(), "consumer B received nothing — the handoff did not happen");
        // The mid-drain ack reached the draining consumer, so phase 1 is acked on the broker and
        // never redelivered: A's deliveries followed by B's are exactly the sent sequence — no
        // duplicates, no loss, per key.
        for (String k : keys) {
            List<String> combined = new ArrayList<>(aGot.getOrDefault(k, List.of()));
            combined.addAll(bGot.getOrDefault(k, List.of()));
            assertEquals(combined, sent.get(k), "per-key order across the slow-ack flip for key=" + k);
        }
    }

    /**
     * Drains until idle, recording values per key in arrival order. Acks every message as it is
     * processed — like an order-sensitive application would — so a release drain during a handoff
     * can complete promptly.
     */
    private Thread drainOrdered(StreamConsumer<String> consumer, Map<String, List<String>> into) {
        return drainOrdered(consumer, into, Duration.ofSeconds(2));
    }

    /** Variant with a configurable idle timeout, for tests whose handoff takes a few seconds. */
    private Thread drainOrdered(StreamConsumer<String> consumer, Map<String, List<String>> into,
                                Duration idleTimeout) {
        Thread t = new Thread(() -> {
            try {
                while (true) {
                    Message<String> msg = consumer.receive(idleTimeout);
                    if (msg == null) {
                        return;
                    }
                    String key = msg.key().orElseThrow(() -> new AssertionError("missing key"));
                    into.computeIfAbsent(key, __ -> new ArrayList<>()).add(msg.value());
                    consumer.acknowledgeCumulative(msg.id());
                }
            } catch (Exception ignored) {
            }
        }, "bucket-share-drainer");
        t.start();
        return t;
    }
}
