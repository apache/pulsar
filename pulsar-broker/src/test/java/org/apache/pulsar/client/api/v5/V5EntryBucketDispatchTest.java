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
import java.util.concurrent.ConcurrentHashMap;
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

    /** Drains until idle, recording values per key in arrival order, then acks cumulatively. */
    private Thread drainOrdered(StreamConsumer<String> consumer, Map<String, List<String>> into) {
        Thread t = new Thread(() -> {
            try {
                MessageId last = null;
                while (true) {
                    Message<String> msg = consumer.receive(Duration.ofSeconds(2));
                    if (msg == null) {
                        if (last != null) {
                            consumer.acknowledgeCumulative(last);
                        }
                        return;
                    }
                    String key = msg.key().orElseThrow(() -> new AssertionError("missing key"));
                    into.computeIfAbsent(key, __ -> new ArrayList<>()).add(msg.value());
                    last = msg.id();
                }
            } catch (Exception ignored) {
            }
        }, "bucket-share-drainer");
        t.start();
        return t;
    }
}
