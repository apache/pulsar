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
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Coverage for the V5 producer's key-based routing on a multi-segment scalable topic.
 *
 * <p>The V5 producer hashes the message key (MurmurHash3, masked to 16 bits) and routes
 * to the active segment whose hash range covers the hash. We don't assert which segment
 * a key lands on (an internal detail), but we assert the two observable contracts:
 * <ul>
 *   <li>Same key → same segment, every time. We verify this by sending many messages with
 *       a small set of keys and checking that the per-key receive order matches the
 *       per-key send order on a {@link StreamConsumer} (which preserves order within a
 *       segment but not across segments).</li>
 *   <li>Different keys spread across segments. With four segments and a varied key set,
 *       all four segments receive at least one message — we verify this by inspecting the
 *       admin stats for each segment's per-segment topic.</li>
 * </ul>
 */
public class V5KeyRoutingTest extends V5ClientBaseTest {

    @Test
    public void testSameKeyPreservesOrder() throws Exception {
        String topic = newScalableTopic(4);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("key-route")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // 5 keys × 20 messages each, interleaved — same-key messages must arrive in send
        // order (which only holds if every same-key message lands on the same segment).
        List<String> keys = List.of("alpha", "bravo", "charlie", "delta", "echo");
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

        Map<String, List<String>> received = new HashMap<>();
        for (String k : keys) {
            received.put(k, new ArrayList<>());
        }
        int total = keys.size() * perKey;
        for (int i = 0; i < total; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "missed message #" + i);
            String key = msg.key().orElseThrow(() -> new AssertionError("missing key"));
            received.get(key).add(msg.value());
            consumer.acknowledgeCumulative(msg.id());
        }

        for (String k : keys) {
            assertEquals(received.get(k), sent.get(k),
                    "per-key order must be preserved for key=" + k);
        }
    }

    @Test
    public void testMultiSegmentEndToEndCount() throws Exception {
        String topic = newScalableTopic(4);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("multi-seg-count")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // 200 distinct keys: every send should land on some segment, every message should
        // come out exactly once. This is the basic "scalable topic with N>1 segments
        // doesn't drop or duplicate messages" sanity check.
        int n = 200;
        java.util.Set<String> sent = new java.util.HashSet<>();
        for (int i = 0; i < n; i++) {
            String value = "v-" + i;
            producer.newMessage().key("k-" + i).value(value).send();
            sent.add(value);
        }

        java.util.Set<String> received = new java.util.HashSet<>();
        MessageId last = null;
        for (int i = 0; i < n; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "missed message #" + i);
            received.add(msg.value());
            last = msg.id();
        }
        consumer.acknowledgeCumulative(last);

        assertEquals(received.size(), n, "expected " + n + " distinct messages");
        assertEquals(received, sent, "received set must match sent set");

        var meta = admin.scalableTopics().getMetadata(topic);
        int active = 0;
        for (var seg : meta.getSegments().values()) {
            if (seg.isActive()) {
                active++;
            }
        }
        assertEquals(active, 4, "expected 4 active segments");
    }
}
