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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Per-key ordering of a {@link StreamConsumer} across a lineage of splits: a key's messages on a
 * segment must all be delivered before its messages on the segment's descendants, however many
 * generations apart.
 */
public class V5StreamConsumerOrderingTest extends V5ClientBaseTest {

    private static final int KEYS = 16;

    /**
     * A subscription created after two splits replays both sealed generations: it must not read
     * the second one while the first still has backlog, since it holds newer messages of the same
     * keys.
     */
    @Test
    public void testPerKeyOrderAcrossTwoSplits() throws Exception {
        String topic = newTopic();
        @Cleanup
        Producer<String> producer = newProducer(topic);

        Map<String, List<String>> sent = new HashMap<>();
        long root = activeSegmentIds(topic).get(0);
        publish(producer, "gen0", 50, sent);
        split(topic, root, 2);
        publish(producer, "gen1", 50, sent);
        split(topic, activeSegmentIds(topic).get(0), 3);
        publish(producer, "gen2", 50, sent);

        @Cleanup
        StreamConsumer<String> consumer = newConsumer(topic);
        // Nothing is received yet, so the root keeps its backlog.
        assertNotReadWhileRootHasBacklog(topic, root);
        assertPerKeyOrder(receive(consumer, total(sent)), sent);
    }

    /**
     * A segment split before anything was published to it has no backlog, so it is drained right
     * away while its parent can still have backlog: its children must wait for the parent too.
     */
    @Test
    public void testPerKeyOrderAcrossAnEmptySegment() throws Exception {
        String topic = newTopic();
        @Cleanup
        Producer<String> producer = newProducer(topic);
        @Cleanup
        StreamConsumer<String> consumer = newConsumer(topic);

        Map<String, List<String>> sent = new HashMap<>();
        long root = activeSegmentIds(topic).get(0);
        publish(producer, "gen0", 50, sent);
        split(topic, root, 2);
        split(topic, activeSegmentIds(topic).get(0), 3);
        publish(producer, "gen2", 50, sent);

        assertNotReadWhileRootHasBacklog(topic, root);
        assertPerKeyOrder(receive(consumer, total(sent)), sent);
    }

    // --- Helpers ---

    private String newTopic() throws Exception {
        String topic = newScalableTopic(1);
        // Only the admin splits below may change the layout.
        admin.scalableTopics().setAutoScalePolicy(topic,
                AutoScalePolicyOverride.builder().enabled(false).build());
        return topic;
    }

    private Producer<String> newProducer(String topic) throws Exception {
        return v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
    }

    private StreamConsumer<String> newConsumer(String topic) throws Exception {
        return v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
    }

    /** Publish {@code perKey} rounds of one message per key, keys interleaved. */
    private static void publish(Producer<String> producer, String prefix, int perKey,
                                Map<String, List<String>> sent) {
        List<CompletableFuture<MessageId>> sends = new ArrayList<>();
        for (int i = 0; i < perKey; i++) {
            for (int k = 0; k < KEYS; k++) {
                String key = "key-" + k;
                String value = key + "-" + prefix + "-" + i;
                sends.add(producer.async().newMessage().key(key).value(value).send());
                sent.computeIfAbsent(key, __ -> new ArrayList<>()).add(value);
            }
        }
        CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).join();
    }

    private static Map<String, List<String>> receive(StreamConsumer<String> consumer, int count)
            throws Exception {
        Map<String, List<String>> received = new HashMap<>();
        for (int i = 0; i < count; i++) {
            // A segment is read only once the controller's drain poll, which backs off, sees its
            // ancestors drained.
            Message<String> msg = consumer.receive(Duration.ofSeconds(30));
            assertNotNull(msg, "missed message #" + i + " of " + count);
            received.computeIfAbsent(msg.key().orElseThrow(), __ -> new ArrayList<>()).add(msg.value());
            // A sealed segment is drained once the subscription has no backlog on it.
            consumer.acknowledgeCumulative(msg.id());
        }
        return received;
    }

    private static int total(Map<String, List<String>> messages) {
        return messages.values().stream().mapToInt(List::size).sum();
    }

    private static void assertPerKeyOrder(Map<String, List<String>> received, Map<String, List<String>> sent) {
        for (var entry : sent.entrySet()) {
            assertEquals(received.getOrDefault(entry.getKey(), List.of()), entry.getValue(),
                    "per-key order for " + entry.getKey());
        }
    }

    /** For a while, no consumer reads any descendant of the root, which still has backlog. */
    private void assertNotReadWhileRootHasBacklog(String topic, long root) throws Exception {
        Set<Long> descendants = new HashSet<>(admin.scalableTopics().getMetadata(topic).getSegments().keySet());
        descendants.remove(root);
        // Long enough for the controller's first drain polls.
        Awaitility.await().during(Duration.ofSeconds(5)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            for (var subscription : admin.scalableTopics().getStats(topic).getSubscriptions().values()) {
                for (long segmentId : descendants) {
                    var onSegment = subscription.getSegments().get(segmentId);
                    assertTrue(onSegment == null || onSegment.getConsumerCount() == 0,
                            "segment " + segmentId + " is read while the root still has backlog");
                }
            }
        });
    }

    private void split(String topic, long segmentId, int expectedActive) throws Exception {
        admin.scalableTopics().splitSegment(topic, segmentId);
        Awaitility.await().untilAsserted(() -> assertEquals(activeSegmentIds(topic).size(), expectedActive));
    }

    private List<Long> activeSegmentIds(String topic) throws Exception {
        List<Long> ids = new ArrayList<>();
        for (var seg : admin.scalableTopics().getMetadata(topic).getSegments().values()) {
            if (seg.isActive()) {
                ids.add(seg.getSegmentId());
            }
        }
        return ids;
    }
}
