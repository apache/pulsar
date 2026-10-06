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
import static org.testng.Assert.assertNull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Per-key ordering of an ungrouped {@link CheckpointConsumer} across segment splits and merges: a
 * key's messages on a sealed parent segment must all be delivered before its messages on the
 * parent's children.
 */
public class V5CheckpointConsumerOrderingTest extends V5ClientBaseTest {

    private static final int KEYS = 16;

    @Test
    public void testPerKeyOrderAcrossSplitWhileReading() throws Exception {
        String topic = newTopic(1);
        @Cleanup
        Producer<String> producer = newProducer(topic);
        @Cleanup
        CheckpointConsumer<String> consumer = newConsumer(topic, Checkpoint.earliest());

        Map<String, List<String>> sent = new HashMap<>();
        // The consumer is behind when the split happens: the pre-split backlog is larger than what
        // it prefetches, so part of it is still unread on the parent segment.
        publish(producer, "pre", 250, sent);
        split(topic, activeSegmentIds(topic).get(0), 2);
        publish(producer, "post", 50, sent);

        assertPerKeyOrder(receive(consumer, total(sent)), sent);
    }

    @Test
    public void testPerKeyOrderAcrossMergeWhileReading() throws Exception {
        String topic = newTopic(2);
        @Cleanup
        Producer<String> producer = newProducer(topic);
        @Cleanup
        CheckpointConsumer<String> consumer = newConsumer(topic, Checkpoint.earliest());

        Map<String, List<String>> sent = new HashMap<>();
        publish(producer, "pre", 250, sent);
        List<Long> parents = activeSegmentIds(topic);
        admin.scalableTopics().mergeSegments(topic, parents.get(0), parents.get(1));
        Awaitility.await().untilAsserted(() -> assertEquals(activeSegmentIds(topic).size(), 1));
        publish(producer, "post", 50, sent);

        assertPerKeyOrder(receive(consumer, total(sent)), sent);
    }

    @Test
    public void testSplitWhileCaughtUpDeliversChildren() throws Exception {
        String topic = newTopic(1);
        @Cleanup
        Producer<String> producer = newProducer(topic);
        @Cleanup
        CheckpointConsumer<String> consumer = newConsumer(topic, Checkpoint.earliest());

        Map<String, List<String>> sent = new HashMap<>();
        publish(producer, "pre", 5, sent);
        Map<String, List<String>> received = receive(consumer, total(sent));
        // The consumer has read the parent to its end and waits for more when the split seals it.
        split(topic, activeSegmentIds(topic).get(0), 2);
        Map<String, List<String>> postSplit = new HashMap<>();
        publish(producer, "post", 5, postSplit);
        receive(consumer, total(postSplit)).forEach((key, values) -> received.get(key).addAll(values));
        postSplit.forEach((key, values) -> sent.get(key).addAll(values));

        assertPerKeyOrder(received, sent);
    }

    @Test
    public void testPerKeyOrderWhenStartingAfterSplits() throws Exception {
        String topic = newTopic(1);
        @Cleanup
        Producer<String> producer = newProducer(topic);

        // Two generations of sealed segments in front of the active ones.
        Map<String, List<String>> sent = new HashMap<>();
        publish(producer, "gen0", 50, sent);
        split(topic, activeSegmentIds(topic).get(0), 2);
        publish(producer, "gen1", 50, sent);
        split(topic, activeSegmentIds(topic).get(0), 3);
        publish(producer, "gen2", 50, sent);

        @Cleanup
        CheckpointConsumer<String> consumer = newConsumer(topic, Checkpoint.earliest());
        assertPerKeyOrder(receive(consumer, total(sent)), sent);
    }

    @Test
    public void testPerKeyOrderResumingCheckpointAcrossSplit() throws Exception {
        String topic = newTopic(1);
        @Cleanup
        Producer<String> producer = newProducer(topic);

        Map<String, List<String>> sent = new HashMap<>();
        publish(producer, "pre", 100, sent);
        CheckpointConsumer<String> first = newConsumer(topic, Checkpoint.earliest());
        // Take the checkpoint partway through the parent, then close.
        Map<String, List<String>> consumed = receive(first, total(sent) / 2);
        Checkpoint checkpoint = first.checkpoint();
        first.close();

        split(topic, activeSegmentIds(topic).get(0), 2);
        publish(producer, "post", 50, sent);

        @Cleanup
        CheckpointConsumer<String> resumed = newConsumer(topic, checkpoint);
        Map<String, List<String>> expected = new HashMap<>();
        sent.forEach((key, values) -> {
            List<String> remaining = new ArrayList<>(values);
            remaining.removeAll(consumed.getOrDefault(key, List.of()));
            expected.put(key, remaining);
        });
        assertPerKeyOrder(receive(resumed, total(expected)), expected);
        assertNull(resumed.receive(Duration.ofMillis(500)), "nothing past the produced messages");
    }

    // --- Helpers ---

    private String newTopic(int segments) throws Exception {
        String topic = newScalableTopic(segments);
        // Only the admin splits and merges below may change the layout.
        admin.scalableTopics().setAutoScalePolicy(topic,
                AutoScalePolicyOverride.builder().enabled(false).build());
        return topic;
    }

    private Producer<String> newProducer(String topic) throws Exception {
        return v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
    }

    private CheckpointConsumer<String> newConsumer(String topic, Checkpoint start) throws Exception {
        return v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(start)
                .create();
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

    private static Map<String, List<String>> receive(CheckpointConsumer<String> consumer, int count)
            throws Exception {
        Map<String, List<String>> received = new HashMap<>();
        for (int i = 0; i < count; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(10));
            assertNotNull(msg, "missed message #" + i + " of " + count);
            received.computeIfAbsent(msg.key().orElseThrow(), __ -> new ArrayList<>()).add(msg.value());
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
