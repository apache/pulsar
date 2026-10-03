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
package org.apache.pulsar.client.impl.v5;

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.StreamConsumer;
import org.apache.pulsar.client.api.v5.V5ClientBaseTest;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.policies.data.ScalableTopicMetadata;
import org.testng.annotations.Test;

/**
 * Per-key order of async sends that are in flight while the topic layout changes.
 *
 * <p>When a split or merge seals a segment, the sends still pending on it fail with
 * {@code TopicTerminated} / {@code AlreadyClosed} and are re-sent to the successor segments.
 * Meanwhile the producer keeps routing new sends to those successors. Unless the re-sends go
 * first, a newer message for a key is stored before an older one, and every consumer sees that key
 * out of order. These tests keep an async producer busy across the layout change and then check the
 * order the messages of each key were stored in: segment by segment along the key's lineage in the
 * segment DAG, and in each segment in the order it was read.
 */
public class V5ProducerLayoutChangeOrderingTest extends V5ClientBaseTest {

    private static final int KEYS = 200;
    private static final int MESSAGES_BEFORE_CHANGE = 2_000;
    private static final int MESSAGES_AFTER_CHANGE = 2_000;
    /** Sends per burst; a burst is followed by a millisecond pause, a pace the broker keeps up with. */
    private static final int BURST = 20;

    @Test
    public void testAsyncSendsKeepPerKeyOrderAcrossSplit() throws Exception {
        String topic = newScalableTopic(1);
        produceAndVerifyPerKeyOrder(topic, () -> {
            List<Long> active = activeSegmentIds(topic);
            admin.scalableTopics().splitSegment(topic, active.get(0));
        });
    }

    @Test
    public void testAsyncSendsKeepPerKeyOrderAcrossMerge() throws Exception {
        String topic = newScalableTopic(2);
        produceAndVerifyPerKeyOrder(topic, () -> {
            List<Long> active = activeSegmentIds(topic);
            admin.scalableTopics().mergeSegments(topic, active.get(0), active.get(1));
        });
    }

    @Test
    public void testAsyncSendsKeepPerKeyOrderAcrossConsecutiveSplits() throws Exception {
        String topic = newScalableTopic(1);
        produceAndVerifyPerKeyOrder(topic, () -> {
            admin.scalableTopics().splitSegment(topic, activeSegmentIds(topic).get(0));
            // Split a child right away, while the sends re-sent after the first split may still be
            // in flight to it.
            admin.scalableTopics().splitSegment(topic, activeSegmentIds(topic).get(0));
        });
    }

    private interface LayoutChange {
        void run() throws Exception;
    }

    /**
     * Sends {@code key -> 1, 2, 3, ...} for {@link #KEYS} keys with async sends, runs the layout
     * change while the sends keep going, and checks that every key was stored as exactly
     * {@code 1..n}.
     */
    private void produceAndVerifyPerKeyOrder(String topic, LayoutChange change) throws Exception {
        // Subscribe first, so that every message is retained for the subscription.
        @Cleanup
        StreamConsumer<Long> consumer = v5Client.newStreamConsumer(Schema.int64())
                .topic(topic)
                .subscriptionName("ordering-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        @Cleanup
        Producer<Long> producer = v5Client.newProducer(Schema.int64())
                .topic(topic)
                .create();

        long[] nextSequence = new long[KEYS];
        List<CompletableFuture<MessageId>> sends = new ArrayList<>();
        int sent = 0;
        for (; sent < MESSAGES_BEFORE_CHANGE; sent++) {
            sends.add(sendNext(producer, nextSequence, sent));
        }

        // Change the layout while sends keep flowing, so that some are in flight to the sealed
        // segment and the rest are routed to its successors once the new layout arrives.
        CompletableFuture<Void> layoutChange = CompletableFuture.runAsync(() -> {
            try {
                change.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        while (!layoutChange.isDone()) {
            sends.add(sendNext(producer, nextSequence, sent++));
            if (sent % BURST == 0) {
                Thread.sleep(1);
            }
        }
        layoutChange.get(30, TimeUnit.SECONDS);
        for (int i = 0; i < MESSAGES_AFTER_CHANGE; i++) {
            sends.add(sendNext(producer, nextSequence, sent++));
            if (sent % BURST == 0) {
                Thread.sleep(1);
            }
        }
        CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).get(60, TimeUnit.SECONDS);

        // Read until the topic is drained, so that a duplicate shows up as such instead of hiding the
        // last message of another key. Per key, keep the sequence numbers of each segment in the
        // order they were read, which is the order that segment stores them in.
        Map<String, Map<Long, List<Long>>> stored = new HashMap<>();
        int count = 0;
        Message<Long> msg;
        while ((msg = consumer.receive(Duration.ofSeconds(count < sent ? 10 : 2))) != null) {
            long segmentId = ((MessageIdV5) msg.id()).segmentId();
            stored.computeIfAbsent(msg.key().orElseThrow(), __ -> new HashMap<>())
                    .computeIfAbsent(segmentId, __ -> new ArrayList<>())
                    .add(msg.value());
            consumer.acknowledgeCumulative(msg.id());
            count++;
        }
        assertThat(count).as("messages received").isEqualTo(sent);

        // A key's segments are a path in the DAG, each created after the one it came from.
        Map<Long, ScalableTopicMetadata.SegmentInfo> segments =
                admin.scalableTopics().getMetadata(topic).getSegments();
        for (int k = 0; k < KEYS; k++) {
            Map<Long, List<Long>> bySegment = stored.get(key(k));
            assertThat(bySegment).as("messages of %s", key(k)).isNotNull();
            List<Long> inStoredOrder = new ArrayList<>();
            Map<Long, List<Long>> lineage = new LinkedHashMap<>();
            bySegment.keySet().stream()
                    .sorted(Comparator.comparingLong((Long id) -> segments.get(id).getCreatedAtEpoch())
                            .thenComparingLong(id -> id))
                    .forEach(id -> {
                        inStoredOrder.addAll(bySegment.get(id));
                        lineage.put(id, bySegment.get(id));
                    });
            List<Long> expected = LongStream.rangeClosed(1, nextSequence[k]).boxed().toList();
            assertThat(inStoredOrder)
                    .as("stored order of %s across the layout change, by segment: %s", key(k), lineage)
                    .containsExactlyElementsOf(expected);
        }
    }

    private static CompletableFuture<MessageId> sendNext(Producer<Long> producer, long[] nextSequence,
                                                         int index) {
        int k = index % KEYS;
        return producer.async().newMessage().key(key(k)).value(++nextSequence[k]).send();
    }

    private static String key(int k) {
        return "key-" + k;
    }

    private List<Long> activeSegmentIds(String topic) throws Exception {
        return admin.scalableTopics().getMetadata(topic).getSegments().values().stream()
                .filter(ScalableTopicMetadata.SegmentInfo::isActive)
                .map(ScalableTopicMetadata.SegmentInfo::getSegmentId)
                .sorted()
                .toList();
    }
}
