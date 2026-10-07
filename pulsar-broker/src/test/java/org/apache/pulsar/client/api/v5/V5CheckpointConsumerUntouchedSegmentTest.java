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

import static org.assertj.core.api.Assertions.assertThat;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * A {@link Checkpoint} must hold a position for every segment the {@link CheckpointConsumer} reads, including
 * the segments it hasn't received a message from. A restore reads a segment that is missing from the
 * checkpoint from the earliest position, so it would replay history the consumer had skipped or already
 * processed.
 */
public class V5CheckpointConsumerUntouchedSegmentTest extends V5ClientBaseTest {

    @Test
    public void testLatestStartKeepsUntouchedSegment() throws Exception {
        checkLatestStartKeepsUntouchedSegment(null);
    }

    @Test
    public void testGroupedLatestStartKeepsUntouchedSegment() throws Exception {
        checkLatestStartKeepsUntouchedSegment("checkpoint-group");
    }

    private void checkLatestStartKeepsUntouchedSegment(String group) throws Exception {
        String topic = newTopic(2);
        @Cleanup
        Producer<String> producer = newProducer(topic);
        // History on both segments, from before the consumer starts.
        publish(producer, "history", 40);

        CheckpointConsumer<String> first = newConsumer(topic, Checkpoint.latest(), group);
        // A single message lands on one segment: the consumer receives nothing from the other one.
        publish(producer, "new", 1);
        assertThat(first.receive(Duration.ofSeconds(5))).extracting(Message::value).isEqualTo("new-0");
        Checkpoint checkpoint = first.checkpoint();
        first.close();

        // Another group, so the restored consumer is assigned every segment right away.
        String resumedGroup = group != null ? group + "-resumed" : null;
        @Cleanup
        CheckpointConsumer<String> resumed = newConsumer(topic, stored(checkpoint), resumedGroup);
        assertThat(drain(resumed)).as("messages from before the checkpoint").isEmpty();
    }

    @Test
    public void testChainedRestoreKeepsUntouchedSegment() throws Exception {
        String topic = newTopic(2);
        @Cleanup
        Producer<String> producer = newProducer(topic);
        List<String> history = publish(producer, "history", 40);

        CheckpointConsumer<String> first = newConsumer(topic, Checkpoint.earliest(), null);
        receive(first, history.size());
        Checkpoint afterHistory = first.checkpoint();
        first.close();

        // Restored from the first checkpoint, the second consumer receives from one segment only.
        CheckpointConsumer<String> second = newConsumer(topic, stored(afterHistory), null);
        publish(producer, "new", 1);
        assertThat(second.receive(Duration.ofSeconds(5))).extracting(Message::value).isEqualTo("new-0");
        Checkpoint chained = second.checkpoint();
        second.close();

        @Cleanup
        CheckpointConsumer<String> resumed = newConsumer(topic, stored(chained), null);
        assertThat(drain(resumed)).as("messages from before the checkpoint").isEmpty();
    }

    @Test
    public void testRestoreKeepsSealedSegmentReadToItsEnd() throws Exception {
        String topic = newTopic(1);
        @Cleanup
        Producer<String> producer = newProducer(topic);
        List<String> history = publish(producer, "history", 20);

        CheckpointConsumer<String> first = newConsumer(topic, Checkpoint.earliest(), null);
        receive(first, history.size());
        Checkpoint atParentEnd = first.checkpoint();
        first.close();

        admin.scalableTopics().splitSegment(topic, activeSegmentIds(topic).get(0));
        Awaitility.await().untilAsserted(() -> assertThat(activeSegmentIds(topic)).hasSize(2));

        // Restored at the end of the now sealed parent, the second consumer receives nothing from it.
        CheckpointConsumer<String> second = newConsumer(topic, stored(atParentEnd), null);
        publish(producer, "new", 1);
        assertThat(second.receive(Duration.ofSeconds(5))).extracting(Message::value).isEqualTo("new-0");
        Checkpoint chained = second.checkpoint();
        second.close();

        @Cleanup
        CheckpointConsumer<String> resumed = newConsumer(topic, stored(chained), null);
        assertThat(drain(resumed)).as("messages from before the checkpoint").isEmpty();
    }

    /**
     * A checkpoint taken before the consumer received anything resumes where the consumer started, also when
     * messages are published while it attaches to the segments: the restored consumer delivers exactly what the
     * original one went on to deliver.
     */
    @Test
    public void testCheckpointAtLatestStartMatchesLiveConsumer() throws Exception {
        String topic = newTopic(2);
        @Cleanup
        Producer<String> producer = newProducer(topic);

        AtomicBoolean publishing = new AtomicBoolean(true);
        AtomicInteger published = new AtomicInteger();
        List<CompletableFuture<MessageId>> sends = new ArrayList<>();
        Thread publisher = new Thread(() -> {
            while (publishing.get()) {
                int i = published.getAndIncrement();
                sends.add(producer.async().newMessage().key("key-" + i).value("value-" + i).send());
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100));
            }
        }, "publisher");
        publisher.start();
        Checkpoint atStart;
        CheckpointConsumer<String> live;
        try {
            Awaitility.await().until(() -> published.get() >= 200);
            live = newConsumer(topic, Checkpoint.latest(), null);
            atStart = live.checkpoint();
            int publishedAtStart = published.get();
            Awaitility.await().until(() -> published.get() >= publishedAtStart + 200);
        } finally {
            publishing.set(false);
            publisher.join();
        }
        CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);

        List<String> delivered = drain(live);
        live.close();

        // Restored from the checkpoint object itself; the other tests restore from its serialized form.
        @Cleanup
        CheckpointConsumer<String> resumed = newConsumer(topic, atStart, null);
        List<String> restored = drain(resumed);
        Set<String> replayed = new HashSet<>(restored);
        replayed.removeAll(delivered);
        Set<String> lost = new HashSet<>(delivered);
        lost.removeAll(restored);
        assertThat(replayed).as("delivered after the restore, but published before the consumer started")
                .isEmpty();
        assertThat(lost).as("delivered by the consumer, but not after the restore").isEmpty();
        assertThat(restored).hasSameSizeAs(delivered);
    }

    // --- Helpers ---

    private String newTopic(int segments) throws Exception {
        String topic = newScalableTopic(segments);
        // Only the admin split below may change the layout.
        admin.scalableTopics().setAutoScalePolicy(topic,
                AutoScalePolicyOverride.builder().enabled(false).build());
        return topic;
    }

    private Producer<String> newProducer(String topic) throws Exception {
        return v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
    }

    private CheckpointConsumer<String> newConsumer(String topic, Checkpoint start, String group) throws Exception {
        CheckpointConsumerBuilder<String> builder = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(start);
        if (group != null) {
            builder.consumerGroup(group);
        }
        return builder.create();
    }

    /** The checkpoint as an application restores it from its external storage. */
    private static Checkpoint stored(Checkpoint checkpoint) throws IOException {
        return Checkpoint.fromByteArray(checkpoint.toByteArray());
    }

    /** Publish {@code count} messages with distinct keys, so they spread over the segments. */
    private static List<String> publish(Producer<String> producer, String prefix, int count) throws Exception {
        List<String> values = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String value = prefix + "-" + i;
            producer.newMessage().key(prefix + "-key-" + i).value(value).send();
            values.add(value);
        }
        return values;
    }

    private static void receive(CheckpointConsumer<String> consumer, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            assertThat(consumer.receive(Duration.ofSeconds(10))).as("message #%d of %d", i, count).isNotNull();
        }
    }

    /** Receive until nothing more arrives. */
    private static List<String> drain(CheckpointConsumer<String> consumer) throws Exception {
        List<String> values = new ArrayList<>();
        Message<String> msg;
        while ((msg = consumer.receive(Duration.ofSeconds(3))) != null) {
            values.add(msg.value());
        }
        return values;
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
