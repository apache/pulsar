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
import io.netty.util.concurrent.EventExecutor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.Checkpoint;
import org.apache.pulsar.client.api.v5.CheckpointConsumer;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.V5ClientBaseTest;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * An ungrouped {@link CheckpointConsumer} started at {@link Checkpoint#latest()} delivers every message
 * published after it started. A segment that a split or merge creates after that holds only such
 * messages, so it is read from its beginning, while the segments present at the start are read from
 * their end.
 */
public class V5CheckpointConsumerLatestStartTest extends V5ClientBaseTest {

    private static final int MESSAGES = 50;

    @Test
    public void testReadsSplitChildrenFromTheirBeginning() throws Exception {
        String topic = newTopic(1);
        @Cleanup
        Producer<String> producer = newProducer(topic);
        // A client of its own, so that stalling its IO thread leaves the producer running.
        PulsarClient consumerClient = newV5Client();
        @Cleanup
        CheckpointConsumer<String> consumer = newLatestConsumer(consumerClient, topic);
        producer.newMessage().key("k").value("before").send();
        assertThat(receive(consumer, 1)).containsExactly("before");

        // The consumer learns of the children from a layout update, which its IO thread delivers, and
        // the producer can write to them before their readers attach. Make it write first every time.
        List<String> sent;
        try (AutoCloseable stall = stallIoThreads(consumerClient)) {
            split(topic, activeSegmentIds(topic).get(0), 2);
            sent = publish(producer, "post-split");
        }

        assertThat(receive(consumer, sent.size())).containsExactlyInAnyOrderElementsOf(sent);
    }

    @Test
    public void testReadsMergedChildFromItsBeginning() throws Exception {
        String topic = newTopic(2);
        @Cleanup
        Producer<String> producer = newProducer(topic);
        PulsarClient consumerClient = newV5Client();
        @Cleanup
        CheckpointConsumer<String> consumer = newLatestConsumer(consumerClient, topic);
        producer.newMessage().key("k").value("before").send();
        assertThat(receive(consumer, 1)).containsExactly("before");

        List<String> sent;
        try (AutoCloseable stall = stallIoThreads(consumerClient)) {
            List<Long> parents = activeSegmentIds(topic);
            admin.scalableTopics().mergeSegments(topic, parents.get(0), parents.get(1));
            Awaitility.await().untilAsserted(() -> assertThat(activeSegmentIds(topic)).hasSize(1));
            sent = publish(producer, "post-merge");
        }

        assertThat(receive(consumer, sent.size())).containsExactlyInAnyOrderElementsOf(sent);
    }

    @Test
    public void testSkipsMessagesOfSegmentsPresentAtStart() throws Exception {
        String topic = newTopic(1);
        @Cleanup
        Producer<String> producer = newProducer(topic);
        // History on a sealed parent and on its children, all published before the consumer starts.
        publish(producer, "pre-split");
        split(topic, activeSegmentIds(topic).get(0), 2);
        publish(producer, "post-split");

        @Cleanup
        CheckpointConsumer<String> consumer = newLatestConsumer(v5Client, topic);
        List<String> sent = publish(producer, "after-start");

        assertThat(receive(consumer, sent.size())).containsExactlyInAnyOrderElementsOf(sent);
    }

    // --- Helpers ---

    private String newTopic(int segments) throws Exception {
        String topic = newScalableTopic(segments);
        // Only the admin splits and merges in the tests may change the layout.
        admin.scalableTopics().setAutoScalePolicy(topic,
                AutoScalePolicyOverride.builder().enabled(false).build());
        return topic;
    }

    private Producer<String> newProducer(String topic) throws Exception {
        return v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
    }

    private static CheckpointConsumer<String> newLatestConsumer(PulsarClient client, String topic)
            throws Exception {
        return client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(Checkpoint.latest())
                .create();
    }

    /** Publish {@link #MESSAGES} messages, each with its own key, and return their values. */
    private static List<String> publish(Producer<String> producer, String prefix) throws Exception {
        List<String> values = new ArrayList<>();
        for (int i = 0; i < MESSAGES; i++) {
            String value = prefix + "-" + i;
            producer.newMessage().key("key-" + i).value(value).send();
            values.add(value);
        }
        return values;
    }

    /**
     * Receive the expected number of messages, or as many as arrive, plus any that follow them (a
     * duplicate, or a message the start position excludes), and return their values.
     */
    private static List<String> receive(CheckpointConsumer<String> consumer, int expected) throws Exception {
        List<String> values = new ArrayList<>();
        Message<String> msg;
        while (values.size() < expected && (msg = consumer.receive(Duration.ofSeconds(10))) != null) {
            values.add(msg.value());
        }
        while ((msg = consumer.receive(Duration.ofMillis(500))) != null) {
            values.add(msg.value());
        }
        return values;
    }

    /**
     * Hold every IO thread of the client until the returned handle is closed: meanwhile the client
     * processes nothing it receives, layout updates included.
     */
    private static AutoCloseable stallIoThreads(PulsarClient client) throws InterruptedException {
        List<EventExecutor> ioThreads = new ArrayList<>();
        ((PulsarClientV5) client).v4Client().eventLoopGroup().forEach(ioThreads::add);
        CompletableFuture<Void> release = new CompletableFuture<>();
        CountDownLatch stalled = new CountDownLatch(ioThreads.size());
        for (EventExecutor ioThread : ioThreads) {
            ioThread.execute(() -> {
                stalled.countDown();
                release.join();
            });
        }
        if (!stalled.await(10, TimeUnit.SECONDS)) {
            release.complete(null);
            throw new AssertionError("the client's IO threads did not stall");
        }
        return () -> release.complete(null);
    }

    private void split(String topic, long segmentId, int expectedActive) throws Exception {
        admin.scalableTopics().splitSegment(topic, segmentId);
        Awaitility.await().untilAsserted(() -> assertThat(activeSegmentIds(topic)).hasSize(expectedActive));
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
