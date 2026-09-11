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
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * End-to-end coverage for the parent-topic admin seek + clear-backlog operations on a
 * scalable-topic subscription. Exercises the real per-segment cursor-reset path
 * (managed-ledger {@code resetCursor(timestamp)} / {@code clearBacklog}), which the
 * controller-level mock-based tests do not cover.
 */
public class V5SeekSubscriptionTest extends V5ClientBaseTest {

    @Test
    public void testSeekRewindsCursorAcrossSingleSegment() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .batchingPolicy(org.apache.pulsar.client.api.v5.config.BatchingPolicy.ofDisabled())
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("seek-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // Produce 5 messages, consume + ack — drains the cursor so the rewind is observable.
        for (int i = 0; i < 5; i++) {
            producer.newMessage().value("v-" + i).send();
        }
        for (int i = 0; i < 5; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg);
            consumer.acknowledge(msg.id());
        }

        // Snapshot the rewind target — anything published before this should be redelivered.
        long mark = System.currentTimeMillis();
        // Slack for the broker's wall-clock vs the test's: producing immediately at `mark`
        // can land at `mark` or `mark+1`. Sleep a tick so post-mark messages have a strictly
        // later publish time.
        Thread.sleep(10);

        for (int i = 5; i < 10; i++) {
            producer.newMessage().value("v-" + i).send();
        }
        for (int i = 5; i < 10; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg);
            consumer.acknowledge(msg.id());
        }

        // Drained — confirm.
        assertNull(consumer.receive(Duration.ofMillis(500)));

        // Admin seek back to the mark — post-mark messages must be redelivered.
        admin.scalableTopics().seekSubscription(topic, "seek-sub", mark);

        Set<String> redelivered = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(10));
            assertNotNull(msg, "expected redelivery of post-mark message #" + i);
            redelivered.add(msg.value());
            consumer.acknowledge(msg.id());
        }
        assertEquals(redelivered, Set.of("v-5", "v-6", "v-7", "v-8", "v-9"),
                "seek must redeliver exactly the post-mark window");
    }

    /**
     * Regression: a freshly-split active child segment that has received no messages
     * straddles {@code timestampMs} but has an empty managed ledger; the per-segment
     * {@code resetCursor} on it would have failed with {@code SubscriptionInvalidCursorPosition},
     * which used to bring the entire parent-level seek down. The fix treats empty
     * segments as a no-op.
     */
    @Test
    public void testSeekToleratesEmptyChildSegmentsAfterSplit() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .batchingPolicy(org.apache.pulsar.client.api.v5.config.BatchingPolicy.ofDisabled())
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("split-seek-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // Populate the initial segment.
        for (int i = 0; i < 5; i++) {
            producer.newMessage().value("pre-" + i).send();
        }

        // Drain so we don't redeliver them later.
        for (int i = 0; i < 5; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg);
            consumer.acknowledge(msg.id());
        }

        // Split — children are empty; parent is sealed with all 5 messages.
        long activeSegmentId = -1;
        var meta = admin.scalableTopics().getMetadata(topic);
        for (var seg : meta.getSegments().values()) {
            if (seg.isActive()) {
                activeSegmentId = seg.getSegmentId();
                break;
            }
        }
        assertTrue(activeSegmentId >= 0);
        admin.scalableTopics().splitSegment(topic, activeSegmentId);

        // Wait for the split to be visible.
        Awaitility.await().untilAsserted(() -> {
            int active = 0;
            var m = admin.scalableTopics().getMetadata(topic);
            for (var seg : m.getSegments().values()) {
                if (seg.isActive()) {
                    active++;
                }
            }
            assertEquals(active, 2, "split must produce 2 active children");
        });

        // Seek to "now" — this exercises the bug. The two empty active children
        // straddle the timestamp; without the fix, the controller's allOf would fail
        // because resetCursor on an empty managed ledger throws
        // SubscriptionInvalidCursorPosition.
        long now = System.currentTimeMillis();
        admin.scalableTopics().seekSubscription(topic, "split-seek-sub", now);

        // After seek, no backlog (the sealed parent's data is all from before `now`,
        // and the children have no data). So receive must time out.
        assertNull(consumer.receive(Duration.ofSeconds(2)),
                "seek across empty children must succeed and leave no undelivered messages");
    }

    @Test
    public void testClearBacklogDropsAllUndeliveredMessages() throws Exception {
        String topic = newScalableTopic(2);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .batchingPolicy(org.apache.pulsar.client.api.v5.config.BatchingPolicy.ofDisabled())
                .create();

        // Establish the subscription via a short-lived consumer — and then close it. The
        // V5 receive-queue would otherwise prefetch the produced messages into its
        // client-side buffer, masking the broker-side cursor advance from clearBacklog.
        // Closing also releases the segment-cursor fences so clearBacklog itself can
        // fence them cleanly.
        QueueConsumer<String> bootstrap = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("clear-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        bootstrap.close();

        for (int i = 0; i < 20; i++) {
            producer.newMessage().key("k-" + i).value("v-" + i).send();
        }

        admin.scalableTopics().clearBacklog(topic, "clear-sub");

        // Re-attach the consumer — every cursor is at the end, so no backlog remains.
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("clear-sub")
                .subscribe();

        assertNull(consumer.receive(Duration.ofSeconds(2)),
                "clear-backlog must skip every undelivered message");

        // Subsequent messages still flow through.
        producer.newMessage().value("after-clear").send();
        Message<String> msg = consumer.receive(Duration.ofSeconds(5));
        assertNotNull(msg);
        assertEquals(msg.value(), "after-clear");
        consumer.acknowledge(msg.id());
    }
}
