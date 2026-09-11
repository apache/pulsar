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
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Coverage for the V5 producer's transparent retry across a layout transition.
 *
 * <p>The split admin call is synchronous server-side, but the V5 client's DAG watch is
 * async — the producer's view of the segment layout doesn't update until the watch
 * delivers the new revision. A send issued in this window targets the now-sealed parent
 * segment and the v4 layer surfaces either {@code TopicTerminated} (if the broker
 * replies to the still-open producer) or {@code AlreadyClosed} (if the v4 producer
 * noticed the seal first). In either case the V5 producer must drop the stale
 * per-segment producer, wait briefly for the layout watch to catch up, and re-route to
 * an active child — without surfacing the failure to the application.
 *
 * <p>This is the no-wait counterpart to {@link V5SegmentSplitTest}, which also exercises
 * a split mid-flow but with an explicit Awaitility wait before the post-split sends.
 */
public class V5ProducerSplitRetryTest extends V5ClientBaseTest {

    @Test
    public void testSendImmediatelyAfterSplitTransparentlyRetries() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("split-retry-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // Pre-split batch: lands on the only initial segment, opens the per-segment v4
        // producer that will become stale on split.
        int firstBatch = 20;
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < firstBatch; i++) {
            String v = "before-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }

        // Find and split the active segment.
        long activeSegmentId = -1;
        var meta = admin.scalableTopics().getMetadata(topic);
        for (var seg : meta.getSegments().values()) {
            if (seg.isActive()) {
                activeSegmentId = seg.getSegmentId();
                break;
            }
        }
        assertTrue(activeSegmentId >= 0, "expected exactly one active segment before split");

        admin.scalableTopics().splitSegment(topic, activeSegmentId);

        // Critical: do NOT wait for the V5 client's DAG watch to converge. The very next
        // send will target the now-sealed parent's per-segment v4 producer, which by now
        // has been closed by the broker — the V5 producer must observe AlreadyClosed (or
        // TopicTerminated), invalidate the stale entry, give the watch a moment to
        // deliver the new layout, and re-route to a child. From the application side,
        // every send must succeed.
        int secondBatch = 20;
        for (int i = 0; i < secondBatch; i++) {
            String v = "after-" + i;
            producer.newMessage().key("k-after-" + i).value(v).send();
            sent.add(v);
        }

        // Drain everything via the consumer.
        Set<String> received = new HashSet<>();
        int total = firstBatch + secondBatch;
        for (int i = 0; i < total; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(10));
            assertNotNull(msg, "missed message #" + i + " (received so far: " + received.size() + ")");
            received.add(msg.value());
            consumer.acknowledge(msg.id());
        }

        assertEquals(received.size(), total, "expected " + total + " distinct messages");
        assertEquals(received, sent, "received set must equal sent set across the split");
    }
}
