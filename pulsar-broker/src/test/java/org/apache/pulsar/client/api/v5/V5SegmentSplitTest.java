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
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Coverage for {@code admin.scalableTopics().splitSegment(...)}: an admin-triggered split
 * mid-flow seals the parent and creates two child segments. Producers and consumers
 * already attached to the topic must keep working through the layout change without
 * dropping or duplicating messages.
 */
public class V5SegmentSplitTest extends V5ClientBaseTest {

    @Test
    public void testSplitMidFlowKeepsAllMessages() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("split-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // First batch: lands on the only initial segment (id=0).
        int firstBatch = 50;
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < firstBatch; i++) {
            String v = "before-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }

        // Find the active segment id, then split it.
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

        // The split admin call is synchronous server-side, but the V5 client's DAG watch
        // is async — sending into the now-sealed parent before the watch delivers the new
        // layout would fail with TopicTerminated. Wait for the producer's view to catch up.
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

        // Second batch: must land on the new children.
        int secondBatch = 50;
        for (int i = 0; i < secondBatch; i++) {
            String v = "after-" + i;
            producer.newMessage().key("k-after-" + i).value(v).send();
            sent.add(v);
        }

        // Drain everything via the consumer. Total = firstBatch + secondBatch.
        Set<String> received = new HashSet<>();
        int total = firstBatch + secondBatch;
        for (int i = 0; i < total; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(10));
            assertNotNull(msg, "missed message #" + i + " (received so far: " + received.size() + ")");
            received.add(msg.value());
            consumer.acknowledge(msg.id());
        }

        // No duplicates, no losses, across the split.
        assertEquals(received.size(), total, "expected " + total + " distinct messages");
        assertEquals(received, sent, "received set must equal sent set across the split");
    }
}
