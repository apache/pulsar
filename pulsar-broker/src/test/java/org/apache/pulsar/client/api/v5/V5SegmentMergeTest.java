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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Coverage for {@code admin.scalableTopics().mergeSegments(...)}: two adjacent active
 * segments get merged into one, the originals seal, and the new child takes over their
 * combined hash range. Producers and consumers in flight must keep working without
 * drop or duplication.
 */
public class V5SegmentMergeTest extends V5ClientBaseTest {

    @Test
    public void testMergeMidFlowKeepsAllMessages() throws Exception {
        String topic = newScalableTopic(2);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("merge-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // Produce against both initial segments.
        int firstBatch = 50;
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < firstBatch; i++) {
            String v = "before-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }

        // Pick the two active segment IDs to merge.
        var meta = admin.scalableTopics().getMetadata(topic);
        List<Long> activeIds = new ArrayList<>();
        for (var seg : meta.getSegments().values()) {
            if (seg.isActive()) {
                activeIds.add(seg.getSegmentId());
            }
        }
        assertEquals(activeIds.size(), 2, "expected 2 active segments before merge");

        admin.scalableTopics().mergeSegments(topic, activeIds.get(0), activeIds.get(1));

        // The merge admin call is synchronous server-side, but the V5 client's DAG watch
        // is async — sending into the now-sealed children before the watch delivers the
        // new layout would fail with TopicTerminated. Wait for the producer's view to
        // catch up.
        Awaitility.await().untilAsserted(() -> {
            int active = 0;
            var m = admin.scalableTopics().getMetadata(topic);
            for (var seg : m.getSegments().values()) {
                if (seg.isActive()) {
                    active++;
                }
            }
            assertEquals(active, 1, "merge must collapse to 1 active segment");
        });

        // Produce after the merge: must land on the new (sole) segment.
        int secondBatch = 50;
        for (int i = 0; i < secondBatch; i++) {
            String v = "after-" + i;
            producer.newMessage().key("k-after-" + i).value(v).send();
            sent.add(v);
        }

        // Drain.
        Set<String> received = new HashSet<>();
        int total = firstBatch + secondBatch;
        for (int i = 0; i < total; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(10));
            assertNotNull(msg, "missed message #" + i + " (received so far: " + received.size() + ")");
            received.add(msg.value());
            consumer.acknowledge(msg.id());
        }

        assertEquals(received.size(), total, "expected " + total + " distinct messages");
        assertEquals(received, sent, "received set must equal sent set across the merge");
    }
}
