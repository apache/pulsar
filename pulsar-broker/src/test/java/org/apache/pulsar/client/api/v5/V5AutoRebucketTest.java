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
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.policies.data.ScalableTopicMetadata;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * PIP-486 segments-vs-buckets policy, UC#2 end to end: on a low-throughput topic (below the
 * split-vs-rebucket rate floor), surplus stream consumers must be served by growing the
 * segment's entry-buckets — an automatic rebucket rollover — instead of materializing physical
 * segments for consumer count alone.
 */
public class V5AutoRebucketTest extends V5ClientBaseTest {

    @Test
    public void testConsumerSurplusOnColdTopicRebucketsInsteadOfSplitting() throws Exception {
        String topic = newScalableTopic(1);
        String subscription = "auto-rebucket";

        // Five consumers on a cold one-segment topic (N=4 by default): the per-subscription
        // surplus (5 > 1 segment) is below the 1k msg/s split floor and beyond the bucket
        // capacity (5 > 4), so the controller must roll the segment over to 8 buckets.
        List<StreamConsumer<String>> consumers = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            consumers.add(track(v5Client.newStreamConsumer(Schema.string())
                    .topic(topic)
                    .subscriptionName(subscription)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                    .subscribe()));
        }

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            ScalableTopicMetadata md = admin.scalableTopics().getMetadata(topic);
            List<ScalableTopicMetadata.SegmentInfo> active = md.getSegments().values().stream()
                    .filter(ScalableTopicMetadata.SegmentInfo::isActive).toList();
            assertEquals(active.size(), 1,
                    "a cold topic must not split for consumer count — expected one active segment");
            assertEquals(active.get(0).getEntryBucketSplits().size() + 1, 8,
                    "the surplus must be absorbed by an automatic rebucket to 8");
            assertEquals(md.getSegments().size(), 2,
                    "exactly one rollover: sealed parent + successor");
        });

        // Once the (empty) parent drains, every consumer must move to the successor — none
        // may stay pinned to the drained parent, and none may idle.
        long successorId = admin.scalableTopics().getMetadata(topic).getSegments().values().stream()
                .filter(ScalableTopicMetadata.SegmentInfo::isActive).findFirst().orElseThrow()
                .getSegmentId();
        String successorTopic = admin.scalableTopics().getStats(topic).getSegments().values()
                .stream().filter(seg -> seg.name().endsWith("-" + successorId)).findFirst()
                .orElseThrow().name();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var sub = getTopicReference(successorTopic).orElseThrow().getSubscription(subscription);
            assertTrue(sub != null && sub.getConsumers().size() == 5,
                    "all five consumers must attach to the successor, got "
                            + (sub == null ? "no subscription" : sub.getConsumers().size()));
        });

        // The rebucketed segment serves all five consumers: keyed traffic must reach them
        // collectively, per-key in order, with every key wholly on one consumer.
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            keys.add("key-" + i);
        }
        int perKey = 5;
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

        List<Map<String, List<String>>> got = new ArrayList<>();
        List<Thread> drainers = new ArrayList<>();
        for (StreamConsumer<String> consumer : consumers) {
            Map<String, List<String>> into = new ConcurrentHashMap<>();
            got.add(into);
            Thread t = new Thread(() -> {
                try {
                    while (true) {
                        Message<String> msg = consumer.receive(Duration.ofSeconds(3));
                        if (msg == null) {
                            return;
                        }
                        String key = msg.key().orElseThrow();
                        into.computeIfAbsent(key, __ -> new ArrayList<>()).add(msg.value());
                        consumer.acknowledgeCumulative(msg.id());
                    }
                } catch (Exception ignored) {
                }
            });
            t.start();
            drainers.add(t);
        }
        for (Thread t : drainers) {
            t.join();
        }

        int receiving = 0;
        for (String k : keys) {
            List<String> combined = null;
            for (Map<String, List<String>> into : got) {
                List<String> values = into.get(k);
                if (values == null) {
                    continue;
                }
                assertTrue(combined == null, "key " + k + " was split across consumers");
                combined = values;
            }
            assertEquals(combined, sent.get(k), "per-key order/content for key=" + k);
        }
        for (Map<String, List<String>> into : got) {
            if (!into.isEmpty()) {
                receiving++;
            }
        }
        assertTrue(receiving >= 2, "expected the bucket fan-out to feed several consumers, got "
                + receiving);
    }
}
