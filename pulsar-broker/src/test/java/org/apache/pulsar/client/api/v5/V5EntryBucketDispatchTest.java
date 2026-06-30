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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * PIP-486 end-to-end: a single stream consumer on an entry-bucketed segment.
 *
 * <p>A one-segment scalable topic with the default entry-bucket budget (4) gives that segment
 * {@code N = 4} entry-buckets. A lone stream consumer is therefore assigned all four buckets and
 * subscribes {@code Key_Shared} STICKY over the whole ring (rather than {@code Exclusive}), so the
 * broker routes each entry by its producer-stamped entry-bucket range. This exercises the full
 * produce → per-bucket batch + stamp → dispatch-by-bucket → consume path for the single-owner case
 * (the multi-consumer bucket handoff is a separate change). The observable contract is unchanged:
 * per-key order is preserved and no message is dropped or duplicated.
 */
public class V5EntryBucketDispatchTest extends V5ClientBaseTest {

    @Test
    public void testBucketedSegmentPreservesPerKeyOrderAndDeliversAll() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("bucket-dispatch")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // 8 keys × 25 messages, interleaved. With per-bucket batching on, same-key messages must
        // still arrive in send order (only holds if every entry routes to the one consumer that owns
        // the key's bucket), and every message must be delivered exactly once.
        List<String> keys = List.of("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel");
        int perKey = 25;
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

        Map<String, List<String>> received = new HashMap<>();
        for (String k : keys) {
            received.put(k, new ArrayList<>());
        }
        int total = keys.size() * perKey;
        MessageId last = null;
        for (int i = 0; i < total; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "missed message #" + i);
            String key = msg.key().orElseThrow(() -> new AssertionError("missing key"));
            received.get(key).add(msg.value());
            last = msg.id();
        }
        consumer.acknowledgeCumulative(last);

        for (String k : keys) {
            assertEquals(received.get(k), sent.get(k), "per-key order must be preserved for key=" + k);
        }
    }
}
