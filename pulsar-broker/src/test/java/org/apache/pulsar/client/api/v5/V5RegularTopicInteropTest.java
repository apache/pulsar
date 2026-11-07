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
import static org.testng.Assert.expectThrows;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * End-to-end interop tests for V5 clients against regular (non-scalable) topics.
 *
 * <p>PIP-475: when a V5 client looks up a {@code persistent://...} topic that has not
 * yet been migrated to a scalable topic, the broker returns a synthetic layout that
 * wraps the existing partitions as legacy segments. The V5 SDK must:
 * <ul>
 *   <li>Route to legacy segments using v4 partitioned-topic mod-N routing so per-key
 *       destinations match what a v4 producer would do.</li>
 *   <li>Attach per-segment v4 producers/consumers to the underlying
 *       {@code persistent://...-partition-K} URIs.</li>
 * </ul>
 * These tests verify the SDK side end-to-end against a real broker.
 */
public class V5RegularTopicInteropTest extends V5ClientBaseTest {

    @Test
    public void testV5ProducerToPartitionedRegularTopicRoutesV4Compatibly() throws Exception {
        // V5 producer writes to a 4-partition regular topic. The broker returns a
        // synthetic layout; the V5 router uses mod-N over segment_id, which matches
        // v4 partitioned-topic routing. A v4 producer with the default configuration
        // publishes the same keys: each key must land in the same partition from both.
        String regular = "persistent://" + getNamespace() + "/regular-"
                + UUID.randomUUID().toString().substring(0, 8);
        admin.topics().createPartitionedTopic(regular, 4);

        List<org.apache.pulsar.client.api.Consumer<String>> v4Consumers = new ArrayList<>();
        for (int k = 0; k < 4; k++) {
            v4Consumers.add(track(pulsarClient
                    .newConsumer(org.apache.pulsar.client.api.Schema.STRING)
                    .topic(regular + "-partition-" + k)
                    .subscriptionName("v4-sub")
                    .subscriptionInitialPosition(
                            org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest)
                    .subscribe()));
        }

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(regular)
                .create();
        @Cleanup
        org.apache.pulsar.client.api.Producer<String> v4Producer = pulsarClient
                .newProducer(org.apache.pulsar.client.api.Schema.STRING)
                .topic(regular)
                .create();

        final int n = 64;
        for (int i = 0; i < n; i++) {
            producer.newMessage().key("k-" + i).value("v5-" + i).send();
            v4Producer.newMessage().key("k-" + i).value("v4-" + i).send();
        }

        Map<String, Integer> v5Partitions = new HashMap<>();
        Map<String, Integer> v4Partitions = new HashMap<>();
        for (int k = 0; k < 4; k++) {
            org.apache.pulsar.client.api.Message<String> m;
            while ((m = v4Consumers.get(k).receive(2, TimeUnit.SECONDS)) != null) {
                Map<String, Integer> partitions = m.getValue().startsWith("v5-") ? v5Partitions : v4Partitions;
                assertNull(partitions.put(m.getKey(), k), "key=" + m.getKey() + " received twice");
            }
        }
        assertEquals(v5Partitions.size(), n, "every V5 message must be received exactly once");
        assertEquals(v5Partitions, v4Partitions, "V5 must route every key to the partition v4 routes it to");
    }

    @Test
    public void testV5ProducerToNonPartitionedRegularTopicV4ConsumerReceives() throws Exception {
        // V5 producer writes to a non-partitioned regular topic. The broker returns a
        // synthetic layout with a single legacy segment wrapping the persistent:// URI.
        // A v4 consumer on the same URI should receive everything.
        String regular = "persistent://" + getNamespace() + "/regular-np-"
                + UUID.randomUUID().toString().substring(0, 8);
        admin.topics().createNonPartitionedTopic(regular);

        @Cleanup
        org.apache.pulsar.client.api.Consumer<String> v4Consumer = pulsarClient
                .newConsumer(org.apache.pulsar.client.api.Schema.STRING)
                .topic(regular)
                .subscriptionName("v4-sub")
                .subscriptionInitialPosition(
                        org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest)
                .subscribe();

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(regular)
                .create();

        final int n = 16;
        for (int i = 0; i < n; i++) {
            producer.newMessage().value("v-" + i).send();
        }

        Set<String> received = new HashSet<>();
        for (int i = 0; i < n; i++) {
            org.apache.pulsar.client.api.Message<String> m =
                    v4Consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(m, "v4 consumer must receive every V5 send");
            received.add(m.getValue());
        }
        assertEquals(received.size(), n);
        // No straggler messages beyond the n sends.
        assertNull(v4Consumer.receive(500, java.util.concurrent.TimeUnit.MILLISECONDS));
    }

    @Test
    public void testV5QueueConsumerFromPartitionedRegularTopic() throws Exception {
        // v4 producer writes to a 4-partition regular topic; V5 queue consumer
        // subscribes via the synthetic layout (one per-segment v4 consumer per
        // partition) and must receive every published message.
        String regular = "persistent://" + getNamespace() + "/regular-q-"
                + UUID.randomUUID().toString().substring(0, 8);
        admin.topics().createPartitionedTopic(regular, 4);

        @Cleanup
        org.apache.pulsar.client.api.Producer<String> v4Producer = pulsarClient
                .newProducer(org.apache.pulsar.client.api.Schema.STRING)
                .topic(regular)
                .create();

        @Cleanup
        QueueConsumer<String> v5Consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(regular)
                .subscriptionName("v5-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        final int n = 64;
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < n; i++) {
            String v = "v-" + i;
            v4Producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }

        Set<String> received = new HashSet<>();
        for (int i = 0; i < n; i++) {
            org.apache.pulsar.client.api.v5.Message<String> m = v5Consumer.receive(Duration.ofSeconds(5));
            assertNotNull(m, "V5 queue consumer must drain every v4 send");
            received.add(m.value());
            v5Consumer.acknowledge(m.id());
        }
        assertEquals(received, sent);
    }

    @Test
    public void testV5ProducerBuilderRejectsNonPersistent() {
        // The V5 builder must reject non-persistent:// inputs synchronously, with a
        // clear error rather than deferring to a broker-side failure.
        String topic = "non-persistent://" + getNamespace() + "/regular-"
                + UUID.randomUUID().toString().substring(0, 8);
        UnsupportedOperationException ex = expectThrows(UnsupportedOperationException.class,
                () -> v5Client.newProducer(Schema.string()).topic(topic).create());
        assertTrue(ex.getMessage().contains("non-persistent"), ex.getMessage());
    }
}
