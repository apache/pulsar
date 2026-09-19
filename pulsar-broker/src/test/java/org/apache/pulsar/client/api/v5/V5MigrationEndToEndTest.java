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
import static org.testng.Assert.expectThrows;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * End-to-end tests for the PIP-475 regular-to-scalable migration, exercising the full
 * operator timeline against a live broker: V5 clients operate on a regular topic via the
 * synthetic layout, the operator migrates, and the same clients transparently transition
 * to the real DAG with no data loss across the migration boundary.
 */
public class V5MigrationEndToEndTest extends V5ClientBaseTest {

    private String baseName(String suffix) {
        return getNamespace() + "/" + suffix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    @Test
    public void testV5ProducerSurvivesMigrationAndAllDataIsConsumable() throws Exception {
        // Timeline:
        //  1. A 2-partition regular topic exists.
        //  2. A V5 producer publishes batch #1 through the synthetic layout (mod-N routing
        //     to the legacy segments == the partitions).
        //  3. The operator migrates the topic (only the V5 producer is attached, and it
        //     carries the V5-managed marker, so the precheck passes without --force).
        //  4. The same V5 producer publishes batch #2 — it has transparently transitioned to
        //     the real DAG and now range-routes to the new active child segments.
        //  5. A V5 queue consumer reading EARLIEST drains everything: batch #1 from the sealed
        //     legacy parents, batch #2 from the active children.
        String topic = baseName("e2e");
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic("persistent://" + topic)
                .create();

        Set<String> sent = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            String v = "pre-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }

        // Migrate. The attached V5 producer is marked, so no legacy connections are seen.
        admin.scalableTopics().migrateToScalable(topic, false);

        // The producer transparently follows the layout change to the real DAG.
        for (int i = 0; i < 20; i++) {
            String v = "post-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }

        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic("persistent://" + topic)
                .subscriptionName("e2e-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        Set<String> received = new HashSet<>();
        for (int i = 0; i < 40; i++) {
            org.apache.pulsar.client.api.v5.Message<String> m = consumer.receive(Duration.ofSeconds(10));
            assertNotNull(m, "expected 40 messages, missing after " + received.size());
            received.add(m.value());
            consumer.acknowledge(m.id());
        }
        assertEquals(received, sent, "every pre- and post-migration message must be consumable");
    }

    @Test
    public void testV5AsyncProducerSurvivesMigration() throws Exception {
        // Same as the producer-survives-migration case, but driving the *async* producer API
        // (producer.async()...send()). Async sends issued right after migration must ride
        // through the synthetic→real-DAG transition — the per-segment producer for a
        // just-terminated partition fails, and the send must retry onto an active child
        // rather than fail the user's future.
        String topic = baseName("e2e-async");
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic("persistent://" + topic)
                .create();

        java.util.List<java.util.concurrent.CompletableFuture<MessageId>> sends =
                new java.util.ArrayList<>();
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            String v = "pre-" + i;
            sends.add(producer.async().newMessage().key("k-" + i).value(v).send());
            sent.add(v);
        }

        admin.scalableTopics().migrateToScalable(topic, false);

        // Issue the post-migration batch via async sends without awaiting in between, so the
        // retry-across-transition path is exercised.
        for (int i = 0; i < 20; i++) {
            String v = "post-" + i;
            sends.add(producer.async().newMessage().key("k-" + i).value(v).send());
            sent.add(v);
        }
        // Every async send must eventually complete (none fail across the migration boundary).
        java.util.concurrent.CompletableFuture
                .allOf(sends.toArray(new java.util.concurrent.CompletableFuture[0]))
                .get(60, java.util.concurrent.TimeUnit.SECONDS);

        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic("persistent://" + topic)
                .subscriptionName("e2e-async-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        Set<String> received = new HashSet<>();
        for (int i = 0; i < 40; i++) {
            org.apache.pulsar.client.api.v5.Message<String> m = consumer.receive(Duration.ofSeconds(10));
            assertNotNull(m, "expected 40 messages, missing after " + received.size());
            received.add(m.value());
            consumer.acknowledge(m.id());
        }
        assertEquals(received, sent,
                "every async pre- and post-migration message must be consumable");
    }

    @Test
    public void testV4ProducerLockedOutAfterMigration() throws Exception {
        // After migration the old topic is terminated, so a legacy v4 producer can no longer
        // write to it — the produce fails with a terminated-topic error.
        String topic = baseName("lockout");
        admin.topics().createNonPartitionedTopic("persistent://" + topic);

        admin.scalableTopics().migrateToScalable(topic, false);

        // A v4 producer either fails to create on, or fails to send to, the now-terminated
        // (and scalable-shadowed) persistent:// topic.
        expectThrows(org.apache.pulsar.client.api.PulsarClientException.class, () -> {
            org.apache.pulsar.client.api.Producer<byte[]> v4Producer = pulsarClient.newProducer()
                    .topic("persistent://" + topic)
                    .create();
            v4Producer.send("blocked".getBytes());
        });
    }
}
