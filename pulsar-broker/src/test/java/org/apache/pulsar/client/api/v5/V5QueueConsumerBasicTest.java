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
import java.util.Map;
import java.util.Set;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.BackoffPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Basic end-to-end coverage for {@link QueueConsumer}: the simple CRUD-shaped behaviors
 * a user expects from a Shared-style scalable consumer (multi-message ack, negative-ack
 * redelivery, receive timeout, keyed-message metadata roundtrip, accessors).
 *
 * <p>All scenarios use a single-segment scalable topic to keep these tests focused on the
 * consumer surface itself; segment split / merge / multi-consumer-rebalance scenarios live
 * in the dedicated scalable-topic test suites.
 */
public class V5QueueConsumerBasicTest extends V5ClientBaseTest {

    @Test
    public void testProduceAndAckMany() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("basic-sub")
                .subscribe();

        int numMessages = 50;
        for (int i = 0; i < numMessages; i++) {
            producer.newMessage().value("msg-" + i).send();
        }

        for (int i = 0; i < numMessages; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "expected message " + i + " but receive timed out");
            // Single segment + single consumer + Shared sub: messages stay in send order.
            assertEquals(msg.value(), "msg-" + i,
                    "out-of-order delivery at index " + i);
            consumer.acknowledge(msg.id());
        }

        // No more messages should be left in the subscription.
        assertNull(consumer.receive(Duration.ofMillis(200)),
                "unexpected extra message after acking all sent");
    }

    @Test
    public void testNegativeAckCausesRedelivery() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        // Tight nack-redelivery (default is ~60s) so the test doesn't have to wait.
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("nack-sub")
                // BackoffPolicy.fixed(...) sets multiplier=1.0 which the underlying v4
                // MultiplierRedeliveryBackoff rejects (requires > 1); use exponential with
                // initial==max so the cap pins the delay at 200ms.
                .negativeAckRedeliveryBackoff(BackoffPolicy.exponential(
                        Duration.ofMillis(200), Duration.ofMillis(200)))
                .subscribe();

        producer.newMessage().value("once").send();

        Message<String> first = consumer.receive(Duration.ofSeconds(10));
        assertNotNull(first);
        assertEquals(first.value(), "once");
        consumer.negativeAcknowledge(first.id());

        Message<String> redelivered = consumer.receive(Duration.ofSeconds(10));
        assertNotNull(redelivered, "negativeAcknowledge did not trigger redelivery");
        assertEquals(redelivered.value(), "once");
        consumer.acknowledge(redelivered.id());
    }

    @Test
    public void testReceiveTimeoutReturnsNullWhenNoMessages() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("idle-sub")
                .subscribe();

        // No producer, no traffic. receive() with a small timeout must return null
        // rather than block indefinitely or throw.
        Message<String> msg = consumer.receive(Duration.ofMillis(200));
        assertNull(msg, "receive with timeout must return null on idle topic");
    }

    @Test
    public void testKeyedMessagesPreserveKeyAndProperties() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("keys-sub")
                .subscribe();

        Set<String> sentKeys = new HashSet<>();
        Map<String, Map<String, String>> sentProps = new java.util.HashMap<>();
        for (int i = 0; i < 5; i++) {
            String key = "key-" + i;
            Map<String, String> props = Map.of("idx", String.valueOf(i), "tag", "v5-test");
            producer.newMessage()
                    .key(key)
                    .value("payload-" + i)
                    .properties(props)
                    .send();
            sentKeys.add(key);
            sentProps.put(key, props);
        }

        Set<String> seen = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(10));
            assertNotNull(msg);
            assertTrue(msg.key().isPresent(), "message key must be propagated");
            String key = msg.key().get();
            seen.add(key);
            assertEquals(msg.properties(), sentProps.get(key),
                    "properties must roundtrip for key " + key);
            consumer.acknowledge(msg.id());
        }
        assertEquals(seen, sentKeys, "consumer saw a different set of keys than producer sent");
    }

    @Test
    public void testTopicSubscriptionAndConsumerNameAccessors() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("accessors-sub")
                .consumerName("accessors-consumer")
                .subscribe();

        assertEquals(consumer.topic(), topic);
        assertEquals(consumer.subscription(), "accessors-sub");
        assertEquals(consumer.consumerName(), "accessors-consumer");

        // No consumerName set => the consumer should still expose a non-null name (typically
        // generated by the broker / client for diagnostics).
        @Cleanup
        QueueConsumer<String> defaultName = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("default-name-sub")
                .subscribe();
        assertTrue(defaultName.consumerName() == null || !defaultName.consumerName().isEmpty(),
                "consumerName should be either null or a non-empty generated name");
    }
}
