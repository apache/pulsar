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
import java.time.Instant;
import java.util.Map;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Coverage for the {@link MessageBuilder} metadata setters: {@code property}/{@code properties},
 * {@code eventTime}, {@code deliverAfter}, {@code deliverAt}. Each scenario sends one message
 * with the metadata set on the producer side and asserts the consumer sees it back.
 *
 * <p>Single-segment scalable topic — the wire format is the same regardless of segment count
 * so multi-segment tests don't add coverage here.
 */
public class V5MessageMetadataTest extends V5ClientBaseTest {

    @Test
    public void testEventTimeIsPropagated() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("event-time-sub")
                .subscribe();

        Instant eventTime = Instant.parse("2024-01-15T08:30:00Z");
        producer.newMessage()
                .value("with-event-time")
                .eventTime(eventTime)
                .send();

        Message<String> msg = consumer.receive(Duration.ofSeconds(5));
        assertNotNull(msg);
        assertTrue(msg.eventTime().isPresent(), "consumer must see the event time");
        assertEquals(msg.eventTime().get(), eventTime);
        consumer.acknowledge(msg.id());
    }

    @Test
    public void testEventTimeAbsentWhenNotSet() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("no-event-time-sub")
                .subscribe();

        producer.newMessage().value("no-event-time").send();

        Message<String> msg = consumer.receive(Duration.ofSeconds(5));
        assertNotNull(msg);
        // The wire carries event time as 0 when unset; the V5 API must surface that as
        // an empty Optional so users can distinguish "no event time" from "epoch".
        assertTrue(msg.eventTime().isEmpty(),
                "eventTime should be absent when the producer didn't set it, was: " + msg.eventTime());
        consumer.acknowledge(msg.id());
    }

    @Test
    public void testPropertiesPropagateOneAtATime() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("props-single-sub")
                .subscribe();

        producer.newMessage()
                .value("with-props")
                .property("a", "1")
                .property("b", "two")
                .send();

        Message<String> msg = consumer.receive(Duration.ofSeconds(5));
        assertNotNull(msg);
        assertEquals(msg.properties(), Map.of("a", "1", "b", "two"));
        consumer.acknowledge(msg.id());
    }

    @Test
    public void testPropertiesPropagateAsBatch() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("props-batch-sub")
                .subscribe();

        Map<String, String> props = Map.of("k1", "v1", "k2", "v2", "k3", "v3");
        producer.newMessage()
                .value("with-props-batch")
                .properties(props)
                .send();

        Message<String> msg = consumer.receive(Duration.ofSeconds(5));
        assertNotNull(msg);
        assertEquals(msg.properties(), props);
        consumer.acknowledge(msg.id());
    }

    @Test
    public void testDeliverAfterDelaysVisibility() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        // Shared subscription is required for delayed delivery.
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("delay-after-sub")
                .subscribe();

        long sentNanos = System.nanoTime();
        producer.newMessage()
                .value("delayed")
                .deliverAfter(Duration.ofSeconds(2))
                .send();

        // A short poll right after send must not see the message yet.
        assertNull(consumer.receive(Duration.ofMillis(500)),
                "delayed message must not be visible before deliverAfter elapses");

        // Wait long enough for the broker's redelivery tracker to release it.
        Message<String> msg = consumer.receive(Duration.ofSeconds(10));
        long elapsedMs = (System.nanoTime() - sentNanos) / 1_000_000;
        assertNotNull(msg, "delayed message did not arrive after delay");
        assertEquals(msg.value(), "delayed");
        assertTrue(elapsedMs >= 1500,
                "deliverAfter delivered too early: " + elapsedMs + "ms");
        consumer.acknowledge(msg.id());
    }

    @Test
    public void testDeliverAtDelaysVisibility() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("delay-at-sub")
                .subscribe();

        Instant deliverAt = Instant.now().plusSeconds(2);
        producer.newMessage()
                .value("delayed-at")
                .deliverAt(deliverAt)
                .send();

        assertNull(consumer.receive(Duration.ofMillis(500)),
                "deliverAt must not deliver before the timestamp");

        Message<String> msg = consumer.receive(Duration.ofSeconds(10));
        assertNotNull(msg, "deliverAt did not deliver");
        assertEquals(msg.value(), "delayed-at");
        // Allow 500ms slack on the lower bound for clock-skew / scheduling jitter.
        assertTrue(Instant.now().isAfter(deliverAt.minusMillis(500)),
                "delivered earlier than the configured timestamp");
        consumer.acknowledge(msg.id());
    }
}
