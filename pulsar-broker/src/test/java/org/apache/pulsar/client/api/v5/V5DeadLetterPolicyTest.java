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
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.BackoffPolicy;
import org.apache.pulsar.client.api.v5.config.DeadLetterPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.util.RetryMessageUtil;
import org.testng.annotations.Test;

/**
 * Coverage for {@link QueueConsumerBuilder#deadLetterPolicy(DeadLetterPolicy)}: after
 * {@code maxRedeliverCount} negative-acks, the V5 layer forwards the message to the
 * configured dead-letter topic.
 *
 * <p>V5 owns the DLQ at the consumer layer (not per-segment), so the DLQ topic is
 * itself a scalable {@code topic://} topic and a single shared producer fans messages
 * from every segment into it.
 */
public class V5DeadLetterPolicyTest extends V5ClientBaseTest {

    @Test
    public void testMessageGoesToScalableDlqWhenExplicitlyConfigured() throws Exception {
        String topic = newScalableTopic(1);
        String dlqTopic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("dlq-sub")
                .negativeAckRedeliveryBackoff(BackoffPolicy.exponential(
                        Duration.ofMillis(200), Duration.ofMillis(200)))
                .deadLetterPolicy(new DeadLetterPolicy(2, null, dlqTopic, null))
                .subscribe();

        @Cleanup
        QueueConsumer<byte[]> dlqConsumer = v5Client.newQueueConsumer(Schema.bytes())
                .topic(dlqTopic)
                .subscriptionName("dlq-watcher")
                .subscribe();

        producer.newMessage().value("dead").send();

        // V4 DLQ kicks in only when redeliveryCount > maxRedeliverCount (strictly greater).
        // With maxRedeliverCount=2, the user sees deliveries with counts 0, 1, 2 (three
        // total), and the fourth delivery (count=3) is intercepted and forwarded to DLQ.
        for (int i = 0; i < 3; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "expected delivery #" + (i + 1));
            assertEquals(msg.value(), "dead");
            consumer.negativeAcknowledge(msg.id());
        }

        Message<byte[]> dlqMsg = dlqConsumer.receive(Duration.ofSeconds(10));
        assertNotNull(dlqMsg, "message did not land on the DLQ topic");
        assertEquals(new String(dlqMsg.value()), "dead");
        dlqConsumer.acknowledge(dlqMsg.id());
    }

    @Test
    public void testMessageGoesToDefaultScalableDlqTopic() throws Exception {
        String topic = newScalableTopic(1);
        // When deadLetterTopic is null, V5 defaults to topic://<tenant>/<ns>/<source-local>-DLQ.
        // Pre-create it so the V5 producer's layout lookup succeeds.
        String defaultDlqTopic = topic + RetryMessageUtil.DLQ_GROUP_TOPIC_SUFFIX;
        admin.scalableTopics().createScalableTopic(defaultDlqTopic, 1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("default-dlq-sub")
                .negativeAckRedeliveryBackoff(BackoffPolicy.exponential(
                        Duration.ofMillis(200), Duration.ofMillis(200)))
                .deadLetterPolicy(DeadLetterPolicy.of(1))
                .subscribe();

        @Cleanup
        QueueConsumer<byte[]> dlqConsumer = v5Client.newQueueConsumer(Schema.bytes())
                .topic(defaultDlqTopic)
                .subscriptionName("dlq-watcher")
                .subscribe();

        producer.newMessage().value("dead-default").send();

        // maxRedeliverCount=1 → user sees deliveries with counts 0 and 1, the
        // third delivery (count=2) is intercepted.
        for (int i = 0; i < 2; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "expected delivery #" + (i + 1));
            assertEquals(msg.value(), "dead-default");
            consumer.negativeAcknowledge(msg.id());
        }

        Message<byte[]> dlqMsg = dlqConsumer.receive(Duration.ofSeconds(10));
        assertNotNull(dlqMsg, "message did not land on the default DLQ topic");
        assertEquals(new String(dlqMsg.value()), "dead-default");
        dlqConsumer.acknowledge(dlqMsg.id());
    }

    /**
     * The V5 DLQ producer must preserve message key, user properties, and event time,
     * and attach origin metadata (REAL_TOPIC / REAL_SUBSCRIPTION / ORIGIN_MESSAGE_ID)
     * so DLQ consumers can correlate back to the source delivery.
     */
    @Test
    public void testDlqMessagePreservesKeyPropertiesAndOriginMetadata() throws Exception {
        String topic = newScalableTopic(1);
        String dlqTopic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("dlq-meta-sub")
                .negativeAckRedeliveryBackoff(BackoffPolicy.exponential(
                        Duration.ofMillis(200), Duration.ofMillis(200)))
                .deadLetterPolicy(new DeadLetterPolicy(1, null, dlqTopic, null))
                .subscribe();
        @Cleanup
        QueueConsumer<byte[]> dlqConsumer = v5Client.newQueueConsumer(Schema.bytes())
                .topic(dlqTopic)
                .subscriptionName("dlq-meta-watcher")
                .subscribe();

        long eventTime = System.currentTimeMillis();
        producer.newMessage()
                .key("k-42")
                .value("payload")
                .eventTime(Instant.ofEpochMilli(eventTime))
                .property("user-prop", "user-val")
                .send();

        for (int i = 0; i < 2; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg);
            consumer.negativeAcknowledge(msg.id());
        }

        Message<byte[]> dlqMsg = dlqConsumer.receive(Duration.ofSeconds(10));
        assertNotNull(dlqMsg, "message did not land on the DLQ topic");
        assertEquals(new String(dlqMsg.value()), "payload");
        assertEquals(dlqMsg.key().orElse(null), "k-42");
        assertEquals(dlqMsg.eventTime().map(Instant::toEpochMilli).orElse(-1L).longValue(),
                eventTime);

        Map<String, String> props = dlqMsg.properties();
        assertEquals(props.get("user-prop"), "user-val", "user properties must be preserved");
        assertEquals(props.get(RetryMessageUtil.SYSTEM_PROPERTY_REAL_TOPIC), topic);
        assertEquals(props.get(RetryMessageUtil.SYSTEM_PROPERTY_REAL_SUBSCRIPTION), "dlq-meta-sub");
        assertNotNull(props.get(RetryMessageUtil.PROPERTY_ORIGIN_MESSAGE_ID),
                "origin message id must be attached");
        assertTrue(props.get(RetryMessageUtil.PROPERTY_ORIGIN_MESSAGE_ID).matches("\\d+:\\d+(:.+)?"),
                "origin message id should look like a v4 message id, got: "
                        + props.get(RetryMessageUtil.PROPERTY_ORIGIN_MESSAGE_ID));

        dlqConsumer.acknowledge(dlqMsg.id());
    }

    /**
     * Verifies a single V5-side DLQ producer handles forwarding from messages that
     * arrived on different source segments — i.e., the V5 layer (not the v4
     * per-segment {@code ConsumerImpl}) owns the DLQ producer.
     */
    @Test
    public void testDlqAcrossMultipleSourceSegments() throws Exception {
        String topic = newScalableTopic(3);
        String dlqTopic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("dlq-multi-sub")
                .negativeAckRedeliveryBackoff(BackoffPolicy.exponential(
                        Duration.ofMillis(200), Duration.ofMillis(200)))
                .deadLetterPolicy(new DeadLetterPolicy(1, null, dlqTopic, null))
                .subscribe();
        @Cleanup
        QueueConsumer<byte[]> dlqConsumer = v5Client.newQueueConsumer(Schema.bytes())
                .topic(dlqTopic)
                .subscriptionName("dlq-multi-watcher")
                .subscribe();

        // 6 keys → spread across the 3 segments.
        int n = 6;
        for (int i = 0; i < n; i++) {
            producer.newMessage().key("k-" + i).value("v-" + i).send();
        }

        // For each source delivery (counts 0 and 1), nack everything; the third
        // delivery (count=2) of each message is intercepted and DLQ-forwarded.
        for (int round = 0; round < 2; round++) {
            for (int i = 0; i < n; i++) {
                Message<String> msg = consumer.receive(Duration.ofSeconds(5));
                assertNotNull(msg, "round " + round + " expected delivery #" + (i + 1));
                consumer.negativeAcknowledge(msg.id());
            }
        }

        Set<String> dlqValues = new HashSet<>();
        for (int i = 0; i < n; i++) {
            Message<byte[]> dlqMsg = dlqConsumer.receive(Duration.ofSeconds(10));
            assertNotNull(dlqMsg, "expected DLQ message #" + (i + 1));
            dlqValues.add(new String(dlqMsg.value()));
            dlqConsumer.acknowledge(dlqMsg.id());
        }
        for (int i = 0; i < n; i++) {
            assertTrue(dlqValues.contains("v-" + i), "missing DLQ value v-" + i);
        }
    }
}
