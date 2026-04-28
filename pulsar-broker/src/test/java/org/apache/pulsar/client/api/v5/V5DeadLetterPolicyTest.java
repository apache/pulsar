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
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.BackoffPolicy;
import org.apache.pulsar.client.api.v5.config.DeadLetterPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Coverage for {@link QueueConsumerBuilder#deadLetterPolicy(DeadLetterPolicy)}: after
 * {@code maxRedeliverCount} negative-acks, the broker forwards the message to the
 * configured dead-letter topic.
 *
 * <p><b>Known V5 gap:</b> the DLQ topic is currently a non-scalable persistent topic.
 * The V5 source consumer's underlying v4 {@code ConsumerImpl} creates the DLQ producer
 * via {@code client.newProducer(...)}, which rejects {@code topic://} scalable topic
 * names. Routing the DLQ producer through V5's segment-bypass path is required before
 * a scalable DLQ can be used here.
 */
public class V5DeadLetterPolicyTest extends V5ClientBaseTest {

    @Test
    public void testMessageGoesToDlqAfterMaxRedeliveries() throws Exception {
        String topic = newScalableTopic(1);
        // Non-scalable DLQ topic — see class-level note about the V5 gap.
        String dlqTopic = "persistent://" + getNamespace() + "/dlq-explicit";

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

        // Subscribe to the DLQ via the V4 client (DLQ is a regular persistent topic).
        @Cleanup
        org.apache.pulsar.client.api.Consumer<String> dlqConsumer = pulsarClient
                .newConsumer(org.apache.pulsar.client.api.Schema.STRING)
                .topic(dlqTopic)
                .subscriptionName("dlq-watcher")
                .subscriptionInitialPosition(
                        org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest)
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

        // Now it should appear on the DLQ topic.
        org.apache.pulsar.client.api.Message<String> dlqMsg =
                dlqConsumer.receive(10, java.util.concurrent.TimeUnit.SECONDS);
        assertNotNull(dlqMsg, "message did not land on the DLQ topic");
        assertEquals(dlqMsg.getValue(), "dead");
        dlqConsumer.acknowledge(dlqMsg);
    }
}
