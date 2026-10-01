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
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Basic end-to-end coverage for {@link StreamConsumer}: ordered cumulative-ack
 * receive flow, {@code receiveMulti}, idle receive timeout, and the consumer
 * accessors. All scenarios use a single-segment scalable topic.
 *
 * <p>Multi-segment cumulative-ack-with-position-vector behavior and consumer
 * rebalance scenarios live in dedicated scalable-topic suites.
 */
public class V5StreamConsumerBasicTest extends V5ClientBaseTest {

    @Test
    public void testProduceAndCumulativeAck() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("stream-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        int numMessages = 50;
        for (int i = 0; i < numMessages; i++) {
            producer.newMessage().value("msg-" + i).send();
        }

        // Receive all and ack cumulatively at the last id only.
        MessageId last = null;
        for (int i = 0; i < numMessages; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "expected message " + i + " but receive timed out");
            // Single segment: stream consumer must preserve send order.
            assertEquals(msg.value(), "msg-" + i,
                    "out-of-order delivery at index " + i);
            last = msg.id();
        }
        assertNotNull(last);
        consumer.acknowledgeCumulative(last);

        // Nothing should redeliver after a cumulative ack of the last id.
        assertNull(consumer.receive(Duration.ofMillis(200)),
                "stream consumer redelivered a message after cumulative ack of last id");
    }

    @Test
    public void testReceiveMultiReturnsBatch() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("multi-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        int produced = 10;
        for (int i = 0; i < produced; i++) {
            producer.newMessage().value("v-" + i).send();
        }

        // Ask for up to 20 with a short timeout. Only 10 are available, so receiveMulti
        // waits until the timeout fires (or the cap is hit). Keep the timeout small so
        // the test stays fast.
        Messages<String> batch = consumer.receiveMulti(20, Duration.ofMillis(500));
        assertNotNull(batch);
        assertTrue(batch.count() >= 1 && batch.count() <= produced,
                "unexpected batch size: " + batch.count());
        assertNotNull(batch.lastId(), "lastId() must be set on a non-empty batch");

        // Iterating yields exactly batch.count() messages.
        int seen = 0;
        for (Message<String> ignored : batch) {
            seen++;
        }
        assertEquals(seen, batch.count(),
                "iteration count must match Messages.count()");
        consumer.acknowledgeCumulative(batch.lastId());
    }

    @Test
    public void testReceiveTimeoutReturnsNullWhenNoMessages() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("idle-sub")
                .subscribe();

        Message<String> msg = consumer.receive(Duration.ofMillis(200));
        assertNull(msg, "receive(timeout) must return null on idle topic");
    }

    @Test
    public void testTopicSubscriptionAndConsumerNameAccessors() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("accessors-sub")
                .consumerName("accessors-consumer")
                .subscribe();

        assertEquals(consumer.topic(), topic);
        assertEquals(consumer.subscription(), "accessors-sub");
        assertEquals(consumer.consumerName(), "accessors-consumer");
    }
}
