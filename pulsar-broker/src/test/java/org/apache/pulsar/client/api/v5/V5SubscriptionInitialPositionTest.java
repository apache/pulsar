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
import java.time.Duration;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Coverage for {@code subscriptionInitialPosition}. For each consumer type that takes
 * the option (Queue, Stream — the CheckpointConsumer uses {@code startPosition} instead,
 * tested separately), assert that EARLIEST sees prior data and LATEST does not.
 */
public class V5SubscriptionInitialPositionTest extends V5ClientBaseTest {

    @Test
    public void testQueueConsumerEarliestSeesPriorMessages() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        // Pre-existing data.
        for (int i = 0; i < 5; i++) {
            producer.newMessage().value("pre-" + i).send();
        }

        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("queue-earliest-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        for (int i = 0; i < 5; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "EARLIEST queue consumer must see pre-existing pre-" + i);
            assertEquals(msg.value(), "pre-" + i);
            consumer.acknowledge(msg.id());
        }
    }

    @Test
    public void testQueueConsumerLatestSkipsPriorMessages() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        for (int i = 0; i < 5; i++) {
            producer.newMessage().value("pre-" + i).send();
        }

        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("queue-latest-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.LATEST)
                .subscribe();

        // Idle for now — pre-existing messages must be invisible.
        assertNull(consumer.receive(Duration.ofMillis(200)),
                "LATEST queue consumer must skip pre-existing messages");

        // Anything published after subscribe is delivered.
        producer.newMessage().value("post-1").send();
        Message<String> msg = consumer.receive(Duration.ofSeconds(5));
        assertNotNull(msg);
        assertEquals(msg.value(), "post-1");
        consumer.acknowledge(msg.id());
    }

    @Test
    public void testStreamConsumerEarliestSeesPriorMessages() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        for (int i = 0; i < 5; i++) {
            producer.newMessage().value("pre-" + i).send();
        }

        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("stream-earliest-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        MessageId last = null;
        for (int i = 0; i < 5; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "EARLIEST stream consumer must see pre-" + i);
            assertEquals(msg.value(), "pre-" + i);
            last = msg.id();
        }
        consumer.acknowledgeCumulative(last);
    }

    @Test
    public void testStreamConsumerLatestSkipsPriorMessages() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        for (int i = 0; i < 5; i++) {
            producer.newMessage().value("pre-" + i).send();
        }

        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("stream-latest-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.LATEST)
                .subscribe();

        assertNull(consumer.receive(Duration.ofMillis(200)),
                "LATEST stream consumer must skip pre-existing messages");

        producer.newMessage().value("post-1").send();
        Message<String> msg = consumer.receive(Duration.ofSeconds(5));
        assertNotNull(msg);
        assertEquals(msg.value(), "post-1");
        consumer.acknowledgeCumulative(msg.id());
    }
}
