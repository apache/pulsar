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
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Producer sequence-id behavior: {@link Producer#lastSequenceId()},
 * {@link ProducerBuilder#initialSequenceId(long)}, and explicit per-message
 * {@link MessageMetadata#sequenceId(long)}.
 *
 * <p>Single-segment scalable topic with broker-side deduplication enabled by the shared
 * cluster (see {@code SharedPulsarCluster.setBrokerDeduplicationEnabled(true)}), so
 * sending the same {@code sequenceId} twice gives the producer a no-op on the second
 * call.
 */
public class V5ProducerSequenceIdTest extends V5ClientBaseTest {

    @Test
    public void testLastSequenceIdAdvancesAsMessagesAreSent() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .producerName("seq-1")
                .create();

        // Before any send, lastSequenceId is "no message yet" (-1).
        assertEquals(producer.lastSequenceId(), -1L,
                "lastSequenceId before first send must be -1");

        for (int i = 0; i < 5; i++) {
            producer.newMessage().value("v-" + i).send();
        }
        // Without explicit sequenceId, the producer auto-increments from 0; after 5
        // sends the last one had id 4.
        assertEquals(producer.lastSequenceId(), 4L);
    }

    @Test
    public void testInitialSequenceIdShiftsTheStartingPoint() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .producerName("seq-2")
                .initialSequenceId(99)
                .create();

        // Before send: lastSequenceId reflects the configured initial.
        assertEquals(producer.lastSequenceId(), 99L,
                "initialSequenceId must seed lastSequenceId");

        producer.newMessage().value("post-init").send();
        assertEquals(producer.lastSequenceId(), 100L,
                "next message after initialSequenceId=99 should have id 100");
    }

    @Test
    public void testExplicitSequenceIdIsHonored() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .producerName("seq-3")
                .create();

        producer.newMessage().value("explicit").sequenceId(123L).send();
        assertEquals(producer.lastSequenceId(), 123L);
    }

    @Test
    public void testDeduplicationDropsRepeatedSequenceId() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .producerName("seq-dedup")
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("dedup-sub")
                .subscribe();

        // Same sequenceId twice — the broker must accept the first and dedup the second.
        // Both sends complete without an exception (the second returns a sentinel message
        // id; we don't pin the exact value because that's a broker-internal contract).
        MessageId firstId = producer.newMessage().value("once").sequenceId(7L).send();
        assertNotNull(firstId);
        producer.newMessage().value("once-dup").sequenceId(7L).send();

        // Observable behavior: consumer sees the first message exactly once.
        Message<String> msg = consumer.receive(Duration.ofSeconds(5));
        assertNotNull(msg);
        assertEquals(msg.value(), "once");
        consumer.acknowledge(msg.id());

        assertNull(consumer.receive(Duration.ofMillis(200)),
                "duplicate sequenceId must not produce a second consumer-visible message");
    }
}
