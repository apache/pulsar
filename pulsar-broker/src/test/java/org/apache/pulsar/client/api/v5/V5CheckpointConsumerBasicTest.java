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
import java.util.ArrayList;
import java.util.List;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Basic end-to-end coverage for {@link CheckpointConsumer}: the unmanaged reader-style
 * API used by connector frameworks (Flink, Spark) — specifically the start-position
 * sentinels (earliest / latest), checkpoint roundtrip via {@link Checkpoint#toByteArray()},
 * resume from a saved checkpoint, and {@link CheckpointConsumer#seek(Checkpoint)}.
 *
 * <p>All scenarios use a single-segment scalable topic to keep the focus on the
 * consumer surface itself; cross-segment position-vector behavior lives in the
 * scalable-topic suites.
 */
public class V5CheckpointConsumerBasicTest extends V5ClientBaseTest {

    @Test
    public void testReadFromEarliest() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        for (int i = 0; i < 5; i++) {
            producer.newMessage().value("msg-" + i).send();
        }

        @Cleanup
        CheckpointConsumer<String> consumer = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(Checkpoint.earliest())
                .create();

        for (int i = 0; i < 5; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "expected message " + i);
            assertEquals(msg.value(), "msg-" + i, "out-of-order at index " + i);
        }
        assertNull(consumer.receive(Duration.ofMillis(200)),
                "no extra messages after draining");
    }

    @Test
    public void testReadFromLatestSkipsExistingMessages() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        // Pre-existing data — should not be visible to a LATEST consumer.
        for (int i = 0; i < 5; i++) {
            producer.newMessage().value("pre-" + i).send();
        }

        @Cleanup
        CheckpointConsumer<String> consumer = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(Checkpoint.latest())
                .create();

        // Idle until we publish more.
        assertNull(consumer.receive(Duration.ofMillis(200)),
                "LATEST consumer must not see pre-existing messages");

        for (int i = 0; i < 3; i++) {
            producer.newMessage().value("post-" + i).send();
        }
        for (int i = 0; i < 3; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "expected post-" + i);
            assertEquals(msg.value(), "post-" + i);
        }
    }

    @Test
    public void testCheckpointAndResume() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        for (int i = 0; i < 10; i++) {
            producer.newMessage().value("msg-" + i).send();
        }

        // Read 5, take a checkpoint, close.
        Checkpoint saved;
        {
            @Cleanup
            CheckpointConsumer<String> first = v5Client.newCheckpointConsumer(Schema.string())
                    .topic(topic)
                    .startPosition(Checkpoint.earliest())
                    .create();
            for (int i = 0; i < 5; i++) {
                Message<String> msg = first.receive(Duration.ofSeconds(5));
                assertNotNull(msg);
                assertEquals(msg.value(), "msg-" + i);
            }
            saved = first.checkpoint();
            assertNotNull(saved, "checkpoint() must return a non-null position");
        }

        // Reopen using the saved checkpoint — should pick up at msg-5.
        @Cleanup
        CheckpointConsumer<String> resumed = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(saved)
                .create();
        for (int i = 5; i < 10; i++) {
            Message<String> msg = resumed.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "expected msg-" + i);
            assertEquals(msg.value(), "msg-" + i,
                    "resume from checkpoint delivered the wrong message");
        }
        assertNull(resumed.receive(Duration.ofMillis(200)),
                "no extra messages after resuming through the tail");
    }

    @Test
    public void testCheckpointSerializationRoundtrip() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        for (int i = 0; i < 6; i++) {
            producer.newMessage().value("v-" + i).send();
        }

        // Read 3 then save a checkpoint as bytes, simulating external storage.
        byte[] savedBytes;
        {
            @Cleanup
            CheckpointConsumer<String> first = v5Client.newCheckpointConsumer(Schema.string())
                    .topic(topic)
                    .startPosition(Checkpoint.earliest())
                    .create();
            for (int i = 0; i < 3; i++) {
                first.receive(Duration.ofSeconds(5));
            }
            savedBytes = first.checkpoint().toByteArray();
            assertNotNull(savedBytes);
        }

        // Reopen by deserializing the checkpoint bytes — must resume at index 3.
        Checkpoint restored = Checkpoint.fromByteArray(savedBytes);
        @Cleanup
        CheckpointConsumer<String> resumed = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(restored)
                .create();
        List<String> received = new ArrayList<>();
        for (int i = 3; i < 6; i++) {
            Message<String> msg = resumed.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "expected v-" + i);
            received.add(msg.value());
        }
        assertEquals(received, List.of("v-3", "v-4", "v-5"));
    }

    @Test
    public void testSeekRewindsToEarlierCheckpoint() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        for (int i = 0; i < 6; i++) {
            producer.newMessage().value("v-" + i).send();
        }

        @Cleanup
        CheckpointConsumer<String> consumer = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(Checkpoint.earliest())
                .create();

        // Read 3, snapshot, read 3 more, then seek back to the snapshot — should
        // re-deliver the second batch.
        for (int i = 0; i < 3; i++) {
            assertEquals(consumer.receive(Duration.ofSeconds(5)).value(), "v-" + i);
        }
        Checkpoint mark = consumer.checkpoint();
        for (int i = 3; i < 6; i++) {
            assertEquals(consumer.receive(Duration.ofSeconds(5)).value(), "v-" + i);
        }

        consumer.seek(mark);
        for (int i = 3; i < 6; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "seek did not redeliver message v-" + i);
            assertEquals(msg.value(), "v-" + i);
        }
    }

    @Test
    public void testReceiveTimeoutReturnsNullWhenNoMessages() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        CheckpointConsumer<String> consumer = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(Checkpoint.earliest())
                .create();

        Message<String> msg = consumer.receive(Duration.ofMillis(200));
        assertNull(msg, "receive(timeout) must return null on idle topic");
    }

    @Test
    public void testTopicAccessor() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        CheckpointConsumer<String> consumer = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(Checkpoint.earliest())
                .create();

        assertEquals(consumer.topic(), topic);
    }
}
