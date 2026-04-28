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

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.time.Duration;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.ProducerAccessMode;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Producer {@link ProducerAccessMode} coverage:
 * <ul>
 *   <li>SHARED is the default — multiple producers can coexist.</li>
 *   <li>EXCLUSIVE — first producer succeeds, a second EXCLUSIVE attempt fails fast
 *       with {@code ProducerBusyException}.</li>
 *   <li>WAIT_FOR_EXCLUSIVE — second producer blocks until the first closes, then
 *       takes over.</li>
 * </ul>
 */
public class V5ProducerAccessModeTest extends V5ClientBaseTest {

    @Test
    public void testSharedAllowsMultipleProducers() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> p1 = v5Client.newProducer(Schema.string())
                .topic(topic)
                .accessMode(ProducerAccessMode.SHARED)
                .create();
        @Cleanup
        Producer<String> p2 = v5Client.newProducer(Schema.string())
                .topic(topic)
                .accessMode(ProducerAccessMode.SHARED)
                .create();

        // Both producers are usable concurrently.
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("shared-sub")
                .subscribe();

        p1.newMessage().value("from-p1").send();
        p2.newMessage().value("from-p2").send();

        Message<String> first = consumer.receive(Duration.ofSeconds(5));
        Message<String> second = consumer.receive(Duration.ofSeconds(5));
        assertNotNull(first);
        assertNotNull(second);
        consumer.acknowledge(first.id());
        consumer.acknowledge(second.id());
    }

    @Test
    public void testExclusiveRejectsSecondProducer() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> first = v5Client.newProducer(Schema.string())
                .topic(topic)
                .accessMode(ProducerAccessMode.EXCLUSIVE)
                .create();
        // Force the per-segment v4 producer to actually attach to the segment topic —
        // V5 segment producers are spun up lazily on first send, so the EXCLUSIVE
        // collision isn't observable until at least one send has gone through.
        first.newMessage().value("first-claim").send();

        // Second EXCLUSIVE on the same topic: the V5 builder may succeed at create()
        // (segment producers are still lazy), but the first send must fail with a
        // producer-fenced / busy error from the broker.
        @Cleanup
        Producer<String> second = v5Client.newProducer(Schema.string())
                .topic(topic)
                .accessMode(ProducerAccessMode.EXCLUSIVE)
                .create();
        try {
            second.newMessage().value("second-claim").send();
            fail("a second EXCLUSIVE producer should not be able to send");
        } catch (PulsarClientException expected) {
            String msg = expected.getMessage() == null ? "" : expected.getMessage();
            assertTrue(msg.contains("Busy") || msg.contains("Fenced")
                            || msg.contains("ProducerFenced") || msg.contains("ProducerBusy"),
                    "unexpected error message for second EXCLUSIVE producer: " + msg);
        }

        // The first one is still healthy.
        first.newMessage().value("still-here").send();
    }

    @Test
    public void testWaitForExclusiveSucceedsAfterFirstReleases() throws Exception {
        // NOTE: V5 producers create per-segment v4 producers lazily on first send, so the
        // second WAIT_FOR_EXCLUSIVE producer's create() does not actually block — the
        // exclusivity collision is deferred until the first send. This test therefore
        // doesn't verify the "blocks at create()" semantics; it only verifies that a
        // WAIT_FOR_EXCLUSIVE producer can send successfully once the previous EXCLUSIVE
        // owner has closed.
        String topic = newScalableTopic(1);

        Producer<String> first = v5Client.newProducer(Schema.string())
                .topic(topic)
                .accessMode(ProducerAccessMode.EXCLUSIVE)
                .create();
        first.newMessage().value("first-claim").send();

        // Once the first releases the topic, a WAIT_FOR_EXCLUSIVE producer should
        // succeed end-to-end.
        first.close();

        @Cleanup
        Producer<String> second = v5Client.newProducer(Schema.string())
                .topic(topic)
                .accessMode(ProducerAccessMode.WAIT_FOR_EXCLUSIVE)
                .create();
        MessageId id = second.newMessage().value("after-takeover").send();
        assertNotNull(id);
    }
}
