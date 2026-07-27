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
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Smoke test: end-to-end verification that the longest V5 path works against a real broker.
 *
 * <p>Exercises: admin {@code createScalableTopic} → {@code DagWatchClient} session → segment
 * lookup → per-segment v4 producer creation → wire-format send → segment v4 consumer attach →
 * receive → ack on the V5 {@link QueueConsumer}.
 */
public class V5SmokeTest extends V5ClientBaseTest {

    @Test
    public void testProduceAndConsumeOneMessageOnSingleSegmentTopic() throws Exception {
        String topic = newScalableTopic(1);
        PulsarClient client = newV5Client();

        Producer<String> producer = client.newProducer(Schema.string())
                .topic(topic)
                .create();
        track(producer);

        QueueConsumer<String> consumer = client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("smoke-sub")
                .subscribe();
        track(consumer);

        MessageId sentId = producer.newMessage().value("hello-pulsar-v5").send();
        assertNotNull(sentId, "producer must return a message id");

        Message<String> received = consumer.receive(Duration.ofSeconds(10));
        assertNotNull(received, "consumer must receive within timeout");
        assertEquals(received.value(), "hello-pulsar-v5");
        consumer.acknowledge(received.id());
    }
}
