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
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Coverage for {@link QueueConsumerBuilder#ackTimeout(Duration)}: when a consumer
 * receives a message but doesn't ack within the configured timeout, the broker
 * redelivers it.
 */
public class V5AckTimeoutTest extends V5ClientBaseTest {

    @Test
    public void testUnackedMessageIsRedeliveredAfterAckTimeout() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        // Default ack timeout is disabled (or 60s); use a tight one so the test stays fast.
        // Pulsar enforces a minimum of 1s on ackTimeout.
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("ack-timeout-sub")
                .ackTimeout(Duration.ofSeconds(1))
                .subscribe();

        producer.newMessage().value("once").send();

        // Receive but don't ack.
        Message<String> first = consumer.receive(Duration.ofSeconds(5));
        assertNotNull(first);
        assertEquals(first.value(), "once");

        // The broker's ack-timeout sweeper runs at ackTimeout/2 cadence, so wait
        // generously past 1s for the redelivery to fire.
        Message<String> redelivered = consumer.receive(Duration.ofSeconds(10));
        assertNotNull(redelivered, "ack-timeout did not trigger redelivery");
        assertEquals(redelivered.value(), "once");
        consumer.acknowledge(redelivered.id());
    }
}
