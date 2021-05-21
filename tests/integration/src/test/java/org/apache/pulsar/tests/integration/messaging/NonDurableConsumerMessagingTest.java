/**
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
package org.apache.pulsar.tests.integration.messaging;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.testng.annotations.Test;

public class NonDurableConsumerMessagingTest extends MessagingBase {

    @Test(dataProvider = "ServiceUrls")
    public void testNonDurableConsumer(Supplier<String> serviceUrl) throws Exception {
        final String topicName = getNonPartitionedTopic("test-non-durable-consumer", false);
        @Cleanup
        final PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl.get()).build();

        int numMessages = 20;

        try (final Producer<byte[]> producer = client.newProducer()
            .topic(topicName)
            .create()) {

            IntStream.range(0, numMessages).forEach(i -> {
                String payload = "message-" + i;
                producer.sendAsync(payload.getBytes(UTF_8));
            });
            // flush the producer to make sure all messages are persisted
            producer.flush();

            try (final Consumer<byte[]> consumer = client.newConsumer()
                .topic(topicName)
                .subscriptionName("non-durable-consumer")
                .subscriptionMode(SubscriptionMode.NonDurable)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe()) {

                for (int i = 0; i < numMessages; i++) {
                    Message<byte[]> msg = consumer.receive();
                    assertEquals(new String(msg.getValue(), UTF_8), "message-" + i);
                }
            }
        }

    }
}
