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
package org.apache.pulsar.tests.integration.topologies;

import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class ClientTestBase {
    private static final int RECEIVE_TIMEOUT_SECONDS = 3;

    public void resetCursorCompatibility(String serviceUrl, String serviceHttpUrl, String topicName) throws Exception {
        final String subName = "my-sub";
        @Cleanup final PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();
        @Cleanup final PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(serviceHttpUrl)
                .build();

        Message<String> lastMsg = null;
        {
            @Cleanup
            Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                    .enableBatching(false).topic(topicName).create();
            @Cleanup
            Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                    .topic(topicName).subscriptionName(subName).subscribe();
            for (int i = 0; i < 50; i++) {
                producer.send("msg" + i);
            }
            for (int i = 0; i < 10; i++) {
                lastMsg = consumer.receive();
                assertNotNull(lastMsg);
                consumer.acknowledge(lastMsg);
            }
        }

        admin.topics().resetCursor(topicName, subName, lastMsg.getMessageId());
        {
            @Cleanup
            Consumer<String> consumer2 =
                    pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionName(subName).subscribe();
            Message<String> message = consumer2.receive(RECEIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertEquals(message.getMessageId(), lastMsg.getMessageId());
        }

        admin.topics().resetCursorAsync(topicName, subName, lastMsg.getMessageId()).get(3, TimeUnit.SECONDS);
        {
            @Cleanup
            Consumer<String> consumer3 =
                    pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionName(subName).subscribe();
            Message<String> message = consumer3.receive(RECEIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertEquals(message.getMessageId(), lastMsg.getMessageId());
        }
    }
}
