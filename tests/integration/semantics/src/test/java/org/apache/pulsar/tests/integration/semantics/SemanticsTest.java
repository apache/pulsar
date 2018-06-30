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
package org.apache.pulsar.tests.integration.semantics;

import static org.testng.Assert.assertEquals;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.tests.topologies.PulsarClusterTestBase;
import org.testng.annotations.Test;

/**
 * Test pulsar produce/consume semantics
 */
public class SemanticsTest extends PulsarClusterTestBase {

    @Test
    public void testPublishAndConsumePlainTextServiceUrl() throws Exception {
        testPublishAndConsume(
            pulsarCluster.getPlainTextServiceUrl(), "test-publish-consume-plain-text");
    }

    private void testPublishAndConsume(String serviceUrl, String topicName) throws Exception {

        int numMessages = 10;

        try (PulsarClient client = PulsarClient.builder()
            .serviceUrl(serviceUrl)
            .build()) {

            try (Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscribe()) {

                try (Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(topicName)
                    .create()) {

                    for (int i = 0; i < numMessages; i++) {
                        producer.send("smoke-message-" + i);
                    }
                }

                for (int i = 0; i < numMessages; i++) {
                    Message<String> m = consumer.receive();
                    assertEquals("smoke-message-" + i, m.getValue());
                }
            }
        }
    }


}
