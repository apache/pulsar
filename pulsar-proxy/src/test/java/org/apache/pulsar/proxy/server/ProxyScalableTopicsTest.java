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
package org.apache.pulsar.proxy.server;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * End-to-end verification that scalable topics ({@code topic://}) work through the Pulsar proxy
 * with the V5 client.
 *
 * <p>Exercises every control- and data-plane path that has to traverse the proxy:
 * <ul>
 *   <li>the DAG-watch lookup and per-segment producer/consumer connections ride the proxy's
 *       transparent (direct) mode via ordinary {@code topic://} / {@code segment://} lookups;</li>
 *   <li>the consumer's controller subscribe isn't tied to a specific broker, so it's sent over an
 *       any-broker connection that the proxy pairs to a broker it selects.</li>
 * </ul>
 */
public class ProxyScalableTopicsTest extends ProxyMultiBrokerBaseTest {

    @Test
    public void testProduceAndConsumeScalableTopicThroughProxy() throws Exception {
        // Two segments so the topic can span more than one broker behind the proxy.
        String topic = "topic://public/default/" + BrokerTestUtil.newUniqueName("scalable-proxy");
        admin.scalableTopics().createScalableTopic(topic, 2);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(getProxyServiceUrl())
                .build();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string())
                .topic(topic)
                .create();

        @Cleanup
        QueueConsumer<String> consumer = client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("proxy-sub")
                .subscribe();

        int numMessages = 20;
        for (int i = 0; i < numMessages; i++) {
            producer.newMessage().value("msg-" + i).send();
        }

        Set<String> received = new HashSet<>();
        for (int i = 0; i < numMessages; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(15));
            assertNotNull(msg, "consumer must receive a message within the timeout through the proxy");
            received.add(msg.value());
            consumer.acknowledge(msg.id());
        }
        assertEquals(received.size(), numMessages, "every produced message must be received through the proxy");
    }
}
