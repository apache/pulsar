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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Redelivery tests that replace an entry of the broker's topic map, so they run on a broker of their own.
 * {@link MessageRedeliveryTest} has the tests that share one.
 */
@Test(groups = "broker-impl")
public class MessageRedeliveryOwnBrokerTest extends ProducerConsumerBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    /**
     * A redeliver called while the consumer reconnects bumps the client epoch after the subscribe was sent with
     * the previous one. The broker must still learn the new epoch, or the client discards every message.
     */
    @Test(timeOut = 60_000)
    public void testRedeliveryWhileReconnecting() throws Exception {
        final String topic = "persistent://my-property/my-ns/testRedeliveryWhileReconnecting";
        final String subName = "my-sub";
        // A client of its own, so that the consumer's connection only carries the consumer's subscribe
        @Cleanup
        PulsarClient consumerClient = newPulsarClient(lookupUrl.toString(), 0);
        @Cleanup
        ConsumerImpl<String> consumer = (ConsumerImpl<String>) consumerClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create()) {
            producer.send("Pulsar");
        }

        // Hold the topic, so that the subscribe sent by the reconnection stays pending on the broker
        Map<String, CompletableFuture<Optional<Topic>>> topics = pulsar.getBrokerService().getTopics();
        CompletableFuture<Optional<Topic>> topicFuture = topics.get(topic);
        CompletableFuture<Optional<Topic>> heldTopicFuture = new CompletableFuture<>();
        topics.put(topic, heldTopicFuture);
        try {
            consumer.getClientCnx().channel().close();
            Awaitility.await().until(() -> {
                ClientCnx cnx = consumer.getClientCnx();
                return consumer.getState() == HandlerState.State.Connecting && cnx != null
                        && !cnx.pendingRequests.isEmpty();
            });

            consumer.redeliverUnacknowledgedMessages();
        } finally {
            topics.put(topic, topicFuture);
            heldTopicFuture.complete(topicFuture.join());
        }

        Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(message.getValue(), "Pulsar");
    }
}
