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
package org.apache.pulsar.broker.service.schema;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

import lombok.Cleanup;
import org.apache.pulsar.broker.service.BkEnsemblesTestBase;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PartitionedTopicsSchemaTest extends BkEnsemblesTestBase {

    /**
     * Test that sequence id from a producer is correct when there are send errors
     */
    @Test
    public void partitionedTopicWithSchema() throws Exception {
        admin.namespaces().createNamespace("prop/my-test", Collections.singleton("usc"));

        String topicName = "prop/my-test/my-topic";

        admin.topics().createPartitionedTopic(topicName, 16);

        int N = 10;

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();

        CompletableFuture<Producer<String>> producerFuture = client.newProducer(Schema.STRING)
            .topic(topicName)
            .createAsync();
        CompletableFuture<Consumer<String>> consumerFuture = client.newConsumer(Schema.STRING)
            .topic(topicName)
            .subscriptionName("sub")
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribeAsync();

        CompletableFuture.allOf(producerFuture, consumerFuture).get();

        Producer<String> producer = producerFuture.get();
        Consumer<String> consumer = consumerFuture.get();

        for (int i = 0; i < N; i++) {
            producer.send("Hello-" + i);
        }

        consumer.close();
        producer.close();

        // Force topic reloading to re-open the schema multiple times in parallel
        admin.namespaces().unload("prop/my-test");

        producerFuture = client.newProducer(Schema.STRING)
            .topic(topicName)
            .createAsync();
        consumerFuture = client.newConsumer(Schema.STRING)
            .topic(topicName)
            .subscriptionName("sub")
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribeAsync();

        // Re-opening the topic should succeed
        CompletableFuture.allOf(producerFuture, consumerFuture).get();

        consumer = consumerFuture.get();

        Set<String> messages = new TreeSet<>();

        for (int i = 0; i < N; i++) {
            Message<String> msg = consumer.receive();
            messages.add(msg.getValue());
            consumer.acknowledge(msg);
        }

        assertEquals(messages.size(), N);
        for (int i = 0; i < N; i++) {
            assertTrue(messages.contains("Hello-" + i));
        }
    }

}
