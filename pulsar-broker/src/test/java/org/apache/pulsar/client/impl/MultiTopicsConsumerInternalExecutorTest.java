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

import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class MultiTopicsConsumerInternalExecutorTest extends SharedPulsarBaseTest {

    @Test(timeOut = 60000)
    public void testInternalConsumersShareTheMultiTopicsConsumersInternalExecutor() throws Exception {
        String topic = newTopicName();
        String partitionedTopic = newTopicName();
        String addedTopic = newTopicName();
        admin.topics().createPartitionedTopic(partitionedTopic, 2);
        Map<String, List<String>> receivedByTopic = new ConcurrentHashMap<>();
        // two internal executors, so that consumers that pick their own don't all share the parent's
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(getBrokerServiceUrl()).ioThreads(2).build();

        @Cleanup
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topics(List.of(topic, partitionedTopic))
                .subscriptionName("sub")
                .messageListener((c, msg) -> {
                    receivedByTopic.computeIfAbsent(TopicName.get(msg.getTopicName()).getPartitionedTopicName(),
                            k -> new CopyOnWriteArrayList<>()).add(msg.getValue());
                    c.acknowledgeAsync(msg);
                })
                .subscribe();
        MultiTopicsConsumerImpl<String> multiTopicsConsumer = (MultiTopicsConsumerImpl<String>) consumer;
        multiTopicsConsumer.subscribeAsync(addedTopic, true).get(30, TimeUnit.SECONDS);

        // the consumers of the topics, the partitions and the topic added later
        assertThat(multiTopicsConsumer.getConsumers()).hasSize(4);
        for (ConsumerImpl<String> internalConsumer : multiTopicsConsumer.getConsumers()) {
            assertThat(internalConsumer.internalPinnedExecutor)
                    .isSameAs(multiTopicsConsumer.internalPinnedExecutor);
        }

        // each topic's messages are received in order
        int messages = 100;
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < messages; i++) {
            expected.add("message-" + i);
        }
        for (String t : List.of(topic, partitionedTopic, addedTopic)) {
            @Cleanup
            Producer<String> producer = client.newProducer(Schema.STRING).topic(t)
                    .enableBatching(false).create();
            for (String value : expected) {
                producer.send(value);
            }
        }
        Awaitility.await().untilAsserted(() -> {
            assertThat(receivedByTopic.get(TopicName.get(topic).getPartitionedTopicName()))
                    .containsExactlyElementsOf(expected);
            assertThat(receivedByTopic.get(TopicName.get(partitionedTopic).getPartitionedTopicName()))
                    .containsExactlyInAnyOrderElementsOf(expected);
            assertThat(receivedByTopic.get(TopicName.get(addedTopic).getPartitionedTopicName()))
                    .containsExactlyElementsOf(expected);
        });
    }
}
