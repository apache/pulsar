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
package org.apache.pulsar.tests.integration.topologies;

import static org.testng.Assert.assertEquals;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.tests.TestRetrySupport;
import org.testng.Assert;
import org.testng.annotations.DataProvider;

public abstract class PulsarTestBase extends TestRetrySupport {

    @DataProvider(name = "TopicDomain")
    public Object[][] topicDomain() {
        return new Object[][] {
                {"persistent"},
                {"non-persistent"}
        };
    }

    public static String randomName() {
        return randomName(6);
    }

    public static String randomName(int numChars) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numChars; i++) {
            sb.append((char) (ThreadLocalRandom.current().nextInt(26) + 'a'));
        }
        return sb.toString();
    }

    protected static String generateNamespaceName() {
        return "ns-" + randomName(8);
    }

    protected static String generateTopicName(String topicPrefix, boolean isPersistent) {
        return generateTopicName("default", topicPrefix, isPersistent);
    }

    protected static String generateTopicName(String namespace, String topicPrefix, boolean isPersistent) {
        String topicName = new StringBuilder(topicPrefix)
                .append("-")
                .append(randomName(8))
                .append("-")
                .append(System.currentTimeMillis())
                .toString();
        if (isPersistent) {
            return "persistent://public/" + namespace + "/" + topicName;
        } else {
            return "non-persistent://public/" + namespace + "/" + topicName;
        }
    }

    protected void testPublishAndConsume(String serviceUrl, boolean isPersistent) throws Exception {
        String topicName = generateTopicName("testpubconsume", isPersistent);

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

    protected void testBatchMessagePublishAndConsume(String serviceUrl, boolean isPersistent) throws Exception {
        String topicName = generateTopicName("test-batch-publish-consume", isPersistent);

        final int numMessages = 10000;
        try (PulsarClient client = PulsarClient.builder()
            .serviceUrl(serviceUrl)
            .build()) {

            try (Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .receiverQueueSize(10000)
                .subscriptionName("my-sub")
                .subscribe()) {

                try (Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(topicName)
                    .blockIfQueueFull(true)
                    .create()) {

                    List<CompletableFuture<MessageId>> futures = new ArrayList<>();
                    for (int i = 0; i < numMessages; i++) {
                        futures.add(producer.sendAsync("smoke-message-" + i));
                    }
                    // Wait for all messages are publish succeed.
                    FutureUtil.waitForAll(futures).get();
                }

                for (int i = 0; i < numMessages; i++) {
                    Message<String> m = consumer.receive();
                    assertEquals("smoke-message-" + i, m.getValue());
                }
            }
        }
    }

    protected void testBatchIndexAckDisabled(String serviceUrl) throws Exception {
        String topicName = generateTopicName("test-batch-index-ack-disabled", true);
        final int numMessages = 100;
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build()) {

            try (Consumer<Integer> consumer = client.newConsumer(Schema.INT32)
                    .topic(topicName)
                    .subscriptionName("sub")
                    .receiverQueueSize(100)
                    .subscriptionType(SubscriptionType.Shared)
                    .enableBatchIndexAcknowledgment(false)
                    .ackTimeout(1, TimeUnit.SECONDS)
                    .subscribe();) {

                try (Producer<Integer> producer = client.newProducer(Schema.INT32)
                        .topic(topicName)
                        .batchingMaxPublishDelay(50, TimeUnit.MILLISECONDS)
                        .create()) {

                    List<CompletableFuture<MessageId>> futures = new ArrayList<>();
                    for (int i = 0; i < numMessages; i++) {
                        futures.add(producer.sendAsync(i));
                    }
                    // Wait for all messages are publish succeed.
                    FutureUtil.waitForAll(futures).get();
                }

                for (int i = 0; i < numMessages; i++) {
                    Message<Integer> m = consumer.receive();
                    if (i % 2 == 0) {
                        consumer.acknowledge(m);
                    }
                }

                Message<Integer> redelivery = consumer.receive(3, TimeUnit.SECONDS);
                Assert.assertNotNull(redelivery);
            }
        }
    }

    protected ObjectMapper jsonMapper () {
        return ObjectMapperFactory.getMapper().getObjectMapper();
    }
}
