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
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class BatchIndexAcknowledgmentFlushTest extends SharedPulsarBaseTest {
    @DataProvider
    public Object[][] receipts() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "receipts", timeOut = 90000)
    public void testConcurrentPartialAcksWithPeriodicFlush(boolean receipt) throws Exception {
        String topic = newTopicName();
        int messageCount = 256;
        assertThat(getConfig().isAcknowledgmentAtBatchIndexLevelEnabled()).isTrue();
        ExecutorService actors = Executors.newFixedThreadPool(2, new DefaultThreadFactory("partial-batch-acks"));
        try (Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                     .topic(topic)
                     .subscriptionName("sub")
                     .subscriptionType(SubscriptionType.Shared)
                     .receiverQueueSize(messageCount)
                     .enableBatchIndexAcknowledgment(true)
                     .isAckReceiptEnabled(receipt)
                     .acknowledgmentGroupTime(1, TimeUnit.MILLISECONDS)
                     .subscribe();
             Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                     .topic(topic)
                     .enableBatching(true)
                     .batchingMaxMessages(4)
                     .batchingMaxPublishDelay(1, TimeUnit.DAYS)
                     .create()) {
            List<CompletableFuture<MessageId>> sends = new ArrayList<>();
            for (int i = 0; i < messageCount; i++) {
                sends.add(producer.sendAsync("message-" + i));
            }
            producer.flush();
            CompletableFuture.allOf(sends.toArray(new CompletableFuture[0])).get(20, TimeUnit.SECONDS);
            List<Message<String>> messages = new ArrayList<>();
            Set<String> expectedRedeliveries = new HashSet<>();
            for (int i = 0; i < messageCount; i++) {
                Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
                assertThat(message.getValue()).isEqualTo("message-" + i);
                assertThat(((MessageIdAdv) message.getMessageId()).getBatchIndex()).isEqualTo(i % 4);
                messages.add(message);
                if (i % 4 >= 2) {
                    expectedRedeliveries.add(message.getValue());
                }
            }

            Queue<CompletableFuture<Void>> acknowledgments = new ConcurrentLinkedQueue<>();
            List<CompletableFuture<Void>> workers = new ArrayList<>();
            for (int offset = 0; offset < 2; offset++) {
                final int batchIndex = offset;
                workers.add(CompletableFuture.runAsync(() -> {
                    for (int i = batchIndex; i < messageCount; i += 4) {
                        acknowledgments.add(consumer.acknowledgeAsync(messages.get(i)));
                    }
                }, actors));
            }
            CompletableFuture.allOf(workers.toArray(new CompletableFuture[0])).get(20, TimeUnit.SECONDS);
            CompletableFuture.allOf(acknowledgments.toArray(new CompletableFuture[0])).get(20, TimeUnit.SECONDS);
            assertThat(acknowledgments).hasSize(messageCount / 2);
            Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(admin.topics().getStats(topic).getSubscriptions().get("sub").getUnackedMessages())
                            .isEqualTo(messageCount / 2));

            // Validate the broker's ACK state: only indices 2 and 3 of each batch may be redelivered.
            consumer.redeliverUnacknowledgedMessages();
            Set<String> redelivered = new HashSet<>();
            List<CompletableFuture<Void>> finalAcks = new ArrayList<>();
            for (int i = 0; i < messageCount / 2; i++) {
                Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
                assertThat(redelivered.add(message.getValue())).as("each unacknowledged index is delivered once")
                        .isTrue();
                assertThat(((MessageIdAdv) message.getMessageId()).getBatchIndex()).isIn(2, 3);
                finalAcks.add(consumer.acknowledgeAsync(message));
            }
            assertThat(redelivered).isEqualTo(expectedRedeliveries);
            CompletableFuture.allOf(finalAcks.toArray(new CompletableFuture[0])).get(20, TimeUnit.SECONDS);
            Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(admin.topics().getStats(topic).getSubscriptions().get("sub").getMsgBacklog()).isZero());
        } finally {
            actors.shutdownNow();
            assertThat(actors.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }
}
