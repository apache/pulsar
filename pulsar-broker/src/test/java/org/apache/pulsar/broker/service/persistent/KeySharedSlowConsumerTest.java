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
package org.apache.pulsar.broker.service.persistent;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import io.netty.channel.Channel;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/** Verifies that a paused Key_Shared socket does not prevent other consumers from making progress. */
@Test(groups = "broker-api")
public class KeySharedSlowConsumerTest extends SharedPulsarBaseTest {
    @Test(timeOut = 120000)
    public void testSlowKeySharedSocketDoesNotBlockFastConsumer() throws Exception {
        String topicName = newTopicName();
        int fastMessageCount = 1024;
        // Exceed the socket receive window without reaching the dispatcher's replay look-ahead limit.
        int slowMessagesPerFastMessage = 4;
        int slowMessageCount = fastMessageCount * slowMessagesPerFastMessage;
        try (PulsarClient slowClient = newPulsarClient();
             PulsarClient fastClient = newPulsarClient();
             Consumer<byte[]> slow = slowClient.newConsumer(Schema.BYTES).topic(topicName)
                     .subscriptionName("sub").consumerName("slow").subscriptionType(SubscriptionType.Key_Shared)
                     .receiverQueueSize(32768).subscribe();
             Consumer<byte[]> fast = fastClient.newConsumer(Schema.BYTES).topic(topicName)
                     .subscriptionName("sub").consumerName("fast").subscriptionType(SubscriptionType.Key_Shared)
                     .receiverQueueSize(32768).subscribe();
             Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES).topic(topicName)
                     .enableBatching(false).maxPendingMessages(256).blockIfQueueFull(true).create()) {
            PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().orElseThrow();
            PersistentStickyKeyDispatcherMultipleConsumers dispatcher =
                    (PersistentStickyKeyDispatcherMultipleConsumers) topic.getSubscription("sub").getDispatcher();
            var slowBrokerConsumer = dispatcher.getConsumers().stream()
                    .filter(c -> c.consumerName().equals("slow")).findFirst().orElseThrow();
            var fastBrokerConsumer = dispatcher.getConsumers().stream()
                    .filter(c -> c.consumerName().equals("fast")).findFirst().orElseThrow();
            Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(slowBrokerConsumer.getAvailablePermits()).isEqualTo(32768);
                assertThat(fastBrokerConsumer.getAvailablePermits()).isEqualTo(32768);
            });
            String slowKey = keyForConsumer(dispatcher, "slow");
            String fastKey = keyForConsumer(dispatcher, "fast");
            Channel brokerChannel = ((ServerCnx) slowBrokerConsumer.cnx()).ctx().channel();
            Channel clientChannel = ((ConsumerImpl<byte[]>) slow).getClientCnx().ctx().channel();
            clientChannel.eventLoop().submit(() -> clientChannel.config().setAutoRead(false)).sync();
            try {
                List<CompletableFuture<MessageId>> sends = new ArrayList<>();
                for (int i = 0; i < slowMessageCount; i++) {
                    byte[] payload = new byte[16384];
                    ByteBuffer.wrap(payload).putInt(i);
                    sends.add(producer.newMessage().key(slowKey).value(payload).sendAsync());
                    // Spread healthy messages across the slow stream so some remain when its socket fills.
                    // Keep healthy batches below the watermark: writable events must not hide the barrier.
                    if (i % slowMessagesPerFastMessage == 0) {
                        byte[] fastPayload = new byte[128];
                        ByteBuffer.wrap(fastPayload).putInt(i / slowMessagesPerFastMessage);
                        sends.add(producer.newMessage().key(fastKey).value(fastPayload).sendAsync());
                    }
                }
                CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).get(15, TimeUnit.SECONDS);
                Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> !brokerChannel.isWritable());
                List<Message<byte[]>> fastMessages = new ArrayList<>();
                // No FLOW or ACK from either consumer: a slow socket must not stop the other hash range.
                for (int i = 0; i < fastMessageCount; i++) {
                    Message<byte[]> message = fast.receive(3, TimeUnit.SECONDS);
                    assertThat(message).as("fast message %s while the slow socket is paused", i).isNotNull();
                    assertThat(ByteBuffer.wrap(message.getData()).getInt()).isEqualTo(i);
                    fastMessages.add(message);
                }
                clientChannel.eventLoop().submit(() -> clientChannel.config().setAutoRead(true)).sync();
                for (int i = 0; i < slowMessageCount; i++) {
                    Message<byte[]> message = slow.receive(5, TimeUnit.SECONDS);
                    assertThat(message).as("slow message %s after resume", i).isNotNull();
                    assertThat(ByteBuffer.wrap(message.getData()).getInt()).isEqualTo(i);
                    slow.acknowledge(message);
                }
                for (Message<byte[]> message : fastMessages) {
                    fast.acknowledge(message);
                }
            } finally {
                clientChannel.eventLoop().submit(() -> clientChannel.config().setAutoRead(true)).sync();
            }
        }
    }

    private static String keyForConsumer(PersistentStickyKeyDispatcherMultipleConsumers dispatcher, String name) {
        for (int i = 0; i < 10000; i++) {
            String key = "key-" + i;
            if (dispatcher.getSelector().select(key.getBytes(UTF_8)).consumerName().equals(name)) {
                return key;
            }
        }
        throw new AssertionError("No key found for " + name);
    }

}
