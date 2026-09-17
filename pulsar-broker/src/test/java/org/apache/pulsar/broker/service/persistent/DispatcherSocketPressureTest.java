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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Verifies dispatch recovery after actual client sockets stop reading with receive permits outstanding. */
@Test(groups = "broker-api")
public class DispatcherSocketPressureTest extends SharedPulsarBaseTest {
    @DataProvider(name = "subscriptionTypes")
    public Object[][] subscriptionTypes() {
        return new Object[][] {{SubscriptionType.Shared}, {SubscriptionType.Key_Shared}};
    }

    @Test(dataProvider = "subscriptionTypes", timeOut = 120000)
    public void testDispatchResumesAfterSocketBecomesWritable(SubscriptionType subscriptionType) throws Exception {
        String topicName = newTopicName();
        // Keep enough messages and unused receive permits to fill the kernel socket buffers while reads
        // are paused. Leave TCP buffer sizing unchanged so recovery does not depend on window resizing.
        int messageCount = 8192;
        try (PulsarClient consumerClient = newPulsarClient();
             Consumer<byte[]> consumer = consumerClient.newConsumer(Schema.BYTES).topic(topicName)
                     .subscriptionName("sub").subscriptionType(subscriptionType).receiverQueueSize(32768).subscribe();
             Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES).topic(topicName)
                     .enableBatching(false).maxPendingMessages(256).blockIfQueueFull(true).create()) {
            PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().orElseThrow();
            PersistentDispatcherMultipleConsumers dispatcher =
                    (PersistentDispatcherMultipleConsumers) topic.getSubscription("sub").getDispatcher();
            var brokerConsumer = dispatcher.getConsumers().get(0);
            Channel brokerChannel = ((ServerCnx) brokerConsumer.cnx()).ctx().channel();
            Channel clientChannel = ((ConsumerImpl<byte[]>) consumer).getClientCnx().ctx().channel();
            Awaitility.await().atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> assertThat(brokerConsumer.getAvailablePermits()).isEqualTo(32768));
            clientChannel.eventLoop().submit(() -> clientChannel.config().setAutoRead(false)).sync();
            try {
                List<CompletableFuture<MessageId>> sends = new ArrayList<>();
                for (int i = 0; i < messageCount; i++) {
                    byte[] payload = new byte[8192];
                    ByteBuffer.wrap(payload).putInt(i);
                    sends.add(producer.newMessage().key("same-key").value(payload).sendAsync());
                }
                CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).get(15, TimeUnit.SECONDS);
                Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                        assertThat(brokerChannel.isWritable())
                                .as("writable with %s permits, pending read %s, backlog %s",
                                        brokerConsumer.getAvailablePermits(), dispatcher.havePendingRead,
                                        dispatcher.cursor.getNumberOfEntriesInBacklog(false))
                                .isFalse());
                assertThat(brokerConsumer.getAvailablePermits()).isPositive();
                clientChannel.eventLoop().submit(() -> clientChannel.config().setAutoRead(true)).sync();
                List<Message<byte[]>> received = new ArrayList<>();
                for (int i = 0; i < messageCount; i++) {
                    Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);
                    assertThat(message).as("message %s", i).isNotNull();
                    assertThat(ByteBuffer.wrap(message.getData()).getInt()).isEqualTo(i);
                    received.add(message);
                }
                for (Message<byte[]> message : received) {
                    consumer.acknowledge(message);
                }
            } finally {
                clientChannel.eventLoop().submit(() -> clientChannel.config().setAutoRead(true)).sync();
            }
        }
    }
}
