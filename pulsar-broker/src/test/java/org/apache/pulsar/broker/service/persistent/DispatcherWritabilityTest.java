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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class DispatcherWritabilityTest extends SharedPulsarBaseTest {
    @DataProvider(name = "subscriptionTypes")
    public Object[][] subscriptionTypes() {
        return new Object[][] {{SubscriptionType.Shared}, {SubscriptionType.Key_Shared}};
    }

    @Test(dataProvider = "subscriptionTypes", timeOut = 30000)
    public void testWritableEventResumesWithoutFlowOrPublish(SubscriptionType subscriptionType) throws Exception {
        String topicName = newTopicName();
        // Keep the producer on a different connection: server-side request throttling can pause auto-read
        // on the consumer connection while its outbound buffer is unwritable.
        try (PulsarClient consumerClient = newPulsarClient();
             Consumer<Integer> consumer = consumerClient.newConsumer(Schema.INT32).topic(topicName)
                     .subscriptionName("sub").subscriptionType(subscriptionType).receiverQueueSize(1000).subscribe();
             Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32).topic(topicName)
                     .enableBatching(false).create()) {
            PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().orElseThrow();
            PersistentDispatcherMultipleConsumers dispatcher =
                    (PersistentDispatcherMultipleConsumers) topic.getSubscription("sub").getDispatcher();
            var brokerConsumer = dispatcher.getConsumers().get(0);
            Channel channel = ((ServerCnx) brokerConsumer.cnx()).ctx().channel();
            Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(brokerConsumer.getAvailablePermits()).isEqualTo(1000);
                assertThat(dispatcher.havePendingRead).isTrue();
            });
            channel.eventLoop().submit(() -> channel.unsafe().outboundBuffer()
                    .setUserDefinedWritability(1, false)).sync();
            try {
                for (int i = 0; i < 32; i++) {
                    producer.newMessage().key("same-key").value(i).send();
                }
                Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                    synchronized (dispatcher) {
                        assertThat(dispatcher.havePendingRead).isFalse();
                        assertThat(dispatcher.cursor.getNumberOfEntriesInBacklog(false)).isEqualTo(32);
                    }
                });
                assertThat(consumer.receive(100, TimeUnit.MILLISECONDS)).isNull();
                assertThat(brokerConsumer.getAvailablePermits()).isEqualTo(1000);

                // No more publishing, no acknowledgments and too few messages to replenish receive permits:
                // only the real Netty writable event can resume this transport-blocked dispatcher.
                CompletableFuture<Void> notificationDelivered = new CompletableFuture<>();
                channel.eventLoop().submit(() -> channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
                        if (ctx.channel().isWritable()) {
                            notificationDelivered.complete(null);
                        }
                        ctx.fireChannelWritabilityChanged();
                    }
                })).sync();
                synchronized (topic.getSubscription("sub")) {
                    synchronized (dispatcher) {
                        channel.eventLoop().submit(() -> channel.unsafe().outboundBuffer()
                                .setUserDefinedWritability(1, true)).sync();
                        // Observe delivery after ServerCnx handles the event, not after queued dispatch tasks:
                        // the notification itself must not wait for either monitor held by this thread.
                        notificationDelivered.get(5, TimeUnit.SECONDS);
                    }
                }
                List<Message<Integer>> received = new ArrayList<>();
                for (int i = 0; i < 32; i++) {
                    Message<Integer> message = consumer.receive(5, TimeUnit.SECONDS);
                    assertThat(message).as("message %s after channel becomes writable", i).isNotNull();
                    assertThat(message.getValue()).isEqualTo(i);
                    received.add(message);
                }
                for (Message<Integer> message : received) {
                    consumer.acknowledge(message);
                }
            } finally {
                if (channel.isActive()) {
                    channel.eventLoop().submit(() -> channel.unsafe().outboundBuffer()
                            .setUserDefinedWritability(1, true)).sync();
                }
            }
        }
    }
}
