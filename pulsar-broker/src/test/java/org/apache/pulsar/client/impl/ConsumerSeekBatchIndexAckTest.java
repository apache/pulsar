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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.InjectedClientCnxClientBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.api.proto.CommandSuccess;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class ConsumerSeekBatchIndexAckTest extends SharedPulsarBaseTest {

    @DataProvider
    public Object[][] partialBatchAck() {
        return new Object[][] {{false}, {true}};
    }

    @Test(timeOut = 60000, dataProvider = "partialBatchAck")
    public void testSeekReplaysBatchAfterSuccessWhileDisconnected(boolean partialBatchAck) throws Exception {
        String topic = newTopicName();
        AtomicReference<ConsumerImpl<String>> consumerReference = new AtomicReference<>();
        AtomicBoolean captureSeekResponse = new AtomicBoolean();
        CompletableFuture<Boolean> seekResponseHandled = new CompletableFuture<>();
        ClientBuilderImpl builder = (ClientBuilderImpl) PulsarClient.builder().serviceUrl(getBrokerServiceUrl());
        @Cleanup
        PulsarClient client = InjectedClientCnxClientBuilder.create(builder, (configuration, eventLoopGroup) ->
                new ClientCnx(InstrumentProvider.NOOP, configuration, eventLoopGroup) {
                    @Override
                    protected void handleSuccess(CommandSuccess success) {
                        if (captureSeekResponse.compareAndSet(true, false)) {
                            boolean disconnected = consumerReference.get().getClientCnx() == null;
                            super.handleSuccess(success);
                            seekResponseHandled.complete(disconnected);
                        } else {
                            super.handleSuccess(success);
                        }
                    }
                });
        @Cleanup
        ConsumerImpl<String> consumer = (ConsumerImpl<String>) client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("seek-batch-index-ack")
                .enableBatchIndexAcknowledgment(true)
                .acknowledgmentGroupTime(1, TimeUnit.HOURS)
                .subscribe();
        consumerReference.set(consumer);
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(true)
                .batchingMaxMessages(2)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .create();
        CompletableFuture<MessageId> firstSend = producer.sendAsync("0");
        CompletableFuture<MessageId> secondSend = producer.sendAsync("1");
        producer.flush();
        BatchMessageIdImpl firstId = (BatchMessageIdImpl) firstSend.get(5, TimeUnit.SECONDS);
        BatchMessageIdImpl secondId = (BatchMessageIdImpl) secondSend.get(5, TimeUnit.SECONDS);
        assertThat(firstId.getBatchIndex()).isZero();
        assertThat(secondId.getBatchIndex()).isEqualTo(1);
        assertThat(MessageIdAdvUtils.discardBatch(firstId)).isEqualTo(MessageIdAdvUtils.discardBatch(secondId));

        Message<String> first = consumer.receive(5, TimeUnit.SECONDS);
        Message<String> second = consumer.receive(5, TimeUnit.SECONDS);
        assertThat(first).isNotNull();
        assertThat(second).isNotNull();
        assertThat(first.getValue()).isEqualTo("0");
        assertThat(second.getValue()).isEqualTo("1");
        if (partialBatchAck) {
            consumer.acknowledge(first);
        }
        consumer.pause();

        CompletableFuture<Void> allowReconnect = new CompletableFuture<>();
        CompletableFuture<Void> reconnected = new CompletableFuture<>();
        ConnectionHandler handler = consumer.getConnectionHandler();
        ConnectionHandler.Connection originalConnection = handler.connection;
        // Delay the normal reconnect callback without blocking the client's event loop. The broker's seek
        // response must be processed while the consumer is disconnected, before reconnecting and flushing acks.
        handler.connection = cnx -> allowReconnect.thenComposeAsync(
                __ -> originalConnection.connectionOpened(cnx), cnx.ctx().executor()).whenComplete((__, error) -> {
                    if (error == null) {
                        reconnected.complete(null);
                    } else {
                        reconnected.completeExceptionally(error);
                    }
                });
        try {
            captureSeekResponse.set(true);
            CompletableFuture<Void> seek = consumer.seekAsync(MessageId.earliest);
            assertThat(seekResponseHandled.get(5, TimeUnit.SECONDS))
                    .as("seek success is handled before the consumer reconnects").isTrue();
            allowReconnect.complete(null);
            reconnected.get(5, TimeUnit.SECONDS);
            seek.get(5, TimeUnit.SECONDS);

            // Reconnection flushes pending acknowledgments. Wait for a subsequent broker response before
            // allowing replay, so a stale batch bitmap cannot race with the first replayed batch.
            consumer.getLastMessageIdsAsync().get(5, TimeUnit.SECONDS);
            consumer.resume();
            Message<String> replayedFirst = consumer.receive(5, TimeUnit.SECONDS);
            assertThat(replayedFirst).as("first replayed message").isNotNull();
            assertThat(replayedFirst.getValue()).isEqualTo("0");
            Message<String> replayedSecond = consumer.receive(5, TimeUnit.SECONDS);
            assertThat(replayedSecond).as("second replayed message").isNotNull();
            assertThat(replayedSecond.getValue()).isEqualTo("1");
        } finally {
            allowReconnect.complete(null);
            handler.connection = originalConnection;
        }
    }
}
