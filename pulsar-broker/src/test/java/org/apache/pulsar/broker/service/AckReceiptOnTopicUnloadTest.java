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
package org.apache.pulsar.broker.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandAckResponse;
import org.apache.pulsar.common.api.proto.ServerError;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class AckReceiptOnTopicUnloadTest extends SharedPulsarBaseTest {

    @DataProvider
    public Object[][] delayedAckScenarios() {
        return new Object[][] {{false, false}, {true, false}, {false, true}, {true, true}};
    }

    @Test(dataProvider = "delayedAckScenarios", timeOut = 60000)
    public void testDelayedAckReceipt(boolean cumulative, boolean unload) throws Exception {
        String topic = newTopicName();
        String controlTopic = newTopicName();
        try (PulsarClient client = PulsarClient.builder()
                     .serviceUrl(getBrokerServiceUrl())
                     .connectionsPerBroker(1)
                     .operationTimeout(60, TimeUnit.SECONDS)
                     .build();
             Consumer<String> consumer = client.newConsumer(Schema.STRING)
                     .topic(topic)
                     .subscriptionName("sub")
                     .isAckReceiptEnabled(true)
                     .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                     .subscribe();
             Producer<String> controlProducer = client.newProducer(Schema.STRING)
                     .topic(controlTopic)
                     .enableBatching(false)
                     .create()) {
            try (Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(topic).enableBatching(false).create()) {
                producer.send("message");
            }
            Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
            assertThat(message).isNotNull();
            assertThat(message.getValue()).isEqualTo("message");
            var brokerConsumer = getTopicReference(topic).orElseThrow().getSubscription("sub").getConsumers().get(0);
            ServerCnx cnx = (ServerCnx) brokerConsumer.cnx();
            assertThat(getTopicReference(controlTopic).orElseThrow().getProducers().values().iterator().next().getCnx())
                    .isSameAs(cnx);

            try (DelayedAck delayed = new DelayedAck(cnx.ctx().channel(), brokerConsumer.consumerId())) {
                CompletableFuture<Void> ack = cumulative
                        ? consumer.acknowledgeCumulativeAsync(message)
                        : consumer.acknowledgeAsync(message);
                CommandAck command = delayed.received.get(10, TimeUnit.SECONDS);
                assertThat(command.hasRequestId()).isTrue();
                assertThat(ack).isNotDone();
                if (unload) {
                    admin.topics().unload(topic);
                    delayed.consumerClosed.get(10, TimeUnit.SECONDS);
                    cnx.ctx().executor().submit(() ->
                            assertThat(cnx.getConsumers().containsKey(brokerConsumer.consumerId())).isFalse())
                            .get(10, TimeUnit.SECONDS);
                    assertThat(getTopicReference(topic)).isEmpty();
                    assertThat(ack).isNotDone();
                }

                delayed.releaseAck();
                CommandAckResponse response = delayed.receipt.get(10, TimeUnit.SECONDS);
                assertThat(response.getRequestId()).isEqualTo(command.getRequestId());
                assertThat(response.getConsumerId()).isEqualTo(command.getConsumerId());
                assertThat(response.hasError()).isEqualTo(unload);
                if (unload) {
                    assertThat(response.getError()).isEqualTo(ServerError.ConsumerNotFound);
                    assertThatThrownBy(() -> ack.get(10, TimeUnit.SECONDS))
                            .isInstanceOf(ExecutionException.class)
                            .hasCauseExactlyInstanceOf(PulsarClientException.class)
                            .hasMessageContaining(response.getMessage());
                } else {
                    ack.get(10, TimeUnit.SECONDS);
                }

                // Keep the close notification delayed until a successful publish proves that the same
                // connection remains usable. Neither a disconnect nor a reconnect can satisfy the receipt check.
                controlProducer.sendAsync("still connected").get(10, TimeUnit.SECONDS);
                assertThat(cnx.ctx().channel().isActive()).isTrue();
                assertThat(getTopicReference(controlTopic).orElseThrow()
                        .getProducers().values().iterator().next().getCnx()).isSameAs(cnx);
            }
        }
    }

    /** Delays original protocol frames on the event loop without changing broker or client state. */
    private static final class DelayedAck extends ChannelDuplexHandler implements AutoCloseable {
        private final Channel channel;
        private final long consumerId;
        private final CompletableFuture<CommandAck> received = new CompletableFuture<>();
        private final CompletableFuture<Void> consumerClosed = new CompletableFuture<>();
        private final CompletableFuture<CommandAckResponse> receipt = new CompletableFuture<>();
        private ChannelHandlerContext ctx;
        private ByteBuf ackFrame;
        private ByteBuf closeFrame;
        private ChannelPromise closePromise;

        private DelayedAck(Channel channel, long consumerId) throws Exception {
            this.channel = channel;
            this.consumerId = consumerId;
            channel.eventLoop().submit(() -> channel.pipeline().addBefore("handler", "delayedAck", this))
                    .get(10, TimeUnit.SECONDS);
        }

        @Override
        public void handlerAdded(ChannelHandlerContext context) {
            ctx = context;
        }

        @Override
        public void channelRead(ChannelHandlerContext context, Object message) {
            if (message instanceof ByteBuf frame && !received.isDone()) {
                BaseCommand command = decode(frame, false);
                if (command.getType() == BaseCommand.Type.ACK && command.getAck().getConsumerId() == consumerId) {
                    ackFrame = frame;
                    received.complete(new CommandAck().copyFrom(command.getAck()));
                    return;
                }
            }
            context.fireChannelRead(message);
        }

        @Override
        public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) {
            if (message instanceof ByteBuf frame) {
                BaseCommand command = decode(frame, true);
                if (command.getType() == BaseCommand.Type.CLOSE_CONSUMER
                        && command.getCloseConsumer().getConsumerId() == consumerId) {
                    closeFrame = frame;
                    closePromise = promise;
                    consumerClosed.complete(null);
                    return;
                }
                if (command.getType() == BaseCommand.Type.ACK_RESPONSE
                        && command.getAckResponse().getConsumerId() == consumerId) {
                    receipt.complete(new CommandAckResponse().copyFrom(command.getAckResponse()));
                }
            }
            context.write(message, promise);
        }

        private static BaseCommand decode(ByteBuf frame, boolean outbound) {
            ByteBuf copy = frame.duplicate();
            if (outbound) {
                copy.skipBytes(Integer.BYTES);
            }
            int commandSize = copy.readInt();
            BaseCommand command = new BaseCommand();
            command.parseFrom(copy, commandSize);
            return command;
        }

        private void releaseAck() throws Exception {
            channel.eventLoop().submit(() -> {
                ByteBuf frame = ackFrame;
                ackFrame = null;
                ctx.fireChannelRead(frame);
                ctx.fireChannelReadComplete();
            }).get(10, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws Exception {
            channel.eventLoop().submit(() -> {
                if (ackFrame != null) {
                    ackFrame.release();
                    ackFrame = null;
                }
                if (closeFrame != null) {
                    ctx.writeAndFlush(closeFrame, closePromise);
                    closeFrame = null;
                }
                ctx.pipeline().remove(this);
            }).get(10, TimeUnit.SECONDS);
        }
    }
}
