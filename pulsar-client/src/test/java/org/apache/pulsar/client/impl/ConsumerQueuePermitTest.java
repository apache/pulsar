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

import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ConsumerQueuePermitTest {
    private enum ResizeOrder {
        BEFORE_SUBSCRIBE, DURING_SUBSCRIBE_COMPLETION, AFTER_SUBSCRIBE
    }

    @DataProvider
    public Object[][] executionOrders() {
        return new Object[][] {{ResizeOrder.BEFORE_SUBSCRIBE}, {ResizeOrder.DURING_SUBSCRIBE_COMPLETION},
                {ResizeOrder.AFTER_SUBSCRIBE}};
    }

    @Test(dataProvider = "executionOrders", timeOut = 120000)
    public void testReconnectAndResizeGrantCapacityOnce(ResizeOrder order) throws Exception {
        try (TestConsumer fixture = new TestConsumer()) {
            Connection oldConnection = new Connection();
            CompletableFuture<Void> initialOpen = fixture.consumer.connectionOpened(oldConnection.cnx);
            oldConnection.subscribeReply.complete(null);
            initialOpen.get(5, TimeUnit.SECONDS);
            assertThat(oldConnection.sentPermits.get()).isEqualTo(1);

            // Fill the old queue through the real enqueue path to retain an autoscale hint across reconnect.
            MessageImpl<byte[]> oldMessage = new MessageImpl<>(fixture.consumer.getTopic(),
                    new MessageIdImpl(1, 1, -1), new MessageMetadata(), Unpooled.EMPTY_BUFFER,
                    oldConnection.cnx, Schema.BYTES);
            fixture.consumer.enqueueMessageAndCheckBatchReceive(oldMessage);
            assertThat(fixture.consumer.changeToConnecting()).isTrue();
            Connection newConnection = new Connection();
            CompletableFuture<Void> reconnect = fixture.consumer.connectionOpened(newConnection.cnx);
            assertThat(fixture.consumer.numMessagesInQueue()).isZero();

            if (order == ResizeOrder.AFTER_SUBSCRIBE) {
                newConnection.subscribeReply.complete(null);
                reconnect.get(5, TimeUnit.SECONDS);
                assertThat(newConnection.sentPermits.get()).isEqualTo(1);
            }
            if (order == ResizeOrder.DURING_SUBSCRIBE_COMPLETION) {
                // Resize while completing the user subscription future, before granting the initial queue budget.
                fixture.beforeSubscribeCompletion = () -> {
                    try {
                        assertThat(fixture.consumer.receive(0, TimeUnit.MILLISECONDS)).isNull();
                    } catch (Exception error) {
                        throw new AssertionError(error);
                    }
                };
            } else {
                // No message is consumed, so all new-connection permits must come from its capacity.
                assertThat(fixture.consumer.receive(0, TimeUnit.MILLISECONDS)).isNull();
            }
            if (order != ResizeOrder.AFTER_SUBSCRIBE) {
                newConnection.subscribeReply.complete(null);
                reconnect.get(5, TimeUnit.SECONDS);
            }

            assertThat(fixture.consumer.getCurrentReceiverQueueSize()).isEqualTo(2);
            assertThat(newConnection.sentPermits.get()).as("total FLOW permits for capacity 2").isEqualTo(2);
            assertThat(fixture.consumer.getAvailablePermits()).isZero();
            assertThat(oldConnection.sentPermits.get()).isEqualTo(1);
        }
    }

    @Test(timeOut = 120000)
    public void testParentInitializesQueuePermitsOnce() throws Exception {
        try (TestConsumer fixture = new TestConsumer(true)) {
            Connection connection = new Connection();
            CompletableFuture<Void> opened = fixture.consumer.connectionOpened(connection.cnx);
            connection.subscribeReply.complete(null);
            opened.get(5, TimeUnit.SECONDS);
            assertThat(connection.sentPermits.get()).as("child waits for parent initialization").isZero();

            fixture.consumer.setCurrentReceiverQueueSize(2);
            fixture.consumer.initializeReceiverQueuePermits();
            fixture.consumer.initializeReceiverQueuePermits();
            assertThat(connection.sentPermits.get()).isEqualTo(2);

            assertThat(fixture.consumer.changeToConnecting()).isTrue();
            Connection reconnected = new Connection();
            CompletableFuture<Void> reopen = fixture.consumer.connectionOpened(reconnected.cnx);
            reconnected.subscribeReply.complete(null);
            reopen.get(5, TimeUnit.SECONDS);
            fixture.consumer.initializeReceiverQueuePermits();
            assertThat(reconnected.sentPermits.get()).as("reconnect and parent share one initial grant").isEqualTo(2);
        }
    }

    @Test(timeOut = 120000)
    public void testMemoryTriggeredResizeDoesNotAcquireConsumerMonitor() throws Exception {
        try (TestConsumer fixture = new TestConsumer()) {
            fixture.consumer.setCurrentReceiverQueueSize(2);
            MemoryLimitController memory = new MemoryLimitController(1024 * 1024, 1,
                    fixture.consumer::reduceCurrentReceiverQueueSize);
            when(fixture.client.getMemoryLimitController()).thenReturn(memory);
            ByteBuf payload = Unpooled.wrappedBuffer(new byte[] {1});
            MessageImpl<byte[]> message;
            try {
                message = new MessageImpl<>(fixture.consumer.getTopic(), new MessageIdImpl(1, 1, -1),
                        new MessageMetadata(), payload, Optional.empty(), null, Schema.BYTES, 0, true,
                        DEFAULT_CONSUMER_EPOCH);
            } finally {
                payload.release();
            }
            ExecutorService enqueueExecutor = Executors.newSingleThreadExecutor();
            Future<?> enqueued = null;
            try {
                synchronized (fixture.consumer) {
                    enqueued = enqueueExecutor.submit(
                            () -> fixture.consumer.enqueueMessageAndCheckBatchReceive(message));
                    enqueued.get(5, TimeUnit.SECONDS);
                    assertThat(fixture.consumer.getCurrentReceiverQueueSize()).isEqualTo(1);
                }
            } finally {
                // Release the consumer monitor before awaiting a worker blocked by a regression.
                try {
                    if (enqueued != null) {
                        enqueued.get(5, TimeUnit.SECONDS);
                    }
                } finally {
                    enqueueExecutor.shutdownNow();
                    fixture.consumer.clearIncomingMessages();
                    assertThat(memory.currentUsage()).isZero();
                    assertThat(payload.refCnt()).isZero();
                }
            }
        }
    }

    private static final class TestConsumer implements AutoCloseable {
        private final ExecutorProvider executorProvider = new ExecutorProvider(1, "queue-permit-test");
        private final ExecutorService internalExecutor = Executors.newSingleThreadExecutor();
        private final ConsumerImpl<byte[]> consumer;
        private final PulsarClientImpl client;
        private Runnable beforeSubscribeCompletion = () -> { };

        TestConsumer() {
            this(false);
        }

        TestConsumer(boolean hasParentConsumer) {
            client = ClientTestFixtures.createPulsarClientMock(executorProvider, internalExecutor);
            client.getConfiguration().setStatsIntervalSeconds(0);
            when(client.instrumentProvider()).thenReturn(InstrumentProvider.NOOP);
            when(client.getMemoryLimitController()).thenReturn(new MemoryLimitController(1024 * 1024));
            when(client.getConnection(anyString(), anyInt())).thenReturn(new CompletableFuture<>());
            AtomicLong requestIds = new AtomicLong();
            when(client.newRequestId()).thenAnswer(invocation -> requestIds.incrementAndGet());
            ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
            conf.setSubscriptionName("queue-permit-test");
            conf.setSubscriptionMode(SubscriptionMode.NonDurable);
            conf.setReceiverQueueSize(2);
            conf.setAutoScaledReceiverQueueSizeEnabled(true);
            CompletableFuture<Consumer<byte[]>> subscribeFuture = new CompletableFuture<>() {
                @Override
                public boolean complete(Consumer<byte[]> value) {
                    beforeSubscribeCompletion.run();
                    return super.complete(value);
                }
            };
            consumer = ConsumerImpl.newConsumerImpl(client, "non-persistent://public/default/queue-permit-test",
                    conf, executorProvider, -1, hasParentConsumer, subscribeFuture,
                    null, Schema.BYTES, null, true);
        }

        @Override
        public void close() throws Exception {
            try {
                consumer.closeAsync().get(5, TimeUnit.SECONDS);
            } finally {
                internalExecutor.shutdownNow();
                executorProvider.shutdownNow();
            }
        }
    }

    private static final class Connection {
        private final ClientCnx cnx = mock(ClientCnx.class);
        private final CompletableFuture<ProducerResponse> subscribeReply = new CompletableFuture<>();
        private final AtomicInteger sentPermits = new AtomicInteger();

        Connection() {
            ChannelHandlerContext context = mock(ChannelHandlerContext.class);
            Channel channel = mock(Channel.class);
            when(cnx.ctx()).thenReturn(context);
            when(cnx.channel()).thenReturn(channel);
            when(cnx.getMaxMessageSize()).thenReturn(5 * 1024 * 1024);
            when(context.channel()).thenReturn(channel);
            when(context.voidPromise()).thenReturn(mock(ChannelPromise.class));
            doAnswer(invocation -> {
                BaseCommand command = decodeAndRelease(invocation.getArgument(0));
                assertThat(command.getType()).isEqualTo(BaseCommand.Type.FLOW);
                sentPermits.addAndGet(command.getFlow().getMessagePermits());
                return mock(ChannelFuture.class);
            }).when(context).writeAndFlush(any(ByteBuf.class), any(ChannelPromise.class));
            when(cnx.sendRequestWithId(any(ByteBuf.class), anyLong())).thenAnswer(invocation -> {
                BaseCommand command = decodeAndRelease(invocation.getArgument(0));
                if (command.getType() == BaseCommand.Type.SUBSCRIBE) {
                    return subscribeReply;
                }
                assertThat(command.getType()).isEqualTo(BaseCommand.Type.CLOSE_CONSUMER);
                return CompletableFuture.<ProducerResponse>completedFuture(null);
            });
        }

        private static BaseCommand decodeAndRelease(ByteBuf frame) {
            try {
                frame.skipBytes(Integer.BYTES);
                int commandSize = (int) frame.readUnsignedInt();
                BaseCommand command = new BaseCommand();
                command.parseFrom(frame, commandSize);
                return command;
            } finally {
                frame.release();
            }
        }
    }
}
