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
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
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

    @DataProvider
    public Object[][] parentConsumerModes() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "parentConsumerModes", timeOut = 120000)
    public void testSubscribeReplySendsCurrentEpochBeforeResizedQueuePermits(boolean hasParentConsumer)
            throws Exception {
        try (TestConsumer fixture = new TestConsumer(hasParentConsumer)) {
            Connection connection = new Connection();
            CompletableFuture<Void> opened = fixture.consumer.connectionOpened(connection.cnx);
            assertThat(connection.subscribeEpochs).hasSize(1);
            long subscribeEpoch = connection.subscribeEpochs.get(0);

            assertThat(fixture.consumer.getCurrentReceiverQueueSize()).isEqualTo(1);
            fixture.consumer.setCurrentReceiverQueueSize(2);
            fixture.consumer.redeliverUnacknowledgedMessages();
            fixture.consumer.redeliverUnacknowledgedMessages();
            assertThat(connection.sentCommands).as("SUBSCRIBE is still pending").isEmpty();

            connection.subscribeReply.complete(null);
            opened.get(5, TimeUnit.SECONDS);
            if (hasParentConsumer) {
                assertThat(connection.sentCommands).extracting(BaseCommand::getType)
                        .containsExactly(BaseCommand.Type.REDELIVER_UNACKNOWLEDGED_MESSAGES);
                assertThat(connection.sentPermits.get()).as("child waits for parent initialization").isZero();
            } else {
                assertThat(connection.sentPermits.get()).as("standalone consumer initializes on subscribe")
                        .isEqualTo(2);
            }

            fixture.consumer.initializeReceiverQueuePermits();
            fixture.consumer.initializeReceiverQueuePermits();
            assertThat(connection.sentCommands).extracting(BaseCommand::getType)
                    .containsExactly(BaseCommand.Type.REDELIVER_UNACKNOWLEDGED_MESSAGES, BaseCommand.Type.FLOW);
            assertThat(connection.sentCommands.get(0).getRedeliverUnacknowledgedMessages().getConsumerEpoch())
                    .as("broker receives the latest epoch before any FLOW").isEqualTo(subscribeEpoch + 2);
            assertThat(fixture.consumer.getCurrentReceiverQueueSize()).isEqualTo(2);
            assertThat(connection.sentPermits.get()).as("resized capacity is granted once").isEqualTo(2);
            assertThat(fixture.consumer.getAvailablePermits()).isZero();
        }
    }

    @Test(timeOut = 120000)
    public void testStaleSubscribeReplyOnSameConnectionDoesNotInitializeNewAttempt() throws Exception {
        try (TestConsumer fixture = new TestConsumer()) {
            Connection connection = new Connection();
            AtomicInteger subscriptionCompletions = new AtomicInteger();
            fixture.beforeSubscribeCompletion = subscriptionCompletions::incrementAndGet;
            CompletableFuture<ProducerResponse> firstReply = connection.subscribeReply;
            CompletableFuture<Void> firstOpen = fixture.consumer.connectionOpened(connection.cnx);
            long firstSubscribeEpoch = connection.subscribeEpochs.get(0);
            fixture.consumer.redeliverUnacknowledgedMessages();

            assertThat(fixture.consumer.changeToConnecting()).isTrue();
            CompletableFuture<ProducerResponse> secondReply = connection.newSubscription();
            CompletableFuture<Void> secondOpen = fixture.consumer.connectionOpened(connection.cnx);
            fixture.consumer.redeliverUnacknowledgedMessages();
            assertThat(connection.subscribeEpochs).containsExactly(firstSubscribeEpoch, firstSubscribeEpoch + 1);
            assertThat(firstOpen).isNotDone();
            assertThat(secondOpen).isNotDone();

            // A previous SUBSCRIBE can complete after a replacement is published on the same connection.
            firstReply.complete(null);
            assertThat(firstOpen).as("superseded success does not complete the attempt").isNotDone();
            assertThat(fixture.consumer.getState()).isEqualTo(HandlerState.State.Connecting);
            assertThat(secondOpen).isNotDone();
            assertThat(subscriptionCompletions.get()).isZero();
            assertThat(connection.sentCommands).as("stale reply sends neither epoch update nor FLOW").isEmpty();
            assertThat(connection.sentPermits.get()).isZero();
            assertThat(fixture.consumer.getAvailablePermits()).isZero();

            fixture.consumer.setCurrentReceiverQueueSize(2);
            secondReply.complete(null);
            secondOpen.get(5, TimeUnit.SECONDS);
            assertThat(fixture.consumer.getState()).isEqualTo(HandlerState.State.Ready);
            assertThat(subscriptionCompletions.get()).isEqualTo(1);
            assertThat(fixture.consumer.getCurrentReceiverQueueSize()).isEqualTo(2);
            assertThat(connection.sentCommands).extracting(BaseCommand::getType)
                    .containsExactly(BaseCommand.Type.REDELIVER_UNACKNOWLEDGED_MESSAGES, BaseCommand.Type.FLOW);
            assertThat(connection.sentCommands.get(0).getRedeliverUnacknowledgedMessages().getConsumerEpoch())
                    .isEqualTo(firstSubscribeEpoch + 2);
            assertThat(connection.sentPermits.get()).isEqualTo(2);
            assertThat(fixture.consumer.getAvailablePermits()).isZero();
            fixture.consumer.initializeReceiverQueuePermits();
            assertThat(connection.sentPermits.get()).isEqualTo(2);
        }
    }

    @Test(timeOut = 120000)
    public void testStaleSubscribeTimeoutOnSameConnectionRetainsResizedQueuePermits() throws Exception {
        try (TestConsumer fixture = new TestConsumer(false, false, 4)) {
            Connection connection = new Connection();
            CompletableFuture<ProducerResponse> firstReply = connection.subscribeReply;
            CompletableFuture<Void> firstOpen = fixture.consumer.connectionOpened(connection.cnx);

            fixture.consumer.connectionClosed(connection.cnx, Optional.of(0L), Optional.empty());
            assertThat(fixture.consumer.getClientCnx()).isNull();
            fixture.consumer.setCurrentReceiverQueueSize(2);
            assertThat(connection.sentPermits.get()).as("resize while disconnected does not send FLOW").isZero();
            CompletableFuture<ProducerResponse> secondReply = connection.newSubscription();
            CompletableFuture<Void> secondOpen = fixture.consumer.connectionOpened(connection.cnx);
            secondReply.complete(null);
            secondOpen.get(5, TimeUnit.SECONDS);
            assertThat(fixture.consumer.isConnected()).isTrue();
            assertThat(fixture.consumer.getClientCnx()).isSameAs(connection.cnx);
            assertThat(connection.sentPermits.get()).isEqualTo(2);
            assertThat(fixture.consumer.getAvailablePermits()).isZero();
            long currentCnxEpoch = fixture.consumer.getConnectionHandler().getEpoch();
            clearInvocations(fixture.client.timer());

            firstReply.completeExceptionally(new PulsarClientException.TimeoutException("stale subscribe timed out"));

            assertThat(firstOpen).as("superseded failure must not trigger reconnection").isNotDone();
            assertThat(fixture.consumer.getState()).isEqualTo(HandlerState.State.Ready);
            assertThat(fixture.consumer.isConnected()).isTrue();
            assertThat(fixture.consumer.getClientCnx()).isSameAs(connection.cnx);
            assertThat(fixture.consumer.getConnectionHandler().getEpoch()).isEqualTo(currentCnxEpoch);
            assertThat(connection.sentPermits.get()).as("stale failure preserves the new budget").isEqualTo(2);
            assertThat(fixture.consumer.getAvailablePermits()).isZero();
            verify(connection.cnx, never()).removeConsumer(anyLong());
            verify(connection.cnx.channel(), never()).close();
            verify(connection.cnx, times(2)).sendRequestWithId(any(ByteBuf.class), anyLong());
            verify(fixture.client.timer(), never()).newTimeout(any(), anyLong(), any(TimeUnit.class));

            fixture.consumer.setCurrentReceiverQueueSize(4);
            assertThat(fixture.consumer.getCurrentReceiverQueueSize()).isEqualTo(4);
            assertThat(connection.sentCommands).extracting(BaseCommand::getType)
                    .containsExactly(BaseCommand.Type.FLOW, BaseCommand.Type.FLOW);
            assertThat(connection.sentPermits.get()).as("later growth grants only its delta").isEqualTo(4);
            assertThat(fixture.consumer.getAvailablePermits()).isZero();
        }
    }

    @Test(timeOut = 120000)
    public void testStaleInitializationDoesNotBypassParentOnSameConnection() throws Exception {
        try (TestConsumer fixture = new TestConsumer(true)) {
            fixture.consumer.setCurrentReceiverQueueSize(2);
            Connection connection = new Connection();
            CompletableFuture<ProducerResponse> firstReply = connection.subscribeReply;
            fixture.beforeSubscribeCompletion = () -> {
                fixture.beforeSubscribeCompletion = () -> { };
                assertThat(fixture.consumer.changeToConnecting()).isTrue();
                CompletableFuture<ProducerResponse> replacementReply = connection.newSubscription();
                CompletableFuture<Void> replacementOpen = fixture.consumer.connectionOpened(connection.cnx);
                replacementReply.complete(null);
                assertThat(replacementOpen).isDone();
                assertThat(replacementOpen.isCompletedExceptionally()).isFalse();
            };

            CompletableFuture<Void> firstOpen = fixture.consumer.connectionOpened(connection.cnx);
            firstReply.complete(null);
            firstOpen.get(5, TimeUnit.SECONDS);

            // The replacement completed the user future first and must still wait for its parent.
            assertThat(fixture.consumer.getState()).isEqualTo(HandlerState.State.Ready);
            assertThat(connection.sentPermits.get()).isZero();
            assertThat(fixture.consumer.getAvailablePermits()).isZero();
            fixture.consumer.initializeReceiverQueuePermits();
            fixture.consumer.initializeReceiverQueuePermits();
            assertThat(fixture.consumer.getCurrentReceiverQueueSize()).isEqualTo(2);
            assertThat(connection.sentPermits.get()).isEqualTo(2);
            assertThat(fixture.consumer.getAvailablePermits()).isZero();
        }
    }

    @DataProvider
    public Object[][] pausedModes() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "pausedModes", timeOut = 120000)
    public void testFailedUnsubscribeFromSubscribeCallbackRetainsInitialGrant(boolean startPaused) throws Exception {
        try (TestConsumer fixture = new TestConsumer(false, startPaused)) {
            AtomicReference<CompletableFuture<Void>> unsubscribe = new AtomicReference<>();
            // A user callback runs before subscription success initializes the queue's permits.
            CompletableFuture<Void> callback = fixture.subscribeFuture.thenAccept(
                    consumer -> unsubscribe.set(consumer.unsubscribeAsync(false)));
            Connection connection = new Connection();
            CompletableFuture<Void> opened = fixture.consumer.connectionOpened(connection.cnx);
            connection.subscribeReply.complete(null);
            opened.get(5, TimeUnit.SECONDS);
            callback.get(5, TimeUnit.SECONDS);
            assertThat(fixture.consumer.getState()).isEqualTo(HandlerState.State.Closing);
            assertThat(unsubscribe.get()).isNotNull().isNotDone();

            // Keep UNSUBSCRIBE pending until the subscription-success callback has returned.
            connection.unsubscribeReply.completeExceptionally(
                    new PulsarClientException.BrokerMetadataException("Unsubscribe rejected"));
            assertThat(unsubscribe.get().isCompletedExceptionally()).isTrue();
            assertThat(fixture.consumer.getState()).isEqualTo(HandlerState.State.Ready);
            if (startPaused) {
                assertThat(connection.sentPermits.get()).as("paused consumer retains its initial grant").isZero();
                fixture.consumer.resume();
            }
            assertThat(connection.sentPermits.get()).as("initial FLOW survives failed unsubscribe").isEqualTo(1);

            MessageImpl<byte[]> queued = new MessageImpl<>(fixture.consumer.getTopic(),
                    new MessageIdImpl(1, 1, -1), new MessageMetadata(), Unpooled.EMPTY_BUFFER,
                    connection.cnx, Schema.BYTES);
            fixture.consumer.enqueueMessageAndCheckBatchReceive(queued);
            Message<byte[]> received = fixture.consumer.receive(5, TimeUnit.SECONDS);
            try {
                assertThat(received).as("consumer remains usable after unsubscribe failure").isSameAs(queued);
            } finally {
                if (received != null) {
                    received.release();
                }
            }
        }
    }

    @Test(dataProvider = "pausedModes", timeOut = 120000)
    public void testMemoryShrinkDuringFailedUnsubscribeRetainsPermitDebt(boolean paused) throws Exception {
        try (TestConsumer fixture = new TestConsumer()) {
            Connection connection = new Connection();
            CompletableFuture<Void> opened = fixture.consumer.connectionOpened(connection.cnx);
            connection.subscribeReply.complete(null);
            opened.get(5, TimeUnit.SECONDS);
            assertThat(fixture.consumer.getCurrentReceiverQueueSize()).isEqualTo(1);
            assertThat(connection.sentPermits.get()).isEqualTo(1);

            fixture.consumer.setCurrentReceiverQueueSize(2);
            assertThat(connection.sentPermits.get()).isEqualTo(2);
            assertThat(fixture.consumer.getAvailablePermits()).isZero();
            if (paused) {
                fixture.consumer.pause();
            }
            CompletableFuture<Void> unsubscribe = fixture.consumer.unsubscribeAsync(false);
            assertThat(fixture.consumer.getState()).isEqualTo(HandlerState.State.Closing);
            assertThat(unsubscribe).isNotDone();

            // Shared memory pressure can shrink this consumer while UNSUBSCRIBE is outstanding.
            MemoryLimitController memory = new MemoryLimitController(1024 * 1024, 1,
                    fixture.consumer::reduceCurrentReceiverQueueSize);
            memory.forceReserveMemory(1);
            memory.releaseMemory(1);
            assertThat(fixture.consumer.getCurrentReceiverQueueSize()).isEqualTo(1);

            connection.unsubscribeReply.completeExceptionally(
                    new PulsarClientException.BrokerMetadataException("Unsubscribe rejected"));
            assertThat(unsubscribe.isCompletedExceptionally()).isTrue();
            assertThat(fixture.consumer.getState()).isEqualTo(HandlerState.State.Ready);
            assertThat(fixture.consumer.getAvailablePermits()).as("shrink debt survives failed unsubscribe")
                    .isEqualTo(-1);
            if (paused) {
                fixture.consumer.resume();
                assertThat(connection.sentPermits.get()).as("resume does not grant permits while in debt")
                        .isEqualTo(2);
            }

            MessageImpl<byte[]> queued = new MessageImpl<>(fixture.consumer.getTopic(),
                    new MessageIdImpl(1, 1, -1), new MessageMetadata(), Unpooled.EMPTY_BUFFER,
                    connection.cnx, Schema.BYTES);
            fixture.consumer.enqueueMessageAndCheckBatchReceive(queued);
            Message<byte[]> received = fixture.consumer.receive(5, TimeUnit.SECONDS);
            try {
                assertThat(received).isSameAs(queued);
                assertThat(connection.sentPermits.get()).as("consuming a message pays the shrink debt").isEqualTo(2);
                assertThat(fixture.consumer.getAvailablePermits()).isZero();
            } finally {
                if (received != null) {
                    received.release();
                }
            }
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
        private final CompletableFuture<Consumer<byte[]>> subscribeFuture;
        private final ConsumerImpl<byte[]> consumer;
        private final PulsarClientImpl client;
        private Runnable beforeSubscribeCompletion = () -> { };

        TestConsumer() {
            this(false);
        }

        TestConsumer(boolean hasParentConsumer) {
            this(hasParentConsumer, false);
        }

        TestConsumer(boolean hasParentConsumer, boolean startPaused) {
            this(hasParentConsumer, startPaused, 2);
        }

        TestConsumer(boolean hasParentConsumer, boolean startPaused, int receiverQueueSize) {
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
            conf.setReceiverQueueSize(receiverQueueSize);
            conf.setAutoScaledReceiverQueueSizeEnabled(true);
            conf.setStartPaused(startPaused);
            subscribeFuture = new CompletableFuture<>() {
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
        private CompletableFuture<ProducerResponse> subscribeReply = new CompletableFuture<>();
        private final CompletableFuture<ProducerResponse> unsubscribeReply = new CompletableFuture<>();
        private final AtomicInteger sentPermits = new AtomicInteger();
        private final List<BaseCommand> sentCommands = new CopyOnWriteArrayList<>();
        private final List<Long> subscribeEpochs = new CopyOnWriteArrayList<>();

        Connection() {
            ChannelHandlerContext context = mock(ChannelHandlerContext.class);
            Channel channel = mock(Channel.class);
            when(cnx.ctx()).thenReturn(context);
            when(cnx.channel()).thenReturn(channel);
            when(cnx.getMaxMessageSize()).thenReturn(5 * 1024 * 1024);
            when(cnx.getRemoteEndpointProtocolVersion()).thenReturn(Commands.getCurrentProtocolVersion());
            when(context.channel()).thenReturn(channel);
            when(context.voidPromise()).thenReturn(mock(ChannelPromise.class));
            doAnswer(invocation -> {
                BaseCommand command = decodeAndRelease(invocation.getArgument(0));
                assertThat(command.getType()).isIn(BaseCommand.Type.FLOW,
                        BaseCommand.Type.REDELIVER_UNACKNOWLEDGED_MESSAGES);
                sentCommands.add(command);
                if (command.getType() == BaseCommand.Type.FLOW) {
                    sentPermits.addAndGet(command.getFlow().getMessagePermits());
                }
                return mock(ChannelFuture.class);
            }).when(context).writeAndFlush(any(ByteBuf.class), any(ChannelPromise.class));
            when(cnx.sendRequestWithId(any(ByteBuf.class), anyLong())).thenAnswer(invocation -> {
                BaseCommand command = decodeAndRelease(invocation.getArgument(0));
                if (command.getType() == BaseCommand.Type.SUBSCRIBE) {
                    subscribeEpochs.add(command.getSubscribe().getConsumerEpoch());
                    CompletableFuture<ProducerResponse> reply = subscribeReply;
                    return reply;
                }
                if (command.getType() == BaseCommand.Type.UNSUBSCRIBE) {
                    return unsubscribeReply;
                }
                assertThat(command.getType()).isEqualTo(BaseCommand.Type.CLOSE_CONSUMER);
                return CompletableFuture.<ProducerResponse>completedFuture(null);
            });
        }

        CompletableFuture<ProducerResponse> newSubscription() {
            subscribeReply = new CompletableFuture<>();
            return subscribeReply;
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
