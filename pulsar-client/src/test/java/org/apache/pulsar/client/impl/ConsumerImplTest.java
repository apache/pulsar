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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.MessagePayload;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.TopicConsumerConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.client.impl.metrics.UpDownCounter;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.client.util.ScheduledExecutorProvider;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandMessage;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.Backoff;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConsumerImplTest {
    private static final long RANDOM_PERMIT_SEED = 0x491C11E17L;

    private final String topic = "non-persistent://tenant/ns1/my-topic";

    private ExecutorProvider executorProvider;
    private ExecutorService internalExecutor;
    private ConsumerImpl<byte[]> consumer;
    private ConsumerConfigurationData<byte[]> consumerConf;
    private UpDownCounter messagesPrefetchedGauge;
    private UpDownCounter bytesPrefetchedGauge;

    @BeforeMethod(alwaysRun = true)
    public void setUp() {
        consumerConf = new ConsumerConfigurationData<>();
        createConsumer(consumerConf);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void createConsumer(ConsumerConfigurationData consumerConf) {
        executorProvider = new ExecutorProvider(1, "ConsumerImplTest");
        internalExecutor = Executors.newSingleThreadScheduledExecutor();

        PulsarClientImpl client = ClientTestFixtures.createPulsarClientMock(executorProvider, internalExecutor);
        InstrumentProvider instrumentProvider = spy(InstrumentProvider.NOOP);
        messagesPrefetchedGauge = mock(UpDownCounter.class);
        bytesPrefetchedGauge = mock(UpDownCounter.class);
        doAnswer(invocation -> invocation.<String>getArgument(0).endsWith(".count")
                ? messagesPrefetchedGauge : bytesPrefetchedGauge)
                .when(instrumentProvider).newUpDownCounter(any(), any(), any(), any(), any());
        when(client.instrumentProvider()).thenReturn(instrumentProvider);
        ClientConfigurationData clientConf = client.getConfiguration();
        clientConf.setOperationTimeoutMs(100);
        clientConf.setStatsIntervalSeconds(0);
        CompletableFuture<Consumer<byte[]>> subscribeFuture = new CompletableFuture<>();

        consumerConf.setSubscriptionName("test-sub");
        consumer = ConsumerImpl.newConsumerImpl(client, topic, consumerConf,
                executorProvider, -1, false, subscribeFuture, null, null, null,
                true);
        consumer.setState(HandlerState.State.Ready);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (executorProvider != null) {
            executorProvider.shutdownNow();
            executorProvider = null;
        }
        if (internalExecutor != null) {
            internalExecutor.shutdownNow();
            internalExecutor = null;
        }
    }

    @Test(invocationTimeOut = 1000)
    public void testNotifyPendingReceivedCallback_EmptyQueueNotThrowsException() {
        consumer.notifyPendingReceivedCallback(null, null);
    }

    @Test
    public void testGetMessagePermitsUsesCommandValueWhenPresent() {
        CommandMessage command = new CommandMessage().setMessagePermits(3);
        command.addAckSet(0b10101L);

        Assert.assertEquals(ConsumerImpl.getMessagePermits(command, 10), 3);
    }

    @Test
    public void testGetMessagePermitsFallsBackToAckSetForOldBroker() {
        CommandMessage command = new CommandMessage();
        command.addAckSet(0b101101L);

        Assert.assertEquals(ConsumerImpl.getMessagePermits(command, 10), 4);
    }

    @Test
    public void testGetMessagePermitsFallsBackToBatchSizeForOldBroker() {
        Assert.assertEquals(ConsumerImpl.getMessagePermits(new CommandMessage(), 10), 10);
    }

    @Test
    public void testGetMessagePermitsValidatesNativeCommand() {
        Assert.assertEquals(ConsumerImpl.getMessagePermits(new CommandMessage().setMessagePermits(1), 1), 1);
        Assert.assertThrows(RuntimeException.class,
                () -> ConsumerImpl.getMessagePermits(new CommandMessage().setMessagePermits(0), 1));
        Assert.assertThrows(RuntimeException.class,
                () -> ConsumerImpl.getMessagePermits(new CommandMessage().setMessagePermits(-1), 1));
        Assert.assertThrows(RuntimeException.class,
                () -> ConsumerImpl.getMessagePermits(new CommandMessage(), 0));
        Assert.assertThrows(RuntimeException.class,
                () -> {
                    CommandMessage command = new CommandMessage();
                    command.addAckSet(0);
                    ConsumerImpl.getMessagePermits(command, 1);
                });
        Assert.assertThrows(RuntimeException.class,
                () -> {
                    CommandMessage command = new CommandMessage();
                    command.addAckSet(0b10);
                    ConsumerImpl.getMessagePermits(command, 1);
                });
        Assert.assertThrows(RuntimeException.class,
                () -> {
                    CommandMessage command = new CommandMessage().setMessagePermits(2);
                    command.addAckSet(0b1);
                    ConsumerImpl.getMessagePermits(command, 2);
                });
    }

    @Test
    public void testChecksumFailureReturnsCommandPermits() {
        final int messagePermits = 10;
        CommandMessage command = new CommandMessage()
                .setConsumerId(consumer.consumerId)
                .setMessagePermits(messagePermits);
        command.setMessageId().setLedgerId(1).setEntryId(2);
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("producer")
                .setSequenceId(0)
                .setPublishTime(System.currentTimeMillis())
                .setNumMessagesInBatch(messagePermits);
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[] {1});
        ByteBuf metadataAndPayload = Commands.serializeMetadataAndPayload(
                Commands.ChecksumType.Crc32c, metadata, payload);
        payload.release();
        metadataAndPayload.setByte(metadataAndPayload.writerIndex() - 1,
                metadataAndPayload.getByte(metadataAndPayload.writerIndex() - 1) ^ 1);

        ClientCnx cnx = mock(ClientCnx.class);
        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        ChannelPromise promise = mock(ChannelPromise.class);
        when(cnx.ctx()).thenReturn(context);
        when(context.voidPromise()).thenReturn(promise);
        consumer.setClientCnx(cnx);
        int permitsBefore = consumer.getAvailablePermits();
        try {
            consumer.messageReceived(command, metadataAndPayload, cnx);
            Assert.assertEquals(consumer.getAvailablePermits(), permitsBefore + messagePermits);
        } finally {
            metadataAndPayload.release();
        }
    }

    @Test
    public void testPermitReturnIsBoundToConsumerIncarnationOnSameConnection() {
        ClientCnx cnx = mock(ClientCnx.class);
        consumer.setClientCnx(cnx);
        ConsumerImpl.ConsumerPermitState oldState = consumer.getPermitState();
        consumer.increaseAvailablePermits(cnx, 1);
        Assert.assertEquals(oldState.availablePermits.get(), 1);

        consumer.deactivatePermitState(oldState);
        consumer.setClientCnx(cnx);
        ConsumerImpl.ConsumerPermitState replacementState = consumer.getPermitState();
        Assert.assertNotSame(replacementState, oldState);
        Assert.assertSame(replacementState.cnx, oldState.cnx);

        MessageImpl<byte[]> oldMessage = new MessageImpl<>(topic, new MessageIdImpl(1, 1, -1),
                new MessageMetadata(), Unpooled.EMPTY_BUFFER, cnx, Schema.BYTES);
        oldMessage.setFlowPermitOwnership(oldState, 1);
        consumer.increaseAvailablePermits(oldMessage);
        Assert.assertEquals(replacementState.availablePermits.get(), 0);

        MessageImpl<byte[]> currentMessage = new MessageImpl<>(topic, new MessageIdImpl(1, 2, -1),
                new MessageMetadata(), Unpooled.EMPTY_BUFFER, cnx, Schema.BYTES);
        currentMessage.setFlowPermitOwnership(replacementState, 1);
        consumer.increaseAvailablePermits(currentMessage);
        consumer.increaseAvailablePermits(currentMessage);
        Assert.assertEquals(replacementState.availablePermits.get(), 1,
                "A terminal message path must return its permit at most once");
    }

    @Test
    public void testMalformedExplicitPermitsCloseSourceIncarnationWithoutCredit() {
        ClientCnx cnx = mock(ClientCnx.class);
        Channel channel = mock(Channel.class);
        when(cnx.channel()).thenReturn(channel);
        consumer.setClientCnx(cnx);
        CommandMessage command = new CommandMessage().setMessagePermits(0);
        command.setMessageId().setLedgerId(1).setEntryId(2);

        consumer.messageReceived(command, Unpooled.EMPTY_BUFFER, cnx);

        verify(channel).close();
        Assert.assertNull(consumer.getPermitState());
        Assert.assertEquals(consumer.getAvailablePermits(), 0);
    }

    @Test
    public void testPermitAccumulatorOverflowClosesSourceIncarnation() {
        ClientCnx cnx = mock(ClientCnx.class);
        Channel channel = mock(Channel.class);
        when(cnx.channel()).thenReturn(channel);
        consumer.setClientCnx(cnx);
        ConsumerImpl.ConsumerPermitState state = consumer.getPermitState();
        state.availablePermits.set(Integer.MAX_VALUE);
        MessageImpl<byte[]> message = new MessageImpl<>(topic, new MessageIdImpl(1, 1, -1),
                new MessageMetadata(), Unpooled.EMPTY_BUFFER, cnx, Schema.BYTES);
        message.setFlowPermitOwnership(state, 1);

        consumer.increaseAvailablePermits(message);

        verify(channel).close();
        Assert.assertNull(consumer.getPermitState());
    }

    @Test
    public void testPooledMessageCanBeRetainedByDeadLetterAndMessageLifecycles() {
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[] {1});
        MessageImpl<byte[]> message = MessageImpl.create(topic, new MessageIdImpl(1, 1, -1),
                new MessageMetadata(), payload, Optional.empty(), mock(ClientCnx.class), Schema.BYTES,
                0, true, DEFAULT_CONSUMER_EPOCH);
        try {
            Assert.assertEquals(payload.refCnt(), 2);
            message.retain();
            message.release();
            Assert.assertEquals(payload.refCnt(), 2,
                    "Releasing one owner must not recycle a message retained by dead-letter handling");
            message.release();
            Assert.assertEquals(payload.refCnt(), 1);
        } finally {
            payload.release();
        }
    }

    @Test
    public void testStaleEpochBatchOnCurrentIncarnationReturnsPermitAndClosesPrefetchGauges() throws Exception {
        consumer.setCurrentReceiverQueueSize(2);
        ClientCnx messageCnx = setCurrentConnectionWithFlowEnabled();
        ChannelHandlerContext context = messageCnx.ctx();
        List<Integer> flowPermits = new ArrayList<>();
        doAnswer(invocation -> {
            flowPermits.add(parseFlowPermits(invocation.getArgument(0)));
            return null;
        }).when(context).writeAndFlush(any(ByteBuf.class), any(ChannelPromise.class));
        ConsumerBase.CONSUMER_EPOCH.set(consumer, 2);
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("producer")
                .setSequenceId(1)
                .setPublishTime(1)
                .setNumMessagesInBatch(1);
        ByteBuf batch = Unpooled.buffer();
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[] {1});
        Commands.serializeSingleMessageInBatchWithPayload(new SingleMessageMetadata(), payload, batch);
        payload.release();

        try {
            consumer.receiveIndividualMessagesFromBatch(null, metadata, 0, null, batch,
                    new MessageIdData().setLedgerId(1).setEntryId(2), messageCnx, 1, false, 1);
            consumer.internalPinnedExecutor.submit(
                    () -> Assert.assertEquals(consumer.numMessagesInQueue(), 0)).get(5, TimeUnit.SECONDS);

            Assert.assertEquals(consumer.numMessagesInQueue(), 0);
            Assert.assertEquals(consumer.getAvailablePermits(), 0);
            Assert.assertEquals(flowPermits, List.of(1));
            verify(messagesPrefetchedGauge).increment();
            verify(messagesPrefetchedGauge).decrement();
            ArgumentCaptor<Long> addedBytes = ArgumentCaptor.forClass(Long.class);
            ArgumentCaptor<Long> subtractedBytes = ArgumentCaptor.forClass(Long.class);
            verify(bytesPrefetchedGauge).add(addedBytes.capture());
            verify(bytesPrefetchedGauge).subtract(subtractedBytes.capture());
            Assert.assertEquals(subtractedBytes.getValue(), addedBytes.getValue());
        } finally {
            batch.release();
        }
    }

    @Test
    public void testRandomizedBatchDecodeWritesExactlyTheReturnedPermitsToFlow() throws Exception {
        consumer.setCurrentReceiverQueueSize(2);
        ClientCnx messageCnx = setCurrentConnectionWithFlowEnabled();
        ChannelHandlerContext context = messageCnx.ctx();
        List<Integer> flowPermits = new ArrayList<>();
        doAnswer(invocation -> {
            flowPermits.add(parseFlowPermits(invocation.getArgument(0)));
            return null;
        }).when(context).writeAndFlush(any(ByteBuf.class), any(ChannelPromise.class));
        SplittableRandom random = new SplittableRandom(RANDOM_PERMIT_SEED);
        int expectedReturnedPermits = 0;

        for (int testCase = 0; testCase < 250; testCase++) {
            int batchSize = random.nextInt(1, 65);
            int requiredWords = (batchSize + Long.SIZE - 1) / Long.SIZE;
            long[] ackSet = new long[requiredWords + random.nextInt(3)];
            for (int word = 0; word < ackSet.length; word++) {
                ackSet[word] = random.nextLong();
            }
            int requiredIndex = random.nextInt(batchSize);
            ackSet[requiredIndex / Long.SIZE] |= 1L << (requiredIndex % Long.SIZE);
            BitSet deliveredIndexes = BitSet.valueOf(ackSet);
            deliveredIndexes.clear(batchSize, Math.max(batchSize, deliveredIndexes.length()));
            int messagePermits = deliveredIndexes.cardinality();
            int expectedQueuedMessages = 0;
            MessageMetadata metadata = new MessageMetadata()
                    .setProducerName("producer")
                    .setSequenceId(testCase)
                    .setPublishTime(1)
                    .setNumMessagesInBatch(batchSize);
            ByteBuf batch = Unpooled.buffer();
            for (int index = 0; index < batchSize; index++) {
                ByteBuf payload = Unpooled.wrappedBuffer(new byte[] {(byte) index});
                boolean compactedOut = deliveredIndexes.get(index) && random.nextInt(5) == 0;
                if (deliveredIndexes.get(index) && !compactedOut) {
                    expectedQueuedMessages++;
                }
                Commands.serializeSingleMessageInBatchWithPayload(
                        new SingleMessageMetadata().setCompactedOut(compactedOut), payload, batch);
                payload.release();
            }

            try {
                consumer.receiveIndividualMessagesFromBatch(null, metadata, 0, ackSet, batch,
                        new MessageIdData().setLedgerId(1).setEntryId(testCase), messageCnx,
                        DEFAULT_CONSUMER_EPOCH, false, messagePermits);
                int queuedMessages = consumer.internalPinnedExecutor.submit(consumer::numMessagesInQueue)
                        .get(5, TimeUnit.SECONDS);

                Assert.assertEquals(queuedMessages, expectedQueuedMessages,
                        "seed=" + RANDOM_PERMIT_SEED + ", case=" + testCase);
                for (int permit = 0; permit < expectedQueuedMessages; permit++) {
                    Message<byte[]> message = consumer.incomingMessages.poll();
                    Assert.assertNotNull(message);
                    consumer.messageProcessed(message);
                    message.release();
                }
                expectedReturnedPermits += messagePermits;
                Assert.assertEquals(consumer.getAvailablePermits(), 0,
                        "seed=" + RANDOM_PERMIT_SEED + ", case=" + testCase);
                Assert.assertEquals(flowPermits.stream().mapToInt(Integer::intValue).sum(), expectedReturnedPermits,
                        "seed=" + RANDOM_PERMIT_SEED + ", case=" + testCase);
            } finally {
                batch.release();
            }
        }
    }

    private ClientCnx setCurrentConnectionWithFlowEnabled() {
        ClientCnx messageCnx = mock(ClientCnx.class);
        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        EventExecutor eventExecutor = mock(EventExecutor.class);
        when(context.voidPromise()).thenReturn(mock(ChannelPromise.class));
        when(context.channel()).thenReturn(channel);
        when(context.executor()).thenReturn(eventExecutor);
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(eventExecutor).execute(any(Runnable.class));
        when(messageCnx.ctx()).thenReturn(context);
        when(messageCnx.channel()).thenReturn(channel);
        consumer.setClientCnx(messageCnx);
        consumer.getPermitState().flowEnabled = true;
        return messageCnx;
    }

    private static int parseFlowPermits(ByteBuf frame) {
        try {
            frame.skipBytes(Integer.BYTES);
            int commandSize = (int) frame.readUnsignedInt();
            BaseCommand command = new BaseCommand();
            command.parseFrom(frame, commandSize);
            Assert.assertEquals(command.getType(), BaseCommand.Type.FLOW);
            return command.getFlow().getMessagePermits();
        } finally {
            frame.release();
        }
    }

    @Test(invocationTimeOut = 500)
    public void testCorrectBackoffConfiguration() {
        final Backoff backoff = consumer.getConnectionHandler().backoff;
        ClientConfigurationData clientConfigurationData = new ClientConfigurationData();
        Assert.assertEquals(backoff.getMax().toMillis(),
                TimeUnit.NANOSECONDS.toMillis(clientConfigurationData.getMaxBackoffIntervalNanos()));
        Assert.assertEquals(backoff.getInitial().toMillis(),
                TimeUnit.NANOSECONDS.toMillis(clientConfigurationData.getInitialBackoffIntervalNanos()));
    }

    @Test(invocationTimeOut = 1000)
    public void testNotifyPendingReceivedCallback_CompleteWithException() {
        CompletableFuture<Message<byte[]>> receiveFuture = new CompletableFuture<>();
        consumer.pendingReceives.add(receiveFuture);
        Exception exception = new PulsarClientException.InvalidMessageException("some random exception");
        consumer.notifyPendingReceivedCallback(null, exception);

        try {
            receiveFuture.join();
        } catch (CompletionException e) {
            // Completion exception must be the same we provided at calling time
            Assert.assertEquals(e.getCause(), exception);
        }

        Assert.assertTrue(receiveFuture.isCompletedExceptionally());
    }

    @Test(invocationTimeOut = 1000)
    public void testNotifyPendingReceivedCallback_CompleteWithExceptionWhenMessageIsNull() {
        CompletableFuture<Message<byte[]>> receiveFuture = new CompletableFuture<>();
        consumer.pendingReceives.add(receiveFuture);
        consumer.notifyPendingReceivedCallback(null, null);

        try {
            receiveFuture.join();
        } catch (CompletionException e) {
            Assert.assertEquals("received message can't be null", e.getCause().getMessage());
        }

        Assert.assertTrue(receiveFuture.isCompletedExceptionally());
    }

    @Test(invocationTimeOut = 1000)
    @SuppressWarnings("unchecked")
    public void testNotifyPendingReceivedCallback_InterceptorsWorksWithPrefetchDisabled() {
        CompletableFuture<Message<byte[]>> receiveFuture = new CompletableFuture<>();
        @SuppressWarnings("rawtypes")
        MessageImpl message = mock(MessageImpl.class);
        ConsumerImpl<byte[]> spy = spy(consumer);

        consumer.pendingReceives.add(receiveFuture);
        consumerConf.setReceiverQueueSize(0);
        doReturn(message).when(spy).beforeConsume(any());
        spy.notifyPendingReceivedCallback(message, null);
        Message<byte[]> receivedMessage = receiveFuture.join();

        verify(spy, times(1)).beforeConsume(message);
        Assert.assertTrue(receiveFuture.isDone());
        Assert.assertFalse(receiveFuture.isCompletedExceptionally());
        Assert.assertEquals(receivedMessage, message);
    }

    @Test(invocationTimeOut = 1000)
    @SuppressWarnings("unchecked")
    public void testNotifyPendingReceivedCallback_WorkNormally() {
        CompletableFuture<Message<byte[]>> receiveFuture = new CompletableFuture<>();
        @SuppressWarnings("rawtypes")
        MessageImpl message = mock(MessageImpl.class);
        ConsumerImpl<byte[]> spy = spy(consumer);

        consumer.pendingReceives.add(receiveFuture);
        doReturn(message).when(spy).beforeConsume(any());
        doNothing().when(spy).messageProcessed(message);
        spy.notifyPendingReceivedCallback(message, null);
        Message<byte[]> receivedMessage = receiveFuture.join();

        verify(spy, times(1)).beforeConsume(message);
        verify(spy, times(1)).messageProcessed(message);
        Assert.assertTrue(receiveFuture.isDone());
        Assert.assertFalse(receiveFuture.isCompletedExceptionally());
        Assert.assertEquals(receivedMessage, message);
    }

    @Test
    public void testReceiveAsyncCanBeCancelled() {
        // given
        CompletableFuture<Message<byte[]>> future = consumer.receiveAsync();
        Awaitility.await().untilAsserted(() -> Assert.assertTrue(consumer.hasNextPendingReceive()));
        // when
        future.cancel(true);
        // then
        Assert.assertTrue(consumer.pendingReceives.isEmpty());
    }

    @Test
    public void testBatchReceiveAsyncCanBeCancelled() {
        // given
        CompletableFuture<Messages<byte[]>> future = consumer.batchReceiveAsync();
        Awaitility.await().untilAsserted(() -> Assert.assertTrue(consumer.hasPendingBatchReceive()));
        // when
        future.cancel(true);
        // then
        Assert.assertFalse(consumer.hasPendingBatchReceive());
    }

    @Test
    public void testClose() {
        Exception checkException = null;
        try {
            if (consumer != null) {
                consumer.negativeAcknowledge(new MessageIdImpl(0, 0, -1));
                consumer.close();
            }
        } catch (Exception e) {
            checkException = e;
        }
        Assert.assertNull(checkException);
    }

    @Test
    public void testConsumerCreatedWhilePaused() throws InterruptedException {
        PulsarClientImpl client = ClientTestFixtures.createPulsarClientMock(executorProvider, internalExecutor);
        ClientConfigurationData clientConf = client.getConfiguration();
        clientConf.setOperationTimeoutMs(100);
        clientConf.setStatsIntervalSeconds(0);
        String topic = "non-persistent://tenant/ns1/my-topic";

        consumerConf.setStartPaused(true);

        consumer = ConsumerImpl.newConsumerImpl(client, topic, consumerConf,
                executorProvider, -1, false, new CompletableFuture<>(), null, null, null,
                true);

        Assert.assertTrue(consumer.paused);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testCreateConsumerWhenSchemaIsNull() throws PulsarClientException {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
            .serviceUrl("pulsar://127.0.0.1:6650")
            .build();

        client.newConsumer(null)
            .topic("topic_testCreateConsumerWhenSchemaIsNull")
            .subscriptionName("testCreateConsumerWhenSchemaIsNull")
            .subscribe();
    }

    @Test
    public void testMaxReceiverQueueSize() {
        consumer.setClientCnx(mock(ClientCnx.class));
        int size = consumer.getCurrentReceiverQueueSize();
        int permits = consumer.getAvailablePermits();
        consumer.setCurrentReceiverQueueSize(size + 100);
        Assert.assertEquals(consumer.getCurrentReceiverQueueSize(), size + 100);
        Assert.assertEquals(consumer.getAvailablePermits(), permits + 100);
    }

    @Test
    public void testTopicPriorityLevel() {
        ConsumerConfigurationData<byte[]> consumerConf2 = new ConsumerConfigurationData<>();
        consumerConf2.getTopicConfigurations().add(
                TopicConsumerConfigurationData.ofTopicName(topic, 1));

        createConsumer(consumerConf2);

        assertThat(consumer.getPriorityLevel()).isEqualTo(1);
    }

    @Test
    public void testSeekAsyncInternal() {
        // given
        ClientCnx cnx = mock(ClientCnx.class);
        CompletableFuture<ProducerResponse> clientReq = new CompletableFuture<>();
        when(cnx.sendRequestWithId(any(ByteBuf.class), anyLong())).thenReturn(clientReq);

        ScheduledExecutorProvider provider = mock(ScheduledExecutorProvider.class);
        ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
        when(provider.getExecutor()).thenReturn(scheduledExecutorService);
        when(consumer.getClient().getScheduledExecutorProvider()).thenReturn(provider);

        CompletableFuture<Void> result = consumer.seekAsync(1L);
        verify(scheduledExecutorService, atLeast(1)).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

        consumer.setClientCnx(cnx);
        consumer.setState(HandlerState.State.Ready);
        consumer.seekStatus.set(ConsumerImpl.SeekStatus.NOT_STARTED);

        // when
        CompletableFuture<Void> firstResult = consumer.seekAsync(1L);
        CompletableFuture<Void> secondResult = consumer.seekAsync(1L);

        clientReq.complete(null);

        assertTrue(firstResult.isDone());
        assertTrue(secondResult.isCompletedExceptionally());
        verify(cnx, times(1)).sendRequestWithId(any(ByteBuf.class), anyLong());
    }

    @Test(invocationTimeOut = 1000)
    public void testAutoGenerateConsumerName() {
        Pattern consumerNamePattern = Pattern.compile("[a-zA-Z0-9]{5}");
        assertTrue(consumerNamePattern.matcher(consumer.getConsumerName()).matches());
    }

    @Test(invocationTimeOut = 1000)
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testUpdateAutoScaleReceiverQueueHintRaceWithConcurrentDrain() {
        // Regression test: ConsumerBase.enqueueMessageAndCheckBatchReceive() calls
        // updateAutoScaleReceiverQueueHint() after incomingMessages.offer(message) under
        // incomingQueueLock, but incomingMessages.take()/poll() does NOT acquire that lock.
        // A consumer thread draining the queue in parallel with the client-IO thread's
        // enqueue can therefore remove the just-offered message before the hint read of
        // incomingMessages.size() runs. The hint would then see size() == 0 and be
        // spuriously cleared, even though the pipeline was full at enqueue time.
        consumerConf = new ConsumerConfigurationData<>();
        consumerConf.setAutoScaledReceiverQueueSizeEnabled(true);
        createConsumer(consumerConf);
        consumer.setCurrentReceiverQueueSize(1);

        // Simulate the race: enqueue a message and drain it before the hint is computed.
        MessageImpl message = mock(MessageImpl.class);
        when(message.size()).thenReturn(100);
        consumer.incomingMessages.offer(message);
        consumer.incomingMessages.poll();

        Assert.assertEquals(consumer.incomingMessages.size(), 0);
        Assert.assertEquals(consumer.getAvailablePermits(), 0);
        Assert.assertEquals(consumer.getCurrentReceiverQueueSize(), 1);

        consumer.updateAutoScaleReceiverQueueHint();

        Assert.assertTrue(consumer.scaleReceiverQueueHint.get(),
                "Hint must reflect the post-enqueue state (pipeline had >=1 message); "
                        + "a concurrent drain of the just-enqueued message must not clear it.");
    }

    @Test(invocationTimeOut = 1000)
    public void testGetMessageAtSyncsAckSetInMessageIdWithBrokerAckSet() {
        // Regression test for MessagePayloadContextImpl#getMessageAt: the BatchMessageIdImpl handed
        // back to the caller carries a shared ackSetInMessageId bitset that must be seeded from the
        // broker-reported ackSet, not a fresh "all unacked" bitset. Otherwise indices the broker
        // already knows are acked would be reported as still-outstanding in the returned MessageId,
        // which is the same root cause that let acked batch messages leak into the DLQ (see
        // ConsumerImpl#receiveIndividualMessagesFromBatch and its ackSetInMessageId.and(...) fix).
        final int batchSize = 3;
        MessageMetadata messageMetadata = new MessageMetadata()
                .setProducerName("test-producer")
                .setSequenceId(0)
                .setPublishTime(System.currentTimeMillis())
                .setNumMessagesInBatch(batchSize);

        // Broker reports index 0 as already acked (bit cleared); indices 1 and 2 are still
        // outstanding (bits set). This mirrors the ackSet the broker attaches on redelivery.
        BitSet brokerAckSet = new BitSet(batchSize);
        brokerAckSet.set(1);
        brokerAckSet.set(2);
        List<Long> ackSet = Arrays.stream(brokerAckSet.toLongArray()).boxed().collect(Collectors.toList());

        MessageIdImpl messageId = new MessageIdImpl(1L, 2L, -1);
        ClientCnx messageCnx = mock(ClientCnx.class);
        ConsumerImpl.ConsumerPermitState permitState = new ConsumerImpl.ConsumerPermitState(messageCnx);
        MessagePayloadContextImpl context = MessagePayloadContextImpl.get(
                null, messageMetadata, messageId, consumer, 0, ackSet, DEFAULT_CONSUMER_EPOCH, permitState);
        MessagePayload payload0 = MessagePayloadImpl.create(Unpooled.wrappedBuffer(new byte[]{0}));
        MessagePayload payload1 = MessagePayloadImpl.create(Unpooled.wrappedBuffer(new byte[]{1}));
        Message<byte[]> message1 = null;
        try {
            // Index 0 is already acked per the broker, so it must not be redelivered to the app.
            Assert.assertNull(context.getMessageAt(0, batchSize, payload0, false, Schema.BYTES));

            message1 = context.getMessageAt(1, batchSize, payload1, false, Schema.BYTES);
            Assert.assertNotNull(message1);
            Assert.assertSame(((MessageImpl<?>) message1).getCnx(), messageCnx,
                    "The message must retain the connection that delivered its command");
            Assert.assertSame(((MessageImpl<?>) message1).getPermitState(), permitState);
            Assert.assertEquals(((MessageImpl<?>) message1).getFlowPermitCost(), 1);

            BitSet ackSetInMessageId = ((MessageIdAdv) message1.getMessageId()).getAckSet();
            Assert.assertFalse(ackSetInMessageId.get(0),
                    "index 0 was already acked by the broker, so the returned MessageId's ackSet "
                            + "must reflect it as acked, not fall back to the default all-unacked state");
            Assert.assertTrue(ackSetInMessageId.get(1), "index 1 is still outstanding");
            Assert.assertTrue(ackSetInMessageId.get(2), "index 2 is still outstanding");
        } finally {
            if (message1 != null) {
                message1.release();
            }
            payload0.release();
            payload1.release();
            context.recycle();
        }
    }
}
