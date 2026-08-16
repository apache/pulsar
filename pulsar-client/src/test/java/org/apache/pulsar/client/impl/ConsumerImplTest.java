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
import static org.mockito.Mockito.never;
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
import io.netty.channel.EventLoop;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
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
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.client.util.ScheduledExecutorProvider;
import org.apache.pulsar.common.api.proto.CommandMessage;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.Backoff;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConsumerImplTest {
    private final String topic = "non-persistent://tenant/ns1/my-topic";

    private ExecutorProvider executorProvider;
    private ExecutorService internalExecutor;
    private ConsumerImpl<byte[]> consumer;
    private ConsumerConfigurationData<byte[]> consumerConf;

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
    public void testInvalidExplicitMessagePermitsCloseSourceConnection() {
        CommandMessage command = new CommandMessage()
                .setConsumerId(consumer.consumerId)
                .setMessagePermits(0);
        command.setMessageId().setLedgerId(1).setEntryId(2);
        ClientCnx messageCnx = mock(ClientCnx.class);
        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        when(messageCnx.ctx()).thenReturn(context);
        ByteBuf emptyPayload = Unpooled.buffer(0);
        int permitsBefore = consumer.getAvailablePermits();

        try {
            consumer.messageReceived(command, emptyPayload, messageCnx);
            verify(context).close();
            Assert.assertEquals(consumer.getAvailablePermits(), permitsBefore);
        } finally {
            emptyPayload.release();
        }
    }

    @Test
    public void testTruncatedFrameAndMetadataFailureReturnExplicitMessagePermits() {
        ClientCnx messageCnx = setCurrentConnection();
        ChannelHandlerContext context = messageCnx.ctx();
        int permitsBefore = consumer.getAvailablePermits();
        ByteBuf[] malformedFrames = {
                Unpooled.wrappedBuffer(new byte[] {1}),
                Unpooled.wrappedBuffer(new byte[] {0, 0, 0, 10})
        };

        try {
            for (ByteBuf malformedFrame : malformedFrames) {
                consumer.messageReceived(newCommandMessage(5), malformedFrame, messageCnx);
            }

            Assert.assertEquals(consumer.getAvailablePermits(), permitsBefore + 10);
            releaseValidationCommands(context, 2);
        } finally {
            Arrays.stream(malformedFrames).forEach(ByteBuf::release);
        }
    }

    @Test
    public void testDecompressionFailureReturnsExplicitMessagePermits() {
        ClientCnx messageCnx = setCurrentConnection();
        ChannelHandlerContext context = messageCnx.ctx();
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("producer")
                .setSequenceId(1)
                .setPublishTime(1)
                .setNumMessagesInBatch(5)
                .setCompression(CompressionType.ZLIB)
                .setUncompressedSize(100);
        ByteBuf invalidCompressedPayload = Unpooled.wrappedBuffer(new byte[] {1});
        ByteBuf metadataAndPayload = Commands.serializeMetadataAndPayload(
                Commands.ChecksumType.Crc32c, metadata, invalidCompressedPayload);
        invalidCompressedPayload.release();
        int permitsBefore = consumer.getAvailablePermits();

        try {
            consumer.messageReceived(newCommandMessage(5), metadataAndPayload, messageCnx);

            Assert.assertEquals(consumer.getAvailablePermits(), permitsBefore + 5);
            releaseValidationCommands(context, 1);
        } finally {
            metadataAndPayload.release();
        }
    }

    @Test
    public void testPermitAccumulatorOverflowClosesSourceConnection() {
        ClientCnx messageCnx = setCurrentConnection();
        ChannelHandlerContext context = messageCnx.ctx();
        consumer.paused = true;
        ByteBuf firstMalformedMetadata = Unpooled.wrappedBuffer(new byte[] {1});
        ByteBuf secondMalformedMetadata = Unpooled.wrappedBuffer(new byte[] {1});

        try {
            consumer.messageReceived(newCommandMessage(Integer.MAX_VALUE), firstMalformedMetadata, messageCnx);
            Assert.assertEquals(consumer.getAvailablePermits(), Integer.MAX_VALUE);

            consumer.messageReceived(newCommandMessage(1), secondMalformedMetadata, messageCnx);

            Assert.assertEquals(consumer.getAvailablePermits(), Integer.MAX_VALUE);
            verify(context).close();
            releaseValidationCommands(context, 2);
        } finally {
            firstMalformedMetadata.release();
            secondMalformedMetadata.release();
        }
    }

    @Test
    public void testSameClientCnxReuseCreatesNewPermitIncarnation() {
        consumer.setCurrentReceiverQueueSize(2);
        ClientCnx messageCnx = setCurrentConnection();
        ChannelHandlerContext context = messageCnx.ctx();
        ConsumerImpl.ConsumerPermitState oldPermitState = consumer.getPermitState();
        MessageImpl<?> oldMessage = mock(MessageImpl.class);
        when(oldMessage.getPermitState()).thenReturn(oldPermitState);

        // Recreate the broker consumer while reusing the same pooled physical ClientCnx.
        consumer.setClientCnx(messageCnx);
        Assert.assertNotSame(consumer.getPermitState(), oldPermitState);
        consumer.consumerIsReconnectedToBroker(messageCnx, 0);
        consumer.increaseAvailablePermits(oldMessage);

        Assert.assertEquals(consumer.getAvailablePermits(), 0);
        verify(context, never()).writeAndFlush(any(), any(ChannelPromise.class));
    }

    @Test(invocationTimeOut = 5000)
    public void testPermitReturnRaceWithSameClientCnxReconnectDoesNotContaminateNewAccumulator() {
        consumer.paused = true;
        ClientCnx messageCnx = setCurrentConnection();
        ExecutorService raceExecutor = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 100; i++) {
                ConsumerImpl.ConsumerPermitState oldPermitState = consumer.getPermitState();
                MessageImpl<?> oldMessage = mock(MessageImpl.class);
                when(oldMessage.getPermitState()).thenReturn(oldPermitState);
                CountDownLatch start = new CountDownLatch(1);

                CompletableFuture<Void> returnPermit = CompletableFuture.runAsync(() -> {
                    await(start);
                    consumer.increaseAvailablePermits(oldMessage);
                }, raceExecutor);
                CompletableFuture<Void> reconnect = CompletableFuture.runAsync(() -> {
                    await(start);
                    consumer.setClientCnx(messageCnx);
                    consumer.consumerIsReconnectedToBroker(messageCnx, 0);
                }, raceExecutor);

                start.countDown();
                CompletableFuture.allOf(returnPermit, reconnect).join();
                Assert.assertEquals(consumer.getAvailablePermits(), 0);
            }
        } finally {
            raceExecutor.shutdownNow();
        }
    }

    @Test(invocationTimeOut = 5000)
    public void testConcurrentPermitReturnsStayInCurrentIncarnationAccumulator() {
        consumer.paused = true;
        setCurrentConnection();
        ConsumerImpl.ConsumerPermitState currentPermitState = consumer.getPermitState();
        int threadCount = 8;
        int returnsPerThread = 1000;
        ExecutorService returnExecutor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<CompletableFuture<Void>> returns = new ArrayList<>(threadCount);
            for (int i = 0; i < threadCount; i++) {
                returns.add(CompletableFuture.runAsync(() -> {
                    await(start);
                    for (int permit = 0; permit < returnsPerThread; permit++) {
                        consumer.increaseAvailablePermits(currentPermitState);
                    }
                }, returnExecutor));
            }
            start.countDown();
            CompletableFuture.allOf(returns.toArray(CompletableFuture[]::new)).join();

            Assert.assertEquals(consumer.getAvailablePermits(), threadCount * returnsPerThread);
        } finally {
            returnExecutor.shutdownNow();
        }
    }

    @Test
    public void testQueuedFlowFromOldIncarnationIsDroppedAfterSameClientCnxReuse() {
        consumer.setCurrentReceiverQueueSize(2);
        ClientCnx messageCnx = setCurrentConnection();
        ChannelHandlerContext context = messageCnx.ctx();
        EventLoop eventLoop = context.channel().eventLoop();
        List<Runnable> queuedTasks = new ArrayList<>();
        doAnswer(invocation -> {
            queuedTasks.add(invocation.getArgument(0));
            return null;
        }).when(eventLoop).execute(any(Runnable.class));

        ConsumerImpl.ConsumerPermitState oldPermitState = consumer.getPermitState();
        consumer.increaseAvailablePermits(oldPermitState);
        Assert.assertEquals(queuedTasks.size(), 1);

        // Recreate the broker consumer before the old Flow task reaches the shared physical connection.
        consumer.setClientCnx(messageCnx);
        queuedTasks.get(0).run();

        Assert.assertEquals(consumer.getAvailablePermits(), 0);
        verify(context, never()).writeAndFlush(any(), any(ChannelPromise.class));
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException(e);
        }
    }

    private ClientCnx setCurrentConnection() {
        ClientCnx messageCnx = mock(ClientCnx.class);
        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        EventLoop eventLoop = mock(EventLoop.class);
        when(context.voidPromise()).thenReturn(mock(ChannelPromise.class));
        when(context.channel()).thenReturn(channel);
        when(channel.eventLoop()).thenReturn(eventLoop);
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(eventLoop).execute(any(Runnable.class));
        when(messageCnx.ctx()).thenReturn(context);
        consumer.setClientCnx(messageCnx);
        consumer.consumerIsReconnectedToBroker(messageCnx, 0);
        return messageCnx;
    }

    private CommandMessage newCommandMessage(int messagePermits) {
        CommandMessage command = new CommandMessage()
                .setConsumerId(consumer.consumerId)
                .setMessagePermits(messagePermits);
        command.setMessageId().setLedgerId(1).setEntryId(2);
        return command;
    }

    private static void releaseValidationCommands(ChannelHandlerContext context, int expectedCommands) {
        ArgumentCaptor<Object> commandCaptor = ArgumentCaptor.forClass(Object.class);
        verify(context, times(expectedCommands)).writeAndFlush(
                commandCaptor.capture(), any(ChannelPromise.class));
        commandCaptor.getAllValues().forEach(ReferenceCountUtil::release);
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
        MessagePayloadContextImpl context = MessagePayloadContextImpl.get(
                null, messageMetadata, messageId, consumer, 0, ackSet, DEFAULT_CONSUMER_EPOCH);
        MessagePayload payload0 = MessagePayloadImpl.create(Unpooled.wrappedBuffer(new byte[]{0}));
        MessagePayload payload1 = MessagePayloadImpl.create(Unpooled.wrappedBuffer(new byte[]{1}));
        try {
            // Index 0 is already acked per the broker, so it must not be redelivered to the app.
            Assert.assertNull(context.getMessageAt(0, batchSize, payload0, false, Schema.BYTES));

            Message<byte[]> message1 = context.getMessageAt(1, batchSize, payload1, false, Schema.BYTES);
            Assert.assertNotNull(message1);

            BitSet ackSetInMessageId = ((MessageIdAdv) message1.getMessageId()).getAckSet();
            Assert.assertFalse(ackSetInMessageId.get(0),
                    "index 0 was already acked by the broker, so the returned MessageId's ackSet "
                            + "must reflect it as acked, not fall back to the default all-unacked state");
            Assert.assertTrue(ackSetInMessageId.get(1), "index 1 is still outstanding");
            Assert.assertTrue(ackSetInMessageId.get(2), "index 2 is still outstanding");
        } finally {
            payload0.release();
            payload1.release();
            context.recycle();
        }
    }
}
