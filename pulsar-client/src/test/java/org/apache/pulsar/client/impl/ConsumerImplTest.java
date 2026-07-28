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
import java.util.concurrent.atomic.AtomicReference;
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
import org.apache.pulsar.common.api.proto.CompressionType;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.util.Backoff;
import org.awaitility.Awaitility;
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

    /**
     * Reproduces the chunked-message bookkeeping data race in {@link ConsumerImpl}, using the real
     * production cross-thread pair:
     * <ul>
     *   <li>{@code processMessageChunk(...)} on the Netty IO event-loop thread (here: the "receiver" thread);</li>
     *   <li>{@code removeExpireIncompleteChunkedMessages()} on the internalPinnedExecutor (here: the "expirer"
     *       thread).</li>
     * </ul>
     *
     * <p>The faithful scenario is "a late chunk arrives for a uuid that is concurrently being expired": for each uuid
     * the receiver delivers chunk 0, marks the ctx as already expired (receivedTime = 0), then delivers a late chunk 1
     * that writes into the same {@code chunkedMsgBuffer} — exactly when the expiry task may release/recycle that ctx.
     * Without serialization this races (use-after-free / double-recycle). With the fix, both paths take
     * chunkedMessageLock, so it passes and pendingChunkedMessageCount stays equal to chunkedMessagesMap.size().
     */
    @Test(timeOut = 60000)
    public void testChunkedMessageCountRaceBetweenReceiveAndExpiry() throws Exception {
        // No max-pending eviction on the receive thread, so only the expiry path removes entries
        consumerConf.setMaxPendingChunkedMessage(0);
        createConsumer(consumerConf);
        // Enable the expiry path (so removeExpireIncompleteChunkedMessages actually works) but skip its lazy
        // self-scheduling so the test doesn't need a scheduled-executor mock.
        consumer.expireTimeOfIncompleteChunkedMessageMillis = 1L;
        consumer.expireChunkMessageTaskScheduled.set(true);

        final int rounds = 5;
        final int iterations = 50000;
        final AtomicReference<Throwable> error = new AtomicReference<>();

        for (int round = 0; round < rounds && error.get() == null; round++) {
            final int currentRound = round;
            final CountDownLatch start = new CountDownLatch(1);
            Thread receiver = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < iterations && error.get() == null; i++) {
                        String uuid = "uuid-" + currentRound + "-" + i;
                        sendChunk(uuid, 0, 3);
                        ConsumerImpl.ChunkedMessageCtx ctx = consumer.chunkedMessagesMap.get(uuid);
                        if (ctx != null) {
                            // Make it eligible for removeExpireIncompleteChunkedMessages right away.
                            ctx.receivedTime = 0;
                        }
                        // Late chunk for the now-expiring uuid -> writes into the same ctx buffer.
                        sendChunk(uuid, 1, 3);
                    }
                } catch (Throwable t) {
                    error.compareAndSet(null, t);
                }
            }, "receiver-netty-sim");

            Thread expirer = new Thread(() -> {
                try {
                    start.await();
                    while (receiver.isAlive() || !consumer.chunkedMessagesMap.isEmpty()) {
                        consumer.removeExpireIncompleteChunkedMessages();
                    }
                } catch (Throwable t) {
                    error.compareAndSet(null, t);
                }
            }, "expirer-pinned");

            receiver.start();
            expirer.start();
            start.countDown();
            receiver.join();
            expirer.join();
        }

        if (error.get() != null) {
            throw new AssertionError("chunked-message bookkeeping race: concurrent ctx/buffer access corrupted state",
                    error.get());
        }
        int count = consumer.pendingChunkedMessageCount;
        int mapSize = consumer.chunkedMessagesMap.size();
        Assert.assertEquals(count, mapSize, "pendingChunkedMessageCount (" + count
                + ") drifted from chunkedMessagesMap.size() (" + mapSize + ")");
    }

    private void sendChunk(String uuid, int chunkId, int numChunks) {
        MessageMetadata md = new MessageMetadata()
                .setProducerName("p").setSequenceId(0).setPublishTime(System.currentTimeMillis())
                .setUuid(uuid).setChunkId(chunkId).setNumChunksFromMsg(numChunks).setTotalChunkMsgSize(64);
        md.setCompression(CompressionType.NONE);
        MessageIdData idData = new MessageIdData().setLedgerId(1L).setEntryId(chunkId);
        MessageIdImpl msgId = new MessageIdImpl(1L, chunkId, -1);
        ByteBuf chunk = Unpooled.wrappedBuffer(new byte[] {1, 2, 3, 4});
        consumer.processMessageChunk(chunk, md, msgId, idData, null);
    }

    /**
     * When a duplicated first chunk (chunkId == 0, redelivered) arrives for a uuid that already has an
     * in-progress ctx, the old ctx is replaced: removing/recycling it must be paired with decrementing
     * pendingChunkedMessageCount before the unconditional increment for the new ctx, so the counter stays
     * equal to the real number of in-progress chunked messages (chunkedMessagesMap.size()).
     */
    @Test
    public void testDuplicateFirstChunkOvercountsPendingChunkedMessageCount() throws Exception {
        // Disable the lazy expiry self-scheduling so this test doesn't need a scheduled-executor mock.
        consumer.expireTimeOfIncompleteChunkedMessageMillis = 0L;

        final String uuid = "uuid-dup";
        // Deliver the first chunk (chunkId == 0) twice for the SAME uuid and SAME (ledgerId, entryId): a redelivered
        // first chunk of a 2-chunk message. The message never completes, so only the ++/-- bookkeeping is exercised.
        for (int call = 0; call < 2; call++) {
            sendChunk(uuid, 0, 2);
        }

        int count = consumer.pendingChunkedMessageCount;
        int mapSize = consumer.chunkedMessagesMap.size();
        Assert.assertEquals(count, mapSize,
                "a redelivered first chunk over-counted pendingChunkedMessageCount (" + count
                        + ") vs chunkedMessagesMap.size() (" + mapSize + ")");

        // Only this single uuid is ever enqueued, so the queue size equals its occurrence count. After a redelivered
        // first chunk it must be exactly 1 (stale entry removed, re-added once) — not 2 as the old double-enqueue did.
        // (GrowableArrayBlockingQueue intentionally doesn't support iteration, so assert on size().)
        Assert.assertEquals(consumer.pendingChunkedMessageUuidQueue.size(), 1,
                "uuid should appear exactly once in pendingChunkedMessageUuidQueue but queue size was "
                        + consumer.pendingChunkedMessageUuidQueue.size());
    }

    /**
     * A completed chunked message leaves a "ghost" entry in {@code pendingChunkedMessageUuidQueue}: the finalize in
     * {@code messageReceived} removes the ctx from {@code chunkedMessagesMap} but not from the queue.
     * {@code removeExpireIncompleteChunkedMessages} must {@code poll} past such null-ctx heads (as
     * {@code removeOldestPendingChunkedMessage} already does) and keep going; otherwise a ghost at the head halts
     * expiry and a genuinely-expired incomplete chunk queued behind it is never cleaned (its {@code chunkedMsgBuffer}
     * leaks and its chunks are never acked).
     */
    @Test
    public void testExpiryDrainsPastGhostQueueEntries() throws Exception {
        // No max-pending eviction, so only the expiry path consumes the queue.
        consumerConf.setMaxPendingChunkedMessage(0);
        createConsumer(consumerConf);
        // Enable expiry but skip the lazy self-scheduling so the test doesn't need a scheduled-executor mock.
        consumer.expireTimeOfIncompleteChunkedMessageMillis = 1L;
        consumer.expireChunkMessageTaskScheduled.set(true);

        // uuid-A: first chunk of a 3-chunk message -> in-progress. Enqueued first (queue head).
        sendChunk("uuid-A", 0, 3);
        // uuid-B: first chunk of a 2-chunk message -> in-progress, enqueued after A. Never completed.
        sendChunk("uuid-B", 0, 2);

        // Simulate uuid-A completing exactly as the messageReceived finalize does: remove its ctx from the map and
        // recycle it, but leave its uuid in pendingChunkedMessageUuidQueue -> a ghost head entry. (Releasing the buffer
        // mirrors the assembled-payload release that completion performs.)
        ConsumerImpl.ChunkedMessageCtx ctxA = consumer.chunkedMessagesMap.remove("uuid-A");
        assertThat(ctxA).isNotNull();
        if (ctxA.chunkedMsgBuffer != null) {
            ctxA.chunkedMsgBuffer.release();
        }
        ctxA.recycle();

        // Make uuid-B eligible for expiry right away.
        ConsumerImpl.ChunkedMessageCtx ctxB = consumer.chunkedMessagesMap.get("uuid-B");
        assertThat(ctxB).isNotNull();
        ctxB.receivedTime = 0;
        ByteBuf bufB = ctxB.chunkedMsgBuffer;

        consumer.removeExpireIncompleteChunkedMessages();

        // With the bug, the ghost head (uuid-A) makes expiry return before reaching the expired uuid-B.
        assertThat(consumer.chunkedMessagesMap.containsKey("uuid-B"))
                .as("expired incomplete chunked message behind a ghost queue head must be cleaned")
                .isFalse();
        assertThat(bufB.refCnt()).as("expired chunk buffer must be released").isZero();
        assertThat(consumer.pendingChunkedMessageUuidQueue)
                .as("ghost and expired entries must both be drained from the queue").isEmpty();
    }

    /**
     * The "lost first chunk" / forward-gap discard path in {@code processMessageChunk} removes an in-progress ctx from
     * {@code chunkedMessagesMap}, but must also keep the other bookkeeping in sync: decrement
     * {@code pendingChunkedMessageCount} (the ctx was counted when its first chunk created it) and drop the uuid from
     * {@code pendingChunkedMessageUuidQueue} (otherwise a ghost lingers). Without the decrement the count drifts upward
     * and prematurely triggers {@code removeOldestPendingChunkedMessage}.
     */
    @Test
    public void testForwardGapDiscardKeepsCountAndQueueConsistent() throws Exception {
        // Disable the lazy expiry self-scheduling so this test doesn't need a scheduled-executor mock.
        consumer.expireTimeOfIncompleteChunkedMessageMillis = 0L;

        final String uuid = "uuid-gap";
        // First chunk (chunkId 0) of a 3-chunk message -> in-progress: count=1, map={uuid}, queue=[uuid].
        sendChunk(uuid, 0, 3);
        // A chunk that skips chunkId 1 (forward gap): chunkId 2 != lastChunkedMessageId(0)+1 and > lastChunkedMessageId
        // -> hits the "lost first chunk" discard path that removes the ctx from chunkedMessagesMap.
        sendChunk(uuid, 2, 3);

        int count = consumer.pendingChunkedMessageCount;
        int mapSize = consumer.chunkedMessagesMap.size();
        Assert.assertEquals(count, mapSize,
                "forward-gap discard over-counted pendingChunkedMessageCount (" + count
                        + ") vs chunkedMessagesMap.size() (" + mapSize + ")");

        Assert.assertEquals(consumer.pendingChunkedMessageUuidQueue.size(), mapSize,
                "discarded uuid must be removed from pendingChunkedMessageUuidQueue (no ghost), queue size was "
                        + consumer.pendingChunkedMessageUuidQueue.size());
    }

    @Test(invocationTimeOut = 1000)
    public void testNotifyPendingReceivedCallback_EmptyQueueNotThrowsException() {
        consumer.notifyPendingReceivedCallback(null, null);
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
