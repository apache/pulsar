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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
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
import org.apache.pulsar.client.api.ConsumerInterceptor;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.MessageListener;
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
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.Backoff;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
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
        createConsumer(consumerConf, topic);
    }

    private void createConsumer(ConsumerConfigurationData consumerConf, String topicName) {
        createConsumer(consumerConf, topicName, null);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void createConsumer(ConsumerConfigurationData consumerConf, String topicName,
                                ConsumerInterceptor<byte[]> interceptor) {
        cleanup();
        executorProvider = new ExecutorProvider(1, "ConsumerImplTest");
        internalExecutor = Executors.newSingleThreadScheduledExecutor();

        PulsarClientImpl client = ClientTestFixtures.createPulsarClientMock(executorProvider, internalExecutor);
        ClientConfigurationData clientConf = client.getConfiguration();
        clientConf.setOperationTimeoutMs(100);
        clientConf.setStatsIntervalSeconds(0);
        CompletableFuture<Consumer<byte[]>> subscribeFuture = new CompletableFuture<>();

        consumerConf.setSubscriptionName("test-sub");
        ConsumerInterceptors<byte[]> interceptors =
                interceptor == null ? null : new ConsumerInterceptors<>(Collections.singletonList(interceptor));
        consumer = ConsumerImpl.newConsumerImpl(client, topicName, consumerConf,
                executorProvider, -1, false, subscribeFuture, null, null, interceptors,
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
        sendChunk(uuid, chunkId, numChunks, chunkId);
    }

    /**
     * Delivers one chunk through {@code processMessageChunk} with an explicit entry id, so a test can redeliver a
     * chunk id under a different message id.
     */
    private void sendChunk(String uuid, int chunkId, int numChunks, long entryId) {
        MessageMetadata md = new MessageMetadata()
                .setProducerName("p").setSequenceId(0).setPublishTime(System.currentTimeMillis())
                .setUuid(uuid).setChunkId(chunkId).setNumChunksFromMsg(numChunks).setTotalChunkMsgSize(64);
        md.setCompression(CompressionType.NONE);
        MessageIdData idData = new MessageIdData().setLedgerId(1L).setEntryId(entryId);
        MessageIdImpl msgId = new MessageIdImpl(1L, entryId, -1);
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
        try {
            // Deliver the first chunk (chunkId == 0) twice for the SAME uuid and SAME (ledgerId, entryId): a
            // redelivered first chunk of a 2-chunk message. The message never completes, so only the ++/--
            // bookkeeping is exercised.
            for (int call = 0; call < 2; call++) {
                sendChunk(uuid, 0, 2);
            }

            int count = consumer.pendingChunkedMessageCount;
            int mapSize = consumer.chunkedMessagesMap.size();
            Assert.assertEquals(count, mapSize,
                    "a redelivered first chunk over-counted pendingChunkedMessageCount (" + count
                            + ") vs chunkedMessagesMap.size() (" + mapSize + ")");

            // Only this single uuid is ever enqueued, so the queue size equals its occurrence count. After a
            // redelivered first chunk it must be exactly 1 (stale entry removed, re-added once) — not 2 as the old
            // double-enqueue did. (GrowableArrayBlockingQueue intentionally doesn't support iteration, so assert on
            // size().)
            Assert.assertEquals(consumer.pendingChunkedMessageUuidQueue.size(), 1,
                    "uuid should appear exactly once in pendingChunkedMessageUuidQueue but queue size was "
                            + consumer.pendingChunkedMessageUuidQueue.size());
        } finally {
            // The message is never completed, so its partial buffer is still live: release it even if an assertion
            // above failed.
            releasePendingChunkedMessages();
        }
    }

    /**
     * Releases every partial chunked message still held by the consumer, for tests that intentionally leave one.
     */
    private void releasePendingChunkedMessages() {
        for (ConsumerImpl.ChunkedMessageCtx ctx : consumer.chunkedMessagesMap.values()) {
            if (ctx.chunkedMsgBuffer != null) {
                ctx.chunkedMsgBuffer.release();
            }
        }
        consumer.chunkedMessagesMap.clear();
    }

    /**
     * A uuid whose ctx is no longer in {@code chunkedMessagesMap} but still sits at the head of
     * {@code pendingChunkedMessageUuidQueue} is a "ghost" head. Every removal path now drops the queue entry along
     * with the map entry, so this is a defensive invariant of {@code removeExpireIncompleteChunkedMessages}: it must
     * drop such null-ctx heads and keep scanning (as {@code removeOldestPendingChunkedMessage} does), because a ghost
     * that stopped the scan would leave every genuinely-expired incomplete chunk behind it uncleaned (its
     * {@code chunkedMsgBuffer} leaked and its chunks never acked), which is what happened before this invariant was
     * enforced.
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

        // Plant a ghost head by hand: take uuid-A's ctx out of the map and recycle it while leaving its uuid in
        // pendingChunkedMessageUuidQueue. (Releasing the buffer mirrors the assembled-payload release that a real
        // completion performs.)
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

    /**
     * On a non-persistent topic the broker keeps nothing to replay, so an ack timeout can never lead to a
     * redelivery: {@code NonPersistentSubscription.redeliverUnacknowledgedMessages} is a no-op. Tracking
     * messages anyway is worse than useless, because acks do not clear the tracker either — the consumer
     * installs {@code NonPersistentAcknowledgmentGroupingTracker}, whose {@code addAcknowledgment} is a no-op,
     * and the tracker is only cleared from the persistent one. The tracker therefore fills up even for an
     * application that acks every message, and once it times out the consumer clears its receive queue,
     * destroying messages that nothing can replay.
     */
    @Test
    public void testMessagesAreNotTrackedForAckTimeoutOnNonPersistentTopic() {
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setAckTimeoutMillis(TimeUnit.SECONDS.toMillis(10));
        createConsumer(conf, "non-persistent://tenant/ns1/ack-timeout-topic");

        consumer.trackMessage(new MessageIdImpl(1L, 1L, -1), 0);

        Assert.assertTrue(consumer.getUnAckedMessageTracker().isEmpty(),
                "a message was tracked for ack timeout on a non-persistent topic, where an ack never clears"
                        + " the tracker and a redelivery can never happen");
    }

    /** The same configuration must keep working on a persistent topic, where redelivery is possible. */
    @Test
    public void testMessagesAreStillTrackedForAckTimeoutOnPersistentTopic() {
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setAckTimeoutMillis(TimeUnit.SECONDS.toMillis(10));
        createConsumer(conf, "persistent://tenant/ns1/ack-timeout-topic");

        consumer.trackMessage(new MessageIdImpl(1L, 1L, -1), 0);

        Assert.assertFalse(consumer.getUnAckedMessageTracker().isEmpty(),
                "messages are no longer tracked for ack timeout on a persistent topic");
    }

    /**
     * The listener dispatch path adds to the tracker itself, bypassing {@code trackMessage} entirely
     * ({@code trackUnAckedMsgIfNoListener} only adds when no listener is set). Consumers with a
     * {@code messageListener} are the common case, so this path must honour the same rule.
     */
    @Test
    public void testMessagesAreNotTrackedForAckTimeoutOnNonPersistentTopicWithListener() {
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setAckTimeoutMillis(TimeUnit.SECONDS.toMillis(10));
        conf.setMessageListener((MessageListener<byte[]>) (c, msg) -> { });
        createConsumer(conf, "non-persistent://tenant/ns1/ack-timeout-listener-topic");

        Assert.assertFalse(consumer.isAckTimeoutTrackingEnabled(),
                "ack timeout tracking is still enabled on a non-persistent topic, so the listener dispatch path"
                        + " would keep filling the tracker");
    }

    /** On a persistent topic the listener path must keep tracking. */
    @Test
    public void testMessagesAreStillTrackedForAckTimeoutOnPersistentTopicWithListener() {
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setAckTimeoutMillis(TimeUnit.SECONDS.toMillis(10));
        conf.setMessageListener((MessageListener<byte[]>) (c, msg) -> { });
        createConsumer(conf, "persistent://tenant/ns1/ack-timeout-listener-topic");

        Assert.assertTrue(consumer.isAckTimeoutTrackingEnabled(),
                "ack timeout tracking was disabled on a persistent topic");
    }

    /**
     * A chunk-ack interceptor that records every individually acked message id and can be paused on a latch, so a test
     * can hold an acknowledgment callback open and observe what the consumer does meanwhile.
     */
    private static class RecordingAckInterceptor implements ConsumerInterceptor<byte[]> {
        final List<MessageId> acked = new CopyOnWriteArrayList<>();
        final CountDownLatch ackStarted = new CountDownLatch(1);
        final CountDownLatch ackRelease = new CountDownLatch(1);
        volatile boolean pauseAcks;

        @Override
        public void close() {
        }

        @Override
        public Message<byte[]> beforeConsume(Consumer<byte[]> consumer, Message<byte[]> message) {
            return message;
        }

        @Override
        public void onAcknowledge(Consumer<byte[]> consumer, MessageId messageId, Throwable exception) {
            acked.add(messageId);
            if (pauseAcks) {
                ackStarted.countDown();
                try {
                    ackRelease.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public void onAcknowledgeCumulative(Consumer<byte[]> consumer, MessageId messageId, Throwable exception) {
        }

        @Override
        public void onNegativeAcksSend(Consumer<byte[]> consumer, Set<MessageId> messageIds) {
        }

        @Override
        public void onAckTimeoutSend(Consumer<byte[]> consumer, Set<MessageId> messageIds) {
        }
    }

    private static final String PERSISTENT_TOPIC = "persistent://tenant/ns1/my-topic";

    /**
     * Delivers one chunk through the real {@code messageReceived} entry point, framed as the broker would send it
     * (checksum, metadata, payload): chunk {@code chunkId} of a {@code numChunks}-chunk message carrying the given
     * still-compressed bytes. The entry id doubles as the chunk id.
     */
    private void deliverChunk(ClientCnx cnx, String uuid, int chunkId, int numChunks, int totalChunkMsgSize,
                              CompressionType compressionType, int uncompressedSize, byte[] chunkBytes) {
        MessageMetadata md = new MessageMetadata()
                .setProducerName("p").setSequenceId(0).setPublishTime(System.currentTimeMillis())
                .setUuid(uuid).setChunkId(chunkId).setNumChunksFromMsg(numChunks)
                .setTotalChunkMsgSize(totalChunkMsgSize)
                .setCompression(compressionType).setUncompressedSize(uncompressedSize);
        ByteBuf headersAndPayload = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, md,
                Unpooled.wrappedBuffer(chunkBytes));
        CommandMessage cmd = new CommandMessage().setConsumerId(consumer.consumerId);
        cmd.setMessageId().setLedgerId(1L).setEntryId(chunkId);
        try {
            consumer.messageReceived(cmd, headersAndPayload, cnx);
        } finally {
            headersAndPayload.release();
        }
    }

    private static byte[] zlibCompress(byte[] data) {
        ByteBuf source = Unpooled.wrappedBuffer(data);
        ByteBuf compressed = CompressionCodecProvider.getCompressionCodec(CompressionType.ZLIB).encode(source);
        try {
            byte[] bytes = new byte[compressed.readableBytes()];
            compressed.readBytes(bytes);
            return bytes;
        } finally {
            compressed.release();
            source.release();
        }
    }

    private static void assertNoPendingChunkedMessages(ConsumerImpl<?> consumer) {
        assertThat(consumer.chunkedMessagesMap).as("chunkedMessagesMap").isEmpty();
        assertThat(consumer.pendingChunkedMessageUuidQueue).as("pendingChunkedMessageUuidQueue").isEmpty();
        assertThat(consumer.pendingChunkedMessageCount).as("pendingChunkedMessageCount").isZero();
    }

    /**
     * The final chunk, delivered through {@code messageReceived}, transfers the assembled buffer out of the context
     * (removed and recycled under the lock) and decompresses it outside the lock. On success the message must reach
     * the receive queue with the original payload and a chunk message id, all bookkeeping must be empty, the assembled
     * buffer released, and the chunk ids registered with the unack tracker.
     */
    @Test(timeOut = 30000)
    public void testFinalChunkThroughMessageReceivedDeliversAssembledMessage() throws Exception {
        RecordingAckInterceptor interceptor = new RecordingAckInterceptor();
        createConsumer(consumerConf, PERSISTENT_TOPIC, interceptor);
        consumer.expireTimeOfIncompleteChunkedMessageMillis = 0L;
        ClientCnx cnx = ClientTestFixtures.mockClientCnx();

        byte[] original = new byte[4096];
        new Random(1).nextBytes(original);
        byte[] compressed = zlibCompress(original);
        int split = compressed.length / 2;
        byte[] chunk0 = Arrays.copyOfRange(compressed, 0, split);
        byte[] chunk1 = Arrays.copyOfRange(compressed, split, compressed.length);

        deliverChunk(cnx, "uuid-ok", 0, 2, compressed.length, CompressionType.ZLIB, original.length, chunk0);
        ByteBuf assembled = consumer.chunkedMessagesMap.get("uuid-ok").chunkedMsgBuffer;
        assertThat(assembled).isNotNull();
        deliverChunk(cnx, "uuid-ok", 1, 2, compressed.length, CompressionType.ZLIB, original.length, chunk1);

        Awaitility.await().untilAsserted(() -> assertThat(consumer.numMessagesInQueue()).isEqualTo(1));
        Message<byte[]> message = consumer.incomingMessages.poll();
        assertThat(message).isNotNull();
        try {
            assertThat(message.getData()).isEqualTo(original);
            assertThat(message.getMessageId()).isInstanceOf(ChunkMessageIdImpl.class);
            assertThat(consumer.unAckedChunkedMessageIdSequenceMap.containsKey(message.getMessageId()))
                    .as("chunk ids registered with the unack tracker").isTrue();
            assertThat(assembled.refCnt()).as("assembled buffer released after decompression").isZero();
            assertNoPendingChunkedMessages(consumer);
            assertThat(interceptor.acked).as("a successful completion acks nothing").isEmpty();
        } finally {
            message.release();
        }
    }

    @DataProvider
    public Object[][] decompressionFailures() {
        return new Object[][] {
                // ZLib reports corrupt input as an IOException
                {"corrupt-input"},
                // ZLib reports a decoded size that differs from the advertised one as an IllegalArgumentException
                {"size-mismatch"},
        };
    }

    /**
     * When the final chunk's decompression fails, the assembled buffer has already been detached from its context, so
     * nothing but this path can release it or dispose of the earlier chunks' message ids. Whether the codec fails with
     * a checked or an unchecked exception, the buffer must be released, the earlier chunk ids individually acked, the
     * final chunk discarded, all bookkeeping left empty, and nothing must escape {@code messageReceived}.
     */
    @Test(dataProvider = "decompressionFailures", timeOut = 30000)
    public void testFinalChunkDecompressionFailureThroughMessageReceived(String failure) throws Exception {
        RecordingAckInterceptor interceptor = new RecordingAckInterceptor();
        createConsumer(consumerConf, PERSISTENT_TOPIC, interceptor);
        consumer.expireTimeOfIncompleteChunkedMessageMillis = 0L;
        ClientCnx cnx = ClientTestFixtures.mockClientCnx();

        byte[] original = new byte[4096];
        new Random(2).nextBytes(original);
        byte[] compressed = zlibCompress(original);
        int uncompressedSize = original.length;
        if ("corrupt-input".equals(failure)) {
            // keep the stream header, corrupt the deflate body
            for (int i = 4; i < compressed.length; i++) {
                compressed[i] = (byte) ~compressed[i];
            }
        } else {
            uncompressedSize = original.length + 1;
        }
        int split = compressed.length / 2;
        byte[] chunk0 = Arrays.copyOfRange(compressed, 0, split);
        byte[] chunk1 = Arrays.copyOfRange(compressed, split, compressed.length);

        deliverChunk(cnx, "uuid-bad", 0, 2, compressed.length, CompressionType.ZLIB, uncompressedSize, chunk0);
        ByteBuf assembled = consumer.chunkedMessagesMap.get("uuid-bad").chunkedMsgBuffer;
        assertThat(assembled).isNotNull();
        deliverChunk(cnx, "uuid-bad", 1, 2, compressed.length, CompressionType.ZLIB, uncompressedSize, chunk1);

        assertThat(assembled.refCnt()).as("detached assembled buffer released on decompression failure").isZero();
        assertNoPendingChunkedMessages(consumer);
        assertThat(consumer.numMessagesInQueue()).as("no message delivered").isZero();
        assertThat(consumer.unAckedChunkedMessageIdSequenceMap).as("nothing registered with the unack tracker")
                .isEmpty();
        // The earlier chunk (entry 0) is acked individually; the final chunk (entry 1) is discarded with a validation
        // error straight on the connection, so it is not routed through the interceptor. Two chunks are far below the
        // flow-control threshold, so that discard is the only write on the connection.
        assertThat(interceptor.acked).containsExactly(new MessageIdImpl(1L, 0L, -1));
        verify(cnx.ctx(), times(1)).writeAndFlush(any(), any());
    }

    /**
     * Acks decided under {@code chunkedMessageLock} are issued after it is released. The persistent acknowledgments
     * grouping tracker runs the consumer's acknowledgment interceptors inline, so while the expiry thread is held in
     * an application interceptor, a chunk arriving on the IO thread must still be processed rather than block on the
     * lock behind that callback.
     */
    @Test(timeOut = 30000)
    public void testChunkReceiveProceedsWhileExpiryAckInterceptorIsBlocked() throws Exception {
        RecordingAckInterceptor interceptor = new RecordingAckInterceptor();
        consumerConf.setMaxPendingChunkedMessage(0);
        createConsumer(consumerConf, PERSISTENT_TOPIC, interceptor);
        // Enable expiry but skip the lazy self-scheduling so the test doesn't need a scheduled-executor mock.
        consumer.expireTimeOfIncompleteChunkedMessageMillis = 1L;
        consumer.expireChunkMessageTaskScheduled.set(true);

        sendChunk("uuid-expired", 0, 3);
        consumer.chunkedMessagesMap.get("uuid-expired").receivedTime = 0;
        interceptor.pauseAcks = true;

        Thread expirer = new Thread(consumer::removeExpireIncompleteChunkedMessages, "expirer-pinned");
        expirer.start();
        try {
            assertThat(interceptor.ackStarted.await(10, TimeUnit.SECONDS))
                    .as("expiry reached the acknowledgment interceptor").isTrue();

            // The expiry thread is now parked inside the application callback. A chunk for another uuid must still
            // be processed on the "IO thread".
            CountDownLatch received = new CountDownLatch(1);
            Thread receiver = new Thread(() -> {
                sendChunk("uuid-live", 0, 3);
                received.countDown();
            }, "receiver-netty-sim");
            receiver.start();
            try {
                assertThat(received.await(5, TimeUnit.SECONDS))
                        .as("chunk reception must not wait for the acknowledgment interceptor to return").isTrue();
            } finally {
                // never leave the receiver parked on the lock if the assertion fails
                interceptor.ackRelease.countDown();
                receiver.join();
            }
            assertThat(consumer.chunkedMessagesMap).containsKey("uuid-live");
            assertThat(consumer.chunkedMessagesMap).doesNotContainKey("uuid-expired");
        } finally {
            interceptor.ackRelease.countDown();
            expirer.join();
            releasePendingChunkedMessages();
        }
        assertThat(interceptor.acked).containsExactly(new MessageIdImpl(1L, 0L, -1));
    }

    /**
     * A first chunk arriving under a new message id for a uuid that still has an in-progress context means the earlier
     * chunks belong to a corrupted chunked message that will never be delivered. Replacing the context must ack those
     * earlier chunk ids, through the deferred-ack path, and leave the count and queue consistent with the map.
     */
    @Test(timeOut = 30000)
    public void testReplacedCorruptedChunkedMessageAcksItsEarlierChunks() throws Exception {
        RecordingAckInterceptor interceptor = new RecordingAckInterceptor();
        createConsumer(consumerConf, PERSISTENT_TOPIC, interceptor);
        consumer.expireTimeOfIncompleteChunkedMessageMillis = 0L;

        try {
            // entries 10 and 11: chunks 0 and 1 of a corrupted 3-chunk message that never completes
            sendChunk("uuid-corrupt", 0, 3, 10L);
            sendChunk("uuid-corrupt", 1, 3, 11L);
            ByteBuf corruptedBuffer = consumer.chunkedMessagesMap.get("uuid-corrupt").chunkedMsgBuffer;
            // entry 20: chunk 0 of the re-published message under the same uuid
            sendChunk("uuid-corrupt", 0, 3, 20L);

            assertThat(interceptor.acked).as("earlier chunks of the replaced message are acked")
                    .containsExactly(new MessageIdImpl(1L, 10L, -1), new MessageIdImpl(1L, 11L, -1));
            assertThat(corruptedBuffer.refCnt()).as("replaced buffer released").isZero();
            assertThat(consumer.chunkedMessagesMap).containsOnlyKeys("uuid-corrupt");
            assertThat(consumer.pendingChunkedMessageCount).isEqualTo(1);
            assertThat(consumer.pendingChunkedMessageUuidQueue.size()).isEqualTo(1);
        } finally {
            releasePendingChunkedMessages();
        }
    }

    /**
     * Exceeding {@code maxPendingChunkedMessage} evicts the oldest in-progress chunked message on the receive path.
     * With {@code autoAckOldestChunkedMessageOnQueueFull} its chunk ids are acked, through the deferred-ack path, and
     * the eviction keeps the count and queue consistent with the map.
     */
    @Test(timeOut = 30000)
    public void testEvictingOldestPendingChunkedMessageAcksItsChunks() throws Exception {
        RecordingAckInterceptor interceptor = new RecordingAckInterceptor();
        consumerConf.setMaxPendingChunkedMessage(1);
        consumerConf.setAutoAckOldestChunkedMessageOnQueueFull(true);
        createConsumer(consumerConf, PERSISTENT_TOPIC, interceptor);
        consumer.expireTimeOfIncompleteChunkedMessageMillis = 0L;

        try {
            sendChunk("uuid-old", 0, 3, 10L);
            ByteBuf oldBuffer = consumer.chunkedMessagesMap.get("uuid-old").chunkedMsgBuffer;
            // a second in-progress message exceeds maxPendingChunkedMessage = 1 and evicts the oldest
            sendChunk("uuid-new", 0, 3, 20L);

            assertThat(interceptor.acked).as("evicted chunks are acked")
                    .containsExactly(new MessageIdImpl(1L, 10L, -1));
            assertThat(oldBuffer.refCnt()).as("evicted buffer released").isZero();
            assertThat(consumer.chunkedMessagesMap).containsOnlyKeys("uuid-new");
            assertThat(consumer.pendingChunkedMessageCount).isEqualTo(1);
            assertThat(consumer.pendingChunkedMessageUuidQueue.size()).isEqualTo(1);
        } finally {
            releasePendingChunkedMessages();
        }
    }
}
