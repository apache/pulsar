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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.InvalidMessageException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.client.impl.MessageImpl.SchemaState;
import org.apache.pulsar.client.impl.ProducerImpl.OpSendMsg;
import org.apache.pulsar.client.impl.metrics.LatencyHistogram;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
@CustomLog
public class MessageChunkingTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "ackReceiptEnabledWithMaxMessageSize")
    public Object[][] ackReceiptEnabledWithMaxMessageSize() {
        return new Object[][] { { true, true }, { true, false }, { false, true }, { false, false } };
    }

    @DataProvider(name = "ackReceiptEnabled")
    public Object[][] ackReceiptEnabled() {
        return new Object[][] { { true }, { false } };
    }

    @Test(dataProvider = "ackReceiptEnabledWithMaxMessageSize")
    public void testLargeMessage(boolean ackReceiptEnabled, boolean clientSizeMaxMessageSize) throws Exception {

        log.info().attr("method", methodName).log("Starting test");
        if (clientSizeMaxMessageSize) {
            this.conf.setMaxMessageSize(35);
        } else {
            this.conf.setMaxMessageSize(50);
        }
        final int totalMessages = 5;
        final String topicName = "persistent://my-property/my-ns/my-topic1";

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .isAckReceiptEnabled(ackReceiptEnabled)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName);
        if (clientSizeMaxMessageSize) {
            producerBuilder.chunkMaxMessageSize(35);
        }

        Producer<byte[]> producer = producerBuilder.compressionType(CompressionType.LZ4).enableChunking(true)
                .enableBatching(false).create();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();

        List<String> publishedMessages = new ArrayList<>();
        for (int i = 0; i < totalMessages; i++) {
            String message = createMessagePayload(i * 100);
            publishedMessages.add(message);
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = new HashSet<>();
        List<MessageId> msgIds = new ArrayList<>();
        for (int i = 0; i < totalMessages; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.info()
                    .attr("i", i)
                    .attr("published", publishedMessages.get(i))
                    .attr("message", receivedMessage)
                    .log("Published Received message");
            String expectedMessage = publishedMessages.get(i);
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            msgIds.add(msg.getMessageId());
        }

        pulsar.getBrokerService().updateRates();

        PublisherStats producerStats = topic.getStats(false, false, false).getPublishers().get(0);

        assertTrue(producerStats.getChunkedMessageRate() > 0);

        ManagedCursorImpl mcursor = (ManagedCursorImpl) topic.getManagedLedger().getCursors().iterator().next();
        Position readPosition = mcursor.getReadPosition();

        for (MessageId msgId : msgIds) {
            consumer.acknowledge(msgId);
        }

        retryStrategically((test) -> {
            return mcursor.getMarkDeletedPosition().getNext().equals(readPosition);
        }, 5, 200);

        assertEquals(readPosition, mcursor.getMarkDeletedPosition().getNext());

        assertEquals(readPosition.getEntryId(), ((ConsumerImpl) consumer).getAvailablePermits());

        consumer.close();
        producer.close();
        log.info().attr("method", methodName).log("Exiting test");

    }
    @SuppressWarnings("deprecation")

    @Test
    public void testChunkingWithOrderingKey() throws Exception {
        this.conf.setMaxMessageSize(100);

        final String topicName = "persistent://my-property/my-ns/testChunkingWithOrderingKey";

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).enableChunking(true)
                .enableBatching(false).create();

        byte[] data = RandomUtils.nextBytes(200);
        byte[] ok = RandomUtils.nextBytes(50);
        producer.newMessage().value(data).orderingKey(ok).send();

        Message<byte[]> msg = consumer.receive();
        Assert.assertEquals(msg.getData(), data);
        Assert.assertEquals(msg.getOrderingKey(), ok);
    }

    @Test(dataProvider = "ackReceiptEnabled")
    public void testLargeMessageAckTimeOut(boolean ackReceiptEnabled) throws Exception {

        log.info().attr("method", methodName).log("Starting test");
        this.conf.setMaxMessageSize(50);
        final int totalMessages = 5;
        final String topicName = "persistent://my-property/my-ns/my-topic1";

        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-subscriber-name").acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .isAckReceiptEnabled(ackReceiptEnabled)
                .ackTimeout(5, TimeUnit.SECONDS).subscribe();

        Reader<byte[]> reader = pulsarClient.newReader().topic(topicName).startMessageId(MessageId.earliest).create();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName);

        Producer<byte[]> producer = producerBuilder.enableChunking(true).enableBatching(false).create();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();

        List<String> publishedMessages = new ArrayList<>();
        for (int i = 0; i < totalMessages; i++) {
            String message = createMessagePayload(i * 100);
            publishedMessages.add(message);
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = new HashSet<>();
        for (int i = 0; i < totalMessages; i++) {
            msg = reader.readNext(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.info().attr("message", receivedMessage).log("Received message");
            String expectedMessage = publishedMessages.get(i);
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        messageSet.clear();
        for (int i = 0; i < totalMessages; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.info().attr("message", receivedMessage).log("Received message");
            String expectedMessage = publishedMessages.get(i);
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        retryStrategically((test) -> consumer.getUnAckedMessageTracker().messageIdPartitionMap.isEmpty(), 10,
                TimeUnit.SECONDS.toMillis(1));

        msg = null;
        messageSet.clear();
        MessageId lastMsgId = null;
        for (int i = 0; i < totalMessages; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            lastMsgId = msg.getMessageId();
            String receivedMessage = new String(msg.getData());
            log.info().attr("message", receivedMessage).log("Received message");
            String expectedMessage = publishedMessages.get(i);
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        ManagedCursorImpl mcursor = (ManagedCursorImpl) topic.getManagedLedger().getCursors().iterator().next();
        Position readPosition = mcursor.getReadPosition();

        consumer.acknowledgeCumulative(lastMsgId);

        retryStrategically((test) -> {
            return mcursor.getMarkDeletedPosition().getNext().equals(readPosition);
        }, 5, 200);

        assertEquals(readPosition, mcursor.getMarkDeletedPosition().getNext());

        consumer.close();
        producer.close();
        reader.close();
        log.info().attr("method", methodName).log("Exiting test");

    }

    @Test
    public void testPublishWithFailure() throws Exception {
        log.info().attr("method", methodName).log("Starting test");
        this.conf.setMaxMessageSize(50);
        final String topicName = "persistent://my-property/my-ns/my-topic1";

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName);

        Producer<byte[]> producer = producerBuilder.enableChunking(true).enableBatching(false)
                .create();

        stopBroker();

        try {
            producer.send(createMessagePayload(1000).getBytes());
            fail("should have failed with timeout exception");
        } catch (PulsarClientException.TimeoutException e) {
            // Ok
        }
        producer.close();
    }

    private void sendSingleChunk(Producer<String> producer, String uuid, int chunkId, int totalChunks)
            throws PulsarClientException {
        TypedMessageBuilderImpl<String> msg = (TypedMessageBuilderImpl<String>) producer.newMessage()
                .value(String.format("chunk-%s-%d|", uuid, chunkId));
        MessageMetadata msgMetadata = msg.getMetadataBuilder();
        msgMetadata.setUuid(uuid)
                .setChunkId(chunkId)
                .setNumChunksFromMsg(totalChunks)
                .setTotalChunkMsgSize(100);
        msg.send();
    }

    /**
     * Verifies that expiry of incomplete chunked messages still works after an earlier chunked
     * message has completed.
     *
     * removeExpireIncompleteChunkedMessages() only peek()s the head of pendingChunkedMessageUuidQueue
     * and bails out (else -> return) the moment the head uuid is no longer present in
     * chunkedMessagesMap. On the original client the completion path never removed a completed uuid
     * from the queue, so the first completed message left a permanent "ghost" uuid at the head. From
     * then on every expiry run saw that ghost, took the else branch and returned without ever
     * inspecting the genuinely-incomplete uuids behind it: expiry was dead for the rest of the
     * consumer's life (an unbounded leak of incomplete contexts).
     *
     * Here we complete one chunked message, then leave a second one incomplete. With the queue kept
     * in sync on completion the incomplete message is expired and chunkedMessagesMap drains to empty;
     * without it, the completed ghost blocks expiry and the incomplete context is never removed.
     */
    @Test
    public void testExpiryWorksAfterAnEarlierChunkedMessageCompletes() throws Exception {
        final String topicName = "persistent://my-property/my-ns/expiryAfterComplete";
        final String subName = "my-sub";
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .expireTimeOfIncompleteChunkedMessage(1, TimeUnit.SECONDS)
                .subscribe();
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .chunkMaxMessageSize(100)
                .enableChunking(true)
                .enableBatching(false)
                .create();

        ConsumerImpl<String> consumerImpl = (ConsumerImpl<String>) consumer;

        // 1. A complete chunked message. Receiving + acking it leaves its uuid as the head of
        //    pendingChunkedMessageUuidQueue unless completion keeps the queue in sync.
        sendSingleChunk(producer, "done", 0, 2);
        sendSingleChunk(producer, "done", 1, 2);
        Message<String> done = consumer.receive(5, TimeUnit.SECONDS);
        assertNotNull(done);
        assertEquals(done.getValue(), "chunk-done-0|chunk-done-1|");
        consumer.acknowledge(done);

        // 2. A second message left incomplete (only its first chunk arrives). Wait for the first
        //    chunk to be received and its assembly context to be registered before asserting expiry.
        sendSingleChunk(producer, "stuck", 0, 2);
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals(consumerImpl.chunkedMessagesMap.size(), 1));

        // 3. Past the expiry window, the incomplete "stuck" context must be discarded. If the
        //    completed "done" uuid still sits at the queue head, expiry bails and this never happens.
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals(consumerImpl.chunkedMessagesMap.size(), 0,
                        "expiry did not run: a completed message's uuid is blocking the queue head"));
    }

    /**
     * Verifies that expiry still works when the queue head is a stale uuid left behind
     *
     * removeExpireIncompleteChunkedMessages() must poll past that ghost head rather than returning on
     * it, otherwise an incomplete message queued behind the ghost would never expire.
     */
    @Test
    public void testExpirySkipsStaleQueueHeadFromDiscardPath() throws Exception {
        final String topicName = "persistent://my-property/my-ns/expirySkipsDiscardGhost";
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("my-sub")
                .expireTimeOfIncompleteChunkedMessage(1, TimeUnit.SECONDS)
                .subscribe();
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .chunkMaxMessageSize(100)
                .enableChunking(true)
                .enableBatching(false)
                .create();

        ConsumerImpl<String> consumerImpl = (ConsumerImpl<String>) consumer;

        // "ghost": chunk 0 arrives (queues the uuid), then chunk 2 of 3 arrives non-contiguously.
        sendSingleChunk(producer, "ghost", 0, 3);
        sendSingleChunk(producer, "ghost", 2, 3);
        // "stuck": a genuinely incomplete message queued behind the ghost head.
        sendSingleChunk(producer, "stuck", 0, 2);
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertNotNull(consumerImpl.chunkedMessagesMap.get("stuck")));

        // Past the expiry window, "stuck" must be collected. If the expiry loop returns on the ghost
        // head instead of polling past it, "stuck" is never reached and stays in the map forever.
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertNull(consumerImpl.chunkedMessagesMap.get("stuck"),
                        "expiry did not run: a stale discard-path uuid is blocking the queue head"));
    }

    /**
     * Chunks are processed on the connection's IO thread while the expiry task runs on another thread.
     * With queue [A, B], A's completion removes A's context and then A's uuid. If expiry could inspect A in
     * between (no context, so it skips A as a stale head) and only remove it after completion already did,
     * it might drop whatever is now at the head: dropping B would leave B's context in chunkedMessagesMap
     * with nothing in the queue referencing it, so it could never expire or be evicted.
     *
     * Completion and expiry both hold chunkedMessageLock, so expiry must not observe that intermediate
     * state at all. The test wraps chunkedMessagesMap: A's completion (map.remove("A") on the IO thread,
     * under the lock) starts expiry and waits until expiry either blocks or inspects A (map.get("A")),
     * and inspecting A is a failure. Either way B must stay pending and reachable for expiry.
     */
    @Test
    public void testExpiryStaleHeadSkipDoesNotDropConcurrentLiveEntry() throws Exception {
        final String topicName = "persistent://my-property/my-ns/expiryStaleHeadRace";
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("my-sub")
                // Expiry enabled, but long enough that the scheduled task never runs during the test;
                // it is driven manually below.
                .expireTimeOfIncompleteChunkedMessage(1, TimeUnit.HOURS)
                .subscribe();
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .chunkMaxMessageSize(100)
                .enableChunking(true)
                .enableBatching(false)
                .create();

        ConsumerImpl<String> consumerImpl = (ConsumerImpl<String>) consumer;

        // Queue [A, B], both incomplete.
        sendSingleChunk(producer, "A", 0, 2);
        sendSingleChunk(producer, "B", 0, 2);
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(consumerImpl.chunkedMessagesMap.size(), 2);
            assertEquals(consumerImpl.getPendingChunkedMessageUuidQueueSizeForTest(), 2);
        });

        final CountDownLatch expiryInspectedA = new CountDownLatch(1);
        final AtomicBoolean raceTriggered = new AtomicBoolean();
        final AtomicBoolean expiryInspectedPartialRemoval = new AtomicBoolean();
        final AtomicReference<Thread> expiryThread = new AtomicReference<>();
        final AtomicReference<Throwable> expiryError = new AtomicReference<>();
        Map<String, ConsumerImpl.ChunkedMessageCtx> hookedMap =
                new ConcurrentHashMap<String, ConsumerImpl.ChunkedMessageCtx>() {
            @Override
            public ConsumerImpl.ChunkedMessageCtx remove(Object key) {
                ConsumerImpl.ChunkedMessageCtx removed = super.remove(key);
                // A's completion on the IO thread: its context is gone but its uuid is still queued.
                // Run expiry now, and hold completion until expiry has inspected A.
                if ("A".equals(key) && raceTriggered.compareAndSet(false, true)) {
                    Thread t = new Thread(() -> {
                        try {
                            consumerImpl.removeExpireIncompleteChunkedMessages();
                        } catch (Throwable e) {
                            expiryError.set(e);
                        }
                    }, "test-chunk-expiry");
                    expiryThread.set(t);
                    t.start();
                    // Completion holds chunkedMessageLock here, so expiry has to block on it instead of
                    // inspecting A between A's map removal and its uuid removal.
                    Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                            t.getState() == Thread.State.BLOCKED || expiryInspectedA.getCount() == 0);
                    expiryInspectedPartialRemoval.set(expiryInspectedA.getCount() == 0);
                }
                return removed;
            }

            @Override
            public ConsumerImpl.ChunkedMessageCtx get(Object key) {
                ConsumerImpl.ChunkedMessageCtx ctx = super.get(key);
                // Expiry has inspected A and found no context: pause it until completion has removed
                // A's uuid from the queue, then let it act on what it observed.
                if ("A".equals(key) && Thread.currentThread() == expiryThread.get()) {
                    expiryInspectedA.countDown();
                    Awaitility.await().atMost(10, TimeUnit.SECONDS)
                            .until(() -> consumerImpl.getPendingChunkedMessageUuidQueueSizeForTest() == 1);
                }
                return ctx;
            }
        };
        hookedMap.putAll(consumerImpl.chunkedMessagesMap);
        consumerImpl.chunkedMessagesMap = hookedMap;

        try {
            // Complete A.
            sendSingleChunk(producer, "A", 1, 2);
            Message<String> a = consumer.receive(10, TimeUnit.SECONDS);
            assertNotNull(a);
            assertEquals(a.getValue(), "chunk-A-0|chunk-A-1|");

            assertTrue(raceTriggered.get(), "A's completion did not trigger the interleaving");
            assertFalse(expiryInspectedPartialRemoval.get(),
                    "expiry inspected A after its context was removed but before its uuid was");
            expiryThread.get().join(TimeUnit.SECONDS.toMillis(10));
            assertNull(expiryError.get());

            // B is still pending, so the queue must still reference it for expiry/eviction to find it.
            ConsumerImpl.ChunkedMessageCtx b = consumerImpl.chunkedMessagesMap.get("B");
            assertNotNull(b);
            assertEquals(consumerImpl.getPendingChunkedMessageUuidQueueSizeForTest(), 1,
                    "B's uuid was dropped from the queue while its context is still pending");

            // Expire B explicitly: this proves it is still reachable for cleanup and releases its
            // incomplete chunk buffer (closing the consumer does not release incomplete contexts).
            ByteBuf bBuffer = b.chunkedMsgBuffer;
            b.receivedTime = 0;
            consumerImpl.removeExpireIncompleteChunkedMessages();
            assertNull(consumerImpl.chunkedMessagesMap.get("B"));
            assertEquals(consumerImpl.getPendingChunkedMessageUuidQueueSizeForTest(), 0);
            assertEquals(bBuffer.refCnt(), 0);
        } finally {
            // On failure paths, stop the helper thread first, then release whatever is still pending
            // so the test does not leak chunk buffers.
            Thread t = expiryThread.get();
            if (t != null) {
                t.join(TimeUnit.SECONDS.toMillis(15));
            }
            for (ConsumerImpl.ChunkedMessageCtx ctx : consumerImpl.chunkedMessagesMap.values()) {
                if (ctx.chunkedMsgBuffer != null && ctx.chunkedMsgBuffer.refCnt() > 0) {
                    ctx.chunkedMsgBuffer.release();
                }
            }
            consumerImpl.chunkedMessagesMap.clear();
        }
    }

    /**
     * Completing a chunked message that is not at the head of pendingChunkedMessageUuidQueue
     * (A0, B0, B1, with A left incomplete) must remove only that message's uuid, leaving the
     * incomplete head A queued and still subject to expiry.
     */
    @Test
    public void testNonHeadCompletionKeepsIncompleteHeadExpirable() throws Exception {
        final String topicName = "persistent://my-property/my-ns/nonHeadCompletion";
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("my-sub")
                .expireTimeOfIncompleteChunkedMessage(5, TimeUnit.SECONDS)
                .subscribe();
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .chunkMaxMessageSize(100)
                .enableChunking(true)
                .enableBatching(false)
                .create();

        ConsumerImpl<String> consumerImpl = (ConsumerImpl<String>) consumer;

        sendSingleChunk(producer, "A", 0, 2);
        sendSingleChunk(producer, "B", 0, 2);
        sendSingleChunk(producer, "B", 1, 2);

        Message<String> b = consumer.receive(10, TimeUnit.SECONDS);
        assertNotNull(b);
        assertEquals(b.getValue(), "chunk-B-0|chunk-B-1|");

        // Only the incomplete A remains, in the map and in the queue.
        assertNotNull(consumerImpl.chunkedMessagesMap.get("A"));
        assertNull(consumerImpl.chunkedMessagesMap.get("B"));
        assertEquals(consumerImpl.getPendingChunkedMessageUuidQueueSizeForTest(), 1);

        // A is still expired once past the expiry window.
        Awaitility.await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertNull(consumerImpl.chunkedMessagesMap.get("A"));
            assertEquals(consumerImpl.getPendingChunkedMessageUuidQueueSizeForTest(), 0);
        });
    }

    /**
     * This test used to test the consumer configuration of maxPendingChunkedMessage.
     * If we set maxPendingChunkedMessage is 1 that means only one incomplete chunk message can be store in this
     * consumer.
     * For example:
     * ChunkMessage1 chunk-1: uuid = 0, chunkId = 0, totalChunk = 2;
     * ChunkMessage2 chunk-1: uuid = 1, chunkId = 0, totalChunk = 2;
     * ChunkMessage2 chunk-2: uuid = 1, chunkId = 1, totalChunk = 2;
     * ChunkMessage1 chunk-2: uuid = 0, chunkId = 1, totalChunk = 2;
     * The chunk-1 in the ChunkMessage1 and ChunkMessage2 all is incomplete.
     * chunk-1 in the ChunkMessage1 will be discarded and acked when receive the chunk-1 in the ChunkMessage2.
     * If ack ChunkMessage2 and redeliver unacknowledged messages, the consumer can not receive any message again.
     * @throws Exception
     */
    @Test
    public void testMaxPendingChunkMessages() throws Exception {
        log.info().attr("method", methodName).log("Starting test");
        final String topicName = "persistent://my-property/my-ns/maxPending";
        final String subName = "my-subscriber-name";
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .maxPendingChunkedMessage(1)
                .autoAckOldestChunkedMessageOnQueueFull(true)
                .subscribe();
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .chunkMaxMessageSize(100)
                .enableChunking(true)
                .enableBatching(false)
                .create();

        sendSingleChunk(producer, "0", 0, 2);
        sendSingleChunk(producer, "1", 0, 2);
        sendSingleChunk(producer, "1", 1, 2);

        // The chunked message of uuid 0 is discarded.
        Message<String> receivedMsg = consumer.receive(5, TimeUnit.SECONDS);
        assertEquals(receivedMsg.getValue(), "chunk-1-0|chunk-1-1|");

        consumer.acknowledge(receivedMsg);
        Awaitility.await().untilAsserted(() -> assertEquals(admin.topics().getStats(topicName)
                .getSubscriptions().get(subName).getNonContiguousDeletedMessagesRanges(), 0));
        consumer.redeliverUnacknowledgedMessages();

        sendSingleChunk(producer, "0", 1, 2);

        // Ensure that the chunked message of uuid 0 is discarded.
        assertNull(consumer.receive(5, TimeUnit.SECONDS));
    }

    /**
     * Verifies that discarding an orphaned last chunk does not leak a flow-control permit, by
     * asserting the user-visible symptom: dispatch must not stall.
     *
     * Each chunk the broker dispatches consumes one permit. Non-last chunks are credited back at
     * arrival; the last chunk is normally credited when the assembled message is consumed. When a
     * chunked message is torn apart (its first chunk expires, then the orphaned last chunk arrives
     * with no assembly context), the last chunk hits the discard branch. Without the fix that branch
     * never returned the permit, so every torn message leaked one permit.
     *
     * The consumer only sends fresh permits to the broker once its returned-permit accumulator
     * reaches receiverQueueSize/2. With a small receiver queue, leaking more than receiverQueueSize/2
     * permits means that threshold is never reached again, the broker's credit drains to zero, and
     * dispatch stalls permanently. We reproduce exactly that: tear more than receiverQueueSize/2
     * messages, then send a normal complete chunked message and assert it is still delivered.
     *
     * Without the fix the final message never arrives (stall). With the fix it is received.
     */
    @Test
    public void testOrphanedLastChunkDoesNotStallDispatch() throws Exception {
        final String topicName = "persistent://my-property/my-ns/orphanChunkPermitLeak";
        final String subName = "my-sub";
        final int receiverQueueSize = 4; // flush threshold = receiverQueueSize/2 = 2
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .receiverQueueSize(receiverQueueSize)
                .expireTimeOfIncompleteChunkedMessage(1, TimeUnit.SECONDS)
                .subscribe();
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .chunkMaxMessageSize(100)
                .enableChunking(true)
                .enableBatching(false)
                .create();

        // Tear apart well more than receiverQueueSize/2 chunked messages. Each torn message leaks
        // one permit on the buggy client; once cumulative leaks exceed receiverQueueSize the broker
        // credit is exhausted and never replenished.
        final int tornMessages = receiverQueueSize * 3; // 12
        for (int i = 0; i < tornMessages; i++) {
            // first (non-last) chunk -> creates an incomplete context
            sendSingleChunk(producer, "orphan-" + i, 0, 2);
            // wait for the scheduled expiry to discard it
            final int idx = i;
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertEquals(
                            ((ConsumerImpl<String>) consumer).chunkedMessagesMap.size(), 0));
            // orphaned last chunk -> discard branch (must return its permit)
            sendSingleChunk(producer, "orphan-" + idx, 1, 2);
        }

        // Now send a NORMAL complete chunked message. On a healthy consumer it is dispatched and
        // received; on the buggy client the leaked permits have stalled dispatch and it never arrives.
        sendSingleChunk(producer, "live", 0, 2);
        sendSingleChunk(producer, "live", 1, 2);

        Message<String> msg = consumer.receive(15, TimeUnit.SECONDS);
        assertNotNull(msg, "dispatch stalled: orphaned last chunks leaked flow-control permits until "
                + "the broker stopped dispatching (receiverQueueSize=" + receiverQueueSize + ")");
        assertEquals(msg.getValue(), "chunk-live-0|chunk-live-1|");
        consumer.acknowledge(msg);
    }

    /**
     * A non-persistent topic cannot carry chunked messages: the slicing block in
     * {@code ProducerImpl.serializeAndSendMessage} is skipped for them, but {@code totalChunks} was computed
     * from the payload size alone, so the send loop still ran once per "chunk" — publishing the whole payload
     * that many times, and releasing the payload buffer more often than it was retained.
     */
    @Test
    public void testLargeMessageOnNonPersistentTopicIsSentOnceWithoutChunking() throws Exception {
        final String topicName = "non-persistent://my-property/my-ns/testNonPersistentChunking";
        final String subName = "my-subscriber-name";

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                // small enough that the payload below would otherwise be split into several chunks
                .chunkMaxMessageSize(100)
                .enableChunking(true)
                .enableBatching(false)
                .create();

        String payload = "a".repeat(500);
        producer.newMessage().value(payload).send();

        Message<String> received = consumer.receive(10, TimeUnit.SECONDS);
        assertNotNull(received, "the message was not delivered");
        assertEquals(received.getValue(), payload);

        // Without the fix the same payload is published once per computed chunk, so more messages follow.
        assertNull(consumer.receive(3, TimeUnit.SECONDS),
                "the payload was published more than once on a non-persistent topic");
    }

    /**
     * The chunking flag is inert on a non-persistent topic, so the message-size checks must behave as if it were
     * off: a payload above the broker limit is rejected locally with {@link InvalidMessageException} instead of
     * being sent to the broker as one oversized frame.
     */
    @Test
    public void testLargeMessageOnNonPersistentTopicAboveBrokerLimitIsRejectedLocally() throws Exception {
        final int maxMessageSize = 1024;
        this.conf.setMaxMessageSize(maxMessageSize);
        final String topicName = "non-persistent://my-property/my-ns/testNonPersistentChunkingAboveLimit";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .enableChunking(true)
                .enableBatching(false)
                .sendTimeout(5, TimeUnit.SECONDS)
                .create();

        // Well above maxMessageSize plus the broker's frame padding: the payload cannot be sent as a single
        // frame, so a local rejection is the only correct outcome.
        byte[] payload = RandomUtils.nextBytes(maxMessageSize + Commands.MESSAGE_SIZE_FRAME_PADDING + 1024);
        try {
            producer.send(payload);
            fail("an oversized message on a non-persistent topic must be rejected by the client");
        } catch (InvalidMessageException expected) {
            assertTrue(expected.getMessage().contains("exceeds"), expected.getMessage());
        }
    }

    /**
     * The final-size check runs on the complete frame, after the metadata has been added: a body within the limit
     * whose metadata pushes the message over it must be rejected there on a non-persistent topic, since nothing will
     * chunk it, and the producer must stay usable for the next valid message.
     */
    @Test
    public void testNonPersistentMessageOverLimitOnlyWithMetadataIsRejectedLocally() throws Exception {
        final int maxMessageSize = 1024;
        final String topicName = "non-persistent://my-property/my-ns/testNonPersistentChunkingMetadataOverLimit";

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName("my-subscriber-name")
                .subscribe();

        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topicName)
                .enableChunking(true)
                .enableBatching(false)
                .sendTimeout(5, TimeUnit.SECONDS)
                .create();
        producer.getConnectionHandler().setMaxMessageSize(maxMessageSize);

        // exactly at the limit, so the early check on the body passes and only the final check can reject it
        byte[] payload = RandomUtils.nextBytes(maxMessageSize);
        try {
            producer.newMessage().value(payload).property("large-metadata", "y".repeat(400)).send();
            fail("a message pushed over the limit by its metadata must be rejected by the client");
        } catch (InvalidMessageException expected) {
            assertTrue(expected.getMessage().contains("exceeds"), expected.getMessage());
        }

        byte[] valid = "valid".getBytes(StandardCharsets.UTF_8);
        producer.send(valid);
        Message<byte[]> received = consumer.receive(10, TimeUnit.SECONDS);
        assertNotNull(received, "the valid message after the rejected one was not delivered");
        assertEquals(received.getValue(), valid);
    }

    /**
     * Verifies that pendingChunkedMessageUuidQueue does not leak entries as chunked messages
     * complete normally. Each first chunk adds the message uuid to the queue; on completion the
     * uuid must be removed so the queue stays in sync with chunkedMessagesMap. Without the fix the
     * queue grew by one entry per completed chunked message unboundedly (a memory leak), since the
     * only other removal paths (eviction/expiry) never run when maxPendingChunkedMessage is not
     * exceeded.
     */
    @Test
    public void testPendingChunkedMessageUuidQueueDoesNotLeak() throws Exception {
        final String topicName = "persistent://my-property/my-ns/uuidQueueNoLeak";
        final String subName = "my-sub";
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .maxPendingChunkedMessage(10)
                .expireTimeOfIncompleteChunkedMessage(1, TimeUnit.HOURS)
                .autoAckOldestChunkedMessageOnQueueFull(true)
                .subscribe();
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .chunkMaxMessageSize(100)
                .enableChunking(true)
                .enableBatching(false)
                .create();

        ConsumerImpl<String> consumerImpl = (ConsumerImpl<String>) consumer;

        final int numMessages = 50;
        for (int i = 0; i < numMessages; i++) {
            String uuid = String.valueOf(i);
            // A complete 2-chunk message.
            sendSingleChunk(producer, uuid, 0, 2);
            sendSingleChunk(producer, uuid, 1, 2);
            Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
            assertEquals(msg.getValue(), "chunk-" + uuid + "-0|chunk-" + uuid + "-1|");
            consumer.acknowledge(msg);
        }

        // Every message completed and was removed from chunkedMessagesMap; the uuid queue must have
        // been drained in lockstep and not accumulated one ghost entry per completed message.
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(consumerImpl.chunkedMessagesMap.size(), 0);
            assertEquals(consumerImpl.getPendingChunkedMessageUuidQueueSizeForTest(), 0,
                    "pendingChunkedMessageUuidQueue leaked entries for completed chunked messages");
        });
    }

    @Test
    public void testResendChunkMessagesWithoutAckHole() throws Exception {
        log.info().attr("method", methodName).log("Starting test");
        final String topicName = "persistent://my-property/my-ns/testResendChunkMessagesWithoutAckHole";
        final String subName = "my-subscriber-name";
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .maxPendingChunkedMessage(10)
                .autoAckOldestChunkedMessageOnQueueFull(true)
                .subscribe();
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .chunkMaxMessageSize(100)
                .enableChunking(true)
                .enableBatching(false)
                .create();

        sendSingleChunk(producer, "0", 0, 2);

        sendSingleChunk(producer, "0", 0, 2); // Resending the first chunk
        sendSingleChunk(producer, "0", 1, 2);

        Message<String> receivedMsg = consumer.receive(5, TimeUnit.SECONDS);
        assertEquals(receivedMsg.getValue(), "chunk-0-0|chunk-0-1|");
        consumer.acknowledge(receivedMsg);
        assertEquals(admin.topics().getStats(topicName).getSubscriptions().get(subName)
                .getNonContiguousDeletedMessagesRanges(), 0);
    }

    @Test
    public void testResendChunkMessages() throws Exception {
        log.info().attr("method", methodName).log("Starting test");
        final String topicName = "persistent://my-property/my-ns/testResendChunkMessages";

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("my-subscriber-name")
                .maxPendingChunkedMessage(10)
                .autoAckOldestChunkedMessageOnQueueFull(true)
                .subscribe();
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .chunkMaxMessageSize(100)
                .enableChunking(true)
                .enableBatching(false)
                .create();

        sendSingleChunk(producer, "0", 0, 2);

        sendSingleChunk(producer, "0", 0, 2); // Resending the first chunk
        sendSingleChunk(producer, "1", 0, 3); // This is for testing the interwoven chunked message
        sendSingleChunk(producer, "1", 1, 3);
        sendSingleChunk(producer, "1", 0, 3); // Resending the UUID-1 chunked message

        sendSingleChunk(producer, "0", 1, 2);

        Message<String> receivedMsg = consumer.receive(5, TimeUnit.SECONDS);
        assertEquals(receivedMsg.getValue(), "chunk-0-0|chunk-0-1|");
        consumer.acknowledge(receivedMsg);

        sendSingleChunk(producer, "1", 1, 3);
        sendSingleChunk(producer, "1", 2, 3);

        receivedMsg = consumer.receive(5, TimeUnit.SECONDS);
        assertEquals(receivedMsg.getValue(), "chunk-1-0|chunk-1-1|chunk-1-2|");
        consumer.acknowledge(receivedMsg);
        Assert.assertEquals(((ConsumerImpl<String>) consumer).getAvailablePermits(), 8);
    }

    @Test
    public void testExpireIncompleteChunkMessage() throws Exception{
        final String topicName = "persistent://my-property/my-ns/expireMsg";

        // 1. producer connect
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .sendTimeout(10, TimeUnit.MINUTES)
            .create();
        Field producerIdField = ProducerImpl.class.getDeclaredField("producerId");
        producerIdField.setAccessible(true);
        long producerId = (long) producerIdField.get(producer);
        producer.cnx().registerProducer(producerId, producer); // registered spy ProducerImpl
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-sub").subscribe();
        ReaderImpl<byte[]> reader = (ReaderImpl<byte[]>) pulsarClient.newReader().topic(topicName)
                .startMessageId(MessageId.earliest).create();

        TypedMessageBuilderImpl<byte[]> msg =
                (TypedMessageBuilderImpl<byte[]>) producer.newMessage().value("message-1".getBytes());
        ByteBuf payload = Unpooled.wrappedBuffer(msg.getContent());
        MessageMetadata msgMetadata = msg.getMetadataBuilder();
        msgMetadata.setProducerName("test").setSequenceId(1).setPublishTime(10L)
                .setUuid("123").setNumChunksFromMsg(2).setChunkId(0).setTotalChunkMsgSize(100);
        ByteBufPair cmd = Commands.newSend(producerId, 1, 1, ChecksumType.Crc32c, msgMetadata, payload);
        MessageImpl msgImpl = ((MessageImpl<byte[]>) msg.getMessage());
        msgImpl.setSchemaState(SchemaState.Ready);
        OpSendMsg op = OpSendMsg.create(LatencyHistogram.NOOP, msgImpl, cmd, 1, null);
        producer.processOpSendMsg(op);

        retryStrategically((test) -> {
            return reader.getConsumer().chunkedMessagesMap.size() > 0 && consumer.chunkedMessagesMap.size() > 0;
        }, 5, 500);
        assertEquals(consumer.chunkedMessagesMap.size(), 1);
        assertEquals(reader.getConsumer().chunkedMessagesMap.size(), 1);

        consumer.expireTimeOfIncompleteChunkedMessageMillis = 1;
        reader.getConsumer().expireTimeOfIncompleteChunkedMessageMillis = 1;
        Thread.sleep(10);
        consumer.removeExpireIncompleteChunkedMessages();
        reader.getConsumer().removeExpireIncompleteChunkedMessages();
        assertEquals(consumer.chunkedMessagesMap.size(), 0);
        assertEquals(reader.getConsumer().chunkedMessagesMap.size(), 0);

        producer.close();
        consumer.close();
        reader.close();
        producer = null; // clean reference of mocked producer
    }

    @Test
    public void testChunksEnqueueFailed() throws Exception {
        final String topicName = "persistent://my-property/my-ns/test-chunks-enqueue-failed";
        log.info().attr("method", methodName).log("Starting test");
        this.conf.setMaxMessageSize(50);

        final MemoryLimitController controller = ((PulsarClientImpl) pulsarClient).getMemoryLimitController();
        assertEquals(controller.currentUsage(), 0);

        final int maxPendingMessages = 10;

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .maxPendingMessages(maxPendingMessages)
                .enableChunking(true)
                .enableBatching(false)
                .create();
        assertTrue(producer instanceof ProducerImpl);
        Semaphore semaphore = ((ProducerImpl<byte[]>) producer).getSemaphore().orElse(null);
        assertNotNull(semaphore);
        assertEquals(semaphore.availablePermits(), maxPendingMessages);
        producer.send(createMessagePayload(1).getBytes());
        try {
            producer.send(createMessagePayload(1000).getBytes(StandardCharsets.UTF_8));
            fail("It should fail with ProducerQueueIsFullError");
        } catch (PulsarClientException e) {
            assertTrue(e instanceof PulsarClientException.ProducerQueueIsFullError);
            assertEquals(controller.currentUsage(), 0);
            assertEquals(semaphore.availablePermits(), maxPendingMessages);
        }
    }

    @Override
    protected void customizeNewPulsarClientBuilder(ClientBuilder clientBuilder) {
        clientBuilder.memoryLimit(10000L, SizeUnit.BYTES);
    }

    interface ThrowingBiConsumer<T, K> {
        void accept(T t, K u) throws Exception;
    }

    @Test
    public void testSeekChunkMessages() throws Exception {
        log.info().attr("method", methodName).log("Starting test");
        this.conf.setMaxMessageSize(50);
        final int totalMessages = 5;
        final String topicName = "persistent://my-property/my-ns/test-seek-chunk";

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName);

        Producer<byte[]> producer = producerBuilder
                .enableChunking(true)
                .enableBatching(false)
                .create();

        Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName("inclusive-seek")
                .startMessageIdInclusive()
                .subscribe();

        Consumer<byte[]> consumer2 = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName("default-seek")
                .subscribe();

        for (int i = 0; i < totalMessages; i++) {
            String message = createMessagePayload(100);
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        List<MessageId> msgIds = new ArrayList<>();
        for (int i = 0; i < totalMessages; i++) {
            msg = consumer1.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.info().attr("i", i).attr("message", receivedMessage).log("Received message");
            msgIds.add(msg.getMessageId());
        }

        consumer1.seek(msgIds.get(1));
        for (int i = 1; i < totalMessages; i++) {
            Message<byte[]> msgAfterSeek = consumer1.receive(5, TimeUnit.SECONDS);
            assertEquals(msgIds.get(i), msgAfterSeek.getMessageId());
        }

        consumer2.seek(msgIds.get(1));
        for (int i = 2; i < totalMessages; i++) {
            Message<byte[]> msgAfterSeek = consumer2.receive(5, TimeUnit.SECONDS);
            assertEquals(msgIds.get(i), msgAfterSeek.getMessageId());
        }

        ThrowingBiConsumer<Boolean, MessageId> assertStartMessageId = (inclusive, expectedFirstMsgId) -> {
            final var builder = pulsarClient.newReader().topic(topicName).startMessageId(msgIds.get(1));
            if (inclusive) {
                builder.startMessageIdInclusive();
            }
            @Cleanup final var reader = builder.create();
            final var readMsg = reader.readNext(5, TimeUnit.SECONDS);
            assertEquals(expectedFirstMsgId, readMsg.getMessageId());
        };
        assertStartMessageId.accept(true, msgIds.get(1));
        assertStartMessageId.accept(false, msgIds.get(2));

        consumer1.close();
        consumer2.close();
        producer.close();

        log.info().attr("method", methodName).log("Exiting test");
    }

    @DataProvider(name = "seekInclusive")
    public Object[][] seekInclusive() {
        return new Object[][] { { false }, { true } };
    }

    @Test(dataProvider = "seekInclusive")
    public void testRepeatedSeekDoesNotRetainFilteredChunkMessageIds(boolean inclusive) throws Exception {
        final String topicName = "persistent://my-property/my-ns/seek-chunk-message-ids";
        final int totalMessages = 4;
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableChunking(true)
                .enableBatching(false)
                .chunkMaxMessageSize(100)
                .create();
        List<MessageId> messageIds = new ArrayList<>();
        for (int i = 0; i < totalMessages; i++) {
            MessageId messageId = producer.send(("message-" + i).repeat(30));
            assertThat(messageId).isInstanceOf(ChunkMessageIdImpl.class);
            messageIds.add(messageId);
        }
        MessageId markerId = producer.send("marker");
        assertThat(markerId).isNotInstanceOf(ChunkMessageIdImpl.class);

        // Subscribe after publishing so no chunk mappings exist before the first seek.
        ConsumerBuilder<String> consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("my-sub")
                .receiverQueueSize(32)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS);
        if (inclusive) {
            consumerBuilder.startMessageIdInclusive();
        }
        @Cleanup
        ConsumerImpl<String> consumer = (ConsumerImpl<String>) consumerBuilder.subscribe();
        assertThat(consumer.unAckedChunkedMessageIdSequenceMap).isEmpty();

        for (int boundary = 0; boundary < totalMessages; boundary++) {
            consumer.seek(messageIds.get(boundary));
            for (int i = inclusive ? boundary : boundary + 1; i < totalMessages; i++) {
                Message<String> message = consumer.receive(5, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
                MessageIdAdv messageId = (MessageIdAdv) message.getMessageId();
                assertThat(messageId).isEqualTo(messageIds.get(i));
                assertThat(message.getValue()).isEqualTo(("message-" + i).repeat(30));
                // Delivered messages must keep their chunk sequence until the application ACKs them.
                assertThat(consumer.unAckedChunkedMessageIdSequenceMap).containsKey(messageId);
                consumer.acknowledge(message);
                assertThat(consumer.unAckedChunkedMessageIdSequenceMap).doesNotContainKey(messageId);
            }
            // Receiving the following marker proves the boundary has been assembled and filtered.
            Message<String> marker = consumer.receive(5, TimeUnit.SECONDS);
            assertThat(marker).isNotNull();
            assertThat(marker.getMessageId()).isEqualTo(markerId);
            consumer.acknowledge(marker);
        }

        assertThat(consumer.unAckedChunkedMessageIdSequenceMap)
                .as("filtered seek boundaries must not accumulate chunk message ID sequences")
                .isEmpty();

        if (!inclusive) {
            // A filtered redelivery must not remove the sequence of an earlier application-held message.
            consumer.seek(MessageId.earliest);
            Message<String> outstanding = consumer.receive(5, TimeUnit.SECONDS);
            assertThat(outstanding).isNotNull();
            MessageIdAdv outstandingId = (MessageIdAdv) outstanding.getMessageId();
            assertThat(outstandingId).isEqualTo(messageIds.get(0));
            consumer.seek(outstandingId);
            for (int i = 1; i < totalMessages; i++) {
                Message<String> message = consumer.receive(5, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
                assertThat(message.getMessageId()).isEqualTo(messageIds.get(i));
                consumer.acknowledge(message);
            }
            Message<String> marker = consumer.receive(5, TimeUnit.SECONDS);
            assertThat(marker).isNotNull();
            assertThat(marker.getMessageId()).isEqualTo(markerId);
            consumer.acknowledge(marker);
            assertThat(consumer.unAckedChunkedMessageIdSequenceMap).containsOnlyKeys(outstandingId);
            consumer.acknowledge(outstanding);
            assertThat(consumer.unAckedChunkedMessageIdSequenceMap).isEmpty();
        }
    }

    @Test
    public void testReaderChunkingConfiguration() throws Exception {
        log.info().attr("method", methodName).log("Starting test");
        final String topicName = "persistent://my-property/my-ns/my-topic1";
        ReaderImpl<byte[]> reader = (ReaderImpl<byte[]>) pulsarClient.newReader().topic(topicName)
                .startMessageId(MessageId.earliest).maxPendingChunkedMessage(12)
                .autoAckOldestChunkedMessageOnQueueFull(true)
                .expireTimeOfIncompleteChunkedMessage(12, TimeUnit.MILLISECONDS).create();
        ConsumerImpl<byte[]> consumer = reader.getConsumer();
        assertEquals(consumer.conf.getMaxPendingChunkedMessage(), 12);
        assertTrue(consumer.conf.isAutoAckOldestChunkedMessageOnQueueFull());
        assertEquals(consumer.conf.getExpireTimeOfIncompleteChunkedMessageMillis(), 12);
    }

    @Test
    public void testChunkSize() throws Exception {
        final int maxMessageSize = 50;
        final int payloadChunkSize = maxMessageSize - 32/* the default message metadata size for string schema */;
        this.conf.setMaxMessageSize(maxMessageSize);

        final Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("my-property/my-ns/test-chunk-size")
                .enableChunking(true)
                .enableBatching(false)
                .create();
        for (int size = 1; size <= maxMessageSize; size++) {
            final MessageId messageId = producer.send(createMessagePayload(size));
            log.info().attr("size", size).attr("messageId", messageId).log("Sent bytes");
            if (size <= payloadChunkSize) {
                assertEquals(messageId.getClass(), MessageIdImpl.class);
            } else {
                assertEquals(messageId.getClass(), ChunkMessageIdImpl.class);
            }
        }
    }

    @Test
    public void testBlockIfQueueFullWhenChunking() throws Exception {
        this.conf.setMaxMessageSize(50);

        @Cleanup
        final Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("my-property/my-ns/test-chunk-size")
                .enableChunking(true)
                .enableBatching(false)
                .blockIfQueueFull(true)
                .maxPendingMessages(3)
                .create();

        // Test sending large message (totalChunks > maxPendingMessages) should not cause deadlock
        // We need to use a separate thread to send the message instead of using the sendAsync, because the deadlock
        // might happen before publishing messages to the broker.
        CompletableFuture<Void> sendMsg = CompletableFuture.runAsync(() -> {
            try {
                producer.send(createMessagePayload(200));
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        });
        try {
            sendMsg.get(5, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            Assert.fail("Deadlock detected when sending large message.");
        }

        // Test sending multiple large messages (For every message, totalChunks < maxPendingMessages) concurrently
        // should not cause the deadlock.
        List<CompletableFuture<Void>> sendMsgFutures = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            sendMsgFutures.add(CompletableFuture.runAsync(() -> {
                try {
                    producer.send(createMessagePayload(100));
                } catch (PulsarClientException e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        try {
            FutureUtil.waitForAll(sendMsgFutures).get(5, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            Assert.fail("Deadlock detected when sending multiple large messages concurrently.");
        }
    }

    private String createMessagePayload(int size) {
        StringBuilder str = new StringBuilder();
        Random rand = new Random();
        for (int i = 0; i < size; i++) {
            str.append(rand.nextInt(10));
        }
        return str.toString();
    }

}
