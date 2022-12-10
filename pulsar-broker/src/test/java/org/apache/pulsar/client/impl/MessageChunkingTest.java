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

import static org.testng.Assert.assertEquals;
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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.client.impl.MessageImpl.SchemaState;
import org.apache.pulsar.client.impl.ProducerImpl.OpSendMsg;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class MessageChunkingTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(MessageChunkingTest.class);

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

    @Test
    public void testInvalidConfig() throws Exception {
        final String topicName = "persistent://my-property/my-ns/my-topic1";
        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName);
        // batching and chunking can't be enabled together
        try {
            Producer<byte[]> producer = producerBuilder.enableChunking(true).enableBatching(true).create();
            fail("producer creation should have fail");
        } catch (IllegalArgumentException ie) {
            // Ok
        }
    }

    @Test(dataProvider = "ackReceiptEnabledWithMaxMessageSize")
    public void testLargeMessage(boolean ackReceiptEnabled, boolean clientSizeMaxMessageSize) throws Exception {

        log.info("-- Starting {} test --", methodName);
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
            log.info("[{}] - Published [{}] Received message: [{}]", i, publishedMessages.get(i), receivedMessage);
            String expectedMessage = publishedMessages.get(i);
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            msgIds.add(msg.getMessageId());
        }

        pulsar.getBrokerService().updateRates();

        PublisherStats producerStats = topic.getStats(false, false, false).getPublishers().get(0);

        assertTrue(producerStats.getChunkedMessageRate() > 0);

        ManagedCursorImpl mcursor = (ManagedCursorImpl) topic.getManagedLedger().getCursors().iterator().next();
        PositionImpl readPosition = (PositionImpl) mcursor.getReadPosition();

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
        log.info("-- Exiting {} test --", methodName);

    }

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

        log.info("-- Starting {} test --", methodName);
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
            log.info("Received message: [{}]", receivedMessage);
            String expectedMessage = publishedMessages.get(i);
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        messageSet.clear();
        for (int i = 0; i < totalMessages; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.info("Received message: [{}]", receivedMessage);
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
            log.info("Received message: [{}]", receivedMessage);
            String expectedMessage = publishedMessages.get(i);
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        ManagedCursorImpl mcursor = (ManagedCursorImpl) topic.getManagedLedger().getCursors().iterator().next();
        PositionImpl readPosition = (PositionImpl) mcursor.getReadPosition();

        consumer.acknowledgeCumulative(lastMsgId);

        retryStrategically((test) -> {
            return mcursor.getMarkDeletedPosition().getNext().equals(readPosition);
        }, 5, 200);

        assertEquals(readPosition, mcursor.getMarkDeletedPosition().getNext());

        consumer.close();
        producer.close();
        reader.close();
        log.info("-- Exiting {} test --", methodName);

    }

    @Test
    public void testPublishWithFailure() throws Exception {
        log.info("-- Starting {} test --", methodName);
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

    @Test
    public void testMaxPendingChunkMessages() throws Exception {
        log.info("-- Starting {} test --", methodName);
        final String topicName = "persistent://my-property/my-ns/maxPending";

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("my-subscriber-name")
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
        consumer.redeliverUnacknowledgedMessages();

        sendSingleChunk(producer, "0", 1, 2);

        // Ensure that the chunked message of uuid 0 is discarded.
        assertNull(consumer.receive(5, TimeUnit.SECONDS));
    }

    /**
     * Validate that chunking is not supported with batching and non-persistent topic
     *
     * @throws Exception
     */
    @Test
    public void testInvalidUseCaseForChunking() throws Exception {

        log.info("-- Starting {} test --", methodName);
        this.conf.setMaxMessageSize(5);
        final String topicName = "persistent://my-property/my-ns/my-topic1";

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName);

        try {
            Producer<byte[]> producer = producerBuilder.enableChunking(true).enableBatching(true).create();
            fail("it should have failied because chunking can't be used with batching enabled");
        } catch (IllegalArgumentException ie) {
            // Ok
        }

        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testExpireIncompleteChunkMessage() throws Exception{
        final String topicName = "persistent://prop/use/ns-abc/expireMsg";

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

        TypedMessageBuilderImpl<byte[]> msg = (TypedMessageBuilderImpl<byte[]>) producer.newMessage().value("message-1".getBytes());
        ByteBuf payload = Unpooled.wrappedBuffer(msg.getContent());
        MessageMetadata msgMetadata = msg.getMetadataBuilder();
        msgMetadata.setProducerName("test").setSequenceId(1).setPublishTime(10L)
                .setUuid("123").setNumChunksFromMsg(2).setChunkId(0).setTotalChunkMsgSize(100);
        ByteBufPair cmd = Commands.newSend(producerId, 1, 1, ChecksumType.Crc32c, msgMetadata, payload);
        MessageImpl msgImpl = ((MessageImpl<byte[]>) msg.getMessage());
        msgImpl.setSchemaState(SchemaState.Ready);
        OpSendMsg op = OpSendMsg.create(msgImpl, cmd, 1, null);
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
        log.info("-- Starting {} test --", methodName);
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

    @Test
    public void testSeekChunkMessages() throws PulsarClientException {
        log.info("-- Starting {} test --", methodName);
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
            log.info("[{}] - Received message: [{}]", i, receivedMessage);
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

        Reader<byte[]> reader = pulsarClient.newReader()
                .topic(topicName)
                .startMessageIdInclusive()
                .startMessageId(msgIds.get(1))
                .create();

        Message<byte[]> readMsg = reader.readNext(5, TimeUnit.SECONDS);
        assertEquals(msgIds.get(1), readMsg.getMessageId());

        consumer1.close();
        consumer2.close();
        producer.close();

        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testReaderChunkingConfiguration() throws Exception {
        log.info("-- Starting {} test --", methodName);
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
            log.info("Send {} bytes to {}", size, messageId);
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
