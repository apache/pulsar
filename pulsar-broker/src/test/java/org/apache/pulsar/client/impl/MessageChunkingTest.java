/**
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
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
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

    @Test(dataProvider = "ackReceiptEnabled")
    public void testLargeMessage(boolean ackReceiptEnabled) throws Exception {

        log.info("-- Starting {} test --", methodName);
        this.conf.setMaxMessageSize(5);
        final int totalMessages = 5;
        final String topicName = "persistent://my-property/my-ns/my-topic1";

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .isAckReceiptEnabled(ackReceiptEnabled)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName);

        Producer<byte[]> producer = producerBuilder.compressionType(CompressionType.LZ4).enableChunking(true)
                .enableBatching(false).create();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();

        List<String> publishedMessages = Lists.newArrayList();
        for (int i = 0; i < totalMessages; i++) {
            String message = createMessagePayload(i * 10);
            publishedMessages.add(message);
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        List<MessageId> msgIds = Lists.newArrayList();
        for (int i = 0; i < totalMessages; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.info("[{}] - Published [{}] Received message: [{}]", i, publishedMessages.get(i), receivedMessage);
            String expectedMessage = publishedMessages.get(i);
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            msgIds.add(msg.getMessageId());
        }

        pulsar.getBrokerService().updateRates();

        PublisherStats producerStats = topic.getStats(false, false).publishers.get(0);

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

    @Test(dataProvider = "ackReceiptEnabled")
    public void testLargeMessageAckTimeOut(boolean ackReceiptEnabled) throws Exception {

        log.info("-- Starting {} test --", methodName);
        this.conf.setMaxMessageSize(5);
        final int totalMessages = 5;
        final String topicName = "persistent://my-property/my-ns/my-topic1";

        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-subscriber-name").acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .isAckReceiptEnabled(ackReceiptEnabled)
                .ackTimeout(5, TimeUnit.SECONDS).subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName);

        Producer<byte[]> producer = producerBuilder.enableChunking(true).enableBatching(false).create();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();

        List<String> publishedMessages = Lists.newArrayList();
        for (int i = 0; i < totalMessages; i++) {
            String message = createMessagePayload(i * 10);
            publishedMessages.add(message);
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
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
        log.info("-- Exiting {} test --", methodName);

    }

    @Test
    public void testPublishWithFailure() throws Exception {
        log.info("-- Starting {} test --", methodName);
        this.conf.setMaxMessageSize(5);
        final String topicName = "persistent://my-property/my-ns/my-topic1";

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName);

        Producer<byte[]> producer = producerBuilder.enableChunking(true).enableBatching(false)
                .create();

        stopBroker();

        try {
            producer.send(createMessagePayload(100).getBytes());
            fail("should have failed with timeout exception");
        } catch (PulsarClientException.TimeoutException e) {
            // Ok
        }
        producer.close();
    }

    @Test(enabled = false)
    public void testMaxPendingChunkMessages() throws Exception {

        log.info("-- Starting {} test --", methodName);
        this.conf.setMaxMessageSize(10);
        final int totalMessages = 25;
        final String topicName = "persistent://my-property/my-ns/maxPending";
        final int totalProducers = 25;
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newFixedThreadPool(totalProducers);

        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-subscriber-name").acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .maxPendingChunkedMessage(1).autoAckOldestChunkedMessageOnQueueFull(true)
                .ackTimeout(5, TimeUnit.SECONDS).subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName);

        Producer<byte[]>[] producers = new Producer[totalProducers];
        int totalPublishedMessages = totalProducers;
        List<CompletableFuture<MessageId>> futures = Lists.newArrayList();
        for (int i = 0; i < totalProducers; i++) {
            producers[i] = producerBuilder.enableChunking(true).enableBatching(false).create();
            int index = i;
            executor.submit(() -> {
                futures.add(producers[index].sendAsync(createMessagePayload(45).getBytes()));
            });
        }

        FutureUtil.waitForAll(futures).get();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < totalMessages; i++) {
            msg = consumer.receive(1, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            String receivedMessage = new String(msg.getData());
            log.info("Received message: [{}]", receivedMessage);
            messageSet.add(receivedMessage);
            consumer.acknowledge(msg);
        }

        assertNotEquals(messageSet.size(), totalPublishedMessages);

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
            return consumer.chunkedMessagesMap.size() > 0;
        }, 5, 500);
        assertEquals(consumer.chunkedMessagesMap.size(), 1);

        consumer.expireTimeOfIncompleteChunkedMessageMillis = 1;
        Thread.sleep(10);
        consumer.removeExpireIncompleteChunkedMessages();
        assertEquals(consumer.chunkedMessagesMap.size(), 0);

        producer.close();
        consumer.close();
        producer = null; // clean reference of mocked producer
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
