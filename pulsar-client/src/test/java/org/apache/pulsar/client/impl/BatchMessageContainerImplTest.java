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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.testng.annotations.Test;

public class BatchMessageContainerImplTest {

    @Test
    public void testUpdateMaxBatchSize() {
        int shrinkCoolingOffPeriod = 10;
        BatchMessageContainerImpl messageContainer = new BatchMessageContainerImpl();
        // check init state
        assertEquals(messageContainer.getMaxBatchSize(), 1024);

        // test expand
        messageContainer.updateMaxBatchSize(2048);
        assertEquals(messageContainer.getMaxBatchSize(), 2048);

        // test cooling-off period
        messageContainer.updateMaxBatchSize(2);
        assertEquals(messageContainer.getMaxBatchSize(), 2048);

        // test shrink
        for (int i = 0; i < 15; ++i) {
            messageContainer.updateMaxBatchSize(2);
            if (i < shrinkCoolingOffPeriod) {
                assertEquals(messageContainer.getMaxBatchSize(), 2048);
            } else {
                assertEquals(messageContainer.getMaxBatchSize(), 2048 * 0.75);
            }
        }

        messageContainer.updateMaxBatchSize(2048);
        // test big message sudden appearance
        for (int i = 0; i < 15; ++i) {
            if (i == shrinkCoolingOffPeriod - 2) {
                messageContainer.updateMaxBatchSize(2000);
            } else {
                messageContainer.updateMaxBatchSize(2);
            }
            assertEquals(messageContainer.getMaxBatchSize(), 2048);
        }

        // test big and small message alternating occurrence
        for (int i = 0; i < shrinkCoolingOffPeriod * 3; ++i) {
            if (i % 2 == 0) {
                messageContainer.updateMaxBatchSize(2);
            } else {
                messageContainer.updateMaxBatchSize(2000);
            }
            assertEquals(messageContainer.getMaxBatchSize(), 2048);
        }

        // test consecutive big message
        for (int i = 0; i < 15; ++i) {
            messageContainer.updateMaxBatchSize(2000);
            assertEquals(messageContainer.getMaxBatchSize(), 2048);
        }

        // test expand after shrink
        messageContainer.updateMaxBatchSize(4096);
        assertEquals(messageContainer.getMaxBatchSize(), 4096);
    }

    @Test
    public void recoveryAfterOom() {
        final AtomicBoolean called = new AtomicBoolean();
        final ProducerImpl<?> producer = mock(ProducerImpl.class);
        final ProducerConfigurationData producerConfigurationData = new ProducerConfigurationData();
        producerConfigurationData.setCompressionType(CompressionType.NONE);
        PulsarClientImpl pulsarClient = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(pulsarClient.getCnxPool()).thenReturn(connectionPool);
        MemoryLimitController memoryLimitController = mock(MemoryLimitController.class);
        when(pulsarClient.getMemoryLimitController()).thenReturn(memoryLimitController);
        try {
            Field clientFiled = HandlerState.class.getDeclaredField("client");
            clientFiled.setAccessible(true);
            clientFiled.set(producer, pulsarClient);
        } catch (Exception e){
            fail(e.getMessage());
        }

        when(producer.getConfiguration()).thenReturn(producerConfigurationData);
        final ByteBufAllocator mockAllocator = mock(ByteBufAllocator.class);
        doAnswer((ignore) -> {
            called.set(true);
            throw new OutOfMemoryError("test");
        }).when(mockAllocator).buffer(anyInt());
        final BatchMessageContainerImpl batchMessageContainer = new BatchMessageContainerImpl(mockAllocator);
        batchMessageContainer.setProducer(producer);
        MessageMetadata messageMetadata1 = new MessageMetadata();
        messageMetadata1.setSequenceId(1L);
        messageMetadata1.setProducerName("producer1");
        messageMetadata1.setPublishTime(System.currentTimeMillis());
        ByteBuffer payload1 = ByteBuffer.wrap("payload1".getBytes(StandardCharsets.UTF_8));
        final MessageImpl<byte[]> message1 = MessageImpl.create(messageMetadata1, payload1, Schema.BYTES, null);
        batchMessageContainer.add(message1, null);
        assertTrue(called.get());
        MessageMetadata messageMetadata2 = new MessageMetadata();
        messageMetadata2.setSequenceId(1L);
        messageMetadata2.setProducerName("producer1");
        messageMetadata2.setPublishTime(System.currentTimeMillis());
        ByteBuffer payload2 = ByteBuffer.wrap("payload2".getBytes(StandardCharsets.UTF_8));
        final MessageImpl<byte[]> message2 = MessageImpl.create(messageMetadata2, payload2, Schema.BYTES, null);
        // after oom, our add can self-healing, won't throw exception
        batchMessageContainer.add(message2, null);
    }

    @Test
    public void testMessagesSize() throws Exception {
        ProducerImpl<?> producer = createTestProducer();

        final int initNum = 32;
        BatchMessageContainerImpl batchMessageContainer = new BatchMessageContainerImpl(producer);
        assertEquals(batchMessageContainer.getMaxMessagesNum(), initNum);

        addMessagesAndCreateOpSendMsg(batchMessageContainer, 10);
        assertEquals(batchMessageContainer.getMaxMessagesNum(), initNum);

        addMessagesAndCreateOpSendMsg(batchMessageContainer, 200);
        assertEquals(batchMessageContainer.getMaxMessagesNum(), 200);

        addMessagesAndCreateOpSendMsg(batchMessageContainer, 10);
        assertEquals(batchMessageContainer.getMaxMessagesNum(), 200);
    }

    @Test
    public void testEntryBucketHashRangeIsStampedWhenCreatingSendOperation() throws Exception {
        BatchMessageContainerImpl batchMessageContainer = new BatchMessageContainerImpl(createTestProducer());
        ArrayList<MessageImpl<?>> messages = new ArrayList<>();
        try {
            MessageImpl<?> singleMessage = createMessage(1);
            messages.add(singleMessage);
            batchMessageContainer.add(singleMessage, null, 0x3000);
            batchMessageContainer.createOpSendMsg();
            assertEquals(batchMessageContainer.messageMetadata.getEntryHashMin(), 0x3000);
            assertEquals(batchMessageContainer.messageMetadata.getEntryHashMax(), 0x3000);

            batchMessageContainer.clear();
            MessageImpl<?> firstMessage = createMessage(2);
            MessageImpl<?> secondMessage = createMessage(3);
            MessageImpl<?> thirdMessage = createMessage(4);
            messages.add(firstMessage);
            messages.add(secondMessage);
            messages.add(thirdMessage);
            batchMessageContainer.add(firstMessage, null, 0x2000);
            batchMessageContainer.add(secondMessage, null, 0x1000);
            batchMessageContainer.add(thirdMessage, null, 0x1800);
            batchMessageContainer.createOpSendMsg();
            assertEquals(batchMessageContainer.messageMetadata.getEntryHashMin(), 0x1000);
            assertEquals(batchMessageContainer.messageMetadata.getEntryHashMax(), 0x2000);
        } finally {
            batchMessageContainer.discard(null);
            messages.forEach(ReferenceCountUtil::safeRelease);
        }
    }

    private MessageImpl<?> createMessage(long sequenceId) {
        MessageMetadata messageMetadata = new MessageMetadata();
        messageMetadata.setSequenceId(sequenceId);
        messageMetadata.setProducerName("producer");
        messageMetadata.setPublishTime(System.currentTimeMillis());
        ByteBuffer payload = ByteBuffer.wrap("payload".getBytes(StandardCharsets.UTF_8));
        return MessageImpl.create(messageMetadata, payload, Schema.BYTES, null);
    }

    private ProducerImpl<?> createTestProducer() throws Exception {
        ProducerImpl<?> producer = mock(ProducerImpl.class);
        ProducerConfigurationData producerConfigurationData = new ProducerConfigurationData();
        producerConfigurationData.setCompressionType(CompressionType.NONE);
        PulsarClientImpl pulsarClient = mock(PulsarClientImpl.class);
        when(pulsarClient.getCnxPool()).thenReturn(mock(ConnectionPool.class));
        when(pulsarClient.getMemoryLimitController()).thenReturn(mock(MemoryLimitController.class));
        Field clientField = HandlerState.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(producer, pulsarClient);
        when(producer.getConfiguration()).thenReturn(producerConfigurationData);
        when(producer.encryptMessage(any(), any())).thenAnswer(__ -> ByteBufAllocator.DEFAULT.buffer()
                .writeBytes("payload".getBytes(StandardCharsets.UTF_8)));
        return producer;
    }

    private void addMessagesAndCreateOpSendMsg(BatchMessageContainerImpl batchMessageContainer, int num)
            throws Exception{
        ArrayList<MessageImpl<?>> messages = new ArrayList<>();
        for (int i = 0; i < num; ++i) {
            MessageImpl<?> message = createMessage(i);
            messages.add(message);
            batchMessageContainer.add(message, null);
        }

        batchMessageContainer.createOpSendMsg();
        batchMessageContainer.clear();
        messages.forEach(ReferenceCountUtil::safeRelease);
    }

    /**
     * A batch carries one transaction id in its metadata, so every message in it inherits that transaction.
     * Mixing a plain message into a transactional batch therefore silently enrolls it in the transaction: it
     * stays invisible until commit and is dropped on abort, even though the application never sent it inside
     * a transaction.
     */
    @Test
    public void testPlainMessageIsNotBatchedWithTransactionalMessages() throws Exception {
        BatchMessageContainerImpl container = new BatchMessageContainerImpl(createTestProducer());
        try {
            container.add(newMessage(1L, 7L, 13L), null);

            assertFalse(container.hasSameTxn(newMessage(2L, null, null)),
                    "a plain message was accepted into a transactional batch");
        } finally {
            container.discard(null);
        }
    }

    /**
     * The mirror image: a transactional message joining a batch that already holds plain messages stamps its
     * transaction id onto the whole batch, retroactively pulling those plain messages into the transaction.
     */
    @Test
    public void testTransactionalMessageIsNotBatchedWithPlainMessages() throws Exception {
        BatchMessageContainerImpl container = new BatchMessageContainerImpl(createTestProducer());
        try {
            container.add(newMessage(1L, null, null), null);

            assertFalse(container.hasSameTxn(newMessage(2L, 7L, 13L)),
                    "a transactional message was accepted into a plain batch");
        } finally {
            container.discard(null);
        }
    }

    /**
     * Key-based batching fails the other way round: the guard reads the outer container's transaction state
     * while the batch metadata is built by the per-key inner container, so a transactional message landing in
     * a key bucket whose first message was plain is published with no transaction id at all — outside the
     * transaction, and not rolled back on abort.
     */
    @Test
    public void testKeyBasedContainerKeepsTransactionalAndPlainMessagesApart() throws Exception {
        BatchMessageKeyBasedContainer container = new BatchMessageKeyBasedContainer();
        container.setProducer(createTestProducer());
        try {
            container.add(newMessage(1L, null, null), null);

            assertFalse(container.hasSameTxn(newMessage(2L, 7L, 13L)),
                    "a transactional message was accepted into a plain key-based batch");
        } finally {
            container.discard(null);
        }
    }

    /** Messages of the same transaction must still batch together, and two different transactions must not. */
    @Test
    public void testSameTransactionStillBatchesTogether() throws Exception {
        BatchMessageContainerImpl container = new BatchMessageContainerImpl(createTestProducer());
        try {
            container.add(newMessage(1L, 7L, 13L), null);

            assertTrue(container.hasSameTxn(newMessage(2L, 7L, 13L)),
                    "two messages of the same transaction were split across batches");
            assertFalse(container.hasSameTxn(newMessage(3L, 7L, 14L)),
                    "messages of two different transactions were put in one batch");
        } finally {
            container.discard(null);
        }
    }

    /** Plain messages must still batch with each other. */
    @Test
    public void testPlainMessagesStillBatchTogether() throws Exception {
        BatchMessageContainerImpl container = new BatchMessageContainerImpl(createTestProducer());
        try {
            container.add(newMessage(1L, null, null), null);

            assertTrue(container.hasSameTxn(newMessage(2L, null, null)),
                    "two plain messages were split across batches");
        } finally {
            container.discard(null);
        }
    }

    private static MessageImpl<byte[]> newMessage(long sequenceId, Long txnIdMostBits, Long txnIdLeastBits) {
        MessageMetadata metadata = new MessageMetadata();
        metadata.setSequenceId(sequenceId);
        metadata.setProducerName("producer1");
        metadata.setPublishTime(System.currentTimeMillis());
        if (txnIdMostBits != null) {
            metadata.setTxnidMostBits(txnIdMostBits);
            metadata.setTxnidLeastBits(txnIdLeastBits);
        }
        ByteBuffer payload = ByteBuffer.wrap(("payload-" + sequenceId).getBytes(StandardCharsets.UTF_8));
        return MessageImpl.create(metadata, payload, Schema.BYTES, null);
    }

    /**
     * An inner batch whose first add fails to allocate clears itself and stays empty, so the outer container's
     * message count is never incremented and the outer is not cleared either. Any transaction identity captured
     * for that failed message must therefore not survive into the next first message, or a later message of that
     * transaction would be admitted into a plain batch and published outside its transaction. Allocation failure
     * is a recoverable path the client already handles.
     */
    @Test
    public void testKeyBasedContainerDropsTheTransactionIdentityOfAFailedFirstAdd() throws Exception {
        ProducerImpl<?> producer = createTestProducer();
        MemoryLimitController memoryLimitController = producer.client.getMemoryLimitController();
        // fail only the first inner-batch allocation
        doThrow(new OutOfMemoryError("test")).doNothing()
                .when(memoryLimitController).forceReserveMemory(anyLong());

        BatchMessageKeyBasedContainer container = new BatchMessageKeyBasedContainer();
        container.setProducer(producer);
        try {
            container.add(newMessage(1L, 7L, 13L), null);
            assertEquals(container.getNumMessagesInBatch(), 0,
                    "the failed add should have left the container empty");

            container.add(newMessage(2L, null, null), null);

            assertFalse(container.hasSameTxn(newMessage(3L, 7L, 13L)),
                    "a transactional message was admitted into a plain batch through the transaction identity"
                            + " left behind by a failed add");
        } finally {
            container.discard(null);
        }
    }

}
