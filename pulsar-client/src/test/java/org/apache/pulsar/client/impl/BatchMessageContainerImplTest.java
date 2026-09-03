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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.DataProvider;
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

    @DataProvider
    public Object[][] compressionTypes() {
        return new Object[][] {
                {CompressionType.NONE},
                {CompressionType.ZLIB},
        };
    }

    /**
     * A failure after the batch payload was built must not break the retry on the next flush, with or without
     * compression: the container must not reuse a buffer released by the compression path.
     */
    @Test(dataProvider = "compressionTypes")
    public void testRecoveryAfterBatchBuildFailure(CompressionType compressionType) throws Exception {
        ProducerImpl<?> producer = createTestProducer(compressionType);

        AtomicReference<ByteBuf> compressedRef = new AtomicReference<>();
        doAnswer(invocation -> {
            ByteBuf source = invocation.getArgument(0);
            ByteBuf compressed = PulsarByteBufAllocator.DEFAULT.buffer(source.readableBytes());
            compressed.writeBytes(source);
            source.release();
            compressedRef.set(compressed);
            return compressed;
        }).when(producer).applyCompression(any());
        AtomicBoolean fail = new AtomicBoolean(true);
        when(producer.encryptMessage(any(), any())).thenAnswer(invocation -> {
            if (fail.get()) {
                throw new RuntimeException("mocked encryption failure");
            }
            return invocation.getArgument(1);
        });
        when(producer.sendMessage(anyLong(), anyLong(), anyLong(), anyInt(), any(), any())).thenAnswer(invocation -> {
            ByteBuf payload = invocation.getArgument(5);
            ByteBuf header = PulsarByteBufAllocator.DEFAULT.buffer();
            header.writeInt(4 + 4 + payload.readableBytes());
            header.writeInt(0);
            return ByteBufPair.get(header, payload);
        });

        BatchMessageContainerImpl batchMessageContainer = new BatchMessageContainerImpl(producer);
        List<MessageImpl<?>> messages = addMessages(batchMessageContainer, 2);

        // First build fails after the batch payload was produced; ProducerImpl.batchMessageAndSend() then resets.
        assertThatThrownBy(batchMessageContainer::createOpSendMsg)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("mocked");
        if (compressionType != CompressionType.NONE) {
            // The container keeps its buffer reference while messages remain, and the compressed payload must not
            // leak when encryption fails before anything took ownership of it.
            assertNotNull(batchMessageContainer.batchedMessageMetadataAndPayload);
            assertEquals(compressedRef.get().refCnt(), 0);
            // Re-entering the build without reset must fail fast instead of writing into released memory.
            assertThatThrownBy(batchMessageContainer::createOpSendMsg)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not owned");
        }
        batchMessageContainer.resetPayloadAfterFailedPublishing();

        // The retry must succeed and produce a valid command instead of reusing a released buffer.
        fail.set(false);
        ProducerImpl.OpSendMsg op = batchMessageContainer.createOpSendMsg();
        assertNotNull(op);
        assertNotNull(op.cmd);
        if (compressionType != CompressionType.NONE) {
            // Guard against a vacuous green: the compression branch must actually have run.
            verify(producer, atLeastOnce()).applyCompression(any());
        }
        op.cmd.release();
        batchMessageContainer.clear();
        messages.forEach(ReferenceCountUtil::safeRelease);
    }

    /**
     * Encryption behaves like compression: encryptMessage() releases the source payload and returns a new buffer.
     * A later failure must not make the retry reuse the released batch buffer.
     */
    @Test
    public void testRecoveryAfterEncryptionFailure() throws Exception {
        ProducerImpl<?> producer = createTestProducer(CompressionType.NONE);

        AtomicBoolean failSend = new AtomicBoolean(true);
        AtomicReference<ByteBuf> firstEncryptedRef = new AtomicReference<>();
        when(producer.encryptMessage(any(), any())).thenAnswer(invocation -> {
            // Real encryption allocates a new buffer and releases the source payload.
            ByteBuf source = invocation.getArgument(1);
            ByteBuf encrypted = PulsarByteBufAllocator.DEFAULT.buffer(source.readableBytes());
            encrypted.writeBytes(source);
            source.release();
            if (failSend.get()) {
                firstEncryptedRef.set(encrypted);
            }
            return encrypted;
        });
        when(producer.sendMessage(anyLong(), anyLong(), anyLong(), anyInt(), any(), any())).thenAnswer(invocation -> {
            if (failSend.getAndSet(false)) {
                throw new RuntimeException("mocked send failure");
            }
            ByteBuf payload = invocation.getArgument(5);
            ByteBuf header = PulsarByteBufAllocator.DEFAULT.buffer();
            header.writeInt(4 + 4 + payload.readableBytes());
            header.writeInt(0);
            return ByteBufPair.get(header, payload);
        });

        BatchMessageContainerImpl batchMessageContainer = new BatchMessageContainerImpl(producer);
        List<MessageImpl<?>> messages = addMessages(batchMessageContainer, 2);

        assertThatThrownBy(batchMessageContainer::createOpSendMsg).isInstanceOf(RuntimeException.class);
        // The encrypted payload is orphaned once sendMessage fails; it must be released, not leaked.
        assertEquals(firstEncryptedRef.get().refCnt(), 0);
        batchMessageContainer.resetPayloadAfterFailedPublishing();

        ProducerImpl.OpSendMsg op = batchMessageContainer.createOpSendMsg();
        assertNotNull(op);
        assertNotNull(op.cmd);
        op.cmd.release();
        batchMessageContainer.clear();
        messages.forEach(ReferenceCountUtil::safeRelease);
    }

    /**
     * Without compression or encryption, a failed build must still leave a retry that produces a well-formed,
     * parseable SEND frame with balanced ref-counts.
     */
    @Test
    public void testNoCompressionBuildFailureProducesValidFrame() throws Exception {
        assertValidSendFrameAfterFailure(true, false);   // failure in encryptMessage
        assertValidSendFrameAfterFailure(false, true);   // failure in sendMessage
    }

    private void assertValidSendFrameAfterFailure(boolean failAtEncrypt, boolean failAtSend) throws Exception {
        ProducerImpl<?> producer = createTestProducer(CompressionType.NONE);

        AtomicBoolean failOnce = new AtomicBoolean(true);
        if (failAtEncrypt) {
            when(producer.encryptMessage(any(), any())).thenAnswer(invocation -> {
                if (failOnce.getAndSet(false)) {
                    throw new RuntimeException("mocked encryption failure");
                }
                return invocation.getArgument(1);
            });
        } else {
            when(producer.encryptMessage(any(), any())).thenAnswer(invocation -> invocation.getArgument(1));
        }
        when(producer.sendMessage(anyLong(), anyLong(), anyLong(), anyInt(), any(), any())).thenAnswer(invocation -> {
            if (failAtSend && failOnce.getAndSet(false)) {
                throw new RuntimeException("mocked send failure");
            }
            MessageMetadata metadata = invocation.getArgument(4);
            ByteBuf payload = invocation.getArgument(5);
            return Commands.newSend(0L, metadata.hasSequenceId() ? metadata.getSequenceId() : 0L, 1,
                    Commands.ChecksumType.Crc32c, metadata, payload);
        });

        BatchMessageContainerImpl batchMessageContainer = new BatchMessageContainerImpl(producer);
        List<MessageImpl<?>> messages = addMessages(batchMessageContainer, 3);

        // First build fails after the batch payload was produced; ProducerImpl.batchMessageAndSend() then resets.
        assertThatThrownBy(batchMessageContainer::createOpSendMsg).isInstanceOf(RuntimeException.class);
        batchMessageContainer.resetPayloadAfterFailedPublishing();

        // The retry must succeed and produce a well-formed SEND frame.
        ProducerImpl.OpSendMsg op = batchMessageContainer.createOpSendMsg();
        assertNotNull(op);
        assertNotNull(op.cmd);

        ByteBufPair cmd = op.cmd;
        ByteBuf header = cmd.getFirst();
        ByteBuf payloadBuf = cmd.getSecond();
        int totalSize = header.getInt(0);
        int cmdSize = header.getInt(4);
        // The total-size field must equal the number of bytes that follow it.
        assertEquals(totalSize, cmd.readableBytes() - 4,
                "TOTAL_SIZE must equal the number of bytes following the total-size field");
        // The command must parse cleanly as a SEND command.
        BaseCommand parsed = new BaseCommand();
        header.markReaderIndex();
        header.skipBytes(4);
        parsed.parseFrom(header, cmdSize);
        assertEquals(parsed.getType(), BaseCommand.Type.SEND);
        header.resetReaderIndex();

        // Ref-counts must be balanced: the op owns the batch buffer exactly once, and it is freed once.
        assertEquals(payloadBuf.refCnt(), 1);
        cmd.release();
        assertEquals(payloadBuf.refCnt(), 0);

        batchMessageContainer.clear();
        messages.forEach(ReferenceCountUtil::safeRelease);
    }

    private ProducerImpl<?> createTestProducer(CompressionType compressionType) throws Exception {
        ProducerImpl<?> producer = mock(ProducerImpl.class);
        ProducerConfigurationData producerConfigurationData = new ProducerConfigurationData();
        producerConfigurationData.setCompressionType(compressionType);
        // Force the compression branch even for the tiny payloads used here, so the ZLIB case
        // actually compresses instead of silently taking the below-threshold no-compression path.
        producerConfigurationData.setCompressMinMsgBodySize(0);
        PulsarClientImpl pulsarClient = mock(PulsarClientImpl.class);
        when(pulsarClient.getMemoryLimitController()).thenReturn(mock(MemoryLimitController.class));
        try {
            Field clientFiled = HandlerState.class.getDeclaredField("client");
            clientFiled.setAccessible(true);
            clientFiled.set(producer, pulsarClient);
            Field confFiled = ProducerBase.class.getDeclaredField("conf");
            confFiled.setAccessible(true);
            confFiled.set(producer, producerConfigurationData);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        when(producer.getConfiguration()).thenReturn(producerConfigurationData);
        // Mirror ProducerImpl.applyCompression semantics: encode into a new buffer and release
        // the source, so the container's ownership-transfer logic is exercised the real way.
        when(producer.applyCompression(any())).thenAnswer(invocation -> {
            ByteBuf source = invocation.getArgument(0);
            ByteBuf compressed = PulsarByteBufAllocator.DEFAULT.buffer(source.readableBytes());
            compressed.writeBytes(source);
            source.release();
            return compressed;
        });
        return producer;
    }

    private List<MessageImpl<?>> addMessages(BatchMessageContainerImpl batchMessageContainer, int count) {
        List<MessageImpl<?>> messages = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            MessageMetadata messageMetadata = new MessageMetadata();
            messageMetadata.setSequenceId(i);
            messageMetadata.setProducerName("producer");
            messageMetadata.setPublishTime(System.currentTimeMillis());
            ByteBuffer payload = ByteBuffer.wrap(("payload-" + i).getBytes(StandardCharsets.UTF_8));
            MessageImpl<?> message = MessageImpl.create(messageMetadata, payload, Schema.BYTES, null);
            messages.add(message);
            batchMessageContainer.add(message, null);
        }
        return messages;
    }
}
