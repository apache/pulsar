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
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.WrappedByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
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
        doAnswer(invocation -> {
            if (fail.get()) {
                throw new RuntimeException("mocked encryption failure");
            }
            return invocation.getArgument(1);
        }).when(producer).encryptMessage(any(), any());
        doAnswer(invocation -> {
            ByteBuf payload = invocation.getArgument(5);
            ByteBuf header = PulsarByteBufAllocator.DEFAULT.buffer();
            header.writeInt(4 + 4 + payload.readableBytes());
            header.writeInt(0);
            return ByteBufPair.get(header, payload);
        }).when(producer).sendMessage(anyLong(), anyLong(), anyLong(), anyInt(), any(), any());

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
        doAnswer(invocation -> {
            // Real encryption allocates a new buffer and releases the source payload.
            ByteBuf source = invocation.getArgument(1);
            ByteBuf encrypted = PulsarByteBufAllocator.DEFAULT.buffer(source.readableBytes());
            encrypted.writeBytes(source);
            source.release();
            if (failSend.get()) {
                firstEncryptedRef.set(encrypted);
            }
            return encrypted;
        }).when(producer).encryptMessage(any(), any());
        doAnswer(invocation -> {
            if (failSend.getAndSet(false)) {
                throw new RuntimeException("mocked send failure");
            }
            ByteBuf payload = invocation.getArgument(5);
            ByteBuf header = PulsarByteBufAllocator.DEFAULT.buffer();
            header.writeInt(4 + 4 + payload.readableBytes());
            header.writeInt(0);
            return ByteBufPair.get(header, payload);
        }).when(producer).sendMessage(anyLong(), anyLong(), anyLong(), anyInt(), any(), any());

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
            doAnswer(invocation -> {
                if (failOnce.getAndSet(false)) {
                    throw new RuntimeException("mocked encryption failure");
                }
                return invocation.getArgument(1);
            }).when(producer).encryptMessage(any(), any());
        } else {
            doAnswer(invocation -> invocation.getArgument(1)).when(producer).encryptMessage(any(), any());
        }
        doAnswer(invocation -> {
            if (failAtSend && failOnce.getAndSet(false)) {
                throw new RuntimeException("mocked send failure");
            }
            MessageMetadata metadata = invocation.getArgument(4);
            ByteBuf payload = invocation.getArgument(5);
            return Commands.newSend(0L, metadata.hasSequenceId() ? metadata.getSequenceId() : 0L, 1,
                    Commands.ChecksumType.Crc32c, metadata, payload);
        }).when(producer).sendMessage(anyLong(), anyLong(), anyLong(), anyInt(), any(), any());

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
        // The command must parse cleanly as a SEND command. Skip both length fields: TOTAL_SIZE and
        // CMD_SIZE (getInt above reads absolutely and does not move the reader index).
        BaseCommand parsed = new BaseCommand();
        header.markReaderIndex();
        header.skipBytes(8);
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

    /**
     * In multi-batch mode, a later sub-batch can fail to build after an earlier one already produced its
     * operation. The already-built operations never reach the send queue, so their commands must be released
     * by the container — otherwise every failed flush leaks command buffers, and repeated retries grow the
     * direct memory usage.
     */
    @Test(dataProvider = "compressionTypes")
    public void testMultiBatchesPartialBuildFailureReleasesBuiltOps(CompressionType compressionType)
            throws Exception {
        ProducerImpl<?> producer = createTestProducer(compressionType);
        doAnswer(invocation -> invocation.getArgument(1)).when(producer).encryptMessage(any(), any());
        List<ByteBufPair> builtPairs = new ArrayList<>();
        // The pair clears its component references when released, so track the buffers at build time.
        List<ByteBuf> builtHeaders = new ArrayList<>();
        List<ByteBuf> builtPayloads = new ArrayList<>();
        AtomicInteger sendCalls = new AtomicInteger();
        doAnswer(invocation -> {
            if (sendCalls.incrementAndGet() == 2) {
                throw new RuntimeException("mocked second sub-batch failure");
            }
            ByteBuf payload = invocation.getArgument(5);
            ByteBuf header = PulsarByteBufAllocator.DEFAULT.buffer();
            header.writeInt(4 + 4 + payload.readableBytes());
            header.writeInt(0);
            ByteBufPair pair = ByteBufPair.get(header, payload);
            builtPairs.add(pair);
            builtHeaders.add(header);
            builtPayloads.add(payload);
            return pair;
        }).when(producer).sendMessage(anyLong(), anyLong(), anyLong(), anyInt(), any(), any());

        BatchMessageKeyBasedContainer container = new BatchMessageKeyBasedContainer();
        container.setProducer(producer);
        List<MessageImpl<?>> messages = new ArrayList<>();
        try {
            for (int i = 0; i < 4; i++) {
                MessageMetadata messageMetadata = new MessageMetadata();
                messageMetadata.setSequenceId(i);
                messageMetadata.setProducerName("producer");
                messageMetadata.setPublishTime(System.currentTimeMillis());
                messageMetadata.setPartitionKey(i < 2 ? "a" : "b");
                ByteBuffer payload = ByteBuffer.wrap(("payload-" + i).getBytes(StandardCharsets.UTF_8));
                MessageImpl<?> message = MessageImpl.create(messageMetadata, payload, Schema.BYTES, null);
                messages.add(message);
                container.add(message, null);
            }

            // Sub-batch "a" builds its operation, sub-batch "b" fails: the built command must not leak.
            assertThatThrownBy(container::createOpSendMsgs)
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("mocked second");
            assertEquals(builtPairs.size(), 1);
            // The pair itself is released (returned to its recycler) and the op recycled, not just the
            // components freed.
            assertEquals(builtPairs.get(0).refCnt(), 0);
            assertEquals(builtHeaders.get(0).refCnt(), 0);
            if (compressionType != CompressionType.NONE) {
                // Compression handed the payload over to the command, so it must be released as well.
                assertEquals(builtPayloads.get(0).refCnt(), 0);
            } else {
                // Without compression the container still owns the payload buffer and reuses it on retry.
                assertEquals(builtPayloads.get(0).refCnt(), 1);
            }

            // All messages stay in their sub-batches and the retry produces a complete batch again.
            assertEquals(container.getNumMessagesInBatch(), 4);
            container.resetPayloadAfterFailedPublishing();
            List<ProducerImpl.OpSendMsg> ops = container.createOpSendMsgs();
            assertEquals(ops.size(), 2);
            ops.forEach(op -> op.cmd.release());
            assertEquals(builtPairs.get(1).refCnt(), 0);
            assertEquals(builtPairs.get(2).refCnt(), 0);
            container.clear();
        } finally {
            messages.forEach(ReferenceCountUtil::safeRelease);
        }
    }

    /**
     * The entry-bucket container builds its buckets through the same loop as the key-based container, so a later
     * bucket failing to build must release the operations already built there as well: a regression in this loop
     * would otherwise leak command buffers on every failed flush.
     */
    @Test(dataProvider = "compressionTypes")
    public void testEntryBucketPartialBuildFailureReleasesBuiltOps(CompressionType compressionType)
            throws Exception {
        ProducerImpl<?> producer = createTestProducer(compressionType);
        doAnswer(invocation -> invocation.getArgument(1)).when(producer).encryptMessage(any(), any());
        List<ByteBufPair> builtPairs = new ArrayList<>();
        List<ByteBuf> builtHeaders = new ArrayList<>();
        List<ByteBuf> builtPayloads = new ArrayList<>();
        AtomicInteger sendCalls = new AtomicInteger();
        doAnswer(invocation -> {
            if (sendCalls.incrementAndGet() == 2) {
                throw new RuntimeException("mocked second bucket failure");
            }
            ByteBuf payload = invocation.getArgument(5);
            ByteBuf header = PulsarByteBufAllocator.DEFAULT.buffer();
            header.writeInt(4 + 4 + payload.readableBytes());
            header.writeInt(0);
            ByteBufPair pair = ByteBufPair.get(header, payload);
            builtPairs.add(pair);
            builtHeaders.add(header);
            builtPayloads.add(payload);
            return pair;
        }).when(producer).sendMessage(anyLong(), anyLong(), anyLong(), anyInt(), any(), any());

        // "key-1" and "key-2" hash into different entry buckets with these splits.
        EntryBucketBatchContainer container =
                new EntryBucketBatchContainer(Arrays.asList(0x4000, 0x8000, 0xC000));
        container.setProducer(producer);
        List<MessageImpl<?>> messages = new ArrayList<>();
        try {
            for (int i = 0; i < 4; i++) {
                MessageMetadata messageMetadata = new MessageMetadata();
                messageMetadata.setSequenceId(i);
                messageMetadata.setProducerName("producer");
                messageMetadata.setPublishTime(System.currentTimeMillis());
                messageMetadata.setPartitionKey(i < 2 ? "key-1" : "key-2");
                ByteBuffer payload = ByteBuffer.wrap(("payload-" + i).getBytes(StandardCharsets.UTF_8));
                MessageImpl<?> message = MessageImpl.create(messageMetadata, payload, Schema.BYTES, null);
                messages.add(message);
                container.add(message, null);
            }

            // Bucket "key-1" builds its operation, bucket "key-2" fails: the built command must not leak.
            assertThatThrownBy(container::createOpSendMsgs)
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("mocked second");
            assertEquals(builtPairs.size(), 1);
            assertEquals(builtPairs.get(0).refCnt(), 0);
            assertEquals(builtHeaders.get(0).refCnt(), 0);
            if (compressionType != CompressionType.NONE) {
                // Compression handed the payload over to the command, so it must be released as well.
                assertEquals(builtPayloads.get(0).refCnt(), 0);
            } else {
                // Without compression the container still owns the payload buffer and reuses it on retry.
                assertEquals(builtPayloads.get(0).refCnt(), 1);
            }

            // All messages stay in their buckets and the retry produces a complete batch again.
            assertEquals(container.getNumMessagesInBatch(), 4);
            container.resetPayloadAfterFailedPublishing();
            List<ProducerImpl.OpSendMsg> ops = container.createOpSendMsgs();
            assertEquals(ops.size(), 2);
            ops.forEach(op -> op.cmd.release());
            assertEquals(builtPairs.get(1).refCnt(), 0);
            assertEquals(builtPairs.get(2).refCnt(), 0);
            container.clear();
        } finally {
            messages.forEach(ReferenceCountUtil::safeRelease);
        }
    }

    /**
     * A single message whose header + payload exceed the max message size is rejected after the command was
     * already built. Without compression or encryption the command's payload IS the container's batch buffer,
     * taken without a retain: releasing the command must therefore be the release of the container's claim too.
     * The container used to keep reporting ownership after the command release, so the internal discard()
     * released the already-freed buffer a second time — the failure was swallowed by safeRelease, but if the
     * buffer had been recycled in between, the second release corrupted a live buffer. The batch buffer must
     * end up freed with release() invoked exactly once.
     */
    @Test
    public void oversizedSingleMessageReleasesTheBatchBufferExactlyOnce() throws Exception {
        ProducerImpl<?> producer = createTestProducer(CompressionType.NONE);
        doAnswer(invocation -> {
            ByteBuf payload = invocation.getArgument(5);
            ByteBuf header = PulsarByteBufAllocator.DEFAULT.buffer();
            header.writeInt(4 + 4 + payload.readableBytes());
            header.writeInt(0);
            return ByteBufPair.get(header, payload);
        }).when(producer).sendMessage(anyLong(), anyLong(), anyLong(), anyInt(), any(), any());

        List<ReleaseCountingByteBuf> containerBuffers = new ArrayList<>();
        ByteBufAllocator recordingAllocator = mock(ByteBufAllocator.class);
        doAnswer(invocation -> {
            ReleaseCountingByteBuf buffer =
                    new ReleaseCountingByteBuf(Unpooled.buffer((int) invocation.getArgument(0)));
            containerBuffers.add(buffer);
            return buffer;
        }).when(recordingAllocator).buffer(anyInt());

        BatchMessageContainerImpl container = new BatchMessageContainerImpl(recordingAllocator);
        container.setProducer(producer);

        MessageImpl<?> message = createMessage(0, Commands.DEFAULT_MAX_MESSAGE_SIZE + 2048);
        try {
            container.add(message, null);
            assertNull(container.createOpSendMsg(), "an oversized single message must not produce an op");

            assertEquals(containerBuffers.size(), 1, "the batch buffer is the container's only allocation");
            ReleaseCountingByteBuf batchBuffer = containerBuffers.get(0);
            assertEquals(batchBuffer.refCnt(), 0, "the batch buffer must be freed");
            assertEquals(batchBuffer.releases(), 1,
                    "the batch buffer must be released exactly once: the command release is the release of "
                            + "the container's claim, so discard() must not release it again");
        } finally {
            ReferenceCountUtil.safeRelease(message);
        }
    }

    /**
     * The multi-message variant of the same defect: the oversized check used to release the payload
     * unconditionally, and without compression or encryption that payload is the container's own batch buffer,
     * which discard() then released a second time. The orphan-aware release (skip when the container still
     * owns the buffer — discard() performs the single release) must hold here too.
     */
    @Test
    public void oversizedBatchReleasesTheBatchBufferExactlyOnce() throws Exception {
        ProducerImpl<?> producer = createTestProducer(CompressionType.NONE);

        List<ReleaseCountingByteBuf> containerBuffers = new ArrayList<>();
        ByteBufAllocator recordingAllocator = mock(ByteBufAllocator.class);
        doAnswer(invocation -> {
            ReleaseCountingByteBuf buffer =
                    new ReleaseCountingByteBuf(Unpooled.buffer((int) invocation.getArgument(0)));
            containerBuffers.add(buffer);
            return buffer;
        }).when(recordingAllocator).buffer(anyInt());

        BatchMessageContainerImpl container = new BatchMessageContainerImpl(recordingAllocator);
        container.setProducer(producer);

        // Two messages whose combined payload exceeds the 5MB limit, checked before any command is built.
        int halfLimit = Commands.DEFAULT_MAX_MESSAGE_SIZE / 2;
        MessageImpl<?> first = createMessage(0, halfLimit + 1024);
        MessageImpl<?> second = createMessage(1, halfLimit + 1024);
        try {
            container.add(first, null);
            container.add(second, null);
            assertNull(container.createOpSendMsg(), "an oversized batch must not produce an op");

            assertEquals(containerBuffers.size(), 1, "the batch buffer is the container's only allocation");
            ReleaseCountingByteBuf batchBuffer = containerBuffers.get(0);
            assertEquals(batchBuffer.refCnt(), 0, "the batch buffer must be freed");
            assertEquals(batchBuffer.releases(), 1,
                    "the batch buffer must be released exactly once: discard() owns the release of a buffer "
                            + "the container never handed off");
        } finally {
            ReferenceCountUtil.safeRelease(first);
            ReferenceCountUtil.safeRelease(second);
        }
    }

    private MessageImpl<?> createMessage(long sequenceId, int payloadSize) {
        MessageMetadata messageMetadata = new MessageMetadata();
        messageMetadata.setSequenceId(sequenceId);
        messageMetadata.setProducerName("producer");
        messageMetadata.setPublishTime(System.currentTimeMillis());
        ByteBuffer payload = ByteBuffer.wrap(new byte[payloadSize]);
        return MessageImpl.create(messageMetadata, payload, Schema.BYTES, null);
    }

    /** Delegates everything and counts {@code release()} invocations, making a swallowed double release visible. */
    private static final class ReleaseCountingByteBuf extends WrappedByteBuf {

        private int releases;

        ReleaseCountingByteBuf(ByteBuf buffer) {
            super(buffer);
        }

        @Override
        public boolean release() {
            releases++;
            return super.release();
        }

        @Override
        public boolean release(int decrement) {
            releases++;
            return super.release(decrement);
        }

        int releases() {
            return releases;
        }
    }

    private ProducerImpl<?> createTestProducer(CompressionType compressionType) throws Exception {
        ProducerConfigurationData producerConfigurationData = new ProducerConfigurationData();
        producerConfigurationData.setCompressionType(compressionType);
        // Force the compression branch even for the tiny payloads used here, so the ZLIB case
        // actually compresses instead of silently taking the below-threshold no-compression path.
        producerConfigurationData.setCompressMinMsgBodySize(0);
        PulsarClientImpl pulsarClient = mock(PulsarClientImpl.class);
        when(pulsarClient.newProducerId()).thenReturn(1L);
        when(pulsarClient.getCnxPool()).thenReturn(mock(ConnectionPool.class));
        when(pulsarClient.getMemoryLimitController()).thenReturn(mock(MemoryLimitController.class));
        Timer timer = mock(Timer.class);
        when(timer.newTimeout(any(), anyLong(), any())).thenReturn(mock(Timeout.class));
        when(pulsarClient.timer()).thenReturn(timer);
        ClientConfigurationData clientConfigurationData = new ClientConfigurationData();
        clientConfigurationData.setStatsIntervalSeconds(0);
        when(pulsarClient.getConfiguration()).thenReturn(clientConfigurationData);
        when(pulsarClient.instrumentProvider()).thenReturn(InstrumentProvider.NOOP);

        ProducerImpl<?> producer = mock(ProducerImpl.class, withSettings()
                .useConstructor(pulsarClient, "persistent://public/default/batch-container-test",
                        producerConfigurationData, new CompletableFuture<>(), 0, Schema.BYTES,
                        null, Optional.empty())
                .defaultAnswer(CALLS_REAL_METHODS));
        // Mirror ProducerImpl.applyCompression semantics: encode into a new buffer and release
        // the source, so the container's ownership-transfer logic is exercised the real way.
        // doAnswer-form stubbing: when-form would execute the real method while registering.
        doAnswer(invocation -> {
            ByteBuf source = invocation.getArgument(0);
            ByteBuf compressed = PulsarByteBufAllocator.DEFAULT.buffer(source.readableBytes());
            compressed.writeBytes(source);
            source.release();
            return compressed;
        }).when(producer).applyCompression(any());
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
