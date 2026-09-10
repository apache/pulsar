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
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.concurrent.ScheduledFuture;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.client.impl.metrics.LatencyHistogram;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ProducerImplTest {
    @Test
    public void testChunkedMessageCtxDeallocate() {
        int totalChunks = 3;
        ProducerImpl.ChunkedMessageCtx ctx = ProducerImpl.ChunkedMessageCtx.get(totalChunks);
        MessageIdImpl testMessageId = new MessageIdImpl(1, 1, 1);
        ctx.firstChunkMessageId = testMessageId;

        for (int i = 0; i < totalChunks; i++) {
            ProducerImpl.OpSendMsg opSendMsg =
                    ProducerImpl.OpSendMsg.create(
                            LatencyHistogram.NOOP,
                            MessageImpl.create(new MessageMetadata(), ByteBuffer.allocate(0), Schema.STRING, null),
                            null, 0, null);
            opSendMsg.chunkedMessageCtx = ctx;
            // check the ctx hasn't been deallocated.
            assertEquals(ctx.firstChunkMessageId, testMessageId);
            opSendMsg.recycle();
        }

        // check if the ctx is deallocated successfully.
        assertNull(ctx.firstChunkMessageId);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPopulateMessageSchema() {
        MessageImpl<?> msg = mock(MessageImpl.class);
        when(msg.hasReplicateFrom()).thenReturn(true);
        doReturn(mock(Schema.class)).when(msg).getSchemaInternal();
        when(msg.getSchemaInfoForReplicator()).thenReturn(null);
        ProducerImpl<?> producer = mock(ProducerImpl.class, withSettings()
                .defaultAnswer(Mockito.CALLS_REAL_METHODS));
        assertTrue(producer.populateMessageSchema(msg, null));
        verify(msg).setSchemaState(MessageImpl.SchemaState.Ready);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFailPendingMessagesSyncRetry()
            throws Exception {
        ProducerImpl<byte[]> producer =
                Mockito.mock(ProducerImpl.class, Mockito.CALLS_REAL_METHODS);
        // Disable batching
        Mockito.doReturn(false)
                .when(producer)
                .isBatchMessagingEnabled();

        // Stub semaphore release (not under test)
        Mockito.doNothing()
                .when(producer)
                .semaphoreRelease(Mockito.anyInt());

        // Stub client cleanup path (not under test)
        PulsarClientImpl client = Mockito.mock(PulsarClientImpl.class);
        Mockito.when(client.getMemoryLimitController())
                .thenReturn(Mockito.mock(MemoryLimitController.class));
        FieldUtils.writeField(producer, "client", client, true);

        // Real pending queue
        ProducerImpl.OpSendMsgQueue pendingQueue = new ProducerImpl.OpSendMsgQueue();
        FieldUtils.writeField(producer, "pendingMessages", pendingQueue, true);

        // OpSendMsg that retries reentrantly
        MessageImpl<?> msg = Mockito.mock(MessageImpl.class);
        Mockito.when(msg.getUncompressedSize()).thenReturn(10);
        ProducerImpl.OpSendMsg op = ProducerImpl.OpSendMsg.create(
                Mockito.mock(LatencyHistogram.class),
                msg,
                Mockito.mock(ByteBufPair.class),
                1L,
                Mockito.mock(SendCallback.class)
        );
        op.totalChunks = 1;
        op.chunkId = 0;
        op.numMessagesInBatch = 1;

        MessageImpl<?> retryMsg = Mockito.mock(MessageImpl.class);
        Mockito.when(retryMsg.getUncompressedSize()).thenReturn(10);

        // Override sendComplete to Reentrant retry via spy
        ProducerImpl.OpSendMsg firstSpy = Mockito.spy(op);
        Mockito.doAnswer(invocation -> {
            // Reentrant retry during callback
            ProducerImpl.OpSendMsg retryOp = ProducerImpl.OpSendMsg.create(
                    Mockito.mock(LatencyHistogram.class),
                    retryMsg,
                    Mockito.mock(ByteBufPair.class),
                    2L,
                    Mockito.mock(SendCallback.class)
            );
            retryOp.totalChunks = 1;
            retryOp.chunkId = 0;
            retryOp.numMessagesInBatch = 1;
            pendingQueue.add(retryOp);
            return null;
        }).when(firstSpy).sendComplete(Mockito.any());
        Mockito.doNothing()
                .when(firstSpy)
                .recycle();

        // Seed initial pending message
        pendingQueue.add(firstSpy);

        // Invoke failPendingMessages(null, ex)
        producer.failPendingMessages(null, new PulsarClientException.TimeoutException("timeout"));
        assertEquals(producer.getPendingQueueSize(), 1,
                "Retry Op should exist in the pending Queue");
        assertEquals(pendingQueue.peek().sequenceId, 2L,
                "Retry Op SequenceId should match with the one in pendingQueue");
    }

    /**
     * When the producer is in a terminal state, {@link ProducerImpl#processOpSendMsg} fails the message right away.
     * The memory reserved for that message must be given back to the {@link MemoryLimitController} exactly once:
     * releasing it twice makes {@code currentUsage} drift below zero and permanently disables the client memory
     * limit.
     */
    @Test
    public void testProcessOpSendMsgInTerminalStateReleasesMemoryOnce() throws Exception {
        for (ProducerImpl.State state : new ProducerImpl.State[] {
                ProducerImpl.State.Terminated, ProducerImpl.State.Closed, ProducerImpl.State.ProducerFenced}) {
            @SuppressWarnings("unchecked")
            ProducerImpl<byte[]> producer = Mockito.mock(ProducerImpl.class, Mockito.CALLS_REAL_METHODS);
            // Disable batching, so that releaseSemaphoreForSendOp() releases a single permit
            Mockito.doReturn(false).when(producer).isBatchMessagingEnabled();
            // The semaphore is not under test
            Mockito.doNothing().when(producer).semaphoreRelease(Mockito.anyInt());

            MemoryLimitController memoryLimitController = new MemoryLimitController(1024 * 1024);
            PulsarClientImpl client = Mockito.mock(PulsarClientImpl.class);
            Mockito.when(client.getMemoryLimitController()).thenReturn(memoryLimitController);
            FieldUtils.writeField(producer, "client", client, true);

            int uncompressedSize = 128;
            MessageImpl<?> msg = Mockito.mock(MessageImpl.class);
            Mockito.when(msg.getUncompressedSize()).thenReturn(uncompressedSize);
            // Build the op through the batch factory so that op.msg is null and the message size check is skipped
            ProducerImpl.OpSendMsg op = ProducerImpl.OpSendMsg.create(
                    Mockito.mock(LatencyHistogram.class),
                    Collections.<MessageImpl<?>>singletonList(msg),
                    Mockito.mock(ByteBufPair.class),
                    1L,
                    Mockito.mock(SendCallback.class),
                    0);

            memoryLimitController.forceReserveMemory(op.uncompressedSize);
            assertEquals(memoryLimitController.currentUsage(), uncompressedSize);

            producer.setState(state);
            producer.processOpSendMsg(op);

            assertEquals(memoryLimitController.currentUsage(), 0,
                    "The memory reserved for the message must be released exactly once in state " + state);
        }
    }

    private ProducerConfigurationData encryptedProducerConf() {
        ProducerConfigurationData conf = new ProducerConfigurationData();
        conf.setEncryptionKeys(new TreeSet<>(Collections.singleton("key")));
        conf.setCryptoKeyReader(mock(CryptoKeyReader.class));
        return conf;
    }

    /** A client mock whose stubs satisfy the {@link ProducerImpl} constructor. */
    private static PulsarClientImpl mockedPulsarClient() {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        when(client.newProducerId()).thenReturn(1L);
        when(client.getCnxPool()).thenReturn(mock(ConnectionPool.class));
        Timer timer = mock(Timer.class);
        when(timer.newTimeout(any(), anyLong(), any())).thenReturn(mock(Timeout.class));
        when(client.timer()).thenReturn(timer);
        ClientConfigurationData clientConfigurationData = new ClientConfigurationData();
        clientConfigurationData.setStatsIntervalSeconds(0);
        when(client.getConfiguration()).thenReturn(clientConfigurationData);
        when(client.instrumentProvider()).thenReturn(InstrumentProvider.NOOP);
        EventLoopGroup eventLoopGroup = mock(EventLoopGroup.class);
        when(eventLoopGroup.scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any()))
                .thenReturn(mock(ScheduledFuture.class));
        when(client.eventLoopGroup()).thenReturn(eventLoopGroup);
        return client;
    }

    /**
     * A producer mock built through the real constructor, so the {@code conf}, {@code log}, {@code client} and
     * {@code msgCrypto} fields hold real values instead of needing reflection: {@code conf.setMessageCrypto()}
     * installs a test crypto through the same constructor branch production uses.
     */
    @SuppressWarnings("unchecked")
    private static ProducerImpl<byte[]> constructProducer(PulsarClientImpl client,
            ProducerConfigurationData conf) {
        return mock(ProducerImpl.class, withSettings()
                .useConstructor(client, "persistent://public/default/producer-impl-test", conf,
                        new CompletableFuture<>(), 0, Schema.BYTES, null, Optional.empty())
                .defaultAnswer(CALLS_REAL_METHODS));
    }

    /**
     * When encryption fails in any way, the partially built encrypted buffer must be released instead of
     * leaking; the source payload stays with the caller (the batch container decides its fate).
     */
    @Test
    public void testEncryptMessageReleasesPartialBufferOnFailure() throws Exception {
        MessageCrypto<?, ?> msgCrypto = mock(MessageCrypto.class);
        when(msgCrypto.getMaxOutputSize(anyInt())).thenReturn(64);
        doThrow(new RuntimeException("mocked encryption failure"))
                .when(msgCrypto).encrypt(any(), any(), any(), any(), any());
        ProducerConfigurationData conf = encryptedProducerConf();
        conf.setMessageCrypto(msgCrypto);
        ProducerImpl<byte[]> producer = constructProducer(mockedPulsarClient(), conf);

        ByteBuf partial = Unpooled.buffer(64);
        doReturn(partial).when(producer).allocateEncryptedBuffer(anyInt());

        ByteBuf source = Unpooled.buffer(8);
        assertThatThrownBy(() -> producer.encryptMessage(new MessageMetadata(), source))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("mocked encryption failure");
        assertEquals(partial.refCnt(), 0, "the partially built encrypted buffer must not leak");
        assertEquals(source.refCnt(), 1, "the source payload stays with the caller");
        source.release();
    }

    /** The SEND crypto-failure action returns the unencrypted source; the partial buffer must not leak. */
    @Test
    public void testEncryptMessageCryptoFailureActionSendReleasesPartialBuffer() throws Exception {
        MessageCrypto<?, ?> msgCrypto = mock(MessageCrypto.class);
        when(msgCrypto.getMaxOutputSize(anyInt())).thenReturn(64);
        doThrow(new PulsarClientException("mocked encryption failure"))
                .when(msgCrypto).encrypt(any(), any(), any(), any(), any());
        ProducerConfigurationData conf = encryptedProducerConf();
        conf.setMessageCrypto(msgCrypto);
        conf.setCryptoFailureAction(ProducerCryptoFailureAction.SEND);
        ProducerImpl<byte[]> producer = constructProducer(mockedPulsarClient(), conf);

        ByteBuf partial = Unpooled.buffer(64);
        doReturn(partial).when(producer).allocateEncryptedBuffer(anyInt());

        ByteBuf source = Unpooled.buffer(8);
        assertSame(producer.encryptMessage(new MessageMetadata(), source), source);
        assertEquals(partial.refCnt(), 0, "the partially built encrypted buffer must not leak");
        source.release();
    }

    /** A failed command serialization must release the payload instead of orphaning it. */
    @Test
    public void testSendMessageFailureReleasesPayload() throws Exception {
        ProducerImpl<byte[]> producer = mock(ProducerImpl.class, CALLS_REAL_METHODS);
        doThrow(new RuntimeException("mocked serialization failure"))
                .when(producer)
                .sendMessage(anyLong(), anyLong(), anyInt(), any(), any(), any());

        ByteBuf payload = Unpooled.buffer(8);
        assertThatThrownBy(() -> producer.sendMessageOrReleasePayload(
                1, 1, 1, null, new MessageMetadata(), payload))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("mocked serialization failure");
        assertEquals(payload.refCnt(), 0, "the payload must be released on a failed serialization");
    }

    /** A failing compression stage must release the source payload the codec left with the caller. */
    @Test
    public void testApplyCompressionFailureReleasesSource() throws Exception {
        ProducerImpl<byte[]> producer = mock(ProducerImpl.class, CALLS_REAL_METHODS);
        doThrow(new RuntimeException("mocked compression failure"))
                .when(producer)
                .applyCompression(any());

        ByteBuf source = Unpooled.buffer(8);
        assertThatThrownBy(() -> producer.applyCompressionOrReleaseSource(source))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("mocked compression failure");
        assertEquals(source.refCnt(), 0, "the source payload must be released on a failed compression");
    }

    /** A failing encryption stage must release the source payload encryptMessage() left with the caller. */
    @Test
    public void testEncryptMessageFailureReleasesSource() throws Exception {
        ProducerImpl<byte[]> producer = mock(ProducerImpl.class, CALLS_REAL_METHODS);
        doThrow(new PulsarClientException("mocked encryption failure"))
                .when(producer)
                .encryptMessage(any(), any());

        ByteBuf source = Unpooled.buffer(8);
        assertThatThrownBy(() -> producer.encryptMessageOrReleaseSource(new MessageMetadata(), source))
                .isInstanceOf(PulsarClientException.class)
                .hasMessageContaining("mocked encryption failure");
        assertEquals(source.refCnt(), 0, "the source payload must be released on a failed encryption");
    }

    /**
     * An op whose command is deferred until the schema is registered holds its payload; when the op is
     * failed before the command was built (send timeout, producer close), recycle() must release it.
     */
    @Test
    public void testPendingSchemaOpPayloadReleasedOnFailure() {
        ByteBuf payload = Unpooled.buffer(8);
        ProducerImpl.OpSendMsg op = ProducerImpl.OpSendMsg.create(
                mock(LatencyHistogram.class),
                mock(MessageImpl.class),
                null,
                1L,
                mock(SendCallback.class));
        op.pendingPayload = payload;

        op.recycle();

        assertEquals(payload.refCnt(), 0, "the deferred payload must be released when the op is recycled");
    }

    /**
     * A deferred command whose first construction fails must keep the payload alive: the op stays pending
     * and the next resend (reconnect) rebuilds the command from the same buffer instead of touching a
     * released one.
     */
    @Test
    public void testDeferredCommandConstructionFailureThenRecovery() throws Exception {
        ProducerImpl<byte[]> producer = mock(ProducerImpl.class, CALLS_REAL_METHODS);
        AtomicInteger sendCalls = new AtomicInteger();
        ByteBufPair builtCmd = mock(ByteBufPair.class);
        doAnswer(invocation -> {
            if (sendCalls.incrementAndGet() == 1) {
                throw new RuntimeException("mocked header allocation failure");
            }
            return builtCmd;
        }).when(producer).sendMessage(anyLong(), anyLong(), anyInt(), any(), any(), any());

        ByteBuf payload = Unpooled.buffer(8);
        ProducerImpl.OpSendMsg op = ProducerImpl.OpSendMsg.create(
                mock(LatencyHistogram.class),
                mock(MessageImpl.class),
                null,
                1L,
                mock(SendCallback.class));
        op.pendingPayload = payload;

        // First construction fails: the payload must stay with the op for the retry.
        assertThatThrownBy(() -> producer.buildDeferredCommand(op, new MessageMetadata(), 1, 1, 1, null, -1))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("mocked header allocation failure");
        assertEquals(payload.refCnt(), 1, "the payload must stay alive for the next resend");
        assertEquals(op.pendingPayload, payload, "the op keeps owning the deferred payload");
        assertNull(op.cmd);

        // The resend after reconnect rebuilds from the same buffer and hands it to the command.
        producer.buildDeferredCommand(op, new MessageMetadata(), 1, 1, 1, null, -1);
        assertEquals(op.cmd, builtCmd, "the retry must rebuild the command");
        assertNull(op.pendingPayload, "the payload's ownership moved into the command");
        // Recycling the op now must not release the payload again.
        op.recycle();
        assertEquals(payload.refCnt(), 1, "the payload belongs to the command now, not to the op");
        payload.release();
    }

    /**
     * Exercises the send-path wiring: {@code sendAsync()} itself must route through the stage helpers, so a
     * failing compression or serialization stage releases the payload buffers instead of orphaning them. The
     * helper-level tests above cannot detect a call site reverting to the bare method.
     */
    @Test
    public void testSendPathFailureReleasesPayloadThroughTheStageHelpers() throws Exception {
        ProducerConfigurationData conf = new ProducerConfigurationData();
        conf.setBatchingEnabled(false);
        conf.setCompressMinMsgBodySize(0);
        PulsarClientImpl client = mockedPulsarClient();
        when(client.getMemoryLimitController()).thenReturn(new MemoryLimitController(1024 * 1024));
        ProducerImpl<byte[]> producer = constructProducer(client, conf);
        producer.setState(ProducerImpl.State.Ready);

        // A failing compression stage must release the message payload. sendAsync() runs this stage on the
        // caller thread and lets the runtime error propagate; the wiring under test is the buffer release.
        doThrow(new RuntimeException("mocked compression failure"))
                .when(producer).applyCompression(any());
        MessageImpl<byte[]> first = newMessage("first");
        SendCallback firstCallback = mock(SendCallback.class);
        assertThatThrownBy(() -> producer.sendAsync(first, firstCallback))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("mocked compression failure");
        verify(firstCallback, never()).sendComplete(any(), any());
        assertEquals(first.getDataBuffer().refCnt(), 0,
                "the payload must be released by the compression stage");

        // A failing command serialization must release the compressed payload handed to it.
        ByteBuf compressed = Unpooled.buffer(8);
        doAnswer(invocation -> {
            ByteBuf source = invocation.getArgument(0);
            source.release();
            return compressed;
        }).when(producer).applyCompression(any());
        doAnswer(invocation -> invocation.getArgument(1)).when(producer).encryptMessage(any(), any());
        doThrow(new RuntimeException("mocked serialization failure"))
                .when(producer).sendMessage(anyLong(), anyLong(), anyInt(), any(), any(), any());

        MessageImpl<byte[]> second = newMessage("second");
        SendCallback secondCallback = mock(SendCallback.class);
        producer.sendAsync(second, secondCallback);
        verify(secondCallback).sendComplete(any(), any());
        assertEquals(compressed.refCnt(), 0,
                "the compressed payload must be released by the serialization stage");
    }

    private static MessageImpl<byte[]> newMessage(String content) {
        MessageMetadata metadata = new MessageMetadata();
        metadata.setPublishTime(System.currentTimeMillis());
        return MessageImpl.create(metadata,
                ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8)), Schema.BYTES, null);
    }
}
