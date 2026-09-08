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
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
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

    /**
     * When encryption fails in any way, the partially built encrypted buffer must be released instead of
     * leaking; the source payload stays with the caller (the batch container decides its fate).
     */
    @Test
    public void testEncryptMessageReleasesPartialBufferOnFailure() throws Exception {
        ProducerImpl<byte[]> producer = mock(ProducerImpl.class, CALLS_REAL_METHODS);
        FieldUtils.writeField(producer, "conf", encryptedProducerConf(), true);

        ByteBuf partial = Unpooled.buffer(64);
        doReturn(partial).when(producer).allocateEncryptedBuffer(anyInt());
        MessageCrypto<?, ?> msgCrypto = mock(MessageCrypto.class);
        when(msgCrypto.getMaxOutputSize(anyInt())).thenReturn(64);
        doThrow(new RuntimeException("mocked encryption failure"))
                .when(msgCrypto).encrypt(any(), any(), any(), any(), any());
        FieldUtils.writeField(producer, "msgCrypto", msgCrypto, true);

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
        ProducerImpl<byte[]> producer = mock(ProducerImpl.class, CALLS_REAL_METHODS);
        // Mock instances skip field initializers: provide the logger the SEND fallback branch uses.
        FieldUtils.writeField(producer, "log",
                mock(io.github.merlimat.slog.Logger.class, RETURNS_DEEP_STUBS), true);
        ProducerConfigurationData conf = encryptedProducerConf();
        conf.setCryptoFailureAction(ProducerCryptoFailureAction.SEND);
        FieldUtils.writeField(producer, "conf", conf, true);

        ByteBuf partial = Unpooled.buffer(64);
        doReturn(partial).when(producer).allocateEncryptedBuffer(anyInt());
        MessageCrypto<?, ?> msgCrypto = mock(MessageCrypto.class);
        when(msgCrypto.getMaxOutputSize(anyInt())).thenReturn(64);
        doThrow(new PulsarClientException("mocked encryption failure"))
                .when(msgCrypto).encrypt(any(), any(), any(), any(), any());
        FieldUtils.writeField(producer, "msgCrypto", msgCrypto, true);

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
}
