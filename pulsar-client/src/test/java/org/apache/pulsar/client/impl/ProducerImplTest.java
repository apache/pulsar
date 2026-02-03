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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
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
    public void testPopulateMessageSchema() {
        MessageImpl<?> msg = mock(MessageImpl.class);
        when(msg.hasReplicateFrom()).thenReturn(true);
        when(msg.getSchemaInternal()).thenReturn(mock(Schema.class));
        when(msg.getSchemaInfoForReplicator()).thenReturn(null);
        ProducerImpl<?> producer = mock(ProducerImpl.class, withSettings()
                .defaultAnswer(Mockito.CALLS_REAL_METHODS));
        assertTrue(producer.populateMessageSchema(msg, null));
        verify(msg).setSchemaState(MessageImpl.SchemaState.Ready);
    }

    @Test
    public void testFailPendingMessagesSyncRetry()
            throws Exception {
        ProducerImpl<byte[]> producer =
                Mockito.mock(ProducerImpl.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doNothing()
                .when(producer)
                .semaphoreRelease(Mockito.anyInt());
        Mockito.doReturn(false)
                .when(producer)
                .isBatchMessagingEnabled();

        // Real pending queue
        ProducerImpl.OpSendMsgQueue pendingQueue = new ProducerImpl.OpSendMsgQueue();
        Field pendingField = ProducerImpl.class.getDeclaredField("pendingMessages");
        pendingField.setAccessible(true);
        pendingField.set(producer, pendingQueue);

        // Stub client cleanup path (not under test)
        PulsarClientImpl client = Mockito.mock(PulsarClientImpl.class);
        Mockito.when(client.getMemoryLimitController())
                .thenReturn(Mockito.mock(MemoryLimitController.class));
        Field clientField = HandlerState.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(producer, client);

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
}
