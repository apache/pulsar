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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ProducerImpl.OpSendMsg;
import org.apache.pulsar.client.impl.ProducerImpl.OpSendMsgQueue;
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
     * Regression test for the send-timeout vs in-flight-write race (scenario B):
     *
     * <p>A batch frame is handed to a connection's event loop for writing, then the connection drops and the
     * producer enters the reconnect window ({@code cnx() == null}). If the send timeout fires in that window,
     * {@code failPendingMessages(null, ex)} runs on the timer thread. It must NOT release the op's cmd or recycle
     * the op there: the write may still be in-flight on the (old) connection's event loop, and releasing the
     * buffers on the timer thread would let new batches reuse (and overwrite) the memory that the in-flight write
     * is still reading, corrupting the frame. The cmd release and op recycle must be deferred to the write's event
     * loop so that they are serialized after the in-flight write.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testSendTimeoutDuringInFlightWriteDefersCmdRelease() throws Exception {
        ProducerImpl<byte[]> producer = Mockito.mock(ProducerImpl.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(false).when(producer).isBatchMessagingEnabled();
        Mockito.doNothing().when(producer).semaphoreRelease(Mockito.anyInt());

        PulsarClientImpl client = Mockito.mock(PulsarClientImpl.class);
        Mockito.when(client.getMemoryLimitController())
                .thenReturn(Mockito.mock(MemoryLimitController.class));
        FieldUtils.writeField(producer, "client", client, true);

        OpSendMsgQueue pendingQueue = new OpSendMsgQueue();
        FieldUtils.writeField(producer, "pendingMessages", pendingQueue, true);

        // The connection event loop the write was handed to. It is still in-flight (the write callback is queued
        // and has not run yet), so it must serialize the cmd release after the write.
        List<Runnable> submittedTasks = new ArrayList<>();
        EventLoop writeEventLoop = Mockito.mock(EventLoop.class);
        doAnswer(invocation -> {
            submittedTasks.add(invocation.getArgument(0));
            return null;
        }).when(writeEventLoop).execute(any(Runnable.class));

        ByteBufPair cmd = ByteBufPair.get(
                Unpooled.buffer().writeBytes("frame-header".getBytes(StandardCharsets.UTF_8)),
                Unpooled.buffer().writeBytes("batch-payload".getBytes(StandardCharsets.UTF_8)));
        MessageImpl<?> msg = Mockito.mock(MessageImpl.class);
        Mockito.when(msg.getUncompressedSize()).thenReturn(10);
        OpSendMsg op = OpSendMsg.create(
                Mockito.mock(LatencyHistogram.class), msg, cmd, 1L, Mockito.mock(SendCallback.class));
        op.totalChunks = 1;
        op.chunkId = 0;
        op.numMessagesInBatch = 1;
        OpSendMsg opSpy = spy(op);
        opSpy.writeEventLoop = writeEventLoop;
        pendingQueue.add(opSpy);

        // The send timeout fires on the timer thread while cnx() == null and the write is still in-flight.
        Thread timerThread = new Thread(() ->
                producer.failPendingMessages(null, new PulsarClientException.TimeoutException("timeout")));
        timerThread.start();
        timerThread.join();

        // The timer thread must not have released the cmd or recycled the op: the buffers must stay alive until
        // the in-flight write completes, otherwise a new batch could reuse them and corrupt the frame.
        assertEquals(cmd.refCnt(), 1,
                "cmd must not be released on the timer thread while a write is in-flight");
        verify(opSpy, never()).recycle();
        assertEquals(submittedTasks.size(), 1,
                "cmd release must be deferred to the in-flight write's event loop");
        assertEquals(pendingQueue.size(), 0, "timed-out op must be removed from the pending queue");

        assertTrue(cmd.getFirst().readableBytes() > 0 && cmd.getSecond().readableBytes() > 0);

        submittedTasks.get(0).run();
        assertEquals(cmd.refCnt(), 0, "cmd must be released after the in-flight write completes");
        verify(opSpy).recycle();
    }

    /**
     * When an op was never handed to a connection (no write can be in-flight), the timeout path must keep
     * releasing the cmd and recycling the op inline to avoid holding them until the next event loop tick.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testSendTimeoutReleasesCmdInlineWhenNoWriteInFlight() throws Exception {
        ProducerImpl<byte[]> producer = Mockito.mock(ProducerImpl.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(false).when(producer).isBatchMessagingEnabled();
        Mockito.doNothing().when(producer).semaphoreRelease(Mockito.anyInt());

        PulsarClientImpl client = Mockito.mock(PulsarClientImpl.class);
        Mockito.when(client.getMemoryLimitController())
                .thenReturn(Mockito.mock(MemoryLimitController.class));
        FieldUtils.writeField(producer, "client", client, true);

        OpSendMsgQueue pendingQueue = new OpSendMsgQueue();
        FieldUtils.writeField(producer, "pendingMessages", pendingQueue, true);

        EventLoop writeEventLoop = Mockito.mock(EventLoop.class);
        doAnswer(invocation -> {
            fail("No task must be submitted to an event loop when the op was never written");
            return null;
        }).when(writeEventLoop).execute(any(Runnable.class));

        ByteBufPair cmd = ByteBufPair.get(
                Unpooled.buffer().writeBytes("frame-header".getBytes(StandardCharsets.UTF_8)),
                Unpooled.buffer().writeBytes("batch-payload".getBytes(StandardCharsets.UTF_8)));
        MessageImpl<?> msg = Mockito.mock(MessageImpl.class);
        Mockito.when(msg.getUncompressedSize()).thenReturn(10);
        OpSendMsg op = OpSendMsg.create(
                Mockito.mock(LatencyHistogram.class), msg, cmd, 1L, Mockito.mock(SendCallback.class));
        op.totalChunks = 1;
        op.chunkId = 0;
        op.numMessagesInBatch = 1;
        // writeEventLoop stays null: this op was queued while disconnected and never handed to a connection.
        pendingQueue.add(op);

        producer.failPendingMessages(null, new PulsarClientException.TimeoutException("timeout"));

        assertEquals(cmd.refCnt(), 0, "cmd must be released inline when no write is in-flight");
        assertEquals(pendingQueue.size(), 0);
    }

    /**
     * Verifies that {@link ProducerImpl#processOpSendMsg(OpSendMsg)} hands the op's cmd lifecycle to the
     * connection's event loop when it schedules the write, so that a later timeout on another thread can defer
     * the cmd release to the right event loop.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testProcessOpSendMsgTracksWriteEventLoop() throws Exception {
        ProducerImpl<byte[]> producer = Mockito.mock(ProducerImpl.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(false).when(producer).isBatchMessagingEnabled();
        Mockito.doReturn(HandlerState.State.Ready).when(producer).getState();

        List<Runnable> submittedTasks = new ArrayList<>();
        EventLoop eventLoop = Mockito.mock(EventLoop.class);
        doAnswer(invocation -> {
            submittedTasks.add(invocation.getArgument(0));
            return null;
        }).when(eventLoop).execute(any(Runnable.class));
        Channel channel = Mockito.mock(Channel.class);
        Mockito.when(channel.eventLoop()).thenReturn(eventLoop);
        ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
        Mockito.when(ctx.channel()).thenReturn(channel);
        ClientCnx cnx = Mockito.mock(ClientCnx.class);
        Mockito.when(cnx.ctx()).thenReturn(ctx);
        Mockito.doReturn(cnx).when(producer).getCnxIfReady();

        ProducerStatsRecorder stats = Mockito.mock(ProducerStatsRecorder.class);
        FieldUtils.writeField(producer, "stats", stats, true);
        OpSendMsgQueue pendingQueue = new OpSendMsgQueue();
        FieldUtils.writeField(producer, "pendingMessages", pendingQueue, true);

        ByteBufPair cmd = ByteBufPair.get(
                Unpooled.buffer().writeBytes("frame-header".getBytes(StandardCharsets.UTF_8)),
                Unpooled.buffer().writeBytes("batch-payload".getBytes(StandardCharsets.UTF_8)));
        MessageImpl<?> msg = Mockito.mock(MessageImpl.class);
        Mockito.when(msg.getUncompressedSize()).thenReturn(10);
        OpSendMsg op = OpSendMsg.create(
                Mockito.mock(LatencyHistogram.class), msg, cmd, 1L, Mockito.mock(SendCallback.class));
        op.totalChunks = 1;
        op.chunkId = 0;
        op.numMessagesInBatch = 1;
        // Bypass the message-dependent paths (batch scheduling / schema registration / size checks) so the test
        // focuses on the write hand-off.
        op.msg = null;

        producer.processOpSendMsg(op);

        assertEquals(op.writeEventLoop, eventLoop,
                "processOpSendMsg must record the event loop the cmd is handed to for writing");
        assertEquals(submittedTasks.size(), 1, "a write must have been scheduled on the connection event loop");
        assertEquals(cmd.refCnt(), 2, "the write path must hold an extra reference on the cmd");
        verify(stats).updateNumMsgsSent(1, 0);
    }

    /**
     * Regression test for the stale write-callback window: an op is handed to connection A's event loop, then
     * re-sent on connection B after a reconnect (so {@code op.writeEventLoop} now points to B's loop), and the
     * send timeout disposes it deferred on B's loop while the callback queued on A's loop has still not run.
     * The stale callback must not write the released cmd or touch the recycled op; it must only drop the
     * reference it took when it was scheduled.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testStaleWriteCallbackSkipsDisposedOp() throws Exception {
        ProducerImpl<byte[]> producer = Mockito.mock(ProducerImpl.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(false).when(producer).isBatchMessagingEnabled();
        Mockito.doReturn(HandlerState.State.Ready).when(producer).getState();
        Mockito.doNothing().when(producer).semaphoreRelease(Mockito.anyInt());
        // The write callback logs through the instance logger, which a mock does not initialize.
        FieldUtils.writeField(producer, "log",
                Mockito.mock(io.github.merlimat.slog.Logger.class, Mockito.RETURNS_DEEP_STUBS), true);
        PulsarClientImpl client = Mockito.mock(PulsarClientImpl.class);
        Mockito.when(client.getMemoryLimitController())
                .thenReturn(Mockito.mock(MemoryLimitController.class));
        FieldUtils.writeField(producer, "client", client, true);

        List<Runnable> tasksOnLoopA = new ArrayList<>();
        EventLoop eventLoopA = Mockito.mock(EventLoop.class);
        doAnswer(invocation -> {
            tasksOnLoopA.add(invocation.getArgument(0));
            return null;
        }).when(eventLoopA).execute(any(Runnable.class));
        Channel channelA = Mockito.mock(Channel.class);
        Mockito.when(channelA.eventLoop()).thenReturn(eventLoopA);
        ChannelHandlerContext ctxA = Mockito.mock(ChannelHandlerContext.class);
        Mockito.when(ctxA.channel()).thenReturn(channelA);
        ClientCnx cnxA = Mockito.mock(ClientCnx.class);
        Mockito.when(cnxA.ctx()).thenReturn(ctxA);
        Mockito.doReturn(cnxA).when(producer).getCnxIfReady();

        ProducerStatsRecorder stats = Mockito.mock(ProducerStatsRecorder.class);
        FieldUtils.writeField(producer, "stats", stats, true);
        OpSendMsgQueue pendingQueue = new OpSendMsgQueue();
        FieldUtils.writeField(producer, "pendingMessages", pendingQueue, true);

        ByteBufPair cmd = ByteBufPair.get(
                Unpooled.buffer().writeBytes("frame-header".getBytes(StandardCharsets.UTF_8)),
                Unpooled.buffer().writeBytes("batch-payload".getBytes(StandardCharsets.UTF_8)));
        MessageImpl<?> msg = Mockito.mock(MessageImpl.class);
        Mockito.when(msg.getUncompressedSize()).thenReturn(10);
        OpSendMsg op = OpSendMsg.create(
                Mockito.mock(LatencyHistogram.class), msg, cmd, 1L, Mockito.mock(SendCallback.class));
        op.totalChunks = 1;
        op.chunkId = 0;
        op.numMessagesInBatch = 1;
        op.msg = null;

        // First write: the callback is queued on connection A's loop and has not run yet.
        producer.processOpSendMsg(op);
        Runnable staleWriteCallback = tasksOnLoopA.get(0);
        assertEquals(cmd.refCnt(), 2, "base reference + the reference taken for the write");

        // Reconnect: the op is re-sent on connection B, so the tracked write loop moves to B's loop.
        List<Runnable> tasksOnLoopB = new ArrayList<>();
        EventLoop eventLoopB = Mockito.mock(EventLoop.class);
        doAnswer(invocation -> {
            tasksOnLoopB.add(invocation.getArgument(0));
            return null;
        }).when(eventLoopB).execute(any(Runnable.class));
        op.writeEventLoop = eventLoopB;

        // The send timeout fires: the disposal is deferred to B's loop and recycles the op.
        producer.failPendingMessages(null, new PulsarClientException.TimeoutException("timeout"));
        assertEquals(tasksOnLoopB.size(), 1, "disposal must be deferred to the tracked write loop");
        tasksOnLoopB.get(0).run();
        assertEquals(cmd.refCnt(), 1, "only the stale callback's reference may remain");

        // The stale callback from A's loop finally runs: it must skip the recycled op and only drop its
        // own reference, instead of writing the released cmd or mutating the recycled op.
        staleWriteCallback.run();
        assertEquals(cmd.refCnt(), 0, "the stale callback must release exactly its own reference");
        assertEquals(op.sequenceId, -1L, "the recycled op must not have been touched by the stale callback");
        assertEquals(op.retryCount, 0, "the recycled op must not have been touched by the stale callback");
        assertEquals(op.firstSentAt, -1L, "the recycled op must not have been touched by the stale callback");
    }

    /**
     * The stale write callback must also skip the write when the op was re-sent on another connection but has
     * NOT been disposed yet: the op's {@code writeEventLoop} now points at the new connection's loop, so the
     * callback on the old connection must not write (that would double-send) and must not mutate the live op. It
     * must only drop the reference it took when it was scheduled.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testStaleWriteCallbackSkipsOpReSentOnAnotherLoop() throws Exception {
        ProducerImpl<byte[]> producer = Mockito.mock(ProducerImpl.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(false).when(producer).isBatchMessagingEnabled();
        Mockito.doReturn(HandlerState.State.Ready).when(producer).getState();
        Mockito.doNothing().when(producer).semaphoreRelease(Mockito.anyInt());
        FieldUtils.writeField(producer, "log",
                Mockito.mock(io.github.merlimat.slog.Logger.class, Mockito.RETURNS_DEEP_STUBS), true);
        PulsarClientImpl client = Mockito.mock(PulsarClientImpl.class);
        Mockito.when(client.getMemoryLimitController())
                .thenReturn(Mockito.mock(MemoryLimitController.class));
        FieldUtils.writeField(producer, "client", client, true);

        List<Runnable> tasksOnLoopA = new ArrayList<>();
        EventLoop eventLoopA = Mockito.mock(EventLoop.class);
        doAnswer(invocation -> {
            tasksOnLoopA.add(invocation.getArgument(0));
            return null;
        }).when(eventLoopA).execute(any(Runnable.class));
        Channel channelA = Mockito.mock(Channel.class);
        Mockito.when(channelA.eventLoop()).thenReturn(eventLoopA);
        ChannelHandlerContext ctxA = Mockito.mock(ChannelHandlerContext.class);
        Mockito.when(ctxA.channel()).thenReturn(channelA);
        ClientCnx cnxA = Mockito.mock(ClientCnx.class);
        Mockito.when(cnxA.ctx()).thenReturn(ctxA);
        Mockito.doReturn(cnxA).when(producer).getCnxIfReady();

        ProducerStatsRecorder stats = Mockito.mock(ProducerStatsRecorder.class);
        FieldUtils.writeField(producer, "stats", stats, true);
        OpSendMsgQueue pendingQueue = new OpSendMsgQueue();
        FieldUtils.writeField(producer, "pendingMessages", pendingQueue, true);

        ByteBufPair cmd = ByteBufPair.get(
                Unpooled.buffer().writeBytes("frame-header".getBytes(StandardCharsets.UTF_8)),
                Unpooled.buffer().writeBytes("batch-payload".getBytes(StandardCharsets.UTF_8)));
        MessageImpl<?> msg = Mockito.mock(MessageImpl.class);
        Mockito.when(msg.getUncompressedSize()).thenReturn(10);
        OpSendMsg op = OpSendMsg.create(
                Mockito.mock(LatencyHistogram.class), msg, cmd, 1L, Mockito.mock(SendCallback.class));
        op.totalChunks = 1;
        op.chunkId = 0;
        op.numMessagesInBatch = 1;
        op.msg = null;

        // First write on connection A: the callback is queued but has not run yet.
        producer.processOpSendMsg(op);
        Runnable staleWriteCallback = tasksOnLoopA.get(0);
        assertEquals(cmd.refCnt(), 2, "base reference + the reference taken for the write");

        // The op is re-sent on connection B: the tracked write loop moves to B's loop, but the op is NOT disposed
        // and op.cmd is unchanged.
        EventLoop eventLoopB = Mockito.mock(EventLoop.class);
        op.writeEventLoop = eventLoopB;

        // The stale callback from A finally runs. It must detect that this loop is no longer the op's write loop,
        // skip the write and drop only its own reference, without touching the live op.
        staleWriteCallback.run();
        assertEquals(cmd.refCnt(), 1, "the stale callback must release exactly its own reference");
        assertEquals(op.retryCount, 0, "the live op must not be mutated by the stale callback");
        assertEquals(op.firstSentAt, -1L, "the live op must not be mutated by the stale callback");
        // Skipping the stale write must not strand the message: the op stays pending, waiting for the
        // response of the write on the new connection (receipt, or a later timeout as the last resort).
        assertTrue(pendingQueue.peek() == op, "the live op must still be pending for the new write");
        assertEquals(pendingQueue.size(), 1);
    }

    /**
     * When the timeout fires while the current connection's event loop is shutting down (e.g. right after a
     * reconnect churn), the deferred failPendingMessages task would be rejected and dropped. The producer must
     * fall back to failing the pending messages inline instead of leaving them queued until the next timeout tick.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testFailPendingMessagesHandlesShuttingDownEventLoop() throws Exception {
        ProducerImpl<byte[]> producer = Mockito.mock(ProducerImpl.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(false).when(producer).isBatchMessagingEnabled();
        Mockito.doNothing().when(producer).semaphoreRelease(Mockito.anyInt());
        FieldUtils.writeField(producer, "log",
                Mockito.mock(io.github.merlimat.slog.Logger.class, Mockito.RETURNS_DEEP_STUBS), true);
        PulsarClientImpl client = Mockito.mock(PulsarClientImpl.class);
        Mockito.when(client.getMemoryLimitController())
                .thenReturn(Mockito.mock(MemoryLimitController.class));
        FieldUtils.writeField(producer, "client", client, true);

        OpSendMsgQueue pendingQueue = new OpSendMsgQueue();
        FieldUtils.writeField(producer, "pendingMessages", pendingQueue, true);

        // The current connection's event loop rejects new tasks (it is shutting down).
        EventLoop eventLoop = Mockito.mock(EventLoop.class);
        doThrow(new RejectedExecutionException("shutting down")).when(eventLoop).execute(any(Runnable.class));
        Channel channel = Mockito.mock(Channel.class);
        Mockito.when(channel.eventLoop()).thenReturn(eventLoop);
        ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
        Mockito.when(ctx.channel()).thenReturn(channel);
        ClientCnx cnx = Mockito.mock(ClientCnx.class);
        Mockito.when(cnx.ctx()).thenReturn(ctx);

        ByteBufPair cmd = ByteBufPair.get(
                Unpooled.buffer().writeBytes("frame-header".getBytes(StandardCharsets.UTF_8)),
                Unpooled.buffer().writeBytes("batch-payload".getBytes(StandardCharsets.UTF_8)));
        MessageImpl<?> msg = Mockito.mock(MessageImpl.class);
        Mockito.when(msg.getUncompressedSize()).thenReturn(10);
        OpSendMsg op = OpSendMsg.create(
                Mockito.mock(LatencyHistogram.class), msg, cmd, 1L, Mockito.mock(SendCallback.class));
        op.totalChunks = 1;
        op.chunkId = 0;
        op.numMessagesInBatch = 1;
        // This op was never handed to a connection, so its disposal is safe inline.
        pendingQueue.add(op);

        producer.failPendingMessages(cnx, new PulsarClientException.TimeoutException("timeout"));

        assertEquals(pendingQueue.size(), 0, "pending messages must still be failed when the event loop is down");
        assertEquals(cmd.refCnt(), 0, "the cmd must be released inline when the event loop rejects the task");
    }
}
