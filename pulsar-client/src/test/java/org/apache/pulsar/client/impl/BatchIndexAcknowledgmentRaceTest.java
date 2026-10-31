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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.TimedCompletableFuture;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BatchIndexAcknowledgmentRaceTest {
    private EventLoopGroup eventLoopGroup;

    private enum Schedule {
        ACK_BEFORE_FLUSH, FLUSH_BEFORE_ACK, CONCURRENT_FLUSH
    }

    private enum CleanupSchedule {
        AFTER_LOOKUP, DURING_RETRY_INITIALIZATION
    }

    @BeforeClass
    public void setup() {
        eventLoopGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    }

    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).get(10, TimeUnit.SECONDS);
        }
    }

    @DataProvider
    public Object[][] ackSchedules() {
        List<Object[]> cases = new ArrayList<>();
        for (Schedule schedule : Schedule.values()) {
            for (boolean receipt : new boolean[] {false, true}) {
                for (boolean list : new boolean[] {false, true}) {
                    cases.add(new Object[] {schedule, receipt, list, false});
                }
            }
        }
        cases.add(new Object[] {Schedule.CONCURRENT_FLUSH, false, false, true});
        cases.add(new Object[] {Schedule.CONCURRENT_FLUSH, false, true, true});
        return cases.toArray(new Object[0][]);
    }

    @Test(dataProvider = "ackSchedules", timeOut = 60000)
    public void testBatchIndexAckSurvivesFlush(Schedule schedule, boolean receipt, boolean list, boolean failFirstWrite)
            throws Exception {
        ConsumerImpl<?> consumer = mock(ConsumerImpl.class);
        consumer.unAckedChunkedMessageIdSequenceMap = new ConcurrentHashMap<>();
        doReturn(new ConsumerStatsRecorderImpl()).when(consumer).getStats();
        doReturn(UnAckedMessageTracker.UNACKED_MESSAGE_TRACKER_DISABLED)
                .when(consumer).getUnAckedMessageTracker();
        doReturn(HandlerState.State.Ready).when(consumer).getState();
        doReturn(receipt).when(consumer).isAckReceiptEnabled();
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        AtomicLong requests = new AtomicLong();
        doAnswer(invocation -> requests.incrementAndGet()).when(client).newRequestId();
        doReturn(client).when(consumer).getClient();
        ClientCnx connection = mock(ClientCnx.class);
        doReturn(connection).when(consumer).getClientCnx();
        doReturn(Commands.getCurrentProtocolVersion()).when(connection).getRemoteEndpointProtocolVersion();
        List<BaseCommand> frames = Collections.synchronizedList(new ArrayList<>());
        List<BaseCommand> failedFrames = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean failNextWrite = new AtomicBoolean(failFirstWrite);
        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        doReturn(context).when(connection).ctx();
        ChannelFuture successfulWrite = completedWrite(true);
        ChannelFuture failedWrite = completedWrite(false);
        doAnswer(invocation -> {
            boolean fail = failNextWrite.getAndSet(false);
            captureFrame(invocation.getArgument(0), fail ? failedFrames : frames);
            return fail ? failedWrite : successfulWrite;
        }).when(context).writeAndFlush(any());
        doAnswer(invocation -> {
            boolean fail = failNextWrite.getAndSet(false);
            captureFrame(invocation.getArgument(0), fail ? failedFrames : frames);
            return fail ? failedWrite : successfulWrite;
        }).when(context).write(any());
        doAnswer(invocation -> {
            captureFrame(invocation.getArgument(0), frames);
            TimedCompletableFuture<Void> future = invocation.getArgument(2);
            future.complete(null);
            return null;
        }).when(connection).newAckForReceiptWithFuture(any(), anyLong(), any());

        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setBatchIndexAckEnabled(true);
        conf.setAckReceiptEnabled(receipt);
        conf.setAcknowledgementsGroupTimeMicros(TimeUnit.HOURS.toMicros(1));
        BitSet ackSet = BatchMessageIdImpl.newAckSet(4);
        CountDownLatch paused = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        AtomicBoolean pauseOnce = new AtomicBoolean(schedule == Schedule.CONCURRENT_FLUSH);
        BatchMessageIdImpl first = new BatchMessageIdImpl(5, 1, 0, 0, 4, ackSet);
        BatchMessageIdImpl second = new BatchMessageIdImpl(5, 1, 0, 1, 4, ackSet) {
            @Override
            public int getBatchIndex() {
                // The input ACK set is updated before the tracker clears the pending bitmap. Pause only
                // after that update, outside its monitor, to leave the map lookup/clear handoff exposed.
                if (!ackSet.get(1) && pauseOnce.compareAndSet(true, false)) {
                    assertThat(Thread.holdsLock(ackSet)).isFalse();
                    paused.countDown();
                    try {
                        assertThat(resume.await(10, TimeUnit.SECONDS)).as("resume the application ACK").isTrue();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                }
                return super.getBatchIndex();
            }
        };
        ExecutorService actors = Executors.newFixedThreadPool(2, new DefaultThreadFactory("batch-ack-race"));
        try (var tracker = new PersistentAcknowledgmentsGroupingTracker(consumer, conf, eventLoopGroup)) {
            CompletableFuture<Void> firstAck = tracker.addAcknowledgment(first, AckType.Individual,
                    Collections.emptyMap());
            if (schedule == Schedule.FLUSH_BEFORE_ACK) {
                tracker.flush();
            }
            AtomicReference<Thread> ackThread = new AtomicReference<>();
            AtomicReference<Thread> flushThread = new AtomicReference<>();
            AtomicReference<CompletableFuture<Void>> secondAck = new AtomicReference<>();
            CompletableFuture<Void> ackAdded = CompletableFuture.runAsync(() -> {
                ackThread.set(Thread.currentThread());
                secondAck.set(list
                        ? tracker.addListAcknowledgment(List.<MessageId>of(second), AckType.Individual,
                                Collections.emptyMap())
                        : tracker.addAcknowledgment(second, AckType.Individual, Collections.emptyMap()));
            }, actors);
            CompletableFuture<Void> flushed = CompletableFuture.completedFuture(null);
            if (schedule == Schedule.CONCURRENT_FLUSH) {
                assertThat(paused.await(10, TimeUnit.SECONDS)).as("application ACK reached the clear operation")
                        .isTrue();
                flushed = CompletableFuture.runAsync(() -> {
                    flushThread.set(Thread.currentThread());
                    tracker.flush();
                }, actors);
                CompletableFuture<Void> concurrentFlush = flushed;
                // Old code can finish this flush before the ACK resumes. Correct synchronization makes it
                // wait for the ACK instead; observe that wait without depending on an arbitrary delay.
                Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> concurrentFlush.isDone()
                        || isFlushWaitingForAck(flushThread.get(), ackThread.get()));
            }
            resume.countDown();
            ackAdded.get(10, TimeUnit.SECONDS);
            flushed.get(10, TimeUnit.SECONDS);
            tracker.flush();
            tracker.flush();
            CompletableFuture.allOf(firstAck, secondAck.get()).get(10, TimeUnit.SECONDS);

            BitSet acknowledged = new BitSet(4);
            assertThat(failedFrames).hasSize(failFirstWrite ? 1 : 0);
            assertThat(frames).isNotEmpty();
            for (BaseCommand frame : frames) {
                var ack = frame.getAck();
                assertThat(ack.hasRequestId()).isEqualTo(receipt);
                assertThat(ack.getMessageIdsCount()).isEqualTo(1);
                var messageId = ack.getMessageIdAt(0);
                assertThat(messageId.getLedgerId()).isEqualTo(5);
                assertThat(messageId.getEntryId()).isEqualTo(1);
                assertThat(messageId.getAckSetsCount()).as("the four-message batch remains partially acknowledged")
                        .isPositive();
                long[] words = new long[messageId.getAckSetsCount()];
                for (int i = 0; i < words.length; i++) {
                    words[i] = messageId.getAckSetAt(i);
                }
                BitSet pending = BitSet.valueOf(words);
                assertThat(pending.get(2)).isTrue();
                assertThat(pending.get(3)).isTrue();
                BitSet sent = BatchMessageIdImpl.newAckSet(4);
                sent.andNot(pending);
                acknowledged.or(sent);
            }
            assertThat(acknowledged).as("both accepted ACKs must reach successful protocol frames")
                    .isEqualTo(BitSet.valueOf(new long[] {3}));
            assertThat(tracker.pendingIndividualBatchIndexAcks).isEmpty();
        } finally {
            resume.countDown();
            actors.shutdownNow();
            assertThat(actors.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @DataProvider
    public Object[][] cleanupSchedules() {
        List<Object[]> cases = new ArrayList<>();
        for (CleanupSchedule schedule : CleanupSchedule.values()) {
            for (boolean list : new boolean[] {false, true}) {
                for (boolean replayAck : new boolean[] {false, true}) {
                    cases.add(new Object[] {schedule, list, replayAck});
                }
            }
        }
        return cases.toArray(new Object[0][]);
    }

    @Test(dataProvider = "cleanupSchedules", timeOut = 60000)
    public void testBatchIndexAckCannotCrossCleanup(CleanupSchedule schedule, boolean list, boolean replayAck)
            throws Exception {
        ConsumerImpl<?> consumer = mock(ConsumerImpl.class);
        consumer.unAckedChunkedMessageIdSequenceMap = new ConcurrentHashMap<>();
        doReturn(new ConsumerStatsRecorderImpl()).when(consumer).getStats();
        doReturn(UnAckedMessageTracker.UNACKED_MESSAGE_TRACKER_DISABLED)
                .when(consumer).getUnAckedMessageTracker();
        doReturn(HandlerState.State.Ready).when(consumer).getState();
        ClientCnx connection = mock(ClientCnx.class);
        AtomicReference<ClientCnx> subscribedConnection = new AtomicReference<>(connection);
        doAnswer(__ -> subscribedConnection.get()).when(consumer).getClientCnx();
        doReturn(Commands.getCurrentProtocolVersion()).when(connection).getRemoteEndpointProtocolVersion();
        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        doReturn(context).when(connection).ctx();
        List<BaseCommand> frames = Collections.synchronizedList(new ArrayList<>());
        ChannelFuture successfulWrite = completedWrite(true);
        doAnswer(invocation -> {
            captureFrame(invocation.getArgument(0), frames);
            return successfulWrite;
        }).when(context).writeAndFlush(any());
        doAnswer(invocation -> {
            captureFrame(invocation.getArgument(0), frames);
            return successfulWrite;
        }).when(context).write(any());

        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setBatchIndexAckEnabled(true);
        conf.setAcknowledgementsGroupTimeMicros(TimeUnit.HOURS.toMicros(1));
        BitSet ackSet = BatchMessageIdImpl.newAckSet(4);
        CountDownLatch lookupPaused = new CountDownLatch(1);
        CountDownLatch resumeLookup = new CountDownLatch(1);
        CountDownLatch initializationPaused = new CountDownLatch(1);
        CountDownLatch resumeInitialization = new CountDownLatch(1);
        AtomicBoolean pauseLookup = new AtomicBoolean(true);
        AtomicBoolean pauseInitialization = new AtomicBoolean();
        BatchMessageIdImpl first = new BatchMessageIdImpl(5, 1, 0, 0, 4, ackSet);
        BatchMessageIdImpl second = new BatchMessageIdImpl(5, 1, 0, 1, 4, ackSet) {
            @Override
            public int getBatchIndex() {
                if (!ackSet.get(1) && pauseLookup.compareAndSet(true, false)) {
                    assertThat(Thread.holdsLock(ackSet)).isFalse();
                    lookupPaused.countDown();
                    awaitResume(resumeLookup);
                }
                return super.getBatchIndex();
            }

            @Override
            public BitSet getAckSet() {
                if (pauseInitialization.compareAndSet(true, false)) {
                    initializationPaused.countDown();
                    awaitResume(resumeInitialization);
                }
                return super.getAckSet();
            }
        };
        BatchMessageIdImpl replay = new BatchMessageIdImpl(5, 1, 0, 2, 4, BatchMessageIdImpl.newAckSet(4));
        ExecutorService actors = Executors.newSingleThreadExecutor(new DefaultThreadFactory("batch-ack-cleanup"));
        try (var tracker = new PersistentAcknowledgmentsGroupingTracker(consumer, conf, eventLoopGroup)) {
            acknowledge(tracker, first, list).get(10, TimeUnit.SECONDS);
            assertThat(tracker.isDuplicate(first)).isTrue();
            CompletableFuture<Void> inFlight = CompletableFuture.supplyAsync(
                    () -> acknowledge(tracker, second, list), actors).thenCompose(future -> future);
            assertThat(lookupPaused.await(10, TimeUnit.SECONDS)).as("ACK paused after bitmap lookup").isTrue();
            if (schedule == CleanupSchedule.DURING_RETRY_INITIALIZATION) {
                tracker.flush();
                pauseInitialization.set(true);
                resumeLookup.countDown();
                assertThat(initializationPaused.await(10, TimeUnit.SECONDS))
                        .as("ordinary flush caused a retry, paused before bitmap publication").isTrue();
            }

            subscribedConnection.set(null);
            tracker.flushAndClean();
            int framesBeforeReplay = frames.size();
            assertThat(tracker.isDuplicate(first)).as("cleanup must allow replay of completed ACK 0").isFalse();
            if (replayAck) {
                acknowledge(tracker, replay, list).get(10, TimeUnit.SECONDS);
                tracker.flush();
            }
            resumeLookup.countDown();
            resumeInitialization.countDown();
            inFlight.get(10, TimeUnit.SECONDS);

            assertThat(tracker.isDuplicate(first)).as("old ACK 0 must not be restored after cleanup").isFalse();
            assertThat(tracker.isDuplicate(second)).as("the invalidated ACK must not modify replay state").isFalse();
            assertThat(tracker.isDuplicate(replay)).isEqualTo(replayAck);
            assertThat(tracker.pendingIndividualBatchIndexAcks).hasSize(replayAck ? 1 : 0);
            subscribedConnection.set(connection);
            tracker.flush();
            tracker.flush();
            List<BaseCommand> replayFrames = frames.subList(framesBeforeReplay, frames.size());
            assertThat(replayFrames).hasSize(replayAck ? 1 : 0);
            if (replayAck) {
                var ack = replayFrames.get(0).getAck();
                assertThat(ack.getMessageIdsCount()).isEqualTo(1);
                var messageId = ack.getMessageIdAt(0);
                assertThat(messageId.getLedgerId()).isEqualTo(5);
                assertThat(messageId.getEntryId()).isEqualTo(1);
                assertThat(messageId.getAckSetsCount()).isEqualTo(1);
                assertThat(messageId.getAckSetAt(0)).as("only replay ACK 2 may be sent after cleanup")
                        .isEqualTo(0b1011L);
            }
            assertThat(tracker.pendingIndividualBatchIndexAcks).isEmpty();
        } finally {
            resumeLookup.countDown();
            resumeInitialization.countDown();
            actors.shutdownNow();
            assertThat(actors.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static CompletableFuture<Void> acknowledge(PersistentAcknowledgmentsGroupingTracker tracker,
                                                       MessageId id, boolean list) {
        return list ? tracker.addListAcknowledgment(List.of(id), AckType.Individual, Collections.emptyMap())
                : tracker.addAcknowledgment(id, AckType.Individual, Collections.emptyMap());
    }

    private static void awaitResume(CountDownLatch resume) {
        try {
            assertThat(resume.await(10, TimeUnit.SECONDS)).as("resume the application ACK").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static ChannelFuture completedWrite(boolean success) {
        ChannelFuture future = mock(ChannelFuture.class);
        doReturn(success).when(future).isSuccess();
        doAnswer(invocation -> {
            GenericFutureListener<Future<Void>> listener = invocation.getArgument(0);
            listener.operationComplete(future);
            return future;
        }).when(future).addListener(any());
        return future;
    }

    private static boolean isFlushWaitingForAck(Thread flushThread, Thread ackThread) {
        if (flushThread == null || ackThread == null) {
            return false;
        }
        ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(flushThread.threadId(), 30);
        return info != null && (info.getLockOwnerId() == ackThread.threadId()
                || (info.getThreadState() == Thread.State.WAITING && Arrays.stream(info.getStackTrace())
                        .anyMatch(frame -> frame.getClassName()
                                .equals(PersistentAcknowledgmentsGroupingTracker.class.getName())
                                && frame.getMethodName().equals("acquireWriteLock"))));
    }

    private static void captureFrame(ByteBuf buffer, List<BaseCommand> frames) {
        try {
            buffer.skipBytes(4);
            int size = buffer.readInt();
            BaseCommand command = new BaseCommand();
            command.parseFrom(buffer, size);
            command.materialize();
            assertThat(command.getType()).isEqualTo(BaseCommand.Type.ACK);
            frames.add(command);
        } finally {
            buffer.release();
        }
    }
}
