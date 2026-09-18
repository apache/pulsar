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
package org.apache.pulsar.broker.service.persistent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.channel.EventLoopGroup;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.AbstractReplicator.State;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator.InFlightTask;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.mockito.InOrder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-replication")
public class PersistentReplicatorReadProcessingTest {

    @Test
    public void testAckCannotSubmitLaterBatchBeforeCurrentBatchSubmissionEnds() throws Exception {
        TestReplicatorFixture fixture = newTestReplicatorFixture();
        TestPersistentReplicator replicator = fixture.replicator;
        CountDownLatch firstEntrySubmitted = new CountDownLatch(1);
        CountDownLatch finishFirstBatch = new CountDownLatch(1);
        AtomicBoolean blockFirstEntry = new AtomicBoolean(true);
        replicator.entryObserver = (entry, task, entries) -> {
            replicator.submittedEntries.add(entry.getEntryId());
            if (blockFirstEntry.compareAndSet(true, false)) {
                firstEntrySubmitted.countDown();
                await(finishFirstBatch);
            }
        };

        List<ReadRequest> requests = new ArrayList<>();
        List<Entry> secondBatch = List.of(entry(2), entry(3));
        AtomicBoolean completeSecondReadInline = new AtomicBoolean(false);
        AtomicBoolean secondReadCompleted = new AtomicBoolean(false);
        doAnswer(invocation -> {
            ReadEntriesCallback callback = invocation.getArgument(2);
            Object context = invocation.getArgument(3);
            synchronized (requests) {
                requests.add(new ReadRequest(callback, context));
            }
            if (completeSecondReadInline.get() && secondReadCompleted.compareAndSet(false, true)) {
                callback.readEntriesComplete(secondBatch, context);
            }
            return null;
        }).when(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());

        replicator.readMoreEntries();
        ReadRequest firstRead;
        synchronized (requests) {
            assertThat(requests).hasSize(1);
            firstRead = requests.get(0);
        }

        Thread firstCompletion = new Thread(
                () -> firstRead.callback.readEntriesComplete(List.of(entry(0), entry(1)), firstRead.context),
                "first-replication-batch");
        firstCompletion.start();
        Thread acknowledgement = null;
        try {
            assertThat(firstEntrySubmitted.await(10, TimeUnit.SECONDS)).isTrue();

            completeSecondReadInline.set(true);
            acknowledgement = new Thread(() -> {
                InFlightTask firstTask = (InFlightTask) firstRead.context;
                firstTask.incCompletedEntries();
                replicator.readMoreEntries();
            }, "replication-acknowledgement");
            acknowledgement.start();
            acknowledgement.join(TimeUnit.SECONDS.toMillis(10));
            assertThat(acknowledgement.isAlive()).isFalse();

            // The acknowledgement may request more work, but it cannot acquire submission ownership while the
            // callback that owns the first batch is paused between its two sends.
            assertThat(replicator.submittedEntries).containsExactly(0L);
            synchronized (requests) {
                assertThat(requests).hasSize(1);
            }

            finishFirstBatch.countDown();
            firstCompletion.join(TimeUnit.SECONDS.toMillis(10));
            assertThat(firstCompletion.isAlive()).isFalse();
            fixture.runQueuedWork();

            assertThat(replicator.submittedEntries).containsExactly(0L, 1L, 2L, 3L);
        } finally {
            finishFirstBatch.countDown();
            firstCompletion.join(TimeUnit.SECONDS.toMillis(10));
            if (acknowledgement != null) {
                acknowledgement.join(TimeUnit.SECONDS.toMillis(10));
            }
        }
    }

    @Test
    public void testInlineAckReadDoesNotOvertakeRemainingEntriesInCurrentBatch() throws Exception {
        TestReplicatorFixture fixture = newTestReplicatorFixture();
        TestPersistentReplicator replicator = fixture.replicator;
        AtomicBoolean firstAcknowledgement = new AtomicBoolean(true);
        replicator.entryObserver = (entry, task, entries) -> {
            replicator.submittedEntries.add(entry.getEntryId());
            if (firstAcknowledgement.compareAndSet(true, false)) {
                task.incCompletedEntries();
                // A producer completion can ask for more work synchronously on this same callback thread.
                replicator.readMoreEntries();
            }
        };

        List<ReadRequest> requests = new ArrayList<>();
        AtomicBoolean completeSecondReadInline = new AtomicBoolean(false);
        AtomicBoolean secondReadCompleted = new AtomicBoolean(false);
        doAnswer(invocation -> {
            ReadEntriesCallback callback = invocation.getArgument(2);
            Object context = invocation.getArgument(3);
            synchronized (requests) {
                requests.add(new ReadRequest(callback, context));
            }
            if (completeSecondReadInline.get() && secondReadCompleted.compareAndSet(false, true)) {
                callback.readEntriesComplete(List.of(entry(2), entry(3)), context);
            }
            return null;
        }).when(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());

        replicator.readMoreEntries();
        ReadRequest firstRead;
        synchronized (requests) {
            assertThat(requests).hasSize(1);
            firstRead = requests.get(0);
        }
        completeSecondReadInline.set(true);
        firstRead.callback.readEntriesComplete(List.of(entry(0), entry(1)), firstRead.context);
        fixture.runQueuedWork();

        assertThat(replicator.submittedEntries).containsExactly(0L, 1L, 2L, 3L);
    }

    @DataProvider
    public Object[][] readProcessingTurnLimits() {
        return new Object[][] {{1}, {2}, {3}, {64}, {128}};
    }

    @Test(dataProvider = "readProcessingTurnLimits")
    public void testCachedReadCallbacksDoNotRecurseAndLeaveOnePendingRead(int turnLimit) throws Exception {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setReplicationMaxReadProcessingStepsPerTurn(turnLimit);
        TestReplicatorFixture fixture = newTestReplicatorFixture(configuration);
        TestPersistentReplicator replicator = fixture.replicator;
        int completedReadCount = 256;
        AtomicInteger cursorCallbackDepth = new AtomicInteger();
        AtomicInteger maxCursorCallbackDepth = new AtomicInteger();
        AtomicInteger completedReads = new AtomicInteger();
        AtomicBoolean activeSubmissionTaskWasRecycled = new AtomicBoolean();
        replicator.entryObserver = (entry, task, entries) -> {
            replicator.submittedEntries.add(entry.getEntryId());
            task.incCompletedEntries();
            replicator.readMoreEntries();
            activeSubmissionTaskWasRecycled.compareAndSet(false, task.getEntries() != entries);
        };

        doAnswer(invocation -> {
            ReadEntriesCallback callback = invocation.getArgument(2);
            Object context = invocation.getArgument(3);
            int readNumber = completedReads.getAndIncrement();
            if (readNumber < completedReadCount) {
                int depth = cursorCallbackDepth.incrementAndGet();
                maxCursorCallbackDepth.accumulateAndGet(depth, Math::max);
                try {
                    callback.readEntriesComplete(List.of(entry(readNumber)), context);
                } finally {
                    cursorCallbackDepth.decrementAndGet();
                }
            }
            return null;
        }).when(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());

        replicator.readMoreEntries();
        // Each cached batch takes two steps: invoke the cursor, then submit the completed batch.
        assertThat(completedReads).hasValue((turnLimit + 1) / 2);
        assertThat(replicator.submittedEntries).hasSize(turnLimit / 2);
        assertThat(fixture.queuedWork).hasSize(1);
        // A request during the yield must not steal ownership or start another drain.
        replicator.readMoreEntries();
        assertThat(completedReads).hasValue((turnLimit + 1) / 2);
        assertThat(replicator.submittedEntries).hasSize(turnLimit / 2);
        assertThat(fixture.queuedWork).hasSize(1);
        fixture.runQueuedWork();

        assertThat(replicator.submittedEntries).containsExactlyElementsOf(entryIds(completedReadCount));
        assertThat(maxCursorCallbackDepth.get()).isEqualTo(1);
        assertThat(activeSubmissionTaskWasRecycled).isFalse();
        assertThat(replicator.inFlightTasks).hasSize(1);
        assertThat(replicator.hasPendingRead()).isTrue();
    }

    @Test
    public void testSchemaRewindDiscardsStaleResultAndRetainsPendingRead() throws Exception {
        TestReplicatorFixture fixture = newTestReplicatorFixture();
        TestPersistentReplicator replicator = fixture.replicator;
        List<ReadRequest> requests = new ArrayList<>();
        doAnswer(invocation -> {
            synchronized (requests) {
                requests.add(new ReadRequest(invocation.getArgument(2), invocation.getArgument(3)));
            }
            return null;
        }).when(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());

        replicator.readMoreEntries();
        ReadRequest initialRead;
        synchronized (requests) {
            assertThat(requests).hasSize(1);
            initialRead = requests.get(0);
        }

        replicator.beforeTerminateOrCursorRewinding(
                PersistentReplicator.ReasonOfWaitForCursorRewinding.Fetching_Schema);
        replicator.doRewindCursor(true);
        Entry staleEntry = entry(0);
        initialRead.callback.readEntriesComplete(List.of(staleEntry), initialRead.context);
        fixture.runQueuedWork();

        assertThat(replicator.submittedEntries).isEmpty();
        verify(fixture.cursor).cancelPendingReadRequest();
        verify(staleEntry).release();
        synchronized (requests) {
            assertThat(requests).hasSize(2);
        }
        assertThat(replicator.hasPendingRead()).isTrue();
    }

    @Test
    public void testCancellationRequestedAfterReadReservationIsAppliedToTheIssuedRead() throws Exception {
        TestReplicatorFixture fixture = newTestReplicatorFixture();
        TestPersistentReplicator replicator = fixture.replicator;
        replicator.rewindBeforeCursorInvocation.set(true);
        List<ReadRequest> requests = new ArrayList<>();
        doAnswer(invocation -> {
            synchronized (requests) {
                requests.add(new ReadRequest(invocation.getArgument(2), invocation.getArgument(3)));
            }
            return null;
        }).when(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());
        when(fixture.cursor.cancelPendingReadRequest()).thenReturn(false);

        replicator.readMoreEntries();
        ReadRequest reservedRead;
        synchronized (requests) {
            assertThat(requests).hasSize(1);
            reservedRead = requests.get(0);
        }
        InOrder cursorCalls = inOrder(fixture.cursor);
        cursorCalls.verify(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());
        cursorCalls.verify(fixture.cursor).cancelPendingReadRequest();

        Entry staleEntry = entry(0);
        reservedRead.callback.readEntriesComplete(List.of(staleEntry), reservedRead.context);
        fixture.runQueuedWork();

        assertThat(replicator.submittedEntries).isEmpty();
        verify(staleEntry).release();
        verify(fixture.cursor).rewind();
        synchronized (requests) {
            assertThat(requests).hasSize(2);
        }
        assertThat(replicator.hasPendingRead()).isTrue();
    }

    @Test
    public void testCancelledReservedReadIsSettledWhenCursorCancellationSucceeds() throws Exception {
        TestReplicatorFixture fixture = newTestReplicatorFixture();
        TestPersistentReplicator replicator = fixture.replicator;
        replicator.cancelBeforeCursorInvocation.set(true);
        List<ReadRequest> requests = new ArrayList<>();
        doAnswer(invocation -> {
            synchronized (requests) {
                requests.add(new ReadRequest(invocation.getArgument(2), invocation.getArgument(3)));
            }
            return null;
        }).when(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());
        when(fixture.cursor.cancelPendingReadRequest()).thenReturn(true);

        replicator.readMoreEntries();

        synchronized (requests) {
            assertThat(requests).hasSize(1);
        }
        InOrder cursorCalls = inOrder(fixture.cursor);
        cursorCalls.verify(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());
        cursorCalls.verify(fixture.cursor).cancelPendingReadRequest();
        InFlightTask cancelledTask = (InFlightTask) requests.get(0).context;
        assertThat(cancelledTask.isDone()).isTrue();
        assertThat(replicator.hasPendingRead()).isFalse();
        assertThat(replicator.getPermitsIfNoPendingRead()).isEqualTo(1000);
    }

    @Test
    public void testReadFailureWaitsForTimerButNewReadDemandCanResumeImmediately() throws Exception {
        TestReplicatorFixture fixture = newTestReplicatorFixture();
        TestPersistentReplicator replicator = fixture.replicator;
        List<ReadRequest> requests = new ArrayList<>();
        doAnswer(invocation -> {
            synchronized (requests) {
                requests.add(new ReadRequest(invocation.getArgument(2), invocation.getArgument(3)));
            }
            return null;
        }).when(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());

        replicator.readMoreEntries();
        ReadRequest failedRead;
        synchronized (requests) {
            assertThat(requests).hasSize(1);
            failedRead = requests.get(0);
        }
        replicator.readEntriesFailed(new ManagedLedgerException.TooManyRequestsException("test read failure"),
                failedRead.context);

        ScheduledWork retry = fixture.takeScheduledWork();
        assertThat(retry.unit.toMillis(retry.delay)).isPositive();
        synchronized (requests) {
            // Failure alone must not retry within the owner loop.
            assertThat(requests).hasSize(1);
        }

        // Producer acknowledgements must be able to bypass the fallback timer.
        replicator.readMoreEntries();
        synchronized (requests) {
            assertThat(requests).hasSize(2);
        }
        // The outstanding read still prevents the old timer from admitting a duplicate read.
        retry.command.run();
        synchronized (requests) {
            assertThat(requests).hasSize(2);
        }
    }

    @DataProvider
    public Object[][] retrySchedulingFailures() {
        return new Object[][] {
                {new RejectedExecutionException("retry rejected")},
                {new IllegalStateException("retry scheduling failed")},
                {new AssertionError("retry scheduler error")}
        };
    }

    @Test(dataProvider = "retrySchedulingFailures")
    public void testReadFailureTerminatesWhenRetrySchedulingFails(Throwable failure) throws Exception {
        TestReplicatorFixture fixture = newTestReplicatorFixture();
        TestPersistentReplicator replicator = fixture.replicator;
        doThrow(failure).when(fixture.executor).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        when(fixture.executor.isShuttingDown()).thenReturn(false);
        List<ReadRequest> requests = new ArrayList<>();
        doAnswer(invocation -> {
            synchronized (requests) {
                requests.add(new ReadRequest(invocation.getArgument(2), invocation.getArgument(3)));
            }
            return null;
        }).when(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());

        replicator.readMoreEntries();
        ReadRequest failedRead;
        synchronized (requests) {
            assertThat(requests).hasSize(1);
            failedRead = requests.get(0);
        }
        failedRead.callback.readEntriesFailed(new ManagedLedgerException.TooManyRequestsException("read failed"),
                failedRead.context);

        assertThat(replicator.getState()).isEqualTo(State.Terminated);
        assertThat(fixture.scheduledWork).isEmpty();
        synchronized (requests) {
            assertThat(requests).hasSize(1);
        }
        verify(fixture.cursor).setInactive();
    }

    @Test
    public void testProducerRestartFailureKeepsScheduledRetry() throws Exception {
        TestReplicatorFixture fixture = newTestReplicatorFixture();
        fixture.replicator.markDisconnected();
        fixture.replicator.producerStartFailure = new IllegalStateException("producer startup failed");

        fixture.replicator.readMoreEntries();
        assertThat(fixture.replicator.getState()).isEqualTo(State.Disconnected);
        fixture.takeScheduledWork().command.run();
        assertThat(fixture.replicator.getState()).isEqualTo(State.Disconnected);
        assertThat(fixture.scheduledWork).hasSize(1);
        verify(fixture.cursor, never()).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());
    }

    @Test
    public void testProducerRestartErrorCannotReleaseAnotherReadOwner() throws Exception {
        TestReplicatorFixture fixture = newTestReplicatorFixture();
        TestPersistentReplicator replicator = fixture.replicator;
        CountDownLatch resultPublished = new CountDownLatch(1);
        CountDownLatch releaseReadOwner = new CountDownLatch(1);
        AtomicInteger reads = new AtomicInteger();
        Entry entry = entry(0);
        replicator.entryObserver = (next, task, entries) -> {
            replicator.submittedEntries.add(next.getEntryId());
            task.incCompletedEntries();
            next.release();
        };
        doAnswer(invocation -> {
            if (reads.incrementAndGet() == 1) {
                ReadEntriesCallback callback = invocation.getArgument(2);
                callback.readEntriesComplete(List.of(entry), invocation.getArgument(3));
                resultPublished.countDown();
                await(releaseReadOwner);
            }
            return null;
        }).when(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());
        Thread newOwner = new Thread(replicator::readMoreEntries, "replication-read-owner");
        replicator.markDisconnected();
        replicator.producerStartAction = () -> {
            replicator.markStarted();
            newOwner.start();
            await(resultPublished);
            throw new AssertionError("producer restart failed after another owner started");
        };
        try {
            replicator.readMoreEntries();
            replicator.readMoreEntries();
            assertThat(reads).hasValue(1);
            assertThat(replicator.submittedEntries).isEmpty();
            verify(entry, never()).release();
            verify(fixture.cursor, never()).cancelPendingReadRequest();
            verify(fixture.cursor, never()).rewind();
        } finally {
            releaseReadOwner.countDown();
            newOwner.join(TimeUnit.SECONDS.toMillis(10));
            assertThat(newOwner.isAlive()).isFalse();
        }
        assertThat(replicator.submittedEntries).containsExactly(0L);
        verify(entry).release();
    }

    @DataProvider
    public Object[][] rejectedCompletionStates() {
        return new Object[][] {{State.Started}, {State.Disconnected}};
    }

    @Test(dataProvider = "rejectedCompletionStates")
    public void testRejectedReadCompletionRewindsBeforeRetriedRead(State stateAtCompletion) throws Exception {
        TestReplicatorFixture fixture = newTestReplicatorFixture();
        List<ReadRequest> requests = new ArrayList<>();
        doAnswer(invocation -> {
            requests.add(new ReadRequest(invocation.getArgument(2), invocation.getArgument(3)));
            return null;
        }).when(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());
        Position start = PositionFactory.create(1, 1);
        Position advanced = PositionFactory.create(1, 3);
        when(fixture.cursor.getReadPosition()).thenReturn(start);
        fixture.replicator.readMoreEntries();
        when(fixture.cursor.getReadPosition()).thenReturn(advanced);
        doAnswer(invocation -> {
            when(fixture.cursor.getReadPosition()).thenReturn(start);
            return null;
        }).when(fixture.cursor).rewind();

        if (stateAtCompletion == State.Disconnected) {
            fixture.replicator.markDisconnected();
        }
        ReadRequest read = requests.get(0);
        read.callback.readEntriesFailed(new ManagedLedgerException(new RejectedExecutionException("handoff rejected")),
                read.context);
        assertThat(requests).hasSize(1);
        verify(fixture.cursor).rewind();
        if (stateAtCompletion == State.Disconnected) {
            fixture.replicator.markStarted();
            fixture.replicator.readMoreEntries();
        } else {
            fixture.takeScheduledWork().command.run();
        }

        assertThat(requests).hasSize(2);
        assertThat(((InFlightTask) requests.get(1).context).getReadPos()).isEqualTo(start);
        InOrder cursorCalls = inOrder(fixture.cursor);
        cursorCalls.verify(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());
        cursorCalls.verify(fixture.cursor).rewind();
        cursorCalls.verify(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());
    }

    @Test
    public void testProducerAckDuringReservedReadRetriesFailedReadBeforeTimerWithoutOverlappingReads()
            throws Exception {
        TestReplicatorFixture fixture = newTestReplicatorFixture();
        TestPersistentReplicator replicator = fixture.replicator;
        ProducerImpl<?> producer = mock(ProducerImpl.class);
        when(producer.isWritable()).thenReturn(true);
        replicator.setProducerForTest(producer);

        List<ReadRequest> requests = new ArrayList<>();
        doAnswer(invocation -> {
            synchronized (requests) {
                requests.add(new ReadRequest(invocation.getArgument(2), invocation.getArgument(3)));
            }
            return null;
        }).when(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());

        AtomicBoolean submitted = new AtomicBoolean();
        PersistentReplicator.ProducerSendCallback[] acknowledgement = new PersistentReplicator.ProducerSendCallback[1];
        Entry firstEntry = entry(0);
        replicator.entryObserver = (entry, task, entries) -> {
            assertThat(submitted.compareAndSet(false, true)).isTrue();
            acknowledgement[0] = PersistentReplicator.ProducerSendCallback.create(replicator, entry, null, task);
        };

        replicator.readMoreEntries();
        ReadRequest firstRead;
        synchronized (requests) {
            assertThat(requests).hasSize(1);
            firstRead = requests.get(0);
        }
        firstRead.callback.readEntriesComplete(List.of(firstEntry), firstRead.context);

        ReadRequest reservedRead;
        synchronized (requests) {
            // The owner can reserve the next read only after it has submitted the first batch.
            assertThat(requests).hasSize(2);
            reservedRead = requests.get(1);
        }

        assertThat(acknowledgement[0]).isNotNull();
        acknowledgement[0].sendComplete(null, null);
        assertThat(fixture.queuedWork).isEmpty();
        verify(fixture.executor, never()).execute(any(Runnable.class));
        synchronized (requests) {
            // The actual producer callback records demand but cannot issue another read while one is reserved.
            assertThat(requests).hasSize(2);
        }

        reservedRead.callback.readEntriesFailed(new ManagedLedgerException.TooManyRequestsException("read failed"),
                reservedRead.context);
        fixture.runQueuedWork();
        ScheduledWork fallbackRetry = fixture.takeScheduledWork();
        synchronized (requests) {
            // The ACK demand is consumed after the failed reservation settles, before its fallback timer runs.
            assertThat(requests).hasSize(3);
        }

        ReadRequest immediateRetry = requests.get(2);
        immediateRetry.callback.readEntriesFailed(new ManagedLedgerException.TooManyRequestsException("retry failed"),
                immediateRetry.context);
        fixture.runQueuedWork();
        assertThat(requests).hasSize(3);
        assertThat(fixture.scheduledWork).isEmpty();

        fallbackRetry.command.run();
        synchronized (requests) {
            // One ACK permits only one immediate retry. A second failure must wait for the existing timer.
            assertThat(requests).hasSize(4);
        }
    }

    @DataProvider
    public Object[][] sendCompletionOutcomes() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "sendCompletionOutcomes")
    public void testSendCompletionQueuesReadProcessingOutsideProducerMonitor(boolean failedSend) throws Exception {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setReplicationProducerQueueSize(2);
        TestReplicatorFixture fixture = newTestReplicatorFixture(configuration);
        TestPersistentReplicator replicator = fixture.replicator;
        ProducerImpl<?> producer = mock(ProducerImpl.class);
        when(producer.isWritable()).thenReturn(true);
        replicator.setProducerForTest(producer);
        List<ReadRequest> requests = new ArrayList<>();
        doAnswer(invocation -> {
            assertThat(Thread.holdsLock(producer)).isFalse();
            requests.add(new ReadRequest(invocation.getArgument(2), invocation.getArgument(3)));
            return null;
        }).when(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());
        doAnswer(invocation -> {
            assertThat(Thread.holdsLock(producer)).isFalse();
            return null;
        }).when(fixture.cursor).rewind();
        doAnswer(invocation -> {
            assertThat(Thread.holdsLock(producer)).isFalse();
            return false;
        }).when(fixture.cursor).cancelPendingReadRequest();
        List<PersistentReplicator.ProducerSendCallback> callbacks = new ArrayList<>();
        replicator.entryObserver = (entry, task, entries) -> callbacks.add(
                PersistentReplicator.ProducerSendCallback.create(replicator, entry, null, task));

        replicator.readMoreEntries();
        ReadRequest read = requests.get(0);
        read.callback.readEntriesComplete(List.of(entry(0), entry(1)), read.context);
        assertThat(requests).hasSize(1);
        assertThat(callbacks).hasSize(2);
        synchronized (producer) {
            callbacks.get(0).sendComplete(failedSend ? new PulsarClientException("send failed") : null, null);
            callbacks.get(1).sendComplete(null, null);
            assertThat(requests).hasSize(1);
            verify(fixture.cursor, never()).rewind();
            verify(fixture.cursor, never()).cancelPendingReadRequest();
            assertThat(fixture.queuedWork).hasSize(1);
        }
        fixture.runQueuedWork();
        assertThat(requests).hasSize(2);
        if (failedSend) {
            verify(fixture.cursor).rewind();
            assertThat(replicator.waitForCursorRewindingRefCnf).isZero();
        }
    }

    @Test
    public void testRejectedAckHandoffTerminatesWithoutDrainingOnAckThread() throws Exception {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setReplicationProducerQueueSize(2);
        TestReplicatorFixture fixture = newTestReplicatorFixture(configuration);
        TestPersistentReplicator replicator = fixture.replicator;
        ProducerImpl<?> producer = mock(ProducerImpl.class);
        when(producer.isWritable()).thenReturn(true);
        when(producer.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        replicator.setProducerForTest(producer);
        List<ReadRequest> requests = new ArrayList<>();
        doAnswer(invocation -> {
            requests.add(new ReadRequest(invocation.getArgument(2), invocation.getArgument(3)));
            return null;
        }).when(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());
        List<PersistentReplicator.ProducerSendCallback> callbacks = new ArrayList<>();
        replicator.entryObserver = (entry, task, entries) -> callbacks.add(
                PersistentReplicator.ProducerSendCallback.create(replicator, entry, null, task));
        replicator.readMoreEntries();
        Entry first = entry(0);
        Entry second = entry(1);
        ReadRequest read = requests.get(0);
        read.callback.readEntriesComplete(List.of(first, second), read.context);
        doThrow(new RejectedExecutionException("ACK handoff rejected"))
                .when(fixture.executor).execute(any(Runnable.class));

        synchronized (producer) {
            callbacks.get(0).sendComplete(null, null);
            callbacks.get(1).sendComplete(null, null);
        }

        assertThat(replicator.getState()).isEqualTo(State.Terminated);
        assertThat(requests).hasSize(1);
        assertThat(fixture.queuedWork).isEmpty();
        verify(fixture.cursor, never()).rewind();
        verify(fixture.cursor, never()).cancelPendingReadRequest();
        verify(first).release();
        verify(second).release();
        verify(producer).closeAsync();
    }

    @Test
    public void testFailedPublishCompletesInFlightTaskBeforeQueuedRecovery() throws Exception {
        TestReplicatorFixture fixture = newTestReplicatorFixture();
        TestPersistentReplicator replicator = fixture.replicator;
        ProducerImpl<?> producer = mock(ProducerImpl.class);
        when(producer.isWritable()).thenReturn(true);
        replicator.setProducerForTest(producer);
        Entry entry = entry(0);
        InFlightTask task = new InFlightTask(PositionFactory.create(1, 1), 1, replicator.getReplicatorId());
        task.setEntries(List.of(entry));
        task.setSubmissionComplete(true);
        replicator.inFlightTasks.add(task);
        assertThat(replicator.getPermitsIfNoPendingRead()).isEqualTo(999);

        PersistentReplicator.ProducerSendCallback callback =
                PersistentReplicator.ProducerSendCallback.create(replicator, entry, null, task);
        callback.sendComplete(new PulsarClientException.ProducerBlockedQuotaExceededException("test failure"), null);

        assertThat(task.isDone()).isTrue();
        assertThat(task.getCompletedEntries()).isEqualTo(1);
        verify(entry).release();
        assertThat(replicator.getPermitsIfNoPendingRead()).isEqualTo(1000);
        assertThat(fixture.queuedWork).hasSize(1);
        verify(fixture.cursor, never()).rewind();
        verify(fixture.cursor, never()).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());

        fixture.runQueuedWork();
        InOrder cursorCalls = inOrder(fixture.cursor);
        cursorCalls.verify(fixture.cursor).rewind();
        cursorCalls.verify(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());
    }

    @Test
    public void testRewindFailureRetriesWithoutTerminatingOrRecursing() throws Exception {
        TestReplicatorFixture fixture = newTestReplicatorFixture();
        TestPersistentReplicator replicator = fixture.replicator;
        doThrow(new AssertionError("transient rewind failure")).doNothing().when(fixture.cursor).rewind();

        replicator.beforeTerminateOrCursorRewinding(
                PersistentReplicator.ReasonOfWaitForCursorRewinding.Fetching_Schema);
        replicator.doRewindCursor(true);

        assertThat(replicator.getState()).isEqualTo(State.Started);
        verify(fixture.cursor).rewind();
        ScheduledWork retry = fixture.takeScheduledWork();
        retry.command.run();

        verify(fixture.cursor, times(2)).rewind();
        verify(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());
        assertThat(replicator.getState()).isEqualTo(State.Started);
    }

    @Test
    public void testCursorThrowingAfterCompletionDoesNotDiscardPublishedResult() throws Exception {
        TestReplicatorFixture fixture = newTestReplicatorFixture();
        TestPersistentReplicator replicator = fixture.replicator;
        replicator.entryObserver = (entry, task, entries) -> replicator.submittedEntries.add(entry.getEntryId());
        AtomicInteger readCalls = new AtomicInteger();
        doAnswer(invocation -> {
            if (readCalls.incrementAndGet() == 1) {
                ReadEntriesCallback callback = invocation.getArgument(2);
                callback.readEntriesComplete(List.of(entry(0)), invocation.getArgument(3));
                throw new AssertionError("cursor threw after publishing its result");
            }
            return null;
        }).when(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());

        replicator.readMoreEntries();

        assertThat(replicator.submittedEntries).containsExactly(0L);
        assertThat(readCalls).hasValue(2);
        assertThat(fixture.scheduledWork).isEmpty();
        assertThat(replicator.getState()).isEqualTo(State.Started);
    }

    @Test
    public void testTerminationReleasesResultPublishedDuringFailedCancellation() throws Exception {
        TestReplicatorFixture fixture = newTestReplicatorFixture();
        TestPersistentReplicator replicator = fixture.replicator;
        List<ReadRequest> requests = new ArrayList<>();
        doAnswer(invocation -> {
            requests.add(new ReadRequest(invocation.getArgument(2), invocation.getArgument(3)));
            return null;
        }).when(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());
        replicator.readMoreEntries();
        ReadRequest pendingRead = requests.get(0);
        Entry entry = entry(0);
        doAnswer(invocation -> {
            pendingRead.callback.readEntriesComplete(List.of(entry), pendingRead.context);
            throw new AssertionError("cancellation failed during shutdown");
        }).when(fixture.cursor).cancelPendingReadRequest();

        replicator.markTerminated();
        replicator.beforeTerminate();

        verify(entry).release();
        assertThat(((InFlightTask) pendingRead.context).isDone()).isTrue();
        assertThat(requests).hasSize(1);
        assertThat(fixture.scheduledWork).isEmpty();
    }

    @Test
    public void testStaleCompletionAfterTerminationReleasesEntriesWithoutStartingRead() throws Exception {
        TestReplicatorFixture fixture = newTestReplicatorFixture();
        TestPersistentReplicator replicator = fixture.replicator;
        List<ReadRequest> requests = new ArrayList<>();
        doAnswer(invocation -> {
            synchronized (requests) {
                requests.add(new ReadRequest(invocation.getArgument(2), invocation.getArgument(3)));
            }
            return null;
        }).when(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());

        replicator.readMoreEntries();
        ReadRequest activeRead;
        synchronized (requests) {
            assertThat(requests).hasSize(1);
            activeRead = requests.get(0);
        }
        replicator.markTerminated();
        Entry staleEntry = entry(0);
        activeRead.callback.readEntriesComplete(List.of(staleEntry), activeRead.context);
        fixture.runQueuedWork();

        verify(staleEntry).release();
        assertThat(replicator.submittedEntries).isEmpty();
        synchronized (requests) {
            assertThat(requests).hasSize(1);
        }
    }

    private static List<Long> entryIds(int count) {
        List<Long> ids = new ArrayList<>(count);
        for (long id = 0; id < count; id++) {
            ids.add(id);
        }
        return ids;
    }

    private static Entry entry(long entryId) {
        Entry entry = mock(Entry.class);
        when(entry.getEntryId()).thenReturn(entryId);
        return entry;
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting for the test batch to continue");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for the test batch to continue", e);
        }
    }

    private static TestReplicatorFixture newTestReplicatorFixture() throws Exception {
        return newTestReplicatorFixture(new ServiceConfiguration());
    }

    @SuppressWarnings("unchecked")
    private static TestReplicatorFixture newTestReplicatorFixture(ServiceConfiguration configuration) throws Exception {
        configuration.setClusterName("local");
        configuration.setDispatcherMaxReadBatchSize(1000);
        configuration.setDispatcherMaxReadSizeBytes(1024 * 1024);

        PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getConfiguration()).thenReturn(configuration);
        when(pulsar.getConfig()).thenReturn(configuration);
        when(pulsar.getClient()).thenReturn(mock(PulsarClientImpl.class));
        when(pulsar.getAdminClient()).thenReturn(mock(PulsarAdmin.class));

        BrokerService brokerService = mock(BrokerService.class);
        EventLoopGroup executor = mock(EventLoopGroup.class);
        Queue<Runnable> queuedWork = new ConcurrentLinkedQueue<>();
        Queue<ScheduledWork> scheduledWork = new ConcurrentLinkedQueue<>();
        doAnswer(invocation -> {
            queuedWork.add(invocation.getArgument(0));
            return null;
        }).when(executor).execute(any(Runnable.class));
        doAnswer(invocation -> {
            scheduledWork.add(new ScheduledWork(invocation.getArgument(0), invocation.getArgument(1),
                    invocation.getArgument(2)));
            return null;
        }).when(executor).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        when(brokerService.pulsar()).thenReturn(pulsar);
        when(brokerService.getPulsar()).thenReturn(pulsar);
        when(brokerService.executor()).thenReturn(executor);

        ProducerBuilder<byte[]> producerBuilder = mock(ProducerBuilder.class);
        when(producerBuilder.topic(any())).thenReturn(producerBuilder);
        when(producerBuilder.messageRoutingMode(any())).thenReturn(producerBuilder);
        when(producerBuilder.enableBatching(anyBoolean())).thenReturn(producerBuilder);
        when(producerBuilder.sendTimeout(anyInt(), any(TimeUnit.class))).thenReturn(producerBuilder);
        when(producerBuilder.maxPendingMessages(anyInt())).thenReturn(producerBuilder);
        when(producerBuilder.producerName(any())).thenReturn(producerBuilder);

        PulsarClientImpl replicationClient = mock(PulsarClientImpl.class);
        when(replicationClient.newProducer(any(Schema.class))).thenReturn(producerBuilder);

        PersistentTopic topic = mock(PersistentTopic.class);
        when(topic.getName()).thenReturn("persistent://prop/ns/replicator-read-processing");
        when(topic.getReplicatorPrefix()).thenReturn("pulsar.repl");
        when(topic.getBrokerService()).thenReturn(brokerService);
        when(topic.getMaxReadPosition()).thenReturn(PositionFactory.create(1, 10000));

        ManagedCursor cursor = mock(ManagedCursor.class);
        when(cursor.getName()).thenReturn("pulsar.repl.remote");
        when(cursor.getReadPosition()).thenReturn(PositionFactory.create(1, 1));

        TestPersistentReplicator replicator = new TestPersistentReplicator(topic, cursor, brokerService,
                replicationClient, mock(PulsarAdmin.class));
        return new TestReplicatorFixture(replicator, cursor, executor, queuedWork, scheduledWork);
    }

    private record ReadRequest(ReadEntriesCallback callback, Object context) {
    }

    private record ScheduledWork(Runnable command, long delay, TimeUnit unit) {
    }

    private static final class TestReplicatorFixture {
        private final TestPersistentReplicator replicator;
        private final ManagedCursor cursor;
        private final EventLoopGroup executor;
        private final Queue<Runnable> queuedWork;
        private final Queue<ScheduledWork> scheduledWork;

        private TestReplicatorFixture(TestPersistentReplicator replicator, ManagedCursor cursor,
                                      EventLoopGroup executor,
                                      Queue<Runnable> queuedWork, Queue<ScheduledWork> scheduledWork) {
            this.replicator = replicator;
            this.cursor = cursor;
            this.executor = executor;
            this.queuedWork = queuedWork;
            this.scheduledWork = scheduledWork;
        }

        private void runQueuedWork() {
            int executed = 0;
            Runnable runnable;
            while ((runnable = queuedWork.poll()) != null) {
                if (++executed > 10000) {
                    throw new AssertionError("Read processing kept scheduling work without becoming idle");
                }
                runnable.run();
            }
        }

        private ScheduledWork takeScheduledWork() {
            assertThat(scheduledWork).hasSize(1);
            return scheduledWork.remove();
        }
    }

    @FunctionalInterface
    private interface EntryObserver {
        void onEntry(Entry entry, InFlightTask task, List<Entry> entries);
    }

    private static final class TestPersistentReplicator extends PersistentReplicator {
        private final List<Long> submittedEntries = Collections.synchronizedList(new ArrayList<>());
        private final AtomicBoolean rewindBeforeCursorInvocation = new AtomicBoolean();
        private final AtomicBoolean cancelBeforeCursorInvocation = new AtomicBoolean();
        private EntryObserver entryObserver;
        private RuntimeException producerStartFailure;
        private Runnable producerStartAction;

        private TestPersistentReplicator(PersistentTopic topic, ManagedCursor cursor, BrokerService brokerService,
                                         PulsarClientImpl replicationClient, PulsarAdmin replicationAdmin)
                throws PulsarServerException {
            super("local", topic, cursor, "remote", topic.getName(), brokerService, replicationClient,
                    replicationAdmin);
            state = State.Started;
        }

        @Override
        protected void startProducer() {
            // The test drives read scheduling directly.
            if (producerStartAction != null) {
                producerStartAction.run();
            }
            if (producerStartFailure != null) {
                throw producerStartFailure;
            }
        }

        @Override
        protected String getProducerName() {
            return "test-replicator";
        }

        @Override
        protected boolean isWritable() {
            return true;
        }

        @Override
        InFlightTask createOrRecycleInFlightTaskIntoQueue(Position readPos, int readingEntries) {
            InFlightTask task = super.createOrRecycleInFlightTaskIntoQueue(readPos, readingEntries);
            if (rewindBeforeCursorInvocation.compareAndSet(true, false)) {
                beforeTerminateOrCursorRewinding(ReasonOfWaitForCursorRewinding.Fetching_Schema);
                doRewindCursor(true);
            } else if (cancelBeforeCursorInvocation.compareAndSet(true, false)) {
                beforeTerminateOrCursorRewinding(ReasonOfWaitForCursorRewinding.Fetching_Schema);
            }
            return task;
        }

        private void markTerminated() {
            state = State.Terminated;
        }

        private void markDisconnected() {
            state = State.Disconnected;
        }

        private void markStarted() {
            state = State.Started;
        }

        private void setProducerForTest(ProducerImpl<?> producer) {
            this.producer = producer;
        }

        @Override
        protected boolean replicateEntries(List<Entry> entries, InFlightTask inFlightTask) {
            for (Entry entry : entries) {
                entryObserver.onEntry(entry, inFlightTask, entries);
            }
            return !entries.isEmpty();
        }
    }
}
