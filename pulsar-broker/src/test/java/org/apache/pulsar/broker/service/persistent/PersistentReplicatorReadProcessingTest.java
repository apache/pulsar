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
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.channel.EventLoopGroup;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
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
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator.InFlightTask;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.awaitility.Awaitility;
import org.mockito.InOrder;
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
    public void testCachedReadCallbacksDoNotRecurseAndLeaveOnePendingRead() throws Exception {
        TestReplicatorFixture fixture = newTestReplicatorFixture();
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
    public void testReadFailureDoesNotBypassRetryDeadlineWhenMoreReadDemandArrives() throws Exception {
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

        // Producer acknowledgements and other demand sources all reach the same readMoreEntries path.
        replicator.readMoreEntries();
        ScheduledWork retry = fixture.takeScheduledWork();
        synchronized (requests) {
            assertThat(requests).hasSize(1);
        }

        Awaitility.await().atMost(retry.delay + TimeUnit.SECONDS.toMillis(1), TimeUnit.MILLISECONDS).until(() ->
                System.nanoTime() - retry.scheduledAtNanos >= retry.unit.toNanos(retry.delay));
        retry.command.run();

        synchronized (requests) {
            assertThat(requests).hasSize(2);
        }
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

    @SuppressWarnings("unchecked")
    private static TestReplicatorFixture newTestReplicatorFixture() throws Exception {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setClusterName("local");
        configuration.setReplicationProducerQueueSize(1000);
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
                    invocation.getArgument(2), System.nanoTime()));
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
        return new TestReplicatorFixture(replicator, cursor, queuedWork, scheduledWork);
    }

    private record ReadRequest(ReadEntriesCallback callback, Object context) {
    }

    private record ScheduledWork(Runnable command, long delay, TimeUnit unit, long scheduledAtNanos) {
    }

    private static final class TestReplicatorFixture {
        private final TestPersistentReplicator replicator;
        private final ManagedCursor cursor;
        private final Queue<Runnable> queuedWork;
        private final Queue<ScheduledWork> scheduledWork;

        private TestReplicatorFixture(TestPersistentReplicator replicator, ManagedCursor cursor,
                                      Queue<Runnable> queuedWork, Queue<ScheduledWork> scheduledWork) {
            this.replicator = replicator;
            this.cursor = cursor;
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

        @Override
        protected boolean replicateEntries(List<Entry> entries, InFlightTask inFlightTask) {
            for (Entry entry : entries) {
                entryObserver.onEntry(entry, inFlightTask, entries);
            }
            return !entries.isEmpty();
        }
    }
}
