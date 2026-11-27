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
package org.apache.pulsar.broker.transaction.buffer.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.util.HashedWheelTimer;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.TransactionBufferSnapshotServiceFactory;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.AbortedTxnProcessor;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.policies.data.TransactionBufferStats;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TopicTransactionBufferStatsTest {

    private static final String TOPIC = "persistent://public/default/txn-buffer-stats";
    private static final long LEDGER_ID = 1;
    private static final Position LAST_CONFIRMED = PositionFactory.create(LEDGER_ID, 0);

    @Test(timeOut = 30_000)
    public void testSegmentStatsWhileAbortSealsSegment() throws Exception {
        PausingPosition firstMarker = new PausingPosition(LEDGER_ID, 1);
        try (TestContext context = new TestContext()) {
            context.abort(new TxnID(1, 1), firstMarker).get(5, TimeUnit.SECONDS);
            context.abort(new TxnID(1, 2), PositionFactory.create(LEDGER_ID, 2)).get(5, TimeUnit.SECONDS);

            Future<TransactionBufferStats> stats;
            CompletableFuture<Void> abort;
            try {
                // Pause the stats call while it lists the first of the two sealed segments.
                firstMarker.pauseNextToString();
                stats = context.statsExecutor.submit(() -> context.buffer.getStats(false, true));
                assertThat(firstMarker.awaitPaused()).as("the stats call must list the segments").isTrue();

                abort = context.abortAsync(new TxnID(1, 3), PositionFactory.create(LEDGER_ID, 3));
                Awaitility.await().atMost(10, TimeUnit.SECONDS)
                        .until(() -> abort.isDone() || context.isAbortBlockedOnBuffer());
            } finally {
                // Closing the buffer needs the monitor that the paused stats call holds, so resume it first.
                firstMarker.resume();
            }

            assertThatCode(() -> stats.get(5, TimeUnit.SECONDS))
                    .as("getStats must not fail while an abort seals a snapshot segment")
                    .doesNotThrowAnyException();
            assertThat(stats.get(5, TimeUnit.SECONDS).segmentsStats.segmentStats)
                    .as("the stats must show the segments from before the concurrent abort")
                    .hasSize(2);
            abort.get(5, TimeUnit.SECONDS);
            assertThat(context.buffer.getStats(false, true).segmentsStats.segmentStats)
                    .as("the stats must show the third segment after the abort finished")
                    .hasSize(3);
        }
    }

    /** A marker position whose {@code toString} can be held, to pause the thread that formats it. */
    private static final class PausingPosition implements Position {
        private final long ledgerId;
        private final long entryId;
        private final AtomicBoolean pauseNext = new AtomicBoolean();
        private final CountDownLatch paused = new CountDownLatch(1);
        private final CountDownLatch resumed = new CountDownLatch(1);

        private PausingPosition(long ledgerId, long entryId) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }

        void pauseNextToString() {
            pauseNext.set(true);
        }

        boolean awaitPaused() throws InterruptedException {
            return paused.await(10, TimeUnit.SECONDS);
        }

        void resume() {
            resumed.countDown();
        }

        @Override
        public long getLedgerId() {
            return ledgerId;
        }

        @Override
        public long getEntryId() {
            return entryId;
        }

        @Override
        public String toString() {
            if (pauseNext.compareAndSet(true, false)) {
                paused.countDown();
                try {
                    if (!resumed.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("The paused toString was not resumed");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            return ledgerId + ":" + entryId;
        }

        @Override
        public int hashCode() {
            return hashCodeForPosition();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Position && compareTo((Position) obj) == 0;
        }
    }

    private static final class TestContext implements AutoCloseable {
        private final ExecutorService transactionExecutor = Executors.newSingleThreadExecutor();
        private final ListeningScheduledExecutorService recoveryExecutor =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor());
        private final ExecutorService abortExecutor = Executors.newSingleThreadExecutor();
        private final ExecutorService statsExecutor = Executors.newSingleThreadExecutor();
        private final Queue<Position> markerPositions = new ConcurrentLinkedQueue<>();
        private final TopicTransactionBuffer buffer;
        private final Thread abortThread;

        TestContext() throws Exception {
            AbortedTxnProcessor createdProcessor = null;
            TopicTransactionBuffer createdBuffer = null;
            try {
                PersistentTopic topic = mock(PersistentTopic.class);
                BrokerService brokerService = mock(BrokerService.class);
                PulsarService pulsar = mock(PulsarService.class);
                ServiceConfiguration configuration = mock(ServiceConfiguration.class);
                ExecutorProvider transactionExecutors = mock(ExecutorProvider.class);
                OrderedScheduler recoveryExecutors = mock(OrderedScheduler.class);
                TransactionBufferSnapshotServiceFactory snapshotServices =
                        mock(TransactionBufferSnapshotServiceFactory.class, RETURNS_DEEP_STUBS);
                ManagedLedgerImpl managedLedger = mock(ManagedLedgerImpl.class);
                when(topic.getName()).thenReturn(TOPIC);
                when(topic.getBrokerService()).thenReturn(brokerService);
                when(topic.getManagedLedger()).thenReturn(managedLedger);
                when(topic.getMaxReadPositionCallBack())
                        .thenReturn(mock(TopicTransactionBuffer.MaxReadPositionCallBack.class));
                when(brokerService.getPulsar()).thenReturn(pulsar);
                when(pulsar.getConfiguration()).thenReturn(configuration);
                when(configuration.getTransactionBufferSnapshotMaxTransactionCount()).thenReturn(1000);
                when(configuration.getTransactionBufferSnapshotMinTimeInMillis()).thenReturn(1000);
                // Every aborted transaction seals a segment.
                when(configuration.getTransactionBufferSnapshotSegmentSize()).thenReturn(8 + TOPIC.length() + 3);
                when(pulsar.getTransactionTimer()).thenReturn(mock(HashedWheelTimer.class));
                when(pulsar.getTransactionExecutorProvider()).thenReturn(transactionExecutors);
                when(transactionExecutors.getExecutor(any(Object.class))).thenReturn(transactionExecutor);
                when(pulsar.getTransactionSnapshotRecoverExecutorProvider()).thenReturn(recoveryExecutors);
                when(recoveryExecutors.chooseThread(any(Object.class))).thenReturn(recoveryExecutor);
                when(pulsar.getTransactionBufferSnapshotServiceFactory()).thenReturn(snapshotServices);
                // Snapshot writes do not complete, so the sealed segments stay queued for writing.
                when(snapshotServices.getTxnBufferSnapshotSegmentService().getReferenceWriter(any()).getFuture())
                        .thenReturn(new CompletableFuture<>());
                when(snapshotServices.getTxnBufferSnapshotIndexService().getReferenceWriter(any()).getFuture())
                        .thenReturn(new CompletableFuture<>());
                when(managedLedger.getLastConfirmedEntry()).thenReturn(LAST_CONFIRMED);
                when(managedLedger.getConfig()).thenReturn(new ManagedLedgerConfig());
                when(managedLedger.newNonDurableCursor(any(), anyString())).thenReturn(mock(ManagedCursor.class));
                when(managedLedger.getLedgersInfo())
                        .thenReturn(new TreeMap<>(Map.of(LEDGER_ID, new LedgerInfo().setLedgerId(LEDGER_ID))));
                doAnswer(invocation -> {
                    AsyncCallbacks.AddEntryCallback callback = invocation.getArgument(1);
                    callback.addComplete(markerPositions.remove(), invocation.getArgument(0), null);
                    return null;
                }).when(managedLedger).asyncAddEntry(any(ByteBuf.class), any(), any());

                // Skip reading the snapshot topics on recovery; every other call reaches the segmented processor.
                createdProcessor = mock(AbortedTxnProcessor.class,
                        delegatesTo(new SnapshotSegmentAbortedTxnProcessorImpl(topic)));
                doReturn(CompletableFuture.completedFuture(LAST_CONFIRMED)).when(createdProcessor)
                        .recoverFromSnapshot();
                createdBuffer = new TopicTransactionBuffer(topic, createdProcessor,
                        AbortedTxnProcessor.SnapshotType.Segment);
                createdBuffer.checkIfTBRecoverCompletely().get(5, TimeUnit.SECONDS);
                abortThread = abortExecutor.submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
            } catch (Throwable failure) {
                // try-with-resources does not close a context whose constructor failed.
                try {
                    close(createdBuffer, createdProcessor);
                } catch (Throwable closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
            buffer = createdBuffer;
        }

        CompletableFuture<Void> abort(TxnID txnID, Position markerPosition) {
            markerPositions.add(markerPosition);
            return buffer.abortTxn(txnID, 0);
        }

        CompletableFuture<Void> abortAsync(TxnID txnID, Position markerPosition) {
            return CompletableFuture.supplyAsync(() -> abort(txnID, markerPosition), abortExecutor)
                    .thenCompose(future -> future);
        }

        boolean isAbortBlockedOnBuffer() {
            ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(abortThread.threadId());
            return info != null && info.getThreadState() == Thread.State.BLOCKED && info.getLockInfo() != null
                    && info.getLockInfo().getIdentityHashCode() == System.identityHashCode(buffer);
        }

        @Override
        public void close() throws Exception {
            close(buffer, null);
        }

        /** Closes the buffer, or the processor when the buffer was not created, and stops the executors. */
        private void close(TopicTransactionBuffer bufferToClose, AbortedTxnProcessor processorToClose)
                throws Exception {
            try {
                if (bufferToClose != null) {
                    bufferToClose.closeAsync().get(5, TimeUnit.SECONDS);
                } else if (processorToClose != null) {
                    processorToClose.closeAsync().get(5, TimeUnit.SECONDS);
                }
            } finally {
                List<ExecutorService> executors =
                        List.of(transactionExecutor, recoveryExecutor, abortExecutor, statsExecutor);
                executors.forEach(ExecutorService::shutdownNow);
                List<ExecutorService> running = new ArrayList<>();
                for (ExecutorService executor : executors) {
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                        running.add(executor);
                    }
                }
                assertThat(running).as("test executors that did not stop").isEmpty();
            }
        }
    }
}
