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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.util.HashedWheelTimer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.AbortedTxnProcessor;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TopicTransactionBufferCloseTest {

    @DataProvider
    public Object[][] transactionCompletionModes() {
        return new Object[][] { { true, false }, { true, true }, { false, false }, { false, true } };
    }

    @Test(dataProvider = "transactionCompletionModes", timeOut = 10_000)
    public void testTxnCompletionUpdatesTopicOutsideBufferLock(boolean replay, boolean abort) throws Exception {
        Position position = PositionFactory.create(1, 1);
        CompletableFuture<Position> recoveryFuture = new CompletableFuture<>();
        try (TestContext context = new TestContext(recoveryFuture, position)) {
            // Fenced publish failures close the buffer while holding the topic monitor.
            doAnswer(__ -> {
                assertThat(Thread.holdsLock(context.transactionBuffer))
                        .as("updating the topic must not acquire its monitor while holding the buffer monitor")
                        .isFalse();
                return null;
            }).when(context.topic).updateLastDispatchablePosition(null);

            TxnID txnID = new TxnID(1, 1);
            if (replay) {
                Entry marker = mock(Entry.class);
                when(marker.getLedgerId()).thenReturn(position.getLedgerId());
                when(marker.getEntryId()).thenReturn(position.getEntryId());
                when(marker.getMessageMetadata()).thenReturn(new MessageMetadata()
                        .setTxnidMostBits(txnID.getMostSigBits())
                        .setTxnidLeastBits(txnID.getLeastSigBits())
                        .setMarkerType((abort ? MarkerType.TXN_ABORT : MarkerType.TXN_COMMIT).getValue()));
                when(context.managedCursor.hasMoreEntries()).thenReturn(true, false);
                doAnswer(invocation -> {
                    AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(1);
                    callback.readEntriesComplete(List.of(marker), null);
                    return null;
                }).when(context.managedCursor).asyncReadEntries(anyInt(), any(), anyLong(), any());

                recoveryFuture.complete(PositionFactory.create(1, 0));
                context.transactionBuffer.getTransactionBufferFuture().get(5, TimeUnit.SECONDS);
                verify(marker).release();
            } else {
                recoveryFuture.complete(position);
                context.transactionBuffer.getTransactionBufferFuture().get(5, TimeUnit.SECONDS);
                doAnswer(invocation -> {
                    AsyncCallbacks.AddEntryCallback callback = invocation.getArgument(1);
                    callback.addComplete(position, invocation.getArgument(0), null);
                    return null;
                }).when(context.managedLedger).asyncAddEntry(any(ByteBuf.class), any(), any());

                CompletableFuture<Void> completion = abort
                        ? context.transactionBuffer.abortTxn(txnID, 0)
                        : context.transactionBuffer.commitTxn(txnID, 0);
                completion.get(5, TimeUnit.SECONDS);
            }
            verify(context.topic).updateLastDispatchablePosition(null);
        }
    }

    @Test(timeOut = 10_000)
    public void testCloseInducedRecoveryFailureDoesNotCloseTopicAgain() throws Exception {
        CompletableFuture<Position> recoveryFuture = new CompletableFuture<>();
        try (TestContext context = new TestContext(recoveryFuture, PositionFactory.EARLIEST)) {
            context.awaitExecutorsIdle();
            when(context.processor.closeAsync()).thenAnswer(__ -> {
                recoveryFuture.completeExceptionally(
                        new BrokerServiceException.ServiceUnitNotReadyException("processor closed"));
                return CompletableFuture.completedFuture(null);
            });

            context.transactionBuffer.closeAsync().get(5, TimeUnit.SECONDS);
            context.awaitExecutorsIdle();

            verify(context.topic, never()).close(true);
            assertTrue(context.transactionBuffer.getTransactionBufferFuture().isCompletedExceptionally());
        }
    }

    @Test(timeOut = 10_000)
    public void testRecoveryContinuationDoesNotStartAfterClose() throws Exception {
        CompletableFuture<Position> recoveryFuture = new CompletableFuture<>();
        try (TestContext context = new TestContext(recoveryFuture, PositionFactory.EARLIEST)) {
            context.awaitExecutorsIdle();

            context.transactionBuffer.closeAsync().get(5, TimeUnit.SECONDS);
            recoveryFuture.complete(PositionFactory.EARLIEST);
            context.awaitExecutorsIdle();

            verify(context.managedLedger, never()).newNonDurableCursor(any(), anyString());
            assertTrue(context.transactionBuffer.getTransactionBufferFuture().isCompletedExceptionally());
        }
    }

    @Test(timeOut = 10_000)
    public void testCloseCancelsQueuedRecoveryReplay() throws Exception {
        CompletableFuture<Position> recoveryFuture = new CompletableFuture<>();
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        try (TestContext context = new TestContext(recoveryFuture, PositionFactory.EARLIEST)) {
            try {
                context.awaitExecutorsIdle();
                AtomicReference<Future<?>> submittedTask = new AtomicReference<>();
                doAnswer(invocation -> {
                    Future<?> task = (Future<?>) invocation.callRealMethod();
                    submittedTask.set(task);
                    return task;
                }).when(context.recoveryExecutor).submit(any(Runnable.class));
                context.recoveryExecutor.execute(() -> {
                    blockerStarted.countDown();
                    try {
                        releaseBlocker.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));

                recoveryFuture.complete(PositionFactory.EARLIEST);
                Future<?> replayTask = submittedTask.get();
                assertNotNull(replayTask);

                context.transactionBuffer.closeAsync().get(5, TimeUnit.SECONDS);

                assertTrue(replayTask.isCancelled());
            } finally {
                releaseBlocker.countDown();
            }
        }
    }

    @Test(timeOut = 10_000)
    public void testLateRecoveryReadIsReleasedAfterClose() throws Exception {
        Position startPosition = PositionFactory.create(1, 0);
        Position lastPosition = PositionFactory.create(1, 3);
        CompletableFuture<Position> recoveryFuture = new CompletableFuture<>();
        CountDownLatch readStarted = new CountDownLatch(1);
        AtomicReference<AsyncCallbacks.ReadEntriesCallback> readCallback = new AtomicReference<>();
        try (TestContext context = new TestContext(recoveryFuture, lastPosition)) {
            when(context.managedCursor.hasMoreEntries()).thenReturn(true);
            doAnswer(invocation -> {
                readCallback.set(invocation.getArgument(1));
                readStarted.countDown();
                return null;
            }).when(context.managedCursor).asyncReadEntries(anyInt(), any(), anyLong(), any());

            recoveryFuture.complete(startPosition);
            assertTrue(readStarted.await(5, TimeUnit.SECONDS));
            // Live transaction work must still run while the recovery read is outstanding.
            context.transactionExecutor.submit(() -> { }).get(5, TimeUnit.SECONDS);
            context.transactionBuffer.closeAsync().get(5, TimeUnit.SECONDS);
            context.awaitExecutorsIdle();

            Entry lateEntry = mock(Entry.class);
            readCallback.get().readEntriesComplete(List.of(lateEntry), null);

            verify(lateEntry).release();
            verify(context.managedCursor).asyncReadEntries(anyInt(), any(), anyLong(), any());
        }
    }

    private static final class TestContext implements AutoCloseable {
        private final ExecutorService transactionExecutor = Executors.newSingleThreadExecutor();
        private final ListeningScheduledExecutorService recoveryExecutor =
                spy(MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor()));
        private final AbortedTxnProcessor processor = mock(AbortedTxnProcessor.class);
        private final PersistentTopic topic = mock(PersistentTopic.class);
        private final ManagedLedgerImpl managedLedger = mock(ManagedLedgerImpl.class);
        private final ManagedCursor managedCursor = mock(ManagedCursor.class);
        private final TopicTransactionBuffer transactionBuffer;

        private TestContext(CompletableFuture<Position> recoveryFuture, Position lastConfirmedEntry) throws Exception {
            BrokerService brokerService = mock(BrokerService.class);
            PulsarService pulsar = mock(PulsarService.class);
            ServiceConfiguration configuration = mock(ServiceConfiguration.class);
            ExecutorProvider executorProvider = mock(ExecutorProvider.class);
            OrderedScheduler recoveryScheduler = mock(OrderedScheduler.class);

            when(topic.getName()).thenReturn("persistent://public/default/test-close-during-recovery");
            when(topic.getBrokerService()).thenReturn(brokerService);
            when(topic.getManagedLedger()).thenReturn(managedLedger);
            when(brokerService.getPulsar()).thenReturn(pulsar);
            when(pulsar.getConfiguration()).thenReturn(configuration);
            when(pulsar.getTransactionTimer()).thenReturn(mock(HashedWheelTimer.class));
            when(pulsar.getTransactionExecutorProvider()).thenReturn(executorProvider);
            when(executorProvider.getExecutor(any(Object.class))).thenReturn(transactionExecutor);
            when(pulsar.getTransactionSnapshotRecoverExecutorProvider()).thenReturn(recoveryScheduler);
            when(recoveryScheduler.chooseThread(any(Object.class))).thenReturn(recoveryExecutor);
            when(managedLedger.getLastConfirmedEntry()).thenReturn(lastConfirmedEntry);
            when(managedLedger.getConfig()).thenReturn(new ManagedLedgerConfig());
            when(managedLedger.newNonDurableCursor(any(), anyString())).thenReturn(managedCursor);
            when(processor.recoverFromSnapshot()).thenReturn(recoveryFuture);
            when(processor.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));

            transactionBuffer = new TopicTransactionBuffer(topic, processor, AbortedTxnProcessor.SnapshotType.Single);
        }

        private void awaitExecutorsIdle() throws Exception {
            transactionExecutor.submit(() -> { }).get(5, TimeUnit.SECONDS);
            recoveryExecutor.submit(() -> { }).get(5, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws Exception {
            transactionBuffer.closeAsync().get(5, TimeUnit.SECONDS);
            transactionExecutor.shutdownNow();
            recoveryExecutor.shutdownNow();
            assertTrue(transactionExecutor.awaitTermination(5, TimeUnit.SECONDS));
            assertTrue(recoveryExecutor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

}
