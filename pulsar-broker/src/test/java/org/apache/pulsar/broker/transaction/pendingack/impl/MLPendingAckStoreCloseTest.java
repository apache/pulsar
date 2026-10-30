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
package org.apache.pulsar.broker.transaction.pendingack.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.BadVersionException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFencedException;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.pendingack.TransactionPendingAckStoreProvider;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.transaction.coordinator.impl.DisabledTxnLogBufferedWriterMetricsStats;
import org.apache.pulsar.transaction.coordinator.impl.TxnLogBufferedWriterConfig;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Closing a replay cursor must not strand its store or resume a subscription with a closed handle. */
public class MLPendingAckStoreCloseTest {

    @DataProvider(name = "cursorCloseFailures")
    public Object[][] cursorCloseFailures() {
        return new Object[][] {
                {null, false},
                {new BadVersionException("A newer owner changed the replay cursor"), false},
                {new ManagedLedgerFencedException("A newer owner fenced the replay log"), false},
                {new ManagedLedgerException("Unexpected cursor close failure"), true}
        };
    }

    @Test(dataProvider = "cursorCloseFailures", timeOut = 10_000)
    public void testCursorCloseAlwaysCleansLedgerAndWriter(ManagedLedgerException failure, boolean shouldFail)
            throws Exception {
        try (TestContext context = new TestContext()) {
            CompletableFuture<Void> append = context.bufferAppend();
            CompletableFuture<Void> closed = context.store.closeAsync();
            context.completeCursorClose(failure);

            assertThat(context.ledgerClose.get())
                    .as("cursor close failure must not skip managed-ledger cleanup").isNotNull();
            assertThat(closed).as("store close must await ledger cleanup").isNotDone();
            context.completeLedgerClose(null);
            assertThat(closed).as("store close must await queued buffered-writer cleanup").isNotDone();
            context.writerExecutor.runAll();

            assertCloseResult(closed, shouldFail ? failure : null);
            context.assertWriterClosed(append);
        }
    }

    @DataProvider(name = "ledgerCloseFailures")
    public Object[][] ledgerCloseFailures() {
        return new Object[][] {
                {new ManagedLedgerFencedException("The raw log was fenced and evicted"), false},
                {new BadVersionException("Unexpected managed-ledger close conflict"), true},
                {new ManagedLedgerException("Unexpected managed-ledger close failure"), true}
        };
    }

    @Test(dataProvider = "ledgerCloseFailures", timeOut = 10_000)
    public void testLedgerCloseFailureStillCleansWriter(ManagedLedgerException failure, boolean shouldFail)
            throws Exception {
        try (TestContext context = new TestContext()) {
            CompletableFuture<Void> append = context.bufferAppend();
            CompletableFuture<Void> closed = context.store.closeAsync();
            context.completeCursorClose(null);
            context.completeLedgerClose(failure);

            assertThat(closed).as("a ledger close failure must still await writer cleanup").isNotDone();
            context.writerExecutor.runAll();
            assertCloseResult(closed, shouldFail ? failure : null);
            context.assertWriterClosed(append);
        }
    }

    @Test(timeOut = 10_000)
    public void testConcurrentAndRepeatedCloseShareCleanupCompletion() throws Exception {
        try (TestContext context = new TestContext()) {
            CompletableFuture<Void> append = context.bufferAppend();
            CompletableFuture<Void> first = context.store.closeAsync();
            CompletableFuture<Void> concurrent = context.store.closeAsync();
            assertThat(concurrent).as("concurrent close must share the original cleanup future").isSameAs(first);
            verify(context.cursor, times(1)).asyncClose(any(), any());
            context.completeCursorClose(null);
            context.completeLedgerClose(null);

            assertThat(first).as("writer cleanup is queued and has not run").isNotDone();
            assertThat(context.store.closeAsync()).isSameAs(first);
            context.writerExecutor.runAll();
            first.get(1, TimeUnit.SECONDS);
            context.assertWriterClosed(append);
            assertThat(context.store.closeAsync()).isSameAs(first);
            verify(context.ledger, times(1)).asyncClose(any(), any());
        }
    }

    @Test(timeOut = 10_000)
    public void testExpectedCursorConflictDoesNotResumeClosedPendingAckHandle() throws Exception {
        try (TestContext context = new TestContext()) {
            PersistentSubscription subscription = context.newSubscription();
            PendingAckHandleImpl handle = (PendingAckHandleImpl) subscription.getPendingAckHandle();
            handle.pendingAckHandleFuture().get(1, TimeUnit.SECONDS);
            assertThat(handle.getState()).isEqualTo(PendingAckHandleState.State.Ready);
            CompletableFuture<Void> closed = subscription.close(false, Optional.empty());
            context.completeCursorClose(new BadVersionException("A newer cursor owner won the CAS"));

            assertThat(handle.getState()).isEqualTo(PendingAckHandleState.State.Close);
            verify(subscription, never()).resumeAfterFence();
            assertThat(context.ledgerClose.get()).as("the subscription must finish releasing the raw store")
                    .isNotNull();
            context.completeLedgerClose(null);
            context.writerExecutor.runAll();
            closed.get(1, TimeUnit.SECONDS);
            assertThat(handle.getState()).isEqualTo(PendingAckHandleState.State.Close);
            verify(subscription, never()).resumeAfterFence();
        }
    }

    private static void assertCloseResult(CompletableFuture<Void> closed, ManagedLedgerException failure)
            throws Exception {
        if (failure == null) {
            closed.get(1, TimeUnit.SECONDS);
        } else {
            try {
                closed.get(1, TimeUnit.SECONDS);
                throw new AssertionError("Unexpected cleanup errors must remain visible");
            } catch (ExecutionException exception) {
                assertThat(exception.getCause()).isSameAs(failure);
            }
        }
    }

    private static final class QueuedExecutor implements Executor {
        private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();

        @Override
        public void execute(Runnable task) {
            tasks.add(task);
        }

        void runAll() {
            Runnable task;
            while ((task = tasks.poll()) != null) {
                task.run();
            }
        }
    }

    private static final class TestContext implements AutoCloseable {
        final ManagedLedger ledger = mock(ManagedLedger.class);
        final ManagedCursor cursor = mock(ManagedCursor.class);
        final Timeout timeout = mock(Timeout.class);
        final QueuedExecutor writerExecutor = new QueuedExecutor();
        final AtomicReference<CloseCallback> cursorClose = new AtomicReference<>();
        final AtomicReference<CloseCallback> ledgerClose = new AtomicReference<>();
        final ExecutorService handleExecutor = MoreExecutors.newDirectExecutorService();
        final MLPendingAckStore store;

        TestContext() {
            when(ledger.getName()).thenReturn("test/pending-ack-log");
            when(ledger.getLastConfirmedEntry()).thenReturn(PositionFactory.EARLIEST);
            when(cursor.getName()).thenReturn("__pending_ack_state");
            when(cursor.getMarkDeletedPosition()).thenReturn(PositionFactory.EARLIEST);
            doAnswer(invocation -> {
                cursorClose.set(invocation.getArgument(0));
                return null;
            }).when(cursor).asyncClose(any(), any());
            doAnswer(invocation -> {
                ledgerClose.set(invocation.getArgument(0));
                return null;
            }).when(ledger).asyncClose(any(), any());
            Timer timer = mock(Timer.class);
            when(timer.newTimeout(any(), anyLong(), any())).thenReturn(timeout);
            TxnLogBufferedWriterConfig config = new TxnLogBufferedWriterConfig();
            config.setBatchEnabled(true);
            config.setBatchedWriteMaxRecords(10);
            config.setBatchedWriteMaxDelayInMillis(60_000);
            store = new MLPendingAckStore(ledger, cursor, mock(ManagedCursor.class), 1, config, timer,
                    DisabledTxnLogBufferedWriterMetricsStats.DISABLED_BUFFERED_WRITER_METRICS, writerExecutor);
        }

        CompletableFuture<Void> bufferAppend() {
            CompletableFuture<Void> append = store.appendAbortMark(new TxnID(1, 1), AckType.Individual);
            writerExecutor.runAll();
            assertThat(append).as("the real writer keeps this record buffered").isNotDone();
            return append;
        }

        void completeCursorClose(ManagedLedgerException failure) {
            complete(cursorClose.getAndSet(null), failure);
        }

        void completeLedgerClose(ManagedLedgerException failure) {
            complete(ledgerClose.getAndSet(null), failure);
        }

        private static void complete(CloseCallback callback, ManagedLedgerException failure) {
            if (callback != null) {
                if (failure == null) {
                    callback.closeComplete(null);
                } else {
                    callback.closeFailed(failure, null);
                }
            }
        }

        void assertWriterClosed(CompletableFuture<Void> append) throws Exception {
            verify(timeout).cancel();
            assertThat(append.isCompletedExceptionally()).as("buffered append must not hang after close").isTrue();
            CompletableFuture<Void> rejected = store.appendAbortMark(new TxnID(1, 2), AckType.Individual);
            writerExecutor.runAll();
            assertThat(rejected.isCompletedExceptionally()).as("a closed writer must reject later appends").isTrue();
        }

        PersistentSubscription newSubscription() {
            PersistentTopic topic = mock(PersistentTopic.class, RETURNS_DEEP_STUBS);
            PulsarService pulsar = mock(PulsarService.class, RETURNS_DEEP_STUBS);
            when(topic.getName()).thenReturn("persistent://test/ns/close-conflict");
            when(topic.getBrokerService().getPulsar()).thenReturn(pulsar);
            when(topic.getBrokerService().pulsar()).thenReturn(pulsar);
            ServiceConfiguration config = new ServiceConfiguration();
            config.setTransactionCoordinatorEnabled(true);
            when(pulsar.getConfig()).thenReturn(config);
            when(pulsar.getTransactionExecutorProvider().getExecutor(any(Object.class))).thenReturn(handleExecutor);
            TransactionPendingAckStoreProvider provider = mock(TransactionPendingAckStoreProvider.class);
            when(provider.checkInitializedBefore(any())).thenReturn(CompletableFuture.completedFuture(true));
            when(provider.newPendingAckStore(any())).thenReturn(CompletableFuture.completedFuture(store));
            when(pulsar.getTransactionPendingAckStoreProvider()).thenReturn(provider);
            ManagedCursor businessCursor = mock(ManagedCursor.class);
            when(businessCursor.getName()).thenReturn("business");
            return spy(new PersistentSubscription(topic, "business", businessCursor, false));
        }

        @Override
        public void close() {
            store.closeAsync();
            completeCursorClose(null);
            completeLedgerClose(null);
            writerExecutor.runAll();
            handleExecutor.shutdownNow();
        }
    }
}
