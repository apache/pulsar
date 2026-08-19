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

import static org.apache.pulsar.transaction.coordinator.impl.TxnLogBufferedWriter.BATCHED_ENTRY_DATA_PREFIX_MAGIC_NUMBER;
import static org.apache.pulsar.transaction.coordinator.impl.TxnLogBufferedWriter.BATCHED_ENTRY_DATA_PREFIX_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckReplyCallBack;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckStore;
import org.apache.pulsar.broker.transaction.pendingack.TransactionPendingAckStoreProvider;
import org.apache.pulsar.broker.transaction.pendingack.proto.PendingAckMetadataEntry;
import org.apache.pulsar.broker.transaction.pendingack.proto.PendingAckOp;
import org.apache.pulsar.broker.transaction.util.LogIndexLagBackoff;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.transaction.coordinator.impl.DisabledTxnLogBufferedWriterMetricsStats;
import org.apache.pulsar.transaction.coordinator.impl.TxnLogBufferedWriterConfig;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker")
public class MLPendingAckStoreTest extends TransactionTestBase {

    private PersistentSubscription persistentSubscriptionMock;

    private ManagedCursor managedCursorMock;

    private ExecutorService internalPinnedExecutor;

    private int pendingAckLogIndexMinLag = 1;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        setUpBase(1, 1, NAMESPACE1 + "/test", 0);
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {
        String topic = NAMESPACE1 + "/test-txn-topic";
        admin.topics().createNonPartitionedTopic(topic);
        PersistentTopic persistentTopic = (PersistentTopic) getPulsarServiceList().get(0).getBrokerService()
                .getTopic(topic, false).get().get();
        getPulsarServiceList().get(0).getConfig().setTransactionPendingAckLogIndexMinLag(pendingAckLogIndexMinLag);
        CompletableFuture<Subscription> subscriptionFuture = persistentTopic.createSubscription("test",
                CommandSubscribe.InitialPosition.Earliest, false, null);
        PersistentSubscription subscription = (PersistentSubscription) subscriptionFuture.get();
        ManagedCursor managedCursor = subscription.getCursor();
        this.managedCursorMock = spy(managedCursor);
        this.persistentSubscriptionMock = spy(subscription);
        when(this.persistentSubscriptionMock.getCursor()).thenReturn(managedCursorMock);
        this.internalPinnedExecutor = this.persistentSubscriptionMock
                .getTopic()
                .getBrokerService()
                .getPulsar()
                .getTransactionExecutorProvider()
                .getExecutor(this);
    }

    @AfterMethod(alwaysRun = true)
    private void afterMethod() throws Exception {
        ServiceConfiguration defaultConfig = new ServiceConfiguration();
        ServiceConfiguration serviceConfiguration =
                persistentSubscriptionMock.getTopic().getBrokerService().getPulsar().getConfiguration();
        serviceConfiguration.setTransactionPendingAckBatchedWriteMaxRecords(
                defaultConfig.getTransactionPendingAckBatchedWriteMaxRecords()
        );
        serviceConfiguration.setTransactionPendingAckBatchedWriteMaxSize(
                defaultConfig.getTransactionPendingAckBatchedWriteMaxSize()
        );
        serviceConfiguration.setTransactionPendingAckBatchedWriteMaxDelayInMillis(
                defaultConfig.getTransactionPendingAckBatchedWriteMaxDelayInMillis()
        );
        serviceConfiguration.setTransactionPendingAckBatchedWriteEnabled(defaultConfig
                .isTransactionPendingAckBatchedWriteEnabled());
        admin.topics().delete("persistent://" + NAMESPACE1 + "/test-txn-topic", true);
    }

    @AfterClass
    public void cleanup(){
        super.internalCleanup();
    }

    private MLPendingAckStore createPendingAckStore(TxnLogBufferedWriterConfig txnLogBufferedWriterConfig)
            throws Exception {
        MLPendingAckStoreProvider mlPendingAckStoreProvider = new MLPendingAckStoreProvider();
        ServiceConfiguration serviceConfiguration =
                persistentSubscriptionMock.getTopic().getBrokerService().getPulsar().getConfiguration();
        serviceConfiguration.setTransactionPendingAckBatchedWriteMaxRecords(
                txnLogBufferedWriterConfig.getBatchedWriteMaxRecords()
        );
        serviceConfiguration.setTransactionPendingAckBatchedWriteMaxSize(
                txnLogBufferedWriterConfig.getBatchedWriteMaxSize()
        );
        serviceConfiguration.setTransactionPendingAckBatchedWriteMaxDelayInMillis(
                txnLogBufferedWriterConfig.getBatchedWriteMaxDelayInMillis()
        );
        serviceConfiguration.setTransactionPendingAckBatchedWriteEnabled(txnLogBufferedWriterConfig.isBatchEnabled());
        return (MLPendingAckStore) mlPendingAckStoreProvider.newPendingAckStore(persistentSubscriptionMock).get();
    }

    @Test
    public void testPendingAckStoreWithSlashSubscriptionName() throws Exception {
        String slashSubName = "tenant/namespace/my-function";
        when(persistentSubscriptionMock.getName()).thenReturn(slashSubName);

        MLPendingAckStoreProvider provider = new MLPendingAckStoreProvider();

        // Should not throw — subscription names containing '/' must be URL-encoded so the
        // resulting pending-ack topic name is a valid V2 persistent topic name.
        MLPendingAckStore store = (MLPendingAckStore) provider.newPendingAckStore(persistentSubscriptionMock).get();

        // Verify the managed ledger persistence path encodes the subscription name correctly.
        // Expected: tenant/namespace/persistent/<encodedLocalName>
        // where localName = "<topic>-<encodedSubName>__transaction_pending_ack"
        String originTopicName = persistentSubscriptionMock.getTopic().getName();
        TopicName origin = TopicName.get(originTopicName);
        String encodedSubName = Codec.encode(slashSubName);
        String expectedLocalName = origin.getLocalName() + "-" + encodedSubName
                + SystemTopicNames.PENDING_ACK_STORE_SUFFIX;
        // getPersistenceNamingEncoding() = tenant/namespace/persistent/encodedLocalName
        String expectedMlName = origin.getTenant() + "/" + origin.getNamespacePortion()
                + "/persistent/" + Codec.encode(expectedLocalName);
        Assert.assertEquals(store.getManagedLedger().get().getName(), expectedMlName);

        closePendingAckStoreWithRetry(store);
    }

    @Test
    public void testReplayFailurePreservesClosedHandle() throws Exception {
        PendingAckHandleImpl handle = new PendingAckHandleImpl(persistentSubscriptionMock);
        handle.pendingAckHandleFuture().get(10, TimeUnit.SECONDS);
        handle.closeAsync().get(10, TimeUnit.SECONDS);

        // Multiple late failures must not turn Close into Error and then enable a retry from Error.
        for (int i = 0; i < 2; i++) {
            handle.exceptionHandleFuture(new ManagedLedgerException.CursorAlreadyClosedException("closed"));
            Assert.assertEquals(handle.getState(), PendingAckHandleState.State.Close);
            Assert.assertFalse(handle.changeToNoneStateIfNotClosed());
            Assert.assertFalse(handle.changeToInitializingState());
        }
    }

    @DataProvider(name = "lateReplayFailures")
    public Object[][] lateReplayFailures() {
        return new Object[][] {
                {new ManagedLedgerException.CursorAlreadyClosedException("closed")},
                {new ManagedLedgerException.ManagedLedgerFencedException()}
        };
    }

    @Test(dataProvider = "lateReplayFailures")
    public void testReplayFailureDoesNotHideStoreFromConcurrentClose(ManagedLedgerException failure) throws Exception {
        // Create the log so the handle opens a real store during initialization.
        closePendingAckStoreWithRetry(createPendingAckStore(new TxnLogBufferedWriterConfig()));
        PendingAckHandleImpl handle = new PendingAckHandleImpl(persistentSubscriptionMock);
        handle.pendingAckHandleFuture().get(10, TimeUnit.SECONDS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) handle.getStoreManageLedger().get(10, TimeUnit.SECONDS);
        ExecutorService closeExecutor = Executors.newSingleThreadExecutor(
                new DefaultThreadFactory("pending-ack-close"));
        try {
            Future<CompletableFuture<Void>> closeResult;
            synchronized (handle) {
                closeResult = closeExecutor.submit(handle::closeAsync);
                // closeAsync publishes Close before taking this monitor. Hold it as replayFailed does,
                // ensuring the failure runs before closeAsync can retrieve the store future.
                Awaitility.await().atMost(10, TimeUnit.SECONDS).until(handle::checkIfClose);
                handle.exceptionHandleFuture(failure);
            }
            closeResult.get(10, TimeUnit.SECONDS).get(10, TimeUnit.SECONDS);
            Assert.assertEquals(ledger.getState(), ManagedLedgerImpl.State.Closed,
                    "The store must still be closed after a late replay failure");
        } finally {
            closeExecutor.shutdownNow();
            ledger.close();
        }
    }

    @Test
    public void testCloseDuringPendingAckStoreCreation() throws Exception {
        PersistentSubscription subscription = mock(PersistentSubscription.class, RETURNS_DEEP_STUBS);
        doReturn(persistentSubscriptionMock.getTopicName()).when(subscription).getTopicName();
        when(subscription.getName()).thenReturn("close-during-store-creation");
        var pulsar = subscription.getTopic().getBrokerService().getPulsar();
        when(pulsar.getTransactionExecutorProvider().getExecutor(any(PendingAckHandleImpl.class)))
                .thenReturn(internalPinnedExecutor);
        TransactionPendingAckStoreProvider provider = mock(TransactionPendingAckStoreProvider.class);
        when(pulsar.getTransactionPendingAckStoreProvider()).thenReturn(provider);
        CompletableFuture<Boolean> initialized = new CompletableFuture<>();
        when(provider.checkInitializedBefore(subscription)).thenReturn(initialized);
        PendingAckStore store = mock(PendingAckStore.class);
        CompletableFuture<Void> storeClosed = new CompletableFuture<>();
        when(store.closeAsync()).thenReturn(storeClosed);
        CompletableFuture<PendingAckStore> storeCreated = new CompletableFuture<>();
        CompletableFuture<CompletableFuture<Void>> closeStarted = new CompletableFuture<>();
        PendingAckHandleImpl handle = new PendingAckHandleImpl(subscription);
        when(provider.newPendingAckStore(subscription)).thenAnswer(invocation -> {
            // Close after init's state check, but before the provider returns its future.
            closeStarted.complete(handle.closeAsync());
            return storeCreated;
        });
        try {
            initialized.complete(true);
            CompletableFuture<Void> closed = closeStarted.get(10, TimeUnit.SECONDS);
            assertThat(closed).as("Close must wait for the store being created").isNotDone();
            storeCreated.complete(store);
            verify(store, timeout(10000)).closeAsync();
            assertThat(closed).as("Close must wait for store cleanup").isNotDone();
            storeClosed.complete(null);
            closed.get(10, TimeUnit.SECONDS);
            handle.getInternalPinnedExecutor().submit(() -> { }).get(10, TimeUnit.SECONDS);
            verify(store, never()).replayAsync(any(), any());
            assertThat(handle.getState()).isEqualTo(PendingAckHandleState.State.Close);
        } finally {
            storeCreated.complete(store);
            storeClosed.complete(null);
            handle.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @DataProvider(name = "replayEntryPayloads")
    public Object[][] replayEntryPayloads() {
        return new Object[][] {
                {new byte[] {0}, true}, // Too short to decode the entry prefix.
                {ByteBuffer.allocate(4).putShort(BATCHED_ENTRY_DATA_PREFIX_MAGIC_NUMBER)
                        .putShort(BATCHED_ENTRY_DATA_PREFIX_VERSION).array(), false} // Empty batch.
        };
    }

    @Test(dataProvider = "replayEntryPayloads")
    public void testReplayReleasesPolledAndQueuedEntries(byte[] payload, boolean expectFailure) {
        ManagedCursor cursor = createReplayCursorMock();
        when(cursor.hasMoreEntries()).thenReturn(true);
        EntryImpl first = EntryImpl.create(5, 10, payload);
        EntryImpl second = EntryImpl.create(5, 11, payload);
        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(1);
            callback.readEntriesComplete(List.of(first, second), null);
            return null;
        }).when(cursor).asyncReadEntries(anyInt(), any(), any(), any());
        try {
            MLPendingAckStore store = createPendingAckStoreForReplay(cursor);
            PendingAckReplyCallBack callback = mock(PendingAckReplyCallBack.class);
            store.new PendingAckReplay(callback, internalPinnedExecutor).run();
            if (expectFailure) {
                verify(callback).replayFailed(any(IndexOutOfBoundsException.class));
                verify(callback, never()).replayComplete();
            } else {
                verify(callback).replayComplete();
                verify(callback, never()).replayFailed(any());
            }
            Assert.assertEquals(first.refCnt(), 0, "The polled entry must be released");
            Assert.assertEquals(second.refCnt(), 0, "The queued entry must be released");
        } finally {
            if (first.refCnt() > 0) {
                first.release();
            }
            if (second.refCnt() > 0) {
                second.release();
            }
        }
    }

    private MLPendingAckStore createPendingAckStoreForReplay(ManagedCursor cursor) {
        return createPendingAckStoreForReplay(cursor, new ManagedLedgerConfig());
    }

    /**
     * Builds a store whose replay loop starts in the state described by the parameters, without going
     * through the provider: the replay guard compares {@code lastConfirmedEntry} (a snapshot of the
     * managed ledger's last confirmed entry, taken here) against a load position seeded from the
     * cursor's mark-delete position, while whether anything can still be read is decided separately by
     * {@link ManagedCursor#hasMoreEntries()}.
     */
    private MLPendingAckStore createPendingAckStoreForReplay(ManagedCursor cursor, ManagedLedgerConfig mlConfig) {
        return createPendingAckStoreForReplay(cursor, mlConfig, mock(Timer.class), false);
    }

    private MLPendingAckStore createPendingAckStoreForReplay(ManagedCursor cursor, ManagedLedgerConfig mlConfig,
                                                             Timer timer, boolean batchEnabled) {
        ManagedLedger managedLedger = mock(ManagedLedger.class);
        when(managedLedger.getName()).thenReturn("test-pending-ack-log");
        when(managedLedger.getConfig()).thenReturn(mlConfig);
        // Deliberately ahead of the cursor's mark-delete position, so the replay loop's guard stays true.
        when(managedLedger.getLastConfirmedEntry()).thenReturn(PositionFactory.create(5, 10));
        TxnLogBufferedWriterConfig config = new TxnLogBufferedWriterConfig();
        config.setBatchEnabled(batchEnabled);
        return new MLPendingAckStore(managedLedger, cursor, mock(ManagedCursor.class), 1, config,
                timer, DisabledTxnLogBufferedWriterMetricsStats.DISABLED_BUFFERED_WRITER_METRICS,
                internalPinnedExecutor);
    }

    private ManagedCursor createReplayCursorMock() {
        ManagedCursor cursor = mock(ManagedCursor.class);
        when(cursor.getName()).thenReturn("test-pending-ack-cursor");
        when(cursor.getMarkDeletedPosition()).thenReturn(PositionFactory.create(1, 0));
        return cursor;
    }

    /**
     * The replay loop must finish once the cursor has nothing left to read, even though its termination
     * guard still believes there is work to do. Those two are measured from different positions -- the
     * guard from the cursor's mark-delete position, the read gate from its read position -- so they can
     * disagree permanently, for instance after the ledger holding the mark-delete position was trimmed
     * and the cursor was recovered onto a later ledger. Before the fix the loop spun on Thread.sleep(1)
     * forever, holding its executor thread and starving every other subscription hashed onto it.
     */
    @Test
    public void testReplayCompletesWhenCursorHasNoMoreEntries() throws Exception {
        ManagedCursor cursor = createReplayCursorMock();
        when(cursor.isClosed()).thenReturn(false);
        // No entry between the mark-delete position and the snapshot can be read any more.
        when(cursor.hasMoreEntries()).thenReturn(false);
        MLPendingAckStore pendingAckStore = createPendingAckStoreForReplay(cursor);

        ExecutorService replayExecutor = Executors.newSingleThreadExecutor();
        try {
            PendingAckHandleImpl pendingAckHandle = mock(PendingAckHandleImpl.class);
            when(pendingAckHandle.getInternalPinnedExecutor()).thenReturn(replayExecutor);
            when(pendingAckHandle.changeToReadyState()).thenReturn(true);
            CountDownLatch replayFinished = new CountDownLatch(1);
            doAnswer(invocation -> {
                replayFinished.countDown();
                return null;
            }).when(pendingAckHandle).completeHandleFuture();

            pendingAckStore.replayAsync(pendingAckHandle, replayExecutor);

            // The replay thread is shared by every subscription hashed onto it, so a task queued behind a
            // finished replay must still get to run. This is what the stall actually broke.
            CountDownLatch queuedBehindReplay = new CountDownLatch(1);
            replayExecutor.execute(queuedBehindReplay::countDown);

            Assert.assertTrue(replayFinished.await(10, TimeUnit.SECONDS),
                    "Replay never completed: the replay loop did not terminate");
            Assert.assertTrue(queuedBehindReplay.await(10, TimeUnit.SECONDS),
                    "A task queued behind the replay never ran: the replay thread was not released");
        } finally {
            replayExecutor.shutdownNow();
        }
    }

    /**
     * A read whose completion never arrives leaves the replay loop waiting with no way to make progress.
     * Closing the cursor -- which is what unloading the topic does -- must stop it, so that the replay
     * thread cannot outlive the subscription it belongs to.
     */
    @Test
    public void testReplayStopsWhenCursorIsClosedWhileWaitingForEntries() throws Exception {
        ManagedCursor cursor = createReplayCursorMock();
        when(cursor.hasMoreEntries()).thenReturn(true);
        AtomicBoolean cursorClosed = new AtomicBoolean(false);
        when(cursor.isClosed()).thenAnswer(invocation -> cursorClosed.get());
        CountDownLatch readIssued = new CountDownLatch(1);
        // Drop the read: neither readEntriesComplete nor readEntriesFailed is ever invoked, so the loop
        // keeps waiting for entries that never arrive.
        doAnswer(invocation -> {
            readIssued.countDown();
            return null;
        }).when(cursor).asyncReadEntries(anyInt(), any(), any(), any());
        MLPendingAckStore pendingAckStore = createPendingAckStoreForReplay(cursor);

        ExecutorService replayExecutor = Executors.newSingleThreadExecutor();
        try {
            PendingAckHandleImpl pendingAckHandle = mock(PendingAckHandleImpl.class);
            when(pendingAckHandle.getInternalPinnedExecutor()).thenReturn(replayExecutor);

            pendingAckStore.replayAsync(pendingAckHandle, replayExecutor);
            Assert.assertTrue(readIssued.await(10, TimeUnit.SECONDS), "Replay never issued a read");

            cursorClosed.set(true);

            verify(pendingAckHandle, timeout(TimeUnit.SECONDS.toMillis(10)))
                    .exceptionHandleFuture(any(ManagedLedgerException.CursorAlreadyClosedException.class));
            // The replay must have released the thread it was holding.
            CountDownLatch queuedBehindReplay = new CountDownLatch(1);
            replayExecutor.execute(queuedBehindReplay::countDown);
            Assert.assertTrue(queuedBehindReplay.await(10, TimeUnit.SECONDS),
                    "A task queued behind the replay never ran: the replay thread was not released");
        } finally {
            replayExecutor.shutdownNow();
        }
    }

    /**
     * A read can still be in flight when the replay ends, and its completion runs on a managed ledger
     * thread. Entries delivered after the replay has gone must be released by the callback itself,
     * because nothing will ever take them off the queue.
     */
    @Test
    public void testEntriesDeliveredAfterReplayEndedAreReleased() throws Exception {
        ManagedCursor cursor = createReplayCursorMock();
        when(cursor.hasMoreEntries()).thenReturn(true);
        AtomicBoolean cursorClosed = new AtomicBoolean(false);
        when(cursor.isClosed()).thenAnswer(invocation -> cursorClosed.get());
        CountDownLatch readIssued = new CountDownLatch(1);
        AtomicReference<AsyncCallbacks.ReadEntriesCallback> readCallback = new AtomicReference<>();
        // Capture the callback and never complete it, so the read is still in flight when the replay ends.
        doAnswer(invocation -> {
            readCallback.set(invocation.getArgument(1));
            readIssued.countDown();
            return null;
        }).when(cursor).asyncReadEntries(anyInt(), any(), any(), any());
        MLPendingAckStore pendingAckStore = createPendingAckStoreForReplay(cursor);

        ExecutorService replayExecutor = Executors.newSingleThreadExecutor();
        try {
            PendingAckHandleImpl pendingAckHandle = mock(PendingAckHandleImpl.class);
            when(pendingAckHandle.getInternalPinnedExecutor()).thenReturn(internalPinnedExecutor);

            pendingAckStore.replayAsync(pendingAckHandle, replayExecutor);
            Assert.assertTrue(readIssued.await(10, TimeUnit.SECONDS), "Replay never issued a read");

            // End the replay while the read is still outstanding.
            cursorClosed.set(true);
            verify(pendingAckHandle, timeout(TimeUnit.SECONDS.toMillis(10)))
                    .exceptionHandleFuture(any(ManagedLedgerException.CursorAlreadyClosedException.class));

            // The read now completes, far too late for the replay to consume anything.
            Entry first = mock(Entry.class);
            Entry second = mock(Entry.class);
            readCallback.get().readEntriesComplete(List.of(first, second), null);

            verify(first).release();
            verify(second).release();
        } finally {
            replayExecutor.shutdownNow();
        }
    }

    /**
     * Shutting down the replay executor must end the replay rather than leaving the thread behind. The
     * replay is incomplete at that point, so it must be reported as failed and not as complete --
     * otherwise the handle would be marked Ready with only part of the pending ack state applied.
     */
    @Test
    public void testReplayStopsWhenInterrupted() throws Exception {
        ManagedCursor cursor = createReplayCursorMock();
        when(cursor.isClosed()).thenReturn(false);
        when(cursor.hasMoreEntries()).thenReturn(true);
        CountDownLatch readIssued = new CountDownLatch(1);
        // Drop the read, so the replay is waiting for entries that never arrive when it is interrupted.
        doAnswer(invocation -> {
            readIssued.countDown();
            return null;
        }).when(cursor).asyncReadEntries(anyInt(), any(), any(), any());
        MLPendingAckStore pendingAckStore = createPendingAckStoreForReplay(cursor);

        ExecutorService replayExecutor = Executors.newSingleThreadExecutor();
        try {
            PendingAckHandleImpl pendingAckHandle = mock(PendingAckHandleImpl.class);
            when(pendingAckHandle.getInternalPinnedExecutor()).thenReturn(internalPinnedExecutor);

            pendingAckStore.replayAsync(pendingAckHandle, replayExecutor);
            Assert.assertTrue(readIssued.await(10, TimeUnit.SECONDS), "Replay never issued a read");

            replayExecutor.shutdownNow();

            Assert.assertTrue(replayExecutor.awaitTermination(10, TimeUnit.SECONDS),
                    "The replay thread did not stop after shutdownNow()");
            verify(pendingAckHandle, timeout(TimeUnit.SECONDS.toMillis(10)))
                    .exceptionHandleFuture(any(InterruptedException.class));
            // An interrupted replay is incomplete and must never be reported as successful.
            verify(pendingAckHandle, never()).completeHandleFuture();
        } finally {
            replayExecutor.shutdownNow();
        }
    }

    /**
     * Makes {@code cursor} fail its first read with {@code failure}, either inline inside asyncReadEntries or
     * later, when the returned callback is completed by the test, as a managed ledger thread would. Every later
     * read completes inline with {@code laterEntries}.
     */
    private AtomicReference<AsyncCallbacks.ReadEntriesCallback> failFirstRead(ManagedCursor cursor,
            ManagedLedgerException failure, boolean failInline, AtomicInteger reads, List<Entry> laterEntries) {
        AtomicReference<AsyncCallbacks.ReadEntriesCallback> pendingRead = new AtomicReference<>();
        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(1);
            if (reads.getAndIncrement() > 0) {
                callback.readEntriesComplete(laterEntries, null);
            } else if (failInline) {
                callback.readEntriesFailed(failure, null);
            } else {
                pendingRead.set(callback);
            }
            return null;
        }).when(cursor).asyncReadEntries(anyInt(), any(), any(), any());
        return pendingRead;
    }

    /**
     * Completes {@code pendingRead} with {@code failure} once the replay has issued it, unless the read
     * already failed inline.
     */
    private static void failPendingRead(AtomicReference<AsyncCallbacks.ReadEntriesCallback> pendingRead,
                                        ManagedLedgerException failure, boolean failInline) {
        if (!failInline) {
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> pendingRead.get() != null);
            // Fail the read while the replay loop keeps polling, so it has to notice the failure between two
            // fillQueue() calls without issuing another read.
            pendingRead.get().readEntriesFailed(failure, null);
        }
    }

    private static void assertReplayThreadReleased(ExecutorService replayExecutor) throws InterruptedException {
        // The replay thread is shared by every subscription assigned to it, so a task queued behind the
        // replay must get to run.
        CountDownLatch queuedBehindReplay = new CountDownLatch(1);
        replayExecutor.execute(queuedBehindReplay::countDown);
        Assert.assertTrue(queuedBehindReplay.await(10, TimeUnit.SECONDS),
                "A task queued behind the replay never ran: the replay thread was not released");
    }

    @DataProvider(name = "unretryableReadFailures")
    public Object[][] unretryableReadFailuresProvider() {
        List<ManagedLedgerException> failures = List.of(
                // A deleted pending ack ledger with autoSkipNonRecoverableData disabled (the default): the read
                // position cannot move, so retrying can never succeed.
                new ManagedLedgerException.LedgerNotExistException("Ledger does not exist"),
                new ManagedLedgerException.NonRecoverableLedgerException("No ledger exist"),
                // A read whose completion the managed ledger executor rejected: the read position has already
                // moved past the entries, so retrying would skip them.
                ManagedLedgerException.getManagedLedgerException(new RejectedExecutionException("shutting down")));
        List<Object[]> cases = new ArrayList<>();
        for (ManagedLedgerException failure : failures) {
            // The read can fail inline, inside asyncReadEntries, or later on a managed ledger thread.
            cases.add(new Object[]{failure, true});
            cases.add(new Object[]{failure, false});
        }
        return cases.toArray(new Object[0][]);
    }

    /**
     * A read failure that retrying cannot fix must fail the replay, handing the original exception unwrapped to
     * {@link PendingAckHandleImpl#exceptionHandleFuture}, which fails the subscription. Before the fix the
     * identical read was re-issued forever, monopolising the replay thread shared by every subscription
     * assigned to it (issue #26374).
     */
    @Test(dataProvider = "unretryableReadFailures")
    public void testUnretryableReadFailureFailsReplay(ManagedLedgerException failure, boolean failInline)
            throws Exception {
        ManagedCursor cursor = createReplayCursorMock();
        when(cursor.isClosed()).thenReturn(false);
        when(cursor.hasMoreEntries()).thenReturn(true);
        AtomicInteger reads = new AtomicInteger();
        AtomicReference<AsyncCallbacks.ReadEntriesCallback> pendingRead =
                failFirstRead(cursor, failure, failInline, reads, List.of());
        Timer timer = mock(Timer.class);
        MLPendingAckStore pendingAckStore =
                createPendingAckStoreForReplay(cursor, new ManagedLedgerConfig(), timer, false);

        ExecutorService replayExecutor = Executors.newSingleThreadExecutor();
        try {
            PendingAckHandleImpl pendingAckHandle = mock(PendingAckHandleImpl.class);
            when(pendingAckHandle.getInternalPinnedExecutor()).thenReturn(replayExecutor);
            // Lets a wrongly reported replayComplete reach completeHandleFuture, where it is caught.
            when(pendingAckHandle.changeToReadyState()).thenReturn(true);

            pendingAckStore.replayAsync(pendingAckHandle, replayExecutor);
            failPendingRead(pendingRead, failure, failInline);

            // The original exception must arrive unwrapped: isRetryableException discriminates on the
            // concrete class.
            verify(pendingAckHandle, timeout(TimeUnit.SECONDS.toMillis(10))).exceptionHandleFuture(same(failure));
            assertReplayThreadReleased(replayExecutor);
            // A failed replay must never also be reported successful. Sequenced after the queued task, so a
            // wrongly scheduled replayComplete would already have run on replayExecutor.
            verify(pendingAckHandle, never()).completeHandleFuture();
            verify(pendingAckHandle, times(1)).exceptionHandleFuture(any());
            // One read, one failure, one outcome: the read is neither re-issued nor retried later.
            assertThat(reads).hasValue(1);
            verify(timer, never()).newTimeout(any(), anyLong(), any());
        } finally {
            replayExecutor.shutdownNow();
        }
    }

    @DataProvider(name = "transientReadFailures")
    public Object[][] transientReadFailuresProvider() {
        List<ManagedLedgerException> failures = List.of(
                // A transient BookKeeper failure ("Bookie handle is not available", BK code -8). The
                // configured read timeout surfaces as this same plain class.
                new ManagedLedgerException("Bookie handle is not available"),
                // Read throttling.
                new ManagedLedgerException.TooManyRequestsException("Too many concurrent reads"));
        List<Object[]> cases = new ArrayList<>();
        for (ManagedLedgerException failure : failures) {
            cases.add(new Object[]{failure, true});
            cases.add(new Object[]{failure, false});
        }
        return cases.toArray(new Object[0][]);
    }

    /**
     * A transient read failure must pause the replay and retry the read after a backoff delay, releasing the
     * replay thread meanwhile. Before the fix the read was re-issued in a hot loop that held the thread
     * (issue #26374).
     */
    @Test(dataProvider = "transientReadFailures")
    public void testTransientReadFailureIsRetriedWithBackoff(ManagedLedgerException failure, boolean failInline)
            throws Exception {
        ManagedCursor cursor = createReplayCursorMock();
        when(cursor.isClosed()).thenReturn(false);
        AtomicInteger reads = new AtomicInteger();
        // The retried read returns the last entry, after which nothing is left to read.
        when(cursor.hasMoreEntries()).thenAnswer(invocation -> reads.get() < 2);
        EntryImpl entry = EntryImpl.create(5, 1, abortEntryPayload(new TxnID(1, 1)));
        AtomicReference<AsyncCallbacks.ReadEntriesCallback> pendingRead =
                failFirstRead(cursor, failure, failInline, reads, List.of(entry));
        Timer timer = mock(Timer.class);
        MLPendingAckStore pendingAckStore =
                createPendingAckStoreForReplay(cursor, new ManagedLedgerConfig(), timer, false);

        ExecutorService replayExecutor = Executors.newSingleThreadExecutor();
        try {
            PendingAckHandleImpl pendingAckHandle = mock(PendingAckHandleImpl.class);
            when(pendingAckHandle.getInternalPinnedExecutor()).thenReturn(replayExecutor);
            when(pendingAckHandle.changeToReadyState()).thenReturn(true);
            CountDownLatch replayFinished = new CountDownLatch(1);
            doAnswer(invocation -> {
                replayFinished.countDown();
                return null;
            }).when(pendingAckHandle).completeHandleFuture();

            pendingAckStore.replayAsync(pendingAckHandle, replayExecutor);
            failPendingRead(pendingRead, failure, failInline);

            ArgumentCaptor<TimerTask> retry = ArgumentCaptor.forClass(TimerTask.class);
            ArgumentCaptor<Long> retryDelay = ArgumentCaptor.forClass(Long.class);
            verify(timer, timeout(TimeUnit.SECONDS.toMillis(10)))
                    .newTimeout(retry.capture(), retryDelay.capture(), same(TimeUnit.MILLISECONDS));
            assertThat(retryDelay.getValue()).as("The retry must be delayed").isPositive();
            assertReplayThreadReleased(replayExecutor);
            // Nothing is read again until the retry is due.
            assertThat(reads).hasValue(1);

            retry.getValue().run(mock(Timeout.class));

            Assert.assertTrue(replayFinished.await(10, TimeUnit.SECONDS), "The retried replay did not complete");
            verify(pendingAckHandle).handleAbort(new TxnID(1, 1), CommandAck.AckType.Individual);
            verify(pendingAckHandle, never()).exceptionHandleFuture(any());
            assertThat(reads).hasValue(2);
            Assert.assertEquals(entry.refCnt(), 0, "The entry read by the retry must be released");
        } finally {
            replayExecutor.shutdownNow();
        }
    }

    /**
     * The replay reads ahead of processing, so entries can still be queued when a read fails. They must be
     * processed, exactly once, before the read is retried: queued entries hold read permits until they are
     * released, so a read failing with TooManyRequestsException could otherwise be retried forever.
     */
    @Test
    public void testTransientReadFailureProcessesQueuedEntriesBeforeRetrying() throws Exception {
        ManagedCursor cursor = createReplayCursorMock();
        when(cursor.isClosed()).thenReturn(false);
        AtomicInteger reads = new AtomicInteger();
        when(cursor.hasMoreEntries()).thenAnswer(invocation -> reads.get() < 3);
        ManagedLedgerException failure = new ManagedLedgerException.TooManyRequestsException("No read permits");
        EntryImpl first = EntryImpl.create(5, 1, abortEntryPayload(new TxnID(1, 1)));
        EntryImpl second = EntryImpl.create(5, 2, abortEntryPayload(new TxnID(1, 2)));
        EntryImpl third = EntryImpl.create(5, 3, abortEntryPayload(new TxnID(1, 3)));
        // The first read returns two entries. The second, issued as soon as the first entry has been processed,
        // fails while the other one is still queued. The retried read returns the last entry.
        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(1);
            int read = reads.getAndIncrement();
            if (read == 0) {
                callback.readEntriesComplete(List.of(first, second), null);
            } else if (read == 1) {
                callback.readEntriesFailed(failure, null);
            } else {
                callback.readEntriesComplete(List.of(third), null);
            }
            return null;
        }).when(cursor).asyncReadEntries(anyInt(), any(), any(), any());
        Timer timer = mock(Timer.class);
        MLPendingAckStore pendingAckStore =
                createPendingAckStoreForReplay(cursor, new ManagedLedgerConfig(), timer, false);

        ExecutorService replayExecutor = Executors.newSingleThreadExecutor();
        try {
            PendingAckHandleImpl pendingAckHandle = mock(PendingAckHandleImpl.class);
            when(pendingAckHandle.getInternalPinnedExecutor()).thenReturn(replayExecutor);
            when(pendingAckHandle.changeToReadyState()).thenReturn(true);
            CountDownLatch replayFinished = new CountDownLatch(1);
            doAnswer(invocation -> {
                replayFinished.countDown();
                return null;
            }).when(pendingAckHandle).completeHandleFuture();

            pendingAckStore.replayAsync(pendingAckHandle, replayExecutor);

            ArgumentCaptor<TimerTask> retry = ArgumentCaptor.forClass(TimerTask.class);
            verify(timer, timeout(TimeUnit.SECONDS.toMillis(10))).newTimeout(retry.capture(), anyLong(), any());
            verify(pendingAckHandle).handleAbort(new TxnID(1, 2), CommandAck.AckType.Individual);
            Assert.assertEquals(second.refCnt(), 0, "The queued entry must be processed and released first");
            assertThat(reads).hasValue(2);

            retry.getValue().run(mock(Timeout.class));

            Assert.assertTrue(replayFinished.await(10, TimeUnit.SECONDS), "The retried replay did not complete");
            verify(pendingAckHandle).handleAbort(new TxnID(1, 1), CommandAck.AckType.Individual);
            verify(pendingAckHandle).handleAbort(new TxnID(1, 2), CommandAck.AckType.Individual);
            verify(pendingAckHandle).handleAbort(new TxnID(1, 3), CommandAck.AckType.Individual);
            verify(pendingAckHandle, never()).exceptionHandleFuture(any());
            verify(cursor, never()).rewind();
            assertThat(reads).hasValue(3);
            Assert.assertEquals(first.refCnt(), 0, "The first entry must be released");
            Assert.assertEquals(third.refCnt(), 0, "The entry read by the retry must be released");
        } finally {
            replayExecutor.shutdownNow();
        }
    }

    /**
     * A retry that cannot be scheduled because the timer has been stopped, when the broker shuts down, must
     * fail the replay instead of leaving it neither running nor finished.
     */
    @Test
    public void testTransientReadFailureFailsReplayWhenRetryCannotBeScheduled() throws Exception {
        ManagedCursor cursor = createReplayCursorMock();
        when(cursor.isClosed()).thenReturn(false);
        when(cursor.hasMoreEntries()).thenReturn(true);
        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(1);
            callback.readEntriesFailed(new ManagedLedgerException("Bookie handle is not available"), null);
            return null;
        }).when(cursor).asyncReadEntries(anyInt(), any(), any(), any());
        Timer timer = mock(Timer.class);
        IllegalStateException timerStopped = new IllegalStateException("cannot be started once stopped");
        when(timer.newTimeout(any(), anyLong(), any())).thenThrow(timerStopped);
        MLPendingAckStore pendingAckStore =
                createPendingAckStoreForReplay(cursor, new ManagedLedgerConfig(), timer, false);

        ExecutorService replayExecutor = Executors.newSingleThreadExecutor();
        try {
            PendingAckHandleImpl pendingAckHandle = mock(PendingAckHandleImpl.class);
            when(pendingAckHandle.getInternalPinnedExecutor()).thenReturn(replayExecutor);

            pendingAckStore.replayAsync(pendingAckHandle, replayExecutor);

            verify(pendingAckHandle, timeout(TimeUnit.SECONDS.toMillis(10))).exceptionHandleFuture(same(timerStopped));
            verify(pendingAckHandle, never()).completeHandleFuture();
        } finally {
            replayExecutor.shutdownNow();
        }
    }

    private static byte[] abortEntryPayload(TxnID txnID) {
        PendingAckMetadataEntry entry = new PendingAckMetadataEntry();
        entry.setPendingAckOp(PendingAckOp.ABORT);
        entry.setAckType(CommandAck.AckType.Individual);
        entry.setTxnidMostBits(txnID.getMostSigBits());
        entry.setTxnidLeastBits(txnID.getLeastSigBits());
        return entry.toByteArray();
    }

    @DataProvider(name = "replayCompletingReadFailures")
    public Object[][] replayCompletingReadFailuresProvider() {
        return new Object[][]{
                {new ManagedLedgerException.NonRecoverableLedgerException("No ledger exist"), true},
                {new ManagedLedgerException.ManagedLedgerFencedException(), false},
                {new ManagedLedgerException.CursorAlreadyClosedException("Cursor already closed"), false},
        };
    }

    /**
     * The three historically recognised read failures keep completing the replay: non-recoverable data
     * is skipped when autoSkipNonRecoverableData is the operator's explicit choice, and a fenced
     * managed ledger or an already-closed cursor means the store is being taken over or torn down,
     * where the handle must still reach Ready to release its callers. Store-level pin of the behaviour
     * TransactionTest#testEndTPRecoveringWhenManagerLedgerDisReadable asserts end to end; a fix that
     * routed every read failure to replayFailed would fail this.
     */
    @Test(dataProvider = "replayCompletingReadFailures")
    public void testReadFailuresThatCompleteTheReplay(ManagedLedgerException failure, boolean autoSkip)
            throws Exception {
        ManagedCursor cursor = createReplayCursorMock();
        when(cursor.isClosed()).thenReturn(false);
        when(cursor.hasMoreEntries()).thenReturn(true);
        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(1);
            callback.readEntriesFailed(failure, null);
            return null;
        }).when(cursor).asyncReadEntries(anyInt(), any(), any(), any());
        ManagedLedgerConfig mlConfig = new ManagedLedgerConfig();
        mlConfig.setAutoSkipNonRecoverableData(autoSkip);
        Timer timer = mock(Timer.class);
        MLPendingAckStore pendingAckStore = createPendingAckStoreForReplay(cursor, mlConfig, timer, false);

        ExecutorService replayExecutor = Executors.newSingleThreadExecutor();
        try {
            PendingAckHandleImpl pendingAckHandle = mock(PendingAckHandleImpl.class);
            when(pendingAckHandle.getInternalPinnedExecutor()).thenReturn(replayExecutor);
            when(pendingAckHandle.changeToReadyState()).thenReturn(true);
            CountDownLatch replayFinished = new CountDownLatch(1);
            doAnswer(invocation -> {
                replayFinished.countDown();
                return null;
            }).when(pendingAckHandle).completeHandleFuture();

            pendingAckStore.replayAsync(pendingAckHandle, replayExecutor);

            Assert.assertTrue(replayFinished.await(10, TimeUnit.SECONDS),
                    "Replay did not complete for " + failure.getClass().getSimpleName());
            verify(pendingAckHandle, never()).exceptionHandleFuture(any());
            verify(timer, never()).newTimeout(any(), anyLong(), any());
        } finally {
            replayExecutor.shutdownNow();
        }
    }

    /**
     * A failed replay orphans its store when the handle retries, because the retry builds a new one. It must
     * therefore close its buffered writer, whose timing flush task otherwise reschedules itself forever when
     * pending ack batching is enabled and nothing is left holding a reference that could stop it.
     */
    @Test
    public void testFailedReplayAttemptClosesBufferedWriter() throws Exception {
        ManagedCursor cursor = createReplayCursorMock();
        when(cursor.isClosed()).thenReturn(false);
        when(cursor.hasMoreEntries()).thenReturn(true);
        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(1);
            callback.readEntriesFailed(new ManagedLedgerException.LedgerNotExistException("Ledger does not exist"),
                    null);
            return null;
        }).when(cursor).asyncReadEntries(anyInt(), any(), any(), any());
        Timer timer = mock(Timer.class);
        Timeout flushTimeout = mock(Timeout.class);
        when(timer.newTimeout(any(), anyLong(), any())).thenReturn(flushTimeout);
        MLPendingAckStore pendingAckStore =
                createPendingAckStoreForReplay(cursor, new ManagedLedgerConfig(), timer, true);

        ExecutorService replayExecutor = Executors.newSingleThreadExecutor();
        try {
            PendingAckHandleImpl pendingAckHandle = mock(PendingAckHandleImpl.class);
            when(pendingAckHandle.getInternalPinnedExecutor()).thenReturn(replayExecutor);

            pendingAckStore.replayAsync(pendingAckHandle, replayExecutor);

            verify(pendingAckHandle, timeout(TimeUnit.SECONDS.toMillis(10))).exceptionHandleFuture(any());
            verify(flushTimeout, timeout(TimeUnit.SECONDS.toMillis(10))).cancel();
        } finally {
            replayExecutor.shutdownNow();
        }
    }

    /**
     * Overridden cases:
     *   1. Batched write and replay with batched feature.
     *   1. Non-batched write and replay without batched feature
     *   1. Batched write and replay without batched feature.
     *   1. Non-batched write and replay with batched feature.
     */
    @DataProvider(name = "mainProcessArgs")
    public Object[][] mainProcessArgsProvider(){
        Object[][] args = new Object[4][];
        args[0] = new Object[]{true, true};
        args[1] = new Object[]{false, false};
        args[2] = new Object[]{true, false};
        args[3] = new Object[]{false, true};
        return args;
    }

    /**
     * This method executed the following steps of validation:
     *   1. Write some data, verify indexes build correct after write.
     *   2. Replay data that has been written, verify indexes build correct after replay.
     *   3. Verify that position deletion is in sync with {@link PersistentSubscription}.
     * @param writeWithBatch Whether to enable batch feature when writing data.
     * @param readWithBatch Whether to enable batch feature when replay.
     */
    @Test(dataProvider = "mainProcessArgs")
    public void testMainProcess(boolean writeWithBatch, boolean readWithBatch) throws Exception {
        // Write some data.
        TxnLogBufferedWriterConfig configForWrite = new TxnLogBufferedWriterConfig();
        configForWrite.setBatchEnabled(writeWithBatch);
        configForWrite.setBatchedWriteMaxRecords(2);
        // Denied scheduled flush.
        configForWrite.setBatchedWriteMaxDelayInMillis(1000 * 3600);
        MLPendingAckStore mlPendingAckStoreForWrite = createPendingAckStore(configForWrite);
        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        for (int i = 0; i < 20; i++){
            TxnID txnID = new TxnID(i, i);
            Position position = PositionFactory.create(i, i);
            futureList.add(mlPendingAckStoreForWrite.appendCumulativeAck(txnID, position));
        }
        for (int i = 0; i < 10; i++){
            TxnID txnID = new TxnID(i, i);
            futureList.add(mlPendingAckStoreForWrite.appendCommitMark(txnID, CommandAck.AckType.Cumulative));
        }
        for (int i = 10; i < 20; i++){
            TxnID txnID = new TxnID(i, i);
            futureList.add(mlPendingAckStoreForWrite.appendAbortMark(txnID, CommandAck.AckType.Cumulative));
        }
        for (int i = 40; i < 50; i++){
            TxnID txnID = new TxnID(i, i);
            Position position = PositionFactory.create(i, i);
            futureList.add(mlPendingAckStoreForWrite.appendCumulativeAck(txnID, position));
        }
        FutureUtil.waitForAll(futureList).get();
        // Verify build sparse indexes correct after add many cmd-ack.
        ArrayList<Long> positionList = new ArrayList<>();
        for (long i = 0; i < 50; i++){
            positionList.add(i);
        }
        // The indexes not contains the data which is commit or abort.
        LinkedHashSet<Long> skipSet = new LinkedHashSet<>();
        for (long i = 20; i < 40; i++){
            skipSet.add(i);
        }
        if (writeWithBatch) {
            for (long i = 0; i < 50; i++){
                if (i % 2 == 0){
                    // The indexes contains only the last position in the batch.
                    skipSet.add(i);
                }
            }
        }
        LinkedHashSet<Long> expectedPositions = calculatePendingAckIndexes(positionList, skipSet);
        Assert.assertEquals(
                mlPendingAckStoreForWrite.pendingAckLogIndex.keySet().stream()
                        .map(Position::getEntryId).collect(Collectors.toList()),
                new ArrayList<>(expectedPositions)
        );
        // Replay.
        TxnLogBufferedWriterConfig configForReplay = new TxnLogBufferedWriterConfig();
        configForReplay.setBatchEnabled(readWithBatch);
        configForReplay.setBatchedWriteMaxRecords(2);
        // Denied scheduled flush.
        configForReplay.setBatchedWriteMaxDelayInMillis(1000 * 3600);
        MLPendingAckStore mlPendingAckStoreForRead = createPendingAckStore(configForReplay);
        PendingAckHandleImpl pendingAckHandle = mock(PendingAckHandleImpl.class);
        when(pendingAckHandle.getInternalPinnedExecutor()).thenReturn(internalPinnedExecutor);
        when(pendingAckHandle.changeToReadyState()).thenReturn(true);
        // Process controller, mark the replay task already finish.
        final AtomicInteger processController = new AtomicInteger();
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                processController.incrementAndGet();
                return null;
            }
        }).when(pendingAckHandle).completeHandleFuture();
        mlPendingAckStoreForRead.replayAsync(pendingAckHandle, internalPinnedExecutor);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> processController.get() == 1);
        // Verify build sparse indexes correct after replay.
        Assert.assertEquals(mlPendingAckStoreForRead.pendingAckLogIndex.size(),
                mlPendingAckStoreForWrite.pendingAckLogIndex.size());
        Iterator<Map.Entry<Position, Position>> iteratorReplay =
                mlPendingAckStoreForRead.pendingAckLogIndex.entrySet().iterator();
        Iterator<Map.Entry<Position, Position>> iteratorWrite =
                mlPendingAckStoreForWrite.pendingAckLogIndex.entrySet().iterator();
        while (iteratorReplay.hasNext()){
            Map.Entry<Position, Position> replayEntry = iteratorReplay.next();
            Map.Entry<Position, Position> writeEntry =  iteratorWrite.next();
            Assert.assertEquals(replayEntry.getKey(), writeEntry.getKey());
            Assert.assertEquals(replayEntry.getValue().getLedgerId(), writeEntry.getValue().getLedgerId());
            Assert.assertEquals(replayEntry.getValue().getEntryId(), writeEntry.getValue().getEntryId());
        }
        // Verify delete correct.
        when(managedCursorMock.getPersistentMarkDeletedPosition()).thenReturn(PositionFactory.create(19, 19));
        mlPendingAckStoreForWrite.clearUselessLogData();
        mlPendingAckStoreForRead.clearUselessLogData();
        Assert.assertTrue(mlPendingAckStoreForWrite.pendingAckLogIndex.keySet().iterator().next().getEntryId() > 19);
        Assert.assertTrue(mlPendingAckStoreForRead.pendingAckLogIndex.keySet().iterator().next().getEntryId() > 19);

        // cleanup.
        closePendingAckStoreWithRetry(mlPendingAckStoreForWrite);
        closePendingAckStoreWithRetry(mlPendingAckStoreForRead);
    }

    /**
     * Why should retry?
     * Because when the cursor close and cursor switch ledger are concurrent executing, the bad version exception is
     * thrown.
     */
    private void closePendingAckStoreWithRetry(MLPendingAckStore pendingAckStore){
        Awaitility.await().until(() -> {
            try {
                pendingAckStore.closeAsync().get();
                return true;
            } catch (Exception ex){
                return false;
            }
        });
    }

    /**
     * Build a sparse index from the {@param positionList}, the logic same as {@link MLPendingAckStore}.
     * @param positionList the position add to pending ack log/
     * @param skipSet the position which should increment the count but not marked to indexes. aka: commit & abort.
     */
    private LinkedHashSet<Long> calculatePendingAckIndexes(List<Long> positionList, LinkedHashSet<Long> skipSet){
        LogIndexLagBackoff logIndexLagBackoff = new LogIndexLagBackoff(pendingAckLogIndexMinLag, Long.MAX_VALUE, 1);
        long nextCount = logIndexLagBackoff.next(0);
        long recordCountInCurrentLoop = 0;
        LinkedHashSet<Long> indexes = new LinkedHashSet<>();
        for (int i = 0; i < positionList.size(); i++){
            recordCountInCurrentLoop++;
            long value = positionList.get(i);
            if (skipSet.contains(value)){
                continue;
            }
            if (recordCountInCurrentLoop >= nextCount){
                indexes.add(value);
                nextCount = logIndexLagBackoff.next(indexes.size());
                recordCountInCurrentLoop = 0;
            }
        }
        return indexes;
    }
}
