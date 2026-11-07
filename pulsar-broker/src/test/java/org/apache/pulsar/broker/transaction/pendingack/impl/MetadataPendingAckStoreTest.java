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

import static org.apache.bookkeeper.mledger.impl.AckSetStateUtil.createPositionWithAckSet;
import static org.apache.bookkeeper.mledger.impl.AckSetStateUtil.getAckSetArrayOrNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.metadata.TxnEvent;
import org.apache.pulsar.broker.transaction.metadata.TxnHeader;
import org.apache.pulsar.broker.transaction.metadata.TxnIds;
import org.apache.pulsar.broker.transaction.metadata.TxnMetadataStore;
import org.apache.pulsar.broker.transaction.metadata.TxnMetadataStore.OpRecord;
import org.apache.pulsar.broker.transaction.metadata.TxnOp;
import org.apache.pulsar.broker.transaction.metadata.TxnOpKind;
import org.apache.pulsar.broker.transaction.metadata.TxnPaths;
import org.apache.pulsar.broker.transaction.metadata.TxnState;
import org.apache.pulsar.broker.transaction.pendingack.TransactionPendingAckStoreProvider;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.ScanConsumer;
import org.apache.pulsar.metadata.api.Stat;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link MetadataPendingAckStore} against the in-memory {@code MetadataStore}.
 * Most tests mock the subscription and handle to focus on the store's state machine and reconcile.
 * Recovery regressions also exercise the real {@link PendingAckHandleImpl} state machine.
 */
public class MetadataPendingAckStoreTest {

    private static final String SEGMENT = "segment://public/default/topic/0000-ffff-0";
    private static final String SUB = "my-sub";

    private MetadataStore store;
    private TxnMetadataStore txnStore;
    private PersistentSubscription subscription;
    private ManagedCursor cursor;
    private PendingAckHandleImpl handle;
    private ExecutorService replayExecutor;
    private boolean legacyEntryBatched;
    private int legacyEntryBatchSize;

    @BeforeMethod
    public void setUp() throws Exception {
        store = MetadataStoreFactory.create("memory:local",
                MetadataStoreConfig.builder().fsyncEnable(false).build());
        txnStore = new TxnMetadataStore(store);
        subscription = mock(PersistentSubscription.class);
        when(subscription.getTopicName()).thenReturn(SEGMENT);
        when(subscription.getName()).thenReturn(SUB);
        cursor = mock(ManagedCursor.class);
        ManagedLedger ledger = mock(ManagedLedger.class);
        when(subscription.getCursor()).thenReturn(cursor);
        when(cursor.getManagedLedger()).thenReturn(ledger);
        when(cursor.getPersistentMarkDeletedPosition()).thenReturn(PositionFactory.create(100, 100));
        legacyEntryBatched = false;
        legacyEntryBatchSize = 4;
        doAnswer(invocation -> {
            Position position = invocation.getArgument(0);
            ReadEntryCallback callback = invocation.getArgument(1);
            callback.readEntryComplete(EntryImpl.create(position.getLedgerId(), position.getEntryId(),
                    legacyEntryBytes()), invocation.getArgument(2));
            return null;
        }).when(ledger).asyncReadEntry(any(), any(), any());
        handle = mock(PendingAckHandleImpl.class);
        when(handle.changeToReadyState()).thenReturn(true);
        replayExecutor = Executors.newSingleThreadExecutor();
        when(handle.commitTxn(any(), any(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(handle.commitRecoveredTxn(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(handle.abortTxn(any(), any(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (replayExecutor != null) {
            replayExecutor.shutdown();
            assertThat(replayExecutor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
        if (store != null) {
            store.close();
        }
    }

    @Test
    public void appendIndividualAck_writesTxnOpsAndTracksOpenTxn() throws Exception {
        MetadataPendingAckStore ackStore = new MetadataPendingAckStore(subscription, txnStore);
        ackStore.replayAsync(handle, replayExecutor);
        ackStore.recoveryFuture().get(5, TimeUnit.SECONDS);

        TxnID txnId = new TxnID(1, 1);
        createOpenHeader(txnId);
        List<MutablePair<Position, Integer>> positions = new ArrayList<>();
        positions.add(MutablePair.of(PositionFactory.create(1, 1), 1));
        positions.add(MutablePair.of(PositionFactory.create(1, 2), 1));
        ackStore.appendIndividualAck(txnId, positions).get();

        // Two ack op records on disk for this (segment, sub, txn).
        List<TxnOp> hits = new ArrayList<>();
        txnStore.listAcksBySegmentSubscription(SEGMENT, SUB,
                collectOps(hits)).get();
        assertThat(hits).hasSize(2);
        assertThat(hits).allMatch(o -> o.getKind() == TxnOpKind.ACK
                && SEGMENT.equals(o.getSegment())
                && SUB.equals(o.getSubscription())
                && !Boolean.TRUE.equals(o.getCumulative()));
    }

    @Test
    public void appendCumulativeAck_writesSingleRecordWithCumulativeTrue() throws Exception {
        MetadataPendingAckStore ackStore = new MetadataPendingAckStore(subscription, txnStore);
        ackStore.replayAsync(handle, replayExecutor);
        ackStore.recoveryFuture().get(5, TimeUnit.SECONDS);

        TxnID txnId = new TxnID(1, 1);
        createOpenHeader(txnId);
        ackStore.appendCumulativeAck(txnId, PositionFactory.create(5, 9)).get();

        List<TxnOp> hits = new ArrayList<>();
        txnStore.listAcksBySegmentSubscription(SEGMENT, SUB, collectOps(hits)).get();
        assertThat(hits).hasSize(1);
        assertThat(hits.get(0).getCumulative()).isTrue();
    }

    @Test
    public void closeAsync_waitsForTheAcksBeingAppended() throws Exception {
        TxnMetadataStore slowStore = spy(txnStore);
        CompletableFuture<Stat> appending = new CompletableFuture<>();
        doReturn(appending).when(slowStore).appendOp(any(), any());
        MetadataPendingAckStore ackStore = new MetadataPendingAckStore(subscription, slowStore);
        ackStore.replayAsync(handle, replayExecutor);
        ackStore.recoveryFuture().get(5, TimeUnit.SECONDS);

        CompletableFuture<Void> ack = ackStore.appendIndividualAck(new TxnID(1, 1), Collections.singletonList(
                MutablePair.of(PositionFactory.create(3, 5), 1)));
        CompletableFuture<Void> closed = ackStore.closeAsync();

        // The ack records of a deleted subscription are deleted once it is closed: none must land after that.
        assertThat(closed).as("close while an ack is being appended").isNotDone();
        assertThatThrownBy(() -> ackStore.appendIndividualAck(new TxnID(1, 2), Collections.singletonList(
                MutablePair.of(PositionFactory.create(3, 6), 1))).get())
                .as("ack after the close")
                .hasCauseInstanceOf(IllegalStateException.class);

        appending.complete(null);
        closed.get(5, TimeUnit.SECONDS);
        ack.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void closeAsync_completesWhenAnAckBeingAppendedFails() throws Exception {
        TxnMetadataStore slowStore = spy(txnStore);
        CompletableFuture<Stat> appending = new CompletableFuture<>();
        doReturn(appending).when(slowStore).appendOp(any(), any());
        MetadataPendingAckStore ackStore = new MetadataPendingAckStore(subscription, slowStore);
        ackStore.replayAsync(handle, replayExecutor);
        ackStore.recoveryFuture().get(5, TimeUnit.SECONDS);

        CompletableFuture<Void> ack = ackStore.appendIndividualAck(new TxnID(1, 1), Collections.singletonList(
                MutablePair.of(PositionFactory.create(3, 5), 1)));
        CompletableFuture<Void> closed = ackStore.closeAsync();
        appending.completeExceptionally(new MetadataStoreException("append failed"));

        closed.get(5, TimeUnit.SECONDS);
        assertThatThrownBy(() -> ack.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(MetadataStoreException.class);
    }

    @Test
    public void appendCommitMarkAndAbortMark_areNoOps() throws Exception {
        MetadataPendingAckStore ackStore = new MetadataPendingAckStore(subscription, txnStore);
        ackStore.replayAsync(handle, replayExecutor);
        ackStore.recoveryFuture().get(5, TimeUnit.SECONDS);

        // The TC drives commits/aborts via subscription events; SPI-level marks are intentional
        // no-ops on the metadata-driven store.
        ackStore.appendCommitMark(new TxnID(1, 1), null).get();
        ackStore.appendAbortMark(new TxnID(1, 1), null).get();
    }

    @Test
    public void commitEvent_callsHandleCommitTxn_andDeletesOpRecords() throws Exception {
        MetadataPendingAckStore ackStore = new MetadataPendingAckStore(subscription, txnStore);
        ackStore.replayAsync(handle, replayExecutor);
        ackStore.recoveryFuture().get(5, TimeUnit.SECONDS);

        TxnID txnId = new TxnID(1, 1);
        createOpenHeader(txnId);
        ackStore.appendIndividualAck(txnId, Collections.singletonList(
                MutablePair.of(PositionFactory.create(3, 5), 1))).get();

        // TC flips header to COMMITTED + publishes the subscription event.
        commitHeader(txnId);
        txnStore.publishSubscriptionEvent(SEGMENT, SUB,
                new TxnEvent(TxnIds.toKey(txnId), TxnState.COMMITTED)).get();

        Awaitility.await().untilAsserted(() ->
                verify(handle).commitTxn(eq(txnId), eq(Map.of()), eq(0L)));

        // Op records cleaned up.
        Awaitility.await().untilAsserted(() -> {
            List<TxnOp> remaining = new ArrayList<>();
            txnStore.listAcksBySegmentSubscription(SEGMENT, SUB, collectOps(remaining)).get();
            assertThat(remaining).isEmpty();
        });
    }

    @Test
    public void abortEvent_callsHandleAbortTxn_andDeletesOpRecords() throws Exception {
        MetadataPendingAckStore ackStore = new MetadataPendingAckStore(subscription, txnStore);
        ackStore.replayAsync(handle, replayExecutor);
        ackStore.recoveryFuture().get(5, TimeUnit.SECONDS);

        TxnID txnId = new TxnID(1, 1);
        createOpenHeader(txnId);
        ackStore.appendIndividualAck(txnId, Collections.singletonList(
                MutablePair.of(PositionFactory.create(3, 5), 1))).get();

        abortHeader(txnId);
        txnStore.publishSubscriptionEvent(SEGMENT, SUB,
                new TxnEvent(
                        TxnIds.toKey(txnId), TxnState.ABORTED)).get();

        Awaitility.await().untilAsserted(() ->
                verify(handle).abortTxn(eq(txnId), eq(null), eq(0L)));

        Awaitility.await().untilAsserted(() -> {
            List<TxnOp> remaining = new ArrayList<>();
            txnStore.listAcksBySegmentSubscription(SEGMENT, SUB, collectOps(remaining)).get();
            assertThat(remaining).isEmpty();
        });
    }

    @DataProvider
    public Object[][] legacySingleMessageEntryFormats() {
        return new Object[][]{{false}, {true}};
    }

    @Test(dataProvider = "legacySingleMessageEntryFormats")
    public void recovery_rebuildsOpenTxnStateFromAckRecords(boolean explicitSingleMessageBatch) throws Exception {
        legacyEntryBatched = explicitSingleMessageBatch;
        legacyEntryBatchSize = 1;
        // Pre-populate: an OPEN txn with two acks on this (seg, sub), and a terminal txn with a
        // lingering ack that the store should drive to cleanup during recovery.
        TxnID openTxn = new TxnID(1, 1);
        TxnID committedTxn = new TxnID(1, 2);

        createOpenHeader(openTxn);
        txnStore.appendOp(TxnIds.toKey(openTxn),
                new TxnOp(TxnOpKind.ACK, SEGMENT, SUB, 7L, 1L, false)).get();
        txnStore.appendOp(TxnIds.toKey(openTxn),
                new TxnOp(TxnOpKind.ACK, SEGMENT, SUB, 7L, 2L, false)).get();

        txnStore.createHeader(TxnIds.toKey(committedTxn),
                new TxnHeader(TxnState.COMMITTED, Duration.ofMillis(5000),
                        Instant.ofEpochMilli(1000), Instant.ofEpochMilli(2000), null)).get();
        txnStore.appendOp(TxnIds.toKey(committedTxn),
                new TxnOp(TxnOpKind.ACK, SEGMENT, SUB, 7L, 3L, false)).get();

        MetadataPendingAckStore ackStore = new MetadataPendingAckStore(subscription, txnStore);
        ackStore.replayAsync(handle, replayExecutor);
        ackStore.recoveryFuture().get(5, TimeUnit.SECONDS);

        // Terminal txn was discovered during recovery → commitTxn fired and its op record cleaned up.
        verify(handle, times(2)).handleIndividualAckRecover(eq(openTxn), any());
        verify(handle).handleIndividualAckRecover(eq(committedTxn), any());
        var order = inOrder(handle);
        order.verify(handle).commitRecoveredTxn(eq(committedTxn));
        order.verify(handle).changeToReadyState();
        Awaitility.await().untilAsserted(() ->
                verify(handle).commitTxn(eq(committedTxn), eq(Map.of()), eq(0L)));
        Awaitility.await().untilAsserted(() -> {
            List<TxnOp> remaining = new ArrayList<>();
            txnStore.listAcksBySegmentSubscription(SEGMENT, SUB, collectOps(remaining)).get();
            // Only the open txn's two ack records remain.
            assertThat(remaining).hasSize(2);
            assertThat(remaining).allMatch(o ->
                    SEGMENT.equals(o.getSegment()) && SUB.equals(o.getSubscription()));
        });
    }

    @Test
    public void recovery_legacyBatchedAckFailsClosedAndRetainsItsOperation() throws Exception {
        legacyEntryBatched = true;
        TxnID txnId = new TxnID(1, 1);
        createOpenHeader(txnId);
        commitHeader(txnId);
        txnStore.appendOp(TxnIds.toKey(txnId),
                new TxnOp(TxnOpKind.ACK, SEGMENT, SUB, 7L, 1L, false)).get();
        MetadataPendingAckStore ackStore = new MetadataPendingAckStore(subscription, txnStore);
        ackStore.replayAsync(handle, replayExecutor);

        assertThatThrownBy(() -> ackStore.recoveryFuture().get(5, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(MetadataStoreException.class)
                .hasStackTraceContaining("Legacy transactional ACK lacks a batch acknowledgement mask");
        List<TxnOp> remaining = new ArrayList<>();
        txnStore.listAcksBySegmentSubscription(SEGMENT, SUB, collectOps(remaining)).get();
        assertThat(remaining).hasSize(1);
        assertThat(txnStore.getHeader(TxnIds.toKey(txnId)).get()).isPresent();
        verify(handle, never()).changeToReadyState();
        verify(handle, never()).commitTxn(any(), any(), anyLong());
    }

    @Test
    public void recovery_batchAckRestoresItsActualMaskBeforeReadiness() throws Exception {
        TxnID txnId = new TxnID(1, 1);
        createOpenHeader(txnId);
        MetadataPendingAckStore writer = new MetadataPendingAckStore(subscription, txnStore);
        writer.replayAsync(handle, replayExecutor);
        writer.recoveryFuture().get(5, TimeUnit.SECONDS);
        writer.appendIndividualAck(txnId, Collections.singletonList(MutablePair.of(
                createPositionWithAckSet(7, 1, new long[]{14L}), 4))).get();
        writer.closeAsync().get();
        List<TxnOp> persisted = new ArrayList<>();
        txnStore.listAcksBySegmentSubscription(SEGMENT, SUB, collectOps(persisted)).get();
        assertThat(persisted).hasSize(1);
        assertThat(persisted.get(0).getBatchSize()).isEqualTo(4);
        assertThat(persisted.get(0).getAckSet()).containsExactly(14L);

        MetadataPendingAckStore recovered = new MetadataPendingAckStore(subscription, txnStore);
        recovered.replayAsync(handle, replayExecutor);
        recovered.recoveryFuture().get(5, TimeUnit.SECONDS);
        ArgumentCaptor<List<MutablePair<Position, Integer>>> positions = ArgumentCaptor.forClass(List.class);
        var ordered = inOrder(handle);
        // The writer's initial empty replay also transitioned to Ready.
        ordered.verify(handle).changeToReadyState();
        ordered.verify(handle).handleIndividualAckRecover(eq(txnId), positions.capture());
        ordered.verify(handle).changeToReadyState();
        assertThat(positions.getValue()).hasSize(1);
        assertThat(positions.getValue().get(0).getRight()).isEqualTo(4);
        assertThat(getAckSetArrayOrNull(positions.getValue().get(0).getLeft())).containsExactly(14L);
    }

    @Test
    public void recovery_appliesCommittedCumulativeAcksBeforeRestoringOpenTransaction() throws Exception {
        TxnID firstCommitted = new TxnID(1, 1);
        TxnID secondCommitted = new TxnID(1, 2);
        TxnID openTxn = new TxnID(1, 3);
        for (TxnID txnId : List.of(firstCommitted, secondCommitted, openTxn)) {
            createOpenHeader(txnId);
        }
        commitHeader(firstCommitted);
        commitHeader(secondCommitted);
        Map<TxnID, Long> entries = Map.of(firstCommitted, 5L, secondCommitted, 10L, openTxn, 12L);
        for (Map.Entry<TxnID, Long> entry : entries.entrySet()) {
            TxnOp op = new TxnOp(TxnOpKind.ACK, SEGMENT, SUB, 7L, entry.getValue(), true);
            op.setBatchSize(1);
            txnStore.appendOp(TxnIds.toKey(entry.getKey()), op).get();
        }

        // The cursor image lost the volatile ACKs of both committed transactions. Keep its
        // persistent prefix behind them even while replay applies ACKs to the live cursor.
        AtomicReference<Position> markDeleted = new AtomicReference<>(PositionFactory.create(7, 0));
        when(cursor.getMarkDeletedPosition()).thenAnswer(invocation -> markDeleted.get());
        when(cursor.getPersistentMarkDeletedPosition()).thenReturn(PositionFactory.create(7, 0));
        PersistentSubscription recoveredSubscription = mock(PersistentSubscription.class, RETURNS_DEEP_STUBS);
        when(recoveredSubscription.getTopicName()).thenReturn(SEGMENT);
        when(recoveredSubscription.getName()).thenReturn(SUB);
        when(recoveredSubscription.getCursor()).thenReturn(cursor);
        CompletableFuture<Void> firstCursorAck = new CompletableFuture<>();
        CompletableFuture<Void> cursorAckStarted = new CompletableFuture<>();
        AtomicInteger cursorAcks = new AtomicInteger();
        when(recoveredSubscription.acknowledgeMessageAsync(anyList(), eq(AckType.Cumulative), any()))
                .thenAnswer(invocation -> {
                    List<Position> positions = invocation.getArgument(0);
                    Position position = positions.get(0);
                    CompletableFuture<Void> persisted = cursorAcks.getAndIncrement() == 0
                            ? firstCursorAck : CompletableFuture.completedFuture(null);
                    cursorAckStarted.complete(null);
                    return persisted.thenRun(() -> markDeleted.updateAndGet(current ->
                            position.compareTo(current) > 0 ? position : current));
                });
        var pulsar = recoveredSubscription.getTopic().getBrokerService().getPulsar();
        when(pulsar.getTransactionExecutorProvider().getExecutor(any(PendingAckHandleImpl.class)))
                .thenReturn(replayExecutor);
        TransactionPendingAckStoreProvider provider = mock(TransactionPendingAckStoreProvider.class);
        when(pulsar.getTransactionPendingAckStoreProvider()).thenReturn(provider);
        when(provider.checkInitializedBefore(recoveredSubscription))
                .thenReturn(CompletableFuture.completedFuture(true));
        MetadataPendingAckStore recovered = new MetadataPendingAckStore(recoveredSubscription, txnStore);
        when(provider.newPendingAckStore(recoveredSubscription))
                .thenReturn(CompletableFuture.completedFuture(recovered));

        PendingAckHandleImpl recoveredHandle = new PendingAckHandleImpl(recoveredSubscription);
        try {
            cursorAckStarted.get(5, TimeUnit.SECONDS);
            assertThat(recoveredHandle.pendingAckHandleFuture()).as("cursor ACK is still outstanding").isNotDone();
            assertThat(recovered.recoveryFuture()).isNotDone();
            assertThat(recoveredHandle.getTransactionInPendingAckStats(openTxn).cumulativeAckPosition).isNull();
            firstCursorAck.complete(null);
            recoveredHandle.pendingAckHandleFuture().get(5, TimeUnit.SECONDS);
            recovered.recoveryFuture().get(5, TimeUnit.SECONDS);
            assertThat(markDeleted.get()).as("both committed cumulative ACKs must be applied")
                    .isEqualTo(PositionFactory.create(7, 10));
            assertThat(recoveredHandle.getTransactionInPendingAckStats(openTxn).cumulativeAckPosition)
                    .as("the later OPEN transaction must remain pending").isEqualTo("7:12");
        } finally {
            firstCursorAck.complete(null);
            recoveredHandle.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @DataProvider
    public Object[][] uncoveredPersistedPrefixes() {
        return new Object[][]{{null}, {PositionFactory.create(3, 4)}};
    }

    @Test(dataProvider = "uncoveredPersistedPrefixes")
    public void committedPartialAckIsRetainedUntilDurableWholeEntryCoverage(Position persisted) throws Exception {
        when(cursor.getPersistentMarkDeletedPosition()).thenReturn(persisted);
        // A checkpoint may truncate individual ranges or batch masks; only the whole-entry prefix is trusted.
        when(cursor.isCursorDataFullyPersistable()).thenReturn(false);
        MetadataPendingAckStore ackStore = new MetadataPendingAckStore(subscription, txnStore);
        ackStore.replayAsync(handle, replayExecutor);
        ackStore.recoveryFuture().get(5, TimeUnit.SECONDS);
        TxnID txnId = new TxnID(1, 1);
        createOpenHeader(txnId);
        ackStore.appendIndividualAck(txnId, Collections.singletonList(MutablePair.of(
                createPositionWithAckSet(3, 5, new long[]{14L}), 4))).get();
        commitHeader(txnId);
        publishDecision(txnId);
        Awaitility.await().untilAsserted(() -> verify(handle).commitTxn(eq(txnId), eq(Map.of()), eq(0L)));
        List<TxnOp> remaining = new ArrayList<>();
        txnStore.listAcksBySegmentSubscription(SEGMENT, SUB, collectOps(remaining)).get();
        assertThat(remaining).hasSize(1);
        assertThat(remaining.get(0).getAckSet()).containsExactly(14L);
        assertThat(txnStore.getHeader(TxnIds.toKey(txnId)).get()).isPresent();

        when(cursor.getPersistentMarkDeletedPosition()).thenReturn(PositionFactory.create(3, 5));
        Awaitility.await().untilAsserted(() -> {
            publishDecision(txnId);
            List<TxnOp> outstanding = new ArrayList<>();
            txnStore.listAcksBySegmentSubscription(SEGMENT, SUB, collectOps(outstanding)).get();
            assertThat(outstanding).as("durable entry coverage permits cleanup and subsequent header GC").isEmpty();
        });
    }

    @Test
    public void committedAckCleanupFailureRetainsOpsAndRetries() throws Exception {
        TxnMetadataStore failingCleanup = spy(txnStore);
        AtomicInteger cleanups = new AtomicInteger();
        doAnswer(invocation -> {
            if (cleanups.getAndIncrement() == 0) {
                return CompletableFuture.failedFuture(new MetadataStoreException("injected cleanup failure"));
            }
            return invocation.callRealMethod();
        }).when(failingCleanup).deleteOps(any());
        MetadataPendingAckStore ackStore = new MetadataPendingAckStore(subscription, failingCleanup);
        ackStore.replayAsync(handle, replayExecutor);
        ackStore.recoveryFuture().get(5, TimeUnit.SECONDS);
        TxnID txnId = new TxnID(1, 1);
        createOpenHeader(txnId);
        ackStore.appendIndividualAck(txnId, Collections.singletonList(
                MutablePair.of(PositionFactory.create(3, 5), 1))).get();
        commitHeader(txnId);
        publishDecision(txnId);
        Awaitility.await().until(() -> cleanups.get() == 1);
        List<TxnOp> retained = new ArrayList<>();
        txnStore.listAcksBySegmentSubscription(SEGMENT, SUB, collectOps(retained)).get();
        assertThat(retained).hasSize(1);
        Awaitility.await().untilAsserted(() -> {
            publishDecision(txnId);
            List<TxnOp> remaining = new ArrayList<>();
            txnStore.listAcksBySegmentSubscription(SEGMENT, SUB, collectOps(remaining)).get();
            assertThat(remaining).isEmpty();
        });
    }

    @Test
    public void committedAckCleanupDoesNotDeleteAReplacedOperationVersion() throws Exception {
        TxnMetadataStore racingCleanup = spy(txnStore);
        AtomicInteger cleanups = new AtomicInteger();
        doAnswer(invocation -> {
            List<OpRecord> captured = invocation.getArgument(0);
            if (cleanups.getAndIncrement() == 0) {
                OpRecord op = captured.get(0);
                Set<Option> options = Set.of(new Option.PartitionKey(op.txnId()),
                        new Option.SecondaryIndex(TxnPaths.IDX_OPS_BY_TXN, op.txnId()),
                        new Option.SecondaryIndex(TxnPaths.IDX_ACKS_BY_SEGMENT_SUBSCRIPTION,
                                TxnPaths.ackIndexKey(SEGMENT, SUB)));
                return store.get(op.path(), Set.of(new Option.PartitionKey(op.txnId())))
                        .thenCompose(value -> store.put(op.path(),
                        value.orElseThrow().getValue(), Optional.of(op.version()), options))
                        .thenCompose(ignored -> txnStore.deleteOps(captured));
            }
            return txnStore.deleteOps(captured);
        }).when(racingCleanup).deleteOps(any());
        MetadataPendingAckStore ackStore = new MetadataPendingAckStore(subscription, racingCleanup);
        ackStore.replayAsync(handle, replayExecutor);
        ackStore.recoveryFuture().get(5, TimeUnit.SECONDS);
        TxnID txnId = new TxnID(1, 1);
        createOpenHeader(txnId);
        ackStore.appendIndividualAck(txnId, Collections.singletonList(
                MutablePair.of(PositionFactory.create(3, 5), 1))).get();
        commitHeader(txnId);
        publishDecision(txnId);
        Awaitility.await().until(() -> cleanups.get() == 1);
        List<TxnOp> retained = new ArrayList<>();
        txnStore.listAcksBySegmentSubscription(SEGMENT, SUB, collectOps(retained)).get();
        assertThat(retained).hasSize(1);
        Awaitility.await().untilAsserted(() -> {
            publishDecision(txnId);
            List<TxnOp> remaining = new ArrayList<>();
            txnStore.listAcksBySegmentSubscription(SEGMENT, SUB, collectOps(remaining)).get();
            assertThat(remaining).isEmpty();
        });
    }

    @Test
    public void beforeCursorResetDropsTerminalRecordsAndKeepsOpenOnes() throws Exception {
        MetadataPendingAckStore ackStore = new MetadataPendingAckStore(subscription, txnStore);
        ackStore.replayAsync(handle, replayExecutor);
        ackStore.recoveryFuture().get(5, TimeUnit.SECONDS);
        TxnID committed = new TxnID(1, 1);
        createOpenHeader(committed);
        // Above the persisted mark-delete (100:100), so the record is retained after the commit is applied.
        ackStore.appendIndividualAck(committed, Collections.singletonList(
                MutablePair.of(PositionFactory.create(200, 1), 1))).get();
        commitHeader(committed);
        publishDecision(committed);
        Awaitility.await().untilAsserted(() -> verify(handle).commitTxn(eq(committed), eq(Map.of()), eq(0L)));
        TxnID open = new TxnID(1, 2);
        createOpenHeader(open);
        ackStore.appendIndividualAck(open, Collections.singletonList(
                MutablePair.of(PositionFactory.create(200, 2), 1))).get();
        List<TxnOp> before = new ArrayList<>();
        txnStore.listAcksBySegmentSubscription(SEGMENT, SUB, collectOps(before)).get();
        assertThat(before).hasSize(2);

        ackStore.beforeCursorReset().get(5, TimeUnit.SECONDS);

        List<TxnOp> after = new ArrayList<>();
        txnStore.listAcksBySegmentSubscription(SEGMENT, SUB, collectOps(after)).get();
        assertThat(after).as("only the OPEN transaction's ACK survives the reset").singleElement()
                .satisfies(op -> assertThat(op.getEntryId()).isEqualTo(2L));
    }

    @Test
    public void failedCursorResetRestoresRecordsDroppedPartWay() throws Exception {
        TxnMetadataStore partialDelete = spy(txnStore);
        doAnswer(invocation -> {
            List<OpRecord> ops = invocation.getArgument(0);
            if (ops.size() < 2) {
                return invocation.callRealMethod();
            }
            // Delete one of the records, then fail: the reset is rejected part way through its preparation.
            return txnStore.deleteOps(ops.subList(0, 1)).thenCompose(__ -> CompletableFuture.<Void>failedFuture(
                    new MetadataStoreException("injected partial deletion failure")));
        }).when(partialDelete).deleteOps(any());
        MetadataPendingAckStore ackStore = new MetadataPendingAckStore(subscription, partialDelete);
        TxnID txnId = commitRetainedAcks(ackStore, PositionFactory.create(200, 1), PositionFactory.create(200, 2));

        assertThatThrownBy(() -> ackStore.beforeCursorReset().get(5, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(MetadataStoreException.class);
        assertThat(ackOps()).as("the preparation deleted one record before it failed").hasSize(1);
        ackStore.afterCursorReset(false).get(5, TimeUnit.SECONDS);
        assertThat(ackOps()).as("the failed reset recreates the dropped record").hasSize(2);

        assertRecoveryReappliesCommittedAcks(txnId, 2);
    }

    @Test
    public void unpersistedCursorResetRestoresDroppedRecords() throws Exception {
        MetadataPendingAckStore ackStore = new MetadataPendingAckStore(subscription, txnStore);
        TxnID txnId = commitRetainedAcks(ackStore, PositionFactory.create(200, 1));

        ackStore.beforeCursorReset().get(5, TimeUnit.SECONDS);
        assertThat(ackOps()).isEmpty();
        ackStore.afterCursorReset(false).get(5, TimeUnit.SECONDS);
        assertThat(ackOps()).as("the reset was not persisted, so its ACK still needs the record").hasSize(1);

        assertRecoveryReappliesCommittedAcks(txnId, 1);
    }

    @Test
    public void persistedCursorResetDiscardsDroppedRecords() throws Exception {
        MetadataPendingAckStore ackStore = new MetadataPendingAckStore(subscription, txnStore);
        commitRetainedAcks(ackStore, PositionFactory.create(200, 1));

        ackStore.beforeCursorReset().get(5, TimeUnit.SECONDS);
        ackStore.afterCursorReset(true).get(5, TimeUnit.SECONDS);
        ackStore.afterCursorReset(false).get(5, TimeUnit.SECONDS);
        assertThat(ackOps()).as("a persisted reset leaves nothing to restore").isEmpty();
    }

    /** Commits ACKs above the persisted mark-delete (100:100), so their records are retained after the commit. */
    private TxnID commitRetainedAcks(MetadataPendingAckStore ackStore, Position... positions) throws Exception {
        ackStore.replayAsync(handle, replayExecutor);
        ackStore.recoveryFuture().get(5, TimeUnit.SECONDS);
        TxnID txnId = new TxnID(1, 1);
        createOpenHeader(txnId);
        List<MutablePair<Position, Integer>> acks = new ArrayList<>();
        for (Position position : positions) {
            acks.add(MutablePair.of(position, 1));
        }
        ackStore.appendIndividualAck(txnId, acks).get();
        commitHeader(txnId);
        publishDecision(txnId);
        Awaitility.await().untilAsserted(() -> verify(handle).commitTxn(eq(txnId), eq(Map.of()), eq(0L)));
        assertThat(ackOps()).hasSize(positions.length);
        return txnId;
    }

    /** A restart: a fresh store and handle recover the committed ACKs from the metadata records alone. */
    private void assertRecoveryReappliesCommittedAcks(TxnID txnId, int acks) throws Exception {
        PendingAckHandleImpl recovering = mock(PendingAckHandleImpl.class);
        when(recovering.changeToReadyState()).thenReturn(true);
        when(recovering.commitRecoveredTxn(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(recovering.commitTxn(any(), any(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));
        MetadataPendingAckStore recovered = new MetadataPendingAckStore(subscription, txnStore);
        recovered.replayAsync(recovering, replayExecutor);
        recovered.recoveryFuture().get(5, TimeUnit.SECONDS);
        verify(recovering, times(acks)).handleIndividualAckRecover(eq(txnId), anyList());
        verify(recovering).commitRecoveredTxn(txnId);
    }

    private List<TxnOp> ackOps() throws Exception {
        List<TxnOp> ops = new ArrayList<>();
        txnStore.listAcksBySegmentSubscription(SEGMENT, SUB, collectOps(ops)).get();
        return ops;
    }

    @Test
    public void closeReleasesACursorResetWaitingForRecovery() throws Exception {
        MetadataPendingAckStore ackStore = new MetadataPendingAckStore(subscription, txnStore);
        CompletableFuture<Void> reset = ackStore.beforeCursorReset();
        assertThat(reset).as("a reset waits for recovery").isNotDone();
        ackStore.closeAsync().get(5, TimeUnit.SECONDS);
        assertThatThrownBy(() -> reset.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    private void publishDecision(TxnID txnId) throws Exception {
        txnStore.publishSubscriptionEvent(SEGMENT, SUB,
                new TxnEvent(TxnIds.toKey(txnId), TxnState.COMMITTED)).get();
    }

    // ---- helpers ----------------------------------------------------------

    private byte[] legacyEntryBytes() {
        MessageMetadata metadata = new MessageMetadata().setProducerName("legacy-ack")
                .setSequenceId(1).setPublishTime(1);
        if (legacyEntryBatched) {
            metadata.setNumMessagesInBatch(legacyEntryBatchSize);
        }
        ByteBuf payload = Unpooled.buffer(1).writeByte(0);
        ByteBuf entry = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, payload);
        try {
            byte[] bytes = new byte[entry.readableBytes()];
            entry.getBytes(entry.readerIndex(), bytes);
            return bytes;
        } finally {
            entry.release();
            payload.release();
        }
    }

    private static ScanConsumer collectOps(List<TxnOp> out) {
        return new ScanConsumer() {
            @Override
            public void onNext(GetResult r) {
                out.add(TxnMetadataStore.fromJson(r.getValue(), TxnOp.class));
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onCompleted() {
            }
        };
    }

    private void createOpenHeader(TxnID txnId) throws Exception {
        txnStore.createHeader(TxnIds.toKey(txnId),
                new TxnHeader(TxnState.OPEN, Duration.ofMillis(60_000),
                        Instant.ofEpochMilli(1000), null, null)).get();
    }

    private void commitHeader(TxnID txnId) throws Exception {
        Stat created;
        var v = txnStore.getHeader(TxnIds.toKey(txnId)).get().orElseThrow();
        var h = v.value();
        created = txnStore.updateHeader(TxnIds.toKey(txnId),
                new TxnHeader(TxnState.COMMITTED, h.getTimeout(), h.getCreatedAt(), Instant.now(), null),
                v.version()).get();
        assertThat(created.getVersion()).isPositive();
    }

    private void abortHeader(TxnID txnId) throws Exception {
        var v = txnStore.getHeader(TxnIds.toKey(txnId)).get().orElseThrow();
        var h = v.value();
        txnStore.updateHeader(TxnIds.toKey(txnId),
                new TxnHeader(TxnState.ABORTED, h.getTimeout(), h.getCreatedAt(), Instant.now(), null),
                v.version()).get();
    }
}
