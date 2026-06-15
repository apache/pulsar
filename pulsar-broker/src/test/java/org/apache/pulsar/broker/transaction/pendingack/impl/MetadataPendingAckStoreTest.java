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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.metadata.TxnHeader;
import org.apache.pulsar.broker.transaction.metadata.TxnIds;
import org.apache.pulsar.broker.transaction.metadata.TxnMetadataStore;
import org.apache.pulsar.broker.transaction.metadata.TxnOp;
import org.apache.pulsar.broker.transaction.metadata.TxnOpKind;
import org.apache.pulsar.broker.transaction.metadata.TxnState;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.Stat;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link MetadataPendingAckStore} against the in-memory {@code MetadataStore}.
 * The {@link PersistentSubscription} and {@link PendingAckHandleImpl} collaborators are mocked so
 * the test focuses on the store's state machine, append behaviour, and event-driven reconcile.
 */
public class MetadataPendingAckStoreTest {

    private static final String SEGMENT = "segment://public/default/topic/0000-ffff-0";
    private static final String SUB = "my-sub";

    private MetadataStore store;
    private TxnMetadataStore txnStore;
    private PersistentSubscription subscription;
    private PendingAckHandleImpl handle;

    @BeforeMethod
    public void setUp() throws Exception {
        store = MetadataStoreFactory.create("memory:local",
                MetadataStoreConfig.builder().fsyncEnable(false).build());
        txnStore = new TxnMetadataStore(store);
        subscription = mock(PersistentSubscription.class);
        when(subscription.getTopicName()).thenReturn(SEGMENT);
        when(subscription.getName()).thenReturn(SUB);
        handle = mock(PendingAckHandleImpl.class);
        when(handle.commitTxn(any(), any(), org.mockito.ArgumentMatchers.anyLong()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(handle.abortTxn(any(), any(), org.mockito.ArgumentMatchers.anyLong()))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (store != null) {
            store.close();
        }
    }

    @Test
    public void appendIndividualAck_writesTxnOpsAndTracksOpenTxn() throws Exception {
        MetadataPendingAckStore ackStore = new MetadataPendingAckStore(subscription, txnStore);
        ackStore.replayAsync(handle, null);
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
        ackStore.replayAsync(handle, null);
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
    public void appendCommitMarkAndAbortMark_areNoOps() throws Exception {
        MetadataPendingAckStore ackStore = new MetadataPendingAckStore(subscription, txnStore);
        ackStore.replayAsync(handle, null);
        ackStore.recoveryFuture().get(5, TimeUnit.SECONDS);

        // The TC drives commits/aborts via subscription events; SPI-level marks are intentional
        // no-ops on the metadata-driven store.
        ackStore.appendCommitMark(new TxnID(1, 1), null).get();
        ackStore.appendAbortMark(new TxnID(1, 1), null).get();
    }

    @Test
    public void commitEvent_callsHandleCommitTxn_andDeletesOpRecords() throws Exception {
        MetadataPendingAckStore ackStore = new MetadataPendingAckStore(subscription, txnStore);
        ackStore.replayAsync(handle, null);
        ackStore.recoveryFuture().get(5, TimeUnit.SECONDS);

        TxnID txnId = new TxnID(1, 1);
        createOpenHeader(txnId);
        ackStore.appendIndividualAck(txnId, Collections.singletonList(
                MutablePair.of(PositionFactory.create(3, 5), 1))).get();

        // TC flips header to COMMITTED + publishes the subscription event.
        commitHeader(txnId);
        txnStore.publishSubscriptionEvent(SEGMENT, SUB,
                new org.apache.pulsar.broker.transaction.metadata.TxnEvent(
                        TxnIds.toKey(txnId), TxnState.COMMITTED)).get();

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
        ackStore.replayAsync(handle, null);
        ackStore.recoveryFuture().get(5, TimeUnit.SECONDS);

        TxnID txnId = new TxnID(1, 1);
        createOpenHeader(txnId);
        ackStore.appendIndividualAck(txnId, Collections.singletonList(
                MutablePair.of(PositionFactory.create(3, 5), 1))).get();

        abortHeader(txnId);
        txnStore.publishSubscriptionEvent(SEGMENT, SUB,
                new org.apache.pulsar.broker.transaction.metadata.TxnEvent(
                        TxnIds.toKey(txnId), TxnState.ABORTED)).get();

        Awaitility.await().untilAsserted(() ->
                verify(handle).abortTxn(eq(txnId), eq(null), eq(0L)));

        Awaitility.await().untilAsserted(() -> {
            List<TxnOp> remaining = new ArrayList<>();
            txnStore.listAcksBySegmentSubscription(SEGMENT, SUB, collectOps(remaining)).get();
            assertThat(remaining).isEmpty();
        });
    }

    @Test
    public void recovery_rebuildsOpenTxnStateFromAckRecords() throws Exception {
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
        ackStore.replayAsync(handle, null);
        ackStore.recoveryFuture().get(5, TimeUnit.SECONDS);

        // Terminal txn was discovered during recovery → commitTxn fired and its op record cleaned up.
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

    // ---- helpers ----------------------------------------------------------

    private static org.apache.pulsar.metadata.api.ScanConsumer collectOps(List<TxnOp> out) {
        return new org.apache.pulsar.metadata.api.ScanConsumer() {
            @Override
            public void onNext(org.apache.pulsar.metadata.api.GetResult r) {
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
