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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.metadata.AbortedTxnRecord;
import org.apache.pulsar.broker.transaction.metadata.TxnEvent;
import org.apache.pulsar.broker.transaction.metadata.TxnHeader;
import org.apache.pulsar.broker.transaction.metadata.TxnIds;
import org.apache.pulsar.broker.transaction.metadata.TxnMetadataStore;
import org.apache.pulsar.broker.transaction.metadata.TxnOp;
import org.apache.pulsar.broker.transaction.metadata.TxnOpKind;
import org.apache.pulsar.broker.transaction.metadata.TxnPaths;
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
 * Unit tests for {@link MetadataTransactionBuffer} against the in-memory {@code MetadataStore}.
 * The TB's collaborators ({@link PersistentTopic}, {@link ManagedLedger}) are mocked so the test
 * focuses on the buffer's state machine and metadata interactions.
 */
public class MetadataTransactionBufferTest {

    private static final String SEGMENT = "segment://public/default/topic/0000-ffff-0";

    private MetadataStore store;
    private TxnMetadataStore txnStore;
    private ManagedLedger ledger;
    private PersistentTopic topic;
    private AtomicLong nextEntryId;

    @BeforeMethod
    public void setUp() throws Exception {
        store = MetadataStoreFactory.create("memory:local",
                MetadataStoreConfig.builder().fsyncEnable(false).build());
        txnStore = new TxnMetadataStore(store);
        nextEntryId = new AtomicLong(1);
        ledger = mockManagedLedger();
        topic = mockTopic(SEGMENT, ledger);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (store != null) {
            store.close();
        }
    }

    @Test
    public void openTxnAppend_pinsMaxReadPosition() throws Exception {
        TxnID txnId = new TxnID(1, 1);
        createOpenHeader(txnId);
        MetadataTransactionBuffer tb = new MetadataTransactionBuffer(topic, txnStore);
        tb.checkIfTBRecoverCompletely().get();

        Position firstPos = tb.appendBufferToTxn(txnId, 0, payload("a")).get();

        // maxReadPosition should be just before the first txn entry.
        assertThat(tb.getMaxReadPosition()).isEqualTo(ledger.getPreviousPosition(firstPos));
        assertThat(tb.isTxnAborted(txnId, firstPos)).isFalse();
        assertThat(tb.getOngoingTxnCount()).isOne();
    }

    @Test
    public void concurrentOpenTxns_minPositionPins() throws Exception {
        TxnID t1 = new TxnID(1, 1);
        TxnID t2 = new TxnID(1, 2);
        createOpenHeader(t1);
        createOpenHeader(t2);
        MetadataTransactionBuffer tb = new MetadataTransactionBuffer(topic, txnStore);
        tb.checkIfTBRecoverCompletely().get();

        Position p1 = tb.appendBufferToTxn(t1, 0, payload("a")).get();
        tb.appendBufferToTxn(t2, 0, payload("b")).get();

        assertThat(tb.getMaxReadPosition()).isEqualTo(ledger.getPreviousPosition(p1));
    }

    @Test
    public void commitEvent_dropsTxn_advancesMaxReadPosition() throws Exception {
        TxnID txnId = new TxnID(1, 1);
        createOpenHeader(txnId);
        MetadataTransactionBuffer tb = new MetadataTransactionBuffer(topic, txnStore);
        tb.checkIfTBRecoverCompletely().get();
        tb.appendBufferToTxn(txnId, 0, payload("a")).get();

        // TC-side: flip header to COMMITTED + publish segment event.
        commitTxn(txnId);
        txnStore.publishSegmentEvent(SEGMENT, new TxnEvent(TxnIds.toKey(txnId), TxnState.COMMITTED)).get();

        Awaitility.await().untilAsserted(() -> {
            assertThat(tb.getOngoingTxnCount()).isZero();
            assertThat(tb.getCommittedTxnCount()).isOne();
        });

        // No OPEN txns pin the read position — should now sit at LAC.
        assertThat(tb.getMaxReadPosition()).isEqualTo(ledger.getLastConfirmedEntry());
    }

    @Test
    public void abortEvent_marksAborted_isTxnAbortedTrue() throws Exception {
        TxnID txnId = new TxnID(1, 1);
        createOpenHeader(txnId);
        MetadataTransactionBuffer tb = new MetadataTransactionBuffer(topic, txnStore);
        tb.checkIfTBRecoverCompletely().get();
        Position firstPos = tb.appendBufferToTxn(txnId, 0, payload("a")).get();

        abortTxn(txnId);
        txnStore.publishSegmentEvent(SEGMENT, new TxnEvent(TxnIds.toKey(txnId), TxnState.ABORTED)).get();

        Awaitility.await().untilAsserted(() -> {
            assertThat(tb.isTxnAborted(txnId, firstPos)).isTrue();
            assertThat(tb.getAbortedTxnCount()).isOne();
            assertThat(tb.getOngoingTxnCount()).isZero();
        });
    }

    @Test
    public void terminalTxns_prunedFromCache_visibilityUnchanged() throws Exception {
        // Resolve many transactions (mixed commit/abort) and confirm the in-memory per-txn cache is
        // pruned back to empty rather than growing for the segment's lifetime, while visibility stays
        // correct: aborted txns remain filtered (via the durable aborted set) and committed/unknown
        // txns remain visible.
        MetadataTransactionBuffer tb = new MetadataTransactionBuffer(topic, txnStore);
        tb.checkIfTBRecoverCompletely().get();

        int n = 20;
        TxnID lastAborted = null;
        Position lastAbortedPos = null;
        TxnID lastCommitted = null;
        for (int i = 1; i <= n; i++) {
            TxnID txnId = new TxnID(1, i);
            createOpenHeader(txnId);
            Position p = tb.appendBufferToTxn(txnId, 0, payload("v" + i)).get();
            if (i % 2 == 0) {
                commitTxn(txnId);
                txnStore.publishSegmentEvent(SEGMENT, new TxnEvent(TxnIds.toKey(txnId), TxnState.COMMITTED)).get();
                lastCommitted = txnId;
            } else {
                abortTxn(txnId);
                txnStore.publishSegmentEvent(SEGMENT, new TxnEvent(TxnIds.toKey(txnId), TxnState.ABORTED)).get();
                lastAborted = txnId;
                lastAbortedPos = p;
            }
        }

        // Once every txn is terminal, the cache holds nothing (no OPEN txns remain).
        Awaitility.await().untilAsserted(() -> {
            assertThat(tb.getOngoingTxnCount()).isZero();
            assertThat(tb.trackedTxnCount()).isZero();
        });

        // Visibility correctness survives pruning.
        assertThat(tb.isTxnAborted(lastAborted, lastAbortedPos)).isTrue();
        assertThat(tb.isTxnAborted(lastCommitted, PositionFactory.create(1, 0))).isFalse();
        assertThat(tb.getCommittedTxnCount()).isEqualTo(n / 2);
        assertThat(tb.getAbortedTxnCount()).isEqualTo(n / 2);
    }

    @Test
    public void appendToCommittedTxn_failsTxnConflict() throws Exception {
        TxnID txnId = new TxnID(1, 1);
        // Pre-set header to COMMITTED — txn is terminal before any append.
        Stat created = txnStore.createHeader(TxnIds.toKey(txnId),
                new TxnHeader(TxnState.COMMITTED, Duration.ofMillis(5000),
                        Instant.ofEpochMilli(1000), Instant.ofEpochMilli(2000), null)).get();
        assertThat(created.getVersion()).isZero();

        MetadataTransactionBuffer tb = new MetadataTransactionBuffer(topic, txnStore);
        tb.checkIfTBRecoverCompletely().get();

        assertThatThrownBy(() -> tb.appendBufferToTxn(txnId, 0, payload("a")).get())
                .hasCauseInstanceOf(BrokerServiceException.NotAllowedException.class);
    }

    @Test
    public void unknownTxn_isTxnAbortedReturnsFalse() throws Exception {
        // P3.5 semantics: "below watermark + not in aborted set → visible". Unknown txns default
        // visible — orphans are eliminated by the publish-path ordering reversal (op-record
        // before ML append), so a txnId we have no record of is either long-cleaned-up
        // (committed) or never existed. Either way: visible/committed-default.
        MetadataTransactionBuffer tb = new MetadataTransactionBuffer(topic, txnStore);
        tb.checkIfTBRecoverCompletely().get();

        assertThat(tb.isTxnAborted(new TxnID(99, 99), PositionFactory.create(1, 0))).isFalse();
    }

    @Test
    public void restartAfterCommit_committedDataStillVisible() throws Exception {
        // The bug P3.5 fixes: after a TB restart on a topic with old committed transactional
        // data, the original /txn/id/<txnId> headers may have been GC'd. With P3's
        // "unknown → aborted" default + cleanup-on-apply, the committed messages would be
        // wrongly filtered. With P3.5's "unknown → visible (committed) below watermark", they
        // are not.
        //
        // Simulate: pre-populate the durable per-segment watermark (representing a TB instance
        // that previously processed commits and persisted the resolved-below mark). The header
        // and /txn/op records are absent — GC ran. Construct a fresh TB and verify the
        // committed txn's messages are visible.

        TxnID oldCommittedTxn = new TxnID(7, 42);
        // Watermark sits at 5:0 — below this is fully resolved per the previous TB instance.
        txnStore.casSegmentWatermark(SEGMENT,
                new org.apache.pulsar.broker.transaction.metadata.SegmentWatermark(5, 0),
                Optional.of(-1L)).get();

        MetadataTransactionBuffer tb = new MetadataTransactionBuffer(topic, txnStore);
        tb.checkIfTBRecoverCompletely().get();

        // The bug fix: isTxnAborted no longer treats unknown-below-watermark as aborted, so
        // committed messages whose original /txn/id headers have been GC'd are still visible.
        assertThat(tb.isTxnAborted(oldCommittedTxn, PositionFactory.create(3, 5))).isFalse();
        // With no open txns recovered, maxReadPosition can advance past the watermark to LAC —
        // the absence of /txn/op records is positive evidence (by the orphan-elimination
        // invariant) that no unresolved txn data sits above the watermark.
        assertThat(tb.getMaxReadPosition()).isEqualTo(ledger.getLastConfirmedEntry());
    }

    @Test
    public void restartAfterAbort_abortedTxnStillFiltered() throws Exception {
        // Mirror of the commit scenario: an aborted txn's durable record persists in
        // /txn/segment-state/<segment>/aborted/<txnId> with an index entry on max position.
        // Across restarts, isTxnAborted continues to return true even if the original
        // /txn/id/<txnId> header has been GC'd.

        TxnID oldAbortedTxn = new TxnID(7, 99);
        String txnIdKey = TxnIds.toKey(oldAbortedTxn);
        txnStore.casSegmentWatermark(SEGMENT,
                new org.apache.pulsar.broker.transaction.metadata.SegmentWatermark(5, 0),
                Optional.of(-1L)).get();
        // Persisted aborted record. Header doesn't exist (GC'd).
        txnStore.putAbortedTxn(SEGMENT, txnIdKey, 3L, 5L).get();

        MetadataTransactionBuffer tb = new MetadataTransactionBuffer(topic, txnStore);
        tb.checkIfTBRecoverCompletely().get();

        assertThat(tb.isTxnAborted(oldAbortedTxn, PositionFactory.create(3, 5))).isTrue();
    }

    @Test
    public void pruneTrimmedAborted_dropsBelowFirstValid_retainsAbove() throws Exception {
        // An aborted txn whose data the ML has fully trimmed (max position below the first valid
        // position) is dropped from both the durable aborted records and the in-memory set; an
        // aborted txn whose data is still readable is retained.
        TxnID trimmedTxn = new TxnID(1, 100);   // max position on ledger 1 — will be trimmed away
        TxnID liveTxn = new TxnID(1, 200);      // max position on ledger 10 — still readable
        txnStore.putAbortedTxn(SEGMENT, TxnIds.toKey(trimmedTxn), 1L, 5L).get();
        txnStore.putAbortedTxn(SEGMENT, TxnIds.toKey(liveTxn), 10L, 5L).get();

        MetadataTransactionBuffer tb = new MetadataTransactionBuffer(topic, txnStore);
        tb.checkIfTBRecoverCompletely().get();
        assertThat(tb.isTxnAborted(trimmedTxn, PositionFactory.create(1, 5))).isTrue();
        assertThat(tb.isTxnAborted(liveTxn, PositionFactory.create(10, 5))).isTrue();

        // The ML has trimmed everything below ledger 5.
        when(ledger.getFirstPosition()).thenReturn(PositionFactory.create(5, 0));
        tb.pruneTrimmedAbortedTxns().get();

        // In-memory: trimmed dropped, live retained.
        assertThat(tb.isTxnAborted(trimmedTxn, PositionFactory.create(1, 5))).isFalse();
        assertThat(tb.isTxnAborted(liveTxn, PositionFactory.create(10, 5))).isTrue();

        // Durable record also deleted: a fresh TB recovers only the live txn.
        MetadataTransactionBuffer tb2 = new MetadataTransactionBuffer(topic, txnStore);
        tb2.checkIfTBRecoverCompletely().get();
        assertThat(tb2.isTxnAborted(trimmedTxn, PositionFactory.create(1, 5))).isFalse();
        assertThat(tb2.isTxnAborted(liveTxn, PositionFactory.create(10, 5))).isTrue();
    }

    @Test
    public void recoveryDiscoveredOpenTxn_pinsAtWatermark() throws Exception {
        // /txn/op records exist for an open txn (broker was processing publishes for txn T;
        // T's resolution hadn't yet completed; watermark was persisted at 5:0 before the crash).
        // On restart the TB pins maxReadPosition at the persisted watermark — it doesn't know
        // T's real positions and must not expose any segment data above the watermark until T
        // resolves.

        TxnID openTxn = new TxnID(1, 1);
        createOpenHeader(openTxn);
        // Position values in the legacy /txn/op format are no longer read by recovery; only the
        // existence of the record matters (it tells recovery this txn is involved on this segment).
        txnStore.appendOp(TxnIds.toKey(openTxn),
                new TxnOp(TxnOpKind.WRITE, SEGMENT, null, 0L, 0L, null)).get();
        // An OPEN txn whose only op record is on a *different* segment must not be recovered here.
        TxnID otherSegmentTxn = new TxnID(2, 2);
        createOpenHeader(otherSegmentTxn);
        txnStore.appendOp(TxnIds.toKey(otherSegmentTxn),
                new TxnOp(TxnOpKind.WRITE, "segment://public/default/topic/0000-ffff-1",
                        null, 0L, 0L, null)).get();
        txnStore.casSegmentWatermark(SEGMENT,
                new org.apache.pulsar.broker.transaction.metadata.SegmentWatermark(5, 0),
                Optional.of(-1L)).get();

        MetadataTransactionBuffer tb = new MetadataTransactionBuffer(topic, txnStore);
        tb.checkIfTBRecoverCompletely().get();

        assertThat(tb.getMaxReadPosition()).isEqualTo(PositionFactory.create(5, 0));
        // Only the same-segment txn is tracked; the other-segment op record isn't recovered here.
        assertThat(tb.getOngoingTxnCount()).isOne();
        // An OPEN recovery-discovered txn is not in the aborted set.
        assertThat(tb.isTxnAborted(openTxn, PositionFactory.create(5, 0))).isFalse();
    }

    @Test
    public void recoveryDiscoveredAbortedTxns_filteredImmediatelyAfterRecovery() throws Exception {
        // Regression for the recovery race: txns whose headers are ABORTED and whose only remaining
        // trace is a leftover /txn/op record — no durable aborted record was ever written (the
        // previous broker crashed before applyTerminalNow persisted it). isTxnAborted must return
        // true for every such txn the instant recovery completes. ABORTED recovery entries aren't
        // watermark-pinned, so any visibility window here would expose the aborted data.
        //
        // The race only opens with multiple terminal txns: the first applyTerminalNow runs
        // synchronously (stateTail was complete) and adds itself, but a second is queued behind the
        // first's in-flight persist chain. We force exactly that by hanging the first putAbortedTxn,
        // so the second txn's queued apply cannot run before recovery completes — only the
        // synchronous hydrate in applyHeaderForRecovery makes it filtered in time.

        TxnID abortedA = new TxnID(7, 42);
        TxnID abortedB = new TxnID(7, 43);
        for (TxnID t : new TxnID[] {abortedA, abortedB}) {
            createOpenHeader(t);
            abortTxn(t);
            // Leftover op record on this segment; no putAbortedTxn — durable aborted record is gone.
            txnStore.appendOp(TxnIds.toKey(t),
                    new TxnOp(TxnOpKind.WRITE, SEGMENT, null, 0L, 0L, null)).get();
        }

        // Spy the store and hang the first persisted-aborted-record write so its apply chain stays
        // in flight, blocking any txn queued behind it on stateTail.
        TxnMetadataStore spied = org.mockito.Mockito.spy(txnStore);
        CompletableFuture<Stat> firstPut = new CompletableFuture<>();
        java.util.concurrent.atomic.AtomicBoolean first = new java.util.concurrent.atomic.AtomicBoolean(true);
        doAnswer(inv -> {
            if (first.getAndSet(false)) {
                return firstPut;
            }
            return inv.callRealMethod();
        }).when(spied).putAbortedTxn(any(), any(), org.mockito.ArgumentMatchers.anyLong(),
                org.mockito.ArgumentMatchers.anyLong());

        MetadataTransactionBuffer tb = new MetadataTransactionBuffer(topic, spied);
        tb.checkIfTBRecoverCompletely().get();

        // Both must be filtered as of recovery completion — without the synchronous hydrate, the
        // txn queued behind the hung persist would still read as visible here.
        assertThat(tb.isTxnAborted(abortedA, PositionFactory.create(10, 0))).isTrue();
        assertThat(tb.isTxnAborted(abortedB, PositionFactory.create(10, 0))).isTrue();

        // Release the hung write so the apply chain can drain cleanly.
        firstPut.complete(null);
    }

    @Test
    public void recoveryDiscoveredAbortedTxn_persistsAbortedRecordAtSegmentLac() throws Exception {
        // Regression for the prune-key bug: a recovery-discovered aborted txn has unknown
        // positions, so persistAbortedRecord falls back to the segment LAC — NOT the durable
        // watermark. The txn's data sits above the watermark, so keying the record on the
        // watermark would let trim-pruning drop it while the data is still readable.
        TxnID abortedTxn = new TxnID(8, 99);
        String txnIdKey = TxnIds.toKey(abortedTxn);
        createOpenHeader(abortedTxn);
        abortTxn(abortedTxn);
        txnStore.appendOp(txnIdKey, new TxnOp(TxnOpKind.WRITE, SEGMENT, null, 0L, 0L, null)).get();
        // Watermark (5,0) is well below the segment LAC (10,0).
        txnStore.casSegmentWatermark(SEGMENT,
                new org.apache.pulsar.broker.transaction.metadata.SegmentWatermark(5, 0),
                Optional.of(-1L)).get();

        MetadataTransactionBuffer tb = new MetadataTransactionBuffer(topic, txnStore);
        tb.checkIfTBRecoverCompletely().get();

        // The aborted record is persisted on stateTail after recovery completes; await it and
        // assert its stored max position is the LAC (10,0), not the watermark (5,0).
        Awaitility.await().untilAsserted(() -> {
            var opt = store.get(TxnPaths.segmentAbortedTxnPath(SEGMENT, txnIdKey)).get();
            assertThat(opt).isPresent();
            AbortedTxnRecord rec = TxnMetadataStore.fromJson(opt.get().getValue(), AbortedTxnRecord.class);
            assertThat(rec.maxLedgerId()).isEqualTo(10L);
            assertThat(rec.maxEntryId()).isEqualTo(0L);
        });
    }

    @Test
    public void publishOrdering_opRecordWrittenBeforeMlAppend() throws Exception {
        // The orphan-elimination invariant: when appendBufferToTxn returns, both /txn/op and
        // the ML entry exist. With the reversed ordering, the op record is written first; if
        // the ML append then fails, the op record is still present and the TC's timeout sweep
        // will abort the txn and clean up. The reverse — entry without op — cannot happen by
        // construction.

        TxnID txnId = new TxnID(1, 1);
        createOpenHeader(txnId);
        MetadataTransactionBuffer tb = new MetadataTransactionBuffer(topic, txnStore);
        tb.checkIfTBRecoverCompletely().get();

        tb.appendBufferToTxn(txnId, 0, payload("a")).get();

        // /txn/op record should exist for this txn on this segment.
        java.util.List<TxnOp> hits = new java.util.ArrayList<>();
        txnStore.listWritesBySegment(SEGMENT,
                new org.apache.pulsar.metadata.api.ScanConsumer() {
                    @Override
                    public void onNext(org.apache.pulsar.metadata.api.GetResult r) {
                        hits.add(TxnMetadataStore.fromJson(r.getValue(), TxnOp.class));
                    }
                    @Override public void onError(Throwable t) { }
                    @Override public void onCompleted() { }
                }).get();
        assertThat(hits).hasSize(1);
        assertThat(hits.get(0).getKind()).isEqualTo(TxnOpKind.WRITE);
        // The WRITE op record carries sentinel positions (0/0); positions live in TB's
        // in-memory tracking, not in the op record.
        assertThat(hits.get(0).getLedgerId()).isZero();
        assertThat(hits.get(0).getEntryId()).isZero();
    }

    // ---- helpers -----------------------------------------------------------

    private static ByteBuf payload(String s) {
        return Unpooled.copiedBuffer(s.getBytes());
    }

    private void createOpenHeader(TxnID txnId) throws Exception {
        txnStore.createHeader(TxnIds.toKey(txnId),
                new TxnHeader(TxnState.OPEN, Duration.ofMillis(60_000),
                        Instant.ofEpochMilli(1000), null, null)).get();
    }

    private void commitTxn(TxnID txnId) throws Exception {
        var v = txnStore.getHeader(TxnIds.toKey(txnId)).get().orElseThrow();
        var h = v.value();
        txnStore.updateHeader(TxnIds.toKey(txnId),
                new TxnHeader(TxnState.COMMITTED, h.getTimeout(), h.getCreatedAt(), Instant.now(), null),
                v.version()).get();
    }

    private void abortTxn(TxnID txnId) throws Exception {
        var v = txnStore.getHeader(TxnIds.toKey(txnId)).get().orElseThrow();
        var h = v.value();
        txnStore.updateHeader(TxnIds.toKey(txnId),
                new TxnHeader(TxnState.ABORTED, h.getTimeout(), h.getCreatedAt(), Instant.now(), null),
                v.version()).get();
    }

    private ManagedLedger mockManagedLedger() {
        ManagedLedger ml = mock(ManagedLedger.class);
        when(ml.getLastConfirmedEntry()).thenReturn(PositionFactory.create(10, 0));
        // asyncAddEntry: synthesize a unique increasing position, invoke addComplete.
        doAnswer(inv -> {
            AsyncCallbacks.AddEntryCallback cb = inv.getArgument(1);
            Position p = PositionFactory.create(10, nextEntryId.getAndIncrement());
            cb.addComplete(p, inv.getArgument(0), inv.getArgument(2));
            return null;
        }).when(ml).asyncAddEntry(any(ByteBuf.class), any(), any());
        when(ml.getPreviousPosition(any())).thenAnswer(inv -> {
            Position p = inv.getArgument(0);
            return PositionFactory.create(p.getLedgerId(), p.getEntryId() - 1);
        });
        return ml;
    }

    private PersistentTopic mockTopic(String name, ManagedLedger ml) {
        PersistentTopic t = mock(PersistentTopic.class);
        when(t.getName()).thenReturn(name);
        when(t.getManagedLedger()).thenReturn(ml);
        when(t.getMaxReadPositionCallBack()).thenReturn(null);
        return t;
    }

    @SuppressWarnings("unused") // referenced from Optional to suppress unused-imports
    private static <T> Optional<T> emptyOpt() {
        return Optional.empty();
    }
}
