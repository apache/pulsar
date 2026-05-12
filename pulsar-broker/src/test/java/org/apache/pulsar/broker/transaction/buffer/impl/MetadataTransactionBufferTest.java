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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.metadata.TxnEvent;
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
    public void appendToCommittedTxn_failsTxnConflict() throws Exception {
        TxnID txnId = new TxnID(1, 1);
        // Pre-set header to COMMITTED — txn is terminal before any append.
        Stat created = txnStore.createHeader(TxnIds.toKey(txnId),
                new TxnHeader(TxnState.COMMITTED, Duration.ofMillis(5000),
                        Instant.ofEpochMilli(1000), Instant.ofEpochMilli(2000))).get();
        assertThat(created.getVersion()).isZero();

        MetadataTransactionBuffer tb = new MetadataTransactionBuffer(topic, txnStore);
        tb.checkIfTBRecoverCompletely().get();

        assertThatThrownBy(() -> tb.appendBufferToTxn(txnId, 0, payload("a")).get())
                .hasCauseInstanceOf(BrokerServiceException.NotAllowedException.class);
    }

    @Test
    public void unknownTxn_isTxnAbortedReturnsTrue() throws Exception {
        MetadataTransactionBuffer tb = new MetadataTransactionBuffer(topic, txnStore);
        tb.checkIfTBRecoverCompletely().get();

        // Never seen — must be filtered as aborted (orphan or long-cleaned).
        assertThat(tb.isTxnAborted(new TxnID(99, 99), PositionFactory.create(1, 0))).isTrue();
    }

    @Test
    public void recovery_rebuildsOpenTxnStateFromOpRecords() throws Exception {
        // Pre-populate: one OPEN txn with two writes on this segment, one COMMITTED txn with one
        // (lingering) write, and one txn touching a different segment (must not appear here).
        TxnID openTxn = new TxnID(1, 1);
        TxnID committedTxn = new TxnID(1, 2);
        TxnID otherSegTxn = new TxnID(1, 3);

        createOpenHeader(openTxn);
        txnStore.appendOp(TxnIds.toKey(openTxn),
                new TxnOp(TxnOpKind.WRITE, SEGMENT, null, 5L, 1L)).get();
        txnStore.appendOp(TxnIds.toKey(openTxn),
                new TxnOp(TxnOpKind.WRITE, SEGMENT, null, 5L, 2L)).get();

        txnStore.createHeader(TxnIds.toKey(committedTxn),
                new TxnHeader(TxnState.COMMITTED, Duration.ofMillis(5000),
                        Instant.ofEpochMilli(1000), Instant.ofEpochMilli(2000))).get();
        txnStore.appendOp(TxnIds.toKey(committedTxn),
                new TxnOp(TxnOpKind.WRITE, SEGMENT, null, 5L, 3L)).get();

        createOpenHeader(otherSegTxn);
        txnStore.appendOp(TxnIds.toKey(otherSegTxn),
                new TxnOp(TxnOpKind.WRITE, "segment://public/default/topic/other-seg", null, 5L, 9L)).get();

        MetadataTransactionBuffer tb = new MetadataTransactionBuffer(topic, txnStore);
        tb.checkIfTBRecoverCompletely().get();

        // openTxn pins max read position at min(5:1, 5:2) - 1 = 5:0.
        assertThat(tb.getMaxReadPosition()).isEqualTo(PositionFactory.create(5, 0));
        assertThat(tb.getOngoingTxnCount()).isOne();
        assertThat(tb.isTxnAborted(openTxn, PositionFactory.create(5, 1))).isFalse();
        // Committed-txn lingering writes should not pin max read position; the txn isn't in OPEN set.
        // (Cleanup is async; we don't assert on its completion here.)
    }

    // ---- helpers -----------------------------------------------------------

    private static ByteBuf payload(String s) {
        return Unpooled.copiedBuffer(s.getBytes());
    }

    private void createOpenHeader(TxnID txnId) throws Exception {
        txnStore.createHeader(TxnIds.toKey(txnId),
                new TxnHeader(TxnState.OPEN, Duration.ofMillis(60_000),
                        Instant.ofEpochMilli(1000), null)).get();
    }

    private void commitTxn(TxnID txnId) throws Exception {
        var v = txnStore.getHeader(TxnIds.toKey(txnId)).get().orElseThrow();
        var h = v.value();
        txnStore.updateHeader(TxnIds.toKey(txnId),
                new TxnHeader(TxnState.COMMITTED, h.getTimeout(), h.getCreatedAt(), Instant.now()),
                v.version()).get();
    }

    private void abortTxn(TxnID txnId) throws Exception {
        var v = txnStore.getHeader(TxnIds.toKey(txnId)).get().orElseThrow();
        var h = v.value();
        txnStore.updateHeader(TxnIds.toKey(txnId),
                new TxnHeader(TxnState.ABORTED, h.getTimeout(), h.getCreatedAt(), Instant.now()),
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
