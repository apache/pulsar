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
package org.apache.pulsar.broker.transaction.coordinator.v5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.transaction.metadata.TxnEvent;
import org.apache.pulsar.broker.transaction.metadata.TxnIds;
import org.apache.pulsar.broker.transaction.metadata.TxnMetadataStore;
import org.apache.pulsar.broker.transaction.metadata.TxnOp;
import org.apache.pulsar.broker.transaction.metadata.TxnOpKind;
import org.apache.pulsar.broker.transaction.metadata.TxnState;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link TransactionCoordinatorV5} against the in-memory metadata store. The
 * {@link PulsarService} / {@link BrokerService} dependencies are mocked just enough for the TC
 * to construct and to satisfy {@code handleClientConnect}'s ownership check.
 */
public class TransactionCoordinatorV5Test {

    private static final TransactionCoordinatorID TC_ID = TransactionCoordinatorID.get(0L);

    private MetadataStoreExtended store;
    private TxnMetadataStore txnStore;
    private PulsarService pulsar;
    private TransactionCoordinatorV5 tc;

    @BeforeMethod
    public void setUp() throws Exception {
        store = MetadataStoreExtended.create("memory:local",
                MetadataStoreConfig.builder().fsyncEnable(false).build());
        txnStore = new TxnMetadataStore(store);
        pulsar = mock(PulsarService.class);
        when(pulsar.getLocalMetadataStore()).thenReturn(store);
        BrokerService brokerService = mock(BrokerService.class);
        when(pulsar.getBrokerService()).thenReturn(brokerService);
        // Default: owned. Tests that want to assert the not-owned path can override.
        when(brokerService.checkTopicNsOwnership(any())).thenReturn(CompletableFuture.completedFuture(null));
        tc = new TransactionCoordinatorV5(pulsar);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (store != null) {
            store.close();
        }
    }

    @Test
    public void newTransaction_createsHeader_withSequentialLeastBits() throws Exception {
        TxnID t1 = tc.newTransaction(TC_ID, 60_000L, "owner-a").get();
        TxnID t2 = tc.newTransaction(TC_ID, 60_000L, "owner-a").get();
        TxnID t3 = tc.newTransaction(TC_ID, 60_000L, "owner-a").get();

        // mostSigBits is the tcId.
        assertThat(t1.getMostSigBits()).isEqualTo(TC_ID.getId());
        // leastSigBits is monotonic per tcId.
        assertThat(t1.getLeastSigBits()).isLessThan(t2.getLeastSigBits());
        assertThat(t2.getLeastSigBits()).isLessThan(t3.getLeastSigBits());

        // Header lives at /txn/id/<TxnIds.toKey> with state = OPEN and the opening principal.
        var header = txnStore.getHeader(TxnIds.toKey(t1)).get().orElseThrow();
        assertThat(header.value().getState()).isEqualTo(TxnState.OPEN);
        assertThat(header.value().getOwner()).isEqualTo("owner-a");
    }

    @Test
    public void endTransaction_commit_casesHeaderAndFansOutSegmentEvent() throws Exception {
        TxnID txnId = tc.newTransaction(TC_ID, 60_000L, "owner").get();
        String txnIdKey = TxnIds.toKey(txnId);
        String segment = "segment://public/default/topic/0000-ffff-0";

        // Simulate a participant appending an op for the segment.
        txnStore.appendOp(txnIdKey,
                new TxnOp(TxnOpKind.WRITE, segment, null, 5L, 1L, null)).get();

        // Subscribe to segment events before endTxn — receive the published event.
        List<String> received = new ArrayList<>();
        try (var sub = txnStore.subscribeSegmentEvents(segment, received::add)) {
            tc.endTransaction(txnId, TxnAction.COMMIT_VALUE).get();
            // Header is now COMMITTED.
            var header = txnStore.getHeader(txnIdKey).get().orElseThrow();
            assertThat(header.value().getState()).isEqualTo(TxnState.COMMITTED);
            // A segment-event for this segment was published.
            Awaitility.await().untilAsserted(() -> assertThat(received).isNotEmpty());
        }
    }

    @Test
    public void endTransaction_abort_casesHeaderAndFansOutEvents() throws Exception {
        TxnID txnId = tc.newTransaction(TC_ID, 60_000L, "owner").get();
        String txnIdKey = TxnIds.toKey(txnId);
        String segment = "segment://public/default/topic/0000-ffff-0";
        String subscription = "sub-x";

        // Two participants: a WRITE on the segment and an ACK on (segment, sub).
        txnStore.appendOp(txnIdKey,
                new TxnOp(TxnOpKind.WRITE, segment, null, 5L, 1L, null)).get();
        txnStore.appendOp(txnIdKey,
                new TxnOp(TxnOpKind.ACK, segment, subscription, 5L, 2L, false)).get();

        List<String> segReceived = new ArrayList<>();
        List<String> subReceived = new ArrayList<>();
        try (var s1 = txnStore.subscribeSegmentEvents(segment, segReceived::add);
             var s2 = txnStore.subscribeSubscriptionEvents(segment, subscription, subReceived::add)) {
            tc.endTransaction(txnId, TxnAction.ABORT_VALUE).get();

            // Header is ABORTED.
            var header = txnStore.getHeader(txnIdKey).get().orElseThrow();
            assertThat(header.value().getState()).isEqualTo(TxnState.ABORTED);

            // Both participant streams received an event.
            Awaitility.await().untilAsserted(() -> assertThat(segReceived).isNotEmpty());
            Awaitility.await().untilAsserted(() -> assertThat(subReceived).isNotEmpty());

            // The published event's decision matches.
            byte[] bytes = store.get(segReceived.get(segReceived.size() - 1)).get().orElseThrow().getValue();
            TxnEvent event = TxnMetadataStore.fromJson(bytes, TxnEvent.class);
            assertThat(event.getDecision()).isEqualTo(TxnState.ABORTED);
            assertThat(event.getTxnId()).isEqualTo(txnIdKey);
        }
    }

    @Test
    public void endTransaction_idempotent_onRetryWithSameAction() throws Exception {
        TxnID txnId = tc.newTransaction(TC_ID, 60_000L, "owner").get();
        tc.endTransaction(txnId, TxnAction.COMMIT_VALUE).get();
        // Second call with the same action succeeds and leaves the header terminal-and-matching.
        tc.endTransaction(txnId, TxnAction.COMMIT_VALUE).get();

        var header = txnStore.getHeader(TxnIds.toKey(txnId)).get().orElseThrow();
        assertThat(header.value().getState()).isEqualTo(TxnState.COMMITTED);
    }

    @Test
    public void endTransaction_idempotentRetry_republishesEvents() throws Exception {
        // A retry of a terminal-and-matching txn re-drives the fan-out, so a participant that
        // missed the first event (e.g. a partial publish on the first attempt) still gets it.
        TxnID txnId = tc.newTransaction(TC_ID, 60_000L, "owner").get();
        String txnIdKey = TxnIds.toKey(txnId);
        String segment = "segment://public/default/topic/0000-ffff-0";
        txnStore.appendOp(txnIdKey,
                new TxnOp(TxnOpKind.WRITE, segment, null, 5L, 1L, null)).get();

        List<String> received = new ArrayList<>();
        try (var sub = txnStore.subscribeSegmentEvents(segment, received::add)) {
            tc.endTransaction(txnId, TxnAction.COMMIT_VALUE).get();
            Awaitility.await().untilAsserted(() -> assertThat(received).hasSize(1));
            // Retry re-publishes — a second event lands for the same segment.
            tc.endTransaction(txnId, TxnAction.COMMIT_VALUE).get();
            Awaitility.await().untilAsserted(() -> assertThat(received).hasSize(2));
        }
    }

    @Test
    public void verifyTxnOwnership_matchesOwnerAndRejectsOthers() throws Exception {
        TxnID txnId = tc.newTransaction(TC_ID, 60_000L, "alice").get();
        assertThat(tc.verifyTxnOwnership(txnId, "alice").get()).isTrue();
        assertThat(tc.verifyTxnOwnership(txnId, "bob").get()).isFalse();
    }

    @Test
    public void verifyTxnOwnership_nullOwnerAlwaysAllowed() throws Exception {
        // Authentication disabled — owner stored as null, mirroring the legacy "null ⟹ allowed".
        TxnID txnId = tc.newTransaction(TC_ID, 60_000L, null).get();
        assertThat(tc.verifyTxnOwnership(txnId, "anyone").get()).isTrue();
        assertThat(tc.verifyTxnOwnership(txnId, null).get()).isTrue();
    }

    @Test
    public void verifyTxnOwnership_unknownTxnReturnsFalse() throws Exception {
        assertThat(tc.verifyTxnOwnership(new TxnID(0L, 9999L), "alice").get()).isFalse();
    }

    @Test
    public void endTransaction_failsOnMismatchedAction() throws Exception {
        TxnID txnId = tc.newTransaction(TC_ID, 60_000L, "owner").get();
        tc.endTransaction(txnId, TxnAction.COMMIT_VALUE).get();
        // Trying to ABORT a COMMITTED txn → InvalidTxnStatusException.
        assertThatThrownBy(() -> tc.endTransaction(txnId, TxnAction.ABORT_VALUE).get())
                .hasCauseInstanceOf(CoordinatorException.InvalidTxnStatusException.class);
    }

    @Test
    public void endTransaction_failsForUnknownTxn() {
        assertThatThrownBy(() -> tc.endTransaction(new TxnID(0L, 9999L), TxnAction.COMMIT_VALUE).get())
                .hasCauseInstanceOf(CoordinatorException.TransactionNotFoundException.class);
    }

    @Test
    public void addPartitionAndAddSubscription_areNoOps() throws Exception {
        // v5: participants advertise themselves via /txn/op writes; ADD_PARTITION /
        // ADD_SUBSCRIPTION are wire-level no-ops in the TC.
        TxnID txnId = tc.newTransaction(TC_ID, 60_000L, "owner").get();
        tc.addProducedPartitionToTxn(txnId, List.of("persistent://public/default/topic1")).get();
        tc.addAckedSubscriptionToTxn(txnId, List.of()).get();
    }

    @Test
    public void newTransaction_collectionsAreScopedByTcId() throws Exception {
        TransactionCoordinatorID tc1 = TransactionCoordinatorID.get(1L);
        TransactionCoordinatorID tc2 = TransactionCoordinatorID.get(2L);
        TxnID a = tc.newTransaction(tc1, 60_000L, "owner").get();
        TxnID b = tc.newTransaction(tc2, 60_000L, "owner").get();
        // Different tcIds → different mostSigBits, independent leastSigBits sequences.
        assertThat(a.getMostSigBits()).isEqualTo(1L);
        assertThat(b.getMostSigBits()).isEqualTo(2L);
    }
}
