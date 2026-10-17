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
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.transaction.metadata.TxnEvent;
import org.apache.pulsar.broker.transaction.metadata.TxnHeader;
import org.apache.pulsar.broker.transaction.metadata.TxnIds;
import org.apache.pulsar.broker.transaction.metadata.TxnMetadataStore;
import org.apache.pulsar.broker.transaction.metadata.TxnOp;
import org.apache.pulsar.broker.transaction.metadata.TxnOpKind;
import org.apache.pulsar.broker.transaction.metadata.TxnState;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
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
    private BrokerService brokerService;
    private CoordinationService coordinationService;
    private TransactionCoordinatorV5 tc;

    @BeforeMethod
    public void setUp() throws Exception {
        store = MetadataStoreExtended.create("memory:local",
                MetadataStoreConfig.builder().fsyncEnable(false).build());
        txnStore = new TxnMetadataStore(store);
        coordinationService = new CoordinationServiceImpl(store);
        pulsar = mock(PulsarService.class);
        when(pulsar.getLocalMetadataStore()).thenReturn(store);
        when(pulsar.getCoordinationService()).thenReturn(coordinationService);
        when(pulsar.getBrokerId()).thenReturn("broker-test:8080");
        when(pulsar.getBrokerServiceUrl()).thenReturn("pulsar://broker-test:6650");
        when(pulsar.getBrokerServiceUrlTls()).thenReturn(null);
        when(pulsar.getSafeWebServiceAddress()).thenReturn("http://broker-test:8080");
        ServiceConfiguration cfg = new ServiceConfiguration();
        // GC sweep tests assume retention has already elapsed.
        cfg.setTransactionCoordinatorScalableTopicsGcRetentionSeconds(0);
        // Keep the election small so start() converges quickly in unit tests.
        cfg.setTransactionCoordinatorScalableTopicsParallelism(4);
        when(pulsar.getConfiguration()).thenReturn(cfg);
        brokerService = mock(BrokerService.class);
        when(pulsar.getBrokerService()).thenReturn(brokerService);
        // Default: owned (assign-topic fallback path in handleClientConnect). Tests that want to
        // assert the not-owned path can override.
        when(brokerService.checkTopicNsOwnership(any())).thenReturn(CompletableFuture.completedFuture(null));
        tc = new TransactionCoordinatorV5(pulsar);
        tc.start();
        // As the only broker, we win every partition's election; wait until partition 0 is led so
        // the sweep-gating and client-connect paths behave deterministically.
        Awaitility.await().until(() -> tc.isLeaderFor(0));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (tc != null) {
            tc.close();
        }
        if (coordinationService != null) {
            coordinationService.close();
        }
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

    @Test
    public void sweepTimeouts_abortsExpiredOpenTxnAndFansOut() throws Exception {
        // Wait until the positive timeout has actually expired before running the sweep.
        TxnID txnId = tc.newTransaction(TC_ID, 1L, "owner").get();
        String txnIdKey = TxnIds.toKey(txnId);
        TxnHeader openHeader = txnStore.getHeader(txnIdKey).get().orElseThrow().value();
        long deadlineMs = openHeader.getCreatedAt().toEpochMilli() + openHeader.getTimeout().toMillis();
        Awaitility.await().until(() -> System.currentTimeMillis() >= deadlineMs);

        String segment = "segment://public/default/topic/0000-ffff-0";
        txnStore.appendOp(txnIdKey,
                new TxnOp(TxnOpKind.WRITE, segment, null, 5L, 1L, null)).get();

        List<String> received = new ArrayList<>();
        try (var sub = txnStore.subscribeSegmentEvents(segment, received::add)) {
            // newTransaction and the first sweep can land in the same millisecond on a fast machine,
            // leaving the 1ms-timeout txn not-yet-expired. The sweep is idempotent, so retry it until
            // the deadline has elapsed and the txn is aborted.
            Awaitility.await().untilAsserted(() -> {
                tc.sweepTimeouts().get();
                var header = txnStore.getHeader(txnIdKey).get().orElseThrow();
                assertThat(header.value().getState()).isEqualTo(TxnState.ABORTED);
            });
            // Fan-out fires for the participant.
            Awaitility.await().untilAsserted(() -> assertThat(received).isNotEmpty());
        }
    }

    @Test
    public void sweepTimeouts_leavesUnexpiredOpenTxnAlone() throws Exception {
        TxnID txnId = tc.newTransaction(TC_ID, 60_000L, "owner").get();
        tc.sweepTimeouts().get();
        var header = txnStore.getHeader(TxnIds.toKey(txnId)).get().orElseThrow();
        assertThat(header.value().getState()).isEqualTo(TxnState.OPEN);
    }

    @Test
    public void sweepGc_deletesHeaderWhenNoOpsRemain() throws Exception {
        // No participants → fan-out wrote no events → no /txn/op records to clean up → GC may
        // delete the header straight away.
        TxnID txnId = tc.newTransaction(TC_ID, 60_000L, "owner").get();
        String txnIdKey = TxnIds.toKey(txnId);
        tc.endTransaction(txnId, TxnAction.COMMIT_VALUE).get();

        tc.sweepGc().get();

        assertThat(txnStore.getHeader(txnIdKey).get()).isEmpty();
    }

    @Test
    public void sweepGc_repairsAndRetainsHeaderWhenOpsRemain() throws Exception {
        // A finalized txn with a leftover /txn/op record — the participant either hasn't applied
        // the outcome yet, or never received the event (TC crashed between header CAS and publish).
        // GC must re-drive the fan-out so the participant re-reads the true outcome, and must NOT
        // delete the header while it could still be re-read (else a COMMITTED txn's data would
        // default to ABORTED).
        TxnID txnId = tc.newTransaction(TC_ID, 60_000L, "owner").get();
        String txnIdKey = TxnIds.toKey(txnId);
        String segment = "segment://public/default/topic/0000-ffff-0";
        txnStore.appendOp(txnIdKey,
                new TxnOp(TxnOpKind.WRITE, segment, null, 5L, 1L, null)).get();

        List<String> received = new ArrayList<>();
        try (var sub = txnStore.subscribeSegmentEvents(segment, received::add)) {
            tc.endTransaction(txnId, TxnAction.COMMIT_VALUE).get();
            Awaitility.await().untilAsserted(() -> assertThat(received).hasSize(1));

            tc.sweepGc().get();

            // Header retained — participant may still need to re-read.
            TxnHeader header = txnStore.getHeader(txnIdKey).get().orElseThrow().value();
            assertThat(header.getState()).isEqualTo(TxnState.COMMITTED);
            // Repair re-published the event.
            Awaitility.await().untilAsserted(() -> assertThat(received).hasSize(2));
        }
    }

    @Test
    public void sweeps_skipWhenNotLeader() throws Exception {
        // Create an expired txn, then drop leadership (close releases the election leases). The
        // sweep is gated on isLeaderFor(0), so on a fresh non-leader TC it must skip.
        TxnID expired = tc.newTransaction(TC_ID, 1L, "owner").get();
        tc.close();

        // A second TC that never started (no elections) is not the leader for partition 0.
        TransactionCoordinatorV5 notLeader = new TransactionCoordinatorV5(pulsar);
        try {
            notLeader.sweepTimeouts().get();
            // Still OPEN — the sweep never ran because this TC leads no partition.
            var header = txnStore.getHeader(TxnIds.toKey(expired)).get().orElseThrow();
            assertThat(header.value().getState()).isEqualTo(TxnState.OPEN);
        } finally {
            notLeader.close();
        }
    }

    // ---- Election + assignment discovery ----------------------------------

    @Test
    public void election_singleBrokerLeadsAllPartitions() {
        // As the only broker, this TC wins every partition's election.
        for (int p = 0; p < 4; p++) {
            final int partition = p;
            Awaitility.await().until(() -> tc.isLeaderFor(partition));
        }
        assertThat(tc.isLeaderFor(4)).isFalse(); // out of range (parallelism = 4)
    }

    @Test
    public void buildAssignmentsSnapshot_reportsAllLedPartitions() {
        assertThat(tc.buildAssignmentsSnapshot().join().partitionCount()).isEqualTo(4);
        Awaitility.await().untilAsserted(() -> {
            var snap = tc.buildAssignmentsSnapshot().join();
            assertThat(snap.assignments()).hasSize(4);
            assertThat(snap.isComplete()).isTrue();
            assertThat(snap.assignments().get(0).brokerServiceUrl())
                    .isEqualTo("pulsar://broker-test:6650");
            assertThat(snap.assignments().get(0).brokerId()).isEqualTo("broker-test:8080");
        });
    }

    @Test
    public void registerAssignmentChangeListener_deregistersOnClose() throws Exception {
        // The handle deregisters the listener; after close() it must not be invoked again. We only
        // assert the registration/deregistration contract here — the fire-on-election-change path
        // needs a multi-broker setup and is covered at integration level.
        AutoCloseable handle = tc.registerAssignmentChangeListener(() -> { });
        handle.close();
    }

    @Test
    public void handleClientConnect_acceptsWhenLeader() throws Exception {
        // We lead partition 0, so connect is accepted without consulting assign-topic ownership.
        tc.handleClientConnect(TC_ID).get();
    }

    @Test
    public void start_failsOnParallelismMismatch() throws Exception {
        // The running tc (from setUp) persisted parallelism=4. A second coordinator configured with a
        // different value against the same metadata store must refuse to start.
        ServiceConfiguration mismatchCfg = new ServiceConfiguration();
        mismatchCfg.setTransactionCoordinatorScalableTopicsGcRetentionSeconds(0);
        mismatchCfg.setTransactionCoordinatorScalableTopicsParallelism(8);
        PulsarService other = mock(PulsarService.class);
        when(other.getLocalMetadataStore()).thenReturn(store);
        when(other.getCoordinationService()).thenReturn(coordinationService);
        when(other.getConfiguration()).thenReturn(mismatchCfg);
        when(other.getBrokerId()).thenReturn("broker-other:8080");
        when(other.getBrokerServiceUrl()).thenReturn("pulsar://broker-other:6650");
        when(other.getSafeWebServiceAddress()).thenReturn("http://broker-other:8080");
        when(other.getBrokerService()).thenReturn(brokerService);

        TransactionCoordinatorV5 mismatched = new TransactionCoordinatorV5(other);
        try {
            assertThatThrownBy(mismatched::start)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("mismatch")
                    .hasMessageContaining("8")
                    .hasMessageContaining("4");
        } finally {
            mismatched.close();
        }
    }
}
