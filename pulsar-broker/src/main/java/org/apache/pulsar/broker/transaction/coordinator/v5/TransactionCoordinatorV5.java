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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.transaction.exception.coordinator.TransactionCoordinatorException;
import org.apache.pulsar.broker.transaction.metadata.TxnEvent;
import org.apache.pulsar.broker.transaction.metadata.TxnHeader;
import org.apache.pulsar.broker.transaction.metadata.TxnIds;
import org.apache.pulsar.broker.transaction.metadata.TxnMetadataStore;
import org.apache.pulsar.broker.transaction.metadata.TxnOp;
import org.apache.pulsar.broker.transaction.metadata.TxnOpKind;
import org.apache.pulsar.broker.transaction.metadata.TxnState;
import org.apache.pulsar.broker.transaction.metadata.Versioned;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.ScanConsumer;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionSubscription;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException;

/**
 * PIP-473 v5 transaction coordinator — broker-side service.
 *
 * <p>Per-partition coordinator. A broker runs the v5 TC for partition {@code N} iff it owns
 * partition {@code N} of {@code SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN} — same
 * leader-election mechanism the legacy {@code TransactionMetadataStoreService} uses; reusing
 * it keeps the client-side discovery surface unchanged.
 *
 * <p>Wire commands handled (routed by {@code ServerCnx} when
 * {@code transactionCoordinatorScalableTopicsEnabled} is on):
 * <ul>
 *   <li>{@code TC_CLIENT_CONNECT} → {@link #handleClientConnect}</li>
 *   <li>{@code NEW_TXN} → {@link #newTransaction}</li>
 *   <li>{@code ADD_PARTITION_TO_TXN}, {@code ADD_SUBSCRIPTION_TO_TXN} — no-ops per PIP; the v5
 *       participants advertise themselves by writing {@code /txn/op} records, so the TC doesn't
 *       need a pre-registration step.</li>
 *   <li>{@code END_TXN} → {@link #endTransaction}</li>
 * </ul>
 *
 * <p>{@code endTransaction} CAS-updates the header to the terminal state, enumerates
 * {@code /txn/op/<txnId>-*} via {@link TxnPaths#IDX_OPS_BY_TXN}, and publishes one
 * segment-event per affected segment + one subscription-event per affected
 * {@code (segment, subscription)} pair. The fan-out is metadata-store writes (not RPCs) and
 * is bounded by the txn's participant count.
 *
 * <p>P5.1 scope: happy-path newTxn / endTxn. No timeout sweep, no GC sweep — those land in
 * P5.2.
 */
@CustomLog
public class TransactionCoordinatorV5 {

    private final PulsarService pulsar;
    private final TxnMetadataStore txnStore;

    public TransactionCoordinatorV5(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.txnStore = new TxnMetadataStore(pulsar.getLocalMetadataStore());
    }

    // ---- TC client connect ------------------------------------------------

    /**
     * Verify this broker is the leader for {@code tcId} (owns the corresponding partition of
     * {@code transaction_coordinator_assign}). Mirrors the ownership check the legacy
     * {@code TransactionMetadataStoreService.handleTcClientConnect} performs — the same
     * topic-ownership mechanism serves as our leader-election surface.
     */
    public CompletableFuture<Void> handleClientConnect(TransactionCoordinatorID tcId) {
        String assignPartition = SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN
                .getPartition((int) tcId.getId()).toString();
        return pulsar.getBrokerService().checkTopicNsOwnership(assignPartition);
    }

    // ---- newTransaction ---------------------------------------------------

    /**
     * Create a new transaction header at {@code /txn/id/<tcId>_<seq>}. The {@code leastSigBits}
     * is drawn from the per-tcId monotonic sequence counter ({@link TxnMetadataStore#nextTxnSequence})
     * so txnIds are never reused — the participant-side aborted-set is keyed by txnId, and reuse
     * would break that.
     */
    public CompletableFuture<TxnID> newTransaction(TransactionCoordinatorID tcId, long timeoutInMillis,
                                                   String owner) {
        return txnStore.nextTxnSequence(tcId.getId()).thenCompose(seq -> {
            TxnID txnId = new TxnID(tcId.getId(), seq);
            TxnHeader header = new TxnHeader(TxnState.OPEN,
                    Duration.ofMillis(timeoutInMillis), Instant.now(), null, owner);
            return txnStore.createHeader(TxnIds.toKey(txnId), header).thenApply(stat -> txnId);
        });
    }

    /**
     * Verify {@code principal} owns {@code txnId}. Mirrors the legacy coordinator's
     * {@code TransactionMetadataStoreService.verifyTxnOwnership} semantics: a {@code null} stored
     * owner (authentication disabled, or a legacy txn) is always allowed; otherwise the principal
     * must match. The superuser fallback lives in {@code ServerCnx#verifyTxnOwnership}, same as the
     * legacy path. A missing header resolves to {@code false} (not owned).
     */
    public CompletableFuture<Boolean> verifyTxnOwnership(TxnID txnId, String principal) {
        return txnStore.getHeader(TxnIds.toKey(txnId)).thenApply(opt -> opt
                .map(v -> {
                    String owner = v.value().getOwner();
                    return owner == null || owner.equals(principal);
                })
                .orElse(false));
    }

    // ---- addPartition / addSubscription (no-op in v5) ---------------------

    /**
     * No-op per PIP-473 — in v5, participants advertise themselves by writing {@code /txn/op}
     * records when they actually apply ops. The pre-registration step is unnecessary.
     */
    public CompletableFuture<Void> addProducedPartitionToTxn(TxnID txnId, List<String> partitions) {
        return CompletableFuture.completedFuture(null);
    }

    /** No-op (see {@link #addProducedPartitionToTxn}). */
    public CompletableFuture<Void> addAckedSubscriptionToTxn(TxnID txnId,
                                                             List<TransactionSubscription> subscriptions) {
        return CompletableFuture.completedFuture(null);
    }

    // ---- endTransaction ---------------------------------------------------

    /**
     * Finalise a transaction: CAS the header to {@code COMMITTED}/{@code ABORTED}, enumerate
     * the txn's participants via {@link TxnMetadataStore#listOpsByTxn}, and publish one
     * segment-event per affected segment and one subscription-event per affected
     * {@code (segment, subscription)} pair. Idempotent against retries — a header already in
     * the requested terminal state short-circuits without republishing.
     */
    public CompletableFuture<Void> endTransaction(TxnID txnId, int txnAction) {
        TxnState newState = newStateFor(txnAction);
        if (newState == null) {
            return FutureUtil.failedFuture(
                    new TransactionCoordinatorException.UnsupportedTxnActionException(txnId, txnAction));
        }
        String txnIdKey = TxnIds.toKey(txnId);
        return txnStore.getHeader(txnIdKey).thenCompose(opt -> {
            if (opt.isEmpty()) {
                return FutureUtil.failedFuture(
                        new CoordinatorException.TransactionNotFoundException(
                                "Transaction not found: " + txnId));
            }
            Versioned<TxnHeader> v = opt.get();
            TxnHeader current = v.value();
            if (current.getState() == newState) {
                // Idempotent retry — header already terminal-and-matching. Re-drive the fan-out
                // rather than short-circuiting: if a previous attempt CAS'd the header but failed
                // (partially or fully) before publishing, the only way a participant ever learns
                // the decision is the event, and a terminal header gives the reconcile path nothing
                // to act on. Re-publishing is safe — participants key on (txnId, decision) and the
                // decision can't change once terminal.
                return fanOutEvents(txnId, txnIdKey, newState);
            }
            if (current.getState() != TxnState.OPEN) {
                return FutureUtil.failedFuture(
                        new CoordinatorException.InvalidTxnStatusException(
                                "Transaction " + txnId + " is " + current.getState()
                                        + ", cannot transition to " + newState));
            }
            TxnHeader updated = new TxnHeader(newState, current.getTimeout(),
                    current.getCreatedAt(), Instant.now(), current.getOwner());
            return txnStore.updateHeader(txnIdKey, updated, v.version())
                    .thenCompose(stat -> fanOutEvents(txnId, txnIdKey, newState));
        });
    }

    private static TxnState newStateFor(int txnAction) {
        if (txnAction == TxnAction.COMMIT_VALUE) {
            return TxnState.COMMITTED;
        } else if (txnAction == TxnAction.ABORT_VALUE) {
            return TxnState.ABORTED;
        }
        return null;
    }

    /**
     * Enumerate {@code /txn/op} via {@link TxnMetadataStore#listOpsByTxn}, group by participant,
     * and publish one event per participant. Writes are independent so we fire them in parallel.
     */
    private CompletableFuture<Void> fanOutEvents(TxnID txnId, String txnIdKey, TxnState decision) {
        Set<String> writeSegments = ConcurrentHashMap.newKeySet();
        Set<AckParticipant> ackParticipants = ConcurrentHashMap.newKeySet();
        return txnStore.listOpsByTxn(txnIdKey, new ScanConsumer() {
            @Override
            public void onNext(GetResult r) {
                TxnOp op = TxnMetadataStore.fromJson(r.getValue(), TxnOp.class);
                if (op.getKind() == TxnOpKind.WRITE) {
                    writeSegments.add(op.getSegment());
                } else if (op.getKind() == TxnOpKind.ACK && op.getSubscription() != null) {
                    ackParticipants.add(new AckParticipant(op.getSegment(), op.getSubscription()));
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log.warn().attr("txnId", txnId).exception(throwable)
                        .log("endTxn participant enumeration encountered an error");
            }

            @Override
            public void onCompleted() {
            }
        }).thenCompose(__ -> {
            TxnEvent event = new TxnEvent(txnIdKey, decision);
            CompletableFuture<?>[] publishes = new CompletableFuture<?>[
                    writeSegments.size() + ackParticipants.size()];
            int i = 0;
            for (String segment : writeSegments) {
                publishes[i++] = txnStore.publishSegmentEvent(segment, event);
            }
            for (AckParticipant p : ackParticipants) {
                publishes[i++] = txnStore.publishSubscriptionEvent(p.segment(), p.subscription(), event);
            }
            return CompletableFuture.allOf(publishes);
        });
    }

    /** A {@code (segment, subscription)} ack participant; keys the ack fan-out de-dup set. */
    private record AckParticipant(String segment, String subscription) {
    }
}
