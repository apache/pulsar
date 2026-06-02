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

import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.transaction.exception.coordinator.TransactionCoordinatorException;
import org.apache.pulsar.broker.transaction.metadata.TxnEvent;
import org.apache.pulsar.broker.transaction.metadata.TxnHeader;
import org.apache.pulsar.broker.transaction.metadata.TxnIds;
import org.apache.pulsar.broker.transaction.metadata.TxnMetadataStore;
import org.apache.pulsar.broker.transaction.metadata.TxnOp;
import org.apache.pulsar.broker.transaction.metadata.TxnOpKind;
import org.apache.pulsar.broker.transaction.metadata.TxnPaths;
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
 * Metadata-driven transaction coordinator for scalable topics — broker-side service.
 *
 * <p>Per-partition coordinator. A broker runs the TC for partition {@code N} iff it owns
 * partition {@code N} of {@code SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN} — same
 * leader-election mechanism the legacy {@code TransactionMetadataStoreService} uses; reusing
 * it keeps the client-side discovery surface unchanged.
 *
 * <p>Wire commands handled (routed by {@code ServerCnx} when
 * {@code transactionCoordinatorScalableTopicsEnabled} is on):
 * <ul>
 *   <li>{@code TC_CLIENT_CONNECT} → {@link #handleClientConnect}</li>
 *   <li>{@code NEW_TXN} → {@link #newTransaction}</li>
 *   <li>{@code ADD_PARTITION_TO_TXN}, {@code ADD_SUBSCRIPTION_TO_TXN} — no-ops; participants
 *       advertise themselves by writing {@code /txn/op} records, so the TC doesn't need a
 *       pre-registration step.</li>
 *   <li>{@code END_TXN} → {@link #endTransaction}</li>
 * </ul>
 *
 * <p>{@code endTransaction} CAS-updates the header to the terminal state, enumerates
 * {@code /txn/op/<txnId>-*} via {@link TxnPaths#IDX_OPS_BY_TXN}, and publishes one
 * segment-event per affected segment + one subscription-event per affected
 * {@code (segment, subscription)} pair. The fan-out is metadata-store writes (not RPCs) and
 * is bounded by the txn's participant count.
 *
 * <p>Background sweeps: a single elected broker — the owner of partition 0 of
 * {@code transaction_coordinator_assign} — periodically (a) aborts timed-out open transactions
 * ({@link #sweepTimeouts}) and (b) garbage-collects finalized transactions whose retention has
 * elapsed ({@link #sweepGc}). Concurrent sweeps from a stale owner are still safe — every state
 * transition is a header CAS — so the single-sweeper election is an efficiency measure, not a
 * correctness one.
 */
@CustomLog
public class TransactionCoordinatorV5 {

    private final PulsarService pulsar;
    private final TxnMetadataStore txnStore;

    private final long timeoutSweepIntervalMs;
    private final long gcSweepIntervalMs;
    private final long gcRetentionMs;
    private volatile ScheduledExecutorService sweepExecutor;
    private volatile boolean closed;
    private final AtomicBoolean timeoutSweepRunning = new AtomicBoolean(false);
    private final AtomicBoolean gcSweepRunning = new AtomicBoolean(false);

    public TransactionCoordinatorV5(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.txnStore = new TxnMetadataStore(pulsar.getLocalMetadataStore());
        var config = pulsar.getConfiguration();
        this.timeoutSweepIntervalMs = TimeUnit.SECONDS.toMillis(
                config.getTransactionCoordinatorScalableTopicsTimeoutSweepIntervalSeconds());
        this.gcSweepIntervalMs = TimeUnit.SECONDS.toMillis(
                config.getTransactionCoordinatorScalableTopicsGcIntervalSeconds());
        this.gcRetentionMs = TimeUnit.SECONDS.toMillis(
                config.getTransactionCoordinatorScalableTopicsGcRetentionSeconds());
    }

    // ---- Lifecycle --------------------------------------------------------

    /**
     * Start the periodic timeout / GC sweeps on a dedicated single-thread scheduler. Each tick is
     * gated by {@link #ifElectedSweeper} so only the partition-0 owner does the scan. Idempotent —
     * a second call is ignored.
     */
    public synchronized void start() {
        if (closed || sweepExecutor != null) {
            return;
        }
        sweepExecutor = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("pulsar-txn-v5-sweep"));
        sweepExecutor.scheduleWithFixedDelay(
                () -> runSweep("timeout", timeoutSweepRunning, this::sweepTimeouts),
                timeoutSweepIntervalMs, timeoutSweepIntervalMs, TimeUnit.MILLISECONDS);
        sweepExecutor.scheduleWithFixedDelay(
                () -> runSweep("gc", gcSweepRunning, this::sweepGc),
                gcSweepIntervalMs, gcSweepIntervalMs, TimeUnit.MILLISECONDS);
    }

    /** Stop the sweeps. Idempotent. */
    public synchronized void close() {
        closed = true;
        if (sweepExecutor != null) {
            sweepExecutor.shutdownNow();
            sweepExecutor = null;
        }
    }

    /**
     * Run one sweep cycle on the scheduler thread and block until it completes, so the
     * fixed-delay scheduling never overlaps two cycles. The {@code running} flag is a
     * defense-in-depth guard: the single-thread scheduler plus the blocking {@code get()} already
     * serialise cycles, but the flag makes overlap impossible even if the scheduling were later
     * changed (e.g. to a fixed-rate or multi-threaded executor). Errors are logged and swallowed —
     * the next tick retries.
     */
    private void runSweep(String name, AtomicBoolean running, Supplier<CompletableFuture<Void>> sweep) {
        if (closed || !running.compareAndSet(false, true)) {
            return;
        }
        try {
            sweep.get().get();
        } catch (InterruptedException ie) {
            // shutdownNow() interrupted the sweep thread mid-wait — restore the flag and exit
            // quietly; this is the expected shutdown signal, not a failure.
            Thread.currentThread().interrupt();
        } catch (Throwable t) {
            if (closed) {
                // close() raced with an in-flight async chain; not worth a WARN.
                return;
            }
            log.warn().attr("sweep", name).exception(t).log("v5 TC sweep cycle failed; will retry");
        } finally {
            running.set(false);
        }
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
            List<CompletableFuture<Void>> publishes = new ArrayList<>(
                    writeSegments.size() + ackParticipants.size());
            for (String segment : writeSegments) {
                publishes.add(txnStore.publishSegmentEvent(segment, event).thenApply(s -> null));
            }
            for (AckParticipant p : ackParticipants) {
                publishes.add(txnStore.publishSubscriptionEvent(p.segment(), p.subscription(), event)
                        .thenApply(s -> null));
            }
            return FutureUtil.waitForAll(publishes);
        });
    }

    // ---- Sweeps -----------------------------------------------------------

    /**
     * Abort transactions whose deadline has passed. Scans the by-deadline index up to "now" and
     * drives each through {@link #endTransaction} with {@code ABORT}, which re-reads and CAS-guards
     * the header — so a txn the client commits in the same window is left alone (the CAS loses and
     * the resulting InvalidTxnStatus / BadVersion is treated as a benign race).
     */
    CompletableFuture<Void> sweepTimeouts() {
        return ifElectedSweeper(() -> {
            long now = System.currentTimeMillis();
            List<TxnID> expired = Collections.synchronizedList(new ArrayList<>());
            return txnStore.listOpenByDeadlineRange(null, now, new ScanConsumer() {
                @Override
                public void onNext(GetResult r) {
                    String txnIdKey = TxnPaths.txnIdFromHeaderPath(r.getStat().getPath());
                    if (txnIdKey != null) {
                        expired.add(TxnIds.fromKey(txnIdKey));
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    log.warn().exception(throwable).log("Timeout-sweep deadline scan errored");
                }

                @Override
                public void onCompleted() {
                }
            }).thenCompose(__ -> {
                List<CompletableFuture<Void>> aborts = new ArrayList<>(expired.size());
                for (TxnID txnId : expired) {
                    aborts.add(endTransaction(txnId, TxnAction.ABORT_VALUE)
                            .exceptionally(ex -> {
                                // Benign: the client may have committed/aborted it between the scan
                                // and our CAS, or another sweeper got there first.
                                log.debug().attr("txnId", txnId).exception(ex)
                                        .log("Timeout-sweep abort skipped");
                                return null;
                            }));
                }
                return FutureUtil.waitForAll(aborts);
            });
        });
    }

    /**
     * Garbage-collect finalized transactions whose retention window has elapsed. For each terminal
     * state, scans the by-final-state index up to {@code now - retention} and applies
     * {@link #gcOneTxn}.
     */
    CompletableFuture<Void> sweepGc() {
        return ifElectedSweeper(() -> {
            long cutoff = System.currentTimeMillis() - gcRetentionMs;
            return gcFinalized(TxnState.COMMITTED, cutoff)
                    .thenCompose(__ -> gcFinalized(TxnState.ABORTED, cutoff));
        });
    }

    private CompletableFuture<Void> gcFinalized(TxnState state, long cutoffMs) {
        List<Versioned<String>> candidates = Collections.synchronizedList(new ArrayList<>());
        return txnStore.listFinalizedByStateAndTimeRange(state, null, cutoffMs, new ScanConsumer() {
            @Override
            public void onNext(GetResult r) {
                String txnIdKey = TxnPaths.txnIdFromHeaderPath(r.getStat().getPath());
                if (txnIdKey != null) {
                    candidates.add(new Versioned<>(txnIdKey, r.getStat().getVersion()));
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log.warn().attr("state", state).exception(throwable).log("GC-sweep scan errored");
            }

            @Override
            public void onCompleted() {
            }
        }).thenCompose(__ -> {
            List<CompletableFuture<Void>> gcs = new ArrayList<>(candidates.size());
            for (Versioned<String> c : candidates) {
                gcs.add(gcOneTxn(c.value(), c.version(), state));
            }
            return FutureUtil.waitForAll(gcs);
        });
    }

    /**
     * GC one finalized txn. If it still has {@code /txn/op} records, some participant hasn't applied
     * the outcome yet — or never received the event (e.g. the TC crashed between the header CAS and
     * the fan-out). Re-drive the fan-out and leave the header in place so the participant re-reads
     * the true outcome; it removes its op records once it applies them, and a later GC pass — seeing
     * no op records — deletes the header. We never delete a header while a participant might still
     * re-read it, so a committed txn's data is never stranded as "unknown".
     */
    private CompletableFuture<Void> gcOneTxn(String txnIdKey, long version, TxnState state) {
        TxnID txnId = TxnIds.fromKey(txnIdKey);
        boolean[] hasOps = {false};
        return txnStore.listOpsByTxn(txnIdKey, new ScanConsumer() {
            @Override
            public void onNext(GetResult r) {
                hasOps[0] = true;
            }

            @Override
            public void onError(Throwable throwable) {
                // Treat a scan error as "ops may exist" — safer to retry the repair than to delete.
                hasOps[0] = true;
                log.warn().attr("txnId", txnId).exception(throwable).log("GC-sweep op scan errored");
            }

            @Override
            public void onCompleted() {
            }
        }).thenCompose(__ -> {
            if (hasOps[0]) {
                return fanOutEvents(txnId, txnIdKey, state);
            }
            return txnStore.deleteHeader(txnIdKey, version).exceptionally(ex -> {
                // Benign: header changed or was already deleted since the scan.
                log.debug().attr("txnId", txnId).exception(ex).log("GC-sweep header delete skipped");
                return null;
            });
        });
    }

    /**
     * Run {@code action} only on the elected sweeper — the broker that owns partition 0 of
     * {@code transaction_coordinator_assign}. Not owning it (or any error checking ownership) means
     * "skip this cycle". Correctness doesn't depend on the election: every transition is a header
     * CAS, so a stale owner sweeping concurrently is harmless.
     */
    private CompletableFuture<Void> ifElectedSweeper(Supplier<CompletableFuture<Void>> action) {
        if (closed) {
            return CompletableFuture.completedFuture(null);
        }
        String assignPartition0 = SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN
                .getPartition(0).toString();
        return pulsar.getBrokerService().checkTopicNsOwnership(assignPartition0)
                .handle((v, ex) -> ex == null)
                .thenCompose(owned -> (owned && !closed)
                        ? action.get() : CompletableFuture.completedFuture(null));
    }

    /** A {@code (segment, subscription)} ack participant; keys the ack fan-out de-dup set. */
    private record AckParticipant(String segment, String subscription) {
    }
}
