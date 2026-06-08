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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.transaction.exception.coordinator.TransactionCoordinatorException;
import org.apache.pulsar.broker.transaction.metadata.TcLeader;
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
import org.apache.pulsar.metadata.api.coordination.LeaderElection;
import org.apache.pulsar.metadata.api.coordination.LeaderElectionState;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionSubscription;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException;

/**
 * Metadata-driven transaction coordinator for scalable topics — broker-side service.
 *
 * <p>Per-partition coordinator. Leadership rests on the metadata store directly: each TC
 * partition {@code N} has a {@link LeaderElection} at {@code /txn/tc/leader/<N>}, and a broker
 * runs the TC for partition {@code N} iff it currently leads that election. This removes the
 * dependency on the {@code transaction_coordinator_assign} topic and its bundle ownership — TC
 * coordination liveness no longer rides on the topic/namespace/load-balancer machinery, only on
 * the metadata store (which the TC already hard-depends on for every header read/write).
 *
 * <p><b>Distribution.</b> Every broker calls {@code elect()} on every partition (elect-all):
 * the {@code LeaderElection} primitive only fails a leader over to a broker that is already a
 * candidate, so to keep every partition survivable every broker must be a candidate for every
 * partition. The N independent elections start concurrently, so on a co-start leadership lands
 * roughly balanced across brokers, and every partition has B−1 standby candidates for instant
 * failover. (After a strictly sequential scale-up an early broker can hold more partitions until
 * it restarts; TC load is light, so v1 does not actively rebalance.)
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
 *   <li>{@code WATCH_TC_ASSIGNMENTS} → {@link #buildAssignmentsSnapshot} + push-on-change, the
 *       client's discovery surface (which broker leads which partition).</li>
 * </ul>
 *
 * <p>{@code endTransaction} CAS-updates the header to the terminal state, enumerates
 * {@code /txn/op/<txnId>-*} via {@link TxnPaths#IDX_OPS_BY_TXN}, and publishes one
 * segment-event per affected segment + one subscription-event per affected
 * {@code (segment, subscription)} pair. The fan-out is metadata-store writes (not RPCs) and
 * is bounded by the txn's participant count.
 *
 * <p>Background sweeps: the broker that leads partition 0 periodically (a) aborts timed-out open
 * transactions ({@link #sweepTimeouts}) and (b) garbage-collects finalized transactions whose
 * retention has elapsed ({@link #sweepGc}). Concurrent sweeps from a stale leader are still safe
 * — every state transition is a header CAS — so the single-sweeper election is an efficiency
 * measure, not a correctness one.
 */
@CustomLog
public class TransactionCoordinatorV5 {

    private final PulsarService pulsar;
    private final TxnMetadataStore txnStore;
    private final int partitionCount;

    private final long timeoutSweepIntervalMs;
    private final long gcSweepIntervalMs;
    private final long gcRetentionMs;
    private volatile ScheduledExecutorService sweepExecutor;
    private volatile boolean closed;
    private final AtomicBoolean timeoutSweepRunning = new AtomicBoolean(false);
    private final AtomicBoolean gcSweepRunning = new AtomicBoolean(false);

    /** Per-partition leader-election controllers, keyed by partition (0..partitionCount-1). */
    private final Map<Integer, LeaderElection<TcLeader>> elections = new ConcurrentHashMap<>();
    /** The local broker's election value — what we propose for every partition we lead. */
    private volatile TcLeader localLeader;
    /** Open assignment-watch listeners (one per watching client connection). */
    private final List<Runnable> assignmentChangeListeners = new CopyOnWriteArrayList<>();

    public TransactionCoordinatorV5(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.txnStore = new TxnMetadataStore(pulsar.getLocalMetadataStore());
        var config = pulsar.getConfiguration();
        this.partitionCount = config.getTransactionCoordinatorScalableTopicsParallelism();
        this.timeoutSweepIntervalMs = TimeUnit.SECONDS.toMillis(
                config.getTransactionCoordinatorScalableTopicsTimeoutSweepIntervalSeconds());
        this.gcSweepIntervalMs = TimeUnit.SECONDS.toMillis(
                config.getTransactionCoordinatorScalableTopicsGcIntervalSeconds());
        this.gcRetentionMs = TimeUnit.SECONDS.toMillis(
                config.getTransactionCoordinatorScalableTopicsGcRetentionSeconds());
    }

    // ---- Lifecycle --------------------------------------------------------

    /**
     * Start the coordinator: create a per-partition {@link LeaderElection} and {@code elect()} on
     * every partition (elect-all), then start the periodic timeout / GC sweeps on a dedicated
     * single-thread scheduler. Sweep ticks are gated by {@link #ifElectedSweeper} so only the
     * partition-0 leader scans. Idempotent — a second call is ignored.
     */
    public synchronized void start() {
        if (closed || sweepExecutor != null) {
            return;
        }
        verifyParallelismConsistency();
        this.localLeader = new TcLeader(pulsar.getBrokerId(), pulsar.getBrokerServiceUrl(),
                pulsar.getBrokerServiceUrlTls(), pulsar.getSafeWebServiceAddress());
        for (int partition = 0; partition < partitionCount; partition++) {
            final int p = partition;
            LeaderElection<TcLeader> election = pulsar.getCoordinationService().getLeaderElection(
                    TcLeader.class, TxnPaths.tcLeaderPath(p), state -> onElectionStateChange(p, state));
            elections.put(p, election);
            // elect-all: become a candidate for every partition so leadership is balanced across
            // brokers and every partition has standbys for failover. Errors are logged; the
            // LeaderElection retries internally.
            election.elect(localLeader).exceptionally(ex -> {
                log.warn().attr("partition", p).exception(ex).log("v5 TC initial elect failed");
                return null;
            });
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

    /**
     * Persist this broker's configured parallelism cluster-wide on first start, and verify every
     * subsequent broker agrees. A mismatch means brokers would run different election sets and the
     * coordinator-count encoded in transaction ids would be ambiguous — fatal misconfiguration, so
     * we fail fast rather than start in an inconsistent state.
     */
    private void verifyParallelismConsistency() {
        var store = pulsar.getLocalMetadataStore();
        try {
            byte[] value = Integer.toString(partitionCount).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            var existing = store.get(TxnPaths.TXN_TC_PARALLELISM_PATH).get();
            if (existing.isEmpty()) {
                // First broker to start writes the value (CAS create; lose harmlessly to a racing peer).
                store.put(TxnPaths.TXN_TC_PARALLELISM_PATH, value, java.util.Optional.of(-1L))
                        .get();
                var after = store.get(TxnPaths.TXN_TC_PARALLELISM_PATH).get();
                if (after.isPresent()) {
                    checkParallelismMatches(after.get().getValue());
                }
            } else {
                checkParallelismMatches(existing.get().getValue());
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            // A racing create (BadVersion) or read-after-write resolves by re-reading and comparing.
            try {
                var after = store.get(TxnPaths.TXN_TC_PARALLELISM_PATH).get();
                after.ifPresent(r -> checkParallelismMatches(r.getValue()));
            } catch (Exception ignore) {
                log.warn().exception(e).log("Could not verify TC parallelism consistency; proceeding");
            }
        }
    }

    private void checkParallelismMatches(byte[] storedValue) {
        int stored = Integer.parseInt(new String(storedValue, java.nio.charset.StandardCharsets.UTF_8).trim());
        if (stored != partitionCount) {
            throw new IllegalStateException(
                    "transactionCoordinatorScalableTopicsParallelism mismatch: this broker is configured"
                            + " with " + partitionCount + " but the cluster was initialized with " + stored
                            + ". The value is fixed at cluster bring-up and must be identical on every"
                            + " broker.");
        }
    }

    /** Stop the sweeps and release every leader-election lease. Idempotent. */
    public synchronized void close() {
        closed = true;
        if (sweepExecutor != null) {
            sweepExecutor.shutdownNow();
            sweepExecutor = null;
        }
        elections.values().forEach(e -> e.asyncClose().exceptionally(ex -> {
            log.warn().exception(ex).log("v5 TC election close failed");
            return null;
        }));
        elections.clear();
        assignmentChangeListeners.clear();
    }

    /**
     * Whether this broker currently leads TC partition {@code partition}. Used to gate
     * client-connect acceptance and the sweep. A partition with no local election (out-of-range or
     * pre-{@code start()}) is not led here.
     */
    public boolean isLeaderFor(int partition) {
        LeaderElection<TcLeader> election = elections.get(partition);
        return election != null && election.getState() == LeaderElectionState.Leading;
    }

    /** Fire every assignment-watch listener — a leader changed somewhere, so the map moved. */
    private void onElectionStateChange(int partition, LeaderElectionState state) {
        log.debug().attr("partition", partition).attr("state", state).log("v5 TC election state changed");
        for (Runnable listener : assignmentChangeListeners) {
            try {
                listener.run();
            } catch (Throwable t) {
                log.warn().exception(t).log("v5 TC assignment listener failed");
            }
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
     * Verify this broker may coordinate {@code tcId}. A new client reaches us via the assignment
     * watch, so we are the metadata-store election leader for that partition. An old client (no
     * assignment-watch support) reaches us via an assign-topic lookup, so we own that partition's
     * assign-topic bundle. Accept either: both are correctness-safe because every transaction
     * state transition is a metadata-store CAS, so even a stale router can't corrupt state. We
     * accept-if-leader first (cheap, in-memory) and fall back to the assign-topic ownership check
     * only when we're not the election leader.
     */
    public CompletableFuture<Void> handleClientConnect(TransactionCoordinatorID tcId) {
        if (isLeaderFor((int) tcId.getId())) {
            return CompletableFuture.completedFuture(null);
        }
        String assignPartition = SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN
                .getPartition((int) tcId.getId()).toString();
        return pulsar.getBrokerService().checkTopicNsOwnership(assignPartition);
    }

    // ---- Assignment discovery (client watch) ------------------------------

    /**
     * Build the current full {@code partition → leader} snapshot from the election state. Uses the
     * async {@link LeaderElection#getLeaderValue()} (which loads from the metadata store on a cache
     * miss) rather than the cache-only {@code getLeaderValueIfPresent()}: when this broker just
     * transitioned to {@code Following} for a partition, its local cache for the new leader's node
     * may not be repopulated yet, and a cache-only read would silently omit that partition. Loading
     * from the store closes that window so a follower's snapshot is still complete.
     *
     * <p>A partition still genuinely without a leader (no broker elected yet) is omitted; the caller
     * ({@code ServerCnx}) re-pushes shortly after so the client isn't stranded. Always the complete
     * map — the watch protocol sends full snapshots, never diffs.
     *
     * @return a future of the snapshot plus whether it is complete (every partition has a leader)
     */
    public CompletableFuture<TcAssignmentsSnapshot> buildAssignmentsSnapshot() {
        Map<Integer, TcLeader> assignments = new ConcurrentSkipListMap<>();
        List<CompletableFuture<Void>> loads = new ArrayList<>(elections.size());
        for (Map.Entry<Integer, LeaderElection<TcLeader>> e : elections.entrySet()) {
            int partition = e.getKey();
            loads.add(e.getValue().getLeaderValue()
                    .thenAccept(opt -> opt.ifPresent(leader -> assignments.put(partition, leader)))
                    .exceptionally(ex -> {
                        // Treat a load error as "leader unknown for now"; the re-push will retry.
                        log.debug().attr("partition", partition).exception(ex)
                                .log("v5 TC leader-value load failed while building snapshot");
                        return null;
                    }));
        }
        return FutureUtil.waitForAll(loads)
                .thenApply(__ -> new TcAssignmentsSnapshot(partitionCount, new TreeMap<>(assignments)));
    }

    /**
     * Register a listener fired whenever the assignment map may have changed (any partition's
     * leadership moved). Returns an {@link AutoCloseable} that deregisters it — the
     * {@code ServerCnx} closes it when the client closes the watch or disconnects.
     */
    public AutoCloseable registerAssignmentChangeListener(Runnable listener) {
        assignmentChangeListeners.add(listener);
        return () -> assignmentChangeListeners.remove(listener);
    }

    /** Immutable full assignment snapshot: partition count + the currently-known leaders. */
    public record TcAssignmentsSnapshot(int partitionCount, Map<Integer, TcLeader> assignments) {
        /** @return true if every partition has a known leader (no mid-election gaps). */
        public boolean isComplete() {
            return assignments.size() == partitionCount;
        }
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
     * Run {@code action} only on the elected sweeper — the broker that leads TC partition 0. Not
     * leading it means "skip this cycle". Correctness doesn't depend on the election: every
     * transition is a header CAS, so a stale leader sweeping concurrently is harmless.
     */
    private CompletableFuture<Void> ifElectedSweeper(Supplier<CompletableFuture<Void>> action) {
        if (closed || !isLeaderFor(0)) {
            return CompletableFuture.completedFuture(null);
        }
        return action.get();
    }

    /** A {@code (segment, subscription)} ack participant; keys the ack fan-out de-dup set. */
    private record AckParticipant(String segment, String subscription) {
    }
}
