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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.AbortedTxnProcessor;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferReader;
import org.apache.pulsar.broker.transaction.buffer.TransactionMeta;
import org.apache.pulsar.broker.transaction.metadata.AbortedTxnRecord;
import org.apache.pulsar.broker.transaction.metadata.SegmentWatermark;
import org.apache.pulsar.broker.transaction.metadata.TxnHeader;
import org.apache.pulsar.broker.transaction.metadata.TxnIds;
import org.apache.pulsar.broker.transaction.metadata.TxnMetadataStore;
import org.apache.pulsar.broker.transaction.metadata.TxnOp;
import org.apache.pulsar.broker.transaction.metadata.TxnOpKind;
import org.apache.pulsar.broker.transaction.metadata.TxnPaths;
import org.apache.pulsar.broker.transaction.metadata.TxnState;
import org.apache.pulsar.broker.transaction.metadata.Versioned;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.policies.data.TransactionBufferStats;
import org.apache.pulsar.common.policies.data.TransactionInBufferStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.Runnables;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.ScanConsumer;

/**
 * {@link TransactionBuffer} for {@code segment://} topics that reads truth from the
 * metadata-store transaction layout (PIP-473).
 *
 * <p><b>Publish-path ordering.</b> {@link #appendBufferToTxn} writes the {@code /txn/op} record
 * <em>before</em> the managed-ledger append. This eliminates the orphan class — a crash between
 * the two writes leaves either (a) no entry and no op record, or (b) an op record with no entry
 * (TC times out → ABORTED → cleanup). There's never a ledger entry with no op record. The invariant
 * <i>every transactional entry in the segment has a corresponding {@code /txn/op} record at the
 * time of append</i> lets recovery scan {@code /txn/op} authoritatively without segment-replay.
 *
 * <p><b>Durable visibility state.</b> The TB persists a per-segment
 * {@code /txn/segment-state/<segment>/watermark} record (the resolved-below mark) plus one
 * {@code aborted/<txnId>} record per aborted txn with still-readable data. Together they let
 * {@link #isTxnAborted} answer correctly for as long as the data is in the segment ML, even after
 * the original {@code /txn/id/<txnId>} headers have been GC'd. The aborted records carry the txn's
 * max position via a secondary index so the TB can range-delete them when the segment ML trims its
 * older data.
 *
 * <p><b>Recovery.</b> Load the durable watermark + aborted set; scan {@code /txn/op} for this
 * segment to discover any txns still open at the time of the previous shutdown (their first
 * positions aren't known — the in-memory watermark stays pinned at the durable value until they
 * resolve); subscribe to the segment-event stream.
 *
 * <p><b>TC ordering contract.</b> There is a TOCTOU window between the header authorization read
 * and the {@code /txn/op} write: the TC may flip the header (commit or abort) in between. The op
 * record still lands. On commit that's harmless. On abort, the subsequent segment-event delivery
 * marks the txn ABORTED and the per-txn aborted record is written before the op record is deleted,
 * so {@link #isTxnAborted} filters subsequent reads. This relies on the TC publishing the segment
 * event <em>after</em> the header CAS.
 */
@CustomLog
public class MetadataTransactionBuffer implements TransactionBuffer {

    private final PersistentTopic topic;
    private final ManagedLedger ledger;
    private final TxnMetadataStore txnStore;
    private final String segmentName;
    private final TopicTransactionBuffer.MaxReadPositionCallBack maxReadPositionCallBack;

    private final CompletableFuture<Void> recoveryFuture = new CompletableFuture<>();
    private volatile AutoCloseable subscription;
    private volatile boolean closed;

    /** Guards mutable state below. */
    private final Object lock = new Object();

    /** In-memory per-txn state for txns this segment is involved in. */
    private final Map<String, TxnEntry> txns = new HashMap<>();

    /** Aborted-txn set hydrated from durable state at recovery, updated on abort-apply. */
    private final Set<String> abortedTxns = new HashSet<>();

    /** Count of OPEN txns we discovered at recovery whose first position we don't know. */
    private int recoveryDiscoveredOpen;

    /** Durable watermark, mirrored in memory. May be null on a fresh segment. */
    private SegmentWatermark watermark;
    /** Version of the durable watermark record; -1 if it doesn't exist yet. */
    private long watermarkVersion = -1L;

    /** Current maxReadPosition; never moves above the watermark while recovery-discovered opens exist. */
    private Position maxReadPosition;

    /**
     * Serialised chain for watermark-persist + op-record-cleanup. Each apply enqueues itself on the
     * tail so we never have two in-flight watermark CASes racing.
     */
    private CompletableFuture<Void> stateTail = CompletableFuture.completedFuture(null);

    private final LongAdder committedCount = new LongAdder();
    private final LongAdder abortedCount = new LongAdder();

    /** Periodic task that range-deletes aborted-txn records once the segment ML trims past them. */
    private final ScheduledFuture<?> abortedGcTask;
    /** Guards against a new GC cycle starting while the previous async one is still in flight. */
    private final AtomicBoolean gcRunning = new AtomicBoolean(false);

    public MetadataTransactionBuffer(PersistentTopic topic, TxnMetadataStore txnStore) {
        this.topic = topic;
        this.ledger = topic.getManagedLedger();
        this.txnStore = txnStore;
        this.segmentName = topic.getName();
        this.maxReadPositionCallBack = topic.getMaxReadPositionCallBack();
        this.maxReadPosition = ledger.getLastConfirmedEntry();
        recover();
        this.abortedGcTask = scheduleAbortedGc();
    }

    /**
     * Schedule the periodic aborted-record GC on the broker executor. Returns {@code null} when no
     * executor is reachable (e.g. a unit test with a mocked topic); such callers drive
     * {@link #pruneTrimmedAbortedTxns()} directly.
     */
    private ScheduledFuture<?> scheduleAbortedGc() {
        ScheduledExecutorService executor = brokerExecutor();
        if (executor == null) {
            return null;
        }
        long intervalSeconds = Math.max(1, topic.getBrokerService().getPulsar().getConfiguration()
                .getTransactionCoordinatorScalableTopicsGcIntervalSeconds());
        long intervalMs = TimeUnit.SECONDS.toMillis(intervalSeconds);
        // Wrap in catchingAndLoggingThrowables so an unexpected RuntimeException doesn't cancel the
        // fixed-delay schedule. The gcRunning guard skips a cycle while the previous async sweep is
        // still in flight (slow metadata store) rather than overlapping sweeps.
        return executor.scheduleWithFixedDelay(Runnables.catchingAndLoggingThrowables(() -> {
            if (closed || !gcRunning.compareAndSet(false, true)) {
                return;
            }
            CompletableFuture<Void> sweep;
            try {
                sweep = pruneTrimmedAbortedTxns();
            } catch (Throwable t) {
                gcRunning.set(false);
                throw t;
            }
            sweep.whenComplete((__, ex) -> {
                gcRunning.set(false);
                if (ex != null) {
                    log.warn().attr("segment", segmentName).exception(ex)
                            .log("Aborted-txn GC sweep failed; will retry next cycle");
                }
            });
        }), intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    private ScheduledExecutorService brokerExecutor() {
        try {
            if (topic.getBrokerService() != null && topic.getBrokerService().getPulsar() != null) {
                return topic.getBrokerService().getPulsar().getExecutor();
            }
        } catch (Throwable t) {
            // Mocked topic in unit tests — no broker executor; GC is driven directly.
        }
        return null;
    }

    // ---- Recovery ----------------------------------------------------------

    private void recover() {
        AutoCloseable handle;
        try {
            handle = txnStore.subscribeSegmentEvents(segmentName, path -> triggerReconcile());
        } catch (MetadataStoreException e) {
            recoveryFuture.completeExceptionally(e);
            return;
        }
        subscription = handle;

        // 1. Load durable watermark.
        CompletableFuture<Void> watermarkLoad = txnStore.getSegmentWatermark(segmentName)
                .thenAccept(opt -> {
                    if (opt.isPresent()) {
                        synchronized (lock) {
                            watermark = opt.get().value();
                            watermarkVersion = opt.get().version();
                            // Initialise maxReadPosition at the watermark so we don't expose anything
                            // above it until in-memory state catches up.
                            maxReadPosition = PositionFactory.create(
                                    watermark.ledgerId(), watermark.entryId());
                        }
                    }
                });

        // 2. Load the aborted-txn set (scan by segment-scoped index range).
        CompletableFuture<Void> abortedLoad = watermarkLoad.thenCompose(__ ->
                txnStore.scanAbortedTxns(segmentName,
                        TxnPaths.abortedByPositionSegmentLowerBound(segmentName),
                        TxnPaths.abortedByPositionSegmentUpperBound(segmentName),
                        new ScanConsumer() {
                            @Override
                            public void onNext(GetResult r) {
                                String txnIdKey = TxnPaths.txnIdFromAbortedPath(r.getStat().getPath());
                                if (txnIdKey != null) {
                                    synchronized (lock) {
                                        abortedTxns.add(txnIdKey);
                                    }
                                }
                            }

                            @Override
                            public void onError(Throwable throwable) {
                                log.warn().attr("segment", segmentName).exception(throwable)
                                        .log("Aborted-txn scan errored during recovery");
                            }

                            @Override
                            public void onCompleted() {
                            }
                        }));

        // 3. Scan /txn/op for this segment to discover txns that were open at last shutdown.
        Map<String, Boolean> writeOpsByTxn = new ConcurrentHashMap<>();
        CompletableFuture<Void> opsLoad = abortedLoad.thenCompose(__ ->
                txnStore.listWritesBySegment(segmentName, new ScanConsumer() {
                    @Override
                    public void onNext(GetResult r) {
                        String txnIdKey = TxnPaths.txnIdFromOpPath(r.getStat().getPath());
                        if (txnIdKey != null) {
                            writeOpsByTxn.put(txnIdKey, Boolean.TRUE);
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        log.warn().attr("segment", segmentName).exception(throwable)
                                .log("Op-scan errored during recovery");
                    }

                    @Override
                    public void onCompleted() {
                    }
                }));

        // 4. For each discovered txn, fetch its header and seed the in-memory state.
        opsLoad.thenCompose(__ -> {
            List<CompletableFuture<Void>> reads = new ArrayList<>();
            writeOpsByTxn.keySet().forEach(txnIdKey -> reads.add(
                    txnStore.getHeader(txnIdKey).thenAccept(opt ->
                            applyHeaderForRecovery(txnIdKey, opt))));
            return FutureUtil.waitForAll(reads);
        })
        .whenComplete((__, err) -> {
            if (err != null) {
                log.error().attr("segment", segmentName).exception(err).log("TB recovery failed");
                closeSubscriptionQuietly();
                recoveryFuture.completeExceptionally(err);
                return;
            }
            synchronized (lock) {
                recomputeMaxReadPositionLocked();
            }
            recoveryFuture.complete(null);
            // Drain any events that fired between subscribe and now.
            triggerReconcile();
        });
    }

    private void applyHeaderForRecovery(String txnIdKey, Optional<Versioned<TxnHeader>> opt) {
        TxnState state = opt.map(v -> v.value().getState()).orElse(TxnState.ABORTED);
        synchronized (lock) {
            TxnEntry entry = new TxnEntry(state);
            if (state == TxnState.OPEN) {
                entry.recoveryDiscovered = true;
                recoveryDiscoveredOpen++;
            } else if (state == TxnState.ABORTED) {
                // Hydrate the aborted set now, under the lock, so isTxnAborted is correct the
                // instant recoveryFuture completes. The terminal apply that would otherwise add
                // this txn runs later on stateTail (after recovery completes), leaving a window
                // in which the txn's data — which isn't watermark-pinned, since ABORTED entries
                // don't set recoveryDiscovered — would read as visible. add() is idempotent with
                // applyTerminalNow's aborted-set update.
                abortedTxns.add(txnIdKey);
            }
            txns.put(txnIdKey, entry);
        }
        // Terminal txns with leftover /txn/op records still need their outcome materialised and
        // the records cleaned up — enqueue an apply.
        if (state.isTerminal()) {
            enqueueApplyTerminal(txnIdKey, state);
        }
    }

    // ---- Publish path ------------------------------------------------------

    @Override
    public CompletableFuture<Position> appendBufferToTxn(TxnID txnId, long sequenceId, ByteBuf buffer) {
        buffer.retain();
        return recoveryFuture.thenCompose(__ -> internalAppend(txnId, buffer))
                .whenComplete((p, ex) -> buffer.release());
    }

    private CompletableFuture<Position> internalAppend(TxnID txnId, ByteBuf buffer) {
        if (closed) {
            return FutureUtil.failedFuture(new BrokerServiceException.ServiceUnitNotReadyException(
                    "Transaction buffer is closed"));
        }
        String txnIdKey = TxnIds.toKey(txnId);
        return readStateCacheFirst(txnIdKey)
                .thenCompose(state -> {
                    if (state.isTerminal()) {
                        return FutureUtil.failedFuture(new BrokerServiceException.NotAllowedException(
                                "Transaction " + txnId + " is already " + state + " — TxnConflict"));
                    }
                    // 1. Write /txn/op first. Positions are unknown at this point; the WRITE-kind
                    // record uses sentinel 0/0 (positions live in the TB's in-memory tracking).
                    TxnOp op = new TxnOp(TxnOpKind.WRITE, segmentName, null, 0L, 0L, null);
                    return txnStore.appendOp(txnIdKey, op).thenCompose(opStat ->
                            // 2. ML append. By the invariant above, /txn/op is durable before any
                            // ledger entry exists for this op.
                            appendToLedger(buffer).thenApply(position -> {
                                trackPosition(txnIdKey, position);
                                return position;
                            }));
                });
    }

    private CompletableFuture<TxnState> readStateCacheFirst(String txnIdKey) {
        synchronized (lock) {
            TxnEntry cached = txns.get(txnIdKey);
            if (cached != null) {
                return CompletableFuture.completedFuture(cached.state);
            }
        }
        return txnStore.getHeader(txnIdKey).thenApply(opt -> {
            TxnState state = opt.map(v -> v.value().getState()).orElse(TxnState.ABORTED);
            synchronized (lock) {
                txns.putIfAbsent(txnIdKey, new TxnEntry(state));
            }
            return state;
        });
    }

    private CompletableFuture<Position> appendToLedger(ByteBuf buffer) {
        CompletableFuture<Position> result = new CompletableFuture<>();
        ledger.asyncAddEntry(buffer, new AsyncCallbacks.AddEntryCallback() {
            @Override
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                result.complete(position);
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, null);
        return result;
    }

    private void trackPosition(String txnIdKey, Position position) {
        synchronized (lock) {
            TxnEntry entry = txns.get(txnIdKey);
            // Only an OPEN, non-recovery entry tracks positions. Recovery-discovered entries pin
            // at the watermark and shouldn't be re-keyed by later appends (we don't know the
            // earliest position).
            if (entry == null || entry.state != TxnState.OPEN || entry.recoveryDiscovered) {
                return;
            }
            if (entry.firstPosition == null || position.compareTo(entry.firstPosition) < 0) {
                entry.firstPosition = position;
            }
            if (entry.lastPosition == null || position.compareTo(entry.lastPosition) > 0) {
                entry.lastPosition = position;
            }
            recomputeMaxReadPositionLocked();
        }
    }

    // ---- Reconcile (event-driven) -----------------------------------------

    private void triggerReconcile() {
        if (closed || !recoveryFuture.isDone() || recoveryFuture.isCompletedExceptionally()) {
            return;
        }
        Set<String> snapshot;
        synchronized (lock) {
            snapshot = new HashSet<>();
            for (Map.Entry<String, TxnEntry> e : txns.entrySet()) {
                if (e.getValue().state == TxnState.OPEN) {
                    snapshot.add(e.getKey());
                }
            }
        }
        if (snapshot.isEmpty()) {
            return;
        }
        List<CompletableFuture<Void>> reads = new ArrayList<>(snapshot.size());
        for (String txnIdKey : snapshot) {
            reads.add(txnStore.getHeader(txnIdKey).thenAccept(opt -> {
                TxnState newState = opt.map(v -> v.value().getState()).orElse(TxnState.ABORTED);
                if (newState.isTerminal()) {
                    enqueueApplyTerminal(txnIdKey, newState);
                }
            }));
        }
        FutureUtil.waitForAll(reads).whenComplete((__, err) -> {
            if (err != null) {
                log.warn().attr("segment", segmentName).exception(err).log("Reconcile encountered error");
            }
        });
    }

    /**
     * Enqueue the durable side-effects for a txn that has been observed terminal: write the
     * aborted record (if any), advance the persisted watermark, delete the {@code /txn/op}
     * records. Serialised through {@link #stateTail} so concurrent applies don't race the
     * watermark CAS.
     */
    private void enqueueApplyTerminal(String txnIdKey, TxnState newState) {
        synchronized (lock) {
            stateTail = stateTail.thenCompose(__ -> applyTerminalNow(txnIdKey, newState))
                    .exceptionally(err -> {
                        log.warn().attr("segment", segmentName).attr("txnId", txnIdKey)
                                .exception(err).log("Apply-terminal failed; will retry on next reconcile");
                        return null;
                    });
        }
    }

    private CompletableFuture<Void> applyTerminalNow(String txnIdKey, TxnState newState) {
        // Snapshot the entry under the lock — we need its positions / recovery flag to decide
        // what to persist.
        boolean alreadyTerminal;
        Position lastPos;
        synchronized (lock) {
            TxnEntry entry = txns.get(txnIdKey);
            if (entry == null) {
                return CompletableFuture.completedFuture(null);
            }
            alreadyTerminal = entry.state.isTerminal();
            lastPos = entry.lastPosition;
            // Mark in-memory now so subsequent appendBufferToTxn for this txn fail with TxnConflict.
            if (!alreadyTerminal) {
                entry.state = newState;
                if (entry.recoveryDiscovered) {
                    recoveryDiscoveredOpen = Math.max(0, recoveryDiscoveredOpen - 1);
                    entry.recoveryDiscovered = false;
                }
                if (newState == TxnState.COMMITTED) {
                    committedCount.increment();
                } else if (newState == TxnState.ABORTED) {
                    abortedCount.increment();
                    abortedTxns.add(txnIdKey);
                }
                recomputeMaxReadPositionLocked();
            } else if (newState == TxnState.ABORTED) {
                // Idempotent path — header re-confirms ABORTED. Make sure the in-memory set holds
                // it (e.g. after an in-memory rebuild that lost the set).
                abortedTxns.add(txnIdKey);
            }
            // Drop the now-terminal entry so the cache stays bounded by the open-txn count rather
            // than growing for the segment's lifetime. This is safe: recomputeMaxReadPositionLocked
            // only consults OPEN entries, and isTxnAborted reads the separate abortedTxns set (an
            // aborted txn stays there, a committed/unknown one correctly reads as visible). The
            // positions needed for the durable side-effects below were already captured above.
            txns.remove(txnIdKey);
        }

        // Persist aborted record if this is an abort.
        CompletableFuture<Void> persistAborted = (newState == TxnState.ABORTED)
                ? persistAbortedRecord(txnIdKey, lastPos)
                : CompletableFuture.completedFuture(null);

        return persistAborted
                .thenCompose(__ -> persistWatermarkIfAdvanced())
                .thenCompose(__ -> txnStore.deleteWriteOpsForSegmentAndTxn(segmentName, txnIdKey));
    }

    /**
     * Write {@code /txn/segment-state/<segment>/aborted/<txnId>} with the txn's max position in
     * this segment. The stored position is the prune key: trim-driven pruning drops an aborted
     * record once the segment trims past it, so it must be at least as high as the txn's highest
     * data position, or the record would be dropped while its data is still readable.
     *
     * <p>When the positions are unknown (a recovery-discovered txn with no new appends) we fall
     * back to the current segment LAC. The txn's data was written in a prior epoch, so it cannot
     * sit above the LAC — that makes the LAC a correct conservative upper bound. The durable
     * watermark would be wrong here: the txn's data sits <em>above</em> the watermark, so pruning
     * keyed on the watermark would discard the record too early.
     */
    private CompletableFuture<Void> persistAbortedRecord(String txnIdKey, Position lastPos) {
        long maxLedger;
        long maxEntry;
        if (lastPos != null) {
            maxLedger = lastPos.getLedgerId();
            maxEntry = lastPos.getEntryId();
        } else {
            Position lac = ledger.getLastConfirmedEntry();
            maxLedger = lac == null ? 0L : lac.getLedgerId();
            maxEntry = lac == null ? 0L : lac.getEntryId();
        }
        return txnStore.putAbortedTxn(segmentName, txnIdKey, maxLedger, maxEntry).thenApply(s -> null);
    }

    /**
     * If the in-memory watermark position has advanced beyond the durable one, CAS-write the new
     * value. Idempotent and self-skipping when there's nothing to do.
     */
    private CompletableFuture<Void> persistWatermarkIfAdvanced() {
        SegmentWatermark toWrite;
        long expectedVersion;
        synchronized (lock) {
            Position desired = maxReadPosition;
            if (desired == null) {
                return CompletableFuture.completedFuture(null);
            }
            if (watermark != null
                    && watermark.ledgerId() == desired.getLedgerId()
                    && watermark.entryId() == desired.getEntryId()) {
                return CompletableFuture.completedFuture(null);
            }
            // Only advance forward.
            if (watermark != null) {
                Position existing = PositionFactory.create(watermark.ledgerId(), watermark.entryId());
                if (desired.compareTo(existing) <= 0) {
                    return CompletableFuture.completedFuture(null);
                }
            }
            toWrite = new SegmentWatermark(desired.getLedgerId(), desired.getEntryId());
            expectedVersion = watermarkVersion;
        }
        Optional<Long> expected = Optional.of(expectedVersion);
        return txnStore.casSegmentWatermark(segmentName, toWrite, expected)
                .thenAccept(stat -> {
                    synchronized (lock) {
                        watermark = toWrite;
                        watermarkVersion = stat.getVersion();
                    }
                })
                .exceptionallyCompose(ex -> {
                    Throwable cause = FutureUtil.unwrapCompletionException(ex);
                    if (cause instanceof MetadataStoreException.BadVersionException) {
                        // Our in-memory version is stale (a concurrent writer moved the record).
                        // Re-read so the next enqueued apply CASes against the current version
                        // instead of looping on the stale one. Still propagate this failure so the
                        // caller logs+retries; the retry now has a fresh version to work with.
                        return txnStore.getSegmentWatermark(segmentName).thenAccept(opt -> {
                            if (opt.isPresent()) {
                                synchronized (lock) {
                                    watermark = opt.get().value();
                                    watermarkVersion = opt.get().version();
                                }
                            }
                        }).thenCompose(__ -> FutureUtil.failedFuture(cause));
                    }
                    return FutureUtil.failedFuture(cause);
                });
    }

    // ---- maxReadPosition ---------------------------------------------------

    private void recomputeMaxReadPositionLocked() {
        Position next;
        Position watermarkPos = (watermark == null) ? null
                : PositionFactory.create(watermark.ledgerId(), watermark.entryId());

        if (recoveryDiscoveredOpen > 0) {
            // Pinned at the durable watermark while any recovery-discovered open txn remains:
            // we don't know their first positions and mustn't advance past them.
            next = watermarkPos != null ? watermarkPos : maxReadPosition;
        } else {
            Position min = null;
            boolean anyOpen = false;
            for (TxnEntry e : txns.values()) {
                if (e.state == TxnState.OPEN) {
                    anyOpen = true;
                    if (e.firstPosition != null
                            && (min == null || e.firstPosition.compareTo(min) < 0)) {
                        min = e.firstPosition;
                    }
                }
            }
            if (min != null) {
                // Pin just below the lowest open txn's first write.
                next = ledger.getPreviousPosition(min);
            } else if (anyOpen) {
                // Open txn(s) whose first write isn't tracked yet (an append is in flight between
                // the /txn/op record and the ledger entry): hold the current ceiling rather than
                // risk exposing the in-flight entry.
                next = maxReadPosition;
            } else {
                // No open txns: every written entry is resolved — committed data is visible and
                // aborted data is filtered by isTxnAborted — so advance to the last written entry.
                next = ledger.getLastConfirmedEntry();
            }
        }
        Position prev = maxReadPosition;
        maxReadPosition = next;
        if (next.compareTo(prev) > 0 && maxReadPositionCallBack != null) {
            maxReadPositionCallBack.maxReadPositionMovedForward(prev, next);
        }
    }

    // ---- SPI surface -------------------------------------------------------

    @Override
    public Position getMaxReadPosition() {
        synchronized (lock) {
            return maxReadPosition;
        }
    }

    @Override
    public boolean isTxnAborted(TxnID txnID, Position readPosition) {
        String key = TxnIds.toKey(txnID);
        synchronized (lock) {
            // New semantics (P3.5): default is committed/visible; only txns explicitly in the
            // aborted set are filtered. maxReadPosition caps what the dispatcher delivers — at the
            // lowest open txn's first write in steady state, or pinned at the watermark while
            // recovery-discovered opens remain — so reads above that cap don't reach this check.
            return abortedTxns.contains(key);
        }
    }

    @Override
    public void syncMaxReadPositionForNormalPublish(Position position, boolean isMarkerMessage) {
        if (isMarkerMessage) {
            return;
        }
        topic.updateLastDispatchablePosition(position);
        synchronized (lock) {
            recomputeMaxReadPositionLocked();
            // Persist the new watermark if it advanced as a result of the non-txn append.
            stateTail = stateTail.thenCompose(__ -> persistWatermarkIfAdvanced())
                    .exceptionally(err -> {
                        log.warn().attr("segment", segmentName).exception(err)
                                .log("Watermark persist on normal publish failed; will retry");
                        return null;
                    });
        }
    }

    @Override
    public CompletableFuture<Void> checkIfTBRecoverCompletely() {
        return recoveryFuture;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        closed = true;
        if (abortedGcTask != null) {
            abortedGcTask.cancel(false);
        }
        closeSubscriptionQuietly();
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Range-delete aborted-txn records — and drop their in-memory {@link #abortedTxns} entries —
     * whose highest position in this segment is below the ML's first still-valid position, i.e. whose
     * data has been fully trimmed. Safe because a trimmed position is never dispatched, so its abort
     * filtering is no longer needed; records for still-readable data (max position at or above the
     * first valid position) are retained. Without this the durable aborted set and the heap set grow
     * for the segment's whole lifetime even as the underlying data is trimmed away.
     */
    @VisibleForTesting
    CompletableFuture<Void> pruneTrimmedAbortedTxns() {
        if (closed) {
            return CompletableFuture.completedFuture(null);
        }
        Position firstValid = ledger.getFirstPosition();
        if (firstValid == null) {
            return CompletableFuture.completedFuture(null);
        }
        List<String> toPrune = Collections.synchronizedList(new ArrayList<>());
        return txnStore.scanAbortedTxns(segmentName,
                TxnPaths.abortedByPositionSegmentLowerBound(segmentName),
                TxnPaths.abortedByPositionSegmentUpperBound(segmentName),
                new ScanConsumer() {
                    @Override
                    public void onNext(GetResult r) {
                        String txnIdKey = TxnPaths.txnIdFromAbortedPath(r.getStat().getPath());
                        if (txnIdKey == null) {
                            return;
                        }
                        AbortedTxnRecord rec = TxnMetadataStore.fromJson(r.getValue(), AbortedTxnRecord.class);
                        Position maxPos = PositionFactory.create(rec.maxLedgerId(), rec.maxEntryId());
                        // Strictly below the first valid position → fully trimmed (conservative).
                        if (maxPos.compareTo(firstValid) < 0) {
                            toPrune.add(txnIdKey);
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        log.warn().attr("segment", segmentName).exception(throwable)
                                .log("Aborted-txn GC scan errored");
                    }

                    @Override
                    public void onCompleted() {
                    }
                }).thenCompose(__ -> {
                    if (toPrune.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    synchronized (lock) {
                        toPrune.forEach(abortedTxns::remove);
                    }
                    List<CompletableFuture<Void>> deletes = new ArrayList<>(toPrune.size());
                    for (String txnIdKey : toPrune) {
                        deletes.add(txnStore.deleteAbortedTxn(segmentName, txnIdKey));
                    }
                    return FutureUtil.waitForAll(deletes);
                });
    }

    private void closeSubscriptionQuietly() {
        AutoCloseable handle = subscription;
        if (handle == null) {
            return;
        }
        subscription = null;
        try {
            handle.close();
        } catch (Throwable t) {
            log.warn().attr("segment", segmentName).exception(t).log("Failed to close subscription");
        }
    }

    @Override
    public CompletableFuture<Void> commitTxn(TxnID txnID, long lowWaterMark) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> abortTxn(TxnID txnID, long lowWaterMark) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<TransactionMeta> getTransactionMeta(TxnID txnID) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<TransactionBufferReader> openTransactionBufferReader(TxnID txnID, long startSequenceId) {
        return FutureUtil.failedFuture(new BrokerServiceException.NotAllowedException(
                "openTransactionBufferReader is not supported on segment topics"));
    }

    @Override
    public CompletableFuture<Void> purgeTxns(List<Long> dataLedgers) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> clearSnapshot() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> clearSnapshotAndClose() {
        return closeAsync();
    }

    @Override
    public AbortedTxnProcessor.SnapshotType getSnapshotType() {
        return null;
    }

    @Override
    public TransactionInBufferStats getTransactionInBufferStats(TxnID txnID) {
        return null;
    }

    @Override
    public TransactionBufferStats getStats(boolean lowWaterMarks, boolean segmentStats) {
        return null;
    }

    @Override
    public TransactionBufferStats getStats(boolean lowWaterMarks) {
        return null;
    }

    @Override
    public long getOngoingTxnCount() {
        synchronized (lock) {
            long n = 0;
            for (TxnEntry e : txns.values()) {
                if (e.state == TxnState.OPEN) {
                    n++;
                }
            }
            return n;
        }
    }

    /** Size of the in-memory per-txn cache; bounded by the open-txn count once terminals are pruned. */
    @VisibleForTesting
    int trackedTxnCount() {
        synchronized (lock) {
            return txns.size();
        }
    }

    @Override
    public long getAbortedTxnCount() {
        return abortedCount.sum();
    }

    @Override
    public long getCommittedTxnCount() {
        return committedCount.sum();
    }

    // ---- helpers ----------------------------------------------------------

    private static final class TxnEntry {
        TxnState state;
        /** Earliest position the TB itself has seen on this segment for this txn. */
        Position firstPosition;
        /** Latest position the TB itself has seen on this segment for this txn. */
        Position lastPosition;
        /**
         * True if this entry was created by recovery from a leftover {@code /txn/op} record —
         * the TB doesn't know the real positions and pins {@code maxReadPosition} at the watermark
         * until this txn resolves.
         */
        boolean recoveryDiscovered;

        TxnEntry(TxnState state) {
            this.state = state;
        }
    }
}
