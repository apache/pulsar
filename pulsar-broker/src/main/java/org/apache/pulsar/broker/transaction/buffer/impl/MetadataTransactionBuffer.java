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

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.ScanConsumer;

/**
 * {@link TransactionBuffer} for {@code segment://} topics that reads truth from the metadata-store
 * transaction layout (PIP-473) rather than from a per-topic snapshot log.
 *
 * <p>Lifecycle:
 * <ul>
 *   <li><b>Publish</b> — {@link #appendBufferToTxn} reads the txn header (cache-first), appends the
 *       entry to the managed ledger, then appends a {@link TxnOp} record under
 *       {@code /txn/op/<txnId>-<seq>}. Both must succeed before we ack the publisher.</li>
 *   <li><b>State transitions</b> — driven by {@code /txn/segment-events/<segment>-*} sequence
 *       events. The events are wake-ups; the truth is the header. On each notification we
 *       re-read headers for every currently-open txn and apply the resulting state changes.</li>
 *   <li><b>Recovery</b> (Option C) — scan {@code idx:writes-by-segment} for this segment, group by
 *       {@code txnId}, fetch each header, and seed the in-memory cache. Then subscribe to the
 *       event stream for forward updates.</li>
 * </ul>
 *
 * <p><b>TC ordering contract.</b> There is a TOCTOU window between the header authorization read
 * in {@link #appendBufferToTxn} and the managed-ledger append: the TC may flip the header (commit
 * or abort) in between, and the entry still lands. On commit that's harmless — the message is
 * visible. On abort, the subsequent segment-event delivery marks the txn ABORTED in the cache and
 * {@link #isTxnAborted} filters it. This relies on the TC publishing the segment event <em>after</em>
 * the header CAS lands so a participant that lost the race always learns the decision. The legacy
 * {@code TopicTransactionBuffer} has the same window with marker-message ordering.
 *
 * <p><b>In-memory growth.</b> Terminal txns stay in the {@code txns} cache for the segment's
 * lifetime so {@code isTxnAborted} can answer authoritatively for dispatcher reads — evicting a
 * COMMITTED entry would mean the default "unknown → aborted" filter wrongly hides its messages.
 * Long-running segments with high txn turnover will accumulate cached entries. Cache pruning tied
 * to data-ledger trimming / header GC is a P5/P6 concern.
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

    /** Guards {@link #txns} + {@link #maxReadPosition} + {@link #lastDispatchable}. */
    private final Object lock = new Object();

    /** Cached per-txn state, populated by appendBufferToTxn and refreshed by event reconcile. */
    private final Map<String, TxnEntry> txns = new HashMap<>();

    private Position maxReadPosition;
    private Position lastDispatchable;

    private final LongAdder committedCount = new LongAdder();
    private final LongAdder abortedCount = new LongAdder();

    public MetadataTransactionBuffer(PersistentTopic topic, TxnMetadataStore txnStore) {
        this.topic = topic;
        this.ledger = topic.getManagedLedger();
        this.txnStore = txnStore;
        this.segmentName = topic.getName();
        this.maxReadPositionCallBack = topic.getMaxReadPositionCallBack();
        this.maxReadPosition = ledger.getLastConfirmedEntry();
        this.lastDispatchable = this.maxReadPosition;
        recover();
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

        // Scan all /txn/op records for this segment, group by txnId.
        Map<String, List<Position>> opsByTxn = new ConcurrentHashMap<>();
        txnStore.listWritesBySegment(segmentName, new ScanConsumer() {
            @Override
            public void onNext(GetResult r) {
                TxnOp op = TxnMetadataStore.fromJson(r.getValue(), TxnOp.class);
                String txnIdKey = TxnPaths.txnIdFromOpPath(r.getStat().getPath());
                if (txnIdKey == null) {
                    return;
                }
                opsByTxn.computeIfAbsent(txnIdKey, k -> new ArrayList<>())
                        .add(PositionFactory.create(op.getLedgerId(), op.getEntryId()));
            }

            @Override
            public void onError(Throwable throwable) {
                // Recovery still fails loudly via the scan's returned future and the terminal
                // whenComplete below; logging here captures the cause with segment context.
                log.warn().attr("segment", segmentName).exception(throwable)
                        .log("TB recovery scan errored");
            }

            @Override
            public void onCompleted() {
            }
        })
        .thenCompose(__ -> {
            // Fan out one header read per distinct txnId; build initial state.
            List<CompletableFuture<Void>> reads = new ArrayList<>();
            opsByTxn.forEach((txnIdKey, positions) -> reads.add(
                    txnStore.getHeader(txnIdKey).thenAccept(opt -> applyHeaderForRecovery(
                            txnIdKey, opt, positions))));
            return FutureUtil.waitForAll(reads);
        })
        .whenComplete((__, err) -> {
            if (err != null) {
                log.error().attr("segment", segmentName).exception(err).log("TB recovery failed");
                // Close the subscription we opened above so the listener doesn't outlive a
                // failed-to-recover TB instance (closeAsync may never be called if recovery never
                // succeeded).
                closeSubscriptionQuietly();
                recoveryFuture.completeExceptionally(err);
                return;
            }
            synchronized (lock) {
                recomputeMaxReadPositionLocked();
            }
            recoveryFuture.complete(null);
            // Drain any events that fired between subscribe and now — triggerReconcile short-
            // circuits while recoveryFuture is not done, so we explicitly kick a reconcile pass
            // now to pick up state transitions whose only notification landed in that window.
            triggerReconcile();
        });
    }

    private void applyHeaderForRecovery(String txnIdKey, Optional<Versioned<TxnHeader>> opt, List<Position> positions) {
        TxnState state = opt.map(v -> v.value().getState()).orElse(TxnState.ABORTED);
        Position first = positions.stream().min(Position::compareTo).orElse(null);
        synchronized (lock) {
            txns.put(txnIdKey, new TxnEntry(state, first));
        }
        // Schedule op-record cleanup for terminal txns (best-effort, async).
        if (state.isTerminal()) {
            cleanupOpRecords(txnIdKey);
        }
    }

    // ---- Publish path ------------------------------------------------------

    @Override
    public CompletableFuture<Position> appendBufferToTxn(TxnID txnId, long sequenceId, ByteBuf buffer) {
        // Retain so the buffer survives the chain — release in the terminal handlers.
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
                    return appendToLedger(buffer)
                            .thenCompose(position -> recordOp(txnId, txnIdKey, position));
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
                txns.putIfAbsent(txnIdKey, new TxnEntry(state, null));
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

    private CompletableFuture<Position> recordOp(TxnID txnId, String txnIdKey, Position position) {
        TxnOp op = new TxnOp(TxnOpKind.WRITE, segmentName, null,
                position.getLedgerId(), position.getEntryId(), null);
        return txnStore.appendOp(txnIdKey, op).thenApply(stat -> {
            synchronized (lock) {
                TxnEntry entry = txns.get(txnIdKey);
                // Only an OPEN entry pins maxReadPosition. If a concurrent reconcile flipped this
                // txn to terminal between the cache-first authorization read and this update, do
                // NOT resurrect it as OPEN — the ML append still lands but isTxnAborted will mask
                // it for aborted txns (see class javadoc on the publish-side TOCTOU window).
                if (entry != null && entry.state == TxnState.OPEN) {
                    if (entry.firstPosition == null || position.compareTo(entry.firstPosition) < 0) {
                        entry.firstPosition = position;
                        recomputeMaxReadPositionLocked();
                    }
                }
            }
            return position;
        });
    }

    // ---- Reconcile (event-driven) -----------------------------------------

    private void triggerReconcile() {
        if (closed || !recoveryFuture.isDone()) {
            return;
        }
        Set<String> snapshot;
        synchronized (lock) {
            snapshot = new HashSet<>(openTxnsLocked());
        }
        if (snapshot.isEmpty()) {
            return;
        }
        List<CompletableFuture<Void>> reads = new ArrayList<>(snapshot.size());
        for (String txnIdKey : snapshot) {
            reads.add(txnStore.getHeader(txnIdKey).thenAccept(opt -> {
                TxnState newState = opt.map(v -> v.value().getState()).orElse(TxnState.ABORTED);
                applyReconciledState(txnIdKey, newState);
            }));
        }
        FutureUtil.waitForAll(reads).whenComplete((__, err) -> {
            if (err != null) {
                log.warn().attr("segment", segmentName).exception(err).log("Reconcile encountered error");
            }
        });
    }

    private void applyReconciledState(String txnIdKey, TxnState newState) {
        boolean cleanup = false;
        synchronized (lock) {
            TxnEntry entry = txns.get(txnIdKey);
            if (entry == null || entry.state == newState) {
                return;
            }
            entry.state = newState;
            if (newState.isTerminal()) {
                entry.firstPosition = null;
                cleanup = true;
                if (newState == TxnState.COMMITTED) {
                    committedCount.increment();
                } else {
                    abortedCount.increment();
                }
                recomputeMaxReadPositionLocked();
            }
        }
        if (cleanup) {
            cleanupOpRecords(txnIdKey);
        }
    }

    /**
     * Delete every {@code /txn/op} record for {@code (this segment, txnIdKey)}. Best-effort —
     * failures are logged and retried by the next reconcile.
     */
    private void cleanupOpRecords(String txnIdKey) {
        txnStore.deleteWriteOpsForSegmentAndTxn(segmentName, txnIdKey)
                .exceptionally(err -> {
                    log.warn().attr("segment", segmentName).attr("txnId", txnIdKey).exception(err)
                            .log("Op-record cleanup failed; will retry on next reconcile");
                    return null;
                });
    }

    // ---- maxReadPosition ---------------------------------------------------

    private Set<String> openTxnsLocked() {
        Set<String> open = new HashSet<>();
        for (Map.Entry<String, TxnEntry> e : txns.entrySet()) {
            if (e.getValue().state == TxnState.OPEN) {
                open.add(e.getKey());
            }
        }
        return open;
    }

    private void recomputeMaxReadPositionLocked() {
        Position min = null;
        for (TxnEntry e : txns.values()) {
            if (e.state == TxnState.OPEN && e.firstPosition != null) {
                if (min == null || e.firstPosition.compareTo(min) < 0) {
                    min = e.firstPosition;
                }
            }
        }
        Position next = (min == null) ? lastDispatchable : ledger.getPreviousPosition(min);
        Position prev = maxReadPosition;
        maxReadPosition = next;
        // Only fire the callback on forward motion. Initial-state setup at recovery may move
        // the position backwards (LAC -> previous(firstOpenWrite)); that's not a "moved forward".
        if (next.compareTo(prev) > 0 && maxReadPositionCallBack != null) {
            maxReadPositionCallBack.maxReadPositionMovedForward(prev, next);
        }
    }

    // ---- SPI surface (lifecycle, queries) ---------------------------------

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
            TxnEntry entry = txns.get(key);
            if (entry == null) {
                // No record of this txn — must be either orphan (broker crash mid-publish) or
                // long-aborted-and-cleaned. Filtering is the safe default.
                return true;
            }
            return entry.state == TxnState.ABORTED;
        }
    }

    @Override
    public void syncMaxReadPositionForNormalPublish(Position position, boolean isMarkerMessage) {
        if (isMarkerMessage) {
            return;
        }
        topic.updateLastDispatchablePosition(position);
        synchronized (lock) {
            lastDispatchable = position;
            recomputeMaxReadPositionLocked();
        }
    }

    @Override
    public CompletableFuture<Void> checkIfTBRecoverCompletely() {
        return recoveryFuture;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        closed = true;
        closeSubscriptionQuietly();
        return CompletableFuture.completedFuture(null);
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
        // No-op for the metadata-driven TB: v5 commits are driven by the TC writing /txn header CAS
        // and the segment-event stream, not by direct SPI calls.
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> abortTxn(TxnID txnID, long lowWaterMark) {
        // No-op (see commitTxn).
        return CompletableFuture.completedFuture(null);
    }

    // ---- SPI surface (snapshots / readers — unused in v5) -----------------

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
        Position firstPosition; // null if no writes on this segment yet, or after termination

        TxnEntry(TxnState state, Position firstPosition) {
            this.state = state;
            this.firstPosition = firstPosition;
        }
    }
}
