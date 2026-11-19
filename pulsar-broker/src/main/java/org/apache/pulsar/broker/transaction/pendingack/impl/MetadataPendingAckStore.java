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

import static org.apache.bookkeeper.mledger.impl.AckSetStateUtil.createPositionWithAckSet;
import static org.apache.bookkeeper.mledger.impl.AckSetStateUtil.getAckSetArrayOrNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.metadata.TxnIds;
import org.apache.pulsar.broker.transaction.metadata.TxnMetadataStore;
import org.apache.pulsar.broker.transaction.metadata.TxnMetadataStore.OpImage;
import org.apache.pulsar.broker.transaction.metadata.TxnMetadataStore.OpRecord;
import org.apache.pulsar.broker.transaction.metadata.TxnOp;
import org.apache.pulsar.broker.transaction.metadata.TxnOpKind;
import org.apache.pulsar.broker.transaction.metadata.TxnPaths;
import org.apache.pulsar.broker.transaction.metadata.TxnState;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckStore;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.ScanConsumer;

/**
 * {@link PendingAckStore} for {@code segment://} subscriptions that reads truth from the
 * metadata-store transaction layout (PIP-473) rather than from a per-subscription log.
 *
 * <p>Lifecycle:
 * <ul>
 *   <li><b>Ack</b> — {@link #appendIndividualAck} / {@link #appendCumulativeAck} write a
 *       {@link TxnOp} record under {@code /txn/op/<txnId>-<seq>} with
 *       {@code kind=ACK, segment, subscription, ledgerId, entryId, cumulative}, including the
 *       original batch size and acknowledgement mask. The associated
 *       {@code PendingAckHandle} keeps the in-memory state via the legacy interface.</li>
 *   <li><b>Commit / Abort marks</b> — {@link #appendCommitMark} / {@link #appendAbortMark} are
 *       <b>no-ops</b>. In v5 the TC owns the lifecycle: it CAS-updates the header and publishes
 *       subscription events; this store consumes those events.</li>
 *   <li><b>State transitions</b> — driven by {@code /txn/subscription-events/&lt;seg&gt;:&lt;sub&gt;-*}
 *       sequence events. The events are wake-ups; the truth is the header. On each notification
 *       we re-read headers for every currently-open txn this subscription is involved in and
 *       call {@code PendingAckHandleImpl.commitTxn} / {@code abortTxn} for those that have gone
 *       terminal — then delete the corresponding {@code /txn/op} ack records.</li>
 *   <li><b>Recovery</b> (Option C) — on {@link #replayAsync}, subscribe to the event stream,
 *       scan {@code idx:acks-by-segment-subscription} for this {@code (segment, sub)}, group by
 *       {@code txnId}, fetch each header, and seed the in-memory open-txn set; then mark the
 *       handle ready and drain any events that fired during recovery.</li>
 *   <li><b>Cursor reset</b> — {@link #beforeCursorReset} marks the records of terminal transactions
 *       {@code resetPending} in place before the reset is persisted, so recovery cannot reapply ACKs the
 *       reset discards while the TC keeps their headers; {@link #afterCursorReset} deletes them if the
 *       reset is persisted, or clears the mark if it is not.</li>
 * </ul>
 *
 * <p>Same TOCTOU / unbounded-cache caveats documented on {@code MetadataTransactionBuffer} apply
 * here; see that class's javadoc for the TC ordering contract.
 */
@CustomLog
public class MetadataPendingAckStore implements PendingAckStore {

    private final PersistentSubscription subscription;
    private final TxnMetadataStore txnStore;
    private final String segmentName;
    private final String subscriptionName;

    private final Object lock = new Object();
    /** Transactions awaiting a decision or durable cursor coverage — drained by reconcile. */
    private final Set<String> openTxns = new HashSet<>();
    /**
     * The ack appends in progress, which {@link #closeAsync()} waits for: the ack records of a deleted subscription
     * are deleted once it is closed, and none must land after that. Guarded by {@link #lock}, like the
     * {@link #closed} check of an append.
     */
    private final Set<CompletableFuture<Void>> appendsInProgress = new HashSet<>();
    /**
     * Cursor reset bookkeeping, guarded by {@link #lock}; the subscription runs one reset at a time.
     * {@link #beforeCursorReset} marks the records of terminal transactions {@code resetPending} in place, so the TC
     * keeps their headers; {@link #afterCursorReset} deletes them if the reset was persisted, or clears the mark if it
     * was not. A delete or an unmark that fails stays here and is retried on reconcile.
     */
    private boolean resetInProgress;
    /** The records marked by the reset in progress, at their marked versions. */
    private List<OpImage> markedForReset = new ArrayList<>();
    /** Marked records whose ACKs a persisted reset discarded, or that recovery found marked: to be deleted. */
    private final Map<String, OpImage> obsoleteMarked = new HashMap<>();
    /**
     * Marked records of a reset that was not persisted: their mark is to be cleared, unless a later reset marks them
     * again first, which takes them over.
     */
    private final Map<String, OpImage> unmarkPending = new HashMap<>();
    /**
     * The deletes and unmarks of the marked records run one {@link #resolveMarkedRecords} after another, and a reset
     * reads the records only once those issued before it have landed.
     */
    private CompletableFuture<Void> resolution = CompletableFuture.completedFuture(null);

    private final CompletableFuture<Void> recoveryFuture = new CompletableFuture<>();
    private volatile AutoCloseable eventSubscription;
    private volatile PendingAckHandleImpl handle;
    private volatile boolean closed;

    public MetadataPendingAckStore(PersistentSubscription subscription, TxnMetadataStore txnStore) {
        this.subscription = subscription;
        this.txnStore = txnStore;
        this.segmentName = subscription.getTopicName();
        this.subscriptionName = subscription.getName();
    }

    // ---- Replay / recovery -------------------------------------------------

    @Override
    public void replayAsync(PendingAckHandleImpl pendingAckHandle, ExecutorService executorService) {
        this.handle = pendingAckHandle;
        try {
            this.eventSubscription = txnStore.subscribeSubscriptionEvents(
                    segmentName, subscriptionName, path -> triggerReconcile());
        } catch (MetadataStoreException e) {
            recoveryFuture.completeExceptionally(e);
            pendingAckHandle.exceptionHandleFuture(e);
            return;
        }

        // Keep the positions as well as transaction ids: a fresh handle must recover the actual
        // pending acknowledgements before it becomes Ready or applies a terminal decision.
        // The store delivers per-entry errors via this scan's returned future as well — recovery
        // still fails loudly via the terminal whenComplete below — but we log here so the cause
        // is captured with the segment/subscription context.
        Map<String, ConcurrentLinkedQueue<TxnOp>> opsByTxn = new ConcurrentHashMap<>();
        Map<String, TxnState> states = new ConcurrentHashMap<>();
        Map<String, ConcurrentLinkedQueue<RecoveredAck>> recoveredAcks = new ConcurrentHashMap<>();
        txnStore.listAcksBySegmentSubscription(segmentName, subscriptionName, new ScanConsumer() {
            @Override
            public void onNext(GetResult r) {
                TxnOp op = TxnMetadataStore.fromJson(r.getValue(), TxnOp.class);
                // A store with a native index doesn't apply the scan's filter, and its lookup also matches the
                // records of a subscription whose name extends this one's.
                if (op.getKind() != TxnOpKind.ACK || !segmentName.equals(op.getSegment())
                        || !subscriptionName.equals(op.getSubscription())) {
                    return;
                }
                String path = r.getStat().getPath();
                String txnIdKey = TxnPaths.txnIdFromOpPath(path);
                if (txnIdKey == null) {
                    return;
                }
                if (Boolean.TRUE.equals(op.getResetPending())) {
                    // Marked by a cursor reset in progress when this subscription was last loaded: whether the
                    // reset was persisted is unknown, so the ACK is not reapplied (at worst the message is
                    // redelivered), and the record is deleted.
                    synchronized (lock) {
                        obsoleteMarked.put(path,
                                new OpImage(new OpRecord(path, txnIdKey, r.getStat().getVersion()), op));
                    }
                    return;
                }
                opsByTxn.computeIfAbsent(txnIdKey, __ -> new ConcurrentLinkedQueue<>()).add(op);
            }

            @Override
            public void onError(Throwable throwable) {
                log.warn().attr("segment", segmentName).attr("sub", subscriptionName)
                        .exception(throwable).log("PendingAckStore recovery scan errored");
            }

            @Override
            public void onCompleted() {
            }
        })
        .thenCompose(__ -> {
            List<CompletableFuture<Void>> reads = new ArrayList<>(opsByTxn.size());
            for (String txnIdKey : opsByTxn.keySet()) {
                reads.add(txnStore.getHeader(txnIdKey).thenAccept(opt -> {
                    TxnState state = opt.map(v -> v.value().getState()).orElse(TxnState.ABORTED);
                    states.put(txnIdKey, state);
                }));
            }
            return FutureUtil.waitForAll(reads);
        })
        .thenCompose(__ -> {
            List<CompletableFuture<Void>> positions = new ArrayList<>();
            opsByTxn.forEach((txnIdKey, ops) -> {
                // An abort never advances a cursor, so lost legacy batch masks need not prevent
                // rolling it back. Its fresh handle has no pending ACK state to release yet.
                if (states.get(txnIdKey) == TxnState.ABORTED) {
                    return;
                }
                for (TxnOp op : ops) {
                    positions.add(recoverPosition(op).thenAccept(position -> {
                        if (position != null) {
                            recoveredAcks.computeIfAbsent(txnIdKey, key -> new ConcurrentLinkedQueue<>())
                                    .add(new RecoveredAck(TxnIds.fromKey(txnIdKey), op, position));
                        }
                    }));
                }
            });
            return FutureUtil.waitForAll(positions);
        })
        .thenCompose(__ -> {
            // The handle has only one cumulative-ACK transaction slot. Finish each committed
            // transaction before hydrating another, and only then restore OPEN transactions.
            // A later cumulative position may be applied first: recovering an earlier position
            // then skips it because the cursor already covers it, without moving backward.
            CompletableFuture<Void> commits = CompletableFuture.completedFuture(null);
            for (Map.Entry<String, TxnState> entry : states.entrySet()) {
                if (entry.getValue() == TxnState.COMMITTED) {
                    String txnIdKey = entry.getKey();
                    commits = commits.thenComposeAsync(ignored -> {
                        hydrateAcks(pendingAckHandle, recoveredAcks.get(txnIdKey));
                        return pendingAckHandle.commitRecoveredTxn(TxnIds.fromKey(txnIdKey));
                    }, executorService);
                }
            }
            // A recovered committed ACK must already be applied before attaching consumers.
            return commits;
        })
        .thenRunAsync(() -> {
            states.forEach((txnIdKey, state) -> {
                if (state == TxnState.OPEN) {
                    hydrateAcks(pendingAckHandle, recoveredAcks.get(txnIdKey));
                }
            });
            synchronized (lock) {
                openTxns.addAll(states.keySet());
            }
            if (!pendingAckHandle.changeToReadyState()) {
                throw new CompletionException(new BrokerServiceException.ServiceUnitNotReadyException(
                        "Failed to change PendingAckHandle state to Ready"));
            }
            pendingAckHandle.completeHandleFuture();
            pendingAckHandle.handleCacheRequest();
            recoveryFuture.complete(null);
            states.forEach((txnIdKey, state) -> {
                if (state.isTerminal()) {
                    applyTerminal(txnIdKey, state);
                }
            });
            // All hydration, readiness and terminal scheduling share the handle's pinned executor.
            // Events arriving during recovery were gated by recoveryFuture and are re-read now.
            triggerReconcile();
        }, executorService)
        .whenComplete((__, err) -> {
            if (err != null) {
                log.error().attr("segment", segmentName).attr("sub", subscriptionName)
                        .exception(err).log("PendingAckStore recovery failed");
                closeSubscriptionQuietly();
                recoveryFuture.completeExceptionally(err);
                pendingAckHandle.exceptionHandleFuture(err);
            }
        });
    }

    private record RecoveredAck(TxnID txnId, TxnOp op, Position position) {
    }

    private static void hydrateAcks(PendingAckHandleImpl pendingAckHandle, Iterable<RecoveredAck> acks) {
        if (acks == null) {
            return;
        }
        for (RecoveredAck ack : acks) {
            if (Boolean.TRUE.equals(ack.op().getCumulative())) {
                pendingAckHandle.handleCumulativeAckRecover(ack.txnId(), ack.position());
            } else {
                int batchSize = ack.op().getBatchSize() == null ? 1 : ack.op().getBatchSize();
                // A deleted position must only skip that op, not the remaining ACKs of the txn.
                pendingAckHandle.handleIndividualAckRecover(ack.txnId(),
                        Collections.singletonList(MutablePair.of(ack.position(), batchSize)));
            }
        }
    }

    private CompletableFuture<Position> recoverPosition(TxnOp op) {
        return FutureUtil.supplySafely(() -> {
            Position position = PositionFactory.create(op.getLedgerId(), op.getEntryId());
            if (op.getBatchSize() != null) {
                long[] ackSet = op.getAckSet();
                return CompletableFuture.completedFuture(ackSet == null ? position
                        : createPositionWithAckSet(op.getLedgerId(), op.getEntryId(), ackSet.clone()));
            }
            // Older records omitted both batch size and ACK mask. A whole-entry ACK is safe only if
            // the original entry contained exactly one message (or the cursor durably deleted it).
            // A legacy partial multi-message batch cannot be reconstructed: retain its ops and fail
            // recovery rather than acknowledge messages the application never processed.
            if (subscription.getCursor().isMessageDeleted(position)) {
                return CompletableFuture.completedFuture(null);
            }
            CompletableFuture<Position> result = new CompletableFuture<>();
            try {
                subscription.getCursor().getManagedLedger().asyncReadEntry(position, new ReadEntryCallback() {
                    @Override
                    public void readEntryComplete(Entry entry, Object ctx) {
                        try {
                            MessageMetadata metadata = Commands.parseMessageMetadata(entry.getDataBuffer().duplicate());
                            if (metadata.getNumMessagesInBatch() != 1) {
                                result.completeExceptionally(new MetadataStoreException(
                                        "Legacy transactional ACK lacks a batch acknowledgement mask at "
                                                + segmentName + "/" + position + "; operation records retained"));
                            } else {
                                result.complete(position);
                            }
                        } catch (Throwable error) {
                            result.completeExceptionally(error);
                        } finally {
                            entry.release();
                        }
                    }

                    @Override
                    public void readEntryFailed(ManagedLedgerException error, Object ctx) {
                        result.completeExceptionally(error);
                    }
                }, null);
            } catch (Throwable error) {
                result.completeExceptionally(error);
            }
            return result;
        });
    }

    // ---- Ack append --------------------------------------------------------

    @Override
    public CompletableFuture<Void> appendIndividualAck(TxnID txnID,
                                                       List<MutablePair<Position, Integer>> positions) {
        return appendAcks(txnID, positions, false);
    }

    @Override
    public CompletableFuture<Void> appendCumulativeAck(TxnID txnID, Position position) {
        return appendAcks(txnID, Collections.singletonList(MutablePair.of(position, 1)), true);
    }

    private CompletableFuture<Void> appendAcks(TxnID txnID, List<MutablePair<Position, Integer>> positions,
                                               boolean cumulative) {
        CompletableFuture<Void> appended = new CompletableFuture<>();
        synchronized (lock) {
            if (closed) {
                return FutureUtil.failedFuture(new IllegalStateException("PendingAckStore is closed"));
            }
            appendsInProgress.add(appended);
        }
        String txnIdKey = TxnIds.toKey(txnID);
        // Append one TxnOp per position. PendingAckHandleImpl already validates the txn isn't
        // terminal before calling us, so we don't repeat the header check here.
        CompletableFuture<?>[] appends = new CompletableFuture<?>[positions.size()];
        for (int i = 0; i < positions.size(); i++) {
            Position p = positions.get(i).getLeft();
            TxnOp op = new TxnOp(TxnOpKind.ACK, segmentName, subscriptionName,
                    p.getLedgerId(), p.getEntryId(), cumulative ? Boolean.TRUE : null);
            op.setBatchSize(positions.get(i).getRight());
            long[] ackSet = getAckSetArrayOrNull(p);
            op.setAckSet(ackSet == null ? null : ackSet.clone());
            appends[i] = txnStore.appendOp(txnIdKey, op);
        }
        CompletableFuture.allOf(appends).whenComplete((__, err) -> {
            synchronized (lock) {
                appendsInProgress.remove(appended);
                if (err == null) {
                    openTxns.add(txnIdKey);
                }
            }
            if (err != null) {
                appended.completeExceptionally(err);
            } else {
                appended.complete(null);
            }
        });
        return appended;
    }

    @Override
    public CompletableFuture<Void> appendCommitMark(TxnID txnID, AckType ackType) {
        // No-op for the metadata-driven store: v5 commits are driven by the TC writing the
        // /txn header CAS and publishing the subscription event, not by direct SPI calls.
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> appendAbortMark(TxnID txnID, AckType ackType) {
        // No-op (see appendCommitMark).
        return CompletableFuture.completedFuture(null);
    }

    // ---- Reconcile (event-driven) -----------------------------------------

    private void triggerReconcile() {
        if (closed || !recoveryFuture.isDone() || recoveryFuture.isCompletedExceptionally()) {
            return;
        }
        // Retry the marked records left by a reset; the TC keeps re-publishing their outcome while they exist.
        resolveMarkedRecords();
        Set<String> snapshot;
        synchronized (lock) {
            snapshot = new HashSet<>(openTxns);
        }
        if (snapshot.isEmpty()) {
            return;
        }
        List<CompletableFuture<Void>> reads = new ArrayList<>(snapshot.size());
        for (String txnIdKey : snapshot) {
            reads.add(txnStore.getHeader(txnIdKey).thenAccept(opt -> {
                TxnState state = opt.map(v -> v.value().getState()).orElse(TxnState.ABORTED);
                if (state.isTerminal()) {
                    applyTerminal(txnIdKey, state);
                }
            }));
        }
        FutureUtil.waitForAll(reads).whenComplete((__, err) -> {
            if (err != null) {
                log.warn().attr("segment", segmentName).attr("sub", subscriptionName)
                        .exception(err).log("Reconcile encountered error");
            }
        });
    }

    /**
     * Apply a terminal state for {@code txnIdKey} — call the handle to advance / clear its state,
     * delete the matching op records, drop from {@link #openTxns}. Idempotent: if the handle
     * doesn't know the txn (already applied / was never on this sub), the calls are inexpensive.
     */
    private void applyTerminal(String txnIdKey, TxnState state) {
        boolean removed;
        synchronized (lock) {
            removed = openTxns.remove(txnIdKey);
        }
        if (!removed) {
            return;
        }
        TxnID txnID = TxnIds.fromKey(txnIdKey);
        PendingAckHandleImpl h = handle;
        CompletableFuture<Void> handleCall;
        if (h == null) {
            handleCall = CompletableFuture.completedFuture(null);
        } else if (state == TxnState.COMMITTED) {
            handleCall = h.commitTxn(txnID, Map.of(), 0L);
        } else {
            handleCall = h.abortTxn(txnID, null, 0L);
        }
        handleCall.whenComplete((__, err) -> {
            if (err != null) {
                log.warn().attr("segment", segmentName).attr("sub", subscriptionName).attr("txn", txnID)
                        .exception(err).log("Handle terminal-callback failed; will retry on next reconcile");
                // Re-add so a future event can retry.
                synchronized (lock) {
                    openTxns.add(txnIdKey);
                }
                return;
            }
            // A successful cursor ACK can still be only in memory (partial batches and rate
            // limiting). Keep its recovery record until the durable whole-entry prefix covers it.
            CompletableFuture<Void> cleanup = state == TxnState.COMMITTED
                    ? cleanupCommittedAcks(txnIdKey)
                    : txnStore.deleteAckOpsForSegmentSubscriptionAndTxn(segmentName, subscriptionName, txnIdKey);
            cleanup
                    .exceptionally(cleanupErr -> {
                        log.warn().attr("segment", segmentName).attr("sub", subscriptionName)
                                .attr("txn", txnID).exception(cleanupErr)
                                .log("Op-record cleanup failed; will retry on next reconcile");
                        synchronized (lock) {
                            openTxns.add(txnIdKey);
                        }
                        return null;
                    });
        });
    }

    private CompletableFuture<Void> cleanupCommittedAcks(String txnIdKey) {
        return FutureUtil.supplySafely(() -> {
            Position persisted = subscription.getCursor().getPersistentMarkDeletedPosition();
            ConcurrentLinkedQueue<OpRecord> covered = new ConcurrentLinkedQueue<>();
            AtomicBoolean retained = new AtomicBoolean();
            return txnStore.listOpsByTxn(txnIdKey, new ScanConsumer() {
                @Override
                public void onNext(GetResult record) {
                    TxnOp op = TxnMetadataStore.fromJson(record.getValue(), TxnOp.class);
                    String path = record.getStat().getPath();
                    if (!txnIdKey.equals(TxnPaths.txnIdFromOpPath(path)) || op.getKind() != TxnOpKind.ACK
                            || !segmentName.equals(op.getSegment()) || !subscriptionName.equals(op.getSubscription())) {
                        return;
                    }
                    Position position = PositionFactory.create(op.getLedgerId(), op.getEntryId());
                    if (persisted != null && position.compareTo(persisted) <= 0) {
                        covered.add(new OpRecord(path, txnIdKey, record.getStat().getVersion()));
                    } else {
                        retained.set(true);
                    }
                }

                @Override
                public void onError(Throwable error) {
                    // The returned scan future propagates the error and leaves recovery ops intact.
                }

                @Override
                public void onCompleted() {
                }
            }).thenCompose(__ -> txnStore.deleteOps(List.copyOf(covered))).thenCompose(__ -> {
                if (retained.get() || covered.isEmpty()) {
                    return CompletableFuture.completedFuture(retained.get());
                }
                // Versioned deletes can skip an op replaced at the same path. Keep tracking the
                // transaction if such a record still exists, so a later event can retry cleanup.
                AtomicBoolean remaining = new AtomicBoolean();
                return txnStore.listOpsByTxn(txnIdKey, new ScanConsumer() {
                    @Override
                    public void onNext(GetResult record) {
                        TxnOp op = TxnMetadataStore.fromJson(record.getValue(), TxnOp.class);
                        if (txnIdKey.equals(TxnPaths.txnIdFromOpPath(record.getStat().getPath()))
                                && op.getKind() == TxnOpKind.ACK && segmentName.equals(op.getSegment())
                                && subscriptionName.equals(op.getSubscription())) {
                            remaining.set(true);
                        }
                    }

                    @Override
                    public void onError(Throwable error) {
                        // The returned future preserves retry tracking on an error.
                    }

                    @Override
                    public void onCompleted() {
                    }
                }).thenApply(ignored -> remaining.get());
            }).thenAccept(needsRecovery -> {
                if (needsRecovery) {
                    synchronized (lock) {
                        openTxns.add(txnIdKey);
                    }
                }
            });
        });
    }

    // ---- Cursor reset ------------------------------------------------------

    /**
     * Marks the ACK records of terminal transactions {@code resetPending} before the cursor is reset. A committed ACK
     * record is kept after its ACK is applied, until the durable whole-entry prefix covers it, so recovery can reapply
     * an ACK the cursor held only in memory. The reset replaces that cursor state, after which nothing covers the
     * record, and recovery would reapply the ACK and undo the reset; recovery never reapplies a marked record. The
     * records of OPEN transactions stay unmarked: their ACKs apply when they commit, after the reset, as on the live
     * handle. A terminal transaction whose outcome is not applied yet loses only the durability of its ACK, which at
     * worst redelivers the message.
     *
     * <p>The records are marked in place rather than deleted, so the TC never sees a terminal transaction without op
     * records, and keeps its header, while the outcome of the reset is unknown. Each mark that lands is tracked, so
     * {@link #afterCursorReset} can clear it if the reset is not persisted, including after marking failed part way.
     *
     * <p>The records are read only once the unmarks of an earlier reset already issued have landed, and none is issued
     * until this reset completes: an unmark landing between the read and the marking would leave a record this reset
     * discards unmarked. Marking fails if a record was rewritten since it was read, rather than leave it unmarked.
     */
    @Override
    public CompletableFuture<Void> beforeCursorReset() {
        CompletableFuture<Void> resolved;
        synchronized (lock) {
            resetInProgress = true;
            markedForReset = new ArrayList<>();
            resolved = resolution;
        }
        // Recovery applies the recovered committed ACKs before it completes; their records are then marked here.
        return resolved.handle((__, error) -> null)
                .thenCompose(__ -> recoveryFuture)
                .thenCompose(__ -> txnStore.collectAckOpImagesForSegmentSubscription(segmentName, subscriptionName))
                .thenCompose(images -> {
                    Map<String, List<OpImage>> imagesByTxn = new HashMap<>();
                    synchronized (lock) {
                        for (OpImage image : images) {
                            // A record whose ACK a persisted reset already discarded stays marked until deleted.
                            if (!obsoleteMarked.containsKey(image.record().path())) {
                                imagesByTxn.computeIfAbsent(image.record().txnId(), __ -> new ArrayList<>())
                                        .add(image);
                            }
                        }
                    }
                    ConcurrentLinkedQueue<OpImage> marks = new ConcurrentLinkedQueue<>();
                    List<CompletableFuture<Void>> reads = new ArrayList<>(imagesByTxn.size());
                    imagesByTxn.forEach((txnIdKey, txnImages) -> reads.add(txnStore.getHeader(txnIdKey)
                            .thenAccept(header -> {
                                TxnState state = header.map(v -> v.value().getState()).orElse(TxnState.ABORTED);
                                if (state.isTerminal()) {
                                    txnImages.forEach(image -> marks.add(withResetPending(image, true)));
                                }
                            })));
                    return FutureUtil.waitForAll(reads).thenCompose(__ -> txnStore.replaceOps(List.copyOf(marks),
                            true, marked -> {
                                synchronized (lock) {
                                    markedForReset.add(marked);
                                    // This reset now owns a record an earlier reset left to unmark.
                                    unmarkPending.remove(marked.record().path());
                                }
                            }));
                });
    }

    /**
     * Resolves the records {@link #beforeCursorReset} marked: deletes them if the reset was persisted, or clears their
     * mark if it was not, since the cursor state they protect, which may hold their ACKs only in memory, still stands.
     * A persisted reset also discards the ACKs of the records an earlier reset left to unmark.
     */
    @Override
    public CompletableFuture<Void> afterCursorReset(boolean persisted) {
        synchronized (lock) {
            if (persisted) {
                unmarkPending.forEach(obsoleteMarked::put);
                unmarkPending.clear();
            }
            for (OpImage marked : markedForReset) {
                (persisted ? obsoleteMarked : unmarkPending).put(marked.record().path(), marked);
            }
            markedForReset = new ArrayList<>();
            resetInProgress = false;
        }
        return resolveMarkedRecords();
    }

    /**
     * Deletes the marked records whose ACKs a persisted reset discarded, and clears the mark of those whose reset was
     * not persisted. Each stays in the bookkeeping until that succeeds, and is retried on the next reconcile. Runs
     * after the resolution issued before it, so a reset can wait for all of them.
     */
    private CompletableFuture<Void> resolveMarkedRecords() {
        CompletableFuture<Void> next = new CompletableFuture<>();
        CompletableFuture<Void> previous;
        synchronized (lock) {
            previous = resolution;
            resolution = next;
        }
        FutureUtil.completeAfter(next, previous.handle((__, error) -> null)
                .thenCompose(__ -> resolveMarkedRecordsNow()));
        return next;
    }

    private CompletableFuture<Void> resolveMarkedRecordsNow() {
        List<OpImage> obsolete;
        List<OpImage> unmark;
        synchronized (lock) {
            obsolete = List.copyOf(obsoleteMarked.values());
            // A reset in progress marks these records again; clearing their mark meanwhile would race it.
            unmark = resetInProgress ? List.of() : List.copyOf(unmarkPending.values());
        }
        if (obsolete.isEmpty() && unmark.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> deletes = txnStore.deleteOps(obsolete.stream().map(OpImage::record).toList())
                .thenRun(() -> {
                    synchronized (lock) {
                        obsolete.forEach(image -> obsoleteMarked.remove(image.record().path(), image));
                    }
                });
        // A conflict means an earlier unmark of the record landed although it was reported as failed: resets don't
        // mark while an unmark is in flight.
        CompletableFuture<Void> unmarks = txnStore.replaceOps(
                unmark.stream().map(image -> withResetPending(image, false)).toList(), false, unmarked -> {
                    synchronized (lock) {
                        // Track the txn, so reconcile deletes the record once the durable cursor covers it.
                        openTxns.add(unmarked.record().txnId());
                    }
                }).thenRun(() -> {
                    synchronized (lock) {
                        unmark.forEach(image -> unmarkPending.remove(image.record().path(), image));
                    }
                });
        return FutureUtil.waitForAll(List.of(deletes, unmarks)).whenComplete((__, error) -> {
            if (error != null) {
                log.warn().attr("segment", segmentName).attr("sub", subscriptionName).exception(error)
                        .log("Failed to resolve the ACK records marked for a cursor reset; will retry on reconcile");
            }
        });
    }

    private static OpImage withResetPending(OpImage image, boolean pending) {
        TxnOp op = image.op();
        return new OpImage(image.record(), new TxnOp(op.getKind(), op.getSegment(), op.getSubscription(),
                op.getLedgerId(), op.getEntryId(), op.getCumulative(), op.getBatchSize(), op.getAckSet(),
                pending ? Boolean.TRUE : null));
    }

    // ---- Close -------------------------------------------------------------

    /**
     * Completes once the ack appends in progress are done; no append starts after it is called. A failed append
     * fails the ack it was for, not the close.
     */
    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<?>[] inProgress;
        synchronized (lock) {
            closed = true;
            inProgress = appendsInProgress.toArray(new CompletableFuture<?>[0]);
        }
        // A store closed before its replay started never recovers; release a cursor reset waiting for it.
        recoveryFuture.completeExceptionally(new IllegalStateException("PendingAckStore is closed"));
        closeSubscriptionQuietly();
        return CompletableFuture.allOf(inProgress).exceptionally(__ -> null);
    }

    private void closeSubscriptionQuietly() {
        AutoCloseable h = eventSubscription;
        if (h == null) {
            return;
        }
        eventSubscription = null;
        try {
            h.close();
        } catch (Throwable t) {
            log.warn().attr("segment", segmentName).attr("sub", subscriptionName).exception(t)
                    .log("Failed to close event subscription");
        }
    }

    // ---- Test seams --------------------------------------------------------

    /** @return future that completes when initial recovery has finished. Intended for tests. */
    public CompletableFuture<Void> recoveryFuture() {
        return recoveryFuture;
    }
}
