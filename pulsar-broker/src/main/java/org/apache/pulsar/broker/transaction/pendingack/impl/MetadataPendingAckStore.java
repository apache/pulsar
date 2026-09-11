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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.Position;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.metadata.TxnIds;
import org.apache.pulsar.broker.transaction.metadata.TxnMetadataStore;
import org.apache.pulsar.broker.transaction.metadata.TxnOp;
import org.apache.pulsar.broker.transaction.metadata.TxnOpKind;
import org.apache.pulsar.broker.transaction.metadata.TxnState;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckStore;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
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
 *       {@code kind=ACK, segment, subscription, ledgerId, entryId, cumulative}. The associated
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
    /** Set of txnIdKeys we believe are OPEN — populated on append + recovery, drained by reconcile. */
    private final Set<String> openTxns = new HashSet<>();

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

        // Scan all ack op records for this (segment, sub) and seed openTxns from the headers.
        // The store delivers per-entry errors via this scan's returned future as well — recovery
        // still fails loudly via the terminal whenComplete below — but we log here so the cause
        // is captured with the segment/subscription context.
        Set<String> txnIdKeys = ConcurrentHashMap.newKeySet();
        txnStore.listAcksBySegmentSubscription(segmentName, subscriptionName, new ScanConsumer() {
            @Override
            public void onNext(GetResult r) {
                TxnOp op = TxnMetadataStore.fromJson(r.getValue(), TxnOp.class);
                if (op.getKind() != TxnOpKind.ACK) {
                    return;
                }
                String txnIdKey = org.apache.pulsar.broker.transaction.metadata.TxnPaths
                        .txnIdFromOpPath(r.getStat().getPath());
                if (txnIdKey != null) {
                    txnIdKeys.add(txnIdKey);
                }
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
            List<CompletableFuture<Void>> reads = new ArrayList<>(txnIdKeys.size());
            for (String txnIdKey : txnIdKeys) {
                reads.add(txnStore.getHeader(txnIdKey).thenAccept(opt -> {
                    TxnState state = opt.map(v -> v.value().getState()).orElse(TxnState.ABORTED);
                    synchronized (lock) {
                        openTxns.add(txnIdKey);
                    }
                    if (state.isTerminal()) {
                        // Schedule cleanup — applyTerminal's "remove → applied" gate fires once
                        // and processes (handle commit/abort + delete op records).
                        applyTerminal(txnIdKey, state);
                    }
                }));
            }
            return FutureUtil.waitForAll(reads);
        })
        .whenComplete((__, err) -> {
            if (err != null) {
                log.error().attr("segment", segmentName).attr("sub", subscriptionName)
                        .exception(err).log("PendingAckStore recovery failed");
                closeSubscriptionQuietly();
                recoveryFuture.completeExceptionally(err);
                pendingAckHandle.exceptionHandleFuture(err);
                return;
            }
            recoveryFuture.complete(null);
            // Mirror the legacy MLPendingAckStore completion: flip the handle to Ready and
            // complete the handle future — PersistentSubscription.addConsumer blocks on that
            // future, so skipping it hangs every subscribe on a segment topic — then drain any
            // ack requests queued during recovery. Run on the pinned executor so the
            // state-machine transition and the cache drain stay single-threaded.
            executorService.execute(() -> {
                if (pendingAckHandle.changeToReadyState()) {
                    pendingAckHandle.completeHandleFuture();
                } else {
                    pendingAckHandle.exceptionHandleFuture(
                            new BrokerServiceException.ServiceUnitNotReadyException(
                                    "Failed to change PendingAckHandle state to Ready"));
                }
                pendingAckHandle.handleCacheRequest();
            });
            // Drain any events that fired during recovery.
            triggerReconcile();
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
        if (closed) {
            return FutureUtil.failedFuture(new IllegalStateException("PendingAckStore is closed"));
        }
        String txnIdKey = TxnIds.toKey(txnID);
        // Append one TxnOp per position. PendingAckHandleImpl already validates the txn isn't
        // terminal before calling us, so we don't repeat the header check here.
        CompletableFuture<?>[] appends = new CompletableFuture<?>[positions.size()];
        for (int i = 0; i < positions.size(); i++) {
            Position p = positions.get(i).getLeft();
            TxnOp op = new TxnOp(TxnOpKind.ACK, segmentName, subscriptionName,
                    p.getLedgerId(), p.getEntryId(), cumulative ? Boolean.TRUE : null);
            appends[i] = txnStore.appendOp(txnIdKey, op);
        }
        return CompletableFuture.allOf(appends).thenRun(() -> {
            synchronized (lock) {
                openTxns.add(txnIdKey);
            }
        });
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
            // Successful handle call → clean up the ack op records on disk.
            txnStore.deleteAckOpsForSegmentSubscriptionAndTxn(segmentName, subscriptionName, txnIdKey)
                    .exceptionally(cleanupErr -> {
                        log.warn().attr("segment", segmentName).attr("sub", subscriptionName)
                                .attr("txn", txnID).exception(cleanupErr)
                                .log("Op-record cleanup failed; will retry on next reconcile");
                        return null;
                    });
        });
    }

    // ---- Close -------------------------------------------------------------

    @Override
    public CompletableFuture<Void> closeAsync() {
        closed = true;
        closeSubscriptionQuietly();
        return CompletableFuture.completedFuture(null);
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
