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
package org.apache.pulsar.broker.transaction.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import lombok.CustomLog;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.ScanConsumer;
import org.apache.pulsar.metadata.api.Stat;

/**
 * Typed façade over a {@link MetadataStore} that implements the PIP-473 transaction data layout.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>JSON serde for {@link TxnHeader}, {@link TxnOp}, {@link TxnEvent}.</li>
 *   <li>Attach {@link Option.PartitionKey} so all records for one txnId / segment / (segment, sub)
 *       co-locate on sharded backends (Oxia).</li>
 *   <li>Attach {@link Option.SecondaryIndex} entries so the runtime queries hit native indexes when
 *       available, with a deserializing fallback predicate for stores that don't have them.</li>
 *   <li>Append op-log and event records via {@link Option.SequenceKeysDeltas}.</li>
 * </ul>
 *
 * <p>The façade is stateless apart from holding the store reference — index population happens via
 * options on writes, so there is no explicit registration step.
 */
@CustomLog
public class TxnMetadataStore {

    /** Sequence-keys delta used by all append-only streams in this layout. */
    private static final Option.SequenceKeysDeltas APPEND_DELTAS =
            new Option.SequenceKeysDeltas(List.of(1L));

    private final MetadataStore store;

    public TxnMetadataStore(MetadataStore store) {
        this.store = store;
    }

    // ---- Header CRUD -------------------------------------------------------

    /** @return the header at {@code /txn/<txnId>} with its version, or empty if not present. */
    public CompletableFuture<Optional<Versioned<TxnHeader>>> getHeader(String txnId) {
        return store.get(TxnPaths.header(txnId), Set.of(new Option.PartitionKey(txnId)))
                .thenApply(opt -> opt.map(gr ->
                        new Versioned<>(fromJson(gr.getValue(), TxnHeader.class), gr.getStat().getVersion())));
    }

    /**
     * Create the txn header at version -1 (must not exist). Adds the deadline secondary-index entry
     * so the timeout sweeper can range-scan open transactions.
     */
    public CompletableFuture<Stat> createHeader(String txnId, TxnHeader header) {
        return store.put(TxnPaths.header(txnId), toJson(header), Optional.of(-1L),
                headerOptions(txnId, header));
    }

    /**
     * CAS-update the txn header. Pass the {@code version} returned by a previous {@link #getHeader}.
     * Index entries are recomputed by the store based on the options on this write.
     */
    public CompletableFuture<Stat> updateHeader(String txnId, TxnHeader header, long expectedVersion) {
        return store.put(TxnPaths.header(txnId), toJson(header), Optional.of(expectedVersion),
                headerOptions(txnId, header));
    }

    /** Delete the txn header with CAS on the expected version. Tolerates a NotFound result. */
    public CompletableFuture<Void> deleteHeader(String txnId, long expectedVersion) {
        return store.deleteIfExists(TxnPaths.header(txnId), Optional.of(expectedVersion),
                Set.of(new Option.PartitionKey(txnId)));
    }

    private static Set<Option> headerOptions(String txnId, TxnHeader header) {
        Option.SecondaryIndex idx;
        if (header.getState().isTerminal()) {
            long finalizedMs = header.getFinalizedAt() == null ? 0L : header.getFinalizedAt().toEpochMilli();
            idx = new Option.SecondaryIndex(TxnPaths.IDX_TXN_BY_FINAL_STATE,
                    TxnPaths.finalStateKey(header.getState(), finalizedMs));
        } else {
            long deadlineMs = header.getCreatedAt().toEpochMilli() + header.getTimeout().toMillis();
            idx = new Option.SecondaryIndex(TxnPaths.IDX_TXN_BY_DEADLINE, TxnPaths.longKey(deadlineMs));
        }
        return Set.of(new Option.PartitionKey(txnId), idx);
    }

    // ---- Op-log append -----------------------------------------------------

    /**
     * Append a {@link TxnOp} under {@code /txn/op/<txnId>-<seq>}. Adds the per-kind secondary-index
     * entry — {@link TxnPaths#IDX_WRITES_BY_SEGMENT} for writes,
     * {@link TxnPaths#IDX_ACKS_BY_SEGMENT_SUBSCRIPTION} for acks. Returns the {@link Stat} whose
     * {@code path} carries the generated sequence key.
     */
    public CompletableFuture<Stat> appendOp(String txnId, TxnOp op) {
        Option.SecondaryIndex participantIdx = switch (op.getKind()) {
            case WRITE -> new Option.SecondaryIndex(TxnPaths.IDX_WRITES_BY_SEGMENT,
                    TxnPaths.segmentKey(op.getSegment()));
            case ACK -> new Option.SecondaryIndex(TxnPaths.IDX_ACKS_BY_SEGMENT_SUBSCRIPTION,
                    TxnPaths.ackIndexKey(op.getSegment(), op.getSubscription()));
        };
        // Also index by txnId so the TC's endTxn can enumerate this txn's ops without scanning
        // the whole /txn/op namespace.
        Option.SecondaryIndex byTxnIdx = new Option.SecondaryIndex(TxnPaths.IDX_OPS_BY_TXN, txnId);
        Set<Option> opts = Set.of(new Option.PartitionKey(txnId), participantIdx, byTxnIdx,
                APPEND_DELTAS);
        return store.put(TxnPaths.opParent(txnId), toJson(op), Optional.empty(), opts);
    }

    // ---- Index queries -----------------------------------------------------

    /** Stream all write ops targeting {@code segment}. */
    public CompletableFuture<Void> listWritesBySegment(String segment, ScanConsumer consumer) {
        String key = TxnPaths.segmentKey(segment);
        return store.scanByIndex(TxnPaths.TXN_OP_PREFIX, TxnPaths.IDX_WRITES_BY_SEGMENT,
                key, key,
                gr -> {
                    TxnOp op = fromJson(gr.getValue(), TxnOp.class);
                    return op.getKind() == TxnOpKind.WRITE && segment.equals(op.getSegment());
                },
                consumer);
    }

    /** Stream all ack ops targeting {@code (segment, subscription)}. */
    public CompletableFuture<Void> listAcksBySegmentSubscription(String segment, String subscription,
                                                                 ScanConsumer consumer) {
        String key = TxnPaths.ackIndexKey(segment, subscription);
        return store.scanByIndex(TxnPaths.TXN_OP_PREFIX, TxnPaths.IDX_ACKS_BY_SEGMENT_SUBSCRIPTION,
                key, key,
                gr -> {
                    TxnOp op = fromJson(gr.getValue(), TxnOp.class);
                    return op.getKind() == TxnOpKind.ACK
                            && segment.equals(op.getSegment())
                            && subscription.equals(op.getSubscription());
                },
                consumer);
    }

    /**
     * Stream all {@code /txn/op} records for {@code txnId} via the {@link TxnPaths#IDX_OPS_BY_TXN}
     * index. Used by the v5 TC at end-txn time to enumerate participants — distinct segments for
     * writes, distinct {@code (segment, subscription)} pairs for acks.
     */
    public CompletableFuture<Void> listOpsByTxn(String txnId, ScanConsumer consumer) {
        return store.scanByIndex(TxnPaths.TXN_OP_PREFIX, TxnPaths.IDX_OPS_BY_TXN,
                txnId, txnId,
                gr -> txnId.equals(TxnPaths.txnIdFromOpPath(gr.getStat().getPath())),
                consumer);
    }

    /**
     * Delete every {@code /txn/op} write record for {@code (segment, txnId)} — used by the TB once
     * an event tells it the txn is terminal. Path extraction follows the layout in
     * {@link TxnPaths#txnIdFromOpPath}. Best-effort: tolerates concurrent deletions via
     * {@link MetadataStore#deleteIfExists}.
     */
    public CompletableFuture<Void> deleteWriteOpsForSegmentAndTxn(String segment, String txnId) {
        return scanAndDeleteOpsForTxn(txnId, collector -> listWritesBySegment(segment, collector));
    }

    /**
     * Delete every {@code /txn/op} ack record for {@code (segment, subscription, txnId)} — used by
     * the PendingAckStore once an event tells it the txn is terminal. Same path-extraction +
     * best-effort semantics as {@link #deleteWriteOpsForSegmentAndTxn}.
     */
    public CompletableFuture<Void> deleteAckOpsForSegmentSubscriptionAndTxn(String segment, String subscription,
                                                                            String txnId) {
        return scanAndDeleteOpsForTxn(txnId,
                collector -> listAcksBySegmentSubscription(segment, subscription, collector));
    }

    /**
     * Shared implementation: invoke the supplied scan with a collector that captures only paths
     * matching {@code txnId}, then delete each captured path with the txn-scoped partition key.
     */
    private CompletableFuture<Void> scanAndDeleteOpsForTxn(
            String txnId,
            java.util.function.Function<ScanConsumer, CompletableFuture<Void>> scan) {
        java.util.List<String> paths = new java.util.ArrayList<>();
        ScanConsumer collector = new ScanConsumer() {
            @Override
            public void onNext(org.apache.pulsar.metadata.api.GetResult r) {
                if (txnId.equals(TxnPaths.txnIdFromOpPath(r.getStat().getPath()))) {
                    paths.add(r.getStat().getPath());
                }
            }

            @Override
            public void onError(Throwable throwable) {
                // The caller observes failure via the scan's returned future; logging here so
                // the cause is visible alongside the txnId context.
                log.warn().attr("txnId", txnId).exception(throwable)
                        .log("Op-record cleanup scan errored");
            }

            @Override
            public void onCompleted() {
            }
        };
        return scan.apply(collector).thenCompose(__ -> {
            Set<Option> opts = Set.of(new Option.PartitionKey(txnId));
            CompletableFuture<?>[] deletes = new CompletableFuture<?>[paths.size()];
            for (int i = 0; i < paths.size(); i++) {
                deletes[i] = store.deleteIfExists(paths.get(i), Optional.empty(), opts);
            }
            return CompletableFuture.allOf(deletes);
        });
    }

    /**
     * Stream open transactions whose deadline falls in {@code [fromMsInclusive, toMsInclusive]}.
     * Pass {@code null} on either bound for an unbounded range.
     */
    public CompletableFuture<Void> listOpenByDeadlineRange(Long fromMsInclusive, Long toMsInclusive,
                                                           ScanConsumer consumer) {
        String from = fromMsInclusive == null ? null : TxnPaths.longKey(fromMsInclusive);
        String to = toMsInclusive == null ? null : TxnPaths.longKey(toMsInclusive);
        return store.scanByIndex(TxnPaths.TXN_HEADER_PREFIX, TxnPaths.IDX_TXN_BY_DEADLINE,
                from, to,
                gr -> {
                    TxnHeader h = fromJson(gr.getValue(), TxnHeader.class);
                    if (h.getState().isTerminal()) {
                        return false;
                    }
                    long deadline = h.getCreatedAt().toEpochMilli() + h.getTimeout().toMillis();
                    return (fromMsInclusive == null || deadline >= fromMsInclusive)
                            && (toMsInclusive == null || deadline <= toMsInclusive);
                },
                consumer);
    }

    /**
     * Stream terminal transactions in {@code state} whose finalization time falls in
     * {@code [fromMsInclusive, toMsInclusive]}. Pass {@code null} on either bound for unbounded.
     */
    public CompletableFuture<Void> listFinalizedByStateAndTimeRange(TxnState state,
                                                                    Long fromMsInclusive, Long toMsInclusive,
                                                                    ScanConsumer consumer) {
        String from = fromMsInclusive == null ? state.name() + ":" : TxnPaths.finalStateKey(state, fromMsInclusive);
        String to = toMsInclusive == null
                ? state.name() + ":" + TxnPaths.MAX_LONG_KEY
                : TxnPaths.finalStateKey(state, toMsInclusive);
        return store.scanByIndex(TxnPaths.TXN_HEADER_PREFIX, TxnPaths.IDX_TXN_BY_FINAL_STATE,
                from, to,
                gr -> {
                    TxnHeader h = fromJson(gr.getValue(), TxnHeader.class);
                    if (h.getState() != state) {
                        return false;
                    }
                    if (h.getFinalizedAt() == null) {
                        return false;
                    }
                    long finalized = h.getFinalizedAt().toEpochMilli();
                    return (fromMsInclusive == null || finalized >= fromMsInclusive)
                            && (toMsInclusive == null || finalized <= toMsInclusive);
                },
                consumer);
    }

    // ---- Event publishing & subscription ----------------------------------

    /** Append a per-segment notification event. */
    public CompletableFuture<Stat> publishSegmentEvent(String segment, TxnEvent event) {
        Set<Option> opts = Set.of(new Option.PartitionKey(TxnPaths.segmentKey(segment)), APPEND_DELTAS);
        return store.put(TxnPaths.segmentEventsParent(segment), toJson(event), Optional.empty(), opts);
    }

    /** Append a per-(segment, subscription) notification event. */
    public CompletableFuture<Stat> publishSubscriptionEvent(String segment, String subscription,
                                                            TxnEvent event) {
        String pk = TxnPaths.ackIndexKey(segment, subscription);
        Set<Option> opts = Set.of(new Option.PartitionKey(pk), APPEND_DELTAS);
        return store.put(TxnPaths.subscriptionEventsParent(segment, subscription), toJson(event),
                Optional.empty(), opts);
    }

    /**
     * Subscribe to new {@link TxnEvent} entries for {@code segment}. The {@code listener} receives
     * the full generated path of the latest sequence key — fetch the value with {@link #fromJson} on
     * the {@code GetResult} from {@link MetadataStore#get(String, Set)}.
     */
    public AutoCloseable subscribeSegmentEvents(String segment, Consumer<String> listener)
            throws MetadataStoreException {
        return store.subscribeSequence(TxnPaths.segmentEventsParent(segment), listener,
                Set.of(new Option.PartitionKey(TxnPaths.segmentKey(segment))));
    }

    /** Subscribe to new {@link TxnEvent} entries for {@code (segment, subscription)}. */
    public AutoCloseable subscribeSubscriptionEvents(String segment, String subscription,
                                                     Consumer<String> listener)
            throws MetadataStoreException {
        String pk = TxnPaths.ackIndexKey(segment, subscription);
        return store.subscribeSequence(TxnPaths.subscriptionEventsParent(segment, subscription),
                listener, Set.of(new Option.PartitionKey(pk)));
    }

    // ---- Per-segment durable visibility state -----------------------------

    /**
     * Read the segment's watermark record, or {@link Optional#empty()} if it doesn't exist (a
     * fresh segment that has never had a transactional message).
     */
    public CompletableFuture<Optional<Versioned<SegmentWatermark>>> getSegmentWatermark(String segment) {
        Set<Option> opts = Set.of(new Option.PartitionKey(TxnPaths.segmentKey(segment)));
        return store.get(TxnPaths.segmentWatermarkPath(segment), opts)
                .thenApply(opt -> opt.map(gr -> new Versioned<>(
                        fromJson(gr.getValue(), SegmentWatermark.class), gr.getStat().getVersion())));
    }

    /**
     * CAS-write the segment's watermark. Pass {@link Optional#empty()} for unconditional create
     * (must not exist); pass a version from a prior {@link #getSegmentWatermark} for an update.
     */
    public CompletableFuture<Stat> casSegmentWatermark(String segment, SegmentWatermark watermark,
                                                       Optional<Long> expectedVersion) {
        Set<Option> opts = Set.of(new Option.PartitionKey(TxnPaths.segmentKey(segment)));
        return store.put(TxnPaths.segmentWatermarkPath(segment), toJson(watermark),
                expectedVersion, opts);
    }

    /**
     * Persist a per-aborted-txn record for {@code (segment, txnId)}. Also writes the
     * {@link TxnPaths#IDX_TXN_ABORTED_BY_POSITION} secondary-index entry keyed by the max
     * position so the TB can range-delete on ML trim.
     */
    public CompletableFuture<Stat> putAbortedTxn(String segment, String txnId, long maxLedgerId,
                                                 long maxEntryId) {
        AbortedTxnRecord record = new AbortedTxnRecord(maxLedgerId, maxEntryId);
        Option.SecondaryIndex idx = new Option.SecondaryIndex(TxnPaths.IDX_TXN_ABORTED_BY_POSITION,
                TxnPaths.abortedByPositionIndexKey(segment, maxLedgerId, maxEntryId));
        Set<Option> opts = Set.of(new Option.PartitionKey(TxnPaths.segmentKey(segment)), idx);
        return store.put(TxnPaths.segmentAbortedTxnPath(segment, txnId), toJson(record),
                Optional.empty(), opts);
    }

    /**
     * Stream all aborted-txn records for {@code segment} whose max position falls in
     * {@code [fromKeyInclusive, toKeyInclusive]} (use {@code null} on either bound for
     * unbounded). Use {@link TxnPaths#abortedByPositionSegmentLowerBound} /
     * {@link TxnPaths#abortedByPositionSegmentUpperBound} for the segment-scoped full range.
     */
    public CompletableFuture<Void> scanAbortedTxns(String segment,
                                                   String fromKeyInclusive, String toKeyInclusive,
                                                   ScanConsumer consumer) {
        String segKey = TxnPaths.segmentKey(segment);
        return store.scanByIndex(TxnPaths.TXN_SEGMENT_ABORTED_PREFIX,
                TxnPaths.IDX_TXN_ABORTED_BY_POSITION,
                fromKeyInclusive, toKeyInclusive,
                gr -> {
                    // Fallback for stores without native indexes: the records are flat children
                    // of TXN_SEGMENT_ABORTED_PREFIX named "<segKey>:<txnId>" — match by segKey.
                    return segKey.equals(TxnPaths.segmentKeyFromAbortedPath(gr.getStat().getPath()));
                },
                consumer);
    }

    /**
     * Delete a single aborted-txn record (and its index entry).
     */
    public CompletableFuture<Void> deleteAbortedTxn(String segment, String txnId) {
        Set<Option> opts = Set.of(new Option.PartitionKey(TxnPaths.segmentKey(segment)));
        return store.deleteIfExists(TxnPaths.segmentAbortedTxnPath(segment, txnId),
                Optional.empty(), opts);
    }

    /**
     * Delete the segment's watermark record — used by the TB at segment-teardown alongside
     * deleting any remaining aborted records.
     */
    public CompletableFuture<Void> deleteSegmentWatermark(String segment) {
        Set<Option> opts = Set.of(new Option.PartitionKey(TxnPaths.segmentKey(segment)));
        return store.deleteIfExists(TxnPaths.segmentWatermarkPath(segment), Optional.empty(), opts);
    }

    // ---- TC sequence counter ----------------------------------------------

    /**
     * Atomically increment the per-tc txnId sequence counter and return the assigned value.
     * Retries on {@link MetadataStoreException.BadVersionException} so concurrent callers
     * (within a TC partition's broker) serialise correctly. The returned value becomes a
     * txn's {@code leastSigBits}; monotonic per {@code tcId} ⟹ no txnId reuse.
     */
    public CompletableFuture<Long> nextTxnSequence(long tcId) {
        String path = TxnPaths.tcSequencePath(tcId);
        return store.get(path).thenCompose(opt -> {
            long current = opt.map(gr -> fromJson(gr.getValue(), TcSequence.class).next() - 1).orElse(-1L);
            long assigned = current + 1;
            TcSequence updated = new TcSequence(assigned + 1);
            Optional<Long> expectedVersion = opt.map(gr -> gr.getStat().getVersion())
                    .map(Optional::of).orElse(Optional.of(-1L));
            return store.put(path, toJson(updated), expectedVersion, Set.of())
                    .thenApply(stat -> assigned)
                    .exceptionallyCompose(ex -> {
                        Throwable cause = ex instanceof CompletionException && ex.getCause() != null
                                ? ex.getCause() : ex;
                        if (cause instanceof MetadataStoreException.BadVersionException) {
                            // Concurrent write — retry.
                            return nextTxnSequence(tcId);
                        }
                        return FutureUtil.failedFuture(cause);
                    });
        });
    }

    // ---- JSON helpers ------------------------------------------------------

    /** @return UTF-8 JSON bytes for {@code value}. Wraps any I/O error as {@link CompletionException}. */
    public static byte[] toJson(Object value) {
        try {
            return ObjectMapperFactory.getMapper().writer().writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw new CompletionException(new MetadataStoreException(e));
        }
    }

    /** @return the deserialized record. Wraps any I/O error as {@link CompletionException}. */
    public static <T> T fromJson(byte[] bytes, Class<T> type) {
        try {
            return ObjectMapperFactory.getMapper().reader().readValue(bytes, type);
        } catch (IOException e) {
            throw new CompletionException(new MetadataStoreException(e));
        }
    }
}
