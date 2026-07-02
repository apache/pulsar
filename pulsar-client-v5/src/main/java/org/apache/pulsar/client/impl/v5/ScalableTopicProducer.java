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
package org.apache.pulsar.client.impl.v5;

import io.github.merlimat.slog.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.v5.MessageBuilder;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.async.AsyncProducer;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.EntryBucketBatcherBuilder;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.v5.SegmentRouter.ActiveSegment;
import org.apache.pulsar.common.scalable.ScalableTopicConstants;

/**
 * V5 Producer implementation for scalable topics.
 *
 * <p>Maintains a per-segment v4 ProducerImpl and routes messages by hashing
 * the message key to find the target segment. When the layout changes (split/merge),
 * segment producers are created/closed accordingly.
 */
final class ScalableTopicProducer<T> implements Producer<T>, DagWatchClient.LayoutChangeListener {

    private static final Logger LOG = Logger.get(ScalableTopicProducer.class);

    /** Max attempts for a send when the target segment is gone (split/merge seal or migration
     *  termination), giving the DAG watch time to deliver the new layout before giving up. */
    private static final int SEND_RETRY_MAX_ATTEMPTS = 10;
    /** Cap on the per-attempt backoff while waiting for the new layout. */
    private static final long SEND_RETRY_MAX_BACKOFF_MS = 500L;

    private final Logger log;

    private final PulsarClientV5 client;
    private final Schema<T> v5Schema;
    private final org.apache.pulsar.client.api.Schema<T> v4Schema;
    private final ProducerConfigurationData producerConf;
    private final DagWatchClient dagWatch;
    private final SegmentRouter router;
    private final String topicName;

    /**
     * Per-segment v4 producers. Stored as futures so concurrent send-on-cold-segment
     * calls share a single creation attempt without blocking, and so callers running
     * on a netty IO thread can chain on the future asynchronously instead of forcing
     * a blocking {@code .get()} (which would deadlock against the segment producer's
     * own lookup response, processed on the same IO thread).
     */
    private final ConcurrentHashMap<Long, CompletableFuture<org.apache.pulsar.client.api.Producer<T>>>
            segmentProducers = new ConcurrentHashMap<>();

    /**
     * Per-segment dispatch chain. Each async send appends a link whose sole job
     * is to call {@code v4Producer.sendAsync(...)} (fast, synchronous queue insert)
     * once the previous link completes. This serializes the v4-side dispatch in
     * user-call order, side-stepping JDK CompletableFuture's undefined dependent
     * fire-order — which would otherwise let send N enter the v4 queue before
     * send N-1 when both are dependents of the same not-yet-ready producer
     * future. The chain head completes when the producer is ready; subsequent
     * links complete as soon as their {@code sendAsync} call has returned (they
     * do not wait for broker ack — that's the user-visible future).
     */
    private final ConcurrentHashMap<Long, CompletableFuture<org.apache.pulsar.client.api.Producer<T>>>
            dispatchChains = new ConcurrentHashMap<>();
    private final Object dispatchLock = new Object();

    /**
     * Currently in-flight async sends. {@link #flushAsync()} snapshots and
     * awaits these (each user-visible send future completes on broker ack —
     * exactly the flush guarantee).
     */
    private final Set<CompletableFuture<MessageIdV5>> inFlightSends =
            ConcurrentHashMap.newKeySet();

    // Current active segments (volatile for visibility across threads)
    private volatile List<ActiveSegment> activeSegments = List.of();

    private volatile boolean closed = false;
    private final AsyncProducerV5<T> asyncView;

    ScalableTopicProducer(PulsarClientV5 client,
                          Schema<T> v5Schema,
                          ProducerConfigurationData producerConf,
                          DagWatchClient dagWatch,
                          ClientSegmentLayout initialLayout) {
        this.client = client;
        this.v5Schema = v5Schema;
        this.v4Schema = SchemaAdapter.toV4(v5Schema);
        this.producerConf = producerConf;
        this.dagWatch = dagWatch;
        this.router = new SegmentRouter();
        this.topicName = dagWatch.topicName().toString();
        this.log = LOG.with().attr("topic", topicName).build();
        this.asyncView = new AsyncProducerV5<>(this);

        // Register for layout changes
        dagWatch.setListener(this);

        // Initialize with the current layout
        applyLayout(initialLayout);
    }

    @Override
    public String topic() {
        return topicName;
    }

    @Override
    public String producerName() {
        return producerConf.getProducerName();
    }

    @Override
    public MessageBuilder<T> newMessage() {
        return new MessageBuilderV5<>(this);
    }

    @Override
    public long lastSequenceId() {
        // Reflect the configured initialSequenceId even before any segment producer has
        // been created (segment producers are spun up lazily on first send), so a caller
        // that sets initialSequenceId(N) and immediately reads lastSequenceId() sees N.
        long max = producerConf.getInitialSequenceId() == null
                ? -1L : producerConf.getInitialSequenceId();
        for (var future : segmentProducers.values()) {
            // Best-effort: only consult producers that have finished initializing.
            if (future.isDone() && !future.isCompletedExceptionally()) {
                max = Math.max(max, future.join().getLastSequenceId());
            }
        }
        return max;
    }

    @Override
    public AsyncProducer<T> async() {
        return asyncView;
    }

    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Close interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof PulsarClientException pce) {
                throw pce;
            }
            throw new PulsarClientException(cause);
        }
    }

    /**
     * Send a message synchronously with routing. Called by MessageBuilderV5.
     * Returns a MessageIdV5 that includes the segment ID for ack routing.
     */
    MessageIdV5 sendInternal(
            String key, T value, java.util.Map<String, String> properties,
            java.time.Instant eventTime, Long sequenceId,
            java.time.Duration deliverAfter, java.time.Instant deliverAt,
            java.util.List<String> replicationClusters,
            org.apache.pulsar.client.api.v5.Transaction txn) throws PulsarClientException {

        PulsarClientException lastError = null;
        for (int attempt = 0; attempt < SEND_RETRY_MAX_ATTEMPTS; attempt++) {
            long segmentId = routeMessage(key);
            try {
                var producer = getOrCreateSegmentProducer(segmentId);
                var v4MsgId = buildV4Message(producer, key, value, properties,
                        eventTime, sequenceId, deliverAfter, deliverAt, replicationClusters, txn)
                        .send();
                return new MessageIdV5(v4MsgId, segmentId);
            } catch (PulsarClientException e) {
                // Thrown while (re)creating the per-segment producer — already a V5 exception
                // (it may wrap a v4 TopicTerminated/AlreadyClosed cause).
                if (!isSegmentGoneError(e)) {
                    throw e;
                }
                lastError = e;
            } catch (org.apache.pulsar.client.api.PulsarClientException e) {
                // Thrown by the v4 producer's send().
                if (!isSegmentGoneError(e)) {
                    throw new PulsarClientException(e.getMessage(), e);
                }
                lastError = new PulsarClientException(e.getMessage(), e);
            }
            // The target segment is gone: sealed by a split/merge, or terminated by a
            // regular-to-scalable migration. Drop the stale per-segment producer and wait
            // for the DAG watch to deliver the new layout; routeMessage on the next attempt
            // lands on an active child.
            log.info().attr("segmentId", segmentId).attr("attempt", attempt + 1)
                    .log("Target segment gone, waiting for layout update");
            segmentProducers.remove(segmentId);
            try {
                Thread.sleep(Math.min(100L * (attempt + 1), SEND_RETRY_MAX_BACKOFF_MS));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new PulsarClientException("Interrupted while waiting for layout update", ie);
            }
        }
        throw lastError != null ? lastError
                : new PulsarClientException("Failed to send after segment termination retries");
    }

    /**
     * True if {@code t} (or one of its causes) signals that the target segment is gone —
     * sealed by a split/merge or terminated by a regular-to-scalable migration — so the send
     * should be retried once the new layout arrives. Handles both the v4 exceptions thrown by
     * {@code send()} and the V5-wrapped exceptions thrown while (re)creating the per-segment
     * producer on a now-terminated topic.
     */
    private static boolean isSegmentGoneError(Throwable t) {
        for (Throwable cause = t; cause != null; cause = cause.getCause()) {
            if (cause instanceof org.apache.pulsar.client.api.PulsarClientException.TopicTerminatedException) {
                return true;
            }
            if (cause instanceof org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException) {
                return true;
            }
            // The per-segment producer-creation path can surface the broker's terminated /
            // already-closed error as a plain (untyped) PulsarClientException whose message
            // carries the server-side class name; match on that too.
            String msg = cause.getMessage();
            if (msg != null
                    && (msg.contains("TopicTerminated") || msg.contains("already terminated")
                        || msg.contains("AlreadyClosed"))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Send a message asynchronously with routing. Called by AsyncMessageBuilderV5.
     * Returns a future of MessageIdV5 that includes the segment ID.
     */
    CompletableFuture<MessageIdV5> sendInternalAsync(
            String key, T value, java.util.Map<String, String> properties,
            java.time.Instant eventTime, Long sequenceId,
            java.time.Duration deliverAfter, java.time.Instant deliverAt,
            java.util.List<String> replicationClusters,
            org.apache.pulsar.client.api.v5.Transaction txn) {

        CompletableFuture<MessageIdV5> userFuture = new CompletableFuture<>();
        inFlightSends.add(userFuture);
        userFuture.whenComplete((__, ___) -> inFlightSends.remove(userFuture));
        dispatchSendAttempt(userFuture, key, value, properties, eventTime, sequenceId,
                deliverAfter, deliverAt, replicationClusters, txn, 0);
        return userFuture;
    }

    private void dispatchSendAttempt(
            CompletableFuture<MessageIdV5> userFuture,
            String key, T value, java.util.Map<String, String> properties,
            java.time.Instant eventTime, Long sequenceId,
            java.time.Duration deliverAfter, java.time.Instant deliverAt,
            java.util.List<String> replicationClusters,
            org.apache.pulsar.client.api.v5.Transaction txn, int attempt) {

        long segmentId;
        try {
            segmentId = routeMessage(key);
        } catch (Exception e) {
            userFuture.completeExceptionally(e);
            return;
        }
        final long routedSegmentId = segmentId;

        // Re-dispatch this message on the next attempt. Used when the target segment is gone
        // — sealed by a split/merge or terminated by a regular-to-scalable migration — and
        // the DAG watch is expected to refresh the layout shortly so routeMessage lands on an
        // active child.
        Runnable retry = () -> {
            segmentProducers.remove(routedSegmentId);
            dispatchChains.remove(routedSegmentId);
            CompletableFuture.delayedExecutor(
                            Math.min(100L * (attempt + 1), SEND_RETRY_MAX_BACKOFF_MS),
                            java.util.concurrent.TimeUnit.MILLISECONDS)
                    .execute(() -> dispatchSendAttempt(userFuture, key, value, properties,
                            eventTime, sequenceId, deliverAfter, deliverAt,
                            replicationClusters, txn, attempt + 1));
        };

        appendToDispatchChain(routedSegmentId,
                producer -> {
                    var ackFuture = buildV4Message(producer, key, value, properties,
                            eventTime, sequenceId, deliverAfter, deliverAt, replicationClusters, txn)
                            .sendAsync();
                    ackFuture.whenComplete((v4MsgId, ex) -> {
                        if (ex == null) {
                            userFuture.complete(new MessageIdV5(v4MsgId, routedSegmentId));
                        } else {
                            // Failure from the v4 send (e.g. the segment sealed mid-flight).
                            handleAsyncSegmentFailure(userFuture, routedSegmentId, attempt, ex, retry);
                        }
                    });
                },
                // Failure while (re)creating the per-segment producer — e.g. the partition was
                // terminated by a migration between routing and creation.
                createEx -> handleAsyncSegmentFailure(userFuture, routedSegmentId, attempt, createEx, retry));
    }

    /**
     * Decide whether an async send failure should be retried. If the target segment is gone
     * (a split/merge seal or a migration termination) and the retry budget isn't exhausted,
     * run {@code retry}; otherwise fail the user-visible future. Covers both the v4 send
     * failure and the per-segment producer-creation failure.
     */
    private void handleAsyncSegmentFailure(CompletableFuture<MessageIdV5> userFuture, long segmentId,
                                           int attempt, Throwable ex, Runnable retry) {
        Throwable cause = ex instanceof java.util.concurrent.CompletionException ? ex.getCause() : ex;
        if (isSegmentGoneError(cause) && attempt < SEND_RETRY_MAX_ATTEMPTS) {
            log.info().attr("segmentId", segmentId).attr("attempt", attempt + 1)
                    .log("Target segment gone, retrying async send after layout update");
            retry.run();
        } else {
            userFuture.completeExceptionally(ex);
        }
    }

    /**
     * Append a dispatch step to the per-segment chain. The chain head is the
     * segment-producer-creation future; subsequent links complete as soon as
     * their {@code dispatchOp} returns (which calls v4 {@code sendAsync} — a
     * fast queue insert), so dispatch order strictly mirrors call order.
     * If the chain itself fails (e.g., segment producer creation failed),
     * {@code onCreateFailure} is invoked so the caller can retry (when the segment
     * is merely gone) or fail the user-visible future.
     */
    private void appendToDispatchChain(long segmentId,
                                       Consumer<org.apache.pulsar.client.api.Producer<T>> dispatchOp,
                                       Consumer<Throwable> onCreateFailure) {
        synchronized (dispatchLock) {
            var prev = dispatchChains.computeIfAbsent(segmentId,
                    id -> getOrCreateSegmentProducerAsync(id));
            var next = prev.thenApply(producer -> {
                dispatchOp.accept(producer);
                return producer;
            });
            // If the chain link itself faults (creation failure), hand it to the caller.
            next.exceptionally(ex -> {
                onCreateFailure.accept(ex);
                return null;
            });
            dispatchChains.put(segmentId, next);
        }
    }

    private org.apache.pulsar.client.api.TypedMessageBuilder<T> buildV4Message(
            org.apache.pulsar.client.api.Producer<T> producer,
            String key, T value, java.util.Map<String, String> properties,
            java.time.Instant eventTime, Long sequenceId,
            java.time.Duration deliverAfter, java.time.Instant deliverAt,
            java.util.List<String> replicationClusters,
            org.apache.pulsar.client.api.v5.Transaction txn) {

        org.apache.pulsar.client.api.transaction.Transaction v4Txn = TransactionV5.unwrap(txn);
        var msgBuilder = (v4Txn != null ? producer.newMessage(v4Txn) : producer.newMessage())
                .value(value);

        if (key != null) {
            msgBuilder.key(key);
        }
        if (properties != null && !properties.isEmpty()) {
            msgBuilder.properties(properties);
        }
        if (eventTime != null) {
            msgBuilder.eventTime(eventTime.toEpochMilli());
        }
        if (sequenceId != null) {
            msgBuilder.sequenceId(sequenceId);
        }
        if (deliverAfter != null) {
            msgBuilder.deliverAfter(deliverAfter.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        }
        if (deliverAt != null) {
            msgBuilder.deliverAt(deliverAt.toEpochMilli());
        }
        if (replicationClusters != null) {
            msgBuilder.replicationClusters(replicationClusters);
        }

        return msgBuilder;
    }

    /**
     * Flush all in-flight async sends. Each user-visible send future completes
     * on broker ack, so awaiting them is exactly the "all sends so far have
     * landed" guarantee flush() owes the caller. Snapshotting the set means
     * sends issued *after* this call aren't waited on (matches v4 contract).
     */
    CompletableFuture<Void> flushAsync() {
        var pending = inFlightSends.toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(pending);
    }

    CompletableFuture<Void> closeAsync() {
        closed = true;
        dagWatch.close();

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (var future : segmentProducers.values()) {
            // If creation failed, there's nothing to close — swallow so a single bad
            // segment doesn't fail the overall close.
            futures.add(future.thenCompose(p -> p.closeAsync())
                    .exceptionally(__ -> null));
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .whenComplete((__, ___) -> {
                    segmentProducers.clear();
                    dispatchChains.clear();
                });
    }

    // --- Layout change handling ---

    @Override
    public void onLayoutChange(ClientSegmentLayout newLayout, ClientSegmentLayout oldLayout) {
        applyLayout(newLayout);
        // After a layout update under an exclusive access mode, we want to claim any
        // newly-introduced segments eagerly so the exclusivity guarantee covers the
        // whole topic, not just segments hit by the next send. Best-effort: this runs
        // off the DagWatchClient callback and any failure is logged; the next send to
        // that segment will surface the error via the normal PulsarClientException
        // path. (The initial-create path uses {@link #eagerAttachInitialAsync} for
        // strict claim.)
        if (requiresExclusiveAttach() && oldLayout != null) {
            CompletableFuture.runAsync(() -> {
                for (var seg : newLayout.activeSegments()) {
                    if (segmentProducers.containsKey(seg.segmentId())) {
                        continue;
                    }
                    try {
                        getOrCreateSegmentProducer(seg.segmentId());
                    } catch (PulsarClientException e) {
                        log.warn().attr("segmentId", seg.segmentId())
                                .exceptionMessage(e)
                                .log("Eager exclusive attach failed; will retry on next send");
                    }
                }
            }, client.v4Client().getInternalExecutorService());
        }
    }

    private void applyLayout(ClientSegmentLayout layout) {
        this.activeSegments = layout.activeSegments();

        // Determine which segments are new and which are gone
        Set<Long> newSegmentIds = ConcurrentHashMap.newKeySet();
        for (var seg : layout.activeSegments()) {
            newSegmentIds.add(seg.segmentId());
        }

        // Close producers for segments that are no longer active
        for (var entry : segmentProducers.entrySet()) {
            if (!newSegmentIds.contains(entry.getKey())) {
                log.info().attr("segmentId", entry.getKey())
                        .log("Closing producer for sealed segment");
                entry.getValue()
                        .thenCompose(p -> p.closeAsync())
                        .whenComplete((__, ex) -> {
                            if (ex != null) {
                                log.warn().attr("segmentId", entry.getKey())
                                        .exceptionMessage(ex).log("Error closing producer for segment");
                            }
                        });
                segmentProducers.remove(entry.getKey());
                dispatchChains.remove(entry.getKey());
            }
        }

        log.info().attr("epoch", layout.epoch())
                .attr("activeSegments", newSegmentIds).log("Layout applied");
    }

    /**
     * Strict variant of the eager attach used at initial create time: surfaces any
     * exclusivity failure as a {@link PulsarClientException} so {@code create()} fails
     * up front instead of silently deferring the collision to first send.
     */
    CompletableFuture<Void> eagerAttachInitialAsync() {
        if (!requiresExclusiveAttach()) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.runAsync(() -> {
            for (var seg : activeSegments) {
                try {
                    getOrCreateSegmentProducer(seg.segmentId());
                } catch (PulsarClientException e) {
                    throw new java.util.concurrent.CompletionException(e);
                }
            }
        }, client.v4Client().getInternalExecutorService());
    }

    private boolean requiresExclusiveAttach() {
        ProducerAccessMode mode = producerConf.getAccessMode();
        return mode == ProducerAccessMode.Exclusive
                || mode == ProducerAccessMode.ExclusiveWithFencing
                || mode == ProducerAccessMode.WaitForExclusive;
    }

    // --- Internal ---

    private long routeMessage(String key) {
        List<ActiveSegment> segments = activeSegments;
        if (key != null) {
            return router.route(key, segments);
        } else {
            return router.routeRoundRobin(segments);
        }
    }

    /**
     * Async accessor for the per-segment v4 producer. Returns a shared future so
     * concurrent send-on-cold-segment callers race-free funnel through a single
     * creation attempt — and so callers running on a netty IO thread (e.g. the
     * V5 DLQ dispatch off a v4 receive callback) can chain via {@code thenCompose}
     * instead of blocking on {@code .get()} (which would deadlock against the
     * segment producer's own lookup response, processed on the same IO thread).
     */
    private CompletableFuture<org.apache.pulsar.client.api.Producer<T>> getOrCreateSegmentProducerAsync(
            long segmentId) {
        return segmentProducers.computeIfAbsent(segmentId, id -> {
            // Find the segment and the URI to attach the per-segment v4 producer to.
            // Regular segments use the computed segment:// URI; legacy segments (synthetic
            // layouts wrapping an externally managed persistent:// topic) use that URI directly.
            ActiveSegment segment = null;
            for (var seg : activeSegments) {
                if (seg.segmentId() == id) {
                    segment = seg;
                    break;
                }
            }
            if (segment == null) {
                return CompletableFuture.failedFuture(
                        new PulsarClientException("Segment " + id + " not found in active segments"));
            }

            PulsarClientImpl v4Client = client.v4Client();
            // Clone the user-facing producer config so per-segment producers inherit
            // every builder knob (compression, batching, chunking, encryption,
            // initialSequenceId, accessMode, properties, ...) and not just the few
            // fields explicitly carried over.
            var segConf = producerConf.clone();
            segConf.setTopicName(segment.attachTopicName());
            // Only legacy segments wrap a persistent:// topic that the regular-to-scalable
            // migration pre-check (PIP-475) inspects, so mark just those connections as
            // V5-managed — connections to real segment:// topics are never examined.
            if (segment.isLegacy()) {
                segConf.getProperties().put(
                        ScalableTopicConstants.V5_MANAGED_METADATA_KEY,
                        ScalableTopicConstants.V5_MANAGED_METADATA_VALUE);
            }
            if (producerConf.getProducerName() != null
                    && !producerConf.getProducerName().isEmpty()) {
                segConf.setProducerName(producerConf.getProducerName() + "-seg-" + id);
            }
            applyEntryBucketing(segConf, segment);
            return v4Client.createSegmentProducerAsync(segConf, v4Schema);
        });
    }

    /**
     * PIP-486: configure a per-segment producer's batching for entry-bucketing. End-to-end encryption
     * disables batching (an encrypted batch can't be reshaped if re-routed across a divergent layout);
     * otherwise, when batching is enabled, group the segment's batches by entry-bucket and stamp each
     * entry's effective entry-bucket hash range. A segment's bucketing is immutable for its life.
     *
     * <p>The stamp is written for every segment, including single-bucket ones (N = 1, e.g. the
     * legacy/synthetic layouts wrapping a regular {@code persistent://} topic): the effective hash
     * range is standalone metadata a consumer or a geo-replicator can use to check whether a batch
     * still lands cleanly in one bucket of a possibly-different target layout, independent of how any
     * single broker dispatches it.
     */
    static void applyEntryBucketing(ProducerConfigurationData segConf, ActiveSegment segment) {
        if (segConf.isEncryptionEnabled()) {
            segConf.setBatchingEnabled(false);
        } else if (segConf.isBatchingEnabled()) {
            segConf.setBatcherBuilder(new EntryBucketBatcherBuilder(segment.entryBucketSplits()));
        }
    }

    /**
     * Sync wrapper around {@link #getOrCreateSegmentProducerAsync}. Only safe to
     * call from user threads (never from a netty IO thread) since it blocks until
     * the segment producer is ready.
     */
    private org.apache.pulsar.client.api.Producer<T> getOrCreateSegmentProducer(long segmentId)
            throws PulsarClientException {
        try {
            return getOrCreateSegmentProducerAsync(segmentId).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Interrupted while creating segment producer", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof org.apache.pulsar.client.api.PulsarClientException v4Exc) {
                throw new PulsarClientException(v4Exc.getMessage(), v4Exc);
            }
            if (cause instanceof PulsarClientException v5Exc) {
                throw v5Exc;
            }
            throw new PulsarClientException(cause != null ? cause : e);
        }
    }
}
