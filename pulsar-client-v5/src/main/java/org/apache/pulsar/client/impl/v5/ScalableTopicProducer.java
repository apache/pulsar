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
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.v5.SegmentRouter.ActiveSegment;

/**
 * V5 Producer implementation for scalable topics.
 *
 * <p>Maintains a per-segment v4 ProducerImpl and routes messages by hashing
 * the message key to find the target segment. When the layout changes (split/merge),
 * segment producers are created/closed accordingly.
 */
final class ScalableTopicProducer<T> implements Producer<T>, DagWatchClient.LayoutChangeListener {

    private static final Logger LOG = Logger.get(ScalableTopicProducer.class);
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

        for (int attempt = 0; attempt < 3; attempt++) {
            long segmentId = routeMessage(key);
            var producer = getOrCreateSegmentProducer(segmentId);

            try {
                var v4MsgId = buildV4Message(producer, key, value, properties,
                        eventTime, sequenceId, deliverAfter, deliverAt, replicationClusters, txn)
                        .send();
                return new MessageIdV5(v4MsgId, segmentId);
            } catch (org.apache.pulsar.client.api.PulsarClientException.TopicTerminatedException
                     | org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException e) {
                // The segment was sealed (split/merge). We may observe this either as
                // TopicTerminated (broker reply to a still-open producer) or AlreadyClosed
                // (the v4 producer noticed first and shut itself down). Either way, drop
                // the stale per-segment producer and retry — the DAG watch will deliver
                // the new layout shortly, and routeMessage on the next attempt will land
                // on an active child.
                log.info().attr("segmentId", segmentId)
                        .attr("attempt", attempt + 1)
                        .log("Segment sealed, waiting for layout update");
                segmentProducers.remove(segmentId);
                try {
                    Thread.sleep(100L * (attempt + 1));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new PulsarClientException("Interrupted while waiting for layout update", ie);
                }
            } catch (org.apache.pulsar.client.api.PulsarClientException e) {
                throw new PulsarClientException(e.getMessage(), e);
            }
        }
        throw new PulsarClientException("Failed to send after segment termination retries");
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

        appendToDispatchChain(routedSegmentId, producer -> {
            var ackFuture = buildV4Message(producer, key, value, properties,
                    eventTime, sequenceId, deliverAfter, deliverAt, replicationClusters, txn)
                    .sendAsync();
            ackFuture.whenComplete((v4MsgId, ex) -> {
                if (ex == null) {
                    userFuture.complete(new MessageIdV5(v4MsgId, routedSegmentId));
                    return;
                }
                Throwable cause = ex instanceof java.util.concurrent.CompletionException
                        ? ex.getCause() : ex;
                boolean segmentSealed = cause
                        instanceof org.apache.pulsar.client.api.PulsarClientException
                                .TopicTerminatedException
                        || cause instanceof org.apache.pulsar.client.api.PulsarClientException
                                .AlreadyClosedException;
                if (segmentSealed && attempt < 3) {
                    log.info().attr("segmentId", routedSegmentId)
                            .attr("attempt", attempt + 1).log("Segment sealed, retrying");
                    segmentProducers.remove(routedSegmentId);
                    dispatchChains.remove(routedSegmentId);
                    CompletableFuture.delayedExecutor(
                                    100L * (attempt + 1),
                                    java.util.concurrent.TimeUnit.MILLISECONDS)
                            .execute(() -> dispatchSendAttempt(userFuture, key, value, properties,
                                    eventTime, sequenceId, deliverAfter, deliverAt,
                                    replicationClusters, txn, attempt + 1));
                } else {
                    userFuture.completeExceptionally(ex);
                }
            });
        }, userFuture);
    }

    /**
     * Append a dispatch step to the per-segment chain. The chain head is the
     * segment-producer-creation future; subsequent links complete as soon as
     * their {@code dispatchOp} returns (which calls v4 {@code sendAsync} — a
     * fast queue insert), so dispatch order strictly mirrors call order.
     * If the chain itself fails (e.g., segment producer creation failed), the
     * user-visible future is failed too.
     */
    private void appendToDispatchChain(long segmentId,
                                       Consumer<org.apache.pulsar.client.api.Producer<T>> dispatchOp,
                                       CompletableFuture<MessageIdV5> userFuture) {
        synchronized (dispatchLock) {
            var prev = dispatchChains.computeIfAbsent(segmentId,
                    id -> getOrCreateSegmentProducerAsync(id));
            var next = prev.thenApply(producer -> {
                dispatchOp.accept(producer);
                return producer;
            });
            // If the chain link itself faults (creation failure), surface it.
            next.exceptionally(ex -> {
                userFuture.completeExceptionally(ex);
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
        if (requiresExclusiveAttach()) {
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
                if (segmentProducers.containsKey(seg.segmentId())) {
                    continue;
                }
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
            // Find the segment topic name
            String segmentTopicName = null;
            for (var seg : activeSegments) {
                if (seg.segmentId() == id) {
                    segmentTopicName = seg.segmentTopicName();
                    break;
                }
            }
            if (segmentTopicName == null) {
                return CompletableFuture.failedFuture(
                        new PulsarClientException("Segment " + id + " not found in active segments"));
            }

            PulsarClientImpl v4Client = client.v4Client();
            // Clone the user-facing producer config so per-segment producers inherit
            // every builder knob (compression, batching, chunking, encryption,
            // initialSequenceId, accessMode, properties, ...) and not just the few
            // fields explicitly carried over.
            var segConf = producerConf.clone();
            segConf.setTopicName(segmentTopicName);
            if (producerConf.getProducerName() != null
                    && !producerConf.getProducerName().isEmpty()) {
                segConf.setProducerName(producerConf.getProducerName() + "-seg-" + id);
            }
            return v4Client.createSegmentProducerAsync(segConf, v4Schema);
        });
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
