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
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.Messages;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.StreamConsumer;
import org.apache.pulsar.client.api.v5.Transaction;
import org.apache.pulsar.client.api.v5.async.AsyncStreamConsumer;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.v5.SegmentRouter.ActiveSegment;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.ScalableTopicConstants;
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * V5 StreamConsumer implementation for scalable topics.
 *
 * <p>Maintains a v4 Consumer per assigned segment. A segment this consumer owns whole is subscribed
 * {@code Exclusive} (single-active dispatch); a segment shared by entry-bucket (PIP-486, when the
 * controller fans it out across several consumers) is subscribed {@code Key_Shared} STICKY declaring
 * exactly the owned bucket ranges. Messages from all segments are multiplexed into a single receive
 * queue.
 *
 * <p>Each delivered message carries a <em>position vector</em>: a snapshot of the
 * latest delivered message ID per segment at the moment that message enters the
 * queue. When the application calls {@link #acknowledgeCumulative(MessageId)},
 * every segment is acknowledged up to the position recorded in that vector —
 * cumulatively for whole segments, and as individual acks of the delivered ids for
 * bucket-shared segments (Key_Shared forbids cumulative acks). This ensures that
 * acknowledging a single message correctly advances all segments, not just the one
 * it came from.
 */
final class ScalableStreamConsumer<T>
        implements StreamConsumer<T>, ScalableConsumerClient.AssignmentChangeListener {

    private static final Logger LOG = Logger.get(ScalableStreamConsumer.class);
    private final Logger log;

    private final PulsarClientV5 client;
    private final Schema<T> v5Schema;
    private final org.apache.pulsar.client.api.Schema<T> v4Schema;
    private final ConsumerConfigurationData<T> consumerConf;
    private final ScalableConsumerClient session;
    private final String topicName;
    private final String subscriptionName;

    /**
     * Per-segment v4 consumers. Stores futures so concurrent operations (ack, close)
     * can chain on in-flight subscribes without racing against subscribe completion.
     */
    private final ConcurrentHashMap<Long, CompletableFuture<org.apache.pulsar.client.api.Consumer<T>>>
            segmentConsumers = new ConcurrentHashMap<>();
    // PIP-486: the entry-bucket ranges each segment consumer was last subscribed with, so a change in
    // ownership (a consumer joined/left) re-subscribes the segment with the new ranges.
    private final ConcurrentHashMap<Long, List<HashRange>> segmentBucketRanges = new ConcurrentHashMap<>();

    /**
     * Tracks the latest message ID delivered from each segment. Updated atomically
     * inside {@link #startReceiveLoop} before the message is enqueued, and snapshot
     * into each {@link MessageIdV5} so cumulative acks cover all segments.
     */
    private final ConcurrentHashMap<Long, org.apache.pulsar.client.api.MessageId> latestDelivered =
            new ConcurrentHashMap<>();

    /**
     * PIP-486: for bucket-shared segments — Key_Shared under the hood, where cumulative acks are not
     * permitted — the ids delivered to this consumer and not yet acked, in delivery order. A cumulative
     * ack up to a segment's vector position is translated into individually acking every tracked id up
     * to that position. Whole-segment (Exclusive) consumers have no entry here and ack cumulatively.
     */
    private final ConcurrentHashMap<Long, ConcurrentLinkedQueue<org.apache.pulsar.client.api.MessageId>>
            sharedSegmentUnacked = new ConcurrentHashMap<>();

    /**
     * PIP-486: per whole-segment (Exclusive) consumer, the highest position the application has
     * cumulatively acked — the release drain barrier for segments without individual-ack tracking.
     */
    private final ConcurrentHashMap<Long, org.apache.pulsar.client.api.MessageId> lastCumulativeAcked =
            new ConcurrentHashMap<>();

    /**
     * PIP-486: segments paused for a release — their receive loops stop re-arming so what was
     * already handed to the application can drain before the segment consumer is closed.
     */
    private final Set<Long> pausedSegments = ConcurrentHashMap.newKeySet();

    /**
     * PIP-486: segment consumers currently draining for a release. A drain completes only when the
     * application acks what it was handed, so the ack path must keep reaching the old consumer after
     * its {@link #segmentConsumers} slot is vacated (or repopulated with the re-subscribe chain on a
     * mode flip) — {@link #ackSegmentUpTo} consults this map first.
     */
    private final ConcurrentHashMap<Long, CompletableFuture<org.apache.pulsar.client.api.Consumer<T>>>
            drainingConsumers = new ConcurrentHashMap<>();

    /**
     * PIP-486: the broker acks issued for a segment while it is draining. The release barrier
     * waits for these to settle, so the old consumer is not closed while its acks are still in
     * flight (a close racing an unapplied ack would redeliver messages the application already
     * processed). Settling, not succeeding: a failed ack resolves to redelivery, which is the
     * correct at-least-once outcome and must not wedge the release.
     */
    private final ConcurrentHashMap<Long, ConcurrentLinkedQueue<CompletableFuture<?>>>
            pendingDrainAcks = new ConcurrentHashMap<>();

    /**
     * PIP-486: per-segment receive epoch, bumped when a release drain starts. Each receive-loop
     * arm captures the epoch, and a completion whose epoch is stale is discarded without touching
     * the tracking maps or the application — the message stays unacked and redelivers, in order,
     * to the segment's next owner. This fences the receive that was already in flight when the
     * drain paused the segment.
     */
    private final ConcurrentHashMap<Long, Long> segmentReceiveEpoch = new ConcurrentHashMap<>();

    /** Latest controller assignment; {@link #reconcile()} converges the segment consumers onto it. */
    private volatile List<ActiveSegment> latestAssignment;
    /** Coalesces concurrent reconcile attempts; only one runs at a time. */
    private final AtomicBoolean reconcileInProgress = new AtomicBoolean(false);
    private final Backoff reconcileBackoff = Backoff.builder()
            .initialDelay(Duration.ofMillis(100))
            .maxBackoff(Duration.ofSeconds(30))
            .build();

    private final V5ReceiveQueue<T> receiveQueue;
    /**
     * Where each per-segment receive loop deposits a freshly-arrived message. Defaults
     * to enqueueing on {@link #receiveQueue} for the user's {@link #receive()} to pull;
     * the multi-topic wrapper overrides this to forward into its shared multiplexed
     * queue, applying its own multi-topic position-vector capture in the process.
     */
    private final MessageSink<T> messageSink;

    private volatile boolean closed = false;
    private final AsyncStreamConsumerV5<T> asyncView;

    private ScalableStreamConsumer(PulsarClientV5 client,
                                   Schema<T> v5Schema,
                                   ConsumerConfigurationData<T> consumerConf,
                                   ScalableConsumerClient session,
                                   String topicName,
                                   MessageSink<T> messageSink) {
        this.client = client;
        this.v5Schema = v5Schema;
        this.v4Schema = SchemaAdapter.toV4(v5Schema);
        this.consumerConf = consumerConf;
        this.session = session;
        this.topicName = topicName;
        this.subscriptionName = consumerConf.getSubscriptionName();
        this.receiveQueue = new V5ReceiveQueue<>(
                client.v4Client().externalExecutorProvider().getExecutor(), client.v4Client().timer(),
                consumerConf.getReceiverQueueSize());
        this.messageSink = messageSink != null ? messageSink : receiveQueue::offer;
        this.log = LOG.with().attr("topic", topicName).attr("subscription", subscriptionName).build();
        this.asyncView = new AsyncStreamConsumerV5<>(this);
    }

    /**
     * Create a fully initialized consumer asynchronously. The session has already
     * registered with the controller and the {@code initialAssignment} list contains
     * the segments this consumer should attach to. The returned future completes only
     * after every assigned segment has been successfully subscribed.
     */
    static <T> CompletableFuture<StreamConsumer<T>> createAsync(PulsarClientV5 client,
                                                                Schema<T> v5Schema,
                                                                ConsumerConfigurationData<T> consumerConf,
                                                                ScalableConsumerClient session,
                                                                String topicName,
                                                                List<ActiveSegment> initialAssignment) {
        return createAsyncImpl(client, v5Schema, consumerConf, session, topicName, initialAssignment, null)
                .thenApply(c -> c);
    }

    /**
     * Like {@link #createAsync} but resolves to the concrete impl type and accepts an
     * optional external message sink. Used by {@link MultiTopicStreamConsumer}: it
     * passes a sink that forwards into the shared multiplexed queue, replacing the
     * per-topic pump thread with direct delivery.
     */
    static <T> CompletableFuture<ScalableStreamConsumer<T>> createAsyncImpl(
            PulsarClientV5 client,
            Schema<T> v5Schema,
            ConsumerConfigurationData<T> consumerConf,
            ScalableConsumerClient session,
            String topicName,
            List<ActiveSegment> initialAssignment,
            MessageSink<T> messageSink) {
        ScalableStreamConsumer<T> consumer = new ScalableStreamConsumer<>(
                client, v5Schema, consumerConf, session, topicName, messageSink);
        consumer.latestAssignment = initialAssignment;
        return consumer.subscribeInitialWithRetry(initialAssignment)
                .thenApply(__ -> {
                    session.setListener(consumer);
                    return consumer;
                })
                .exceptionallyCompose(ex -> consumer.closeAsync().handle((__, ___) -> {
                    throw ex instanceof CompletionException ce ? ce : new CompletionException(ex);
                }));
    }

    /**
     * Multi-topic ack hook. Synthesises a {@link MessageIdV5} carrying the supplied
     * vector and routes it through the regular cumulative-ack path so segments are
     * acked up to the recorded positions. Used by {@link MultiTopicStreamConsumer}
     * to fan out a cumulative ack across every per-topic consumer.
     */
    void ackUpToVector(java.util.Map<Long, org.apache.pulsar.client.api.MessageId> vector) {
        if (vector == null || vector.isEmpty()) {
            return;
        }
        // The constructed id only needs the positionVector; v4MessageId / segmentId are
        // unused on the cumulative-ack path. Pick any value for the non-vector slots —
        // earliest is convenient and won't accidentally satisfy a peer's check.
        var synthetic = new MessageIdV5(org.apache.pulsar.client.api.MessageId.earliest,
                MessageIdV5.NO_SEGMENT, vector);
        acknowledgeCumulative(synthetic);
    }

    @Override
    public String topic() {
        return topicName;
    }

    @Override
    public String subscription() {
        return subscriptionName;
    }

    @Override
    public String consumerName() {
        return consumerConf.getConsumerName();
    }

    @Override
    public Message<T> receive() throws PulsarClientException {
        return receiveQueue.take();
    }

    @Override
    public Message<T> receive(Duration timeout) throws PulsarClientException {
        return receiveQueue.poll(timeout);
    }

    @Override
    public Messages<T> receiveMulti(int maxNumMessages, Duration timeout) throws PulsarClientException {
        return new MessagesV5<>(receiveQueue.receiveMulti(maxNumMessages, timeout));
    }

    @Override
    public void acknowledgeCumulative(MessageId messageId) {
        if (!(messageId instanceof MessageIdV5 id)) {
            throw new IllegalArgumentException("Expected MessageIdV5, got: " + messageId.getClass());
        }

        // Ack each segment up to the position recorded in the vector
        for (var entry : id.positionVector().entrySet()) {
            ackSegmentUpTo(entry.getKey(), entry.getValue(), null);
        }
    }

    @Override
    public void acknowledgeCumulative(MessageId messageId, Transaction txn) {
        if (!(messageId instanceof MessageIdV5 id)) {
            throw new IllegalArgumentException("Expected MessageIdV5, got: " + messageId.getClass());
        }
        var v4Txn = TransactionV5.unwrap(txn);
        for (var entry : id.positionVector().entrySet()) {
            ackSegmentUpTo(entry.getKey(), entry.getValue(), v4Txn);
        }
    }

    /**
     * Ack one segment up to the given position. Whole-segment (Exclusive) consumers ack
     * cumulatively. PIP-486 bucket-shared segments are Key_Shared underneath, where cumulative acks
     * are not permitted — the ack is translated into individually acking every delivered-but-unacked
     * id up to the position (exactly the messages this consumer received: its buckets' share).
     */
    private void ackSegmentUpTo(long segmentId, org.apache.pulsar.client.api.MessageId position,
                                org.apache.pulsar.client.api.transaction.Transaction v4Txn) {
        // A draining consumer takes precedence: during a release the segment's slot in
        // segmentConsumers is vacated (or already holds the re-subscribe chain), but the acks that
        // complete the drain must still reach the consumer being drained.
        var draining = drainingConsumers.get(segmentId);
        var future = draining != null ? draining : segmentConsumers.get(segmentId);
        if (future == null) {
            return;
        }
        var unacked = sharedSegmentUnacked.get(segmentId);
        if (unacked == null) {
            lastCumulativeAcked.merge(segmentId, position,
                    (a, b) -> a.compareTo(b) >= 0 ? a : b);
            trackDrainAck(segmentId, future.thenCompose(c ->
                    v4Txn == null ? c.acknowledgeCumulativeAsync(position)
                            : c.acknowledgeCumulativeAsync(position, v4Txn)));
            return;
        }
        // Redeliveries can enqueue ids out of order, so scan the whole queue rather than stopping at
        // the first id past the position. Concurrent acks may race on an id; individually re-acking
        // an already-acked id is a harmless no-op.
        List<org.apache.pulsar.client.api.MessageId> toAck = new ArrayList<>();
        for (var it = unacked.iterator(); it.hasNext();) {
            var msgId = it.next();
            if (msgId.compareTo(position) <= 0) {
                it.remove();
                toAck.add(msgId);
            }
        }
        if (toAck.isEmpty()) {
            return;
        }
        trackDrainAck(segmentId, future.thenCompose(c -> {
            if (v4Txn == null) {
                return c.acknowledgeAsync(toAck);
            }
            List<CompletableFuture<Void>> acks = new ArrayList<>(toAck.size());
            for (var msgId : toAck) {
                acks.add(c.acknowledgeAsync(msgId, v4Txn));
            }
            return FutureUtil.waitForAll(acks);
        }));
    }

    /**
     * While the segment is draining for a release, record the broker ack so the release barrier
     * can wait for it to settle before the old consumer is closed.
     */
    private void trackDrainAck(long segmentId, CompletableFuture<?> ackFuture) {
        if (!drainingConsumers.containsKey(segmentId)) {
            return;
        }
        pendingDrainAcks.computeIfAbsent(segmentId, __ -> new ConcurrentLinkedQueue<>())
                .add(ackFuture.exceptionally(ex -> null));
    }

    @Override
    public AsyncStreamConsumer<T> async() {
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
            throw new PulsarClientException(e.getCause());
        }
    }

    // --- Async internals ---

    CompletableFuture<Message<T>> receiveAsync() {
        return receiveQueue.receiveAsync();
    }

    CompletableFuture<Message<T>> receiveAsync(Duration timeout) {
        return receiveQueue.receiveAsync(timeout);
    }

    CompletableFuture<List<Message<T>>> receiveMultiAsync(int maxNumMessages, Duration timeout) {
        return receiveQueue.receiveMultiAsync(maxNumMessages, timeout);
    }

    CompletableFuture<Void> closeAsync() {
        closed = true;
        receiveQueue.close();
        session.close();

        // Draining consumers live outside segmentConsumers; their drain loops observe the closed
        // flag and finish on their own, but they are closed here too (idempotent) so this future
        // does not complete before they are gone. Two ordered snapshots, segmentConsumers first:
        // a release migrates a consumer by registering it in drainingConsumers BEFORE vacating its
        // segmentConsumers slot, so this order guarantees a concurrently-migrating consumer shows
        // up in at least one snapshot (a single concatenated traversal binds both spliterators up
        // front and could miss it in both).
        Set<CompletableFuture<org.apache.pulsar.client.api.Consumer<T>>> toClose =
                new LinkedHashSet<>(segmentConsumers.values());
        toClose.addAll(drainingConsumers.values());
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (var future : toClose) {
            futures.add(future
                    .handle((consumer, ex) -> consumer)
                    .thenCompose(consumer -> consumer != null ? consumer.closeAsync()
                            : CompletableFuture.completedFuture(null)));
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .whenComplete((__, ___) -> {
                    segmentConsumers.clear();
                    drainingConsumers.clear();
                    pendingDrainAcks.clear();
                    sharedSegmentUnacked.clear();
                    lastCumulativeAcked.clear();
                });
    }

    // --- Assignment change handling ---

    @Override
    public void onAssignmentChange(List<ActiveSegment> newSegments, List<ActiveSegment> oldSegments) {
        // Store the target and kick off a reconcile; a reconcile already in flight re-reads
        // latestAssignment when it finishes. Fully async: safe on the netty IO thread that
        // delivered the update.
        latestAssignment = newSegments;
        reconcile();
    }

    /**
     * Initial subscribe with retries for the transient rebalance rejections. Joining a group
     * rebalances it: until a previous owner has released a segment (or shrunk its declared bucket
     * ranges), our subscribe is rejected — {@code ConsumerBusy} while it still holds the segment,
     * {@code ConsumerAssignError} while its STICKY ranges still overlap ours — and neither is
     * retried at the v4 layer. Bounded by the client operation timeout; any other failure fails
     * the subscribe immediately, preserving fail-fast for real errors.
     */
    private CompletableFuture<Void> subscribeInitialWithRetry(List<ActiveSegment> assigned) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(
                client.v4Client().getConfiguration().getOperationTimeoutMs());
        attemptInitialSubscribe(assigned, deadlineNanos, result);
        return result;
    }

    private void attemptInitialSubscribe(List<ActiveSegment> assigned, long deadlineNanos,
                                         CompletableFuture<Void> result) {
        subscribeAssigned(assigned).whenComplete((__, ex) -> {
            if (ex == null) {
                reconcileBackoff.reset();
                result.complete(null);
                return;
            }
            Throwable cause = FutureUtil.unwrapCompletionException(ex);
            boolean transientRebalance = cause instanceof org.apache.pulsar.client.api
                    .PulsarClientException.ConsumerBusyException
                    || cause instanceof org.apache.pulsar.client.api
                            .PulsarClientException.ConsumerAssignException;
            if (closed || !transientRebalance || System.nanoTime() >= deadlineNanos) {
                result.completeExceptionally(ex);
                return;
            }
            evictFailedSegmentConsumers();
            Duration delay = reconcileBackoff.next();
            log.info().attr("delayMs", delay.toMillis()).exceptionMessage(ex)
                    .log("Initial subscribe rejected during rebalance, retrying after backoff");
            scheduler().schedule(() -> attemptInitialSubscribe(assigned, deadlineNanos, result),
                    delay.toMillis(), TimeUnit.MILLISECONDS);
        });
    }

    /**
     * Converge the per-segment consumers onto {@link #latestAssignment}, retrying with backoff.
     * Retrying here is what makes rebalances converge: during a fan-out or a bucket move another
     * consumer may not have released a segment or bucket range yet, so our subscribe is rejected
     * ({@code ConsumerBusy} on Exclusive, {@code ConsumerAssignError} on overlapping STICKY
     * ranges) — and neither is retried at the v4 layer.
     */
    private void reconcile() {
        if (closed || !reconcileInProgress.compareAndSet(false, true)) {
            return;
        }
        List<ActiveSegment> target = latestAssignment;
        subscribeAssigned(target).whenComplete((__, ex) -> {
            reconcileInProgress.set(false);
            if (closed) {
                return;
            }
            if (ex == null) {
                reconcileBackoff.reset();
                // If a newer assignment arrived during this reconcile, run again to converge.
                if (latestAssignment != target) {
                    reconcile();
                }
                return;
            }
            // Evict failed subscribe futures so the next attempt can re-try them.
            evictFailedSegmentConsumers();
            Duration delay = reconcileBackoff.next();
            log.warn().attr("delayMs", delay.toMillis()).exceptionMessage(ex)
                    .log("Failed to apply assignment update, retrying after backoff");
            scheduler().schedule(this::reconcile, delay.toMillis(), TimeUnit.MILLISECONDS);
        });
    }

    private void evictFailedSegmentConsumers() {
        for (var entry : segmentConsumers.entrySet()) {
            var future = entry.getValue();
            if (future.isCompletedExceptionally()) {
                segmentConsumers.remove(entry.getKey(), future);
            }
        }
    }

    private ScheduledExecutorService scheduler() {
        return (ScheduledExecutorService) client.v4Client().getScheduledExecutorProvider().getExecutor();
    }

    private CompletableFuture<Void> subscribeAssigned(List<ActiveSegment> assigned) {
        // Controller-driven assignment: the broker's SubscriptionCoordinator decides
        // which segments this consumer owns at any moment. We subscribe to exactly
        // those, regardless of whether the controller picked them from the active or
        // sealed set — to the v4 layer they're just per-segment Exclusive subscriptions.
        var assignedIds = ConcurrentHashMap.<Long>newKeySet();
        for (var seg : assigned) {
            assignedIds.add(seg.segmentId());
        }

        // Segments that fell out of our assignment (rebalanced away to another
        // consumer): close our v4 consumer so the Exclusive lock is released and
        // the new owner can attach. Sealed-and-drained segments take a different
        // path: the receive loop closes them on TopicTerminated.
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (var entry : segmentConsumers.entrySet()) {
            if (!assignedIds.contains(entry.getKey())) {
                long segmentId = entry.getKey();
                var existing = entry.getValue();
                log.info().attr("segmentId", segmentId)
                        .log("Releasing segment removed from assignment");
                segmentBucketRanges.remove(segmentId);
                futures.add(drainAndClose(segmentId, existing)
                        .whenComplete((__, ___) -> {
                            sharedSegmentUnacked.remove(segmentId);
                            lastCumulativeAcked.remove(segmentId);
                            latestDelivered.remove(segmentId);
                            segmentReceiveEpoch.remove(segmentId);
                        }));
            }
        }

        // Subscribe to newly-assigned segments.
        for (var seg : assigned) {
            // PIP-486: if the controller changed which entry-buckets we own on a segment we are
            // already subscribed to (a consumer joined or left), re-subscribe with the new ranges so
            // the broker's exclusive bucket selector sees a consistent, non-overlapping assignment.
            var existing = segmentConsumers.get(seg.segmentId());
            if (existing != null
                    && !seg.ownedBucketRanges().equals(segmentBucketRanges.get(seg.segmentId()))) {
                log.info().attr("segmentId", seg.segmentId())
                        .log("Re-subscribing segment for changed entry-bucket ownership");
                // Drain what the application already holds, then await our own close before
                // re-subscribing with the new mode: the subscription type cannot change while the
                // old consumer is still attached. The drain must start outside computeIfAbsent —
                // it vacates this segment's slot itself, and ConcurrentHashMap forbids mutating
                // the map from within the mapping function.
                var drained = drainAndClose(seg.segmentId(), existing)
                        .whenComplete((__, ___) -> {
                            // A completed drain proves everything delivered was acked: reset the
                            // release state (as the drop path does) so a watermark from this
                            // consumer generation cannot leak into the next one — a stale
                            // delivered-vs-acked pair from an earlier mode would wedge a later
                            // drain that has nothing left to ack.
                            sharedSegmentUnacked.remove(seg.segmentId());
                            lastCumulativeAcked.remove(seg.segmentId());
                            latestDelivered.remove(seg.segmentId());
                        });
                futures.add(segmentConsumers.computeIfAbsent(seg.segmentId(), id ->
                        drained.thenCompose(__ -> createSegmentConsumerAsync(seg))));
            } else {
                futures.add(segmentConsumers.computeIfAbsent(seg.segmentId(),
                        id -> createSegmentConsumerAsync(seg)));
            }
        }

        log.info().attr("segments", assignedIds).log("Stream consumer assignment applied");
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    /**
     * PIP-486 release: pause the segment's receive loop (stop handing its messages to the
     * application), wait until every message already handed out is acked, then close the v4
     * consumer — anything still prefetched goes back to the broker unread for in-order
     * redelivery to the next owner. Deliberately unbounded: ownership must not move while the
     * application still holds unacked messages, so a stalled application stalls the release,
     * visibly, rather than silently breaking per-key order. Terminates early only when this
     * consumer closes.
     */
    private CompletableFuture<Void> drainAndClose(
            long segmentId, CompletableFuture<org.apache.pulsar.client.api.Consumer<T>> existing) {
        // Fence the receive that may already be in flight: any completion armed under the old
        // epoch is discarded, so nothing new can reach the application or the tracking maps
        // once the drain is underway.
        segmentReceiveEpoch.merge(segmentId, 1L, Long::sum);
        // Register as draining before vacating the segmentConsumers slot, so at every instant the
        // ack path resolves to the consumer being drained — the drain only completes through those
        // acks. The slot removal lives here (not at the call sites) to keep that ordering.
        drainingConsumers.put(segmentId, existing);
        segmentConsumers.remove(segmentId, existing);
        pausedSegments.add(segmentId);
        return awaitReleaseDrained(segmentId, 0, false)
                .thenCompose(__ -> existing.handle((c, ex) -> c))
                .thenCompose(c -> c != null ? c.closeAsync() : CompletableFuture.completedFuture(null))
                .whenComplete((__, ___) -> {
                    drainingConsumers.remove(segmentId, existing);
                    pendingDrainAcks.remove(segmentId);
                    pausedSegments.remove(segmentId);
                });
    }

    /**
     * Poll until the segment's delivered messages are all acked (or this consumer closes). The
     * barrier must hold on two consecutive polls: a receive that raced the drain's start can
     * publish one more message to the tracking maps microseconds after a first check read them,
     * and the 50ms-later confirmation is far past that window.
     */
    private CompletableFuture<Void> awaitReleaseDrained(long segmentId, int pollCount,
                                                        boolean drainedOnPreviousPoll) {
        if (closed) {
            return CompletableFuture.completedFuture(null);
        }
        boolean drained = isReleaseDrained(segmentId);
        if (drained && drainedOnPreviousPoll) {
            return CompletableFuture.completedFuture(null);
        }
        if (pollCount > 0 && pollCount % 200 == 0) {
            // ~every 10s at the 50ms poll interval: make a stalled release observable.
            log.warn().attr("segmentId", segmentId)
                    .log("Still waiting for the application to ack the segment's messages "
                            + "before releasing it");
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        scheduler().schedule(() -> awaitReleaseDrained(segmentId, pollCount + 1, drained)
                        .whenComplete((__, ex) -> {
                            if (ex != null) {
                                result.completeExceptionally(ex);
                            } else {
                                result.complete(null);
                            }
                        }),
                50, TimeUnit.MILLISECONDS);
        return result;
    }

    /**
     * Whether everything this consumer handed to the application for the segment has been acked:
     * bucket-shared segments track individual ids; whole (Exclusive) segments compare the
     * cumulative-ack high-water mark against the latest delivered position. Additionally, every
     * broker ack issued during the drain must have settled, so the close cannot overtake an ack
     * still in flight.
     */
    private boolean isReleaseDrained(long segmentId) {
        var drainAcks = pendingDrainAcks.get(segmentId);
        if (drainAcks != null) {
            for (var ack : drainAcks) {
                if (!ack.isDone()) {
                    return false;
                }
            }
        }
        var unacked = sharedSegmentUnacked.get(segmentId);
        if (unacked != null) {
            return unacked.isEmpty();
        }
        org.apache.pulsar.client.api.MessageId delivered = latestDelivered.get(segmentId);
        if (delivered == null) {
            return true; // nothing was ever handed to the application
        }
        org.apache.pulsar.client.api.MessageId acked = lastCumulativeAcked.get(segmentId);
        return acked != null && acked.compareTo(delivered) >= 0;
    }

    private CompletableFuture<org.apache.pulsar.client.api.Consumer<T>> createSegmentConsumerAsync(
            ActiveSegment segment) {
        PulsarClientImpl v4Client = client.v4Client();
        // Clone so per-segment consumers inherit every builder knob the user set
        // (ackTimeout, readCompacted, replicateSubscriptionState, encryption, ...).
        var segConf = consumerConf.clone();
        segConf.getTopicNames().clear();
        segConf.setTopicsPattern(null);
        // Legacy segments wrap an externally managed persistent:// topic; regular ones use the
        // computed segment:// URI. attachTopicName() collapses both into the right URI.
        segConf.getTopicNames().add(segment.attachTopicName());
        List<HashRange> ownedBucketRanges = segment.ownedBucketRanges();
        if (ownedBucketRanges.isEmpty()) {
            // Single-bucket segment: this consumer owns the whole segment exclusively (pre-PIP-486).
            segConf.setSubscriptionType(SubscriptionType.Exclusive);
        } else {
            // PIP-486: this consumer owns a subset of the segment's entry-buckets. Subscribe Key_Shared
            // STICKY declaring exactly those bucket hash-ranges, so the broker dispatches each entry to
            // the consumer owning its bucket; other owners of the same segment share the subscription
            // with disjoint ranges.
            List<Range> ranges = new ArrayList<>(ownedBucketRanges.size());
            for (HashRange r : ownedBucketRanges) {
                ranges.add(Range.of(r.start(), r.end()));
            }
            segConf.setSubscriptionType(SubscriptionType.Key_Shared);
            segConf.setKeySharedPolicy(KeySharedPolicy.stickyHashRange().ranges(ranges));
            // Route whole entries by their producer-stamped entry-bucket range on this subscription
            // (the gate that keeps plain Key_Shared subscriptions dispatching by key).
            segConf.setEntryBucketDispatch(true);
        }
        segmentBucketRanges.put(segment.segmentId(), ownedBucketRanges);
        if (ownedBucketRanges.isEmpty()) {
            sharedSegmentUnacked.remove(segment.segmentId());
        } else {
            // Key_Shared consumers cannot ack cumulatively: track delivered ids so a cumulative ack
            // can be translated into individual acks (see sharedSegmentUnacked).
            sharedSegmentUnacked.put(segment.segmentId(), new ConcurrentLinkedQueue<>());
        }
        // Only legacy segments wrap a persistent:// topic that the regular-to-scalable
        // migration pre-check inspects, so mark just those connections as V5-managed —
        // connections to real segment:// topics are never examined.
        if (segment.isLegacy()) {
            segConf.getProperties().put(
                    ScalableTopicConstants.V5_MANAGED_METADATA_KEY,
                    ScalableTopicConstants.V5_MANAGED_METADATA_VALUE);
        }
        if (consumerConf.getConsumerName() != null) {
            segConf.setConsumerName(consumerConf.getConsumerName() + "-seg-" + segment.segmentId());
        }
        return v4Client.subscribeSegmentAsync(segConf, v4Schema)
                .thenApply(consumer -> {
                    startReceiveLoop(consumer, segment.segmentId(),
                            segmentReceiveEpoch.getOrDefault(segment.segmentId(), 0L));
                    return consumer;
                });
    }

    /**
     * Async receive loop for a single segment consumer. Each received message:
     * 1. Updates {@link #latestDelivered} for this segment
     * 2. Snapshots the current position vector across all segments
     * 3. Wraps the message with a {@link MessageIdV5} carrying that vector
     * 4. Enqueues the wrapped message for the application to receive
     *
     * <p>{@code armedEpoch} is the segment's {@link #segmentReceiveEpoch} at arm time: a release
     * drain bumps the epoch, so a completion carrying a stale epoch belongs to a consumer being
     * (or already) released and is discarded — the message stays unacked and redelivers, in
     * order, to the segment's next owner.
     */
    private void startReceiveLoop(org.apache.pulsar.client.api.Consumer<T> v4Consumer, long segmentId,
                                  long armedEpoch) {
        v4Consumer.receiveAsync().thenAccept(v4Msg -> {
            if (segmentReceiveEpoch.getOrDefault(segmentId, 0L) != armedEpoch) {
                return;
            }
            // Update the latest delivered position for this segment
            latestDelivered.put(segmentId, v4Msg.getMessageId());

            // PIP-486 bucket-shared segment: remember the id so a cumulative ack can be translated
            // into individual acks (Key_Shared consumers cannot ack cumulatively).
            var unacked = sharedSegmentUnacked.get(segmentId);
            if (unacked != null) {
                unacked.add(v4Msg.getMessageId());
            }

            // Snapshot the position vector (all segments, including this one)
            Map<Long, org.apache.pulsar.client.api.MessageId> positionVector =
                    new HashMap<>(latestDelivered);

            // Create the V5 message with the position vector embedded in the ID
            var msgId = new MessageIdV5(v4Msg.getMessageId(), segmentId, positionVector);
            // Re-arm only once the sink has room, so a slow consumer pauses this segment's
            // receive loop (and the v4 flow-control permits) instead of buffering unboundedly.
            messageSink.accept(new MessageV5<>(v4Msg, msgId)).thenRun(() -> {
                // A paused segment is being released (PIP-486): stop pulling so the messages
                // already handed to the application can drain before the close.
                if (!closed && !pausedSegments.contains(segmentId)) {
                    startReceiveLoop(v4Consumer, segmentId, armedEpoch);
                }
            });
        }).exceptionally(ex -> {
            Throwable cause = ex instanceof java.util.concurrent.CompletionException ce
                    && ce.getCause() != null ? ce.getCause() : ex;
            if (closed
                    || cause instanceof org.apache.pulsar.client.api.PulsarClientException
                            .AlreadyClosedException) {
                // The whole consumer is shutting down or the v4 consumer was closed
                // externally; stop the receive loop without touching the map.
                return null;
            }
            if (cause instanceof org.apache.pulsar.client.api.PulsarClientException
                    .TopicTerminatedException) {
                // Segment fully drained server-side. Drop it from the map and close the
                // v4 consumer; pending acks from this point on are no-ops (cursor is at
                // the end and the entry is gone).
                log.info().attr("segmentId", segmentId)
                        .log("Sealed segment drained, closing v4 consumer");
                segmentConsumers.remove(segmentId);
                sharedSegmentUnacked.remove(segmentId);
                lastCumulativeAcked.remove(segmentId);
                latestDelivered.remove(segmentId);
                segmentReceiveEpoch.remove(segmentId);
                v4Consumer.closeAsync();
                return null;
            }
            log.warn().attr("segmentId", segmentId)
                    .exception(ex).log("Error receiving from segment, retrying");
            startReceiveLoop(v4Consumer, segmentId, armedEpoch);
            return null;
        });
    }
}
