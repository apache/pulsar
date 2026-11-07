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

import com.google.common.annotations.VisibleForTesting;
import io.github.merlimat.slog.Logger;
import io.netty.util.Timeout;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.client.api.v5.Checkpoint;
import org.apache.pulsar.client.api.v5.CheckpointConsumer;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.Messages;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.async.AsyncCheckpointConsumer;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.client.impl.v5.SegmentRouter.ActiveSegment;
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * V5 CheckpointConsumer implementation for scalable topics.
 *
 * <p>Maintains per-segment v4 Readers (no subscription cursor — position state lives
 * client-side and is materialized as {@link Checkpoint} via {@link #checkpoint()}).
 * Messages from all segments are multiplexed into a single receive queue. Supports
 * creating checkpoints (atomic snapshots of positions across all segments) and seeking
 * to previously saved checkpoints.
 *
 * <p>Two segment sources are supported, picked by the caller:
 * <ul>
 *   <li><b>Unmanaged</b> — the consumer reads every active segment (driven by a
 *       {@link DagWatchClient}). Multiple unmanaged consumers each independently see
 *       the full stream.</li>
 *   <li><b>Managed (consumer group)</b> — segments are assigned by the broker's
 *       subscription coordinator (driven by a {@link ScalableConsumerClient}).
 *       Consumers in the same group share segments and rebalance on join/leave.</li>
 * </ul>
 */
final class ScalableCheckpointConsumer<T> implements CheckpointConsumer<T> {

    private static final Logger LOG = Logger.get(ScalableCheckpointConsumer.class);
    /** Buffer bound for backpressure. The checkpoint consumer has no ConsumerConfigurationData;
     * its per-segment readers use the default reader queue size, so mirror that here. */
    private static final int DEFAULT_RECEIVER_QUEUE_SIZE = 1000;
    private final Logger log;

    private final PulsarClientV5 client;
    private final Schema<T> v5Schema;
    private final org.apache.pulsar.client.api.Schema<T> v4Schema;
    private final AutoCloseable sourceHandle;
    private final String topicName;
    private final Checkpoint startPosition;
    private final String consumerName;

    /**
     * Per-segment v4 readers. Stores futures so concurrent operations (seek, close)
     * can chain on in-flight reader creation without racing against completion.
     */
    private final ConcurrentHashMap<Long, SegmentReaderFuture<T>> segmentReaders = new ConcurrentHashMap<>();
    /**
     * One position tracker per reader attachment. A pending latest-start lookup has no message id yet.
     * Seed, receive, and removal use per-key computations, so revocation cannot race a seed or receive
     * into restoring an old attachment. The volatile message id is read by checkpoint snapshots.
     */
    private static final class SegmentPosition {
        private final Object attachment = new Object();
        private volatile MessageId messageId;
    }

    /** The reader owns its position, so a retained checkpoint tracker cannot retain a reader or its future. */
    private static final class SegmentReaderFuture<T> extends CompletableFuture<Reader<T>> {
        private final SegmentPosition position = new SegmentPosition();
    }

    private final ConcurrentHashMap<Long, SegmentPosition> lastReceivedPositions = new ConcurrentHashMap<>();
    private final V5ReceiveQueue<T> receiveQueue;

    /**
     * Per-key order across a split or merge, for the unmanaged consumer, which reads the whole DAG:
     * a segment is read only once every parent still in the layout has been read to its end, since
     * the parents hold the older messages of its keys. A managed assignment carries no lineage, so
     * its segments are read independently. Both are replaced on every layout, under {@code this}.
     */
    private volatile Map<Long, List<Long>> segmentParents = Map.of();
    /** Segments sealed in the latest layout: once read to their end, they have nothing more. */
    private volatile Set<Long> sealedSegments = Set.of();
    /** Sealed segments whose last message is in the receive queue. Guarded by {@code this}. */
    private final Set<Long> drainedSegments = new HashSet<>();
    /** Readers held back until their parents are drained. Guarded by {@code this}. */
    private final Map<Long, HeldReader<T>> heldReaders = new HashMap<>();

    /** A held reader retains its original reader future, even across reassignment. */
    private record HeldReader<T>(Reader<T> reader, SegmentReaderFuture<T> readerFuture) {
    }
    /** Sealed segments found read to their end while their read loop waited for a message. */
    private final Map<Long, Reader<T>> exhaustedSegments = new ConcurrentHashMap<>();
    /** Attached reader identities and coalesced sealed-tail probes, guarded by {@code this}. */
    private final Map<Long, Reader<T>> attachedReaders = new HashMap<>();
    private final Map<Long, SealedProbe> sealedProbes = new HashMap<>();
    /** Internal read/offer progress; independent of the application's receive-time checkpoint. */
    private final Map<Long, ReadPosition> readPositions = new HashMap<>();

    private record ReadPosition(long ledgerId, long entryId, int batchIndex) {
        private static ReadPosition of(MessageId id) {
            if (!(id instanceof MessageIdAdv position)) {
                throw new IllegalArgumentException("Checkpoint reader requires a physical message position");
            }
            return position.getLedgerId() < 0 || position.getEntryId() < 0 ? null
                    : new ReadPosition(position.getLedgerId(), position.getEntryId(), position.getBatchIndex());
        }

        private boolean covers(ReadPosition position) {
            int ledger = Long.compare(ledgerId, position.ledgerId);
            if (ledger != 0) {
                return ledger > 0;
            }
            int entry = Long.compare(entryId, position.entryId);
            // Mirror the v4 start filter: a plain boundary (-1) excludes the same plain message,
            // while actual batch members (0 and up) remain readable until their own saved batch index.
            return entry > 0 || entry == 0 && position.batchIndex <= batchIndex;
        }
    }

    private final class SealedProbe {
        private final Reader<T> reader;
        private Timeout timeout;
        private long generation;
        /** A probe request arrived after the current query started. */
        private boolean coalesced;

        private SealedProbe(Reader<T> reader) {
            this.reader = reader;
        }
    }

    /** A segment assignment, with the layout it comes from for the unmanaged consumer (else null). */
    private record Assignment(List<ActiveSegment> segments, ClientSegmentLayout layout) {
    }

    /** The latest assignment update, which {@link #reconcile()} converges the readers onto. */
    private volatile Assignment latestAssignment;
    /** Set when the readers must be converged onto {@link #latestAssignment} again. */
    private final AtomicBoolean reconcileRequested = new AtomicBoolean(false);
    /** Held while an assignment is applied, so only one is applied at a time. */
    private final AtomicBoolean reconcileInProgress = new AtomicBoolean(false);
    /** Whether a retry of the readers that failed to attach is scheduled. Guarded by {@code this}. */
    private boolean retryScheduled = false;
    /** Paces the retries. Guarded by {@code this}. */
    private final Backoff reconcileBackoff = Backoff.builder()
            .initialDelay(Duration.ofMillis(100))
            .maxBackoff(Duration.ofSeconds(30))
            .build();

    private volatile boolean closed = false;
    private final AsyncCheckpointConsumerV5<T> asyncView;

    private ScalableCheckpointConsumer(PulsarClientV5 client,
                                       Schema<T> v5Schema,
                                       String topicName,
                                       AutoCloseable sourceHandle,
                                       Checkpoint startPosition,
                                       String consumerName) {
        this.client = client;
        this.v5Schema = v5Schema;
        this.v4Schema = SchemaAdapter.toV4(v5Schema);
        this.sourceHandle = sourceHandle;
        this.topicName = topicName;
        this.startPosition = startPosition;
        this.consumerName = consumerName;
        this.receiveQueue = new V5ReceiveQueue<>(
                client.v4Client().externalExecutorProvider().getExecutor(), client.v4Client().timer(),
                DEFAULT_RECEIVER_QUEUE_SIZE);
        this.log = LOG.with().attr("topic", topicName).build();
        this.asyncView = new AsyncCheckpointConsumerV5<>(this);
    }

    /**
     * Create an unmanaged consumer that reads every segment in the active layout
     * (independent of any consumer group). Driven by a {@link DagWatchClient}, which
     * is closed on consumer close.
     */
    static <T> CompletableFuture<CheckpointConsumer<T>> createUnmanagedAsync(
            PulsarClientV5 client, Schema<T> v5Schema, DagWatchClient dagWatch,
            ClientSegmentLayout initialLayout, Checkpoint startPosition, String consumerName) {
        if (startPosition == CheckpointV5.LATEST) {
            // A segment that a split or merge creates later holds only messages published after the
            // consumer started, so start at the end of the segments in the layout now instead: like
            // a restored checkpoint, this has no position for a later segment, which is then read
            // from the earliest.
            startPosition = CheckpointV5.latestOf(allSegmentsOf(initialLayout).stream()
                    .map(ActiveSegment::segmentId)
                    .toList());
        }
        ScalableCheckpointConsumer<T> consumer = new ScalableCheckpointConsumer<>(
                client, v5Schema, dagWatch.topicName().toString(), dagWatch, startPosition, consumerName);
        return consumer.applyAssignment(allSegmentsOf(initialLayout), initialLayout)
                .thenApply(__ -> {
                    dagWatch.setListener((newLayout, oldLayout) -> consumer.onAssignmentChange(
                            allSegmentsOf(newLayout), newLayout));
                    return (CheckpointConsumer<T>) consumer;
                })
                .exceptionallyCompose(ex -> consumer.closeAsync().handle((__, ___) -> {
                    throw ex instanceof CompletionException ce ? ce : new CompletionException(ex);
                }));
    }

    /**
     * Active + sealed segments. The unmanaged checkpoint consumer needs to subscribe
     * to sealed segments too so a {@link Checkpoint} taken before a split or merge
     * still resumes correctly: the reader on each sealed parent picks up from the
     * saved position and drains its remaining backlog before its children are read.
     */
    private static List<ActiveSegment> allSegmentsOf(ClientSegmentLayout layout) {
        List<ActiveSegment> all = new ArrayList<>(
                layout.activeSegments().size() + layout.sealedSegments().size());
        all.addAll(layout.activeSegments());
        all.addAll(layout.sealedSegments());
        return all;
    }

    /**
     * Create a managed consumer that reads only the segments the broker's subscription
     * coordinator assigns to it within the named consumer group. Driven by a
     * {@link ScalableConsumerClient}, which is closed on consumer close.
     */
    static <T> CompletableFuture<CheckpointConsumer<T>> createManagedAsync(
            PulsarClientV5 client, Schema<T> v5Schema, String topicName,
            ScalableConsumerClient session, List<ActiveSegment> initialAssignment,
            Checkpoint startPosition, String consumerName) {
        ScalableCheckpointConsumer<T> consumer = new ScalableCheckpointConsumer<>(
                client, v5Schema, topicName, session, startPosition, consumerName);
        return consumer.applyAssignment(initialAssignment, null)
                .thenApply(__ -> {
                    session.setListener((newSegments, oldSegments) ->
                            consumer.onAssignmentChange(newSegments, null));
                    return (CheckpointConsumer<T>) consumer;
                })
                .exceptionallyCompose(ex -> consumer.closeAsync().handle((__, ___) -> {
                    throw ex instanceof CompletionException ce ? ce : new CompletionException(ex);
                }));
    }

    @Override
    public String topic() {
        return topicName;
    }

    @Override
    public Message<T> receive() throws PulsarClientException {
        return advanceCheckpoint(receiveQueue.take());
    }

    @Override
    public Message<T> receive(Duration timeout) throws PulsarClientException {
        return advanceCheckpoint(receiveQueue.poll(timeout));
    }

    @Override
    public Messages<T> receiveMulti(int maxMessages, Duration timeout) throws PulsarClientException {
        List<Message<T>> batch = receiveQueue.receiveMulti(maxMessages, timeout);
        batch.forEach(this::advanceCheckpoint);
        return new MessagesV5<>(batch);
    }

    /**
     * Update the checkpoint position for the segment this message belongs to. Called as
     * messages cross the boundary from the wire-buffer to the application — that's the
     * point at which a subsequent {@link #checkpoint()} should reflect "I have processed
     * this message".
     *
     * <p>{@code msg} may be null (timeout or interrupt path); returns it unchanged so the
     * caller can pass through the receive result without an extra null-check.
     */
    private Message<T> advanceCheckpoint(Message<T> msg) {
        if (msg instanceof MessageV5<?> message && msg.id() instanceof MessageIdV5 id) {
            lastReceivedPositions.computeIfPresent(id.segmentId(), (__, position) -> {
                if (position.attachment == message.checkpointAttachment()) {
                    position.messageId = id.v4MessageId();
                }
                return position;
            });
        }
        return msg;
    }

    @Override
    public Checkpoint checkpoint() {
        Map<Long, MessageId> positions = new HashMap<>();
        lastReceivedPositions.forEach((segmentId, position) -> {
            MessageId messageId = position.messageId;
            if (messageId != null) {
                positions.put(segmentId, messageId);
            }
        });
        return new CheckpointV5(positions);
    }

    @VisibleForTesting
    int checkpointPositionTrackerCount() {
        return lastReceivedPositions.size();
    }

    /** Remove only the position belonging to the reader being detached, never a later attachment. */
    private void removePosition(long segmentId, SegmentReaderFuture<?> readerFuture) {
        lastReceivedPositions.computeIfPresent(segmentId, (__, position) ->
                position == readerFuture.position ? null : position);
    }

    @Override
    public AsyncCheckpointConsumer<T> async() {
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
            throw V5ExceptionMappings.toV5Exception(e);
        }
    }

    // --- Async internals ---

    CompletableFuture<Message<T>> receiveAsync() {
        return receiveQueue.receiveAsync().thenApply(this::advanceCheckpoint);
    }

    CompletableFuture<Message<T>> receiveAsync(Duration timeout) {
        return receiveQueue.receiveAsync(timeout).thenApply(this::advanceCheckpoint);
    }

    CompletableFuture<List<Message<T>>> receiveMultiAsync(int maxMessages, Duration timeout) {
        return receiveQueue.receiveMultiAsync(maxMessages, timeout).thenApply(batch -> {
            batch.forEach(this::advanceCheckpoint);
            return batch;
        });
    }

    CompletableFuture<Checkpoint> checkpointAsync() {
        return CompletableFuture.completedFuture(checkpoint());
    }

    CompletableFuture<Void> closeAsync() {
        List<Timeout> probes = new ArrayList<>();
        synchronized (this) {
            closed = true;
            sealedProbes.values().forEach(probe -> {
                if (probe.timeout != null) {
                    probes.add(probe.timeout);
                }
            });
            sealedProbes.clear();
            attachedReaders.clear();
            readPositions.clear();
            exhaustedSegments.clear();
        }
        probes.forEach(Timeout::cancel);
        receiveQueue.close();
        try {
            sourceHandle.close();
        } catch (Exception e) {
            log.warn().exceptionMessage(e).log("Error closing segment source");
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (var future : segmentReaders.values()) {
            futures.add(future
                    .handle((reader, ex) -> reader)
                    .thenCompose(reader -> reader != null ? reader.closeAsync()
                            : CompletableFuture.completedFuture(null)));
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .whenComplete((__, ___) -> segmentReaders.clear())
                .exceptionallyCompose(V5ExceptionMappings::failWithV5Exception);
    }

    // --- Assignment change handling ---

    private void onAssignmentChange(List<ActiveSegment> newSegments, ClientSegmentLayout layout) {
        // Fully async: safe to run on the netty IO thread that delivered the update.
        latestAssignment = new Assignment(newSegments, layout);
        reconcile();
    }

    /**
     * Converge the per-segment readers onto {@link #latestAssignment}. An assignment is applied as
     * soon as it arrives, not after the readers of the previous one have attached: a reader that
     * keeps reconnecting must not delay closing a segment that was rebalanced away. A reader that
     * fails to attach is evicted and retried with backoff: otherwise its segment, and every segment
     * held back on it, would never be read.
     */
    private void reconcile() {
        reconcileRequested.set(true);
        // One thread applies at a time, and applies again if asked to meanwhile.
        while (!closed && reconcileRequested.get() && reconcileInProgress.compareAndSet(false, true)) {
            reconcileRequested.set(false);
            evictFailedSegmentReaders();
            Assignment target = latestAssignment;
            applyAssignment(target.segments(), target.layout()).whenComplete((__, ex) -> {
                if (ex == null) {
                    synchronized (this) {
                        reconcileBackoff.reset();
                    }
                } else if (!closed) {
                    scheduleRetry(ex);
                }
            });
            reconcileInProgress.set(false);
        }
    }

    /** Retry the readers that failed to attach after a backoff, with at most one retry scheduled. */
    private void scheduleRetry(Throwable ex) {
        Duration delay;
        synchronized (this) {
            if (retryScheduled) {
                return;
            }
            retryScheduled = true;
            delay = reconcileBackoff.next();
        }
        log.warn().attr("delayMs", delay.toMillis()).exceptionMessage(ex)
                .log("Failed to apply segment assignment, retrying after backoff");
        client.v4Client().timer().newTimeout(timeout -> {
            synchronized (this) {
                retryScheduled = false;
            }
            reconcile();
        }, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void evictFailedSegmentReaders() {
        for (var entry : segmentReaders.entrySet()) {
            var future = entry.getValue();
            if (future.isCompletedExceptionally() && segmentReaders.remove(entry.getKey(), future)) {
                removePosition(entry.getKey(), future);
            }
        }
    }

    /**
     * @param layout the DAG the segments come from, for the unmanaged consumer, which reads each
     *               segment after its parents; null for a managed assignment, which has no lineage
     */
    private CompletableFuture<Void> applyAssignment(List<ActiveSegment> assigned, ClientSegmentLayout layout) {
        var assignedIds = ConcurrentHashMap.<Long>newKeySet();
        for (var seg : assigned) {
            assignedIds.add(seg.segmentId());
        }

        Set<Long> newlySealed = new HashSet<>();
        List<Map.Entry<Long, HeldReader<T>>> released;
        synchronized (this) {
            Set<Long> sealed = new HashSet<>();
            if (layout != null) {
                for (var seg : layout.sealedSegments()) {
                    sealed.add(seg.segmentId());
                    if (!sealedSegments.contains(seg.segmentId())) {
                        newlySealed.add(seg.segmentId());
                    }
                }
            }
            segmentParents = layout != null ? layout.segmentParents() : Map.of();
            sealedSegments = Set.copyOf(sealed);
            drainedSegments.retainAll(assignedIds);
            heldReaders.keySet().retainAll(assignedIds);
            // A parent pruned from the layout leaves nothing to wait for.
            released = releaseHeldReaders();
        }

        // Close readers for segments removed from the assignment (sealed, or rebalanced
        // away to another consumer in the same group).
        for (var entry : segmentReaders.entrySet()) {
            if (!assignedIds.contains(entry.getKey())) {
                log.info().attr("segmentId", entry.getKey())
                        .log("Closing reader for segment removed from assignment");
                Timeout timeout = null;
                synchronized (this) {
                    if (segmentReaders.remove(entry.getKey(), entry.getValue())) {
                        attachedReaders.remove(entry.getKey());
                        readPositions.remove(entry.getKey());
                        heldReaders.remove(entry.getKey());
                        exhaustedSegments.remove(entry.getKey());
                        SealedProbe probe = sealedProbes.remove(entry.getKey());
                        timeout = probe != null ? probe.timeout : null;
                        removePosition(entry.getKey(), entry.getValue());
                    }
                }
                if (timeout != null) {
                    timeout.cancel();
                }
                entry.getValue().thenAccept(reader -> {
                    if (reader != null) {
                        reader.closeAsync();
                    }
                });
            }
        }

        // A segment sealed under its running read loop: see probeSealedSegment.
        for (long segmentId : newlySealed) {
            var readerFuture = segmentReaders.get(segmentId);
            if (readerFuture != null) {
                readerFuture.thenAccept(reader -> {
                    if (reader != null) {
                        probeSealedSegment(reader, segmentId);
                    }
                });
            }
        }

        // Create readers for new segments asynchronously.
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (var seg : assigned) {
            // In segmentReaders before the creation starts, so that a later step of the creation can
            // tell whether the segment still maps to it.
            var readerFuture = new SegmentReaderFuture<T>();
            var current = segmentReaders.putIfAbsent(seg.segmentId(), readerFuture);
            if (current == null) {
                lastReceivedPositions.put(seg.segmentId(), readerFuture.position);
                FutureUtil.completeAfter(readerFuture,
                        FutureUtil.supplySafely(() -> createSegmentReaderAsync(seg, readerFuture)));
                current = readerFuture;
            }
            futures.add(current);
        }
        released.forEach(held -> startReadLoop(held.getValue().reader(), held.getKey(),
                held.getValue().readerFuture()));

        log.info().attr("segments", assignedIds).log("Checkpoint consumer assignment applied");
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    /** Whether every parent of the segment still in the layout has been read to its end. */
    private synchronized boolean parentsDrained(long segmentId) {
        Map<Long, List<Long>> parents = segmentParents;
        for (long parent : parents.getOrDefault(segmentId, List.of())) {
            if (parents.containsKey(parent) && !drainedSegments.contains(parent)) {
                return false;
            }
        }
        return true;
    }

    /** Remove and return the held readers whose parents have all been drained. */
    private synchronized List<Map.Entry<Long, HeldReader<T>>> releaseHeldReaders() {
        List<Map.Entry<Long, HeldReader<T>>> released = new ArrayList<>();
        for (var it = heldReaders.entrySet().iterator(); it.hasNext(); ) {
            var held = it.next();
            if (parentsDrained(held.getKey())) {
                released.add(Map.entry(held.getKey(), held.getValue()));
                it.remove();
            }
        }
        return released;
    }

    /**
     * A sealed parent may still contain an OPEN transaction. hasMessageAvailableAsync only sees the dispatchable
     * frontier, so false is temporary until the broker also reports the physical end of the terminated topic.
     * Coalesce probes per attached reader while its pending read waits for that final notification.
     */
    private void probeSealedSegment(Reader<T> reader, long segmentId) {
        SealedProbe probe;
        synchronized (this) {
            HeldReader<T> held = heldReaders.get(segmentId);
            if (!isAttachedReader(reader, segmentId) || held != null && held.reader() == reader
                    || drainedSegments.contains(segmentId)) {
                return;
            }
            SealedProbe current = sealedProbes.get(segmentId);
            if (current != null) {
                // The outstanding query may hold a true snapshot taken before this empty observation:
                // make that result check again instead of retiring the only probe.
                current.coalesced = true;
                return;
            }
            probe = new SealedProbe(reader);
            sealedProbes.put(segmentId, probe);
        }
        checkSealedProbe(probe, segmentId, 0);
    }

    /** Caller holds this consumer's monitor. */
    private boolean isAttachedReader(Reader<T> reader, long segmentId) {
        return !closed && attachedReaders.get(segmentId) == reader;
    }

    private void checkSealedProbe(SealedProbe probe, long segmentId, long expectedGeneration) {
        final long generation;
        synchronized (this) {
            if (sealedProbes.get(segmentId) != probe || probe.generation != expectedGeneration
                    || !isAttachedReader(probe.reader, segmentId)) {
                return;
            }
            probe.timeout = null;
            probe.coalesced = false;
            generation = ++probe.generation;
        }
        // A false availability result can be queued on the v4 pinned executor. Do not combine an old empty
        // snapshot with an end notification that arrived while that query was outstanding.
        boolean endNotified = probe.reader.hasReachedEndOfTopic();
        FutureUtil.supplySafely(probe.reader::hasMessageAvailableAsync).whenComplete((available, ex) -> {
            synchronized (this) {
                if (sealedProbes.get(segmentId) != probe || probe.generation != generation
                        || !isAttachedReader(probe.reader, segmentId)) {
                    return;
                }
            }
            Throwable cause = ex instanceof CompletionException ce && ce.getCause() != null ? ce.getCause() : ex;
            boolean end = ex == null && endNotified && Boolean.FALSE.equals(available);
            synchronized (this) {
                if (sealedProbes.get(segmentId) != probe || probe.generation != generation
                        || !isAttachedReader(probe.reader, segmentId)) {
                    return;
                }
                if (end) {
                    sealedProbes.remove(segmentId);
                    exhaustedSegments.put(segmentId, probe.reader);
                } else if (cause instanceof AlreadyClosedException
                        || Boolean.TRUE.equals(available) && !probe.coalesced) {
                    sealedProbes.remove(segmentId);
                    return;
                }
            }
            if (end) {
                // Wake the pending read. Only its loop may release children, after its last message was queued.
                probe.reader.closeAsync();
                return;
            }
            Timeout timeout = client.v4Client().timer().newTimeout(
                    __ -> checkSealedProbe(probe, segmentId, generation), 1, TimeUnit.SECONDS);
            boolean cancel;
            synchronized (this) {
                cancel = sealedProbes.get(segmentId) != probe || probe.generation != generation
                        || !isAttachedReader(probe.reader, segmentId);
                if (!cancel) {
                    probe.timeout = timeout;
                }
            }
            if (cancel && timeout != null) {
                timeout.cancel();
            }
        });
    }

    private void cancelSealedProbe(long segmentId, Reader<T> reader) {
        Timeout timeout = null;
        synchronized (this) {
            SealedProbe probe = sealedProbes.get(segmentId);
            if (probe != null && (reader == null || probe.reader == reader)) {
                sealedProbes.remove(segmentId);
                timeout = probe.timeout;
            }
        }
        if (timeout != null) {
            timeout.cancel();
        }
    }

    /** Release children only after this attached parent's last message has crossed into the receive queue. */
    private void onSegmentDrained(Reader<T> reader, long segmentId) {
        List<Map.Entry<Long, HeldReader<T>>> released;
        synchronized (this) {
            if (!isAttachedReader(reader, segmentId)) {
                return;
            }
            exhaustedSegments.remove(segmentId, reader);
            if (!drainedSegments.add(segmentId)) {
                return;
            }
            released = releaseHeldReaders();
        }
        cancelSealedProbe(segmentId, reader);
        reader.closeAsync();
        log.info().attr("segmentId", segmentId).log("Sealed segment drained, closing reader");
        released.forEach(held -> startReadLoop(held.getValue().reader(), held.getKey(),
                held.getValue().readerFuture()));
    }

    /** Remove only this reader's generation when its backing segment no longer exists. */
    private void onSegmentGone(Reader<T> reader, long segmentId,
                               SegmentReaderFuture<T> expectedFuture) {
        List<Map.Entry<Long, HeldReader<T>>> released;
        Timeout timeout;
        synchronized (this) {
            if (reader != null) {
                if (!isAttachedReader(reader, segmentId)) {
                    return;
                }
                expectedFuture = segmentReaders.get(segmentId);
            }
            if (closed || expectedFuture == null || !segmentReaders.remove(segmentId, expectedFuture)) {
                return;
            }
            attachedReaders.remove(segmentId);
            readPositions.remove(segmentId);
            heldReaders.remove(segmentId);
            exhaustedSegments.remove(segmentId);
            removePosition(segmentId, expectedFuture);
            SealedProbe probe = sealedProbes.remove(segmentId);
            timeout = probe != null ? probe.timeout : null;
            drainedSegments.add(segmentId);
            released = releaseHeldReaders();
        }
        if (timeout != null) {
            timeout.cancel();
        }
        if (reader != null) {
            reader.closeAsync();
        }
        released.forEach(held -> startReadLoop(held.getValue().reader(), held.getKey(),
                held.getValue().readerFuture()));
    }

    private CompletableFuture<Reader<T>> createSegmentReaderAsync(ActiveSegment segment,
                                                                 SegmentReaderFuture<T> readerFuture) {
        SegmentPosition position = readerFuture.position;
        PulsarClientImpl v4Client = client.v4Client();
        MessageId startMsgId = resolveStartPosition(segment.segmentId());

        var segConf = new ReaderConfigurationData<T>();
        // Legacy segments wrap an externally managed persistent:// topic; regular ones use the
        // computed segment:// URI. attachTopicName() collapses both into the right URI.
        segConf.getTopicNames().add(segment.attachTopicName());
        segConf.setStartMessageId(startMsgId);
        if (consumerName != null) {
            segConf.setReaderName(consumerName + "-seg-" + segment.segmentId());
        }

        // The original attachment is passed in, never looked up again after an asynchronous boundary.
        return resolveLatestStartAsync(segConf)
                .thenCompose(__ -> {
                    // Seed only this attachment, atomically with revocation and receive advancement.
                    // A lookup that outlives its assignment cannot insert a tracker or seed another one.
                    lastReceivedPositions.computeIfPresent(segment.segmentId(), (ignoredSegmentId, current) -> {
                        if (current == position && segmentReaders.get(segment.segmentId()) == readerFuture
                                && current.messageId == null) {
                            current.messageId = segConf.getStartMessageId();
                        }
                        return current;
                    });
                    // Read the exclusive boundary normally so ReaderImpl auto-acks what the v4 start filter
                    // would otherwise discard without acking. That ack is required for a sealed parent's EOT.
                    // Keep the original complete position: an inclusive chunk reader internally starts at its first
                    // chunk, while the checkpoint's boundary remains the complete message's last chunk.
                    ReadPosition boundary = ReadPosition.of(segConf.getStartMessageId());
                    segConf.setResetIncludeHead(true);
                    return v4Client.createSegmentReaderAsync(segConf, v4Schema)
                            .thenApply(reader -> attachSegmentReader(reader, segment, readerFuture, boundary));
                })
                .exceptionally(ex -> {
                    Throwable cause = ex instanceof CompletionException ce && ce.getCause() != null
                            ? ce.getCause() : ex;
                    if (isSegmentGoneError(cause)) {
                        // The backing topic was deleted by the controller's GC after its
                        // retention window elapsed. The consumer may be restoring from a
                        // checkpoint that pre-dates the prune, or racing a layout update.
                        // Either way, the segment's data is gone — skip it silently.
                        log.info().attr("segmentId", segment.segmentId())
                                .log("Segment backing topic deleted (retention expired); skipping");
                        onSegmentGone(null, segment.segmentId(), readerFuture);
                        return null;
                    }
                    throw ex instanceof CompletionException
                            ? (CompletionException) ex : new CompletionException(ex);
                });
    }

    private Reader<T> attachSegmentReader(Reader<T> reader, ActiveSegment segment,
                                          SegmentReaderFuture<T> readerFuture, ReadPosition boundary) {
        boolean held;
        boolean attached;
        synchronized (this) {
            attached = !closed && segmentReaders.get(segment.segmentId()) == readerFuture
                    && lastReceivedPositions.get(segment.segmentId()) == readerFuture.position;
            held = !parentsDrained(segment.segmentId());
            if (attached) {
                attachedReaders.put(segment.segmentId(), reader);
                if (boundary != null) {
                    readPositions.put(segment.segmentId(), boundary);
                } else {
                    readPositions.remove(segment.segmentId());
                }
                if (held) {
                    heldReaders.put(segment.segmentId(), new HeldReader<>(reader, readerFuture));
                }
            }
        }
        if (!attached) {
            reader.closeAsync();
        } else if (!held) {
            startReadLoop(reader, segment.segmentId(), readerFuture);
        }
        return reader;
    }

    /**
     * Replace a latest start position with the segment's last message id, looked up through a reader
     * opened only for that. A reader attached at latest starts after the segment's last message at that
     * moment, which a lookup made once it is attached can't tell apart from a message published in
     * between, and it starts after the new latest if it reconnects before receiving anything. A reader
     * attached after a known position starts exactly there.
     *
     * <p>The lookup returns the last dispatchable position. While a transaction is open on the segment,
     * that is before the transaction's first message, so the messages behind it are delivered once they
     * become dispatchable, also non-transactional ones published before the consumer started.
     */
    private CompletableFuture<Void> resolveLatestStartAsync(ReaderConfigurationData<T> segConf) {
        if (!org.apache.pulsar.client.api.MessageId.latest.equals(segConf.getStartMessageId())) {
            return CompletableFuture.completedFuture(null);
        }
        return client.v4Client().createSegmentReaderAsync(segConf.clone(), v4Schema)
                .thenCompose(ScalableCheckpointConsumer::lastMessageIdsThenCloseAsync)
                .thenAccept(lastMessageIds -> segConf.setStartMessageId(lastMessageIds.get(0)));
    }

    /**
     * The reader's last message ids, available once the reader is closed. The lookup reader is not in
     * {@link #segmentReaders}, so a consumer close doesn't wait for it: the reader creation goes on only
     * after it is closed, also when the lookup failed, whose error is kept. A failed close is only logged.
     */
    @VisibleForTesting
    static CompletableFuture<List<TopicMessageId>> lastMessageIdsThenCloseAsync(Reader<?> lookupReader) {
        CompletableFuture<List<TopicMessageId>> lookup = lookupReader.getLastMessageIdsAsync();
        return lookup.handle((__, ___) -> lookupReader.closeAsync())
                .thenCompose(closing -> closing.exceptionally(ex -> {
                    LOG.warn().attr("topic", lookupReader.getTopic()).exceptionMessage(ex)
                            .log("Failed to close the lookup reader");
                    return null;
                }))
                .thenCompose(__ -> lookup);
    }

    /**
     * True if {@code cause} indicates the segment's backing topic no longer exists
     * (deleted by the controller's GC, or a race between a layout update and the
     * post-prune topic delete).
     */
    private static boolean isSegmentGoneError(Throwable cause) {
        return cause instanceof org.apache.pulsar.client.api.PulsarClientException
                .TopicDoesNotExistException
                || cause instanceof org.apache.pulsar.client.api.PulsarClientException
                .NotFoundException;
    }

    private org.apache.pulsar.client.api.MessageId resolveStartPosition(long segmentId) {
        if (startPosition instanceof CheckpointV5 cp) {
            var pos = cp.segmentPositions().get(segmentId);
            if (pos != null) {
                return pos;
            }
            // The checkpoint has no position for this segment, which means the
            // segment didn't exist when the checkpoint was taken (it's a child
            // produced by a split or merge after the snapshot). All of its data is
            // therefore newer than the checkpoint — read from the earliest.
            return org.apache.pulsar.client.api.MessageId.earliest;
        }
        if (startPosition == CheckpointV5.EARLIEST) {
            return org.apache.pulsar.client.api.MessageId.earliest;
        }
        // CheckpointV5.LATEST and anything else: latest. Only a managed consumer still has LATEST
        // here (createUnmanagedAsync resolves it against the initial layout): its assignment can't
        // tell a segment created after the consumer started from one moved to it by a rebalance.
        return org.apache.pulsar.client.api.MessageId.latest;
    }

    /**
     * The segment's next message, or null only after the broker's physical-end notification and an empty
     * reader buffer. An OPEN transaction can temporarily hide the tail of an already sealed segment.
     */
    private CompletableFuture<org.apache.pulsar.client.api.Message<T>> readNextAsync(Reader<T> reader,
                                                                                     long segmentId) {
        if (!sealedSegments.contains(segmentId)) {
            return reader.readNextAsync();
        }
        boolean endNotified = reader.hasReachedEndOfTopic();
        return reader.hasMessageAvailableAsync().thenCompose(available -> {
            if (available) {
                return reader.readNextAsync();
            }
            if (endNotified) {
                return CompletableFuture.completedFuture(null);
            }
            probeSealedSegment(reader, segmentId);
            return reader.readNextAsync();
        });
    }

    private void startReadLoop(Reader<T> reader, long segmentId, SegmentReaderFuture<T> readerFuture) {
        SegmentPosition checkpointPosition = readerFuture.position;
        synchronized (this) {
            if (!isAttachedReader(reader, segmentId) || segmentReaders.get(segmentId) != readerFuture) {
                return;
            }
        }
        readNextAsync(reader, segmentId).thenAccept(v4Msg -> {
            boolean attached;
            synchronized (this) {
                attached = isAttachedReader(reader, segmentId) && segmentReaders.get(segmentId) == readerFuture;
            }
            if (!attached) {
                if (v4Msg != null) {
                    v4Msg.release();
                }
                return;
            }
            if (v4Msg == null) {
                onSegmentDrained(reader, segmentId);
                return;
            }
            MessageId messageId = v4Msg.getMessageId();
            ReadPosition position = ReadPosition.of(messageId);
            if (position == null) {
                v4Msg.release();
                throw new IllegalArgumentException("A checkpoint reader message has no physical position");
            }
            // Snapshot the id and construct the wrapper before taking the lifecycle lock: neither a message
            // getter nor the queue's executor/user completions may run inside that critical section.
            MessageV5<T> message = new MessageV5<>(v4Msg, segmentId, checkpointPosition.attachment);
            boolean duplicate = false;
            boolean enqueued = false;
            synchronized (this) {
                attached = isAttachedReader(reader, segmentId) && segmentReaders.get(segmentId) == readerFuture;
                if (attached) {
                    ReadPosition previous = readPositions.get(segmentId);
                    duplicate = previous != null && previous.covers(position);
                    if (!duplicate) {
                        enqueued = receiveQueue.enqueueToBuffer(message);
                        if (enqueued) {
                            readPositions.put(segmentId, position);
                        }
                    }
                }
            }
            if (!enqueued) {
                // ReaderImpl already auto-acked this actual message, preserving the real batch mask. Inclusive
                // readers can repeat the boundary on every reconnect, not only on their initial attachment.
                v4Msg.release();
                if (attached && duplicate) {
                    client.v4Client().getInternalExecutorService()
                            .execute(() -> startReadLoop(reader, segmentId, readerFuture));
                }
                return;
            }
            // Internal read progress is bound to the actual enqueue above. The public checkpoint still advances
            // only in receive/receiveMulti, when the application pulls a message from this buffer.
            // Capacity merely delays the next read; no lifecycle lock is held when receivers are completed.
            receiveQueue.afterEnqueue().thenRun(() -> startReadLoop(reader, segmentId, readerFuture));
        }).exceptionally(ex -> {
            Throwable cause = ex instanceof CompletionException ce && ce.getCause() != null
                    ? ce.getCause() : ex;
            synchronized (this) {
                if (!isAttachedReader(reader, segmentId) || segmentReaders.get(segmentId) != readerFuture) {
                    return null;
                }
            }
            if (cause instanceof AlreadyClosedException) {
                // Closed by probeSealedSegment once the segment was read to its end, or
                // externally (segment rebalanced away or pruned). Stop the loop.
                if (exhaustedSegments.remove(segmentId, reader)) {
                    onSegmentDrained(reader, segmentId);
                }
                return null;
            }
            if (cause instanceof org.apache.pulsar.client.api.PulsarClientException
                    .TopicTerminatedException) {
                // Sealed segment fully drained server-side; the segment's data has
                // already crossed into receiveQueue.
                onSegmentDrained(reader, segmentId);
                return null;
            }
            if (isSegmentGoneError(cause)) {
                // Segment backing topic deleted underneath us — the controller's GC
                // pruned it after retention expired. The DagWatch layout update would
                // normally remove this segment from the assignment first, but a
                // network blip or in-flight read can lose that race. Treat as
                // drained: close the reader and drop the position tracker so a
                // subsequent checkpoint() doesn't carry a stale ID for a topic
                // that no longer exists.
                log.info().attr("segmentId", segmentId)
                        .log("Segment backing topic deleted (retention expired); closing reader");
                onSegmentGone(reader, segmentId, null);
                return null;
            }
            log.warn().attr("segmentId", segmentId)
                    .exception(ex).log("Error reading from segment, retrying");
            // Hop to the v4 client's internal executor so repeated synchronous failures
            // don't grow the stack unboundedly.
            client.v4Client().getInternalExecutorService()
                    .execute(() -> startReadLoop(reader, segmentId, readerFuture));
            return null;
        });
    }
}
