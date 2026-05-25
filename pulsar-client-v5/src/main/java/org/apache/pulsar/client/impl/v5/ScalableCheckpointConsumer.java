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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.v5.Checkpoint;
import org.apache.pulsar.client.api.v5.CheckpointConsumer;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.Messages;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.async.AsyncCheckpointConsumer;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.v5.SegmentRouter.ActiveSegment;

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
    private final ConcurrentHashMap<Long, CompletableFuture<Reader<T>>> segmentReaders = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, org.apache.pulsar.client.api.MessageId> lastReceivedPositions =
            new ConcurrentHashMap<>();
    private final LinkedTransferQueue<MessageV5<T>> messageQueue = new LinkedTransferQueue<>();

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
        ScalableCheckpointConsumer<T> consumer = new ScalableCheckpointConsumer<>(
                client, v5Schema, dagWatch.topicName().toString(), dagWatch, startPosition, consumerName);
        return consumer.applyAssignment(allSegmentsOf(initialLayout))
                .thenApply(__ -> {
                    dagWatch.setListener((newLayout, oldLayout) -> consumer.onAssignmentChange(
                            allSegmentsOf(newLayout),
                            oldLayout != null ? allSegmentsOf(oldLayout) : List.of()));
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
     * saved position and drains its remaining backlog before naturally exiting on
     * {@code TopicTerminated}.
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
        return consumer.applyAssignment(initialAssignment)
                .thenApply(__ -> {
                    session.setListener(consumer::onAssignmentChange);
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
        try {
            return advanceCheckpoint(messageQueue.take());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Receive interrupted", e);
        }
    }

    @Override
    public Message<T> receive(Duration timeout) throws PulsarClientException {
        try {
            return advanceCheckpoint(messageQueue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Receive interrupted", e);
        }
    }

    @Override
    public Messages<T> receiveMulti(int maxMessages, Duration timeout) throws PulsarClientException {
        List<Message<T>> batch = new ArrayList<>();
        long deadlineNanos = System.nanoTime() + timeout.toNanos();

        while (batch.size() < maxMessages) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                break;
            }
            try {
                MessageV5<T> msg = messageQueue.poll(remainingNanos, TimeUnit.NANOSECONDS);
                if (msg == null) {
                    break;
                }
                batch.add(advanceCheckpoint(msg));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new PulsarClientException("Receive interrupted", e);
            }
            // Drain whatever else is immediately ready up to maxMessages.
            List<Message<T>> drained = new ArrayList<>();
            messageQueue.drainTo(drained, maxMessages - batch.size());
            for (Message<T> drainedMsg : drained) {
                batch.add(advanceCheckpoint(drainedMsg));
            }
        }
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
        if (msg != null && msg.id() instanceof MessageIdV5 id) {
            lastReceivedPositions.put(id.segmentId(), id.v4MessageId());
        }
        return msg;
    }

    @Override
    public Checkpoint checkpoint() {
        Map<Long, org.apache.pulsar.client.api.MessageId> positions = new HashMap<>(lastReceivedPositions);
        return new CheckpointV5(positions);
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
            throw new PulsarClientException(e.getCause());
        }
    }

    // --- Async internals ---

    CompletableFuture<Message<T>> receiveAsync() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return receive();
            } catch (PulsarClientException e) {
                throw new CompletionException(e);
            }
        });
    }

    CompletableFuture<Message<T>> receiveAsync(Duration timeout) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return receive(timeout);
            } catch (PulsarClientException e) {
                throw new CompletionException(e);
            }
        });
    }

    CompletableFuture<Checkpoint> checkpointAsync() {
        return CompletableFuture.completedFuture(checkpoint());
    }

    CompletableFuture<Void> closeAsync() {
        closed = true;
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
                .whenComplete((__, ___) -> segmentReaders.clear());
    }

    // --- Assignment change handling ---

    private void onAssignmentChange(List<ActiveSegment> newSegments, List<ActiveSegment> oldSegments) {
        // Fully async: safe to run on the netty IO thread that delivered the update.
        applyAssignment(newSegments).exceptionally(ex -> {
            log.warn().exceptionMessage(ex).log("Failed to apply segment assignment");
            return null;
        });
    }

    private CompletableFuture<Void> applyAssignment(List<ActiveSegment> assigned) {
        var assignedIds = ConcurrentHashMap.<Long>newKeySet();
        for (var seg : assigned) {
            assignedIds.add(seg.segmentId());
        }

        // Close readers for segments removed from the assignment (sealed, or rebalanced
        // away to another consumer in the same group).
        for (var entry : segmentReaders.entrySet()) {
            if (!assignedIds.contains(entry.getKey())) {
                log.info().attr("segmentId", entry.getKey())
                        .log("Closing reader for segment removed from assignment");
                entry.getValue().thenAccept(r -> r.closeAsync());
                segmentReaders.remove(entry.getKey());
                lastReceivedPositions.remove(entry.getKey());
            }
        }

        // Create readers for new segments asynchronously.
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (var seg : assigned) {
            futures.add(segmentReaders.computeIfAbsent(seg.segmentId(),
                    id -> createSegmentReaderAsync(seg)));
        }

        log.info().attr("segments", assignedIds).log("Checkpoint consumer assignment applied");
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    private CompletableFuture<Reader<T>> createSegmentReaderAsync(ActiveSegment segment) {
        PulsarClientImpl v4Client = client.v4Client();
        org.apache.pulsar.client.api.MessageId startMsgId = resolveStartPosition(segment.segmentId());

        var segConf = new org.apache.pulsar.client.impl.conf.ReaderConfigurationData<T>();
        // Legacy segments wrap an externally managed persistent:// topic; regular ones use the
        // computed segment:// URI. attachTopicName() collapses both into the right URI.
        segConf.getTopicNames().add(segment.attachTopicName());
        segConf.setStartMessageId(startMsgId);
        if (consumerName != null) {
            segConf.setReaderName(consumerName + "-seg-" + segment.segmentId());
        }

        return v4Client.createSegmentReaderAsync(segConf, v4Schema)
                .thenApply(reader -> {
                    startReadLoop(reader, segment.segmentId());
                    return reader;
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
                        segmentReaders.remove(segment.segmentId());
                        lastReceivedPositions.remove(segment.segmentId());
                        return null;
                    }
                    throw ex instanceof CompletionException
                            ? (CompletionException) ex : new CompletionException(ex);
                });
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
        // CheckpointV5.LATEST and anything else: latest.
        return org.apache.pulsar.client.api.MessageId.latest;
    }

    private void startReadLoop(Reader<T> reader, long segmentId) {
        reader.readNextAsync().thenAccept(v4Msg -> {
            // Don't advance the checkpoint here — the read loop pre-fetches into the
            // queue, so updating per-segment positions on wire-receive would skip
            // messages that the application hasn't pulled yet (e.g., a checkpoint()
            // taken right after the app received message N could already point past
            // N+1 if the read loop got ahead). The advance happens in receive() /
            // receiveMulti() instead, where the message crosses into application code.
            messageQueue.add(new MessageV5<>(v4Msg, segmentId));
            if (!closed) {
                startReadLoop(reader, segmentId);
            }
        }).exceptionally(ex -> {
            Throwable cause = ex instanceof CompletionException ce && ce.getCause() != null
                    ? ce.getCause() : ex;
            if (closed || cause instanceof AlreadyClosedException) {
                // The whole consumer is shutting down or this reader was closed
                // externally (segment sealed or rebalanced away). Stop the loop.
                return null;
            }
            if (cause instanceof org.apache.pulsar.client.api.PulsarClientException
                    .TopicTerminatedException) {
                // Sealed segment fully drained server-side. Close the reader and drop
                // it from the map so resources are released; the segment's data has
                // already crossed into messageQueue.
                log.info().attr("segmentId", segmentId)
                        .log("Sealed segment drained, closing reader");
                segmentReaders.remove(segmentId);
                lastReceivedPositions.remove(segmentId);
                reader.closeAsync();
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
                segmentReaders.remove(segmentId);
                lastReceivedPositions.remove(segmentId);
                reader.closeAsync();
                return null;
            }
            log.warn().attr("segmentId", segmentId)
                    .exception(ex).log("Error reading from segment, retrying");
            // Hop to the v4 client's internal executor so repeated synchronous failures
            // don't grow the stack unboundedly.
            client.v4Client().getInternalExecutorService()
                    .execute(() -> startReadLoop(reader, segmentId));
            return null;
        });
    }
}
