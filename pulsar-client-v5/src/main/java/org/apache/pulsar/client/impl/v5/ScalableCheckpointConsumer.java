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
import java.time.Instant;
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
 * <p>Maintains per-segment v4 Readers (no subscription). Messages from all segments
 * are multiplexed into a single receive queue. Supports creating checkpoints (atomic
 * snapshots of positions across all segments) and seeking to previously saved checkpoints.
 */
final class ScalableCheckpointConsumer<T> implements CheckpointConsumer<T>, DagWatchClient.LayoutChangeListener {

    private static final Logger LOG = Logger.get(ScalableCheckpointConsumer.class);
    private final Logger log;

    private final PulsarClientV5 client;
    private final Schema<T> v5Schema;
    private final org.apache.pulsar.client.api.Schema<T> v4Schema;
    private final DagWatchClient dagWatch;
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
                                       DagWatchClient dagWatch,
                                       Checkpoint startPosition,
                                       String consumerName) {
        this.client = client;
        this.v5Schema = v5Schema;
        this.v4Schema = SchemaAdapter.toV4(v5Schema);
        this.dagWatch = dagWatch;
        this.topicName = dagWatch.topicName().toString();
        this.startPosition = startPosition;
        this.consumerName = consumerName;
        this.log = LOG.with().attr("topic", topicName).build();
        this.asyncView = new AsyncCheckpointConsumerV5<>(this);
    }

    /**
     * Create a fully initialized consumer asynchronously. The returned future completes
     * only after every initial segment reader has been successfully created. If any
     * reader creation fails, all already-created readers are closed and the future
     * completes exceptionally.
     */
    static <T> CompletableFuture<CheckpointConsumer<T>> createAsync(PulsarClientV5 client,
                                                                    Schema<T> v5Schema,
                                                                    DagWatchClient dagWatch,
                                                                    ClientSegmentLayout initialLayout,
                                                                    Checkpoint startPosition,
                                                                    String consumerName) {
        ScalableCheckpointConsumer<T> consumer = new ScalableCheckpointConsumer<>(
                client, v5Schema, dagWatch, startPosition, consumerName);
        return consumer.createSegmentReaders(initialLayout)
                .thenApply(__ -> {
                    dagWatch.setListener(consumer);
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
            return messageQueue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Receive interrupted", e);
        }
    }

    @Override
    public Message<T> receive(Duration timeout) throws PulsarClientException {
        try {
            return messageQueue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
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
                batch.add(msg);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new PulsarClientException("Receive interrupted", e);
            }
            messageQueue.drainTo(batch, maxMessages - batch.size());
        }
        return new MessagesV5<>(batch);
    }

    @Override
    public Checkpoint checkpoint() {
        Map<Long, org.apache.pulsar.client.api.MessageId> positions = new HashMap<>(lastReceivedPositions);
        return new CheckpointV5(positions, Instant.now());
    }

    @Override
    public void seek(Checkpoint checkpoint) throws PulsarClientException {
        try {
            seekAsync(checkpoint).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Seek interrupted", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof PulsarClientException pce) {
                throw pce;
            }
            throw new PulsarClientException(e.getCause());
        }
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

    CompletableFuture<Void> seekAsync(Checkpoint checkpoint) {
        if (checkpoint instanceof CheckpointV5 cp) {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (var entry : cp.segmentPositions().entrySet()) {
                var readerFuture = segmentReaders.get(entry.getKey());
                if (readerFuture != null) {
                    futures.add(readerFuture.thenCompose(r -> r.seekAsync(entry.getValue())));
                }
            }
            return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                    .whenComplete((__, ___) -> messageQueue.clear());
        } else if (checkpoint == CheckpointV5.EARLIEST) {
            return seekAllAsync(org.apache.pulsar.client.api.MessageId.earliest);
        } else if (checkpoint == CheckpointV5.LATEST) {
            return seekAllAsync(org.apache.pulsar.client.api.MessageId.latest);
        } else {
            return CompletableFuture.failedFuture(
                    new PulsarClientException("Unsupported checkpoint type: " + checkpoint.getClass()));
        }
    }

    private CompletableFuture<Void> seekAllAsync(org.apache.pulsar.client.api.MessageId position) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (var readerFuture : segmentReaders.values()) {
            futures.add(readerFuture.thenCompose(r -> r.seekAsync(position)));
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .whenComplete((__, ___) -> messageQueue.clear());
    }

    CompletableFuture<Void> closeAsync() {
        closed = true;
        dagWatch.close();

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

    // --- Layout change handling ---

    @Override
    public void onLayoutChange(ClientSegmentLayout newLayout, ClientSegmentLayout oldLayout) {
        // Fully async: safe to run on the netty IO thread that delivered the update.
        createSegmentReaders(newLayout).exceptionally(ex -> {
            log.warn().exceptionMessage(ex).log("Failed to apply layout update");
            return null;
        });
    }

    private CompletableFuture<Void> createSegmentReaders(ClientSegmentLayout layout) {
        var activeIds = ConcurrentHashMap.<Long>newKeySet();
        for (var seg : layout.activeSegments()) {
            activeIds.add(seg.segmentId());
        }

        // Close readers for segments that are no longer active (fire-and-forget).
        for (var entry : segmentReaders.entrySet()) {
            if (!activeIds.contains(entry.getKey())) {
                log.info().attr("segmentId", entry.getKey())
                        .log("Closing reader for sealed segment");
                entry.getValue().thenAccept(r -> r.closeAsync());
                segmentReaders.remove(entry.getKey());
            }
        }

        // Create readers for new segments asynchronously.
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (var seg : layout.activeSegments()) {
            futures.add(segmentReaders.computeIfAbsent(seg.segmentId(),
                    id -> createSegmentReaderAsync(seg)));
        }

        log.info().attr("epoch", layout.epoch())
                .attr("segments", activeIds).log("Checkpoint consumer layout applied");
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    private CompletableFuture<Reader<T>> createSegmentReaderAsync(ActiveSegment segment) {
        PulsarClientImpl v4Client = client.v4Client();
        org.apache.pulsar.client.api.MessageId startMsgId = resolveStartPosition(segment.segmentId());

        var builder = v4Client.newReader(v4Schema)
                .topic(segment.segmentTopicName())
                .startMessageId(startMsgId);
        if (consumerName != null) {
            builder.readerName(consumerName + "-seg-" + segment.segmentId());
        }

        return builder.createAsync()
                .thenApply(reader -> {
                    startReadLoop(reader, segment.segmentId());
                    return reader;
                });
    }

    private org.apache.pulsar.client.api.MessageId resolveStartPosition(long segmentId) {
        // If we have a regular checkpoint, use the segment's position
        if (startPosition instanceof CheckpointV5 cp) {
            var pos = cp.segmentPositions().get(segmentId);
            if (pos != null) {
                return pos;
            }
        }
        // For sentinel checkpoints or missing segments, use earliest/latest
        if (startPosition == CheckpointV5.EARLIEST) {
            return org.apache.pulsar.client.api.MessageId.earliest;
        }
        // Default to latest
        return org.apache.pulsar.client.api.MessageId.latest;
    }

    private void startReadLoop(Reader<T> reader, long segmentId) {
        reader.readNextAsync().thenAccept(v4Msg -> {
            lastReceivedPositions.put(segmentId, v4Msg.getMessageId());
            messageQueue.add(new MessageV5<>(v4Msg, segmentId));
            if (!closed) {
                startReadLoop(reader, segmentId);
            }
        }).exceptionally(ex -> {
            if (!closed) {
                log.warn().attr("segmentId", segmentId)
                        .exception(ex).log("Error reading from segment, retrying");
                startReadLoop(reader, segmentId);
            }
            return null;
        });
    }
}
