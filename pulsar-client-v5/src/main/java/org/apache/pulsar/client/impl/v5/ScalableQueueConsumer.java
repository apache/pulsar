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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.Transaction;
import org.apache.pulsar.client.api.v5.async.AsyncQueueConsumer;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.v5.SegmentRouter.ActiveSegment;
import org.apache.pulsar.common.util.Backoff;

/**
 * V5 QueueConsumer implementation for scalable topics.
 *
 * <p>Maintains per-segment v4 Consumers with Shared subscription type.
 * Messages from all segments are multiplexed into a single receive queue.
 * Individual acknowledgments and negative acknowledgments are routed to
 * the correct segment consumer via the segment ID in {@link MessageIdV5}.
 */
final class ScalableQueueConsumer<T> implements QueueConsumer<T>, DagWatchClient.LayoutChangeListener {

    private static final Logger LOG = Logger.get(ScalableQueueConsumer.class);
    private final Logger log;

    private final PulsarClientV5 client;
    private final Schema<T> v5Schema;
    private final org.apache.pulsar.client.api.Schema<T> v4Schema;
    private final ConsumerConfigurationData<T> consumerConf;
    private final DagWatchClient dagWatch;
    private final String topicName;
    private final String subscriptionName;

    /**
     * Per-segment v4 consumers. Stores futures (not consumers) so concurrent
     * operations (ack, close) can chain on in-flight subscribes without
     * racing against subscribe completion.
     */
    private final ConcurrentHashMap<Long, CompletableFuture<org.apache.pulsar.client.api.Consumer<T>>>
            segmentConsumers = new ConcurrentHashMap<>();
    private final LinkedTransferQueue<MessageV5<T>> messageQueue = new LinkedTransferQueue<>();

    private volatile boolean closed = false;
    private final AsyncQueueConsumerV5<T> asyncView;

    /** Most recent layout target. Reconciles always converge toward this value. */
    private volatile ClientSegmentLayout latestLayout;
    /** Coalesces concurrent reconcile attempts; only one runs at a time. */
    private final AtomicBoolean reconcileInProgress = new AtomicBoolean(false);
    private final Backoff reconcileBackoff = Backoff.builder()
            .initialDelay(Duration.ofMillis(100))
            .maxBackoff(Duration.ofSeconds(30))
            .build();

    private ScalableQueueConsumer(PulsarClientV5 client,
                                  Schema<T> v5Schema,
                                  ConsumerConfigurationData<T> consumerConf,
                                  DagWatchClient dagWatch) {
        this.client = client;
        this.v5Schema = v5Schema;
        this.v4Schema = SchemaAdapter.toV4(v5Schema);
        this.consumerConf = consumerConf;
        this.dagWatch = dagWatch;
        this.topicName = dagWatch.topicName().toString();
        this.subscriptionName = consumerConf.getSubscriptionName();
        this.log = LOG.with().attr("topic", topicName).attr("subscription", subscriptionName).build();
        this.asyncView = new AsyncQueueConsumerV5<>(this);
    }

    /**
     * Create a fully initialized consumer asynchronously. The returned future completes
     * only after every initial segment has been successfully subscribed. If any segment
     * fails to subscribe, all already-subscribed segments are closed and the future
     * completes exceptionally.
     */
    static <T> CompletableFuture<QueueConsumer<T>> createAsync(PulsarClientV5 client,
                                                               Schema<T> v5Schema,
                                                               ConsumerConfigurationData<T> consumerConf,
                                                               DagWatchClient dagWatch,
                                                               ClientSegmentLayout initialLayout) {
        ScalableQueueConsumer<T> consumer = new ScalableQueueConsumer<>(client, v5Schema, consumerConf, dagWatch);
        return consumer.subscribeSegments(initialLayout)
                .thenApply(__ -> {
                    dagWatch.setListener(consumer);
                    return (QueueConsumer<T>) consumer;
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
    public String subscription() {
        return subscriptionName;
    }

    @Override
    public String consumerName() {
        return consumerConf.getConsumerName();
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
    public void acknowledge(MessageId messageId) {
        if (!(messageId instanceof MessageIdV5 id)) {
            throw new IllegalArgumentException("Expected MessageIdV5, got: " + messageId.getClass());
        }
        var future = segmentConsumers.get(id.segmentId());
        if (future != null) {
            future.thenAccept(c -> c.acknowledgeAsync(id.v4MessageId()));
        }
    }

    @Override
    public void acknowledge(MessageId messageId, Transaction txn) {
        throw new UnsupportedOperationException("Transactional ack not yet implemented");
    }

    @Override
    public void negativeAcknowledge(MessageId messageId) {
        if (!(messageId instanceof MessageIdV5 id)) {
            throw new IllegalArgumentException("Expected MessageIdV5, got: " + messageId.getClass());
        }
        var future = segmentConsumers.get(id.segmentId());
        if (future != null) {
            future.thenAccept(c -> c.negativeAcknowledge(id.v4MessageId()));
        }
    }

    @Override
    public AsyncQueueConsumer<T> async() {
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

    CompletableFuture<Void> closeAsync() {
        closed = true;
        dagWatch.close();

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (var future : segmentConsumers.values()) {
            futures.add(future
                    .handle((consumer, ex) -> consumer)
                    .thenCompose(consumer -> consumer != null ? consumer.closeAsync()
                            : CompletableFuture.completedFuture(null)));
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .whenComplete((__, ___) -> segmentConsumers.clear());
    }

    // --- Layout change handling ---

    @Override
    public void onLayoutChange(ClientSegmentLayout newLayout, ClientSegmentLayout oldLayout) {
        // Fully async: safe to run on the netty IO thread that delivered the update.
        // Store the target and kick off a reconcile. If a reconcile is already running,
        // it will re-read latestLayout once it finishes. On failure, the reconcile
        // reschedules itself with exponential backoff until it succeeds (or we close).
        latestLayout = newLayout;
        reconcile();
    }

    private void reconcile() {
        if (closed) {
            return;
        }
        if (!reconcileInProgress.compareAndSet(false, true)) {
            // A reconcile is already in progress; it will observe the updated latestLayout
            // when it finishes and re-run if needed.
            return;
        }
        ClientSegmentLayout target = latestLayout;
        subscribeSegments(target).whenComplete((__, ex) -> {
            reconcileInProgress.set(false);
            if (closed) {
                return;
            }
            if (ex == null) {
                reconcileBackoff.reset();
                // If a newer layout arrived during this reconcile, run again to converge.
                if (latestLayout != target) {
                    reconcile();
                }
                return;
            }
            // Evict failed subscribe futures so the next attempt can re-try them.
            evictFailedSegmentConsumers();
            Duration delay = reconcileBackoff.next();
            log.warn().attr("delayMs", delay.toMillis()).exceptionMessage(ex)
                    .log("Failed to apply layout update, retrying after backoff");
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

    private CompletableFuture<Void> subscribeSegments(ClientSegmentLayout layout) {
        var activeIds = ConcurrentHashMap.<Long>newKeySet();
        for (var seg : layout.activeSegments()) {
            activeIds.add(seg.segmentId());
        }

        // Close consumers for segments that are no longer active (fire-and-forget).
        for (var entry : segmentConsumers.entrySet()) {
            if (!activeIds.contains(entry.getKey())) {
                log.info().attr("segmentId", entry.getKey())
                        .log("Closing consumer for sealed segment");
                entry.getValue().thenAccept(c -> c.closeAsync());
                segmentConsumers.remove(entry.getKey());
            }
        }

        // Subscribe to new segments. The returned future completes when all subscribes
        // finish (successfully or with error).
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (var seg : layout.activeSegments()) {
            futures.add(segmentConsumers.computeIfAbsent(seg.segmentId(),
                    id -> createSegmentConsumerAsync(seg)));
        }

        log.info().attr("epoch", layout.epoch())
                .attr("segments", activeIds).log("Queue consumer layout applied");
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    private CompletableFuture<org.apache.pulsar.client.api.Consumer<T>> createSegmentConsumerAsync(
            ActiveSegment segment) {
        PulsarClientImpl v4Client = client.v4Client();
        var builder = v4Client.newConsumer(v4Schema)
                .topic(segment.segmentTopicName())
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared);
        if (consumerConf.getConsumerName() != null) {
            builder.consumerName(consumerConf.getConsumerName() + "-seg-" + segment.segmentId());
        }
        return builder.subscribeAsync()
                .thenApply(consumer -> {
                    startReceiveLoop(consumer, segment.segmentId());
                    return consumer;
                });
    }

    private void startReceiveLoop(org.apache.pulsar.client.api.Consumer<T> v4Consumer, long segmentId) {
        v4Consumer.receiveAsync().thenAccept(v4Msg -> {
            messageQueue.add(new MessageV5<>(v4Msg, segmentId));
            if (!closed) {
                startReceiveLoop(v4Consumer, segmentId);
            }
        }).exceptionally(ex -> {
            Throwable cause = ex instanceof CompletionException ce && ce.getCause() != null ? ce.getCause() : ex;
            if (closed
                    || cause instanceof org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException) {
                // This segment consumer is done (either the whole consumer is closing,
                // or the segment was sealed and its v4 consumer closed by a layout update).
                return null;
            }
            log.warn().attr("segmentId", segmentId)
                    .exception(ex).log("Error receiving from segment, retrying");
            // Hop to the v4 client's internal executor so repeated synchronous failures
            // don't grow the stack unboundedly.
            client.v4Client().getInternalExecutorService()
                    .execute(() -> startReceiveLoop(v4Consumer, segmentId));
            return null;
        });
    }
}
