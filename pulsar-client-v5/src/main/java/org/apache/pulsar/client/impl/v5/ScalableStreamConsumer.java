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

/**
 * V5 StreamConsumer implementation for scalable topics.
 *
 * <p>Maintains per-segment v4 Consumers with Exclusive subscription type.
 * Messages from all segments are multiplexed into a single receive queue.
 *
 * <p>Each delivered message carries a <em>position vector</em>: a snapshot of the
 * latest delivered message ID per segment at the moment that message enters the
 * queue. When the application calls {@link #acknowledgeCumulative(MessageId)},
 * every segment is cumulatively acknowledged up to the position recorded in that
 * vector. This ensures that acknowledging a single message correctly advances
 * all segments, not just the one it came from.
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

    /**
     * Tracks the latest message ID delivered from each segment. Updated atomically
     * inside {@link #startReceiveLoop} before the message is enqueued, and snapshot
     * into each {@link MessageIdV5} so cumulative acks cover all segments.
     */
    private final ConcurrentHashMap<Long, org.apache.pulsar.client.api.MessageId> latestDelivered =
            new ConcurrentHashMap<>();

    private final LinkedTransferQueue<MessageV5<T>> messageQueue = new LinkedTransferQueue<>();

    private volatile boolean closed = false;
    private final AsyncStreamConsumerV5<T> asyncView;

    private ScalableStreamConsumer(PulsarClientV5 client,
                                   Schema<T> v5Schema,
                                   ConsumerConfigurationData<T> consumerConf,
                                   ScalableConsumerClient session,
                                   String topicName) {
        this.client = client;
        this.v5Schema = v5Schema;
        this.v4Schema = SchemaAdapter.toV4(v5Schema);
        this.consumerConf = consumerConf;
        this.session = session;
        this.topicName = topicName;
        this.subscriptionName = consumerConf.getSubscriptionName();
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
        ScalableStreamConsumer<T> consumer = new ScalableStreamConsumer<>(
                client, v5Schema, consumerConf, session, topicName);
        return consumer.subscribeAssigned(initialAssignment)
                .thenApply(__ -> {
                    session.setListener(consumer);
                    return (StreamConsumer<T>) consumer;
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
    public Messages<T> receiveMulti(int maxNumMessages, Duration timeout) throws PulsarClientException {
        List<Message<T>> batch = new ArrayList<>();
        long deadlineNanos = System.nanoTime() + timeout.toNanos();

        while (batch.size() < maxNumMessages) {
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
            // Drain any immediately available messages
            messageQueue.drainTo(batch, maxNumMessages - batch.size());
        }
        return new MessagesV5<>(batch);
    }

    @Override
    public void acknowledgeCumulative(MessageId messageId) {
        if (!(messageId instanceof MessageIdV5 id)) {
            throw new IllegalArgumentException("Expected MessageIdV5, got: " + messageId.getClass());
        }

        // Ack each segment up to the position recorded in the vector
        for (var entry : id.positionVector().entrySet()) {
            var future = segmentConsumers.get(entry.getKey());
            if (future != null) {
                future.thenAccept(c -> c.acknowledgeCumulativeAsync(entry.getValue()));
            }
        }
    }

    @Override
    public void acknowledgeCumulative(MessageId messageId, Transaction txn) {
        // Transaction support not yet implemented
        throw new UnsupportedOperationException("Transactional ack not yet implemented");
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

    CompletableFuture<Void> closeAsync() {
        closed = true;
        session.close();

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

    // --- Assignment change handling ---

    @Override
    public void onAssignmentChange(List<ActiveSegment> newSegments, List<ActiveSegment> oldSegments) {
        // Fully async: safe to run on the netty IO thread that delivered the update.
        subscribeAssigned(newSegments).exceptionally(ex -> {
            log.warn().exceptionMessage(ex).log("Failed to apply assignment update");
            return null;
        });
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
        for (var entry : segmentConsumers.entrySet()) {
            if (!assignedIds.contains(entry.getKey())) {
                log.info().attr("segmentId", entry.getKey())
                        .log("Closing consumer for segment removed from assignment");
                entry.getValue().thenAccept(c -> c.closeAsync());
                segmentConsumers.remove(entry.getKey());
                latestDelivered.remove(entry.getKey());
            }
        }

        // Subscribe to newly-assigned segments.
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (var seg : assigned) {
            futures.add(segmentConsumers.computeIfAbsent(seg.segmentId(),
                    id -> createSegmentConsumerAsync(seg)));
        }

        log.info().attr("segments", assignedIds).log("Stream consumer assignment applied");
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    private CompletableFuture<org.apache.pulsar.client.api.Consumer<T>> createSegmentConsumerAsync(
            ActiveSegment segment) {
        PulsarClientImpl v4Client = client.v4Client();
        // Clone so per-segment consumers inherit every builder knob the user set
        // (ackTimeout, readCompacted, replicateSubscriptionState, encryption, ...).
        var segConf = consumerConf.clone();
        segConf.getTopicNames().clear();
        segConf.setTopicsPattern(null);
        segConf.getTopicNames().add(segment.segmentTopicName());
        segConf.setSubscriptionType(SubscriptionType.Exclusive);
        if (consumerConf.getConsumerName() != null) {
            segConf.setConsumerName(consumerConf.getConsumerName() + "-seg-" + segment.segmentId());
        }
        return v4Client.subscribeSegmentAsync(segConf, v4Schema)
                .thenApply(consumer -> {
                    startReceiveLoop(consumer, segment.segmentId());
                    return consumer;
                });
    }

    /**
     * Async receive loop for a single segment consumer. Each received message:
     * 1. Updates {@link #latestDelivered} for this segment
     * 2. Snapshots the current position vector across all segments
     * 3. Wraps the message with a {@link MessageIdV5} carrying that vector
     * 4. Enqueues the wrapped message for the application to receive
     */
    private void startReceiveLoop(org.apache.pulsar.client.api.Consumer<T> v4Consumer, long segmentId) {
        v4Consumer.receiveAsync().thenAccept(v4Msg -> {
            // Update the latest delivered position for this segment
            latestDelivered.put(segmentId, v4Msg.getMessageId());

            // Snapshot the position vector (all segments, including this one)
            Map<Long, org.apache.pulsar.client.api.MessageId> positionVector =
                    new HashMap<>(latestDelivered);

            // Create the V5 message with the position vector embedded in the ID
            var msgId = new MessageIdV5(v4Msg.getMessageId(), segmentId, positionVector);
            messageQueue.add(new MessageV5<>(v4Msg, msgId));

            if (!closed) {
                startReceiveLoop(v4Consumer, segmentId);
            }
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
                latestDelivered.remove(segmentId);
                v4Consumer.closeAsync();
                return null;
            }
            log.warn().attr("segmentId", segmentId)
                    .exception(ex).log("Error receiving from segment, retrying");
            startReceiveLoop(v4Consumer, segmentId);
            return null;
        });
    }
}
