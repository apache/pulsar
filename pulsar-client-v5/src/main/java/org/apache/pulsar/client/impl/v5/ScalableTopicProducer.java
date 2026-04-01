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

    // Per-segment v4 producers. Key is segmentId.
    private final ConcurrentHashMap<Long, org.apache.pulsar.client.api.Producer<T>> segmentProducers =
            new ConcurrentHashMap<>();

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
        // Aggregate: return the max across all segment producers
        long max = -1;
        for (var producer : segmentProducers.values()) {
            max = Math.max(max, producer.getLastSequenceId());
        }
        return max;
    }

    @Override
    public AsyncProducer<T> async() {
        return asyncView;
    }

    @Override
    public void close() throws PulsarClientException {
        closed = true;
        dagWatch.close();

        List<Exception> errors = new ArrayList<>();
        for (var producer : segmentProducers.values()) {
            try {
                producer.close();
            } catch (Exception e) {
                errors.add(e);
            }
        }
        segmentProducers.clear();

        if (!errors.isEmpty()) {
            PulsarClientException ex = new PulsarClientException("Failed to close some segment producers");
            errors.forEach(ex::addSuppressed);
            throw ex;
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
            java.util.List<String> replicationClusters) throws PulsarClientException {

        for (int attempt = 0; attempt < 3; attempt++) {
            long segmentId = routeMessage(key);
            var producer = getOrCreateSegmentProducer(segmentId);

            try {
                var v4MsgId = buildV4Message(producer, key, value, properties,
                        eventTime, sequenceId, deliverAfter, deliverAt, replicationClusters)
                        .send();
                return new MessageIdV5(v4MsgId, segmentId);
            } catch (org.apache.pulsar.client.api.PulsarClientException.TopicTerminatedException e) {
                // Segment was terminated (split/merge) — remove stale producer and retry
                // with the updated layout (which should arrive via DagWatchClient)
                log.info().attr("segmentId", segmentId)
                        .attr("attempt", attempt + 1)
                        .log("Segment terminated, waiting for layout update");
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
            java.util.List<String> replicationClusters) {

        return sendInternalAsyncWithRetry(key, value, properties,
                eventTime, sequenceId, deliverAfter, deliverAt, replicationClusters, 0);
    }

    private CompletableFuture<MessageIdV5> sendInternalAsyncWithRetry(
            String key, T value, java.util.Map<String, String> properties,
            java.time.Instant eventTime, Long sequenceId,
            java.time.Duration deliverAfter, java.time.Instant deliverAt,
            java.util.List<String> replicationClusters, int attempt) {

        long segmentId;
        try {
            segmentId = routeMessage(key);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }

        org.apache.pulsar.client.api.Producer<T> producer;
        try {
            producer = getOrCreateSegmentProducer(segmentId);
        } catch (PulsarClientException e) {
            return CompletableFuture.failedFuture(e);
        }

        return buildV4Message(producer, key, value, properties,
                eventTime, sequenceId, deliverAfter, deliverAt, replicationClusters)
                .sendAsync()
                .thenApply(v4MsgId -> new MessageIdV5(v4MsgId, segmentId))
                .exceptionallyCompose(ex -> {
                    Throwable cause = ex instanceof java.util.concurrent.CompletionException
                            ? ex.getCause() : ex;
                    if (cause instanceof org.apache.pulsar.client.api.PulsarClientException
                            .TopicTerminatedException && attempt < 3) {
                        log.info().attr("segmentId", segmentId)
                                .attr("attempt", attempt + 1).log("Segment terminated, retrying");
                        segmentProducers.remove(segmentId);
                        return CompletableFuture.supplyAsync(() -> null,
                                CompletableFuture.delayedExecutor(
                                        100L * (attempt + 1),
                                        java.util.concurrent.TimeUnit.MILLISECONDS))
                                .thenCompose(__ -> sendInternalAsyncWithRetry(
                                        key, value, properties, eventTime, sequenceId,
                                        deliverAfter, deliverAt, replicationClusters, attempt + 1));
                    }
                    return CompletableFuture.failedFuture(ex);
                });
    }

    private org.apache.pulsar.client.api.TypedMessageBuilder<T> buildV4Message(
            org.apache.pulsar.client.api.Producer<T> producer,
            String key, T value, java.util.Map<String, String> properties,
            java.time.Instant eventTime, Long sequenceId,
            java.time.Duration deliverAfter, java.time.Instant deliverAt,
            java.util.List<String> replicationClusters) {

        var msgBuilder = producer.newMessage().value(value);

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
     * Flush all segment producers asynchronously.
     */
    CompletableFuture<Void> flushAsync() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (var producer : segmentProducers.values()) {
            futures.add(producer.flushAsync());
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    CompletableFuture<Void> closeAsync() {
        closed = true;
        dagWatch.close();

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (var producer : segmentProducers.values()) {
            futures.add(producer.closeAsync());
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .whenComplete((__, ___) -> segmentProducers.clear());
    }

    // --- Layout change handling ---

    @Override
    public void onLayoutChange(ClientSegmentLayout newLayout, ClientSegmentLayout oldLayout) {
        applyLayout(newLayout);
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
                entry.getValue().closeAsync().whenComplete((__, ex) -> {
                    if (ex != null) {
                        log.warn().attr("segmentId", entry.getKey())
                                .exceptionMessage(ex).log("Error closing producer for segment");
                    }
                });
                segmentProducers.remove(entry.getKey());
            }
        }

        // New segment producers will be created lazily on first message
        log.info().attr("epoch", layout.epoch())
                .attr("activeSegments", newSegmentIds).log("Layout applied");
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

    private org.apache.pulsar.client.api.Producer<T> getOrCreateSegmentProducer(long segmentId)
            throws PulsarClientException {
        var existing = segmentProducers.get(segmentId);
        if (existing != null) {
            return existing;
        }

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
                throw new RuntimeException("Segment " + id + " not found in active segments");
            }

            try {
                PulsarClientImpl v4Client = client.v4Client();
                var segConf = new org.apache.pulsar.client.impl.conf.ProducerConfigurationData();
                segConf.setTopicName(segmentTopicName);
                segConf.setSendTimeoutMs(producerConf.getSendTimeoutMs());
                segConf.setBlockIfQueueFull(producerConf.isBlockIfQueueFull());
                if (producerConf.getProducerName() != null
                        && !producerConf.getProducerName().isEmpty()) {
                    segConf.setProducerName(producerConf.getProducerName() + "-seg-" + id);
                }
                return v4Client.createSegmentProducerAsync(segConf, v4Schema)
                        .get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
