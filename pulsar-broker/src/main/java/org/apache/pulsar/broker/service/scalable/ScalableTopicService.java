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
package org.apache.pulsar.broker.service.scalable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.transaction.metadata.TxnMetadataStore;
import org.apache.pulsar.common.api.proto.ScalableConsumerType;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ScalableTopicStats;
import org.apache.pulsar.common.scalable.SegmentInfo;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;

/**
 * Central service managing all scalable topics on this broker.
 *
 * <p>Lifecycle is tied to {@link BrokerService}. This service handles:
 * <ul>
 *   <li>Creating and deleting scalable topics</li>
 *   <li>Managing {@link ScalableTopicController} instances for topics this broker coordinates</li>
 *   <li>Admin operations: split/merge</li>
 * </ul>
 */
@CustomLog
public class ScalableTopicService {

    private final BrokerService brokerService;
    private final ScalableTopicResources resources;
    private final CoordinationService coordinationService;

    /**
     * Active controllers for topics this broker coordinates. The value is a future so
     * concurrent {@link #getOrCreateController(TopicName)} callers share a single
     * initialize() attempt rather than racing to create separate instances.
     */
    private final ConcurrentHashMap<String, CompletableFuture<ScalableTopicController>> controllers =
            new ConcurrentHashMap<>();

    public ScalableTopicService(BrokerService brokerService,
                                ScalableTopicResources resources,
                                CoordinationService coordinationService) {
        this.brokerService = brokerService;
        this.resources = resources;
        this.coordinationService = coordinationService;
    }

    // --- Lifecycle ---

    public void start() {
        log.info("ScalableTopicService started");
    }

    public void close() {
        log.info().attr("controllerCount", controllers.size())
                .log("Closing ScalableTopicService, releasing controllers");
        List<CompletableFuture<Void>> closeFutures = controllers.values().stream()
                .map(future -> future
                        .thenCompose(ScalableTopicController::close)
                        .exceptionally(ex -> {
                            log.warn().exceptionMessage(ex).log("Error closing controller");
                            return null;
                        }))
                .toList();
        FutureUtil.waitForAll(closeFutures).join();
        controllers.clear();
    }

    // --- Controller management ---

    /**
     * Get or create a controller for a scalable topic. The controller will attempt
     * leader election; only the leader actively coordinates consumers.
     */
    public CompletableFuture<ScalableTopicController> getOrCreateController(TopicName topic) {
        String key = topic.toString();
        CompletableFuture<ScalableTopicController> stored = controllers.computeIfAbsent(key, k -> {
            ScalableTopicController controller = new ScalableTopicController(
                    topic, resources, brokerService, coordinationService);
            return controller.initialize().thenApply(__ -> controller);
        });
        // Evict failed futures so subsequent callers can retry. This runs *outside*
        // computeIfAbsent, so modifying the map here is safe.
        return stored.exceptionally(ex -> {
            controllers.remove(key, stored);
            throw new RuntimeException("Failed to initialize controller for " + topic, ex);
        });
    }

    /**
     * Release the controller for a topic (e.g., on topic unload).
     */
    public CompletableFuture<Void> releaseController(TopicName topic) {
        CompletableFuture<ScalableTopicController> future = controllers.remove(topic.toString());
        if (future != null) {
            return future.thenCompose(ScalableTopicController::close);
        }
        return CompletableFuture.completedFuture(null);
    }

    // --- Admin operations ---

    /**
     * Create a new scalable topic with the given number of initial segments.
     */
    public CompletableFuture<Void> createScalableTopic(TopicName topic, int numInitialSegments) {
        return createScalableTopic(topic, numInitialSegments, Map.of());
    }

    public CompletableFuture<Void> createScalableTopic(TopicName topic, int numInitialSegments,
                                                        Map<String, String> properties) {
        if (topic.getDomain() != TopicDomain.topic) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Expected topic domain, got: " + topic.getDomain()));
        }
        if (numInitialSegments < 1) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("numInitialSegments must be >= 1"));
        }

        ScalableTopicMetadata metadata = ScalableTopicController.createInitialMetadata(
                numInitialSegments,
                brokerService.getPulsar().getConfiguration().getScalableTopicEntryBucketBudget(),
                properties);

        // Write the scalable metadata FIRST, then materialize the underlying segment topics.
        // The metadata is the source of truth: its presence is what defines whether the topic
        // "exists"; segment topics are derived state. Writing metadata first means a partial
        // failure (a segment create throws, or the broker crashes mid-way) leaves no orphaned
        // segment topics dangling without a parent — everything created is already referenced
        // by valid metadata. An active segment whose backing topic is missing is
        // (re)materialized on demand the first time a client connects to it — see the
        // active-segment reconciliation in BrokerService.isAllowAutoTopicCreationAsync — so
        // eager materialization here is a happy-path latency optimization, not a correctness
        // requirement.
        return resources.createScalableTopicAsync(topic, metadata)
                .thenCompose(__ -> {
                    List<CompletableFuture<Void>> segmentFutures =
                            metadata.getSegments().values().stream()
                                    .map(segment -> createUnderlyingSegmentTopic(topic, segment))
                                    .toList();
                    return FutureUtil.waitForAll(segmentFutures);
                });
    }

    /**
     * Split a segment (delegates to controller). Callers must be the controller leader;
     * the REST layer redirects non-leaders via
     * {@code ScalableTopics.redirectToControllerLeaderIfNeeded}, and the controller's
     * {@code checkLeader()} enforces leadership on the service side.
     */
    public CompletableFuture<Void> splitSegment(TopicName topic, long segmentId) {
        return getOrCreateController(topic)
                .thenCompose(controller -> controller.splitSegment(segmentId))
                .thenApply(__ -> null);
    }

    /**
     * Merge two adjacent segments (delegates to controller). Same leader contract as
     * {@link #splitSegment(TopicName, long)}.
     */
    public CompletableFuture<Void> mergeSegments(TopicName topic, long segmentId1, long segmentId2) {
        return getOrCreateController(topic)
                .thenCompose(controller -> controller.mergeSegments(segmentId1, segmentId2))
                .thenApply(__ -> null);
    }

    /**
     * Create a subscription on a scalable topic (delegates to controller leader).
     * Propagates the subscription to all active segments.
     */
    public CompletableFuture<Void> createSubscription(TopicName topic, String subscription,
            org.apache.pulsar.broker.resources.SubscriptionType type) {
        return getOrCreateController(topic)
                .thenCompose(controller -> controller.createSubscription(subscription, type));
    }

    /**
     * Delete a subscription from a scalable topic (delegates to controller leader).
     * Unregisters all consumers and deletes the subscription from every segment.
     */
    public CompletableFuture<Void> deleteSubscription(TopicName topic, String subscription) {
        return getOrCreateController(topic)
                .thenCompose(controller -> controller.deleteSubscription(subscription));
    }

    /**
     * Reset a subscription's cursor across every segment to the given wall-clock
     * timestamp. Delegates to the controller leader; the controller uses each
     * segment's recorded {@code [createdAtMs, sealedAtMs)} window to dispatch the
     * cheapest per-segment op (skip-all, seek-by-timestamp, or seek-to-earliest).
     */
    public CompletableFuture<Void> seekSubscription(TopicName topic, String subscription,
                                                     long timestampMs) {
        return getOrCreateController(topic)
                .thenCompose(controller -> controller.seekSubscription(subscription, timestampMs));
    }

    /**
     * Skip every undelivered message on the subscription, across every segment in
     * the DAG. Delegates to the controller leader.
     */
    public CompletableFuture<Void> clearBacklog(TopicName topic, String subscription) {
        return getOrCreateController(topic)
                .thenCompose(controller -> controller.clearBacklog(subscription));
    }

    /**
     * Get aggregated stats for a scalable topic. Read-only: does not require leadership.
     * Returns segment-DAG counts and per-subscription consumer counts, read from the
     * metadata store so the answer is consistent regardless of which broker is serving the
     * request.
     */
    public CompletableFuture<ScalableTopicStats> getStats(TopicName topic) {
        return getOrCreateController(topic)
                .thenCompose(ScalableTopicController::getStats);
    }

    /**
     * Delete a scalable topic and all its segment topics.
     */
    public CompletableFuture<Void> deleteScalableTopic(TopicName topic) {
        // When transactions are enabled, the segments carry durable /txn/segment-state records
        // (watermark + aborted-txn records). Delete them alongside the segment topics so they don't
        // outlive the data.
        TxnMetadataStore txnStore =
                brokerService.getPulsar().getConfiguration().isTransactionCoordinatorEnabled()
                        ? new TxnMetadataStore(brokerService.getPulsar().getLocalMetadataStore())
                        : null;
        return releaseController(topic)
                .thenCompose(__ -> resources.getScalableTopicMetadataAsync(topic))
                .thenCompose(optMd -> {
                    if (optMd.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    ScalableTopicMetadata metadata = optMd.get();
                    // Delete all underlying segment topics, then their durable transaction state.
                    return FutureUtil.waitForAll(
                            metadata.getSegments().values().stream()
                                    .map(segment -> deleteUnderlyingSegmentTopic(topic, segment)
                                            .thenCompose(__ -> cleanupSegmentTxnState(txnStore, topic, segment)))
                                    .toList()
                    );
                })
                .thenCompose(__ -> resources.deleteScalableTopicAsync(topic));
    }

    /** Delete the durable {@code /txn/segment-state} records for a segment being dropped. */
    private CompletableFuture<Void> cleanupSegmentTxnState(TxnMetadataStore txnStore,
                                                          TopicName parentTopic, SegmentInfo segment) {
        if (txnStore == null) {
            return CompletableFuture.completedFuture(null);
        }
        String segmentName = SegmentTopicName.fromParent(
                parentTopic, segment.hashRange(), segment.segmentId()).toString();
        return txnStore.deleteAllSegmentState(segmentName);
    }

    /**
     * Register a scalable consumer with the controller leader for {@code topic}.
     * Persists a durable session and returns the consumer's segment assignment.
     *
     * <p>The {@code consumerType} drives broker-side semantics that depend on the
     * consumer mode — most importantly whether the {@link SubscriptionCoordinator}
     * enforces parent-drain ordering before handing out children of a split. STREAM
     * consumers want it (per-key ordering); CHECKPOINT and QUEUE consumers don't
     * (they either track position client-side or have shared, already-out-of-order
     * delivery semantics).
     */
    public CompletableFuture<ConsumerAssignment> registerConsumer(TopicName topic, String subscription,
                                                                   String consumerName, long consumerId,
                                                                   ScalableConsumerType
                                                                           consumerType,
                                                                   org.apache.pulsar.broker.service.TransportCnx cnx) {
        return getOrCreateController(topic)
                .thenCompose(controller ->
                        controller.registerConsumer(subscription, consumerName, consumerId,
                                consumerType, cnx));
    }

    /**
     * Called when a scalable consumer's transport connection drops. Forwards to the
     * controller which marks the session disconnected and starts its grace timer.
     * No-op if the controller is not held locally.
     */
    public void onConsumerDisconnect(TopicName topic, String subscription, String consumerName) {
        CompletableFuture<ScalableTopicController> future = controllers.get(topic.toString());
        if (future != null) {
            future.thenAccept(c -> c.onConsumerDisconnect(subscription, consumerName))
                    .exceptionally(ex -> null);
        }
    }

    // --- Internal helpers ---

    private CompletableFuture<Void> createUnderlyingSegmentTopic(TopicName parentTopic, SegmentInfo segment) {
        String segmentName = SegmentTopicName.fromParent(
                parentTopic, segment.hashRange(), segment.segmentId()).toString();
        try {
            return brokerService.getPulsar().getAdminClient()
                    .scalableTopics().createSegmentAsync(segmentName, java.util.List.of())
                    .thenRun(() -> log.info().attr("segment", segmentName)
                            .log("Created segment topic"));
        } catch (PulsarServerException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<Void> deleteUnderlyingSegmentTopic(TopicName parentTopic, SegmentInfo segment) {
        String segmentName = SegmentTopicName.fromParent(
                parentTopic, segment.hashRange(), segment.segmentId()).toString();
        try {
            return brokerService.getPulsar().getAdminClient()
                    .scalableTopics().deleteSegmentAsync(segmentName, true)
                    .exceptionally(ex -> {
                        log.warn().attr("segment", segmentName).exceptionMessage(ex)
                                .log("Failed to delete segment topic");
                        return null;
                    });
        } catch (PulsarServerException e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
