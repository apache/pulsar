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

import io.opentelemetry.api.OpenTelemetry;
import io.prometheus.client.CollectorRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.GetStatsOptions;
import org.apache.pulsar.client.admin.PulsarAdmin;
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

    /**
     * Options used to read a segment's backing-topic stats, both for the per-segment stats
     * endpoint and for the per-segment collection behind the topic-level stats: imprecise
     * backlog (no ledger scan), per-subscription backlog size, no earliest-time-in-backlog
     * lookup, publishers and consumers included.
     */
    public static final GetStatsOptions SEGMENT_STATS_OPTIONS =
            new GetStatsOptions(false, true, false, false, false);

    /** How long a request waits for a controller taking over as the leader, before going on. */
    private static final Duration TAKEOVER_WAIT = Duration.ofSeconds(10);

    private final BrokerService brokerService;
    private final ScalableTopicResources resources;
    private final CoordinationService coordinationService;
    private final ScalableTopicControllerMetrics metrics;

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
        this.metrics = createControllerMetrics();
    }

    private ScalableTopicControllerMetrics createControllerMetrics() {
        try {
            var pulsar = brokerService.getPulsar();
            var telemetry = pulsar.getOpenTelemetry();
            var config = pulsar.getConfiguration();
            return new ScalableTopicControllerMetrics(telemetry != null ? telemetry.getMeter()
                    : OpenTelemetry.noop().getMeter(ScalableTopicService.class.getName()),
                    config != null ? Objects.toString(config.getClusterName(), "") : "", this::metricCounts);
        } catch (RuntimeException e) {
            log.warn().exceptionMessage(e)
                    .log("Failed to initialize scalable-topic controller metrics; continuing without them");
            return ScalableTopicControllerMetrics.NOOP;
        }
    }

    private ScalableTopicControllerMetrics.Counts metricCounts() {
        long leaders = 0;
        long active = 0;
        long sealed = 0;
        for (CompletableFuture<ScalableTopicController> future : controllers.values()) {
            if (!future.isDone() || future.isCompletedExceptionally()) {
                continue;
            }
            ScalableTopicControllerMetrics.Counts counts = future.join().metricCounts();
            leaders += counts.controllers();
            active += counts.active();
            sealed += counts.sealed();
        }
        return new ScalableTopicControllerMetrics.Counts(leaders, active, sealed);
    }

    private volatile ScalableTopicConsumerMetrics consumerMetrics;

    // --- Lifecycle ---

    public synchronized void start() {
        if (consumerMetrics != null) {
            return;
        }
        try {
            consumerMetrics = new ScalableTopicConsumerMetrics(
                    brokerService.getPulsar().getOpenTelemetry().getMeter(), CollectorRegistry.defaultRegistry,
                    brokerService.getPulsar().getConfiguration().getClusterName(), this::consumerStats);
        } catch (RuntimeException e) {
            log.warn().exceptionMessage(e)
                    .log("Failed to initialize scalable-topic consumer metrics; continuing without them");
        }
        log.info("ScalableTopicService started");
    }

    /** Release only this service's instruments, without changing controller or metadata-store lifecycles. */
    public void closeMetrics() {
        if (consumerMetrics != null) {
            consumerMetrics.close();
        }
        metrics.close();
    }

    public void close() {
        closeMetrics();
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

    SubscriptionCoordinator.ConsumerStats consumerStats() {
        SubscriptionCoordinator.ConsumerStats stats = SubscriptionCoordinator.ConsumerStats.EMPTY;
        for (CompletableFuture<ScalableTopicController> future : controllers.values()) {
            // Collection never waits for controller initialization or a failed takeover.
            if (!future.isDone() || future.isCompletedExceptionally()) {
                continue;
            }
            ScalableTopicController controller = future.getNow(null);
            if (controller != null) {
                stats = stats.plus(controller.consumerStats());
            }
        }
        return stats;
    }

    // --- Controller management ---

    /**
     * Get or create a controller for a scalable topic. The controller will attempt
     * leader election; only the leader actively coordinates consumers. A controller that
     * closed itself, having found its topic deleted while taking over, is replaced. A takeover
     * in progress is waited for, bounded, so that a request arriving meanwhile doesn't fail.
     */
    public CompletableFuture<ScalableTopicController> getOrCreateController(TopicName topic) {
        String key = topic.toString();
        CompletableFuture<ScalableTopicController> stored = controllers.compute(key,
                (k, existing) -> existing == null || isClosed(existing) ? newController(key, topic) : existing);
        // Evict failed futures so subsequent callers can retry. This runs *outside*
        // compute, so modifying the map here is safe.
        return stored.exceptionally(ex -> {
            controllers.remove(key, stored);
            throw new RuntimeException("Failed to initialize controller for " + topic, ex);
        }).thenCompose(this::awaitTakeOver);
    }

    private CompletableFuture<ScalableTopicController> newController(String key, TopicName topic) {
        ScalableTopicController controller = new ScalableTopicController(
                topic, resources, brokerService, coordinationService);
        controller.setMetrics(metrics);
        if (consumerMetrics != null) {
            controller.setRebalanceListener(consumerMetrics::recordRebalance);
        }
        CompletableFuture<ScalableTopicController> future = new CompletableFuture<>();
        // Released once its topic is deleted: the admin delete stops the controller on the
        // broker serving it, not the ones other brokers hold for the topic. Closed before it is
        // removed, as in releaseController().
        controller.setTopicDeletedAction(() -> controller.close()
                .whenComplete((__, ___) -> controllers.remove(key, future)));
        controller.initialize().whenComplete((__, ex) -> {
            if (ex == null) {
                future.complete(controller);
            } else {
                // Release its leader election, and the lock if it was elected: the next
                // controller of the topic gets an election of its own once this one is closed.
                controller.close().exceptionally(closeEx -> {
                    log.warn().attr("topic", topic).exceptionMessage(closeEx)
                            .log("Failed to close a controller that failed to initialize");
                    return null;
                });
                future.completeExceptionally(ex);
            }
        });
        return future;
    }

    private static boolean isClosed(CompletableFuture<ScalableTopicController> controller) {
        return controller.isDone() && !controller.isCompletedExceptionally() && controller.join().isClosed();
    }

    private CompletableFuture<ScalableTopicController> awaitTakeOver(ScalableTopicController controller) {
        CompletableFuture<Void> takeover = controller.takeOverInProgress();
        if (takeover.isDone()) {
            return CompletableFuture.completedFuture(controller);
        }
        // Timed out, or the term ended without serving: the request goes on and finds that the
        // controller doesn't serve.
        return FutureUtil.addTimeoutHandling(takeover.thenApply(__ -> controller), TAKEOVER_WAIT,
                        brokerService.getPulsar().getExecutor(),
                        () -> FutureUtil.createTimeoutException("The controller of " + controller.getTopicName()
                                + " did not take over in time", ScalableTopicService.class, "awaitTakeOver"))
                .handle((__, ___) -> controller);
    }

    /**
     * Release the controller for a topic (e.g., on topic unload). It is closed before it is
     * removed, so that a controller created for the topic afterwards gets a leader election of
     * its own, rather than this one's.
     */
    public CompletableFuture<Void> releaseController(TopicName topic) {
        String key = topic.toString();
        CompletableFuture<ScalableTopicController> future = controllers.get(key);
        if (future != null) {
            return future.thenCompose(ScalableTopicController::close)
                    .whenComplete((__, ___) -> controllers.remove(key, future));
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
                brokerService.getPulsar().getConfiguration().getScalableTopicEntryBucketMaxPerSegment(),
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
        // requirement. The creation lock keeps it from interleaving with a migration of the
        // regular topic of the same name (see ScalableTopicResources#runWithCreationLock).
        return resources.runWithCreationLock(topic, __ -> resources.createScalableTopicAsync(topic, metadata))
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
     * Rebucket a segment (delegates to controller). Same leader contract as
     * {@link #splitSegment(TopicName, long)}.
     */
    public CompletableFuture<Void> rebucketSegment(TopicName topic, long segmentId, int bucketCount) {
        return getOrCreateController(topic)
                .thenCompose(controller -> controller.rebucketSegment(segmentId, bucketCount))
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
     * Get the stats of a scalable topic as a whole: the segment DAG with per-segment load,
     * the subscriptions with their backlog across segments, and the producers. Delegates to
     * the controller, which fans out to the segment-owning brokers; the REST layer routes
     * the request to the controller leader so the STREAM consumer sessions it holds are
     * part of the picture.
     */
    public CompletableFuture<ScalableTopicStats> getStats(TopicName topic) {
        return getOrCreateController(topic)
                .thenCompose(ScalableTopicController::getStats);
    }

    /**
     * Delete a scalable topic and all its segment topics.
     */
    public CompletableFuture<Void> deleteScalableTopic(TopicName topic) {
        return releaseController(topic)
                .thenCompose(__ -> resources.getScalableTopicMetadataAsync(topic))
                // Delete the metadata before the segment topics, like the admin delete: the segment delete
                // only deletes the durable transaction state of a segment no longer active in the layout.
                .thenCompose(optMd -> resources.deleteScalableTopicAsync(topic).thenApply(__ -> optMd))
                .thenCompose(optMd -> {
                    if (optMd.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    ScalableTopicMetadata metadata = optMd.get();
                    // Delete all underlying segment topics.
                    return FutureUtil.waitForAll(
                            metadata.getSegments().values().stream()
                                    .map(segment -> deleteUnderlyingSegmentTopic(topic, segment))
                                    .toList()
                    );
                })
                // The segments share the schema of the scalable topic; deleting them leaves it in place.
                .thenCompose(__ -> brokerService.deleteSchema(topic))
                .thenApply(__ -> null);
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

    /**
     * Explicit clean leave: forwards to the locally-held controller, which deletes the
     * persisted registration and rebalances the remaining consumers immediately. No-op when
     * no controller entry exists here — which happens only for a consumer that never
     * registered on this broker, a deleted topic, or a shutting-down service. (A deposed
     * leader keeps its entry and fails via {@code checkLeader()}, taking the error path
     * instead, which preserves the caller's registration ref and grace fallback.)
     */
    public CompletableFuture<Void> unregisterConsumer(TopicName topic, String subscription,
                                                      String consumerName, long consumerId) {
        CompletableFuture<ScalableTopicController> future = controllers.get(topic.toString());
        if (future == null) {
            return CompletableFuture.completedFuture(null);
        }
        return future.thenCompose(c -> c.unregisterConsumer(subscription, consumerName, consumerId));
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
        String segmentName = SegmentTopicName.backingTopicName(parentTopic, segment);
        try {
            PulsarAdmin admin = brokerService.getPulsar().getAdminClient();
            // The segment endpoints can't address the persistent:// topic a legacy segment wraps.
            CompletableFuture<Void> delete = segment.isLegacy()
                    ? admin.topics().deleteAsync(segmentName, true)
                    : admin.scalableTopics().deleteSegmentAsync(segmentName, true);
            return delete
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
