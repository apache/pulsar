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

import io.github.merlimat.slog.Logger;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentInfo;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.coordination.LeaderElection;
import org.apache.pulsar.metadata.api.coordination.LeaderElectionState;

/**
 * Per-topic coordinator that manages the segment layout and consumer assignments
 * for a single scalable topic.
 *
 * <p>Only one instance of this controller runs across the cluster for a given topic,
 * ensured by leader election via the metadata store. The leader stores its broker URL
 * so that clients can discover and connect to it.
 */
public class ScalableTopicController {

    private static final Logger LOG = Logger.get(ScalableTopicController.class);
    private final Logger log;

    @Getter
    private final TopicName topicName;
    private final ScalableTopicResources resources;
    private final BrokerService brokerService;
    private final LeaderElection<String> leaderElection;

    private volatile SegmentLayout currentLayout;

    /** Per-subscription consumer tracking. */
    private final ConcurrentHashMap<String, SubscriptionCoordinator> subscriptions = new ConcurrentHashMap<>();

    @Getter
    private volatile LeaderElectionState leaderState = LeaderElectionState.NoLeader;

    private volatile boolean closed = false;

    ScalableTopicController(TopicName topicName,
                            ScalableTopicResources resources,
                            BrokerService brokerService,
                            CoordinationService coordinationService) {
        this.topicName = topicName;
        this.resources = resources;
        this.brokerService = brokerService;
        this.log = LOG.with().attr("topic", topicName).build();
        this.leaderElection = coordinationService.getLeaderElection(
                String.class,
                resources.controllerLockPath(topicName),
                this::onLeaderStateChange);
    }

    /**
     * Reacts to leader election state transitions. On {@link LeaderElectionState#NoLeader}
     * we kick off another {@link #initialize()} so the cluster always converges toward
     * having a leader.
     */
    private void onLeaderStateChange(LeaderElectionState state) {
        log.info().attr("state", state).log("Leader state change for scalable topic");
        if (state == LeaderElectionState.NoLeader && !closed) {
            initialize().exceptionally(ex -> {
                log.warn().exceptionMessage(ex).log("Failed to re-elect after NoLeader");
                return null;
            });
        }
    }

    /**
     * Initialize: load current layout from metadata store and attempt to become leader.
     *
     * <p>On successful election, also loads all persisted subscriptions and consumer
     * registrations from the metadata store. Each restored consumer is installed in a
     * "just disconnected" state with a fresh grace-period timer, so consumers that were
     * registered under a previous leader will have the full grace window to reconnect to
     * this new leader without losing their segment assignment.
     */
    public CompletableFuture<Void> initialize() {
        return resources.getScalableTopicMetadataAsync(topicName, true)
                .thenCompose(optMd -> {
                    if (optMd.isEmpty()) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("Scalable topic not found: " + topicName));
                    }
                    this.currentLayout = SegmentLayout.fromMetadata(optMd.get());
                    return electLeader();
                })
                .thenCompose(__ -> {
                    if (isLeader()) {
                        return restoreSessionsFromStore();
                    }
                    return CompletableFuture.completedFuture(null);
                });
    }

    /**
     * Load persisted subscriptions and consumer registrations from the metadata store and
     * install them into per-subscription {@link SubscriptionCoordinator} instances. Called
     * on successful leader election so the newly-elected leader can resume servicing
     * consumers that were registered under a previous leader.
     */
    private CompletableFuture<Void> restoreSessionsFromStore() {
        return resources.listSubscriptionsAsync(topicName)
                .thenCompose(subNames -> {
                    if (subNames.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    CompletableFuture<?>[] futures = subNames.stream()
                            .map(this::restoreSubscription)
                            .toArray(CompletableFuture[]::new);
                    return CompletableFuture.allOf(futures);
                });
    }

    private CompletableFuture<Void> restoreSubscription(String subscription) {
        return resources.listConsumersAsync(topicName, subscription)
                .thenAccept(consumerNames -> {
                    SubscriptionCoordinator coordinator = subscriptions.computeIfAbsent(
                            subscription, this::createCoordinator);
                    coordinator.restoreConsumers(consumerNames);
                    log.info().attr("subscription", subscription)
                            .attr("consumerCount", consumerNames.size())
                            .log("Restored subscription");
                });
    }

    private SubscriptionCoordinator createCoordinator(String subscription) {
        return new SubscriptionCoordinator(
                subscription,
                topicName,
                currentLayout,
                resources,
                brokerService.getPulsar().getExecutor());
    }

    private CompletableFuture<Void> electLeader() {
        // Store the brokerId as the leader-election value — not the raw pulsar:// URL.
        // Callers that need a service URL (DagWatchSession for clients, the REST layer for
        // HTTP redirection) look up the broker's advertised addresses via
        // NamespaceService.createLookupResult(brokerId, ...), matching the pattern used by
        // the cluster-leader redirection in NamespacesBase.
        String brokerId = brokerService.getPulsar().getBrokerId();
        return leaderElection.elect(brokerId)
                .thenAccept(state -> {
                    this.leaderState = state;
                    log.info().attr("state", state)
                            .log("Leader election for scalable topic");
                });
    }

    public boolean isLeader() {
        return leaderState == LeaderElectionState.Leading;
    }

    /**
     * Get the current leader's brokerId (as stored in leader election). Callers resolve
     * it to a service URL via
     * {@link org.apache.pulsar.broker.namespace.NamespaceService#createLookupResult(String,
     * boolean, String)}.
     */
    public CompletableFuture<Optional<String>> getLeaderBrokerId() {
        return leaderElection.getLeaderValue();
    }

    // --- Layout operations (only valid on leader) ---

    public CompletableFuture<SegmentLayout> getLayout() {
        return CompletableFuture.completedFuture(currentLayout);
    }

    /**
     * Split an active segment at its midpoint.
     *
     * <p>Critical ordering: child segment topics and their subscription cursors are created
     * BEFORE the metadata update. This ensures that when producers discover the new segments
     * (via DAG watch) and start writing, all subscription cursors already exist. Without this,
     * messages published before a consumer subscribes would be missed.
     */
    public CompletableFuture<SegmentLayout> splitSegment(long segmentId) {
        checkLeader();

        // Compute the new layout locally to derive child segment info
        SegmentLayout newLayout = currentLayout.splitSegment(segmentId);
        SegmentInfo child1 = newLayout.getAllSegments().get(newLayout.getNextSegmentId() - 2);
        SegmentInfo child2 = newLayout.getAllSegments().get(newLayout.getNextSegmentId() - 1);
        SegmentInfo parent = currentLayout.getAllSegments().get(segmentId);
        String parentTopicName = toSegmentPersistentName(parent);

        // Step 1: Discover subscriptions on the parent segment, then create child
        // segment topics with those subscriptions (routed to owning brokers via admin API)
        return discoverSubscriptions(parentTopicName)
          .thenCompose(parentSubs -> {
              var subList = new java.util.ArrayList<>(parentSubs);
              return createSegmentTopic(child1, subList)
                      .thenCompose(__ -> createSegmentTopic(child2, subList));
          })

          // Step 3: Terminate the parent segment topic so producers get TopicTerminated
          .thenCompose(__ -> terminateSegmentTopic(parentTopicName))

          // Step 4: Atomic metadata update (only after topics + cursors are ready + parent terminated)
          .thenCompose(__ -> resources.updateScalableTopicAsync(topicName, md -> {
              SegmentLayout latest = SegmentLayout.fromMetadata(md);
              SegmentLayout updated = latest.splitSegment(segmentId);
              return updated.toMetadata(md.getProperties());
          }))
          .thenCompose(__ -> resources.getScalableTopicMetadataAsync(topicName, true))
          .thenCompose(optMd -> {
              currentLayout = SegmentLayout.fromMetadata(optMd.orElseThrow());

              // Step 5: Notify subscriptions of layout change (triggers consumer reassignment)
              return notifySubscriptions(currentLayout);
          }).thenApply(__ -> currentLayout);
    }

    /**
     * Merge two adjacent active segments.
     *
     * <p>Same ordering invariant as split: merged segment topic and subscription cursors
     * are created before the metadata update.
     */
    public CompletableFuture<SegmentLayout> mergeSegments(long segmentId1, long segmentId2) {
        checkLeader();

        // Compute the new layout locally to derive merged segment info
        SegmentLayout newLayout = currentLayout.mergeSegments(segmentId1, segmentId2);
        SegmentInfo merged = newLayout.getAllSegments().get(newLayout.getNextSegmentId() - 1);
        SegmentInfo parent1 = currentLayout.getAllSegments().get(segmentId1);
        SegmentInfo parent2 = currentLayout.getAllSegments().get(segmentId2);
        String parent1Topic = toSegmentPersistentName(parent1);
        String parent2Topic = toSegmentPersistentName(parent2);

        // Step 1: Discover subscriptions from both parents (union), then create merged segment
        return discoverSubscriptions(parent1Topic)
          .thenCombine(discoverSubscriptions(parent2Topic), (subs1, subs2) -> {
              Set<String> allSubs = new LinkedHashSet<>(subs1);
              allSubs.addAll(subs2);
              return allSubs;
          })
          .thenCompose(parentSubs -> createSegmentTopic(merged, new java.util.ArrayList<>(parentSubs)))

          // Step 2: Terminate both parent segment topics
          .thenCompose(__ -> terminateSegmentTopic(parent1Topic))
          .thenCompose(__ -> terminateSegmentTopic(parent2Topic))

          // Step 3: Atomic metadata update (only after topic + cursors are ready + parents terminated)
          .thenCompose(__ -> resources.updateScalableTopicAsync(topicName, md -> {
              SegmentLayout latest = SegmentLayout.fromMetadata(md);
              SegmentLayout updated = latest.mergeSegments(segmentId1, segmentId2);
              return updated.toMetadata(md.getProperties());
          }))
          .thenCompose(__ -> resources.getScalableTopicMetadataAsync(topicName, true))
          .thenCompose(optMd -> {
              currentLayout = SegmentLayout.fromMetadata(optMd.orElseThrow());
              return notifySubscriptions(currentLayout);
          }).thenApply(__ -> currentLayout);
    }

    // --- Consumer management ---

    /**
     * Register a consumer for a subscription. The controller persists a durable session
     * entry and returns the consumer's segment assignment.
     *
     * <p>If a session with the same {@code consumerName} already exists (for example
     * because the consumer is reconnecting within the grace period), the existing
     * assignment is reused and no rebalance occurs.
     */
    public CompletableFuture<ConsumerAssignment> registerConsumer(String subscription,
                                                                   String consumerName,
                                                                   long consumerId,
                                                                   TransportCnx cnx) {
        checkLeader();
        SubscriptionCoordinator coordinator = subscriptions.computeIfAbsent(
                subscription, this::createCoordinator);
        return coordinator.registerConsumer(consumerName, consumerId, cnx)
                .thenApply(assignments -> {
                    // Look up by name since the key may have been an existing session
                    return assignments.entrySet().stream()
                            .filter(e -> consumerName.equals(e.getKey().getConsumerName()))
                            .map(Map.Entry::getValue)
                            .findFirst()
                            .orElse(null);
                });
    }

    /**
     * Explicit unregister: the consumer is leaving the subscription for good. Deletes the
     * persisted session entry and rebalances remaining consumers.
     */
    public CompletableFuture<Void> unregisterConsumer(String subscription, String consumerName) {
        checkLeader();
        SubscriptionCoordinator coordinator = subscriptions.get(subscription);
        if (coordinator == null) {
            return CompletableFuture.completedFuture(null);
        }
        return coordinator.unregisterConsumer(consumerName)
                .thenAccept(__ -> {
                    if (coordinator.getConsumers().isEmpty()) {
                        subscriptions.remove(subscription);
                    }
                });
    }

    /**
     * Called when a consumer's transport connection drops. Does <em>not</em> delete the
     * persisted session — the coordinator marks the consumer disconnected and starts the
     * grace-period timer. The consumer can reconnect within the grace period and resume
     * with the same segment assignment.
     */
    public void onConsumerDisconnect(String subscription, String consumerName) {
        SubscriptionCoordinator coordinator = subscriptions.get(subscription);
        if (coordinator != null) {
            coordinator.onConsumerDisconnect(consumerName);
        }
    }

    // --- Subscription management ---

    /**
     * Create a subscription on the scalable topic. Persists the {@code SubscriptionMetadata}
     * entry and then propagates the subscription to every active segment topic, creating a
     * cursor at the earliest position on each so that no messages are lost.
     *
     * <p>Idempotent: re-creating an existing subscription succeeds and is a no-op on the
     * metadata store; per-segment cursor creation tolerates already-existing subscriptions.
     */
    public CompletableFuture<Void> createSubscription(String subscription,
            org.apache.pulsar.broker.resources.SubscriptionType type) {
        checkLeader();
        return resources.createSubscriptionAsync(topicName, subscription, type)
                .exceptionally(ex -> {
                    Throwable cause = org.apache.pulsar.common.util.FutureUtil.unwrapCompletionException(ex);
                    if (cause instanceof org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyExistsException) {
                        return null;
                    }
                    throw org.apache.pulsar.common.util.FutureUtil.wrapToCompletionException(cause);
                })
                .thenCompose(__ -> createSubscriptionOnActiveSegments(subscription));
    }

    /**
     * Delete a subscription from the scalable topic. Unregisters any in-memory consumers on
     * this leader, deletes the persisted {@code SubscriptionMetadata} (and all its consumer
     * registration children), and removes the subscription from every segment topic.
     */
    public CompletableFuture<Void> deleteSubscription(String subscription) {
        checkLeader();
        // Remove in-memory coordinator first so no new consumers attach during teardown.
        SubscriptionCoordinator coordinator = subscriptions.remove(subscription);
        CompletableFuture<Void> coordinatorClosed =
                coordinator == null
                        ? CompletableFuture.completedFuture(null)
                        : dropAllConsumers(coordinator);
        return coordinatorClosed
                .thenCompose(__ -> resources.deleteSubscriptionAsync(topicName, subscription))
                .thenCompose(__ -> deleteSubscriptionOnAllSegments(subscription));
    }

    private CompletableFuture<Void> dropAllConsumers(SubscriptionCoordinator coordinator) {
        CompletableFuture<?>[] futures = coordinator.getConsumers().stream()
                .map(session -> coordinator.unregisterConsumer(session.getConsumerName()))
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures);
    }

    private CompletableFuture<Void> createSubscriptionOnActiveSegments(String subscription) {
        CompletableFuture<?>[] futures = currentLayout.getActiveSegments().values().stream()
                .map(segment -> createSubscriptionOnSegment(segment, subscription))
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures);
    }

    private CompletableFuture<Void> deleteSubscriptionOnAllSegments(String subscription) {
        // Delete from every segment in the DAG, including sealed ones, so catch-up readers
        // aren't left with orphaned cursors.
        CompletableFuture<?>[] futures = currentLayout.getAllSegments().values().stream()
                .map(segment -> deleteSubscriptionOnSegment(segment, subscription))
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures);
    }

    private CompletableFuture<Void> createSubscriptionOnSegment(SegmentInfo segment, String subscription) {
        String persistentName = toSegmentUnderlyingPersistentName(segment);
        try {
            return brokerService.getPulsar().getAdminClient()
                    .topics().createSubscriptionAsync(persistentName, subscription,
                            org.apache.pulsar.client.api.MessageId.earliest)
                    .exceptionally(ex -> {
                        Throwable cause = org.apache.pulsar.common.util.FutureUtil.unwrapCompletionException(ex);
                        if (cause instanceof org.apache.pulsar.client.admin.PulsarAdminException.ConflictException) {
                            // Subscription already exists on this segment — treat as success.
                            return null;
                        }
                        throw org.apache.pulsar.common.util.FutureUtil.wrapToCompletionException(cause);
                    });
        } catch (PulsarServerException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<Void> deleteSubscriptionOnSegment(SegmentInfo segment, String subscription) {
        String persistentName = toSegmentUnderlyingPersistentName(segment);
        try {
            return brokerService.getPulsar().getAdminClient()
                    .topics().deleteSubscriptionAsync(persistentName, subscription, true)
                    .exceptionally(ex -> {
                        Throwable cause = org.apache.pulsar.common.util.FutureUtil.unwrapCompletionException(ex);
                        if (cause instanceof org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException) {
                            return null;
                        }
                        log.warn().attr("subscription", subscription)
                                .attr("segment", persistentName).exceptionMessage(cause)
                                .log("Failed to delete subscription from segment");
                        return null;
                    });
        } catch (PulsarServerException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    // --- Stats ---

    /**
     * Build an aggregated snapshot of the scalable topic's state: segment counts, per-segment
     * layout info, and per-subscription consumer counts (loaded from the persisted
     * registrations so the numbers are consistent across controller leader failovers).
     */
    public CompletableFuture<org.apache.pulsar.common.policies.data.ScalableTopicStats> getStats() {
        SegmentLayout layout = this.currentLayout;
        var statsBuilder = org.apache.pulsar.common.policies.data.ScalableTopicStats.builder()
                .epoch(layout.getEpoch());

        Map<Long, org.apache.pulsar.common.policies.data.ScalableTopicStats.SegmentStats> segmentStats =
                new java.util.LinkedHashMap<>();
        int active = 0;
        int sealed = 0;
        for (SegmentInfo segment : layout.getAllSegments().values()) {
            boolean isActive = segment.state() == org.apache.pulsar.common.scalable.SegmentState.ACTIVE;
            if (isActive) {
                active++;
            } else {
                sealed++;
            }
            String segmentName = SegmentTopicName.fromParent(
                    topicName, segment.hashRange(), segment.segmentId()).toString();
            segmentStats.put(segment.segmentId(),
                    new org.apache.pulsar.common.policies.data.ScalableTopicStats.SegmentStats(
                            segmentName, segment.state().name()));
        }
        statsBuilder
                .totalSegments(layout.getAllSegments().size())
                .activeSegments(active)
                .sealedSegments(sealed)
                .segments(segmentStats);

        // Load persisted subscription + consumer counts. This gives a consistent picture
        // regardless of which broker currently holds the controller leadership.
        return resources.listSubscriptionsAsync(topicName)
                .thenCompose(subNames -> {
                    if (subNames.isEmpty()) {
                        return CompletableFuture.completedFuture(statsBuilder.build());
                    }
                    Map<String, org.apache.pulsar.common.policies.data.ScalableTopicStats.SubscriptionStats>
                            subStats = new java.util.LinkedHashMap<>();
                    CompletableFuture<?>[] futures = subNames.stream()
                            .map(subName -> resources.listConsumersAsync(topicName, subName)
                                    .thenAccept(consumerNames -> subStats.put(subName,
                                            new org.apache.pulsar.common.policies.data.ScalableTopicStats
                                                    .SubscriptionStats(consumerNames.size()))))
                            .toArray(CompletableFuture[]::new);
                    return CompletableFuture.allOf(futures)
                            .thenApply(__ -> {
                                statsBuilder.subscriptions(subStats);
                                return statsBuilder.build();
                            });
                });
    }

    // --- Lifecycle ---

    public CompletableFuture<Void> close() {
        closed = true;
        subscriptions.clear();
        return leaderElection.asyncClose();
    }

    // --- Internal helpers ---

    private void checkLeader() {
        if (!isLeader()) {
            throw new IllegalStateException("This broker is not the leader for topic: " + topicName);
        }
    }

    private String toSegmentPersistentName(SegmentInfo segment) {
        TopicName segmentTopicName = SegmentTopicName.fromParent(
                topicName, segment.hashRange(), segment.segmentId());
        return segmentTopicName.toString();
    }

    /**
     * Return the {@code persistent://} form of a segment's underlying managed-ledger topic,
     * suitable for the standard {@link org.apache.pulsar.client.admin.Topics} admin API.
     * The segment-owning broker is discovered by the admin client's normal bundle routing.
     */
    private String toSegmentUnderlyingPersistentName(SegmentInfo segment) {
        TopicName segmentTopicName = SegmentTopicName.fromParent(
                topicName, segment.hashRange(), segment.segmentId());
        return "persistent://" + segmentTopicName.getTenant() + "/"
                + segmentTopicName.getNamespacePortion() + "/"
                + segmentTopicName.getLocalName();
    }

    private CompletableFuture<Void> terminateSegmentTopic(String segmentTopicName) {
        try {
            return brokerService.getPulsar().getAdminClient()
                    .scalableTopics().terminateSegmentAsync(segmentTopicName)
                    .thenRun(() -> log.info().attr("segment", segmentTopicName)
                            .log("Terminated segment topic"));
        } catch (PulsarServerException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<Void> createSegmentTopic(SegmentInfo segment, java.util.List<String> subscriptions) {
        String segmentName = toSegmentPersistentName(segment);
        try {
            return brokerService.getPulsar().getAdminClient()
                    .scalableTopics().createSegmentAsync(segmentName, subscriptions)
                    .thenRun(() -> log.info().attr("segment", segmentName)
                            .log("Created segment topic"));
        } catch (PulsarServerException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Discover all subscription names on a segment topic. Works whether the topic is
     * on this broker or a remote one by using the admin client.
     */
    private CompletableFuture<Set<String>> discoverSubscriptions(String segmentTopicName) {
        // Try local first (avoids RPC if the segment is on this broker)
        return brokerService.getTopicIfExists(segmentTopicName)
                .thenCompose(optTopic -> {
                    if (optTopic.isPresent()) {
                        return CompletableFuture.completedFuture(
                                new LinkedHashSet<>(optTopic.get().getSubscriptions().keySet()));
                    }
                    // Topic is on a remote broker — use admin client
                    try {
                        return brokerService.getPulsar().getAdminClient()
                                .topics().getSubscriptionsAsync(segmentTopicName)
                                .thenApply(LinkedHashSet::new);
                    } catch (PulsarServerException e) {
                        return CompletableFuture.failedFuture(e);
                    }
                });
    }

    private CompletableFuture<Void> notifySubscriptions(SegmentLayout layout) {
        CompletableFuture<?>[] futures = subscriptions.values().stream()
                .map(coordinator -> coordinator.onLayoutChange(layout))
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures);
    }

    /**
     * Create initial segment layout for a new scalable topic.
     */
    public static ScalableTopicMetadata createInitialMetadata(int numInitialSegments,
                                                        Map<String, String> properties) {
        if (numInitialSegments < 1) {
            throw new IllegalArgumentException("Must have at least 1 segment");
        }

        int rangeSize = (HashRange.MAX_HASH + 1) / numInitialSegments;
        Map<Long, SegmentInfo> segments = new LinkedHashMap<>();

        for (int i = 0; i < numInitialSegments; i++) {
            int start = i * rangeSize;
            int end = (i == numInitialSegments - 1) ? HashRange.MAX_HASH : (start + rangeSize - 1);
            HashRange range = HashRange.of(start, end);
            SegmentInfo segment = SegmentInfo.active(i, range, 0);
            segments.put((long) i, segment);
        }

        return ScalableTopicMetadata.builder()
                .epoch(0)
                .nextSegmentId(numInitialSegments)
                .segments(segments)
                .properties(properties != null ? properties : Map.of())
                .build();
    }
}
