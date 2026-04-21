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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentInfo;
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
@Slf4j
public class ScalableTopicController {

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

    ScalableTopicController(TopicName topicName,
                            ScalableTopicResources resources,
                            BrokerService brokerService,
                            LeaderElection<String> leaderElection) {
        this.topicName = topicName;
        this.resources = resources;
        this.brokerService = brokerService;
        this.leaderElection = leaderElection;
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
                    log.info("[{}] restored subscription {} with {} consumer(s)",
                            topicName, subscription, consumerNames.size());
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
                    log.info("Leader election for scalable topic {}: state={}", topicName, state);
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

    // --- Lifecycle ---

    public CompletableFuture<Void> close() {
        subscriptions.clear();
        return leaderElection.asyncClose();
    }

    // --- Internal helpers ---

    private void checkLeader() {
        if (!isLeader()) {
            throw new IllegalStateException("This broker is not the leader for topic: " + topicName);
        }
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
