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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Getter;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.SegmentInfo;
import org.apache.pulsar.common.scalable.SegmentTopicName;

/**
 * Manages segment-to-consumer assignments within a single subscription of a scalable topic.
 *
 * <p>Consumer sessions are persisted in the metadata store (as
 * {@link org.apache.pulsar.broker.resources.ConsumerRegistration}) and tracked in-memory
 * as {@link ConsumerSession} objects. The distinction is important: the session <em>itself</em>
 * is durable (survives TCP disconnects, client restarts, and controller leader failovers),
 * but the keep-alive tracking (connected / grace-period timer) is in-memory only.
 *
 * <p>When a consumer's connection drops, the coordinator does <em>not</em> immediately evict
 * it. Instead it marks the session disconnected and starts a grace-period timer. If the
 * consumer reconnects (with the same {@code consumerName}) before the timer fires, its
 * existing assignment is restored with no rebalance. If the timer fires, the persisted
 * registration is deleted and a rebalance is triggered.
 *
 * <p>On controller leader failover, the new leader reloads persisted registrations via
 * {@link #restoreConsumers(Collection)}, which installs them in a "just disconnected" state
 * with fresh grace-period timers — giving every consumer the full window to reconnect to the
 * new leader regardless of how long they had been disconnected under the old one.
 */
public class SubscriptionCoordinator {

    private static final Logger LOG = Logger.get(SubscriptionCoordinator.class);
    private final Logger log;

    // TODO: make configurable via broker config (e.g. scalableTopicConsumerSessionTimeoutSeconds)
    private static final Duration DEFAULT_GRACE_PERIOD = Duration.ofSeconds(60);

    @Getter
    private final String subscriptionName;
    private final TopicName topicName;
    private final ScalableTopicResources resources;
    private final ScheduledExecutorService scheduler;
    private final Duration gracePeriod;

    /** Keyed by consumerName — the stable session identity. */
    private final Map<String, ConsumerSession> sessions = new ConcurrentHashMap<>();
    private Map<Long, ConsumerSession> segmentAssignments = new LinkedHashMap<>();
    private SegmentLayout currentLayout;

    public SubscriptionCoordinator(String subscriptionName,
                                   TopicName topicName,
                                   SegmentLayout initialLayout,
                                   ScalableTopicResources resources,
                                   ScheduledExecutorService scheduler) {
        this(subscriptionName, topicName, initialLayout, resources, scheduler, DEFAULT_GRACE_PERIOD);
    }

    public SubscriptionCoordinator(String subscriptionName,
                                   TopicName topicName,
                                   SegmentLayout initialLayout,
                                   ScalableTopicResources resources,
                                   ScheduledExecutorService scheduler,
                                   Duration gracePeriod) {
        this.subscriptionName = subscriptionName;
        this.topicName = topicName;
        this.currentLayout = initialLayout;
        this.resources = resources;
        this.scheduler = scheduler;
        this.gracePeriod = gracePeriod;
        this.log = LOG.with().attr("topic", topicName).attr("subscription", subscriptionName).build();
    }

    // --- Register / unregister / reconnect ---

    /**
     * Register a consumer — either a fresh registration or a reconnect of an existing
     * session. If the {@code consumerName} already has a persisted session, its assignment
     * is preserved and the new connection is attached; otherwise the registration is
     * persisted and a rebalance is triggered.
     *
     * @return assignment map for all consumers (unchanged on reconnect, recomputed on fresh register)
     */
    public synchronized CompletableFuture<Map<ConsumerSession, ConsumerAssignment>> registerConsumer(
            String consumerName, long consumerId, TransportCnx cnx) {
        ConsumerSession existing = sessions.get(consumerName);
        if (existing != null) {
            // Reconnect: attach the new connection, cancel any grace timer, and push the
            // current assignment without rebalancing other consumers.
            existing.attach(consumerId, cnx);
            Map<ConsumerSession, ConsumerAssignment> current =
                    computeAssignment(currentLayout, sessions.values());
            ConsumerAssignment assignment = current.get(existing);
            if (assignment != null) {
                existing.sendAssignmentUpdate(assignment);
            }
            return CompletableFuture.completedFuture(current);
        }

        // Fresh registration — persist first, then install in-memory and rebalance.
        ConsumerSession session = newSession(consumerName, consumerId, cnx);
        return resources.registerConsumerAsync(topicName, subscriptionName, consumerName)
                .thenApply(__ -> {
                    synchronized (this) {
                        sessions.put(consumerName, session);
                        return rebalanceAndNotify();
                    }
                });
    }

    /**
     * Explicit unregister (consumer asked to leave the subscription). Cancels any pending
     * grace timer, deletes the persisted registration, and rebalances.
     */
    public synchronized CompletableFuture<Map<ConsumerSession, ConsumerAssignment>> unregisterConsumer(
            String consumerName) {
        ConsumerSession removed = sessions.remove(consumerName);
        if (removed == null) {
            return CompletableFuture.completedFuture(snapshotAssignments());
        }
        removed.cancelGraceTimer();
        return resources.unregisterConsumerAsync(topicName, subscriptionName, consumerName)
                .thenApply(__ -> {
                    synchronized (this) {
                        if (sessions.isEmpty()) {
                            segmentAssignments.clear();
                            return Map.of();
                        }
                        return rebalanceAndNotify();
                    }
                });
    }

    /**
     * Called when a consumer's transport connection drops (not an explicit unregister).
     * Marks the session disconnected and schedules an eviction task after the grace period.
     * If the consumer reconnects with the same name before the timer fires, the timer is
     * cancelled and no rebalance happens.
     */
    public synchronized void onConsumerDisconnect(String consumerName) {
        ConsumerSession session = sessions.get(consumerName);
        if (session == null || !session.isConnected()) {
            return;
        }
        session.markDisconnected();
    }

    /**
     * Restore consumer sessions loaded from the metadata store on controller leader failover.
     * All restored sessions start in the "just disconnected" state with a fresh grace-period
     * timer — consumers reconnecting within that window resume with the same assignment.
     */
    public synchronized Map<ConsumerSession, ConsumerAssignment> restoreConsumers(
            Collection<String> persistedConsumerNames) {
        for (String name : persistedConsumerNames) {
            if (sessions.containsKey(name)) {
                continue;
            }
            // restored() arms the grace timer internally. The eviction callback takes the
            // coordinator's monitor, which we hold here, so ordering against the upcoming
            // sessions.put is guaranteed.
            ConsumerSession session = ConsumerSession.restored(name, gracePeriod, scheduler,
                    () -> evictExpiredConsumer(name), log);
            sessions.put(name, session);
        }
        // Compute the deterministic assignment against the current layout. No sends: the
        // consumers aren't connected yet. They will receive their assignment on reconnect.
        Map<ConsumerSession, ConsumerAssignment> result =
                computeAssignment(currentLayout, sessions.values());
        updateSegmentAssignmentIndex(result);
        return result;
    }

    /**
     * Handle a layout change (split/merge). Recompute and push assignments to connected
     * consumers.
     */
    public synchronized CompletableFuture<Map<ConsumerSession, ConsumerAssignment>> onLayoutChange(
            SegmentLayout newLayout) {
        this.currentLayout = newLayout;
        if (sessions.isEmpty()) {
            segmentAssignments.clear();
            return CompletableFuture.completedFuture(Map.of());
        }
        return CompletableFuture.completedFuture(rebalanceAndNotify());
    }

    // --- Accessors ---

    public synchronized Set<ConsumerSession> getConsumers() {
        return Set.copyOf(sessions.values());
    }

    // --- Internals ---

    /**
     * Build a new {@link ConsumerSession} wired with this coordinator's grace period,
     * scheduler, logger context, and eviction callback.
     */
    private ConsumerSession newSession(String consumerName, long consumerId, TransportCnx cnx) {
        return new ConsumerSession(consumerName, consumerId, cnx, gracePeriod, scheduler,
                () -> evictExpiredConsumer(consumerName), log);
    }

    /**
     * Evict a consumer whose grace-period timer has fired. Runs on the scheduler thread.
     */
    private void evictExpiredConsumer(String consumerName) {
        synchronized (this) {
            ConsumerSession session = sessions.get(consumerName);
            if (session == null) {
                return;
            }
            if (session.isConnected()) {
                // Raced with a reconnect — abort the eviction.
                return;
            }
            sessions.remove(consumerName);
            log.info().attr("consumer", consumerName)
                    .log("Consumer evicted after grace period");
        }
        // Delete persisted registration outside the lock (async) and then rebalance.
        resources.unregisterConsumerAsync(topicName, subscriptionName, consumerName)
                .exceptionally(ex -> {
                    log.warn().attr("consumer", consumerName).exception(ex)
                            .log("Failed to delete persisted registration");
                    return null;
                })
                .thenRun(() -> {
                    synchronized (this) {
                        if (!sessions.isEmpty()) {
                            rebalanceAndNotify();
                        } else {
                            segmentAssignments.clear();
                        }
                    }
                });
    }

    /**
     * Compute a balanced assignment of active segments to consumers.
     *
     * <p>Strategy: sort segments by hash range start, sort consumers by name, then
     * round-robin. Deterministic: the same inputs always produce the same output, so a new
     * leader recomputing assignments after failover gets the same result as the old leader.
     */
    Map<ConsumerSession, ConsumerAssignment> computeAssignment(
            SegmentLayout layout, Collection<ConsumerSession> consumers) {

        if (consumers.isEmpty()) {
            return Map.of();
        }

        List<SegmentInfo> sortedSegments = layout.getActiveSegments().values().stream()
                .sorted(Comparator.comparing(SegmentInfo::hashRange))
                .toList();

        List<ConsumerSession> sortedConsumers = consumers.stream()
                .sorted(Comparator.comparing(ConsumerSession::getConsumerName))
                .toList();

        Map<ConsumerSession, List<ConsumerAssignment.AssignedSegment>> assignmentLists =
                new LinkedHashMap<>();
        for (ConsumerSession consumer : sortedConsumers) {
            assignmentLists.put(consumer, new ArrayList<>());
        }

        int consumerIndex = 0;
        for (SegmentInfo segment : sortedSegments) {
            ConsumerSession consumer = sortedConsumers.get(consumerIndex % sortedConsumers.size());
            TopicName segmentTopic = SegmentTopicName.fromParent(topicName, segment.hashRange(),
                    segment.segmentId());
            assignmentLists.get(consumer).add(new ConsumerAssignment.AssignedSegment(
                    segment.segmentId(),
                    segment.hashRange(),
                    segmentTopic.toString()
            ));
            consumerIndex++;
        }

        Map<ConsumerSession, ConsumerAssignment> result = new LinkedHashMap<>();
        for (var entry : assignmentLists.entrySet()) {
            result.put(entry.getKey(), new ConsumerAssignment(
                    layout.getEpoch(), entry.getValue()));
        }
        return result;
    }

    private Map<ConsumerSession, ConsumerAssignment> rebalanceAndNotify() {
        Map<ConsumerSession, ConsumerAssignment> assignments =
                computeAssignment(currentLayout, sessions.values());
        updateSegmentAssignmentIndex(assignments);

        for (var entry : assignments.entrySet()) {
            entry.getKey().sendAssignmentUpdate(entry.getValue());
        }

        return assignments;
    }

    private void updateSegmentAssignmentIndex(Map<ConsumerSession, ConsumerAssignment> assignments) {
        segmentAssignments.clear();
        for (var entry : assignments.entrySet()) {
            for (ConsumerAssignment.AssignedSegment seg : entry.getValue().assignedSegments()) {
                segmentAssignments.put(seg.segmentId(), entry.getKey());
            }
        }
    }

    private Map<ConsumerSession, ConsumerAssignment> snapshotAssignments() {
        return computeAssignment(currentLayout, sessions.values());
    }
}
