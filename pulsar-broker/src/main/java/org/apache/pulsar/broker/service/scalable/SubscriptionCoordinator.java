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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.SegmentInfo;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.apache.pulsar.common.util.Backoff;

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

    /**
     * First drain-status check fires this long after a sealed segment shows up — fast
     * enough that a freshly subscribed EARLIEST consumer doesn't wait for backlog
     * unblocking on a small topic.
     */
    static final Duration DEFAULT_DRAIN_INITIAL_DELAY = Duration.ofSeconds(2);

    /**
     * Upper bound on the drain-poll interval. Reached by exponential backoff while a
     * sealed segment remains undrained — long-tail backlog (consumers stalled for hours)
     * shouldn't have us hammering the segment owner every couple seconds. Reset on every
     * progress event (drain detected / layout changed / new consumer).
     */
    static final Duration DEFAULT_DRAIN_MAX_DELAY = Duration.ofMinutes(15);

    @Getter
    private final String subscriptionName;
    private final TopicName topicName;
    private final ScalableTopicResources resources;
    private final ScheduledExecutorService scheduler;
    private final Duration gracePeriod;
    private final Duration drainInitialDelay;
    private final Duration drainMaxDelay;

    /** Keyed by consumerName — the stable session identity. */
    private final Map<String, ConsumerSession> sessions = new ConcurrentHashMap<>();
    private Map<Long, ConsumerSession> segmentAssignments = new LinkedHashMap<>();
    private SegmentLayout currentLayout;

    /**
     * Sealed segments confirmed drained for this subscription (cursor at end). In-memory
     * only — on controller-leader failover the new leader rediscovers drain status by
     * polling. {@link #computeAssignment} consults this set to decide which active
     * children of a split / merge are eligible to be assigned (children stay blocked
     * until <em>every</em> sealed parent is drained, so message order isn't broken).
     */
    private final Set<Long> drainedSegmentIds = ConcurrentHashMap.newKeySet();

    /**
     * Backoff governing the drain-poll cadence. Starts at {@link #drainInitialDelay},
     * grows exponentially up to {@link #drainMaxDelay}, and is reset on every progress
     * event (a segment is observed drained, the layout changes, a new consumer joins).
     */
    private final Backoff drainBackoff;
    private ScheduledFuture<?> drainPollTask;
    private boolean drainPollInProgress;
    private boolean closed;
    /**
     * Drain checker installed on the coordinator. Mutable: starts null on the
     * controller-leader-failover restore path (consumer type unknown until reconnect),
     * upgraded to a real checker on first STREAM register via {@link #installDrainChecker}.
     * Once non-null it stays non-null (we don't downgrade to no-ordering mid-flight).
     */
    private SegmentDrainChecker drainChecker;

    public SubscriptionCoordinator(String subscriptionName,
                                   TopicName topicName,
                                   SegmentLayout initialLayout,
                                   ScalableTopicResources resources,
                                   ScheduledExecutorService scheduler) {
        this(subscriptionName, topicName, initialLayout, resources, scheduler,
                DEFAULT_GRACE_PERIOD, null, DEFAULT_DRAIN_INITIAL_DELAY, DEFAULT_DRAIN_MAX_DELAY);
    }

    public SubscriptionCoordinator(String subscriptionName,
                                   TopicName topicName,
                                   SegmentLayout initialLayout,
                                   ScalableTopicResources resources,
                                   ScheduledExecutorService scheduler,
                                   Duration gracePeriod) {
        this(subscriptionName, topicName, initialLayout, resources, scheduler, gracePeriod,
                null, DEFAULT_DRAIN_INITIAL_DELAY, DEFAULT_DRAIN_MAX_DELAY);
    }

    public SubscriptionCoordinator(String subscriptionName,
                                   TopicName topicName,
                                   SegmentLayout initialLayout,
                                   ScalableTopicResources resources,
                                   ScheduledExecutorService scheduler,
                                   Duration gracePeriod,
                                   SegmentDrainChecker drainChecker,
                                   Duration drainInitialDelay,
                                   Duration drainMaxDelay) {
        this.subscriptionName = subscriptionName;
        this.topicName = topicName;
        this.currentLayout = initialLayout;
        this.resources = resources;
        this.scheduler = scheduler;
        this.gracePeriod = gracePeriod;
        this.drainChecker = drainChecker;
        this.drainInitialDelay = drainInitialDelay;
        this.drainMaxDelay = drainMaxDelay;
        this.drainBackoff = Backoff.builder()
                .initialDelay(drainInitialDelay)
                .maxBackoff(drainMaxDelay)
                .build();
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
                        Map<ConsumerSession, ConsumerAssignment> result = rebalanceAndNotify();
                        // First consumer (or rejoining one) — kick off drain checks at the
                        // shortest delay rather than whatever long backoff we'd accumulated
                        // while idle.
                        resetAndRearmDrainPoll();
                        return result;
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
     * consumers, then make sure the drain poller is running so any new sealed segments
     * get noticed quickly (backoff reset → next check fires at the initial delay).
     */
    public synchronized CompletableFuture<Map<ConsumerSession, ConsumerAssignment>> onLayoutChange(
            SegmentLayout newLayout) {
        this.currentLayout = newLayout;
        if (sessions.isEmpty()) {
            segmentAssignments.clear();
            return CompletableFuture.completedFuture(Map.of());
        }
        Map<ConsumerSession, ConsumerAssignment> result = rebalanceAndNotify();
        // New sealed segments may have appeared — restart drain checks from the
        // initial delay rather than continuing whatever long backoff we'd settled into.
        resetAndRearmDrainPoll();
        return CompletableFuture.completedFuture(result);
    }

    /**
     * Install a drain checker on a coordinator that doesn't have one yet. Used on the
     * first STREAM register against a coordinator that was created on the
     * controller-failover restore path (where consumer type wasn't known and we
     * defaulted to "no enforcement"). No-op if a checker is already installed.
     */
    synchronized void installDrainChecker(SegmentDrainChecker checker) {
        if (this.drainChecker != null || checker == null) {
            return;
        }
        this.drainChecker = checker;
        log.info().log("Drain checker installed on existing coordinator; rebalancing");
        if (!sessions.isEmpty()) {
            rebalanceAndNotify();
        }
        resetAndRearmDrainPoll();
    }

    /**
     * Stop the periodic drain-status poller. Called by the controller on close. Idempotent.
     * Also flips a {@code closed} flag so any {@link #pollDrainStatus()} iteration that's
     * mid-flight aborts its rearm step instead of leaking a task into the executor.
     */
    public synchronized void close() {
        closed = true;
        if (drainPollTask != null) {
            drainPollTask.cancel(false);
            drainPollTask = null;
        }
    }

    // --- Drain tracking ---

    /**
     * A segment is assignable to consumers when:
     * <ul>
     *   <li>it's sealed — there's no harm in always handing it out (the v4 layer drains or
     *       sees {@code TopicTerminated} immediately if already drained); or</li>
     *   <li>it's active <b>and</b> every parent in the current layout has been drained for
     *       this subscription. Without the parent-drain check we'd hand a consumer the
     *       child of a just-split segment immediately, breaking per-key ordering against
     *       any unread messages still sitting in the parent.</li>
     * </ul>
     *
     * <p>If no {@link SegmentDrainChecker} was configured (e.g., the simple test
     * constructor), the parent-drain ordering is disabled and every segment is treated
     * as assignable.
     */
    private boolean isAssignable(SegmentInfo segment, SegmentLayout layout) {
        if (drainChecker == null || !segment.isActive()) {
            return true;
        }
        for (long parentId : segment.parentIds()) {
            // A parent that's no longer in the DAG has been pruned (its data is gone), so
            // treat it as drained — there's nothing to wait on.
            if (layout.getAllSegments().containsKey(parentId)
                    && !drainedSegmentIds.contains(parentId)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Mark the given sealed segments as drained. Visible for testing — production code
     * goes through {@link #pollDrainStatus()}, which queries the broker.
     */
    synchronized void markSegmentsDrained(Set<Long> segmentIds) {
        boolean changed = false;
        for (long id : segmentIds) {
            if (drainedSegmentIds.add(id)) {
                changed = true;
            }
        }
        if (changed) {
            // Progress: a sealed segment is now drained. Reset the backoff and re-arm so
            // the next poll lands at the initial delay — once one parent drains, others
            // on the same DAG branch often follow shortly.
            if (!sessions.isEmpty()) {
                log.info().attr("drained", segmentIds).log("Sealed segments drained, rebalancing");
                rebalanceAndNotify();
            }
            resetAndRearmDrainPoll();
        }
    }

    /**
     * Cancel any pending drain-poll task, reset the backoff, and re-arm the poller. Used
     * on every progress event (drain detected, layout change, fresh consumer) so the
     * next check lands at {@link #drainInitialDelay} rather than at whatever long backoff
     * we'd settled into during a quiet period.
     */
    private synchronized void resetAndRearmDrainPoll() {
        drainBackoff.reset();
        if (drainPollTask != null) {
            drainPollTask.cancel(false);
            drainPollTask = null;
        }
        ensureDrainPollerRunning();
    }

    /**
     * (Re-)arm the drain poller. No-op when no {@link SegmentDrainChecker} was configured
     * or when the coordinator has been {@link #close() closed}. Each iteration
     * self-schedules the next via the {@link Backoff} so the cadence grows exponentially
     * while sealed segments stay undrained — short delays at first (a fresh EARLIEST
     * consumer typically drains the parent within seconds), capped at
     * {@link #drainMaxDelay} for long-tail backlogs.
     */
    private void ensureDrainPollerRunning() {
        if (closed || drainChecker == null) {
            return;
        }
        if (drainPollTask != null && !drainPollTask.isCancelled() && !drainPollTask.isDone()) {
            return;
        }
        long delayMs = drainBackoff.next().toMillis();
        drainPollTask = scheduler.schedule(this::pollDrainStatus, delayMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Single poll iteration: ask the drain checker about every sealed segment that isn't
     * already known to be drained, collect the newly-drained ones, and rebalance. Schedule
     * the next iteration via the backoff (or stop, if there's nothing left to check).
     */
    private void pollDrainStatus() {
        SegmentLayout layout;
        List<SegmentInfo> toCheck;
        synchronized (this) {
            drainPollTask = null;
            if (closed || drainPollInProgress || sessions.isEmpty()) {
                return;
            }
            layout = currentLayout;
            toCheck = layout.getAllSegments().values().stream()
                    .filter(seg -> !seg.isActive() && !drainedSegmentIds.contains(seg.segmentId()))
                    .toList();
            if (toCheck.isEmpty()) {
                // Every sealed segment is already drained — no need to keep polling. The
                // poller re-arms when onLayoutChange / registerConsumer adds new work.
                drainBackoff.reset();
                return;
            }
            drainPollInProgress = true;
        }

        Set<Long> newlyDrained = ConcurrentHashMap.newKeySet();
        @SuppressWarnings("rawtypes")
        CompletableFuture[] futures = new CompletableFuture[toCheck.size()];
        for (int i = 0; i < toCheck.size(); i++) {
            SegmentInfo seg = toCheck.get(i);
            futures[i] = drainChecker.isDrained(seg, subscriptionName)
                    .handle((drained, ex) -> {
                        if (ex != null) {
                            // Log at debug — drain checks happen often and transient errors
                            // (e.g., topic still being looked up) shouldn't spam.
                            log.debug().attr("segmentId", seg.segmentId())
                                    .exceptionMessage(ex)
                                    .log("Drain check failed; will retry next poll");
                            return null;
                        }
                        if (Boolean.TRUE.equals(drained)) {
                            newlyDrained.add(seg.segmentId());
                        }
                        return null;
                    });
        }
        CompletableFuture.allOf(futures).whenComplete((__, ex) -> {
            try {
                if (!newlyDrained.isEmpty()) {
                    // markSegmentsDrained resets the backoff for us.
                    markSegmentsDrained(new HashSet<>(newlyDrained));
                }
            } finally {
                synchronized (SubscriptionCoordinator.this) {
                    drainPollInProgress = false;
                    // Re-arm: undrained sealed segments still pending → keep polling, but
                    // with the next (longer) backoff if no progress was made this round.
                    ensureDrainPollerRunning();
                }
            }
        });
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
     * Compute a balanced assignment of segments to consumers.
     *
     * <p>Strategy: sort segments by hash range, then segment id (tiebreak), sort consumers by
     * name, then round-robin. Deterministic: the same inputs always produce the same output,
     * so a new leader recomputing assignments after failover gets the same result as the old
     * leader.
     *
     * <p><b>DAG replay.</b> The assignment includes every <em>sealed</em> segment in the
     * DAG. A fresh EARLIEST subscription needs to read messages produced before it joined,
     * and those may live on segments that have since been sealed by a split / merge.
     *
     * <p><b>Parent-drain ordering.</b> An <em>active</em> child segment is only assigned
     * once <em>every</em> parent in the DAG has been drained for this subscription
     * (tracked in {@link #drainedSegmentIds}). Without this guard a consumer would be
     * handed an active child immediately after a split and start receiving new messages
     * for some key while the same key's pre-split messages still sit unread on the sealed
     * parent — breaking per-key ordering. Initial active segments (those with no parents
     * in the layout) are unaffected and assigned right away.
     *
     * <p>The client side (per-segment v4 consumer) drains a sealed-but-still-present
     * segment naturally and closes it on {@code TopicTerminated}; a sealed-and-already-
     * drained segment yields {@code TopicTerminated} immediately, so the cost of
     * including it is one short-lived v4 subscribe.
     */
    Map<ConsumerSession, ConsumerAssignment> computeAssignment(
            SegmentLayout layout, Collection<ConsumerSession> consumers) {

        if (consumers.isEmpty()) {
            return Map.of();
        }

        List<SegmentInfo> sortedSegments = layout.getAllSegments().values().stream()
                .filter(seg -> isAssignable(seg, layout))
                .sorted(Comparator.comparing(SegmentInfo::hashRange)
                        .thenComparingLong(SegmentInfo::segmentId))
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

    /**
     * Test hook: return the assignment that would be sent right now, computed against the
     * current layout and connected consumers. Visible for unit tests.
     */
    synchronized Map<ConsumerSession, ConsumerAssignment> currentAssignment() {
        return computeAssignment(currentLayout, sessions.values());
    }
}
